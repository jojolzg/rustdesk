//! 组件侧实时桥客户端（Windows 命名管道）。
//!
//! 宿主 `laztool` 桌面助手是唯一 Server（`\\.\pipe\laztool-remote-bridge`），
//! 组件内两个进程各建一条常驻连接：
//! - `service`（SYSTEM，常驻）：上报会话状态；执行 `kill_sessions` /
//!   `get_temp_password` / `rotate_password` / `get_id`；
//! - `cm`（登录用户，连接到达时由 service 拉起）：上报审批请求、接收审批决策。
//!
//! 协议与宿主侧 `src-tauri/src/remote/bridge.rs` 完全对称：UTF-8 换行分隔 JSON，
//! 信封 `{v,type,seq,ts,req_id?,data}`。管道名、协议版本与字段改动必须两端同步。
//!
//! 取代原先的「审批文件目录 + 300ms 轮询 + 会话状态文件」，实时性从秒级降到毫秒级，
//! 且不再依赖文件系统做跨进程通信。

#[cfg(not(windows))]
use hbb_common::log;
use serde_json::{json, Value};

/// 管道名（宿主侧同名常量）。
pub const PIPE_NAME: &str = r"\\.\pipe\laztool-remote-bridge";
/// 协议版本：宿主 hello 校验失败会直接断开。
pub const PROTO: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Service,
    Cm,
}

impl Role {
    fn key(self) -> &'static str {
        match self {
            Role::Service => "service",
            Role::Cm => "cm",
        }
    }
}

/// 启动后台桥客户端。每个进程只启动一次。
pub fn start(role: Role) {
    #[cfg(windows)]
    {
        imp::start(role);
    }
    #[cfg(not(windows))]
    {
        let _ = role;
        log::debug!("[bridge] 非 Windows 平台无宿主桥，跳过后台连接");
    }
}

/// 会话状态变化 → 主动上报（由 `session_state::update` 调用）。
pub fn emit_session(active: bool, count: u32, since_ms: i64) {
    #[cfg(windows)]
    {
        imp::send_event(
            "session.changed",
            json!({ "active": active, "count": count, "since_ms": since_ms }),
        );
    }
    #[cfg(not(windows))]
    {
        let _ = (active, count, since_ms);
    }
}

/// CM 收到未授权连接 → 推送审批请求，阻塞等待宿主决策（30 秒超时自动拒绝）。
pub fn request_approval(id: i32, peer_id: &str, name: &str, kind: &str) {
    #[cfg(windows)]
    {
        imp::request_approval(id, peer_id, name, kind);
    }
    #[cfg(not(windows))]
    {
        let _ = (peer_id, name, kind);
        // 非 Windows 不交付组件；直接拒绝，避免连接悬挂
        log::warn!("[bridge] 非 Windows 平台无法展示确认卡，拒绝 id={id}");
        crate::ui_cm_interface::close(id);
    }
}

#[cfg(windows)]
mod imp {
    use std::collections::HashMap;
    use std::os::windows::io::AsRawHandle;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use hbb_common::log;
    use hbb_common::tokio::{
        self,
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::windows::named_pipe::{ClientOptions, NamedPipeClient},
        sync::mpsc as tokio_mpsc,
    };
    use serde_json::{json, Value};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::Pipes::GetNamedPipeServerProcessId;

    use super::{Role, PIPE_NAME, PROTO};

    const APPROVAL_TIMEOUT_MS: u64 = 30_000;
    const MAX_LINE: usize = 64 * 1024;
    /// 宿主（景智桌面助手）的可执行文件名与产品名。用于生成信任基准路径：
    /// 安装期记录（laztool-host.txt）优先，缺失时用卸载登记项的 InstallLocation。
    const HOST_EXE_NAME: &str = "景智桌面助手.exe";
    const HOST_PRODUCT_NAME: &str = "景智桌面助手";

    /// 当前连接的上行通道；断线时为 None，事件调用方无感跳过。
    static SENDER: Mutex<Option<tokio_mpsc::UnboundedSender<String>>> = Mutex::new(None);
    /// writer 任务是否存活。仅靠 SENDER 是否为 Some 不够：writer 写失败退出后
    /// SENDER 仍可能残留，事件会静默丢进无人消费的通道。
    static SENDER_ALIVE: AtomicBool = AtomicBool::new(false);
    /// 待决策的审批请求：id → 决策通道
    static PENDING: OnceLock<Mutex<HashMap<i32, mpsc::Sender<bool>>>> = OnceLock::new();
    static STARTED: AtomicBool = AtomicBool::new(false);

    pub fn start(role: Role) {
        if STARTED.swap(true, Ordering::SeqCst) {
            return;
        }
        #[cfg(windows)]
        if role == Role::Service {
            // 组件生命周期不得大于宿主：服务端进程定期确认宿主还在。
            spawn_host_watchdog();
        }
        let _ = std::thread::Builder::new()
            .name("laztool-bridge".to_owned())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        log::warn!("[bridge] 创建运行时失败: {e}");
                        return;
                    }
                };
                rt.block_on(connect_loop(role));
            });
    }

    pub fn send_event(kind: &str, data: Value) {
        if !SENDER_ALIVE.load(Ordering::Relaxed) {
            // writer 已退出、连接已半死：静默丢弃，避免把事件塞进无人消费的通道。
            return;
        }
        let sender = SENDER.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(tx) = sender {
            send_line(&tx, kind, None, data);
        }
    }

    pub fn request_approval(id: i32, peer_id: &str, name: &str, kind: &str) {
        let peer_id = peer_id.to_owned();
        let name = name.to_owned();
        let kind = kind.to_owned();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(APPROVAL_TIMEOUT_MS);
            let expires_at_ms = now_ms() + APPROVAL_TIMEOUT_MS as i64;
            let (tx, rx) = mpsc::channel::<bool>();
            pending()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id, tx);

            // CM 进程刚被 service 拉起时，桥可能还没连上：给它 3 秒，
            // 不能因为启动竞态把首个免密连接直接拒掉。
            let sender = wait_for_sender(Duration::from_secs(3));
            let sent = match sender {
                Some(tx) => {
                    send_line(
                        &tx,
                        "approval.request",
                        None,
                        json!({
                            "id": id,
                            "peer_id": peer_id,
                            "name": name,
                            "kind": kind,
                            "expires_at_ms": expires_at_ms,
                        }),
                    );
                    true
                }
                None => false,
            };

            if !sent {
                pending()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id);
                log::warn!("[bridge] 宿主未连接，拒绝 id={id}");
                crate::ui_cm_interface::close(id);
                return;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let decision = rx.recv_timeout(remaining).ok();
            pending()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            match decision {
                Some(true) => {
                    log::info!("[bridge] id={id} allowed by host");
                    crate::ui_cm_interface::authorize(id);
                    send_event("approval.closed", json!({ "id": id, "reason": "allowed" }));
                }
                Some(false) => {
                    log::info!("[bridge] id={id} denied by host");
                    crate::ui_cm_interface::close(id);
                    send_event("approval.closed", json!({ "id": id, "reason": "denied" }));
                }
                None => {
                    log::info!("[bridge] id={id} timeout, deny");
                    crate::ui_cm_interface::close(id);
                    send_event("approval.closed", json!({ "id": id, "reason": "expired" }));
                }
            }
        });
    }

    /// 轮询等待桥连接就绪（最多 `timeout`），避免 CM 启动时序导致误拒。
    fn wait_for_sender(timeout: Duration) -> Option<tokio_mpsc::UnboundedSender<String>> {
        let deadline = Instant::now() + timeout;
        loop {
            if SENDER_ALIVE.load(Ordering::Relaxed) {
                if let Some(tx) = SENDER.lock().unwrap_or_else(|e| e.into_inner()).clone() {
                    return Some(tx);
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn pending() -> &'static Mutex<HashMap<i32, mpsc::Sender<bool>>> {
        PENDING.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// 校验管道另一端的 Server 身份，防止本地进程抢占同名管道后冒充宿主。
    ///
    /// 旧实现直接 `open` 后发 hello，任何先创建 `\\.\pipe\laztool-remote-bridge` 的
    /// 进程都能收到设备的 ID/版本并在审批请求上回 “allow”。这里在发送 hello 前
    /// 校验：宿主进程必须是受信任路径下的桌面助手、不能是 SYSTEM、且会话匹配
    /// （CM 与宿主同会话；service 连接的宿主必须在交互会话）。
    fn verify_server(pipe: &NamedPipeClient, role: Role) -> Result<(), String> {
        let mut pid = 0u32;
        let ok = unsafe { GetNamedPipeServerProcessId(HANDLE(pipe.as_raw_handle()), &mut pid) };
        if ok.is_err() || pid == 0 {
            return Err("无法读取宿主进程 ID".to_owned());
        }

        let path = crate::platform::windows::get_process_executable_path(pid)
            .map_err(|e| format!("无法读取宿主进程路径: {e}"))?;
        // 信任基准 = 安装期写入的宿主绝对路径（安装器在提权上下文写，用户态伪造不了）。
        // 运行期只把基准和**实际运行路径**做动态比对，不写死任何目录：
        // - 有记录：路径必须完全一致（默认 Program Files 与自定义安装目录都能覆盖）；
        // - 无记录：release fail-closed，不再用“名称 + Program Files”猜测；
        //   debug 构建保留 `tauri dev` 放行（这不进发布产物）。
        match expected_host_path() {
            Some(expected) => {
                if !paths_equal(&path, &expected) {
                    return Err(format!(
                        "宿主进程路径与安装记录不一致: 实际 {}, 期望 {}",
                        path.display(),
                        expected.display()
                    ));
                }
            }
            None => {
                #[cfg(debug_assertions)]
                {
                    if !is_trusted_host_path(&path) {
                        return Err(format!(
                            "缺少安装期宿主路径记录，且不是受信任的 debug 宿主: {}",
                            path.display()
                        ));
                    }
                }
                #[cfg(not(debug_assertions))]
                {
                    return Err(format!(
                        "无法确定宿主安装位置（无安装期记录且卸载登记项无 InstallLocation），拒绝连接: {}",
                        path.display()
                    ));
                }
            }
        }
        // 无法判定时按最坏情况处理（视为 SYSTEM），宁可断开重连也不放行。
        if crate::platform::windows::is_process_running_as_system(pid).unwrap_or(true) {
            return Err("宿主进程不能以 SYSTEM 身份运行".to_owned());
        }

        let server_session = crate::platform::windows::get_session_id_of_process(pid)
            .ok_or_else(|| "无法读取宿主会话".to_owned())?;
        let current_session = crate::platform::windows::get_current_process_session_id()
            .ok_or_else(|| "无法读取组件会话".to_owned())?;
        match role {
            Role::Cm => {
                if server_session != current_session {
                    return Err(format!(
                        "宿主会话 {server_session} 与组件会话 {current_session} 不一致"
                    ));
                }
            }
            Role::Service => {
                if server_session == 0 {
                    return Err("宿主进程不应以服务会话身份运行".to_owned());
                }
            }
        }
        Ok(())
    }

    /// 信任基准路径：安装期记录（laztool-host.txt）优先，缺失时回落到
    /// 卸载登记项里的 `InstallLocation`（NSIS 与 MSI 都会写）。
    ///
    /// 两者都来自管理员的安装过程（Program Files 与 HKLM），运行期只和**实际运行的**
    /// 宿主进程路径动态比对；不是用户态能随手写的文件。
    fn expected_host_path() -> Option<PathBuf> {
        expected_host_path_from_file().or_else(|| {
            crate::platform::windows::get_host_install_location(HOST_PRODUCT_NAME)
                .map(|dir| PathBuf::from(dir).join(HOST_EXE_NAME))
        })
    }

    /// 读取安装期写入的宿主 exe 绝对路径（UTF-16LE，位于组件安装目录）。
    fn expected_host_path_from_file() -> Option<PathBuf> {
        let exe = std::env::current_exe().ok()?;
        let dir = exe.parent()?;
        let bytes = std::fs::read(dir.join("laztool-host.txt")).ok()?;
        let mut units = Vec::with_capacity(bytes.len() / 2);
        for chunk in bytes.chunks_exact(2) {
            units.push(u16::from_le_bytes([chunk[0], chunk[1]]));
        }
        let text = String::from_utf16_lossy(&units);
        let line = text
            .trim()
            .trim_start_matches('\u{feff}')
            .lines()
            .next()?
            .trim();
        if line.is_empty() {
            None
        } else {
            Some(PathBuf::from(line))
        }
    }

    fn normalize_path(path: &Path) -> String {
        path.to_string_lossy()
            .replace('/', "\\")
            .to_ascii_lowercase()
            .trim()
            .to_owned()
    }

    fn paths_equal(left: &Path, right: &Path) -> bool {
        normalize_path(left) == normalize_path(right)
    }

    /// 仅 debug 构建使用：没有安装期记录时，允许 `tauri dev` 的宿主。
    /// release 构建要求必须有记录，因此这个函数不参与发布产物。
    #[cfg(debug_assertions)]
    fn is_trusted_host_path(path: &Path) -> bool {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return false;
        };
        if name.eq_ignore_ascii_case(HOST_EXE_NAME) {
            return under_program_files(path);
        }
        name.eq_ignore_ascii_case("src-tauri.exe")
    }

    #[cfg(debug_assertions)]
    fn under_program_files(path: &Path) -> bool {
        let normalized = path
            .to_string_lossy()
            .replace('/', "\\")
            .to_ascii_lowercase();
        for key in ["ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"] {
            if let Ok(base) = std::env::var(key) {
                let base = base.replace('/', "\\").to_ascii_lowercase();
                if normalized.starts_with(&(base + "\\")) {
                    return true;
                }
            }
        }
        false
    }

    async fn connect_loop(role: Role) {
        let mut backoff = Duration::from_millis(200);
        loop {
            match ClientOptions::new().open(PIPE_NAME) {
                Ok(pipe) => {
                    // 先确认 Server 是宿主，再发 hello；不通过就丢弃连接、退避重试。
                    if let Err(e) = verify_server(&pipe, role) {
                        log::warn!("[bridge] 拒绝不受信任的宿主连接: {e}");
                    } else {
                        backoff = Duration::from_millis(200);
                        run_connection(pipe, role).await;
                        // 宿主刚断开时不要立刻复连打满 CPU
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
                Err(e) => {
                    log::debug!("[bridge] 连接宿主失败: {e}");
                }
            }
            // 指数退避 + 以时间戳取模做抖动，避免多进程同时重连
            let jitter = Duration::from_millis((now_ms().unsigned_abs() % 200) as u64);
            tokio::time::sleep(backoff + jitter).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }

    async fn run_connection(pipe: NamedPipeClient, role: Role) {
        let (rd, wr) = tokio::io::split(pipe);
        let (tx, mut rx) = tokio_mpsc::unbounded_channel::<String>();
        *SENDER.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx.clone());
        SENDER_ALIVE.store(true, Ordering::SeqCst);

        let writer = tokio::spawn(async move {
            let mut wr = wr;
            while let Some(line) = rx.recv().await {
                if wr.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if wr.write_all(b"\n").await.is_err() {
                    break;
                }
            }
            SENDER_ALIVE.store(false, Ordering::SeqCst);
        });

        send_line(
            &tx,
            "hello",
            None,
            json!({
                "role": role.key(),
                "version": crate::VERSION,
                "proto": PROTO,
                "pid": std::process::id(),
                // 未来宿主按会话路由多 CM 时需要；当前宿主忽略未知字段。
                "session_id": crate::platform::windows::get_current_process_session_id().unwrap_or(0),
                "id": hbb_common::config::Config::get_id(),
            }),
        );
        // service 连上后先补一帧当前会话态，宿主不必等下一次建连/断连
        if role == Role::Service {
            let count = authed_count();
            send_line(
                &tx,
                "session.changed",
                None,
                json!({
                    "active": count > 0,
                    "count": count,
                    "since_ms": now_ms(),
                }),
            );
        }

        let mut reader = BufReader::new(rd);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) => {
                    log::debug!("[bridge] 读取宿主消息结束: {e}");
                    break;
                }
            }
            if line.len() > MAX_LINE {
                log::warn!("[bridge] 单行超长，断开重连");
                break;
            }
            let text = line.trim();
            if text.is_empty() {
                continue;
            }
            let v: Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("[bridge] 非法 JSON: {e}");
                    continue;
                }
            };
            dispatch(&tx, role, &v);
        }

        // 先清全局发送端并 drop 本地 tx，writer 的 rx 才会收到 None 并退出；
        // 否则 writer.await 会一直等一个永远不会关闭的通道（宿主正常断开时卡死重连）。
        SENDER_ALIVE.store(false, Ordering::SeqCst);
        *SENDER.lock().unwrap_or_else(|e| e.into_inner()) = None;
        drop(tx);
        let _ = writer.await;
        log::info!("[bridge] 与宿主断开 (role={})", role.key());
    }

    fn dispatch(tx: &tokio_mpsc::UnboundedSender<String>, role: Role, v: &Value) {
        match v.get("type").and_then(Value::as_str).unwrap_or("") {
            "cmd" => {
                let req_id = v.get("req_id").and_then(Value::as_str).unwrap_or("");
                let name = v["data"].get("name").and_then(Value::as_str).unwrap_or("");
                let ack = match handle_cmd(role, name) {
                    Ok(data) => json!({ "ok": true, "data": data }),
                    Err(e) => json!({ "ok": false, "error": e }),
                };
                send_line(tx, "ack", Some(req_id), ack);
            }
            "approval.decision" => {
                let req_id = v.get("req_id").and_then(Value::as_str).unwrap_or("");
                let id = v["data"].get("id").and_then(Value::as_i64).unwrap_or(0) as i32;
                let allow = v["data"]
                    .get("allow")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let delivered = if let Some(sender) = pending()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id)
                {
                    let _ = sender.send(allow);
                    true
                } else {
                    log::warn!("[bridge] 决策找不到对应请求 id={id}");
                    false
                };
                // 必须回 ack：宿主按 2s 超时配对决策请求，旧实现不回会导致
                // 每次审批都被判失败、审计写不进，尽管授权/拒绝实际已生效。
                let ack = if delivered {
                    json!({ "ok": true, "data": { "id": id } })
                } else {
                    json!({ "ok": false, "error": format!("决策找不到对应请求 id={id}") })
                };
                send_line(tx, "ack", Some(req_id), ack);
            }
            "ping" => {
                send_line(tx, "pong", None, json!({}));
            }
            other => log::debug!("[bridge] 忽略未知消息: {other}"),
        }
    }

    fn handle_cmd(role: Role, name: &str) -> Result<Value, String> {
        if role != Role::Service {
            return Err("当前连接不支持控制指令".to_owned());
        }
        match name {
            "kill_sessions" => Ok(json!({ "killed": crate::server::kill_remote_sessions() })),
            "shutdown" => {
                // 宿主退出前调用：先断开所有会话，再延迟停服务并退出自身。
                // 延迟是为了让这条 ack 先写回宿主（宿主最多等 2s）。
                let killed = crate::server::kill_remote_sessions();
                std::thread::spawn(|| {
                    std::thread::sleep(Duration::from_millis(600));
                    shutdown_service_and_exit();
                });
                Ok(json!({ "killed": killed }))
            }
            "get_temp_password" => {
                let mut pw = hbb_common::password_security::temporary_password();
                if pw.is_empty() {
                    pw = crate::session_state::read_password();
                }
                Ok(json!({ "password": pw }))
            }
            "rotate_password" => {
                // 只有服务端进程的 static 才是权威值；轮换后立刻落盘供 CLI 兜底
                hbb_common::password_security::update_temporary_password();
                crate::session_state::publish_password();
                send_event("password.rotated", json!({}));
                Ok(json!({ "rotated": true }))
            }
            "get_id" => Ok(json!({ "id": hbb_common::config::Config::get_id() })),
            other => Err(format!("未知指令: {other}")),
        }
    }

    /// 宿主管道是否还在（`WaitNamedPipeW` 只探测，不占用实例）。
    #[cfg(windows)]
    fn host_pipe_present() -> bool {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;

        #[link(name = "kernel32")]
        extern "system" {
            fn WaitNamedPipeW(name: *const u16, timeout: u32) -> i32;
            fn GetLastError() -> u32;
        }

        let name: Vec<u16> = OsStr::new(PIPE_NAME)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        if unsafe { WaitNamedPipeW(name.as_ptr(), 0) } != 0 {
            return true;
        }
        // 231 = ERROR_PIPE_BUSY：管道存在但实例都被占用，仍视为宿主活着
        const ERROR_PIPE_BUSY: u32 = 231;
        unsafe { GetLastError() == ERROR_PIPE_BUSY }
    }

    /// 停止组件服务并退出当前进程（服务端进程以 SYSTEM 身份运行，可执行 sc stop）。
    #[cfg(windows)]
    fn shutdown_service_and_exit() {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let app = crate::get_app_name();
        let _ = std::process::Command::new("sc.exe")
            .args(["stop", app.as_str()])
            .creation_flags(CREATE_NO_WINDOW)
            .status();
        std::thread::sleep(Duration::from_millis(800));
        std::process::exit(0);
    }

    /// 宿主异常消失（崩溃/被强杀）后的兜底：90s 没有宿主就自行收尾。
    #[cfg(windows)]
    fn spawn_host_watchdog() {
        let _ = std::thread::Builder::new()
            .name("laztool-host-watch".to_owned())
            .spawn(|| {
                let mut missing_secs = 0u64;
                loop {
                    std::thread::sleep(Duration::from_secs(15));
                    if host_pipe_present() {
                        missing_secs = 0;
                        continue;
                    }
                    missing_secs += 15;
                    if missing_secs >= 90 {
                        log::warn!("[bridge] 宿主已消失 {missing_secs}s，停止组件服务");
                        shutdown_service_and_exit();
                    }
                }
            });
    }

    fn authed_count() -> u32 {
        crate::server::AUTHED_CONNS
            .lock()
            .unwrap()
            .iter()
            .filter(|c| {
                matches!(
                    c.conn_type,
                    crate::server::AuthConnType::Remote | crate::server::AuthConnType::ViewCamera
                )
            })
            .count() as u32
    }

    fn send_line(
        tx: &tokio_mpsc::UnboundedSender<String>,
        kind: &str,
        req_id: Option<&str>,
        data: Value,
    ) {
        let mut v = json!({
            "v": PROTO,
            "type": kind,
            "seq": 0,
            "ts": now_ms(),
            "data": data,
        });
        if let Some(r) = req_id {
            v["req_id"] = json!(r);
        }
        let _ = tx.send(v.to_string());
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn protocol_contract_is_locked() {
        assert_eq!(super::PROTO, 1);
        assert_eq!(super::PIPE_NAME, r"\\.\pipe\laztool-remote-bridge");
    }

    #[test]
    fn role_keys_match_host() {
        assert_eq!(super::Role::Service.key(), "service");
        assert_eq!(super::Role::Cm.key(), "cm");
    }
}
