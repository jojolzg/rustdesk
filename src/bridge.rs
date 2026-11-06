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
pub fn request_approval(id: i32, peer_id: &str, name: &str) {
    #[cfg(windows)]
    {
        imp::request_approval(id, peer_id, name);
    }
    #[cfg(not(windows))]
    {
        let _ = (peer_id, name);
        // 非 Windows 不交付组件；直接拒绝，避免连接悬挂
        log::warn!("[bridge] 非 Windows 平台无法展示确认卡，拒绝 id={id}");
        crate::ui_cm_interface::close(id);
    }
}

#[cfg(windows)]
mod imp {
    use std::collections::HashMap;
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

    use super::{Role, PIPE_NAME, PROTO};

    const APPROVAL_TIMEOUT_MS: u64 = 30_000;
    const MAX_LINE: usize = 64 * 1024;

    /// 当前连接的上行通道；断线时为 None，事件调用方无感跳过。
    static SENDER: Mutex<Option<tokio_mpsc::UnboundedSender<String>>> = Mutex::new(None);
    /// 待决策的审批请求：id → 决策通道
    static PENDING: OnceLock<Mutex<HashMap<i32, mpsc::Sender<bool>>>> = OnceLock::new();
    static STARTED: AtomicBool = AtomicBool::new(false);

    pub fn start(role: Role) {
        if STARTED.swap(true, Ordering::SeqCst) {
            return;
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
        let sender = SENDER.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(tx) = sender {
            send_line(&tx, kind, None, data);
        }
    }

    pub fn request_approval(id: i32, peer_id: &str, name: &str) {
        let peer_id = peer_id.to_owned();
        let name = name.to_owned();
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
            if let Some(tx) = SENDER.lock().unwrap_or_else(|e| e.into_inner()).clone() {
                return Some(tx);
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

    async fn connect_loop(role: Role) {
        let mut backoff = Duration::from_millis(200);
        loop {
            match ClientOptions::new().open(PIPE_NAME) {
                Ok(pipe) => {
                    backoff = Duration::from_millis(200);
                    run_connection(pipe, role).await;
                    // 宿主刚断开时不要立刻复连打满 CPU
                    tokio::time::sleep(Duration::from_millis(200)).await;
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

        *SENDER.lock().unwrap_or_else(|e| e.into_inner()) = None;
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
                let id = v["data"].get("id").and_then(Value::as_i64).unwrap_or(0) as i32;
                let allow = v["data"]
                    .get("allow")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if let Some(sender) = pending()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id)
                {
                    let _ = sender.send(allow);
                } else {
                    log::warn!("[bridge] 决策找不到对应请求 id={id}");
                }
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
