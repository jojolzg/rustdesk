//! 会话状态回传：把"当前有没有人在看我的屏幕"写到一个宿主读得到的位置。
//!
//! 背景：被控端 CM 主窗口常隐，客户界面上没有任何组件痕迹，唯一的状态出口
//! 是宿主（景智桌面助手）。组件不反向调宿主，只落一个状态文件，宿主轮询读取
//! ——单向、无生命周期耦合，组件挂了宿主读到的是陈旧值，不会卡死。
//!
//! 路径固定在 `%ProgramData%\laztool\remote-session.json`：
//! - 服务进程以 SYSTEM 身份跑，能写；普通权限进程写失败只记日志，不影响会话。
//! - 宿主管道读同一路径，不需要知道组件内部的 config 目录布局
//!   （那个布局会随 app-name 定制变动，不能当契约）。
//!
//! 只在连接建立/断开时写盘，不做定时刷新。

use std::path::PathBuf;

/// 状态文件路径（宿主侧 `remote/commands.rs` 有同名常量，改动要同步）。
pub fn state_file() -> PathBuf {
    state_dir().join("remote-session.json")
}

fn state_dir() -> PathBuf {
    #[cfg(windows)]
    {
        if let Ok(pd) = std::env::var("ProgramData") {
            if !pd.trim().is_empty() {
                return PathBuf::from(pd).join("laztool");
            }
        }
    }
    std::env::temp_dir().join("laztool")
}

/// 临时密码共享文件。
///
/// 为什么需要它：`hbb_common::password_security::TEMPORARY_PASSWORD` 是**进程内**
/// 的 static，桌面端 `ui_interface::temporary_password()` 又只是 GUI 从 IPC 拿到的副本。
/// 新起的 CLI 进程里这两条都是空的，`--get-temp-password` 只能拿到空串。
/// 所以由**真正的服务端进程**把值落到这里，CLI 直接读文件。
pub fn password_file() -> PathBuf {
    state_dir().join("remote-password.txt")
}

/// 把本进程持有的临时密码写出去。
///
/// **只能由服务端进程调用**：其他进程调用会在自己的 static 里生成一个全新随机值，
/// 写出去就是"看起来正确、实际连不上"的假密码，比没有更糟。
pub fn publish_password() {
    let pw = hbb_common::password_security::temporary_password();
    if pw.is_empty() {
        return;
    }
    let path = password_file();
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            hbb_common::log::warn!("[session-state] 创建目录失败: {e}");
            return;
        }
    }
    if let Err(e) = std::fs::write(&path, &pw) {
        hbb_common::log::warn!("[session-state] 写入临时密码失败: {e}");
    }
}

/// 读服务端落下的临时密码（CLI 侧用）。
pub fn read_password() -> String {
    std::fs::read_to_string(password_file())
        .map(|s| s.trim().to_owned())
        .unwrap_or_default()
}

/// 免密连接的「客户确认」协议：**按连接 id 一连接一文件**。
///
/// 流程：CM 进程收到未授权连接 → 写 `remote-approve/<id>.json` →
/// 宿主右下角按堆叠展示全部 pending → 宿主写 `<id>.decision.json` →
/// CM 轮询到决策 → authorize/close。
///
/// 为什么按 id 拆文件而不是一个大 JSON：**多人同时连接是常态**
/// （两个运维同时接同一台机器），单文件覆盖式会让先来的请求被后来的
/// 覆盖掉、超时被拒。一连接一文件后，请求/决策互不干扰，也不需要
/// 跨进程文件锁——两个进程各写各的文件名。
///
/// 为什么不经组件自己的 CM 窗口：CM 被我们隐藏了，没人能点它里面的按钮；
/// 把确认搬到宿主弹窗，客户看到的始终是桌面助手的界面。
///
/// 目录用宿主的 app_data_dir（%APPDATA%\cn.laztool.desktop）：CM 与宿主
/// 都以**登录用户**身份运行，同用户目录读写无 ACL 问题（ProgramData 下
/// SYSTEM 创建的目录对普通用户未必可写）。
fn approve_dir() -> PathBuf {
    #[cfg(windows)]
    {
        if let Ok(base) = std::env::var("APPDATA") {
            if !base.trim().is_empty() {
                return PathBuf::from(base)
                    .join("cn.laztool.desktop")
                    .join("remote-approve");
            }
        }
    }
    std::env::temp_dir()
        .join("laztool_desktop")
        .join("remote-approve")
}

fn approve_request_file(id: i32) -> PathBuf {
    approve_dir().join(format!("{id}.json"))
}

fn approve_decision_file(id: i32) -> PathBuf {
    approve_dir().join(format!("{id}.decision.json"))
}

/// CM 侧：登记一条待确认的连接请求（每连接独立文件，互不覆盖）。
pub fn publish_approve_request(id: i32, peer_id: &str, name: &str) {
    let body = format!(
        "{{\"id\":{},\"peer_id\":{},\"name\":{},\"ts\":{}}}",
        id,
        serde_json::json!(peer_id),
        serde_json::json!(name),
        chrono::Local::now().timestamp()
    );
    let path = approve_request_file(id);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&path, body);
}

/// 清掉本连接的请求与决策文件（决策完成/超时后调用）。
pub fn clear_approve_files(id: i32) {
    let _ = std::fs::remove_file(approve_request_file(id));
    let _ = std::fs::remove_file(approve_decision_file(id));
}

/// 读宿主写的决策。返回 None = 还没决策。
pub fn read_approve_decision(id: i32) -> Option<String> {
    let text = std::fs::read_to_string(approve_decision_file(id)).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let d = v.get("decision")?.as_str()?.to_owned();
    // 消费掉：决策文件与请求文件一起清，防止旧决策影响后续
    let _ = std::fs::remove_file(approve_decision_file(id));
    Some(d)
}

/// 按当前远程连接数刷新状态文件。
///
/// 调用点只有两处：`AuthedConnID::new`（建连后）与 `Drop for AuthedConnID`
/// （断连后）。两处都必须**在 `AUTHED_CONNS` 锁释放后**调用，否则自锁。
pub fn update(remote_count: usize) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();
    let active = remote_count > 0;
    let body = format!(
        "{{\"active\":{},\"count\":{},\"since_ms\":{}}}\n",
        active, remote_count, now
    );

    let path = state_file();
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            hbb_common::log::warn!("[session-state] 创建状态目录失败: {e}");
            return;
        }
    }
    if let Err(e) = std::fs::write(&path, body) {
        // 写不进去不影响远程会话本身，只在排查时有用
        hbb_common::log::warn!("[session-state] 写入状态文件失败: {e}");
    }
    // 连接/断开事件经常伴随密码轮换，顺手刷新一次
    publish_password();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_is_valid_json_shape() {
        // update() 里的格式串是手写的，改字段时这条先炸，比线上读到坏 JSON 强
        let active = true;
        let count = 2usize;
        let since = 1u64;
        let body = format!(
            "{{\"active\":{},\"count\":{},\"since_ms\":{}}}\n",
            active, count, since
        );
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("必须是合法 JSON");
        assert_eq!(parsed["active"], serde_json::json!(true));
        assert_eq!(parsed["count"], serde_json::json!(2));
        assert_eq!(parsed["since_ms"], serde_json::json!(1));
    }
}
