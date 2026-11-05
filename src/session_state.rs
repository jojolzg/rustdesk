//! 会话状态回传：把"当前有没有人在看我的屏幕"实时推给宿主。
//!
//! 背景：被控端 CM 主窗口常隐，客户界面上没有任何组件痕迹，唯一的状态出口
//! 是宿主（景智桌面助手）。
//!
//! - **会话状态**：不再落文件，经 `bridge.rs` 的命名管道主动推送（毫秒级）。
//! - **临时密码**：仍落 `%ProgramData%\laztool\remote-password.txt`，供 CLI
//!   `--get-temp-password` 与重装/修复场景兜底；桥连接正常时宿主直接向服务取。

use std::path::PathBuf;

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

/// 按当前远程连接数推送会话状态。
///
/// 调用点只有两处：`AuthedConnID::new`（建连后）与 `Drop for AuthedConnID`
/// （断连后）。两处都必须**在 `AUTHED_CONNS` 锁释放后**调用，否则自锁。
pub fn update(remote_count: usize) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let active = remote_count > 0;
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    crate::bridge::emit_session(active, remote_count as u32, now);
    #[cfg(any(target_os = "android", target_os = "ios"))]
    let _ = (active, now);
    // 连接/断开事件经常伴随密码轮换，顺手刷新一次
    publish_password();
}
