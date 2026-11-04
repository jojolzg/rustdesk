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
            log::warn!("[session-state] 创建状态目录失败: {e}");
            return;
        }
    }
    if let Err(e) = std::fs::write(&path, body) {
        // 写不进去不影响远程会话本身，只在排查时有用
        log::warn!("[session-state] 写入状态文件失败: {e}");
    }
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
