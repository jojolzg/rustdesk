# 已停用的工作流

本仓库是 laztool 定制分支，只交付 **Windows x64** 组件，以下工作流已停用：

| 文件 | 原因 |
|---|---|
| `ci.yml` | 只构建 `x86_64-unknown-linux-gnu`，与 Windows 交付无关 |
| `fdroid.yml` | Android / F-Droid 版本文件同步 |
| `playground.yml` | 开发用实验构建（macOS + Android） |
| `update-webpki-roots.yml` | 定时 PR 更新证书根列表，定制分支不需要 |

GitHub Actions 只读取 `.github/workflows/`，放在本目录不会被执行。
需要恢复时 `git mv` 回 `.github/workflows/` 即可。
