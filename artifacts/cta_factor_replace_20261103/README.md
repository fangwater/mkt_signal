# CTA 计算环境安装与用户暂停

本地发布源：`arbmm` 的 `163bcf9b`；网关修正提交 `77e4bf18`。
远端私有审计：`~/crypto_cta_manager/maintenance-backups/cta_factor_replace_20261103T040039Z/`。

旧 RX01/03/04 及跨所 Spread 的因子、信号、配置和面板服务已停止。
新 Prep / Spread 单组产物已安装，统一 Config 服务监听 loopback19220，CTA 网关已切换。
首次网关校验失败并自动还原；原因是未定义的 Manager upstream，改为 loopback18201 后校验通过。
新接口返回 200、旧配置 URL 返回 410；81 个受保护进程身份保持。

用户明确要求先继续开发，故于 2026-11-03 04:25 UTC 通过环境本地脚本停止两个新计算进程。
实际扫描无存活计算进程，见 `user-paused.json`。四个仓位开关关闭、新策略无绑定，
Exec06 和跨所两腿的下单进程保持停止。Config 服务和文件保留；后续启动需用户明确指示。
多策略组仅本地开发，尚未部署；不得将安装和暂停回执描述为多组正式上线。

`remote.py` 为本次安装审计脚本，`install` / `switch` / `gateway` 均是生产变更，
本轮不再执行。`payload/` 和 `payload.tar` 含精确发布产物，已排除出 Git。

后续用户确认补齐 Prep02 并明确重新授权部署，已发布多组版本；最新状态见
`../cta_prep02_deploy_20261103/verified.json`。本目录的暂停记录仅描述 04:25 UTC 的历史状态。
