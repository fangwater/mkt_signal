# CTA 配置页精简

按用户四项要求调整原配置入口：沿用旧 CTA Perp 深色样式；删除仓位表、计算参数、
逐行因子编辑和其他因子输入格式；以策略组及频段标签分别配置。
唯一因子输入为 `频率 / NQ / 因子 / 符号 / 标的数组` JSON，频率使用 1m / 5m / 15m，
当前频段只接受相应频率。开关、单份 USDT、开平仓参数按当前频段保存。

发布源 `arbmm` / `38c037e9`，已提交推送。本地从干净已提交的源码构建 Config release，
SHA-256 与 `manifest.json` 一致；上传验证后原子替换，生产主机未运行 Cargo。
只通过环境本地脚本重启 `~/cta-prep-binance-futures/cta-factor-config`，新 PID3562496，
仍使用 loopback19220 和原网关。Prep PID3522829 / start_ticks137144251 / CPU17 保持。
两套环境 JSON、TOML、env.sh 哈希及配置 revision 全部保持，81 个受保护进程身份一致。
没有改动因子、额度、开关、执行绑定或网络源 IP。

本地真实 Chromium 验证七份配置，以及当前频段隔离保存、单一 JSON 格式、
错误频段/无效因子拒绝、重复规则去重、不同 NQ 保留、并发冲突保留草稿、
隐藏配置参数保留、Spread 来源及规则顺序不变、哈希深链接切换、移动端无溢出。
Config HTTP 测试通过。`local-browser-verification.json` 保存本地验证结果。

线上 `browser.mjs` 只校验预览，不保存，七份配置行数/金额/关闭状态及桌面/手机检查通过，
JavaScript 错误0。临时登录会话已删除，截图不含凭据。
`deployment-verification.json` 记录产物及进程、配置不变校验，`browser-verification.json` 为线上结果。

配置入口保持：
`http://13.115.227.29:4191/cta/cta-prep-binance-futures/config/?worker=cta-prep-binance-futures#prep02_binance-futures_1min`

远端私有备份：
`~/crypto_cta_manager/maintenance-backups/cta_config_simplify_20261103T052746Z/`。
