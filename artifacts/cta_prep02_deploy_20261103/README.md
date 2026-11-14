# Prep02 多组计算部署

用户确认将 SOL/DOGE 15min 的 `TD_MI_040` 修正为研究库已有的 `TD_MT_040`，并明确授权部署。
发布源码为 `arbmm` / `fa63d6e4`，已推送。生产发布前本地库测试 1447 通过、3 忽略，
Config HTTP 测试 1 通过；格式与差异检查通过。

为避免混入同一工作区其他操作员的未提交修改，在本地工作区内导出该提交的干净源码快照，
使用它的 Cargo manifest 进行 release 构建。发布 Prep、Config、Spread 三个精确产物，
上传后逐项核对 SHA-256 并原子替换；`manifest.json` 记录所有二进制及配置哈希。
源码快照、二进制 bundle 和 tar 不入 Git。未在生产主机运行 Cargo。

线上环境 `~/cta-prep-binance-futures`：Prep01/Prep02 各自 1min/5min/15min，
六张仓位表、408 项合并计算。Prep PID3522829，CPU17；Config PID3522750，
监听 loopback19220。Spread01 的配置和计算产物同步升级，Spread 计算保持停止。
七个新 Manager 策略定义均为零目标、无绑定，所有发布开关为 false；未启动任何 Exec 下单进程。

四个旧定义从未启用，账本、时钟和观察数据均为空，先完整备份后移除旧账本与旧 Manager 定义。
新表使用组名重新创建。删除旧定义的接口要求管理员角色；首次普通角色请求被 403 拒绝，
在核对部分安装状态及所有哈希未变后，通过已有管理员会话完成激活，没有重写或重置账户状态。

回执 `verified.json`：RX01 USDT 永续仓位/普通及算法挂单仍为 0、账户绑定0；
81 个受保护进程身份保持，所有安装哈希匹配。Prep 正在历史预热，回放记录持续增加，
因子/仓位错误均为空。此处不将进程运行或历史回放等同于所有因子就绪。

`browser.mjs` 使用临时登录会话和 SSH 本机隧道，真实 Chromium 验证七份配置、组选择、
行数、额度、关闭状态、仅预览不落盘以及桌面/390px 无溢出，JavaScript 错误为 0。
临时会话已删除。浏览器首次检查只改变 hash 而未重新加载文档，修正验证器导航方式后通过。

配置入口：
`http://13.115.227.29:4191/cta/cta-prep-binance-futures/config/?worker=cta-prep-binance-futures#prep02_binance-futures_1min`

远端完整私有备份：
`~/crypto_cta_manager/maintenance-backups/cta_prep02_deploy_20261103T050422Z/`。
`deploy.py` 是本次分阶段操作审计工具，生产变更阶段需要 `--execute`；任务已完成，勿重复安装。
