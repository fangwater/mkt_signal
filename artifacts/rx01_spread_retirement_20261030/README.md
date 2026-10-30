# RX01 Spread 退役与 15min 框架准备

2026-10-30 UTC，目标为 `jp-meta-elvpn`。用户确认移除线上环境、信号进程和页面入口，
保留通用 Spread 源码；新 RX01 仅准备框架，等待新的币种、因子和逐组 NQ 清单。

- 旧 `~/binance-cta-spread-rx01` 的 signal 已关闭；通过环境本地 stop wrapper 停止并删除剩余三个服务的 supervisor 登记。
- 原目录整体归档至 `/home/ubuntu/retired_data/rx01_spread_20261030T033029Z/binance-cta-spread-rx01`。364 个文件同盘重命名，逐文件 inode、大小及 mtime 不变；其中历史 data 共 40 个文件、10444961622 字节。私密配置留在远端0700归档目录，未复制到本仓库。
- 旧 Spread 和 `binance-cta-rx01` 别名入口返回410，10186/19174已关闭。同步修改 Nginx active site 和 `~/nginx_locations.txt`；nginx -t 后 reload。Manager 静态页面删除旧配置链接，不重启其服务。其他 RX 页面保持HTTP200。
- 旧因子进程承担共享执行 Vol，先拆出 `~/exec_volatility_binance/exec_volatility_pub`（CPU20，PID2865016）。从 Kafka 预热、追实时，在旧发布器仍运行时抽查BTC/ETH两市场同一根bar，4/4数值在探针8位小数显示精度内一致（绝对差<=5.01e-9）。释放后接管原两条 ModelMsg IPC服务，算法和4096条队列容量保持。两市场522/360个币对新鲜就绪，解码及发布错误均0。
- 新 `~/binance-cta-perp-rx01` 已安装15min二进制、脚本、页面及空配置，43个文件SHA-256核验通过。`enabled=false`、币种/因子/规则均空、无Manager绑定、无进程。CPU17及10195/19185仅预留，未开放网页入口。
- 80个受保护交易/行情进程PID及启动时间保持，Manager PID589839保持。未发送订单、改变仓位或修改网络布局。

源码：mkt_signal `3472f612`（共享Vol与15m规则/框架）、`0a9152ab`（退役网关）；
Manager前端 `c23cc62`。Rust全部库测试1392通过、3忽略，相关Python42项及Node页面测试通过；
release均在本地arbmm构建后上传、逐文件SHA-256校验并原子安装，未在生产机运行Cargo。

`retirement_result.json` 为停止/归档回执；`final_verification.json` 为端口、新框架及新Vol运行核验；
`web_verification.json` 为HTTP验收；`old_candidate_comparison.json` 为接管前数值抽查。
三个Python文件为本次一次性操作及核验的审计副本，固定目标且包含状态断言，不是通用部署入口。
`checksums.json`、`rx01_checksums.json`、`frontend_checksums.json` 保留发布文件哈希。
