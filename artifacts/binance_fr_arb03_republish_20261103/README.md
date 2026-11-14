# Binance FR 03 重发与限额核对

目标：`jp-meta-elvpn:/home/ubuntu/binance_fr_arb03`，Binance Portfolio Margin FR，
全部现有在线币种。按用户要求重新发布并恢复原先运行的交易进程，没有手工平仓、还款
或调整风险参数。

## 发布

- 本地生产分支 `arbmm` 与 `origin/arbmm` 同步，提交
  `6ee63160771cebecf807efd263d92ff465f31b22`。
- `scripts/build-jp-meta-binaries.sh --exchange binance` 本地 release 构建成功，
  6 个二进制 SHA-256 见 `built_binary_sha256.json`。既有未提交改动保留，来源记录在
  `source_state.json` / `source_diff.patch`；本次未修改 Rust 源码，没有部署 51042 退避修复。
- 经环境本地脚本停止、核验挂单为空后，使用
  `scripts/publish-jp-meta-fr.sh --host jp-meta-elvpn --env-name binance_fr_arb03 --exchange binance --skip-build`
  上传到临时路径、核对 SHA-256 并原子安装 16 个文件。生产主机没有运行 Cargo。
- 启动基础进程并等账户/限额初始快照就绪后，通过环境本地
  `scripts/start_trade_signal.sh binance` 恢复原先运行的信号进程。
- `env.sh`、`trade_engine.toml` 和 `config/viz.toml` 哈希保持；`local_ips` 仍为
  `172.31.35.231` 两次。旧运行文件备份在远端
  `run/republish_20261103T143605Z/`，不包含另行复制的凭据文件。
- 启动确认 `limits=783 online_symbols=70 missing_online=0`，私有 symbolConfig、
  positionRisk 和 leverageBracket 全部读取成功。persist_manager PID 468517 / CPU15；
  trade_engine 469039、pre_trade 471322、account_monitor 472267、trade_signal 476386、
  Viz 466178、Config 466005 均为 CPU `0-5,48-95`。原 dashboard PID 61245 保留。

## 私有账户 symbol 限额

14:34 UTC 只读查询，金额单位 USDT；限额来自账户 `/papi/v1/um/symbolConfig`，
不能用公开 leverageBracket 的数百万上限替代。

| symbol | 合约名义持仓 | 账户硬限额 | 含现有缓冲的本地开仓上限 |
| --- | ---: | ---: | ---: |
| AGLDUSDT | 51,081.69 | 50,000 | 48,050 |
| AWEUSDT | 60,284.25 | 60,000 | 57,900 |
| USUALUSDT | 50,043.22 | 50,000 | 48,050 |

上述三项确实超限，影响后续加仓，不代表同等金额的未对冲净敞口。当前持仓可能随市价
变化。原 51042 发生于 `/papi/v1/margin/order`，AEVO 等尚有 symbol 余量的币种也收到
相同拒绝；具体业务触发条件未证实，不能把所有拒单归结为这三个超限项。
逐币证据见 `account_symbol_limits.jsonl` / `position_limit_comparison.json`。

## 波动率依赖

重发后新信号程序因 `factor_pub/binance-futures/rl_vol` 缺失而抑制 ArbOpen/ArbClose。
这段时间没有 51042，不能作为拒单已解决的证据。

只读核对发现共享 `~/trade_flow_feature/binance-futures` 的旧进程于
2026-10-31 23:01:45 UTC 后因 BTCUSDT 金额阈值缺失 panic 退出；当前金额阈值已由其他
操作补回 51 个币种，包含 BTCUSDT。该表控制完整 trade-flow 特征的币种范围，RL 波动率
在阈值筛选前独立处理全部 venue symbol，因此不能将其覆盖范围说成只有 51 币。

2026-11-03 14:56:10 UTC 通过该环境本地启动脚本恢复旧的已部署数据发布程序，
PID 497809 / CPU `0-5,48-95`。未更改其二进制、env.sh、配置或共享阈值。
波动率窗口需重新积累；14:57:31 的消费者日志从 missing snapshot 转为 ready=false，
生产端持续发布且 rl_fail=0，见 `factor_warmup.json`。

14:59:56 风险快照已出现 ArbOpen=30，确认信号恢复。15:00:38 再核对，恢复后的
14:59:40–15:00:07 日志仍有 970 条 pre_trade 的 51042 拒绝记录，涉及31个币种，
包括 AEVOUSDT、FLOWUSDT、TRBUSDT 等；这31个币种不包含上表三个超限项。
故重发及恢复波动率没有解决51042，也不能把这批拒绝直接解释成三个币种的持仓限额。
engine同一错误有两处记录，不能把1940条engine日志当成1940个独立订单。
证据见 `after_factor_warmup.json` / `reproduced_51042.json`。

最终验收 `final_runtime.json`：15:01:50 UTC，FR 8个进程及恢复的共享数据进程均在；
FR配置与共享因子二进制/配置哈希均保持。最近60秒波动率抑制及missing snapshot均为0，
ArbOpen/ArbClose均重新出现，净敞口绝对值合计约172.67 USDT。
同窗口engine仍有2000条51042日志（同一拒绝有两条记录），所以交易拒单问题仍未解决。

## Bitget 缓存问题

既有方案为 `scripts/lib/bitget_tier_pool.py`、`bitget_position_tier_sidecar.py`
及环境注册/sidecar 启动脚本。当前 pool 注册表
`bitget_position_tier_pool:envs` 和缓存 `bitget_position_tier_cache:USDT-FUTURES`
两个整键均不存在，且无活 sidecar。10 月 18 日三套 Bitget FR 启动均成功加载 34/34，
missing=0；刷新失败保留已成功加载的内存状态。因此是共享生产链路中断，不能认定全部
运行中内存限额清空。本次没有修改 Bitget 配置、启动缓存服务或重启交易进程。
原核对证据在 `../fr_full_audit_20261103/bitget_cache_followup.json`。

`verify_runtime.py` 为只读验收脚本，输出进程/配置哈希、风险摘要和安全日志计数，
不输出凭据或带签名的交易请求。
