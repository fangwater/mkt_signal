# Bitget FR 三账户 ADL 修复发布

2026-11-04 10:14–10:45 UTC，依次发布 `jp-meta-elvpn` 上的
`bitget_fr_arb01`、`bitget_fr_arb02`、`bitget_fr_arb03`，基础组件和原有交易信号均已恢复。
Gate 未发布、未重启，原有24个Gate进程的PID、exe、cwd及CPU mask逐项核对保持。

发布源码为干净、同步 `origin/arbmm` 的 `516116fa`，包含 Bitget ADL 提交
`f281e5f4` 和共享原生成交去重回归 `5f52d8f4`。6个release二进制均在本地活跃
工作区构建，经标准publish脚本上传显式临时目录、SHA-256核对及原子安装；
没有在生产机编译。二进制哈希见 [release_manifest.json](release_manifest.json)。

| 环境 | 原有合约持仓数 | 发布前净尾差绝对值合计 | 发布后合计 | 发布后单币最大 | SPOT / MARGIN / USDT-FUTURES 挂单 |
| --- | --- | --- | --- | --- | --- |
| bitget_fr_arb01 | 21 | 48.83 U | 48.85 U | 12.17 U | 0 / 0 / 0 |
| bitget_fr_arb02 | 22 | 41.74 U | 41.87 U | 6.92 U | 0 / 0 / 0 |
| bitget_fr_arb03 | 21 | 52.20 U | 52.34 U | 8.27 U | 0 / 0 / 0 |

以上通过原生签名GET查询现货资产、合约仓位及标记价独立计算：逐币汇总现货量与
有符号合约币量后乘标记价，再求绝对值合计。三套有既有对冲仓，并非空仓；
少量价格变动使尾差金额略变。每套先停信号，再经标准stop封装调用环境本地脚本
停止组件、撤单并核验挂单为空；发布并恢复基础组件、核验原生仓位与新鲜Viz快照后，
再经环境本地 `scripts/start_trade_signal.sh bitget` 恢复信号。

主备交易连接各登录成功，主备账户流各完成订阅，档位缓存22/22且missing=0。
最终所有服务PID相较各自首次恢复后保持稳定，三套信号运行均超过65秒；末次一分钟
分别输出75/74/76条ArbDecision，未出现新的非风控运行错误。信号启动最初几秒的
因子快照等待已经消失。原有2.60杠杆上限仍拦截部分新开仓，没有放宽风控参数。

env.sh、全部已采样配置、账户模式和CPU mask保持；三套 `local_ips` 仍为
`.231/.231`。交易WS与账户流确认使用`.231`，引擎另有既有默认路由`.228`外连。
arb02/03的persist_manager保持CPU15，其余本次恢复进程为`0-5,48-95`。
三个既有dashboard未重启；共享因子与网络配置没有改动。

每套旧二进制与脚本16个文件已备份至各环境的
`run/adl_release_20261104T101356Z/`，附旧文件SHA-256清单。
完整PID、步骤时间及脱敏核验结果见 [verification.json](verification.json)。
原始本地审计在Git忽略目录 `data/ops/bitget_fr_adl_release_20261104/`，
未将凭据或env.sh内容写入发布记录。CPU/IP living docs已同步登记。

代码检查沿用发布前已完成的[回归记录](../okex_adl_patch_20261104/verification.json)：
parsers190、order_common49、trade_engine385、主库1479项通过（3忽略），相关7个
binary检查及格式检查通过。本次另外完成Bitget全套6个binary的本地release构建和实盘运行核验。

本次没有触发实盘ADL测试，不能据此声称已经验证真实ADL成交到现货修复的完整交易。
去重和修复状态仍为进程内状态；实际ADL修复期间重启需先核对在途订单与真实敞口，
历史ADL不自动回放成订单。早期代码验收记录中的`production_deployed=false`保留其历史含义；
本次记录只确认上述三套Bitget已发布，未确认Gate、Bybit、Binance或OKX已发布。
