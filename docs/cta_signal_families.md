# CTA 仓位信号：Perp 与 Spread

最后更新：2026-10-23 04:33 UTC（Binance大小单阈值跨周期统一；RX因子清单保持）

2026-10-23，JP 共享 `amount_thresholds` 已换为 2026-09 Tardis
逐笔计算的 P50/P90，供 2026-10 使用：合约和现货各43币。未覆盖的旧值已清理。
04:32 UTC 已删除重复1min金额表及接口，所有周期共用同一市场的阈值。
各周期公共因子计划保持独立，1min仍配置13币。
当前RX01–04所需币种全部覆盖，独立因子表、交易参数及进程保持不变，未恢复
大小单因子。CTA publisher 的金额缓存仍按启动时加载；恢复大小单因子时须
使用新表重新回放。来源、备份、缺项清单及补数工具见
[大小单阈值发布说明](crypto_order_size_thresholds.md)。以下为此前部署记录。

15:00 UTC，JP RX02的virtual02由新账户 `binance_exec_trade08` / `lgq_ltp_81857`
以倍率1跟随，唯一绑定`lgq_cta_symbol_mean`、执行模板`lgq_grid_prep`。
23币、单币50 USDT及原信号阈值保持。账户为STANDARD USD-M多资产模式、单向持仓。
15:06验收5个非零目标在容差内完成，8笔成交全部Maker。JP/SG仓位推送健康，
RLC仍预热且目标clear。页面`follower_source_id`已更新为Exec08。
旧trade05继续停用；以下各时点描述为历史状态。详情及证据见
`docs/cta_perp_rx02_cutover.md`、`artifacts/rx02_exec08_grid_20261022/verification.json`。

2026-10-22 13:43:58 UTC，RX02 已切换新版仓位推送，JP、SG 双 Manager 均接收：
23币、22因子、69组合，`lgq_cta_symbol_mean`，保留 v1 算法、单币50 USDT和现场
entry/exit/NQ参数。当前68/69组合、22/23币就绪；RLC/factor_114仍需积累样本，
RLC目标为clear，未降低窗口或跳过未就绪因子。13:41快照还缺至少122个有效分钟；
实际时间受后续数据连续性影响。
JP virtual02 无执行跟随；SG virtual01 仍由 bybit_exec_trade01 以倍率1跟随。
两边9个旧虚拟策略绑定移除，旧策略及移出币种目标clear。SG账户保留9条份额0的
历史记录，唯一有效绑定为symbol_mean，待投递0。JP已停用trade05残留的10条
账户绑定已备份后移除，修复其导致的发布502；该source继续禁用，旧真实仓位未操作。
旧Perp运行状态归档在 `~/binance-cta-perp-rx02/run/push_switch_20261022/before/`；
旧Special、Exec05继续停机，09:19完整归档保留。trade08无策略绑定，未启动交易。
publisher PID160373固定CPU18，signal PID177222、Config PID168550、Dashboard
PID168607使用housekeeping CPU；保留loopback19186/10196、已有4191入口和默认出口。
本地arbmm release上传、SHA-256核验后原子安装；双Manager目标逐项匹配，发布无错误。
证据：`artifacts/cta_perp_rx02_push_switch_20261022/verification.json`、`sg_verification.json`。


2026-10-22 10:18 UTC，RX01 已切换用户新因子清单：18币、15因子、153组合；
ICX、STORJ 因当前账户不可交易排除（原清单20币/169组合，减少16组合）。
15个因子均不依赖大小单；补齐6个非大小单因子的免阈值名单，缺少成交大小阈值
不再误阻塞。完整1分钟数据测试需2439–2499根（40小时39分–41小时39分），
两天足够；现场保持3天回溯配置，Kafka追平后153/153组合因子、spread和NQ均就绪。
publisher已本地release构建、SHA-256核验、原子安装并通过环境本地脚本交接，
PID330224 → PID3973994，CPU17，进程名 `cta_spread_factor_rx01_20261022`。
signal/config/dashboard进程未重启；全市场5秒Vol继续发布，核验合约524/525、
现货362/363就绪。现场enabled=true、单组合10 USDT、entry/exit阈值均保留。
Manager新增6个策略绑定，继承现场 `lgq_grid_spread`、shares=1、virtual03→Exec06
倍率1；现有绑定和对冲库存保留。702个移除组合的内部方向状态已归零；当前所有策略目标
与Manager核对一致，publication_error为空，待投递0。未改source IP、ENI或路由。
19项Rust及22项Python定向检查通过。证据：
`artifacts/cta_spread_rx01_factor_update_20261022/verification.json`；远端备份及回执：
`~/binance-cta-spread-rx01/run/factor_update_20261022/`。

2026-10-19 13:10 UTC，Exec06执行器补发完成：Chase新目标仅解除借币不足状态，
普通拒单退避保持原行为。当前38个已激活永续中36个4倍，BEL/ZIL按上限3倍，
BAND回读4倍；RX01已恢复原配置推送。最终回执目录为
`~/binance_exec_trade06/run/borrow_stop_20261019T130500Z/`。

2026-10-19 12:55 UTC，RX01 Spread → Exec06 已发布 RapidX 借币不足修复：
下单拒绝码401126/406017使当前目标以 `borrow_unavailable` 结束，保留已成交仓位，
取消同目标其余挂单并等待迟到成交核对，后续新目标才重新评估；配置刷新不会反复释放
相同目标。限流和网络错误继续使用原处理路径。Exec页面显示“借币不足，已放弃本次执行”。
启动后实盘11个目标触发该分支，均无剩余批次、挂单或pending数量，429为0。
RapidX Exec 默认杠杆4倍；已激活36个永续中34个实际回读4倍，BEL/ZIL因场内上限
回读3倍，BAND已为4倍。RX01推送恢复原enabled=true、max_pos_u=10及全部信号参数，
账户/策略快照与订单历史保留。publisher预热与generation继续，signal未重启。
发布回执：`artifacts/rapidx_borrow_stop_20261019/`。

2026-10-19 10:17:05 UTC，RX01 Spread → Exec06 在taker清仓及新快照后恢复实盘发布。
新账户与策略快照时间戳 `1792404818623482`（10:13:38 UTC），永续和负债均归零，
20种低于交易门槛的现货零头合计约2.7593 USDT归inventory；历史订单和旧快照保留。
virtual03继续由LGQ管理并仅跟随Exec06，现有57因子策略+inventory共58个有效绑定，
旧factor_052绑定份额0。57因子策略从零仓起步，当前线上单组合10 USDT，
entry因子0.9/0.1、spread0.3/0.7、NQ0.5/0.5、exit0.3/0.7现场设置保留。
48币/794组合中773个因子就绪，21个缺大小阈值的组合继续留空；publisher及Kafka
generation未重启。57个策略发布均成功，1632个Manager→Redis目标核验一致，
Manager健康且待投递0，已观察到新成交。当前线上仍沿用本机单地址发布；下文多地址
能力仅完成本地实现及验证，未在本次恢复中发布新二进制。
备份、成交清仓回执及新快照：`~/binance-cta-spread-rx01/run/reenable_20261019/`；
本地核验：`artifacts/rx01_reenable_20261019/verification.json`。

## 多个 Manager 推送地址

Perp（RX02/RX03）和 Spread（RX01）共用多地址发布器。每个信号环境的
`env.sh` 可以配置多个 Manager API 基地址，逗号分隔，修改后重启该环境的 signal：

```bash
export CTA_MANAGER_API_URLS='http://127.0.0.1:4191/manager/api,http://manager-sg:4191/manager/api'
```

把 `manager-sg` 替换为可达的 SG 地址。程序向每个基地址追加
`/catalog/position-strategies` 并发送同一策略名称及完整 targets。
`CTA_MANAGER_API_URLS` 设置时优先于原 `CTA_MANAGER_API_URL`；没有设置列表时，
原单地址配置继续有效。列表中的首尾空格、末尾斜线会规范化，重复地址只推一次；
空项、无效地址以及带用户名、密码、query 或 fragment 的地址拒绝启动。
配置文件/配置页的 `enabled` 仍控制是否发布，env 修改不热加载。

默认所有地址使用已有的 `CTA_PUBLISH_TOKEN`。不同 Manager 使用不同 token 时，
在同一个私有 `env.sh` 中配置按 API 基地址匹配的覆盖值，例如：

```bash
# 下列值仅为占位符；实际凭据只保存在主机的私有 env.sh 中。
export CTA_PUBLISH_TOKENS='{"http://127.0.0.1:4191/manager/api":"JP_TOKEN","http://manager-sg:4191/manager/api":"SG_TOKEN"}'
```

覆盖表中未列出的地址使用 `CTA_PUBLISH_TOKEN`（如果已设置）。每个 Manager
仍须预先配置对应策略的发布权限、虚拟账户绑定、份额和真实账户跟随；
增加地址只广播仓位策略，不会复制 Manager 目录、权限或绑定配置。
RX01 Spread 同时包含现货和永续目标，接收账户必须支持相应市场；
只支持 USDT 永续的原生 Bybit Exec 不应直接绑定该现货／永续策略。

每个地址独立发送、确认、去重及重试；某个地址超时不会阻塞信号计算或其他地址。
成功后相同 targets 不重复推送；失败每秒重试，单请求超时 5 秒。等待期间目标变化时，
重试采用最新完整目标，不回放已被取代的开平仓。每个地址只保留一个在途客户端请求，
等待响应或超时后再发送新目标，避免主动并发发送造成目标倒序。关闭发布后停止未发送任务及重试；
已经发出的请求仍可能完成。重新启用或重启 signal 会重新发送当前完整目标。
超时可能发生在 Manager 已处理请求之后，因此网络异常下仍可能重复投递同一完整目标。

状态文件的 `manager_publications` 按地址记录待确认策略、最近成功时间及错误，
`publication_error` 汇总失败地址。成功回执的信号 journal 保留实际发送时的
原始观察数据和 `manager_url`，失败不伪造成功。凭据不写入状态或 journal，
HTTP 重定向不跟随。此能力的本地实现不会自动修改线上环境的推送地址。
本地验证通过 1,306 项 Rust 库测试（3 项忽略，包含 8 项多地址发布测试）及
26 项 Perp/Spread 配置服务回归；这条记录不表示线上已经发布。

2026-10-19 07:26–07:30 UTC，按用户最终范围只修正 Spread：退出long默认0.7、
short默认0.3，严格比较且边界不退出。Rust默认值、配置服务缺省值、本仓示例、
部署副本及页面说明同步；原始ds研究文件保留。RX01 signal与配置服务已通过本地
release构建、SHA-256核验、原子安装及环境本地脚本重载。线上原有0.7/0.3、
单份100 USDT及发布关闭设置均保留，794条内部仓位仍为0；因子publisher未重启，
Kafka generation保持，Exec06及dashboard停机。Perp默认值、线上配置和进程未调整。
页面同时纠正旧价差说明：默认多头使用前一分钟价差分位<0.7、空头>0.3，
并各自通过NQ过滤；0.3–0.7区间允许两方向，最终由各因子及NQ决定。
43项Rust与18项Python测试通过，配置页、健康接口及48币/57组/794组合导入预览通过。
证据：`artifacts/rx01_corr50_warmup/spread_exit_defaults/validation.json`；
远端备份：`~/binance-cta-spread-rx01/run/spread_exit_defaults_20261019T072100Z/`。

2026-10-19 07:10–07:12 UTC，RX01 Spread完成本轮Kafka因子回溯并追至实时，
48币均有历史，762/794个配置组合的因子就绪。21个依赖缺失大小阈值的组合留空，
另11个 `factor_114` 仍未就绪；未降低样本门槛或填造缺失值。07:10快照中693个组合
同时具有就绪的NQ及spread过滤，实时数据缺失会使该数量变化；累计解码、计算、
发布错误均0。794条内部仓位仍为零，发布关闭，Exec06和dashboard停机。
07:12核对线上配置服务，exit已由现场修改为long=0.7、short=0.3，保留该修改；
研究JSON及本地模板中的0.3/0.7是研究原值，不是当前线上值，不可直接覆盖现场设置。
证据：`artifacts/rx01_corr50_warmup/final.json`、`final_config.json`。

2026-10-19 07:03 UTC，按用户要求清除 ICXUSDT、STORJUSDT 的配置，减少79组合，
现为48币/57个有符号因子组/794组合。研究部署JSON的本仓副本、磁盘策略表和
publisher universe同步，Redis通过配置服务的revision/CAS更新；原始ds文件未改。
signal停止后删除这两币已归零的内部状态，再通过环境本地脚本恢复，发布仍关闭，
Exec06、dashboard继续停机。publisher未重启，保留当前generation的Kafka预热；
它的内存保留启动时50币快照，signal只采用新的48币表，磁盘48币universe在下次
publisher启动时生效。当前27币缺少大小阈值，影响21个配置组合，按用户要求留空。
库级回归1298项通过、0失败、3忽略。备份及回执：
`~/binance-cta-spread-rx01/run/corr50_remove_symbols_20261019T065917Z/`；
本地证据：`artifacts/rx01_corr50_warmup/remove_symbols/`。

2026-10-19 06:47 UTC，用户选择“预热因子，重置旧状态，从当前时点起仓”。
旧内部仓位状态（5组/46组合，12条非零flag）已备份，新的57组/873组合全部归零；
本次不重算历史仓位，不向Manager/Exec写入目标，也不改变交易所实际仓位。
publisher及signal完成本地release构建、SHA-256核验发布，经环境本地脚本启动，
Kafka因子预热进行中，`enabled=false`；Exec06和dashboard保持停机。
29币的成交大小阈值缺失，用户要求暂时保持空缺；25个配置组合依赖这些缺失阈值，
对应 `baseline_046` / `net_buy_small` 的分值和分位为空、ready=false，
其余54个底层因子继续计算，不以0伪造成交大小分类。
阈值在publisher启动时读取，补齐后需重启并重新预热；未知因子默认要求阈值。
本轮generation及Kafka水位、心跳、累计解码/计算/发布错误记录在
`data/cta_spread/factor_history/warmup.json`，历史JSONL保留并标记generation。
43项Spread测试通过；备份及发布回执：
`~/binance-cta-spread-rx01/run/corr50_warmup_20261019T064500Z/`；
本地核验：`artifacts/rx01_corr50_warmup/`。

2026-10-19 06:04 UTC，RX01 Spread 按研究文件
`ds/twoex_deploy_corr50.json` 更新并发布，源文件保存在
`config/cta_spread_deployment.json`。共50币、57个有符号因子组、873个组合；
底层因子56个，factor_169的正反两组分别使用 `_positive` / `_negative` 标识，
10个重复symbol × factor配对保留两份独立策略，不相互覆盖。
实盘环境单份金额保留100 USDT，发布开关设为false；signal和publisher二进制
已本地release构建、校验发布，但仍停机。配置服务已重构并启动，支持研究JSON导入
预览、按策略编辑币种及统一的NQ/spread参数；保存仍使用原文件修订校验及Redis CAS。
Exec06和dashboard保持停机，原仓位状态保留；恢复前须对齐中性化后的实际仓位、
内部目标及Manager/Exec绑定，不能仅打开发布开关。证据：
`artifacts/rx01_corr50_deploy/validation.json`；远端备份及回执：
`~/binance-cta-spread-rx01/run/corr50_publish_20261019T055900Z/`。

2026-10-15 本地新增 Perp 静默仓位恢复：显式的
`run/cta_perp_recovery_request.json` 指定本轮因子发布器 generation，信号在历史
因子与仓位逐分钟重算期间禁止发布；所有配置组合追上新采样的 Kafka 水位、
最新完整分钟并就绪后，才切换为最终目标发布。普通重启不自动触发重算。
历史回放按当时首个 post-bar bid/ask 计算固定入场数量，并保留旧策略/币种的
清零目标。有限 Kafka 保留期无法证明保留期之前且期间从未退出的历史仓位。
本次尚未上线：Manager 当前仅接受数字 qty，显式 `clear` 的端到端接入仍待
用户确认是否允许更新共享 Manager；准备状态和验证见
`artifacts/cta_perp_replay_cutover/README.md`。

## Perp 百分位算法与 symbol 开平阈值（本地实现，未部署）

同一 `cta_perp_signal` 通过 `position_algorithm` 选择仓位计算：

- `v0`：每个 symbol × factor 直接用其百分位 `x`，独立判断开平。
- `v1`（prep-v1）：同一 symbol 的全部启用因子分别计算 `2*x-1`，
  再取等权算术平均 `m = sum(2*x_i-1)/N`，用 `m` 统一判断该 symbol 的开平。
  例如两个因子的 `x` 为0.9、0.7，转换后为0.8、0.4，综合判断值为0.6。

两版沿用单份金额（现有配置键 `max_pos_u`），它是每次入场的金额，
不是新增的仓位上限。入场数量仍为 `方向 × 单份金额 / 入场价`，
同向持仓保持原qty，退出全平原qty；不按均值幅度缩放仓位。
v0 每个因子分别持有一份；v1 综合信号每次入场使用一份。

原有sign方向修正继续执行，均值按修正后的因子值计算；signed weight 的
绝对值不改变等权占比，0表示禁用，不计入分母。必须等所有启用因子到齐
同一根闭合分钟并就绪，缺失时保持已有仓位。归一化、rolling分位和NQ不变。

四个开平阈值直接与所选算法的判断值比较，切换时不会自动换算阈值。
`v0` 的阈值范围为 `[0,1]`，`v1` 为 `[-1,1]`；开空阈值必须小于开多阈值，
退多必须小于开多，退空必须大于开空。阈值相等时不触发，NQ仍使用 `[0,1]`
分位，仅过滤开仓。配置未写 `position_algorithm` 时默认为 `v0`。

`symbol_overrides` 只允许覆盖开多、开空、退多、退空四项，两套算法共用一份：

```json
{
  "position_algorithm": "v1",
  "symbol_overrides": {
    "ETHUSDT": {
      "factor_long_quantile": 0.7,
      "factor_short_quantile": 0.1,
      "factor_exit_quantile_long": 0.4,
      "factor_exit_quantile_short": 0.6
    },
    "BTCUSDT": {
      "factor_long_quantile": 0.8
    }
  }
}
```

上例为新增字段示例，需合入完整配置。未填写或为null的字段继承全局值；
未配置的币种完全继承全局值。覆盖应用于该币种的所有因子，不覆盖金额、NQ
或算法选择。更新全局参数时也会校验每个币种合并后的开平阈值，拒绝无效组合。
配置页提供算法下拉框与按币种编辑的四个输入框，空白表示继承。

同一算法下的金额和阈值热加载后由下一根新闭合分钟使用，持仓继续沿用
原数量。v0与v1的持仓归属不同，切换算法要求原持仓已关闭；配置API及Rust
启动/热加载均检查原持仓，避免将独立因子的数量静默转移到综合策略。
已有零目标保留并向Manager发布，继续使用唯一的Manager/Exec契约。

状态和信号快照记录计算时的算法及该币种实际阈值；`quantile` 保留修正后
百分位（v1为其均值），`decision_value` 记录开平判断值。v1以`symbol_mean`
作为仓位策略名，记录所有因子的输入、sign和等权占比。历史模拟按当前配置
重算，实际发布日志保留当时设置。Dashboard与B/S详情展示综合判断值和全部
因子贡献，每币只展示一份综合仓位。

两套算法共用同一份因子预热结果，单独切换算法无需重新拉取或回放行情。
扩展因子/币种清单仍需补齐行情和元数据并验证ready。本次只修改本地代码与测试，
未部署、未变更RX02线上配置或Manager/Exec绑定。

## RX03 Perp 实盘

2026-10-14 08:05 UTC RX03 CTA Perp 已接入独立 `binance_exec_trade07` 实盘，
Manager source `trade07 / lgq_cta_RX03` 跟随 `virtual04 / cta_perp_rx03`，
倍率1，五因子各1份，14币/34组合，每组合100 USDT。启用前账户为空仓、无挂单；
新Exec使用独立namespace、Redis键及RocksDB，没有迁移旧持仓或历史。
执行模板 `binance_cta_perp_rx03_ctaker` 复制RX02当前 `lgq_cta_ctaker`，
五份实际运行参数逐项相同；买卖各最多10个挂单，10秒200单、每分钟400单。
Maker费率-0.00002、Taker 0.000175、理论TWAP 0.0004。
34个实时目标与Exec一致，账户position_ready、发布日志健康，Manager无待发布项。
证据：`artifacts/cta_perp_rx03_activation/`。

信号发布已开启，NQ 120/min60、因子分位2880/min1440、zscore1440/min1000，
开仓0.9/0.1、NQ过滤0.5/0.5、退出0.7/0.3。运行环境保留100 USDT，
仓库profile默认enabled=false，重新部署必须先检查独立Manager/Exec绑定。
页面：`/cta/binance-cta-perp-rx03/`、`/exec_trade07/`，均由4191网关提供。

## RX03 Perp 初始观察部署（历史）

2026-10-14 已部署JP `~/binance-cta-perp-rx03`，14币、5因子、34个组合，
单份100 USDT，`enabled=false`；不连接Manager，不建立实盘Exec绑定。
07:26已完成Kafka预热，四进程正常，34/34组合实时就绪、NQ 14/14就绪。
07:27、07:28、07:29三个连续实时分钟的34个组合全部继续更新，日志仅有observed、真实仓位发布0条。
配置清单在 `config/cta_perp_rx03/`，与RX02独立。

| 因子 | sign | 币种 |
|---|---:|---|
| baseline_164 | +1 | AAVE、ADA、DASH、DOGE、DOT、NEAR、SOL、UNI、XLM、ZEC |
| baseline_200 | -1 | AAVE、BTC、DASH、DOGE、ETH、NEAR、XLM、ZEC |
| baseline_085 | -1 | AAVE、ADA、BTC、DASH、DOGE、ETH、NEAR、ZEC |
| baseline_141 | +1 | AVAX、DASH、DOT、LTC、NEAR、SOL |
| TD_MT_029 | -1 | DASH、ZEC |

其余信号阈值沿用本次现场RX02：开仓0.9/0.1，NQ过滤0.5/0.5，退出0.7/0.3。
RX02现场单份已为300 USDT；RX03按用户明确指定保持100 USDT。
因子归一化1440/min1000，分位2880/min1440，NQ 120/min60。

同主机第二个Perp实例必须配置 `CTA_PERP_MODEL_SCOPE`：publisher和signal
共同使用 `model_output/<scope>/cta_perp-binance-futures-1m-<factor>`。
RX03取 `binance_cta_perp_rx03`，RX02继续其已有未加scope的通道。
`CTA_PERP_FACTOR_PROCESS_NAME` 同时隔离publisher的启停进程名；RX03为
`cta_perp_factor_binance_cta_perp_rx03`。BBO仍订阅共享行情。
Redis使用 `binance-cta-perp-rx03:cta_symbol_factors`，历史/状态均在新目录。

新页面为4191网关 `/cta/binance-cta-perp-rx03/`、`config/`、`signals/`，
分别代理loopback10197/19187。publisher绑定CPU19，其他三进程0-5,48-95。
配置页清单、100 USDT、B/S开多/开空/平仓标记、快照点击及手机布局检查通过，
无JavaScript错误；50个既有进程及55份配置保持。
本次30项Perp Rust和51项Python检查通过；全库1271项通过、3项忽略，
2项原有测试因默认因子清单变更后断言未更新而失败（Spread服务列表、
cta_factor_universe公式期望表），详见 `artifacts/cta_perp_rx03_deployment/checks.json`。
构建、哈希、页面和现场检查证据均保留在该目录。

2026-10-14 04:12:05 UTC 按操作者要求恢复 RX01 Spread 实盘推送。
启用前从不可变信号日志中找齐46组合最近一次100 USDT入场数量，重建完整绝对目标：
3个方向持有组合使用该方向的一份现货/零永续，43个退出组合只保留最近一份现货
及其反向永续。移除旧轮次累加量，不改写历史日志，不清零账户后再开仓。
按准备时市价，五因子目标双腿合计58,851.68 → 8,876.85 USDT；独立旧inventory
保持原目标与份数，不包含在该五因子合计中。
通过环境本地脚本重启signal载入目标，PID46021 → PID176562，CPU0-5不变，
其余44个受保护进程不变。配置API乐观锁开启 `enabled=true`，五因子发布成功，
Manager模板与Exec06全部92个市场数量逐项一致，跟随队列为空，journal健康且无发布错误。
首轮交易所核验可用保证金已为+1,123.01 USDT，仓位仍在向新目标收敛，不能将此
视为所有执行已经完成。证据：`artifacts/cta_spread_rx01_live_enable/`。
状态/配置备份：`~/binance-cta-spread-rx01/run/live_enable_20261014T041023Z/`。

2026-10-14 03:32:25 UTC 已通过 RX01 环境本地配置接口将 Spread 的
`enabled` 设为 `false`，进程已确认热加载。此操作只暂停新目标发布，未重启
Exec、撤单、改杠杆或改写已有目标与账户仓位。配置和信号状态备份在
`~/binance-cta-spread-rx01/run/reentry_guard_20261014T033225Z/`。

诊断确认旧 Spread `cash_qty += qty` 在对冲退出后每次重新入场继续累积
两腿库存，违背当前操作者要求的绝对目标模型。Manager 份数与跟随倍率均为1，
目录目标与 Exec 目标逐项一致。当前46组合、每份100 USDT 的新策略基准双腿
上限为9200 USDT（市值随价格变化），另有启用时显式承接的独立旧 inventory，
不可混为本次100 USDT配置生成的新仓。

已修正为每次新入场替换完整的现货/永续目标，禁止把历史对冲腿叠加到
新目标；33项 Spread 测试通过，包含100轮同向重入、退出与重启回归。
格式与 `cargo check --bin cta_spread_signal` 通过。完整库测试1265通过、3忽略、
3失败：未改动的因子配置测试仍固定期望旧symbol数量、旧因子列表或旧公式映射，
与当前仓库配置不符；本次未改动这些配置和测试。
04:01 UTC 按操作者要求完成信号推送程序重新部署：本地同步的 `arbmm`
`6ff0248e` 加本次两文件补丁，release 构建、SHA-256 校验后原子安装，
仅通过环境本地脚本重启 signal，PID3132535 → PID46021，CPU0-5不变。
二进制 SHA-256：`4069055ff3a20582f727d8cecdc6a14af7d619af7ca12ed625bad765add330b4`。
46/46组合实时就绪、journal健康、publication_error为空；其余44个受保护进程、
15个配置/环境文件哈希和Exec策略目录均保持不变。发布仍为 `enabled=false`。
04:01部署时未重建累计状态，后于04:12启用前完成上述五因子目标重建；独立旧库存保留。
该次重启JSON往返的12个数量字段仅发生1 ULP舍入，最大2.85e-14，
没有业务目标调整或新的实盘发布。
部署证据：`artifacts/cta_spread_rx01_absolute_target_deploy/`；远端旧程序和状态备份：
`~/binance-cta-spread-rx01/run/absolute_target_20261014T035311Z/`。

2026-10-13 13:42 UTC **RX01 Spread 已启用实盘发布并接入 Exec06**。操作者确认沿用
现货开方向、退出时期货对冲；开仓至退出期间允许方向敞口。
现场原单份金额实际为100 USDT，本次保持100；下方200及未启用状态为旧记录。

- RX01/RX02 均按符号修正后的因子分位：q>0.90做多、q<0.70退多，
  q<0.10做空、q>0.30退空；严格不等式，原有过滤保留。RX01价差q30/q70，
  RX02 NQ q50/q50过滤启用。
- RX01五因子16币46组合，发布在线且46/46就绪、journal健康、publication_error为空。
- 13:44 UTC首周期实盘核验：TD_TI_024 的 DOT 现货空仓已执行，净数量约
  -83.47015 DOT（约100 USDT），余差约0.0028 USDT低于最小下单要求；
  execution_complete=true，活动订单与待下单量为零。五个因子的Manager发布均有成功日志。
- `binance_exec_trade06` 跟随 `virtual03/cta_spread_rx01` 倍率1；五个
  `binance_cta_spread_rx01_` 因子策略与独立 `inventory` 策略各1份，ctaker执行。
  7币12腿旧库存按启用前账户净现货和期货实际数量继承，并写入账户/库存策略NAV基准。
  启用前分配全部核对一致，待下单与活动订单量为零。
- lgq 对source06有configure权限，对virtual03与六个策略有管理权限；
  `/exec_trade06/`、`/exec_trade06/config/` 已以lgq身份验证。
- Manager费用参数对齐RX02：maker=-0.00002，taker=0.000175，理论TWAP=0.0004；
  这是管理端配置口径，未变更交易所费率。
- 本地arbmm release发布，SHA-256校验和原子安装；`dfb70364` 修复双市场Exec
  风控字段加载，`c484f85d` 按Exec市场查询现货快照，`480bc990` 补RapidX资产
  快照完成标记。Manager另修复非USDT现货规则的误拒绝，现货规则480/永续744。
  风控18项、快照runner6项、trade_engine357项、Manager295项测试通过（3项忽略）。
- 验收及部署证据：`artifacts/cta_spread_rx01_activation/`。远端证据在
  `~/binance_exec_trade06/run/activation_20261013/`；该目录含环境备份，禁止整体导出。

2026-10-13 09:20 UTC 更新 JP `binance-cta-spread-rx01` 最新清单，配置提交
`480387a3`。独立 Redis、磁盘 signal 清单、publisher universe 同步为16币、
5因子、46个 symbol × factor 组合。`+/-` 映射为有符号权重 `+1/-1`。
不新增币种，移出 BTC、ETH、TRX；保留33个旧组合，移出24个旧组合，新增13个。

| Spread 因子 | sign | 币种数 |
|---|---:|---:|
| `baseline_141` | +1 | 15 |
| `baseline_164` | +1 | 15 |
| `TD_TI_027` | -1 | 8 |
| `TD_TI_024` | -1 | 5 |
| `factor_052` | +1 | 3 |

publisher 经环境本地脚本重启到 PID663465/CPU17；09:40 UTC Kafka 双市场
历史已追至实时，46/46组合因子与价差就绪。
原二进制不变。signal/config/dashboard 保持 PID3199322/3183433/3390010，
实时热加载配置，发布保持 `enabled=false`，单份200 USDT、价差q30/q70不变。
没有启用RX01交易，没有变更RX02 Perp的清单、virtual组合或Exec。
环境备份为 `run/scope_before_20261013T091935Z`；release.json单独记录config_commit。
页面 `/cta/binance-cta-spread-rx01/`，配置页为其 `config/`，B/S页为其 `signals/`。
当前验收记录见 `artifacts/cta_spread_rx01_latest/`。

最终执行账户明确为 **RX01**。09:39 UTC已预部署专用 `binance_exec_trade06`
运行包（本地arbmm release，逐文件SHA-256校验后安装）；远端凭据核对属于
旧RX01且不同于RX02。全部Exec进程停止，尚未注册Manager source或绑定virtual。
计划使用独立 `cta_spread_rx01` virtual、五个因子策略和一份库存承接策略；
库存需以正式切换时的实际现货净资产与合约仓位重新读取，不能用本次快照
或旧策略状态直接填入新因子。旧账户仍有已对冲库存，不能当作空仓启动Exec。
本次完成的是信号观察上线与执行包预部署，尚未启用实盘仓位发布。

首次部署历史：06:28 UTC安装19币/57组合的旧清单，二进制`456be044`、
脚本/页面`470af607`，当时尚在预热。该清单已由上述最新配置替代。
原验收记录见 `artifacts/cta_spread_rx01_deployment/validation.json`。

币种映射使用 USDT 符号；负 sign 按 `1-q` 修正因子分位。价差过滤保持 q30/q70，
单份金额保持现有 200 USDT，Manager 份数未变更。
不能直接把旧 RX01 逐 lot 状态或 RX02 的状态文件复制成 Spread 状态。

## Spread 信号观察页

2026-10-13 06:49 UTC 已部署页面提交 `ff3a8082`，仅重启 Spread Dashboard
至 PID 3390010（CPU 0–5）；因子预热继续，RX02/Exec 不变，发布保持关闭。
验收 57 个组合 API 全部可读、均有现货 K 线，行情无错误，价差历史就绪
57/57、因子 0/57、实时 0/57（该时刻仍在首次 Kafka 回放中）。线上浏览器
验证桌面/手机、factor 符号及币种切换通过，详见
`artifacts/cta_spread_signals/{validation,browser_validation}.json`。

后续线上 B/S 验证：历史回放推进到 10-12 23:33 UTC，42/57 个组合因子
已就绪。已实际点击 ETH × baseline_164 的 10-12 22:03 现货 B 标记，
并检查 22:14 合约 S 对冲退出；图表均有行情价格，浏览器无错误。
这仍是历史信号回放，不是成交，实时就绪当时为 0/57，发布持续关闭。
证据：`artifacts/cta_spread_signals/live_bs_validation.json`。

`/cta/binance-cta-spread-rx01/signals/` 按 factor 独立切换其已配置币种，
展示现货 1m K 线、修正因子分位、价差分位及方向变化 B/S 点。
绿色/红色三角是现货开仓，金色菱形是合约对冲退出；允许反转时同一分钟
包含先对冲、再反向开仓两个事件。边界和反转规则与 Spread Rust 方向规则一致。

后台只读导入 `data/cta_spread/factor_history/*.jsonl` 至独立
`data/cta_spread/signal_history.sqlite3`，持续保存观测，不写目标、配置、Redis
或发布器状态。发布关闭时仍可观察。它按当前配置从最早可用历史空仓开始
因果回放方向意图，不校验历史 BBO、报价新鲜度或计算成交数量、费用及 PnL；
B/S 不能作为事实发布/成交证据，事实执行继续查看 Manager。
回放状态从显示窗口之前承接，负 sign 使用 `1-q`；未就绪因子保持状态，
价差未就绪阻止新开仓但不阻止退出，不用未来数据补齐历史就绪状态。

信号时间为分钟结束边界，标在下一分钟现货 K 线上，不能倒移到上一根。
K 线由独立只读后台直连 Binance 公共 Spot Klines 补缺，缺失时不编造价格，
对应事件仍列在事件表。图表支持 1/6/24 小时；Kafka 追赶期间窗口跟随已有
历史末端，明确显示数据时间/滞后、历史就绪和实时就绪，心跳不代表可交易。
本地浏览器测试覆盖标记点击、反转双动作、负号、独立清单、移动端、反向代理
路径和行情缺失；合成截图仅用于测试，不作为实盘信号证据。

## RX02 Perp 新清单与 NQ 短窗口

2026-10-13 的 RX02 曾在08:57 UTC正式启用，24组合通过virtual02/lgq_cta发布到
trade05，倍率1；09:14 UTC旧special因子发布器已退役。实际组合/投递核验见
[RX02切换说明](cta_perp_rx02_cutover.md)。下方为预热和切换前的历史记录。

2026-10-13 08:25 UTC 新 Perp 已完成 Kafka 预热，24/24 组合实时就绪、
9/9 NQ 就绪，仍为 enabled=false。首次预热触及旧 1800 秒上限，因此将
上限改为 3600 秒并在独立 CPU18 重启 publisher；新 PID 4133794。
发布器日志确认 `percentile=2880/1440 nq=120/120/60`、解析/计算错误为 0。
桌面/手机已验证真实历史的 B 开多、S 开空、平仓标记及交易快照点击；这些是
历史模拟，不是成交。新信号日志只有 observed，没有 published/partial；
旧 RX02 仍负责实时仓位。验收见 `artifacts/cta_perp_rx02_preview/`。

2026-10-13 07:52 UTC 新 RX02 四个进程均已启动，仓位发布保持关闭。
配置服务 19186 / Dashboard 10196 与旧 RX02 端口独立，网关路径
`/cta/binance-cta-perp-rx02/`、`config/`、`signals/` 已接通，旧页面仍保留。
新观察信号不承接旧实际仓位，仅记录模拟方向；最终切换时先停止观察进程，
备份模拟状态，再复制已停旧进程的完整最终状态。07:56 UTC 浏览器验证 9 币/
24 组合、100 USDT、桌面/手机、五个因子回放加载通过，无 JS 错误；Kafka
仍在追赶历史，不能以页面健康替代实时因子就绪。证据位于
`artifacts/cta_perp_rx02_preview/`。

2026-10-13 07:35 UTC 已将本地 release 二进制 `976d557e` 与切换脚本
`f72ab0e4` 校验后安装到 JP `~/binance-cta-perp-rx02`。只启动新 Kafka 因子
发布器 PID 3841796（CPU 0-5）；新配置保持 enabled=false、单份 100 USDT，
新仓位发布器及两个页面服务均未启动。独立 Redis 清单已初始化。07:36 UTC
仍在历史预热，尚未切换旧 RX02、未发布新仓位目标。Nginx 候选配置测试通过但
没有安装。用户执行的切换命令与状态承接详见 [RX02 切换说明](cta_perp_rx02_cutover.md)。

2026-10-13 更新 CTA Perp 配置与计算；尚未切换线上旧 RX02。
NQ 的价格极值窗口和分位窗口均为 **120 个分钟槽位**，至少 **60 个有效
NQ 样本**后就绪。缺失分钟占槽位、不过滤掉时间间隔，不计入有效样本，
第 121 分钟开始淘汰最早样本。BUY_NQ/SELL_NQ 改为相对最近 2h 最低/最高
mid 的变化率。分位仍包含当前值、采用中间排名；开仓阈值与退出规则不变。
因子分位仍为 2880/min1440，归一化仍为 1440/min1000，Spread 不受影响。

| Perp 因子 | sign | 币种 |
|---|---:|---|
| `baseline_141` | +1 | AAVE、DASH、ETH、NEAR、SOL、XLM、XRP、ZEC |
| `baseline_200` | -1 | AAVE、DASH、DOGE、SOL、XLM、ZEC |
| `TD_TI_024` | -1 | AAVE、DASH、DOGE、NEAR、XLM、ZEC |
| `factor_036` | -1 | NEAR |
| `baseline_164` | +1 | AAVE、DASH、ZEC |

RX02 只读现场状态与旧仓库清单一致：16 币/32 个绑定 → 9 币/24 个绑定，
没有新增币种，移除 AVAX、BNB、DOT、FIL、LINK、LTC、UNI。
只有 NEAR × factor_036(-1) 原样保留；其余移除 31 个绑定、新增 23 个绑定。
新清单和发布器 universe 同步维护在 `config/cta_perp_symbol_factors.json`
和 `config/cta_perp_factor_model_1m_pub.toml`，已写入新环境独立 Redis，旧环境清单未改。

零目标与删除策略含义不同：保留原策略并发布 qty=0 时，Exec 使用原策略的
账本仓位执行目标调仓；删除策略或发现无法归属的账户余量时，剩余仓位可进入
`SYSTEM_POSITION_CLOSE`。同币种已同时加载的新旧目标可通过内部交叉减少
实际下单差额；先清零、后发布新目标不保证避免先平后开。线上旧 RX02 仍在
发布且旧绑定有持仓，不能把直接覆盖清单描述为无交易影响的配置更新。

## 统一命名

| 项目 | CTA Perp（原 special / RX02） | CTA Spread（新 RX01） |
|---|---|---|
| 建议环境名 | `binance-cta-perp-rx02` | `binance-cta-spread-rx01` |
| 因子发布器 | `cta_perp_factor_model_1m_pub` | `cta_spread_factor_model_1m_pub` |
| 仓位发布器 | `cta_perp_signal` | `cta_spread_signal` |
| 策略配置 | `config/cta_perp.json` | `config/cta_spread.json` |
| 配置服务 | `scripts/cta_perp_config_server.py` | `scripts/cta_spread_config_server.py` |
| 仓位页 | `web/cta_perp/` | `web/cta_spread/` |
| 配置页 | `web/cta_perp_config/` | `web/cta_spread_config/` |
| 模型服务前缀 | `model_output/cta_perp-binance-futures-1m-` | `model_output/cta_spread-binance-futures-1m-` |
| 独立 Rust 模块 | `src/cta_perp/` | `src/cta_spread/` |

两套各自维护策略状态、因子发布、参数校验和前端，不用同一策略开关切换含义。
通用 Manager/Exec 的 targets 接口不变：每个 factor 是一份仓位策略，每个
symbol × factor 独立持仓，负权重取 `1-q`，权重绝对值不改变金额。
每个组合开仓使用一份 `max_pos_u`（默认 200 USDT），持仓期间冻结数量。
Manager 的绑定/虚拟账户配置份数，Exec 执行聚合后的实际目标；信号端不维护
第二套份数或订单参数，也不直接下单。

`{env}:cta_symbol_factors` 的 Redis 键后缀保持一致，环境名隔离两套绑定。
两套 publisher 分别以各自 `config/cta_{perp,spread}_factor_model_1m_pub.toml`
的 universe 决定币种和因子，不读取共享 `factor_plan_1m` 清单。
TLen 的通用 `amount_thresholds` 仅提供回放所需
的成交大小分类阈值，不扩大或借用另一套策略清单。Spread允许保留缺失阈值的币种，
仅屏蔽依赖大小分类的因子（当前为 `baseline_046` / `net_buy_small`）；
缺失的分类字段为NaN，未知因子默认也要求阈值，不伪造可交易值。
Perp 的 NQ 开仓过滤、原退出规则和历史回放保留。
旧普通 CTA 因子发布器、CTA/CtaSpecial 专用执行分支、规则服务及页面入口已移除。
通用 Intra 不再接受旧 CTA 环境，旧 CTA 开仓消息也会被拒绝；停止脚本仍可识别
旧环境，便于关闭历史部署。当前仅维护 Perp 与 Spread 两套仓位发布链路。

旧 RX01 的 trade_signal、pre_trade、trade_engine 已停；账户监控和持久化保留。
用户已执行合约对冲，最近只读核对仅剩小于交易最小量/最小名义的净敞口。
旧 RX01 的配置页和 Viz 已下线，账户监控、持久化及历史目录保留。
新 Spread 没有复制旧 lot 仓位，也没有配置 Manager 发布连接或账户绑定。
未来启用前仍须核对现有挂单、持仓承接、因子绑定及 Manager 份数；当前发布保持关闭。

## Spread 5s vol 因子

`cta_spread_factor_model_1m_pub` 同时维护独立的 `vol` 因子。它复用现有 Kafka
`binance-futures` / `binance-spot` 消费及历史预热，只计算同一配置
`universe.symbols` 指定的币对。现货和永续分别保存价格、窗口及就绪状态；
不依赖成交金额分类阈值、分钟因子的就绪状态或两市场的价差配对。

以成交价形成闭合 5 秒 bar 的最后价格 `C_t`：

```text
r_t   = C_t / C_(t-24) - 1
vol_t = sample_std(r_(t-719), ..., r_t)  # ddof=1，不乘 1.3，不年化
```

24 根对应 2 分钟收益率，720 个收益率构成最近 1 小时窗口。必须具有完整窗口，
首次就绪需要 744 根收盘价（约 62 分钟），由 Kafka 历史预热填充。
同一币对已覆盖但无成交的 bar 沿用上一收盘价，尚无有效成交价时不生成 bar。
按该币对 Kafka `PeriodMessage.ts` 的数据时间封口，不按处理时的墙钟补到现在；
其他币对或另一市场的消息不会推进它的时间。重复、倒序 period 被忽略，
period 内成交按时间排序。历史追赶期间只更新状态，不发布 IPC；取得发布权后
先发布各市场/币对的最新快照，再继续按闭合 bar 发布。

| 市场 | IPC 服务 |
|---|---|
| Binance 永续 | `model_output/cta_spread-binance-futures-5s-vol` |
| Binance 现货 | `model_output/cta_spread-binance-margin-5s-vol` |

沿用 `ModelMsg`：`score` 为原始波动率，`score_ready` 表示完整窗口就绪，
未就绪时 `score=NaN`。`ts_in_ms=t` 表示闭合区间 `[t-5s,t)` 的右端点；
`ts_out_ms` 为实际发布时刻。消费者必须检查就绪状态和数据时间的新鲜度。
该因子不做 zscore 或分位归一化，不加入原有 `universe.factors` 的分钟入场列表。
`warmup.json` 和运行统计包含独立的 `vol_published` / `vol_publish_failed` 计数。
旧 MM 的 `factor_pub/<venue>/rl_vol` 继续使用原有 12/30/1.3 口径。
本次仅完成本地实现，未部署。Exec 已增加同市场 vol 订阅和 Grid 下单接入，详见 [Exec Grid](exec_grid.md)。

## Spread 规则

每个币种对齐现货和永续同一个闭合 1 分钟的最后盘口 mid：

`spread = (spot_mid - perpetual_mid) / spot_mid`

2880 个分钟槽位，至少 1440 个有效配对，当前值包含在窗口内。缺失分钟占槽位、
不补旧价格，分位使用相同值的中间排名。第t分钟因子使用第t-1分钟的spread分位，
前一分钟缺失时不入场。NQ使用当前闭合分钟的永续mid，沿用Perp的计算口径：
极值窗口和分位窗口均120分钟，至少60个有效样本。因子分位保留原因果归一化算法。

以下三个条件须同时成立才允许开仓，阈值来自本次研究JSON：

| 方向 | 修正sign后的因子分位 | 当前分钟NQ分位 | 前一分钟spread分位 |
|---|---|---|---|
| 买现货 | q > .90 | long ≥ .50 | rank < .70 |
| 卖空现货 | q < .10 | short ≤ .50 | rank > .30 |

spread的 .30–.70 区间对两个方向都可通过；最终方向由因子信号和相应NQ共同决定。
任一过滤未就绪不入场。使用当前有效且不超过 5 秒的现货 BBO 换算数量。
即使现货流缺失，因子仍可用于退出：实时缺少配对超过 5 秒时发布带
`filter_ready=false` 的因子 bar，不以旧价差替代。因子本身未就绪时保持状态。
现货卖空要求 RapidX Binance 的 MARGIN 模式；不要把这套双向策略配置为 SPOT。

Spread默认退出多头在修正后的因子分位q < .70时触发，退出空头在q > .30时触发，
NQ与价差均不触发或阻止退出。
退出时保留现货目标，设置 `perpetual_qty = -cash_qty`，方向数量归零。
因此方向退出后仍可能持有期现仓位，不能把它的 PnL 全部记为已实现。
实际成交、两市场独立 FIFO 和 PnL 使用 Manager；Spread 页面展示信号目标，
不套用 Perp 单市场的模拟回测。

两套 CTA 使用同一仓位发布请求：`strategy_name` 和 `targets`，
每个目标包含绝对 `qty` 和与 Perp 相同的 `signal=0`。
Spread 发布裸币种现货和 USDT 永续两个目标；Perp 发布合约目标。
信号服务不携带订单模板覆盖，不指定 maker/taker，也不创建执行模板。
份数和执行策略由 Manager 配置，Exec 按分配后的目标执行。
目标数量换算与 Perp 一致：开多参考当前 ask，开空参考当前 bid；
参考价只用于将单份 USDT 换算成数量，不是委托价格或执行方式指令。

两套 config server 独立维护，配置读取、保存、版本冲突校验和 symbol × factor
编辑流程一致。共用相同的发布开关、单份 USDT 和因子开仓/退出参数结构，
Perp 使用 NQ 开仓过滤参数，Spread 同时使用NQ和前一分钟mid价差分位过滤参数。
执行配置入口统一位于 Manager。

反向开仓信号同时通过NQ和价差过滤时，直接反转，不等待下一根 bar：
用新方向的一份数量构造完整目标：现货为该数量，永续为0，替换上一轮两腿。
一次向 Manager 发布最终两个绝对目标。没有中间清仓请求。
反向信号未通过新现货方向的NQ/价差过滤，或现货 BBO 无效，则只对冲旧方向，不开反向仓位。
持仓期间同向信号不重算数量；退出后新入场同样替换两腿目标，不累积旧库存。
重复发布始终是幂等的完整绝对目标。

例如现货 +2 / 永续 0，反向做空数量为 -3 且NQ/价差过滤都通过：
发布现货 -3 / 永续 0，净方向为 -3。再退出空方向时发布现货 -3 / 永续 +3，
净方向归零。没有通过开空过滤时，只发布现货 +2 / 永续 -2。
执行尚未完成时再次更新，Exec 以最新目标减实际已执行数量计算差额。

每个组合持久化方向、方向数量、现货数量、合约数量和入场金额。
发布前先原子替换并 fsync 状态；重启校验数量关系，禁止把旧单市场状态默认为零。
删除绑定会退出其方向并保留已对冲的两腿目标，不能通过删除配置遗忘现货。
真正清掉所有期现库存须作为单独的 Manager/Exec 操作。
关闭发布时保留目标状态，只更新观察数据，不生成新的模拟持仓供以后执行。

## 本地检查和准备

```bash
cargo check --bin cta_perp_signal --bin cta_spread_signal \
  --bin cta_perp_factor_model_1m_pub --bin cta_spread_factor_model_1m_pub
cargo test --lib
python3 -m unittest scripts.tests.test_cta_perp_servers \
  scripts.tests.test_cta_perp_position_replay scripts.tests.test_cta_perp_signal_history \
  scripts.tests.test_cta_spread_servers
scripts/deploy_cta_spread.sh --env-name binance-cta-spread-rx01
```

最后一条默认 dry-run。`--execute` 只安装信号套件，不启动、不创建 Exec companion，
不覆盖已有 env.sh、参数或状态。首次安装的信号配置关闭发布；初始化 Redis
symbol/factor 表后再启动观察。生产构建必须在本地干净且同步的 arbmm 上完成。
远端只能接收校验过的本地 release 产物；信号套件发布与标准 Exec 发布分别处理。
Perp 与 Spread 的环境本地 start/stop 脚本只管理 publisher、signal、config、dashboard。

线上改名不能只替换二进制：须一并迁移配置/状态文件名、Redis 环境前缀、IPC
订阅和启动脚本。已有 Manager 策略名称、历史来源和实际持仓归属不得自动重命名。
