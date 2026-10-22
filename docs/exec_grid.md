# Exec Grid

最后更新：2026-10-21 UTC

2026-10-21 08:14–08:15 UTC 修复 jp-meta RX01 仓位状态停更：现有
`exec_pre_trade_state` 通道容量从 64 KiB 扩为 1 MiB，历史保留 2 条，订阅缓存
上限 8 条，沿用同一紧凑编码及 HTTP/WebSocket 格式。RX01 约 140 KB 的完整状态
此前被拒绝发布；Exec06/Viz 配套替换后连续三次得到 1,506–1,509 行新鲜快照，
覆盖 59 个策略（含系统残差）、现货与合约，延迟 330–513 ms。四条现货策略仍报告
借币不足（RLC、IOST、两条 KNC），属于独立执行约束。

发布基于同步的 `arbmm` `115213a6` 加本次通道补丁，本机构建后逐文件 SHA-256
核验并原子安装。Exec06/Viz PID 为 502412/502880，均保持 CPU0–5；另 134 个
受保护进程身份和 118 份配置哈希不变。交易引擎、账户监控、信号发布器和 Manager
保持运行。通过 1,342 个 Exec 库测试（3 忽略）、2 个 codec 测试、6 个 Viz 测试、
fmt/check/release；新增 6,600 行混合市场全字段往返回归。状态超限报错消失，
HTTP/WS 验证通过。现场清单及验收在 Manager 的
`backups/grid_split_20261021/`，本地在 Manager 的同名 artifacts 目录。

同次操作将原 `lgq_grid` 拆成 `lgq_grid_spread`（RX01）及 `lgq_grid_prep`
（RX02/RX03，按用户指定拼写），所有模板仍归 `lgq`。spread 保留原参数；prep
合约复制现货参数：5 档、120 秒、1 轮 Maker 后转 Taker，现货参数不变。共用
单手量 100 USDT、Vol 倍数 1、signal 开关保持。74 条绑定的份数和跟随倍率不变，
148 个市场配置回读通过；旧模板在解除全部引用后删除。此后本文较早的“未绑定”
和“CTaker”陈述仅描述各次历史部署时点。

`algorithm = "grid"` 使用现有 BatchExec 生命周期，复用 MM 的 vol 网格偏移公式。
仅允许 RapidX Binance/OKX 或已支持的原生统一账户（Binance PM、OKX、Bybit UTA、Bitget UTA）。
Binance 普通账户及其他 backend/market 组合拒绝加载。Manager 发布、Exec Config 写入和
Exec Redis 热加载均校验账户范围。原生统一账户保持该 Exec 配置的合约市场；本次不新增原生现货路由。
RapidX 线性 Exec 沿用现有现货/合约路由：裸币种如 `BTC` 为现货，`BTCUSDT` 为永续。

## 参数与执行

现货和合约共用 `single_order_usdt`（单手名义金额）。`grid.spot` 与 `grid.futures`
使用相同的参数集合，各自独立维护 `vol_multiplier`（Vol 倍数，默认 1）、
`orders_per_batch`（拆单数，默认 3）、`maker_timeout_ms`（每轮挂单时长，默认 1000ms）
和 `max_maker_requotes`（首次之后允许的 Maker 重挂轮数）。现货默认 2（共 3 轮），
合约默认 `null`（无限轮）。两边都可以设置有限轮或无限轮，0 表示只挂首次一轮；
有限值必须小于 uint32 最大值。Vol 倍数必须为有限正数。
Grid 复用原有拆单、合并和 margin 路径；时效阈值仍为算法内部规则。
它只根据目标市场选择参数，不依赖 spread 配对；纯合约策略也可直接使用 Grid。
单手量固定，不会因目标较大和 `max_batch` 自动放大。`max_batch`、tick 阶梯和 maker anchor
不参与 Grid 定价；网格始终以买一/卖一为对应侧基准。

两边 maker 超时都先撤单，等全部订单的撤单/成交终态确认后重挂；有限 maker 轮数
用尽后，当前批次扣除部分成交的剩余量合并为一笔 taker。Manager 展示的“Maker 挂单轮数”
包含首次挂单，对应本市场 `max_maker_requotes + 1`（字段为 2 即最多 3 轮）。
选择无限轮时按本市场挂单时长持续撤挂，不因轮数转 taker。
现有显式 `signal=±1` 主动 taker 语义保留；Manager 关闭 signal 执行时写入 signal=0。

部分成交扣除、原档位剩余量重挂、低于最小数量/名义的尾单合并、聚合残量及目标切换
均沿用 BatchExec。本市场执行参数变更先撤销旧单并等待终态，期间继续核算晚到成交和在途预留。
现货 margin 的借还款、账户风险检查和订单提交继续使用现有统一账户/RapidX 路径。

```json
{
  "algorithm": "grid",
  "single_order_usdt": 100.0,
  "orders_per_batch": 3,
  "max_batch": 20,
  "maker_price_anchor": "own_best",
  "tick_spacing": 1,
  "batch_interval_ms": 500,
  "maker_timeout_ms": 1000,
  "max_maker_requotes": 2,
  "target_tolerance_usdt": 10.0,
  "grid": {
    "spot": {"orders_per_batch": 3, "maker_timeout_ms": 1000, "vol_multiplier": 1.0, "max_maker_requotes": 2},
    "futures": {"orders_per_batch": 5, "maker_timeout_ms": 3000, "vol_multiplier": 1.0, "max_maker_requotes": null}
  },
  "targets": {"BTC": {"qty": 0.01, "signal": 0}, "BTCUSDT": {"qty": -0.01, "signal": 0}}
}
```

这是参数格式示例，不会自动创建目标或写入账户。按原生市场发布时只保留该市场的目标。
币对覆盖配置使用 `algorithm`、完整的 `grid.spot` / `grid.futures` 设置及共用单手量。
Grid 的拆单数与挂单时长仅从对应市场的嵌套字段读取，顶层同名 Batch 字段不控制 Grid。

## Vol 与价格

CTA Spread 的 Kafka factor publisher 对两路行情中的全部 symbol 分别维护现货、合约 5s close，
不再用 Spread 的 `universe.symbols` 过滤执行 Vol，也不维护额外的执行币种白名单。
新出现的 symbol 自动建立窗口；现货与合约、USDT 与 USDC 合约分别保留自己的行情。
计算 `r[t] = close[t] / close[t-24] - 1`，对最近 720 个收益率计算样本标准差，不乘 1.3。
24 bar 收益率跨度为 2 分钟，720 个收益率窗口为 1 小时；冷启动需要 744 个收盘价。
Kafka 回放用于补齐预热，只有关闭的 5s bar 进入窗口。旧 MM/trade-flow vol 不变。
两路 Vol 的发布历史和订阅队列均为4096条，以容纳同一根bar的全市场突发消息；
发布器与Exec必须协调替换这一组队列容量，沿用原来的服务名和 ModelMsg 格式。
普通模型因子的队列容量保持原值。

每侧偏移从 `0` 到 `vol * vol_multiplier`，按该市场的 `orders_per_batch` 均匀拆档；
单档偏移为 0。默认倍数 1 保持原定价；例如现货 0.5、合约 2，则各自外档为 0.5 倍、
2 倍本市场原始 vol。仍不开放内档偏移和有效期配置。价格偏移超出 `[0, 1)` 时拒绝该批报价。
买单价格为 `bid * (1-offset)` 向下对齐 tick；卖单为 `ask * (1+offset)` 向上对齐 tick。
该偏移函数由 MM 与 Exec 共用。

Exec 仅消费对应市场/币对的 `model_output/cta_spread-<venue>-5s-vol` 原始因子。
目前已实现发布的市场是 `binance-futures` 与 `binance-margin`；其他允许的账户市场
仍需各自的 vol producer，缺失时会等待，不使用 Binance 因子替代。
未就绪、异常、过期 vol 或过期盘口均阻止新单/重挂，并撤销工作单；确认终态后保留剩余量，
数据恢复再继续。内部有效期为 Vol 15 秒、盘口 1 秒。因数据过期撤单不消耗 maker 重挂次数。

## 配置入口

Manager 项目 `/home/fanghaizhou/crypto_cta_manager` 提供 Grid 表单、只读展示、目录保存、
币对覆盖和 Redis 发布。当前 schema 新增 `cta_order_strategies.grid` JSONB 并允许 `grid` 算法。
Manager 已支持目录归属、分市场参数及显式数据库维护。已有初稿 Grid JSON 的数据库
使用 `scripts/apply_grid_market_parameters.sql` 将原共用值映射为两套独立参数；
已有分市场 JSON 也通过该显式维护补齐倍数 1 和合约无限轮，保留原参数与已有设置。
Manager、Exec 与 Exec Config 需要同时替换为当前单一格式，旧 Exec 不接受新增字段。
2026-10-21 已作为 `20261021T053158ZGridControls` 发布到 jp-meta，Exec 源提交
`bf9d0ad8`，Manager 源提交 `d5c3cc1`。替换 trade05/06/07 的执行器与 Config、
RX01 共享因子 publisher，以及 Manager API/前端；没有切换现有策略算法或绑定。
全部 87 个实账户绑定、77 个虚拟账户绑定及四张仓位快照表保持不变。
现有 `lgq_grid` 模板仍未绑定，操作者保存的现货一轮参数保留；新模板默认现货三轮。

执行器 PID 分别为 163684、165922、166169，CPU mask 均为 0–5；共享因子 PID166240
仍使用 CPU17，Manager PID166260 仍使用 CPU24。另152个常驻进程身份和54份配置哈希
核对一致。通过 Nginx 验证七套 Exec Viz/snapshot/Config 均返回200，临时未绑定模板的
两组倍数与有限/无限轮数保存回读成功，随后删除测试模板和会话。

05:53 UTC 完成原配置三天 lookback 的 Kafka 回放，恢复 Spread 和 Vol 输出。
本次同时升级原始 Vol producer 导致共享 Spread 输出中断约20分钟；倍数与轮数参数
自身不需要这轮预热。现货与合约 Vol 均实测48/48币就绪并持续收取新消息，
三个执行器均已订阅两路流；解码、计算和发布错误均为0。当时Vol覆盖限于发布器配置的
48币；下述全symbol发布已经解除该名单限制。
最终进程、配置、绑定及快照复核通过，验收结果保存在同release的
`DEPLOYMENT-VERIFICATION.json`。验收后清理部署暂存、探针和本次备份。
部署清单保存在 Manager `web-releases/20261021T053158ZGridControls/RELEASE-MANIFEST.json`。

## 全 symbol 与后台交接

`--wait-for-publishers` 用于同一信号逻辑的运行实例交接：新实例先回放并追平，
此时不写现有 factor history 或 warmup 状态；取得共享输出的发布权后才接管后续写入。
既有历史保留，运行中的发布器可持续提供 Spread/Vol。日志同时报告已发现的symbol数、
15秒内就绪数和最新Spread时间，确认追至实时后才释放原发布者。

环境本地启动/停止脚本支持 `CTA_SPREAD_FACTOR_PROCESS_NAME` 保存当前发布器名称。
后台预热可用 `PMDAEMON_NAME` 指定新实例名，并开启 `CTA_SPREAD_FACTOR_WAIT_FOR_PUBLISHERS=1`。交接停止旧实例时显式设置
`PMDAEMON_NAME` 和 `CTA_SPREAD_FACTOR_KEEP_PID`，避免兜底PID扫描停止已经预热的新实例。
普通停止仍会停止该环境内全部遗留发布器；交接后需保存当前进程名并恢复正式CPU绑定。
后台实例的持久启动参数仍使用正式CPU，预热时可临时调整运行中线程的CPU亲和性。
仅计算范围变化的本次更新沿用已有Spread历史；这个模式不会重算替换既有历史档案。

现场只读Kafka抽样为524个合约、362个现货symbol。上线前验证覆盖自动发现新币种、
独立市场时钟、1536币突发及迟加入订阅者完整接收、后台历史保护和进程交接脚本。

## 2026-10-21 全 symbol 上线验收

07:20 UTC 完成交接，release `20261021T064822ZGridAllSymbols`，Exec `d3b724af`、
Manager 前端 `82f58cb`。06:54 UTC 启动后台回放，原实例持续提供 Spread/Vol 并写入
既有历史；新实例追至实时后接管，最终仅保留 `cta_spread_factor_binance_futures`。
publisher PID330224，全部线程恢复 CPU17、nice0；Exec05/06/07 PID分别为
384442/384533/384621，仍为CPU0–5。先停止三个旧订阅者，再释放原发布者，完成两路
Vol的4096容量交接。整个交接序列51秒，包含首个执行器的supervisor退出等待；停止原
publisher返回后4秒内确认接管。每2秒的连续采样中，warmup状态最大年龄1920ms，
Vol计数推进最大观测间隔8002ms，无读取错误。

当前行情流合约524/524、现货362/362币均有15秒内的就绪Vol。探针另收到历史
EPICUSDT现货值，因已过期不计入当前覆盖。RX02的51个合约目标、RX01的54个合约和
55个现货目标、RX03的14个合约目标全部通过逐币检查，三个Exec均订阅两路流。
解码、计算、发布错误均为0。现有70个策略配置仍为CTaker，未切换算法或更改绑定。

Manager仅原子替换前端，PID166260/CPU24及全部Config/Viz/trade_engine保持运行。
156个受保护进程、53份配置哈希、12个模板、87个实账户绑定、77个虚拟账户绑定及
四张仓位快照表核对不变。唯一配置改动是在factor环境env.sh中保存
`CTA_SPREAD_FACTOR_PROCESS_NAME=cta_spread_factor_binance_futures`，全部IP字段保持。
Grid布局通过1440/1024/768/390/320宽度检查，有限/无限轮切换无横向溢出或区块跳动。

验证包括1341个库测试（3忽略）、3个probe测试、4个脚本测试、release构建和前端构建。
临时会话、部署暂存、备份、探针、截图和浏览器profile已清理。Manager同release下
保存 `RELEASE-MANIFEST.json`、`DEPLOYMENT-VERIFICATION.json`、`UI-VERIFICATION.json`。
