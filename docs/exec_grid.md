# Exec Grid

最后更新：2026-10-21 UTC

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
三个执行器均已订阅两路流；解码、计算和发布错误均为0。Vol覆盖仍限于发布器配置的
48币；其他独立永续目标使用Grid前仍须有对应就绪Vol。
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
