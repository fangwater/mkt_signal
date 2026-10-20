# Exec Grid（本地实现，未部署）

最后更新：2026-10-20 UTC

`algorithm = "grid"` 使用现有 BatchExec 生命周期，复用 MM 的 vol 网格偏移公式。
仅允许 RapidX Binance/OKX 或已支持的原生统一账户（Binance PM、OKX、Bybit UTA、Bitget UTA）。
Binance 普通账户及其他 backend/market 组合拒绝加载。Manager 发布、Exec Config 写入和
Exec Redis 热加载均校验账户范围。原生统一账户保持该 Exec 配置的合约市场；本次不新增原生现货路由。
RapidX 线性 Exec 沿用现有现货/合约路由：裸币种如 `BTC` 为现货，`BTCUSDT` 为永续。

## 参数与执行

现货和合约共用 `single_order_usdt`（单手名义金额）。`grid.spot` 与 `grid.futures`
分别维护自己的 `orders_per_batch`（拆单数）和 `maker_timeout_ms`（每轮挂单时长）；
只有 `grid.spot.max_maker_requotes` 控制现货重挂次数。合约没有次数配置。
Grid 复用原有拆单、合并和 margin 路径；Vol 网格计算和时效阈值为算法内部规则，
不提供额外的用户配置项。
单手量固定，不会因目标较大和 `max_batch` 自动放大。`max_batch`、tick 阶梯和 maker anchor
不参与 Grid 定价；网格始终以买一/卖一为对应侧基准。

现货 maker 超时先撤单，等全部订单的撤单/成交终态确认后重挂；maker 轮数用尽后，
当前批次剩余量合并为一笔 taker。Manager 展示的“Maker 挂单次数”包含首次挂单，
对应 `grid.spot.max_maker_requotes + 1`（例如字段为 2，则挂单最多 3 轮）。
合约不受该次数限制，按合约独立配置的挂单时长持续撤挂；不会因为次数转 taker。
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
    "spot": {"orders_per_batch": 3, "maker_timeout_ms": 1000, "max_maker_requotes": 2},
    "futures": {"orders_per_batch": 5, "maker_timeout_ms": 3000}
  },
  "targets": {"BTC": {"qty": 0.01, "signal": 0}, "BTCUSDT": {"qty": -0.01, "signal": 0}}
}
```

这是参数格式示例，不会自动创建目标或写入账户。按原生市场发布时只保留该市场的目标。
币对覆盖配置可使用 `algorithm`、`grid` 和已有单手量/时长等参数。

## Vol 与价格

CTA Spread 的 Kafka factor publisher 对配置币对分别维护现货、合约 5s close，
计算 `r[t] = close[t] / close[t-24] - 1`，对最近 720 个收益率计算样本标准差，不乘 1.3。
24 bar 收益率跨度为 2 分钟，720 个收益率窗口为 1 小时；冷启动需要 744 个收盘价。
Kafka 回放用于补齐预热，只有关闭的 5s bar 进入窗口。旧 MM/trade-flow vol 不变。

每侧偏移从 `0` 到 `vol`，按该市场的 `orders_per_batch` 均匀拆档；单档取内档偏移。
这保持原默认网格计算口径，不开放 Vol 倍数、内档偏移或有效期配置。
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
使用 `scripts/apply_grid_market_parameters.sql` 将原共用值映射为两套独立参数。
Exec 代码仅提交推送，尚未部署；Manager 模板保存不等于实盘启用。
