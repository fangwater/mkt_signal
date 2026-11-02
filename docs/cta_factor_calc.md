# 独立 CTA 因子计算

最后更新：2026-11-02 UTC（源码框架和本地验证，未部署）

`cta_factor_calc` 是公共计算核心，提供两个独立运行入口：

- `cta-prep-binance-futures`：默认读取 `config/cta_prep_binance_futures.toml`。
- `cta-factor-spread`：默认读取 `config/cta_factor_spread.toml`。

`cta-prep-binance-futures` 统筹下列三份现有策略的 Binance 永续因子需求：

| 组合名称 | 频率 | 普通因子条目 | NQ 条目 |
|---|---|---:|---:|
| `rx01_binance-futures_15min` | 15min | 145 | 35 |
| `rx03_binance-futures_1min` | 1min | 45 | 16 |
| `rx04_binance-futures_5min` | 5min | 48 | 15 |

启动自动合并为 304 个计算条目，仅订阅一份 `binance-futures` Kafka 流。
RX01/RX03/RX04 是组合身份，计算代码不根据这些名称分支。输入清单来自各自
现有 `cta_perp_symbol_factors.json`，NQ 依赖从策略规则显式提取。RX01 的
SKL × factor_170 存在两条不同 NQ 条件的交易规则，因子只计算一次；原策略的
146 条规则保留，不能把合并计算误当作合并交易持仓。仓库内模板的
`enabled=false` 不代表线上策略已停；运行关系按用户给定的现状维护。

两个入口分别维护组合清单、进程生命周期和输出流；Spread 同样支持普通单市场
因子、NQ、跨市场 spread。两个入口各自独立回放所需 Kafka；
同一进程内被多份组合引用的因子只算一次，两个独立进程的重叠条目各自维护状态。
不再按 RX 编号或 1m/5m/15m 启动不同的因子程序。计算服务不配置交易账户、
不产生持仓方向、目标数量或订单，不向 Manager 发布仓位。

## 因子与组合

单市场 ID：`市场-内部币对-频率-因子名`，统一小写：

```text
binance-futures-btcusdt-1min-factor_036
okex-futures-btcusdt-5min-nq
binance-futures-ethusdt-15min-td_ti_024
```

跨市场 ID 包含两个有顺序的输入市场：

```text
okex-futures-binance-futures-btcusdt-1min-spread
binance-margin-binance-futures-btcusdt-1min-spread
```

支持 `binance-futures`、`okex-futures`、`binance-margin`；对应 Kafka topic 分别为
`binance-futures`、`okex-swap`、`binance-spot`。频率为 `1min`、`5min`、`15min`。
内部币对使用 `BTCUSDT`，接收时统一 OKX `BTC-USDT-SWAP` 等线格式。

每份组合是一份 JSON，文件名必须与 `name` 一致。例如
`config/cta_factor_spread/basis.json`：

```json
{
  "name": "basis",
  "factors": {
    "binance-futures-btcusdt-1min-factor_036": 1,
    "binance-futures-btcusdt-5min-nq": 1,
    "okex-futures-binance-futures-btcusdt-1min-spread": -1
  }
}
```

启动时扫描目录全部 `.json`，合并**实际非零引用**，不展开 symbol × 周期 × 因子
的笛卡尔积。权重保留为组合元数据，计算过程不将其应用到原始因子或就绪状态。
同一 ID 必须固定同一组参数；跨组合出现不同定义会拒绝新配置，不覆盖另一份。
可在组合的 `parameters` 中按完整 ID 指定完整参数对象：

```json
{
  "normalize_window": 1440,
  "normalize_min_periods": 1000,
  "clip_zscore": 3,
  "rank_window": 2880,
  "rank_min_periods": 1440,
  "lookback": 120,
  "missing_policy": "slots"
}
```

默认普通因子的归一化窗口为 1 天、分位窗口为 2 天，按输入 bar 数换算；
1min/5min/15min 的归一化最少样本分别为 1000/200/67。普通因子复用现有公式
实现、完整盘口基线和 TP_VPI_006 的既有 360/280 样本口径。

NQ 使用原始盘口 mid，120 根极值窗口和 120 个变化值的分位窗口，60 个有效
变化值就绪。输出 `long`、`short` 两个字段，各自带原值和分位。1min 默认保留
缺失槽位，5/15min 默认使用有效样本窗口；`missing_policy` 明确描述这个差异。
`lookback` 只用于 NQ。普通因子的原值无效时，当前值保持未知，不将过去 zscore
当作当前有效因子。

Spread 对齐同一闭合 bar 的两路最后有效 mid，计算 `(第一个市场 - 第二个市场) /
第一个市场`，直接对原值计算两天滚动分位。任一路缺失为未就绪，窗口保留缺失
槽位。因子本身不滞后一根；若策略需要上一根价差，应由信号层明确选择历史值。
普通因子和 spread 的标量字段名均为 `value`。

## 独立输入与数据契约

一个市场/币种只维护一份原始分钟基线，再分发至所需 1/5/15min 状态。同一
市场/币种/周期的普通因子共享基线历史求值器，各因子维护自己的归一化和排名。
NQ、价差共享对齐后的盘口收盘数据。报价仅在已收到快照后有效；多分钟末价要求
最后一分钟内存在有效盘口。所有数量遵循现有 PeriodMessage 的基础资产数量契约，
不将交易所原始张数隐式当作币数量。

依赖成交大小分类的公式需要真实 `amount_thresholds`。配置键是
`市场:内部币对`，值为 `[medium_notional_threshold, large_notional_threshold]`。
缺失时仅这些公式未就绪，其他公式照常计算；不会填造分类阈值或有效零值。
这些主进程参数和 Kafka 配置在启动时读取；修改它们需要重启。组合目录支持热更新。

两个进程使用一份当前 IPC 格式和不同的服务名：

```text
factor_output/cta-prep-binance-futures
factor_output/cta-factor-spread
```

标量观测的事件示例：

```json
{
  "event": "point",
  "generation": "replay-generation-id",
  "point": {
    "factor_id": "okex-futures-binance-futures-btcusdt-1min-spread",
    "ts_ms": 1800000060000,
    "fields": {
      "value": {"raw": 0.001, "value": 0.001, "quantile": 0.6, "ready": true}
    }
  }
}
```

字段统一为 `raw`、`value`、`quantile`、`ready`；未知值使用 null。普通因子的
`value` 是因果标准化结果，NQ/spread 的 `value` 为原值。NQ 的 `fields` 包含
`long`、`short`，各字段结构相同。删除不再被引用的因子发出 `removed` 事件。
回放代次更换会使消费者丢弃旧代次缓存，也允许同一数据时间的新定义替换旧值。
Rust 消费端使用 `cta_factor_calc::client::FactorFeed`，按 ID/字段选择需要的输入；
`fresh()` 检查就绪、未来时间和新鲜度，`poll()` 仅返回当前代次最新观测。
信号层仍须自行配置组合算法、决策周期、过滤阈值及仓位规则。

## 配置、删除与回放

只读检查合并计划，不连接 Kafka、不创建 IPC 或历史文件：

```bash
cta-prep-binance-futures --config config/cta_prep_binance_futures.toml --check
cta-factor-spread --config config/cta_factor_spread.toml --check
```

新增/替换和删除默认只打印计划；`--execute` 只修改指定组合文件：

```bash
cta-prep-binance-futures --put-combination /path/momentum.json
cta-prep-binance-futures --put-combination /path/momentum.json --execute
cta-prep-binance-futures --delete-combination momentum
cta-prep-binance-futures --delete-combination momentum --execute
```

原有 `symbol -> factor -> signed weight` 格式可在导入时补市场与频率：

```bash
cta-prep-binance-futures --import-symbol-factors /path/symbol_factors.json \
  --name rx01_binance-futures_15min --market binance-futures --interval 15min --include-nq
```

检查计划后加 `--execute` 保存。正负权重不变。旧 `_positive`/`_negative` 后缀
映射到实际公式；若两份旧 sleeve 碰到同一个 ID，导入会拒绝，须拆为独立组合，
不会静默相抵或累加。`--include-nq` 显式补齐原来隐式依赖的 NQ。

进程每秒检查组合目录，非法/冲突配置保留原计划。只改权重或删除条目时保留
仍需使用的预热状态；一个因子直到最后一个非零引用消失才停止计算。新增条目
或改变定义时创建独立候选回放，原输出继续服务。候选追上所有所需 topic 的
最新 head 后统一交接；失败保留原运行代次并记录错误。必要 topic 为空不会
被当作回放成功。回放追上与因子窗口就绪分别记录，不伪造历史覆盖。

`--replay-only` 回放一个固定 Kafka head，写历史后退出，完全不创建 IPC。
历史位于配置的 `history_dir/generations/<generation>/`，观测按日期 JSONL 保存，
每代保留自己的初始计算计划。根目录 `state.json` 是当前计划、候选计划、代次、
最新观测和错误的原子快照。删除组合不删除历史，不拼接不同代次来重复计算信号。

环境内运行 `scripts/start_cta_prep_binance_futures.sh --execute` /
`scripts/stop_cta_prep_binance_futures.sh --execute`；Spread 使用对应同名脚本。
脚本默认 dry-run，读取环境自己的 `env.sh`；CPU 可设置 `CTA_PREP_BINANCE_FUTURES_CORE` /
`CTA_FACTOR_SPREAD_CORE`。生产构建、上传校验和安装仍须遵守 arbmm 和本地构建约束。

本次没有部署或修改线上进程、组合、账户绑定、仓位及历史身份。现有信号发布器
尚未切换到这两个独立输出流；上线接入应使用上述新消费接口并核对原信号语义。
