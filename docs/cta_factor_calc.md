# CTA 因子计算与仓位表

最后更新：2026-11-02 UTC（源码框架和本地验证，未部署）

## 按频段维护仓位表

因子计算继续共享输入与预热状态，仓位和 Manager 归属按频段独立：

| 仓位策略名 | 频率 | 单份 USDT 配置 |
|---|---|---|
| `cta-prep-bn-1min-pos` | 1min | `rx03_binance-futures_1min.json.signal.max_pos_u` |
| `cta-prep-bn-5min-pos` | 5min | `rx04_binance-futures_5min.json.signal.max_pos_u` |
| `cta-prep-bn-15min-pos` | 15min | `rx01_binance-futures_15min.json.signal.max_pos_u` |
| `cta-spread-okx-bn-1min-pos` | 1min | `cta_factor_spread/example.json.signal.max_pos_u` |

现有组合文件名保留为配置身份；Manager 策略名由市场、策略类别和频段确定，
不再由 RX 环境前缀拼接因子。一个市场/频段只允许一份带 `signal` 参数的
Prep 组合。两个计算进程各持有自己的单实例运行锁；只计算因子的组合仍可共享引用。

每个 symbol × factor / NQ 规则仍独立开平仓、冻结入场 qty、持久化和观察；
现有 v0/v1 算法选择保持。发布时将本频段所有子仓位按币种相加，向 Manager
一次提交该策略的完整绝对目标表。不同频段分别发布，不在信号层互相抵消。
Spread 的方向腿和对冲腿按各自 venue 分别求和，保留已退出规则的两腿库存。
删除最后一个 Prep 规则仍显式 clear；从 Spread 整体移除币种仍对两腿发布 taker clear。
某因子退出只撤销自己的贡献，不会清掉同币种其他因子的目标。

USDT 额度仍按当前单份口径使用：v0 每条规则一份，v1 每个标的一份，
Spread 每个 symbol × factor 一份；不是整个频段共享一个总金额上限。
修改一个频段的额度只影响该频段之后的新开仓。Manager 使用四个策略身份
分别配置跟随份数、观察成交、PnL 和资金费；内部子仓位明细继续留在信号状态和日志。

仓位表由因子进程内的 `cta_factor_calc::positions` 模块维护，不再启动独立
`cta-signal` / `cta_*_signal` 进程。Prep 进程持有三个频段的表，Spread 进程
持有 OKX–Binance 1min 的表。仓位直接读取当前有效计算代次的结果，无需通过
IPC 订阅自己的因子流；原因子 IPC 仍可供其他只读消费者使用。

每张表在 `<history_dir>/positions/<仓位表名>.json` 持久化完整组合、子仓位、
最后决策时间及退役状态；状态先原子写入并 fsync，再向 Manager 提交完整目标。
`<history_dir>/state.json.position_tables` 包含当前仓位表、目标及投递状态，
`positions_error` 单独报告仓位故障。统一配置页同时展示所选组合的仓位表。
备份会包含这些持久化表；子仓位变更记录写入同目录的 `.events.jsonl`，Manager
成功投递记录写入 `.deliveries.jsonl`。仓位决策和目标提交每秒检查一次。

首次历史预热以及候选回放都不改仓；新代次接管时更新决策边界，只处理之后的
新 bar。控制参数即时按统一组合读取，新增或改定义的因子在当前有效代次尚不
包含相同定义时暂停该表决策与发布。暂停发布保留子仓位，不生成模拟仓位。
移除 `signal` 或删除组合时以原表名发布清零目标，持久化后持续重试，直到
所有 Manager 确认后才完成退役；重启恢复尚未完成的清零任务。
Spread 因子和当前 NQ 对齐，入场使用前一分钟价差；前一分钟价差来自同一个
计算代次保存的前值。依赖缺失最多等待 5 秒，之后仍允许因子退出。

切换部署须先停旧独立信号进程，核对并明确迁移旧因子策略的账本、目标和
Manager 绑定，再启用新表；不会自动导入旧目录、叠加发布或重写历史归属。
本轮仅修改本地源码，没有部署、改线上额度或更改账户绑定。

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
RX01/RX03/RX04 是组合身份，计算代码不根据这些名称分支。初始清单从各自
`cta_perp_symbol_factors.json` 和策略规则导入；现在以统一四列组合 JSON 为
唯一配置源，NQ 依赖由规则推导。RX01 的
SKL × factor_170 存在两条不同 NQ 条件的交易规则，因子只计算一次；原策略的
146 条规则保留，不能把合并计算误当作合并交易持仓。仓库内模板的
`enabled=false` 不代表线上策略已停；运行关系按用户给定的现状维护。

两个入口分别维护组合清单、进程生命周期和输出流；Spread 同样支持普通单市场
因子、NQ、跨市场 spread。两个入口各自独立回放所需 Kafka；
同一进程内被多份组合引用的因子只算一次，两个独立进程的重叠条目各自维护状态。
不再按 RX 编号或 1m/5m/15m 启动不同的因子程序。因子进程兼任仓位表维护和
Manager 目标发布；交易账户绑定、份数与订单执行仍由下游 Manager/Exec 负责。

## 统一配置入口和 JSON

统一导航入口为 `/delist/`，固定提供 CTA Prep 和 CTA Spread 两个配置链接。
链接通过 `?worker=cta-prep-binance-futures` / `?worker=cta-factor-spread` 选择清单；
配置页也提供“返回 Delist”。Spread 的便捷入口 `/cta/cta-factor-spread/config/`
跳转到同一编辑器的 Spread 清单，保留各自独立的计算与回放状态。

新的页面路径为 `/cta/cta-prep-binance-futures/config/`，由独立
`cta-factor-config` 提供（默认仅监听 `127.0.0.1:19220`）。这是源码中准备的
入口，尚未部署。页面统一维护三套组合的因子行、NQ 条件、开平仓阈值、额度、
信号启用状态和聚合方式。配置服务器只写组合 JSON，不直接发布持仓或订单。
旧策略的可编辑 Config 路由在新网关模板中全部跳转此入口；历史信号面板保持
原身份。新配置不再依赖各策略的 `cta_perp.json`、磁盘 symbol-factor 表或
`<env>:cta_symbol_factors` Redis 键。

每份组合一份 JSON，文件名与 `name` 相同。沿用最新的四列因子行，仅在组合层
补充 `venue`、`interval`。例如：

```json
{
  "name": "rx01_binance-futures_15min",
  "venue": "binance-futures",
  "interval": "15min",
  "factor_rules": [
    {"标的": "SKLUSDT", "nq": "关闭", "符号": -1, "因子": "factor_170"},
    {"标的": "SKLUSDT", "nq": "0.50", "符号": -1, "因子": "factor_170"}
  ],
  "signal": {
    "enabled": false,
    "max_pos_u": 200,
    "position_algorithm": "v0",
    "nq_mode": "strict_quantile",
    "symbol_overrides": {},
    "entry": {
      "factor_long_quantile": 0.9,
      "factor_short_quantile": 0.1,
      "nq_change_enabled": true,
      "nq_long_quantile": 0.5,
      "nq_short_quantile": 0.5
    },
    "execution": {
      "factor_exit_quantile_long": 0.3,
      "factor_exit_quantile_short": 0.7
    }
  }
}
```

单 venue 可用 `binance-futures`、`okex-futures`、`binance-margin`；内部标的
使用 `BTCUSDT` 或 `BTC`。频率为 `1min`、`5min`、`15min`。程序自动生成
`binance-futures-btcusdt-1min-factor_036` 这样的计算 ID；NQ 不为“关闭”时，
自动加入相同市场、币种、频率的 NQ 依赖，不再手工维护第二份因子字典。
同一计算可被不同规则和组合引用。完全相同的因子/符号/NQ 行会被拒绝；同因子
的不同 NQ 规则保留各自的策略持仓。零权重行不创建计算。

跨市场 spread 行使用 `因子=spread` 和 `reference_venue`。同一组合需要另一
市场或频率时，可在具体行显式覆盖 `venue`、`interval`；普通单 venue 行仍只有
原四列。`config/cta_factor_spread/example.json` 提供普通因子与价差混合示例。
`cta-prep-binance-futures` 只接受 Binance 永续输入；OKX 和混合市场计算由独立
`cta-factor-spread` 承担，可用同一个配置服务程序的 `--spread --config ...`
入口维护。也可在主配置服务增加 `--spread-config /path/cta_factor_spread.toml`，
在同一页面切换两个独立计算进程；环境脚本使用 `CTA_SPREAD_FACTOR_CONFIG`。
两份目录、回放状态和并发保存摘要保持独立。Prep 仓位表仅支持 Binance 永续；
Spread 仓位表当前支持 OKX–Binance 永续 1min、v0，并要求显式填写
`signal.spread.long_quantile` 与 `short_quantile`。普通因子行的 `venue` 指向
`binance-futures`，组合的 `venue` / `reference_venue` 分别为 OKX / Binance；
程序自动补齐每个标的的 spread 依赖，无需手工重复添加计算行。

`signal` 可省略，表示只维护因子计算。
已有组合移除 `signal` 或删除组合时，信号撤销完整目标，等待所有 Manager
确认后将该表标记为已退役；计算进程和其他频段继续运行。
保留 `signal` 而设置 `enabled=false` 仍是暂停发布。
`v0` 按因子规则独立持仓，`v1` 对每个
标的的有符号因子值等权取均值。`v1` 要求所有行使用共同 NQ 条件。`nq_mode`
显式选择严格分位比较或线性中位数的包含边界比较；RX01、RX03 使用前者，
RX04 保留后者。`max_pos_u` 的含义分别为每条因子规则、每个标的的单份 USDT
额度。现有非零持仓仍会阻止信号在运行中切换持仓算法；市场/频率需创建新组合。

`parameters` 仍可按完整计算 ID 固定窗口定义。默认普通因子归一化窗口为 1 天、
分位窗口为 2 天，最少样本随频率换算；同 ID 的定义冲突会拒绝保存。NQ 使用
120 根极值窗口和 120 个变化值的排名窗口，60 个有效变化值就绪。1min 保留
缺失槽位，5/15min 使用有效样本窗口。普通因子的无效当前值保持未知。
Spread 同一闭合 bar 的原值为 `(第一个市场 - 第二个市场) / 第一个市场`，
任一路输入缺失即未就绪；因子自身没有额外信号滞后。

页面保存、预览和删除均校验整份合并清单。保存带当前清单的内容摘要，CLI 与
Web 共用跨进程文件锁和原子替换；并发修改返回 409，保留页面未保存的草稿。
删除组合保留其他组合引用和历史记录；切换后的信号读取到文件删除后，以原
策略身份发布零目标，不创建新身份清仓。

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

两个计算进程使用一份当前 IPC 格式和不同的服务名：

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
      "value": {"raw": 0.001, "value": 0.001, "quantile": 0.6, "median": 0.0008, "ready": true}
    }
  }
}
```

字段统一为 `raw`、`value`、`quantile`、`median`、`ready`；未知值使用 null。普通因子的
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
映射到实际公式，导入结果也是四列行格式；若两份旧 sleeve 碰到同一个 ID，导入
会拒绝，须明确拆分。`--include-nq` 将导入行的 NQ 设为 0.50。

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

本次未部署或修改线上进程、账户绑定和仓位。原独立仓位信号入口已从
`Cargo.toml` 的二进制清单中移除，旧停止脚本保留用于切换前关闭历史进程。
统一 `start_cta_signal.sh` 拒绝再启动独立信号；启停因子进程即同时管理其仓位表。
模板 `enabled=false` 不能覆盖线上实际启用状态，发布前应核对运行快照和完整账本。

本地只读检查：

```bash
cta-factor-config --config config/cta_prep_binance_futures.toml --check
cta-prep-binance-futures --config config/cta_prep_binance_futures.toml --check
cta-factor-spread --config config/cta_factor_spread.toml --check
```

配置环境内 `scripts/start_cta_factor_config.sh --execute` 启动统一编辑服务，默认
监听 loopback。因子环境内通过对应 `start_cta_prep_binance_futures.sh` /
`start_cta_factor_spread.sh` 管理运行，脚本默认 dry-run；`env.sh` 配置
`CTA_MANAGER_API_URLS` / `CTA_MANAGER_API_URL` 和发布凭据，脚本仅传递所需变量。

Nginx 模板由 `scripts/render_cta_factor_config_nginx.py --source-ids <完整账户ID范围>`
生成；安装时替换旧 Config locations，不能与旧编辑路由同时保留。认证沿用
Manager 会话，要求完整账户范围的 configure 权限；同时部署 Manager 的
`X-CTA-Access: configure` 校验。旧单账户查看权限不能获得跨组合写权限。
