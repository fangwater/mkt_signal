# RX02 替换准备检查

后续需求已调整：当前不直接配置或替换 RX02。`prep-v1` 指新增仓位计算算法，与现有算法 `v0` 在同一程序内选择；不要求单独部署实例。下文保留前期清单和行情审计，生成的配置文件均为未应用的草稿。当前算法设计范围见 `algorithm_design.md`。

本次只读检查生产环境，未部署、未重启、未修改 Redis / Manager / 实盘信号。

## 清单与配置

- 目标环境：JP `binance-cta-perp-rx02`，Binance USD-M 永续。
- 用户清单：48 币、17 因子、195 个 symbol × factor 组合；全部因子通过现有配置 helper 的名称和权重校验。
- 现场清单：9 币、5 因子、24 组合；保留 12、退出 12、新增 183。
- 现场每组合 300 USDT，发布 enabled=true，24 个当前组合就绪；检查时信号账本全部为零，这不能代替交易账户仓位核验。
- `requested_factors.json` 保留原始清单；`cta_perp_symbol_factors.json` 转换为生产配置格式；publisher TOML 扩展 universe；`cta_perp.json` 是关闭发布的预热草稿，沿用现场金额和分位参数。
- 以上均为草稿，未应用。195 组合沿用每组合 300 USDT，对应同时持有一份时名义金额合计 58,500 USDT；旧24组合为7,200 USDT。该乘积不是账户实际敞口或总风险上限。

## 预热路径

publisher 每次启动新建独立 Kafka 消费组，从 topic 最早保留记录回放，不提交消费位点；内存中的原始因子、归一化、分位与 NQ 状态重新计算。`factor_history/*.jsonl` 是输出历史，启动时不从中恢复滚动计算状态。

- normalize：1440 分钟窗口，至少1000有效样本。
- 因子分位：2880 分钟窗口，至少1440有效归一化样本；这与前一层顺序累积。
- NQ：120分钟窗口，至少60有效样本。
- 配置 lookback_secs=259200（72小时），catchup_timeout_secs=3600。
- 实际读取从 Kafka earliest 开始；lookback 只控制进入输出历史和归一化/分位窗口的时间范围，不会生成缺失数据，也不保证上游保留72小时。
- 预热需以所有配置组合 factor/NQ ready、追上当前闭合分钟且连续更新为准；不能将进程已启动或完成一轮消费视为全部ready。

## 现场前置缺口

1. 48币中22个有有效的通用 amount_thresholds，26个缺失，见 `live_audit.json` 的 missing_or_invalid_threshold_symbols。当前 publisher 启动会拒绝不完整 universe。需补齐有依据的成交额分档阈值，不能填零或凭空设置默认值。
2. Kafka 最早/最新各抽样两条记录，47币均出现盘口；ICXUSDT在两端均未出现。样本时间为2026-10-13 05:56:27至2026-10-15 07:23:48 UTC，跨度约49.46小时。抽样不证明中间连续性、所有币成交覆盖或因子就绪。
3. 2026-10-15 07:26:40 UTC 查询 Binance `/fapi/v1/exchangeInfo`：47币 TRADING，ICXUSDT 为 SETTLING。ICX不能作为当前可新开仓标的。
4. ICX涉及baseline_200、baseline_036、TD_TI_017三个组合。移出后为47币/17因子/192组合，仍有25币缺阈值。原清单未擅自删除ICX；等待用户决定范围。
5. 新增13个因子身份、移出TD_TI_024，正式切换还需核对Manager virtual绑定、Exec接收和退出组合的零目标；仅保存因子清单不完成实盘切换。

## 后续顺序

确认ICX范围；补齐所需阈值及有缺口的数据；在独立计算状态下回放已有Kafka行情；验收全部组合实时ready；核对旧目标和Manager/Exec绑定后正式切换。保留原始历史与目标审计。

大部分币种可复用已有行情进行重新计算，无需重采整套行情。完整清单目前不具备直接切换条件。
