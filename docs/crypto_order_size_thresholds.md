# Binance / OKX 月度大小单阈值发布

最后更新：2026-11-12 06:26 UTC（Binance Futures 51币、OKX 19币，2026-10→2026-11）

2026-11-12，为Spread02补齐AAVE、AVAX、BCH、ETH、FIL、LINK、LTC、TRX、UNI、XRP
十个OKX USDT永续的2026-11阈值。2026-10每币31日/744小时均完整，
共195,325,193笔成交，仍以`price * 原始张数`计算小时KLL P50/P90。
原9项逐项保持，backfill后19项；两个公共因子计划保持为空，Binance表未改。
CSV SHA-256：`0bf7e53c93f8e9918ce8767ab70e80959d842bac1462052bb88486c25fcf10ef`。
CTA按市场将共享表原生OKX键`BASE-USDT-SWAP`转换为内部`BASEUSDT`，数值与单位不变。
本机输入及回执见`artifacts/cta_spread02_deployment/order_size_202610/`，
远端写前备份和原子发布回读在`~/cta-factor-spread/run/spread02_20261112T055433Z/`。


2026-11-03 08:33 UTC，为当前Prep01 / Prep02补齐51个Binance Futures USDT标的，
ICX已移除。完整2026-10自然月31日、每币744小时，共1581文件/934404516笔成交，
复用现有小时KLL计算P50/P90，USDT口径用于2026-11。CSV SHA-256：
`59eb3dd17a35901589327d2ffebd7c15f4b07cb5fa5393c518684e3b7ad81efc`。
JP部署工具先backfill预演、保存精确写前备份，再原子补齐合约金额表0→51；逐项与CSV
读回一致，OKX8项、现货表及因子计划保持。6322维护接口确认51项；历史10月记录不代表
当前值。数据私有审计 `~/tlen_config_shared/run/binance_order_size_202610_20261103T080500Z/`，
脱敏输入/回执及因子未就绪明细见 `artifacts/binance_order_size_202610_20261103/`。
CTA共享表消费修复a0d39e69已发布：404项中331项ready、94项NQ全就绪，大小阈值缺项0；
剩余73项为公式值或有效样本未就绪，其中58项为15min。仓位推送和交易保持关闭。

权威输入是 `tardis_order_size` 输出的 `thresholds.csv`，完整自然月样本用于
下一个 UTC 月。P50/P90 与实时分类端统一使用 `price * amount`：Binance 单位为
USDT，OKX 永续为 `price_x_contracts`（价格乘原始张数）。各所、现货和合约
分别统计，缺项保持缺失，OKX 数值不能当作真实 USDT 成交额跨币种比较。
下面 Binance 的 2026-10 记录来自 2026-09 样本：43 币 × 合约/现货，共 86 组。

## OKX USDT 永续

`tardis_order_size --exchanges okex-swap` 读取
`TardisSource/okex_usd_<symbol>/trades/okex-swap_trades_<day>_<SYMBOL>.csv.gz`。
文件名中的 `DOTUSDT` 对应 CSV 内的 `DOT-USDT-SWAP`；交易所和原生合约名必须匹配。
Tardis 的 OKX 映射直接保留 `sz` 张数；JP 的 OKX JSON/SBE 解析、
`spread_pbs` IPC 和 Period 收集端也保留原始张数。大小单统计和实时分类
统一使用 `price * amount`，不乘 `ctVal`。2026-11-03 同时采集官方公开成交和
JP IPC，在 DOGE 上匹配到 9 笔：例如官方 `sz=7.8`，IPC `amount=7.8`，
该合约 `ctVal=1000`，数量未变成 `7800`。

03:31–03:34 UTC 重新从 JP 直接读取 Kafka `okex-swap` 的 partition 0，
获得 22 条 zlib/PeriodMessage 记录，每条含 220 个币种；仓库原有 Rust
消费者也独立成功读取 8 条记录。将 Kafka 成交与同时段 OKX 公开成交对照，
8 个币种共匹配 54 笔无歧义成交，所有 `amount` 均等于官方原始 `sz`。
例如 DOGE 的 `sz=28.99`，Kafka `amount=28.99`，没有变成 `28990`。
54 笔已按重复 payload 去重；8 笔候选不唯一、2 笔未匹配，未纳入单位结论。
Kafka 探测证据在 `artifacts/okex_order_size_202610_20261103/kafka_reprobe/`。

此前超时发生在本地访问路径，重新探测仍可复现；同一 release Rust
消费者和相同配置在 JP 可正常读取 offset 和消息，不能将本地超时理解为
Kafka 无数据。broker 元数据发布内网地址 `10.140.140.55:1501`，探测沿用
现有 `broker_addr_rewrites` 将其解析到可达的 bootstrap 公网地址。
Kafka topic 为 `okex-swap`，系统内部 venue 为 `okex-futures`。

工具强制要求 `--okex-instruments` 指定保存的 OKX 公共 instruments 响应，验证
线性 SWAP、USDT 结算、`ctValCcy` 为基础币、`ctMult=1` 及正有限 `ctVal`。
缺规格或其他类型合约会失败，不默认面值为 1。`ctVal` 只用于审计，不应用于
分类统计。规格快照和每个原始文件均记录 SHA-256。对同币种固定面值，
阈值与实时数值同时不换算，分类结果与两边同时换算一致。

2026-11-03 核对并计算 TRB、DOT、ADA、NEAR、ATOM、DOGE、ETC、CRV 的
2026-10 全月样本，8 个币各 744 个非空 UTC 小时，共 65,332,809 笔有效成交。
原始文件只读，所有任务完成，非法值、跨月数据和迟到跨小时数据均为 0。
合约面值快照取自 OKX 公共 API，时间为 2026-11-03 02:20:55 UTC。

```bash
target/release/tardis_order_size \
  --root /mnt/nfs/30.133_xt_crypto/TardisSource --month 2026-10 \
  --symbols TRBUSDT,DOTUSDT,ADAUSDT,NEARUSDT,ATOMUSDT,DOGEUSDT,ETCUSDT,CRVUSDT \
  --exchanges okex-swap --okex-instruments /path/to/okex_instruments.json \
  --workers 8 --output /path/to/new-output-directory
```

计算和独立 Python 基准使用同一小时 KLL 算法。工具回归覆盖 OKX 原生合约名、
原始张数保持、缺失/非法规格拒绝，以及原 Binance 的分位、文件完整性和确定性。
当前证据在 `artifacts/okex_order_size_202610_20261103/native_alignment/`，
CSV SHA-256：`e81dfd6e934d5e8a21d01b214a394a0fafdf061b4cfb9fa29430544ab91a3832`。

发布复用 `scripts/publish_binance_order_size_thresholds.py`，新增
`okex-swap -> okex-futures` 映射；CSV 的 `DOTUSDT` 转为线上字段 `DOT-USDT-SWAP`。
OKX 使用 `--mode backfill`，只填充输入币对；同月重复相同值可重入，拒绝覆盖不同值。
backfill 保留全部因子计划，所有市场共用原有 `amount-thresholds` 格式，不增加频率副本。
`replace` 仍仅用于 Binance 的相同币种现货/合约整批替换。
CSV 必须带匹配市场口径的 `unit` 列，拒绝缺单位或错误单位的文件；
backfill 也拒绝混入与线上元数据不同的数量口径。

2026-11-03 02:37:30 UTC 首次发布时错误地只在历史统计侧乘了 `ctVal`，
与实时分类端不一致。03:09:39 UTC 已完成原始张数口径的保护性纠正：
JP `~/tlen_config_shared` 的 `okex-futures:amount-thresholds` 保持 8 项，
更新 ADA、DOGE、ETC、NEAR、TRB 的阈值；DOT、ATOM、CRV 数值不变。
`meta` 记录来源月 2026-10、应用月 2026-11、`price_x_contracts` 单位、
新旧 CSV 摘要与纠正来源。全部因子计划保持不变。
发布脚本经 SHA-256 校验后原子安装；未重启计算、信号或交易进程。
Redis 及现有 `6322/api/thresholds?venue=okex-futures&config_type=amount_thresholds`
接口于 03:09:57 UTC 逐项读回与新 CSV 一致。当前备份、dry-run、发布与
安装回执在远端 `~/tlen_config_shared/run/okex_order_size_202610_20261103/native_alignment/`，
本地同名目录包含当前 `verification.json`。父目录保留首次错误口径的原始证据，
不应作为当前阈值使用。工具 8 项、发布 10 项测试全部通过，发布测试使用隔离 Redis；
Period 相关 17 项测试通过。

2026-10-30为RX01新清单补齐BNBUSDT、CRVUSDT、FILUSDT、THETAUSDT、XRPUSDT、
ZILUSDT六个合约币种。复用tardis_order_size扫描各自完整2026-09的720小时原始成交，
六项均完成；68.1秒处理，单市场独立P50/P90用于2026-10。CSV SHA-256为
`7179881e9a8fdb0270f984ac0f3d4ddbeea2f3e9cb220a95518719f3ea5e1b7d`。
预演确认合约表43→49、零更新/零删除，随后backfill原子写入并读回验证；现货表和
公共factor-plan的20/13项保持。远端备份及回执位于
`~/tlen_config_shared/run/rx01_order_size_202609_20261030/`；本地证据在
`artifacts/rx01_new_factor_readiness_20261030/order_size_*`。下文43币记录属于10月23日整批发布。

## 线上数据源

JP `jp-meta-elvpn` 的本机 Redis DB 0 存储以下现有 Hash，
`~/tlen_config_shared` 的 6322 HTTP 服务直接读取：

| Tardis exchange | 运行时 venue | Redis key |
| --- | --- | --- |
| `binance-futures` | `binance-futures` | `binance-futures:amount-thresholds` |
| `binance` | `binance-margin` | `binance-margin:amount-thresholds` |
| `okex-swap` | `okex-futures` | `okex-futures:amount-thresholds` |

每个 symbol 的 JSON 只含 `medium_notional_threshold` 和
`large_notional_threshold`，保持现有消费者协议。现有 `:meta` 伴随键记录
样本月、应用月、单位及 CSV SHA-256。小单 `<P50`，中单 `[P50,P90)`，
大单 `>=P90`。阈值只按市场、币对和样本月区分，所有因子周期共用，
不按 5s、1min、5min 复制金额边界。

重复的 `amount-thresholds-1m` 及其 `:meta` 已备份删除；TLen 的
`amount_thresholds_1m` 读写接口及页面选项已移除，补数工具也不会再创建。
公共 1min 因子发布源码改为使用共享 `amount_thresholds`。
各周期仍独立管理 `factor-plan` / `factor-plan-1m`、`zscore` / `zscore-1m`。
配置接口可列出共享阈值覆盖的全部币种，但空因子计划会被计算端忽略；
共享阈值扩充不会自动给新币种分配因子。

整批替换会同步移出公共因子计划中的缺阈值条目，避免 TLen 接口报错。
移出项保留在文件备份中，之后补阈值不会自动恢复这些因子计划。
RX01–04 的环境独立因子表不由此工具修改。

## 整批替换与后续补数

工具：`scripts/publish_binance_order_size_thresholds.py`，复用 TLen 的键名和
金额验证函数。默认只读预演，`--execute` 才写入；只管理输入市场对应的上述键。
通过 `REDIS_HOST/PORT/DB/USERNAME/PASSWORD` 设置连接，凭据不进入命令行。

```bash
# 首次清理旧值，或换月：须包含现货/合约相同币种集合。
python3 scripts/publish_binance_order_size_thresholds.py \
  --csv /path/to/thresholds.csv --mode replace --report /path/to/dry_run.json

# 审核预演后执行，backup 必须是尚不存在的新文件。
python3 scripts/publish_binance_order_size_thresholds.py \
  --csv /path/to/thresholds.csv --mode replace --execute \
  --backup /path/to/before.json --report /path/to/applied.json

# 同月缺项补充：可仅包含新增币种或单一市场；先预演，再加 execute/backup。
python3 scripts/publish_binance_order_size_thresholds.py \
  --csv /path/to/missing_thresholds.csv --mode backfill
```

`backfill` 为默认模式，要求线上记录的样本月、应用月与输入一致，允许重复
输入相同值，拒绝覆盖已有不同值。应用月必须为当前 UTC 月；每组必须具有
完整自然月小时数、正有限 P50/P90，重复 market/symbol 或混合月份拒绝。

纠正本工具自身的错误发布使用 `--mode correct --correct-from /path/to/previous_backup.json`。
必须提供上次实际执行的完整写前/目标备份，且当前全部受管 Hash 与其目标完全一致，
输入币种集合也必须与上次写入集合完全一致；任意操作员变更或部分范围均拒绝。
先预演，再加 `--execute --backup /path/to/new_backup.json`，保留精确的新旧值。

执行先保存精确旧 Hash 与新目标，随后在 `WATCH` 保护下以单个 Redis
事务替换所有关联键，并逐项读回验证。并发变更导致整批取消，不自动覆盖
操作员修改。失败后根据备份和实际读回处理，不盲目重试。旧值不留在可被
线上读取的另一套阈值键里。

## 消费者刷新

`trade_flow_feature_pub` 默认每 180 秒重读共享 Hash，日志包含
`symbols reloaded` 和新 symbol 数。其分类输出范围会随阈值集合收缩；
全市场波动率发布使用独立状态。BTCUSDT 为心跳要求，整批替换须保留。
JP 现货实例配置 `depth_channel=none`，只发布波动率，不读取分类阈值。

当前 `cta-prep-binance-futures` 的 Prep01 / Prep02 按需要从同一共享Redis表加载
大小单阈值，不再维护TOML副本；无大小单依赖的纯价格/NQ/Spread清单不读取金额表。
启动及每180秒复查校验应用月、来源月、市场单位和正有限有序阈值；数值变化时重新
回放，读取失败保留最后成功快照并报告错误，不使用默认值。配置编辑与 `--check`
保持只读且不连接Redis。回放完成后仍须核对实际阈值覆盖及因子就绪状态。
2026-11-03发布后的Prep已读取51项，全部94项NQ就绪，仓位推送及交易保持关闭。

## 本次发布与验证

证据位于 `artifacts/binance_order_size_thresholds_20261023/`。
输入 CSV SHA-256：
`9511e93965a98cf8723323cd3949171a9d9cd9fe8834b68eb1aa9e578074f047`。

- 共享合约表：115 → 43；现货表：100 → 43。
- 重复的 1min 金额表及元数据已删除；1min 公共因子计划仍为原来的13币。
- RX01/02/03/04 当前配置币种均被本次 43 币覆盖。
- 6 项测试通过，含独立临时 Redis 的 dry-run 无写入、备份读回、事务替换和
  并发冲突整批取消；测试使用临时 Unix socket、TCP port 0，不连接生产 Redis。
- 3 项 HTTP 测试验证跨周期共享、因子清单独立、旧入口拒绝读写。
- Rust 全库测试1354项通过、3项忽略，`cargo fmt --check` 通过。

04:18:23 UTC 首次在 Redis 单事务中替换；合约实例 PID37684 于04:19:44
自动加载43币并退役95个旧币种。04:32 UTC 进一步统一跨周期阈值来源，
移除重复键及其接口。04:32:59 核验两份43币共享表与原CSV完全一致，
各周期公共因子计划、RX01–04因子表及其他交易所金额阈值均未改变。
仅通过环境本地脚本重启TLen配置服务；全部其他原运行进程保持PID和启动身份。
未更改CPU绑定、网络或交易参数。

新表中保留的是 86 组本月有效值。旧表中没有本批数据的合约 95 币、现货
81 币已移出，名单在同目录 `removed_old_symbols.csv`，用于后续缺项补算。

线上工具安装于 `~/tlen_config_shared/scripts/publish_binance_order_size_thresholds.py`；
CSV、写前备份和回执位于 `~/tlen_config_shared/run/order_size_202609_20261023/`。
`before.json` 包含首次替换的全部旧值与新值。统一入口前的脚本和重复Hash
另保存在该目录的 `unify/` 下，本地证据亦保留。线上TLen原来未包含仓库的
Bitget COIN-M支持，本次只应用金额表统一的补丁，保留其原有场地支持差异。
当前未运行旧公共fusion publisher，因此没有安装或启动新的Rust因子二进制；
未来恢复该服务须发布当前源码构建的版本。CTA publisher无需因本次入口清理重启。

运行测试：

```bash
python3 tests/scripts/test_publish_binance_order_size_thresholds.py -v
python3 tests/scripts/test_tlen_shared_amount_thresholds.py -v
```

需安装 `redis-server` 和 Python `redis` 才运行 2 项事务测试；否则明确跳过。
