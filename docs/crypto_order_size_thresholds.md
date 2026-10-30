# Binance 月度大小单阈值发布

最后更新：2026-10-30 10:24 UTC（JP 合约49币，现货43币）

权威输入是 `tardis_order_size` 输出的 `thresholds.csv`。2026-09 样本用于
2026-10：43 币 × 合约/现货，共 86 组，P50/P90 单位为 USDT。现货与合约
分别统计，不以合约值乘系数生成现货值。缺项必须保持缺失，不填默认值。

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
金额验证函数。默认只读预演，`--execute` 才写入；只管理上述 Binance 键。
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

执行先保存精确旧 Hash 与新目标，随后在 `WATCH` 保护下以单个 Redis
事务替换所有关联键，并逐项读回验证。并发变更导致整批取消，不自动覆盖
操作员修改。失败后根据备份和实际读回处理，不盲目重试。旧值不留在可被
线上读取的另一套阈值键里。

## 消费者刷新

`trade_flow_feature_pub` 默认每 180 秒重读共享 Hash，日志包含
`symbols reloaded` 和新 symbol 数。其分类输出范围会随阈值集合收缩；
全市场波动率发布使用独立状态。BTCUSDT 为心跳要求，整批替换须保留。
JP 现货实例配置 `depth_channel=none`，只发布波动率，不读取分类阈值。

CTA publisher 的金额表在启动时载入；RX01、RX03 的现有进程可能保留启动时
的分类表，Redis 更新不能当作其内存已刷新。当前 RX01–04 的所有选用公式
均不依赖大小单。恢复大小单因子时须通过环境本地脚本启动读取新表的
publisher 并完成 Kafka 回放预热，同时核对因子清单和就绪状态。

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
