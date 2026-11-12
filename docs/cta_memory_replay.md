# CTA 内存阈值回放

最后更新：2026-11-06 UTC

独立的 `cta-memory-replay` 在开发机 `172.16.30.38` 上运行，从
`10.61.10.22:9092` 的 `binance-futures` 读取压缩 PeriodMessage。
页面地址为 `http://172.16.30.38:10242/`。
运行目录入口为 `~/cta-memory-replay`，实际文件在
`/mnt/nvme-raid0-28t/fanghaizhou/deployments/cta-memory-replay`。
构建时根盘空间不足，pip 下载缓存也移至
`/mnt/nvme-raid0-28t/fanghaizhou/cache/pip-cta-replay-space-recovery`，
原 `~/.cache/pip` 保留符号链接；没有删除缓存或安装包。

## 数据与参数

首次部署通过 `scripts/bootstrap_cta_memory_replay.py`，以 SSH 在
`jp-meta-elvpn` 上只读获取
`~/cta-prep-binance-futures/config/cta_prep_binance_futures/` 的当前 Prep01/Prep02
统一组合 JSON 及 `symbol_groups.json`，不使用已经停用的 RX Redis 因子键。

| 策略组 | 券池 | 1min 规则 | 5min 规则 | 15min 规则 |
|---|---|---:|---:|---:|
| Prep01 | all | 45 | 48 | 146 |
| Prep02 | top-5 | 5 | 2 | 7 |
| Prep02 | top-20 | 27 | 0 | 10 |
| Prep02 | last-20 | 26 | 5 | 17 |

共 12 份配置、338 条规则。空券池周期保留身份但不生成模拟规则。
直接使用现行组合的因子定义、NQ、开平仓阈值和单份金额；
同因子不同 NQ 条件保留独立规则选择，底层因子数据仍只计算一次。
页面按策略组、券池、周期切换；每份来源文件保留路径与 SHA-256。
脚本另读取公共 `binance-futures:amount-thresholds` 及 `:meta`，验证月份和单位。
不读取 env.sh，不导出凭据，不向 Redis 写配置。

因子计算复用 `cta_factor_calc::Engine`，NQ 和开平仓复用当前公共状态机。
相同因子只计算一次。首次从 Kafka 按时间定位历史起点，之后持续增量消费；
手动分区分配、不提交 offset，不参与其他消费组的 rebalance。

历史因子、NQ、成交 K 线的成交量和成交额均在进程内存中。
`POST /api/replay` 从同一截止时间的内存数据重算各策略全部币种，不访问 Kafka、
磁盘或 Redis。请求参数不修改初始清单或其他用户的回放。
页面默认汇总全部币种与规则，分别显示 Prep01/Prep02 的总理论盈亏和收益曲线，
并保留策略组、券池、周期维度的12行明细。可以筛选策略组、券池和周期，
统一修改四个开平阈值、单份金额及手续费，并保留最多八次结果比较。
初始阈值、金额及每条规则的 NQ 条件沿用 JP 当前配置；没有币种筛选步骤。
网页资源独立发布在 `web/`，更新页面不需要重启因子进程或重新读取历史。

每次从指定起点空仓开始；反向信号先退出，下一根 bar 才能重新开仓。
同向持有不加仓、不随价格重设 qty，平仓使用原数量。没有未来值填充。
使用 taker 全量成交假设：信号在闭合 bar 边界 `t` 决策，成交价取之后
`[t,t+interval)` 下一根已闭合 K 线的 `VWAP = sum(price × amount) / sum(amount)`。
只使用已经完整闭合的成交 bar；零成交量或缺失价格不填造交易。
不模拟实际传播延迟或挂单成交过程；未平仓以对应最新已闭合 VWAP 估值。
K 线由同一 Kafka 成交流合成，保留真实成交量和成交额。
默认手续费为零，显示理论毛收益；可指定单边 taker 费率，资金费不进入此模型。
胜率只包含已平交易，组合回撤从合计收益曲线计算，不加总各规则回撤。
未就绪规则显示预热状态，避免把尚未产生决策的零收益当作有效回放结果。

历史范围默认最近七天，并受 Kafka 留存范围和当前金额阈值适用自然月限制。
不将本月大小单阈值应用到上个月；因子窗口未就绪的 bar 保持未就绪。
跨自然月时须重新从 JP 获取适用月份的初始化快照并重启。
进程重启会清空内存并重新读取初始历史；调整回放阈值不重启进程。

## 构建、部署与管理

在维护工作树、与 `origin/arbmm` 同步的 `arbmm` 分支构建：

```bash
cargo fmt --check
cargo test --bin cta-memory-replay
cargo build --release --bin cta-memory-replay
scripts/deploy_cta_memory_replay.sh --dir ~/cta-memory-replay \
  --bind 172.16.30.38 --port 10242 --skip-build --execute
```

部署验证 SHA-256 后原子安装二进制；已有 `env.sh` 和 bootstrap 不覆盖。
部署脚本不自动启动进程。以下命令必须使用部署目录内脚本：

```bash
~/cta-memory-replay/scripts/start_cta_memory_replay.sh --execute
~/cta-memory-replay/scripts/status_cta_memory_replay.sh
~/cta-memory-replay/scripts/stop_cta_memory_replay.sh --execute
```

独立 PM2 数据目录为 `<部署目录>/run/pm2`，进程名
`cta-memory-replay`，namespace 为 `cta-replay`。
启动脚本保存该独立 PM2 清单，不操作开发机其他 PM2 进程。
程序日志位于 `<部署目录>/logs/`，发布摘要位于 `run/release.json`。
如需系统重启后恢复，应通过该独立 PM2_HOME 的常规系统启动配置执行 resurrect；
本部署不修改主机启动服务。

主动刷新初始配置需明确执行，再通过环境本地脚本重启：

```bash
cd ~/cta-memory-replay
python3 scripts/bootstrap_cta_memory_replay.py --output config/bootstrap.json --execute
./scripts/start_cta_memory_replay.sh --execute
```

`GET /api/catalog` 提供历史覆盖、预热数量、消费进度和错误；
`GET /api/healthz` 在历史未追上或接收异常时返回 503。
所有 HTTP 操作仅用于观察和模拟，没有 Manager/Exec 发布或下单路径。

## 已有因子存档的离线比较

当 Kafka 留存或本机预热无法覆盖指定行情时，可以使用 JP 已就绪的
`FactorPoint` JSONL 存档和 Manager 已缓存的分钟 K 线，在本机离线比较。
该模式不启动 HTTP 服务、不连接 Kafka，也不影响运行中的内存缓存。

```bash
target/release/cta-memory-replay --bootstrap /path/to/bootstrap_used.json \
  --offline-input /path/to/crash_dataset.json \
  --offline-output /path/to/crash_results.json
```

输入对象包含 `source_generation`、原始 `source_plan`、原生 `points`、
`candles` 和 `jobs`。每条 candle 为
`{symbol, interval_ms, candle: {ts_ms, open, high, low, close, volume, turnover}}`；
每个 job 为 `{label, request}`，request 与 `POST /api/replay` 相同。
各 job 使用同一数据集、相同截止时间，复用在线回放的仓位和组合计算。
导入时校验所需因子和启用的 NQ 定义与 bootstrap 一致，拒绝重复时间点。
离线输入可指定 `rule_details_from_ms`，额外输出每条规则从该时间开始的决策轨迹
及仍与该时段相交的交易，便于分析因子、退出时点和组合回撤贡献。轨迹与组合
共用同一请求构造和原生仓位模型；该选项不改变在线 API 或运行中的服务。
历史信号以已闭合 bar 边界作为可用时间，仍按下一根完整 bar 的 VWAP 成交；
这是理论成交假设，不还原当时实际传播或执行延迟。

2026-11-05 BTC 急跌的比较数据、配置快照、覆盖检查、批处理结果和图表保存在
`~/cta-memory-replay/run/exit-threshold-comparison/`。两套退出阈值均使用当前
103 条 1min 规则（41 个币种），从 11 月 3 日 00:00 UTC 空仓回放到
11 月 6 日 00:00 UTC，并在 11 月 5 日 12:00–16:00 UTC 衡量急跌回撤。
大跌窗口的普通因子和 NQ 全部就绪；价格无缺行，零成交量 bar 跳过成交。
当前规则清单与存档 generation 的策略清单可能不同，因此结果表示当前配置的
理论反事实，不作为原实盘账户历史盈亏或历史仓位的还原。
