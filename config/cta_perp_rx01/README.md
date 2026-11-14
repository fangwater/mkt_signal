# RX01 CTA Perp 15min

环境 `binance-cta-perp-rx01` 使用 Perp 15min 发布器、独立因子仓位信号和页面。
2026-10-30 操作员清单包含38币、89个计算因子、145个币种×因子组合、
146条独立仓位规则。原始154行清单及经操作员确认排除的ICX/STORJ八行保存在
`artifacts/rx01_new_factor_readiness_20261030/`；两币当前不在RapidX市场及在线行情阈值中。

`factor_rules.json` 保留组合输入格式，`cta_perp.json` 保存规范化逐组规则，
`cta_perp_symbol_factors.json` 和发布器universe只列去重后的计算依赖。
SKL/factor_170的NQ关闭和0.50两条规则共用计算结果，各自维护开平、入场数量、
恢复进度和Manager目标。重复币种×因子以规范化NQ及方向区分策略名，调换输入顺序
不改变身份；完全相同的规则被拒绝。单份金额200 USDT分别作用于两条规则，
同时持仓时按两条规则累加。因子订阅上限为128。

NQ固定120根、至少60个有效样本；因子分位192根、至少96根，完整窗口两天；
zscore96/67，完整窗口一天。逐组NQ使用严格分位比较，关闭跳过过滤。
仓库配置默认 `enabled=false`；上线先确认历史追平和仓位表，再配置Manager发布与跟随。
运行状态、Redis、IPC、历史与旧Spread独立，旧仓位和归档不作为新策略状态。

publisher CPU17，Dashboard `127.0.0.1:10195`，Config `127.0.0.1:19195`。
构建使用本地arbmm release，上传核验SHA-256并原子安装，进程使用环境本地脚本启停。

发布器已注册全部23个depth因子，按清单启用ENJ/depth_imb_all、
IOTA/depth_touch_net及IOTA/depth_level_imb。全部盘口档由Kafka消息复用
`depth_pub::OrderBook`重建；口径见 `docs/depth_book_factors.md`。
两个net保持 `(B-A)/(abs(B)+abs(A))`，双方均为零或非有限输入返回NaN。
# 历史预热

长周期原始公式保留其原有窗口；NQ 固定 120 根，因子分位数保留两天。
Kafka 留存不足时，先用 `cta_baseline_seed` 从连续日期的 Tardis 原始
`trades` / `incremental_book_L2` 生成闭合 baseline，再加载到
`baseline_seed_path`。工具复用线上 `LocalBaselineAggregator` 和
`MultiMinuteBaseline`，使用与线上相同的真实月度成交额阈值和毫秒事件时间。
重叠的 Kafka bar 不重复计算，首个新 bar 必须与 seed 连续或包含 seed 末端。
运行时持续原子保存最近 2048 根 baseline，供之后重启恢复长窗口。

示例（默认仅检查输入，确认清单后加 `--execute`）：

```bash
target/release/cta_baseline_seed \
  --config config/cta_perp_rx01/cta_perp_factor_model_15m_pub.toml \
  --thresholds /path/to/current_amount_thresholds.json \
  --source-root /path/to/TardisSource --start 2026-10-22 --end 2026-10-28 \
  --output /path/to/baseline_seed --workers 8
```
