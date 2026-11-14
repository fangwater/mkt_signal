# Prep02 用户清单录入

`requested_rules.json` 保留用户原始 18 组规则；展开后 102 条，按频率、标的、因子、符号及
NQ 完全相同去重，得到 99 条：1min 59、5min 7、15min 33。
重复项为 SOL 15min baseline_104、BTC/XRP 15min baseline_022；不同 NQ 的规则均保留。
各标的补 USDT，频率规范为 1min/5min/15min。未更改 Prep01 或 Spread 因子清单。

三个频段均写入 `config/cta_prep_binance_futures/prep02_*.json`。
研究库确认 `TD_MT_040 = rolling_sum(high-close, 10)`，与 Rust 实现一致。
用户明确确认将 SOL/DOGE 15min 的 `TD_MI_040` 两条改为 `TD_MT_040`，已移出草稿目录。
修正后的 99 条通过计算器计划校验。

USDT 额度及开平仓参数暂沿用同频段模板：1min 100、5min/15min 200；v0 因子独立持仓，
三个 `enabled=false`。用户随后明确授权部署，已随 `fa63d6e4` 安装并启动 Prep 历史预热；
未启用仓位发布或创建执行绑定。部署回执见 `../cta_prep02_deploy_20261103/verified.json`。
`verification.json` 包含条数、配置文件位置与待核对项；30 项 CTA 计算/仓位测试通过。
