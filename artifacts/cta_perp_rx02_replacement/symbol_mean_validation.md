# v1 同币种因子算术平均

2026-10-15。本地实现，未部署；未修改线上配置或下单。

## 最终行为

- v0：各 symbol × factor 使用百分位 x，独立判断开平。
- v1：同一 symbol 的启用因子分别计算 2x−1，然后取等权算术平均，
  以综合值统一判断开平。sign 保留原有方向修正，weight 的绝对值不改变占比。
- 两版都使用现有单份金额参数；每次入场数量为方向×单份金额/入场价，
  持仓期间数量固定，退出使用原数量。没有新增仓位上限参数，不按均值幅度缩放。
- 四项 symbol 阈值覆盖仍逐字段继承全局值，作用于所选算法的最终判断值。
- v1 等待所有启用因子同分钟就绪；缺失时保持原持仓，不用部分因子重算分母。
- v0 保留独立因子策略，v1 以 symbol_mean 记录每币综合仓位。现有持仓须先
  关闭才能切换算法；API、启动恢复和热加载检查归属，不静默转移持仓。
- 行情和预热结果共用，切换无需新增时间序列 SMA 窗口或重新采集行情。

## 针对性验证

- `cargo test --lib cta_perp::`：33 项通过。
- `python3 -m unittest discover -s scripts/tests -p 'test_cta_perp_*.py'`：62 项通过。
- `node scripts/tests/test_cta_perp_panel.js`：通过。
- 配置页浏览器检查通过：算法切换、四项覆盖、继承、范围和修订校验。
- 交易面板浏览器检查通过：v0 因子隔离、v1 每币综合方向、全部因子贡献、
  B/S 和 NQ、单份金额换算，桌面/手机无横向页面溢出，无 JS 错误。
- `cargo fmt --check` 和 `git diff --check`：通过。
- `cargo check --bin cta_perp_signal`：通过。

`cargo test`：库测试1284项通过、2项失败、3项忽略。两项失败仍为之前已知的
因子清单/断言不一致：
`factor_pub::cta_factor_universe::tests::configured_factors_match_raw_formulas_after_warmup`
和 `factor_pub::cta_spread_factor_model_1m_pub::app::tests::publisher_exposes_configured_factor_services`。
全量测试在库测试失败后停止，未声称其余二进制测试通过。

用例包含：因子值均值未越阈值时不因单个因子投票而开仓、权重绝对值不改变
等权均值、相反值抵消、严格阈值边界、部分分钟到达不提前消费、禁用因子排除、
NQ 只过滤综合方向开仓、缺失因子保留原仓位、平仓沿用原 qty、对冲因子净额为0
但尚有原持仓时禁止转移归属、配置与历史回放/API/页面一致。

原始日志保留在 `/tmp/cta_perp_mean_{rust,python,config_browser,panel_browser,fmt,check,full_tests}.log`。
上一轮全量测试的既有失败说明见 `algorithm_validation.md`。
