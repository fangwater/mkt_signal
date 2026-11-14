# prep-v1 本地验证

2026-10-15。只修改本地代码、页面和文档；未部署、未修改线上 RX02 配置。

实现：`position_algorithm=v0` 使用 `x`，`v1` 使用 `2*x-1`。
两者独立于现有 sign 设置，共用行情和因子预热结果。四项 symbol 开平阈值
逐字段继承全局值，覆盖配置在两种算法间共用。阈值直接比较计算结果，切换
算法不会自动换算阈值。

## 已通过

- `cargo check --bin cta_perp_signal`
- `cargo fmt --check`
- `git diff --check`
- `cargo test --lib cta_perp::`：28 项通过。
- `python3 -m unittest discover -s scripts/tests -p 'test_cta_perp_*.py'`：57 项通过。
- `node scripts/tests/test_cta_perp_panel.js`
- `scripts/tests/cta_perp_config_browser.py`：算法切换、部分覆盖保存、负数范围
  校验、空白恢复继承、配置修订校验、NQ 保留及桌面/手机布局均通过，无 JS 错误。
  使用 `/tmp/cta-signal-browser-venv/bin/python` 和本机 Chromium；浏览器运行库
  来自 `/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu` 与
  `/tmp/cta-browser-libs/usr/lib/x86_64-linux-gnu`。

针对性测试覆盖同一百分位在两套算法下产生不同开平结果、负阈值与严格边界、
原有 sign/NQ 行为、symbol 隔离、合并后阈值校验、退出沿用原持仓数量，
以及回放快照保存实际算法和生效阈值。

## 全量测试限制

`cargo test` 的库测试结果为 1279 通过、2 失败、3 忽略。失败项是既有因子
清单与旧断言不一致，此前已记录在 `docs/cta_signal_families.md`：

- `factor_pub::cta_factor_universe::tests::configured_factors_match_raw_formulas_after_warmup`
- `factor_pub::cta_spread_factor_model_1m_pub::app::tests::publisher_exposes_configured_factor_services`

跳过上述两项继续执行后，库测试 1279 通过、3 忽略；后续二进制测试运行至
`cme_tas_census` 时有 8 项失败。原因是 TAS 列规则加载到额外的 Auction、
Reference Change、Settlement Price、Symbology Change 类型，与该工具要求
的四种类型不符。该路径及规则文件未在本任务中修改。测试在此停止，不能
认定其后的二进制测试通过。

原始日志：`/tmp/cta_perp_algorithms_{check,fmt,rust_tests,python_tests,browser,full_tests,remaining_tests}.log`。
