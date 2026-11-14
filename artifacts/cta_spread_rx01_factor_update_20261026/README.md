# RX01 因子更新及执行核验

原始请求：18 币、7 个不同因子、59 个组合。

7 个因子都不依赖大/中/小单字段，且均已列入 Spread publisher 的免金额阈值名单。实际 Rust 计算链路在 2880 根完整分钟线中，带大小单、缺失大小单和大小单全零三种输入的 score、quantile、ready 全部一致。

| 因子 | 首个原始值所需分钟数 | 完整预热所需分钟数 | 完整预热时长 |
| --- | ---: | ---: | --- |
| TD_PR_011 | 1 | 2439 | 40小时39分 |
| TD_TI_024 | 1 | 2439 | 40小时39分 |
| baseline_005 | 61 | 2499 | 41小时39分 |
| baseline_028 | 31 | 2469 | 41小时09分 |
| baseline_105 | 15 | 2453 | 40小时53分 |
| baseline_140 | 60 | 2498 | 41小时38分 |
| baseline_164 | 30 | 2468 | 41小时08分 |

预热包含标准化（1440槽、至少1000样本）和后续分位数（2880槽、至少1440样本）；两天完整数据足够。真实缺失数据可能延迟就绪，以上不是线上新清单已完成回放的声明。候选配置保留现场3天 Kafka 回溯和现有标准化参数。

当前 Exec06 的 binance-margin 与 binance-futures 规则均缺失 ICXUSDT，其余17币均为 TRADING。`market_rules_audit.json` 保存当次新鲜规则的状态与时间戳，不含账户凭据。

候选配置排除 ICX 的4个组合，得到17币、7因子、55组合；原始请求完整保留。已通过现有 normalize_table、validate_available 和 prepare_deployment 校验，候选文件均带 candidate 后缀。

用户确认排除ICX后，已于2026-10-26 03:42:36 UTC切换线上因子表。新清单55/55组合就绪，133个移除组合的内部方向状态归零；对冲库存及旧账本保留。

`warmup_probe.rs` 是仅在本地保留、Git忽略的临时审计程序，复制当前实现并链接现有本地 debug 库；`warmup_bounds.json` 保存方法、结果及源文件 SHA-256。生产部署复用已验证的既有release二进制，未更改运行算法。随后本地标准cargo test已重新编译并通过全部19项publisher定向测试。


## 部署与执行结果

- `binance-cta-spread-rx01` publisher通过环境本地脚本旁路预热并交接，PID3973994 → PID3173585，所有线程CPU17。signal/config/dashboard和Exec06六个进程未重启。
- 复用既有release二进制，SHA-256为 `693b9277c42dfed0ba39e1c5c7f6aff0cc8855ccbf2a49af8094538443f0d023`；本地与远端一致。上传的候选文件均按manifest核验，配置原子替换，因子表通过现有API的revision CAS更新。
- 现场enabled=true、单组合10 USDT、全部entry/exit/NQ参数、lgq_grid_spread、virtual03→Exec06倍率1保持。env.sh仅修改publisher进程名，完整凭据备份仅保存在远端。
- 解码/计算/发布错误均0；1710项signal→Manager→Redis→Exec目标全部匹配，待投递0。
- 03:46最终快照中新清单110个现货/合约目标中109个在原10 USDT容差内。baseline_140/IOST现货差约19.82 USDT，原因为borrow_unavailable；新清单无挂单或pending，不能声称全部实仓到位。
- 另有20项历史对冲库存目标超容差，原因包括既有借币不足、最小下单限制及正在执行的旧目标；完整清单在execution_final.json。未手动改变借款、账本或容差。
- 只读RapidX查询的110项余额/仓位与Exec账户快照一致。切换后的取样窗口已观察到20笔成交记录，约79.85 USDT名义金额；成交角色在该报告中为unknown，未将其归为Maker或Taker。
- 19项Rust、19项Python及4项发布器交接测试通过。仓库三份RX01配置、既有配置断言及living ops文档已同步更新。

`deployment_result.json`为精简结论；`verification.json`、`execution_final.json`和`process_verification.json`为纳入Git的详细证据。完整轮询时间线`post_activation_timeline.json`、重复执行快照及原始仓位输入仅本地保留并忽略。行情和策略会持续变化，仓位数值是所记时间的快照。

远端备份和回执：`~/binance-cta-spread-rx01/run/factor_update_20261026/`。其中`before/env.sh`含凭据，禁止复制到本地、仓库或输出。

## 04:22 UTC 按币退场规则及清仓结果

用户明确：新表内的币继续按仓位表执行，仅完全从新表移除的币直接清仓。
已更新并部署 `src/cta_spread/signal.rs`，双边使用既有 `clear` 数量和taker信号；
新表内456个旧对冲组合保留，清理新表之外344个历史组合及独立库存中相应币种。
初次批量退出遇到RapidX 429，暂停/恢复Exec06 pre_trade后，以现有内部对敲记账承接
账户真实余仓至inventory，再逐币ctaker退出，没有清零账本或重写历史快照。
新表17币、55组合、普通执行模板和信号参数保持。36项Spread Rust测试通过。

最终退场因子仓位为0，无相关挂单/pending；旧币绝对市值约2519.62降至34.06 USDT。
剩47项市场零头均因最小下单门槛未退出，最大单市场约4.80 USDT；包括现货负债差额和
少量合约零头，因此没有声称账户完全归零。费用/借息微量差额由SYSTEM_POSITION_CLOSE
如实保留，未分摊编造交易。新表内IOST借币不足差额仍保持原处理，不纳入退场清理。

最终回执：`coin_retirement_result.json`。订单链路及交易所核对：
`execution_retirement_final.json`。源程序、发布哈希和测试日志在本目录保留。
Signal PID3267446，pre_trade PID3289558，其余Exec进程与publisher/config/dashboard保持；
两个更新进程CPU mask仍0-5,48-95，IP/ENI/监听未改。远端备份和明细：
`~/binance-cta-spread-rx01/run/factor_update_20261026/coin_retirement/`。
