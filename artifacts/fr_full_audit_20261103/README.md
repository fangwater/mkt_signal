# FR 全面核查与 Delta 中性开启记录

交易所只读对账主要采样时间：2026-11-03 14:04 UTC；最终选定盘子快照复核：14:23:46 UTC。Delta 设置最终确认：14:21:37 UTC。

## 已执行操作

- gate_fr_arb01：按用户授权买回 124 张 ESPORTS_USDT 合约，乘数 100，即 12,400 ESPORTS；订单 256986654986929807 全部成交。交易所精确净多 1.745125 ESPORTS，约 0.023 USDT。Gate 显示 full_hedged、unhedged_size=0、ADL 排名 5。
- 此前 ALPINE_USDT 已补建 8,500 张空仓，净多剩余 0.718616 ALPINE，约 0.218 USDT；14:23 快照仍已对齐。
- 11 套运行中 FR 的 Delta 中性均已回读确认 ON。本次从 OFF 开启 Binance 02、Gate 03、Bitget 03、Bybit 01；其他账户原本为 ON。
- Gate 04 最初按要求排除全面核查；用户后来单独授权打开其 Delta。14:21 回读它原本已 ON，因此没有额外 POST。未扩大为 Gate 04 全面审计。
- 未调整其他盘子的杠杆、风险参数、下单节奏、抵押开关或进程。

## Binance FR 03 结论

现货新单路径 `/papi/v1/margin/order` 持续返回 HTTP 400 / code 51042 / Operation performed too frequently。14:00:43–14:15:42 UTC 的本地日志中，有 14,860 次此类下单拒绝，涉及多个币种；同期另有 585 条撤单 Unknown order 错误。共享源 IP 的 Binance 04 同期没有 51042。

已经确认的程序问题是：51042 在本地仍归类为 unknown；REST dispatcher 的统一限流退避只处理 HTTP 429/418。失败后新信号继续创建新策略/订单，arb_open 的本地速率参数为每分钟 1,000、每 10 秒 500，因此形成持续重试。查询到交易所普通 Margin ORDERS 限额为每分钟 1,200；UM apiTradingStatus 没有 isLocked 指标。现有接口未提供 51042 的具体业务触发阈值，不能把它直接等同于公开的 1,200/分钟配额超限。

最早保留的 51042 日志在 2026-10-18 13:34:47 UTC；期间交易仍有成交，因此不能描述为自该时刻起持续完全锁死。当前进程、账户风险、行情快照均在更新；14:04 交易所未成交订单为零，净敞口约 172 USDT。当前杠杆约 1.664，低于配置 1.7。

两笔较大账本差额均已精确解释为启动后现货币本位手续费：

| 币种 | 实际净仓与策略账本之差（币） | 启动后现货手续费（币） | 对应差额估值 |
| --- | ---: | ---: | ---: |
| BEAMX | -4,673.468 | 4,673.468 | 约 -18.7 USDT |
| GENIUS | -40.763084 | 40.763084 | 约 -12.4 USDT |

采样实际净仓分别为 -4,332.04944 BEAMX（约 -17.4 USDT）、-41.126332 GENIUS（约 -12.5 USDT）。手续费差额和实际净仓不同，因为策略账本本身另有小额残余。BNB 另有约 0.05593 个利息负债，估值约 39 USDT。未自动交易或还款。

修复方向：为 51042 增加账户级的新开仓退避/恢复机制，结合真实业务限流调整下单节奏；现货手续费需要进入净仓/对冲记账。此次仅诊断，没有部署这些代码或修改运行参数。

## 全部在审运行盘子

下表敞口是 Viz 已估值资产的绝对敞口合计，不能正负抵消；数值取 14:04 左右快照。Binance 04 正在交易，其瞬时敞口和挂单数会变化。

| 环境 | 总敞口 USDT | 杠杆 / 上限 | 交易所未成交 合约/现货 | Delta 最终状态 |
| --- | ---: | --- | --- | --- |
| binance_fr_arb02 | 18.33 | 1.857 / 2.100 | 0 / 0 | ON |
| binance_fr_arb03 | 171.86 | 1.665 / 1.700 | 0 / 0 | ON |
| binance_fr_arb04 | 518.29 | 1.680 / 1.700 | 5 / 40 | ON |
| bitget_fr_arb01 | 49.60 | 2.682 / 2.600 | 0 / 0 | ON |
| bitget_fr_arb02 | 41.20 | 2.676 / 2.600 | 0 / 0 | ON |
| bitget_fr_arb03 | 43.99 | 2.679 / 2.600 | 0 / 0 | ON |
| gate_fr_arb01 | 102.77 | 1.612 / 1.600 | 0 / 0 | ON |
| gate_fr_arb02 | 124.00 | 1.521 / 1.500 | 0 / 0 | ON |
| gate_fr_arb03 | 57.17 | 1.614 / 1.600 | 0 / 0 | ON |
| bybit_fr_arb01 | 24.87 | 1.673 / 1.600 | 0 / 0 | ON |

Gate 04 仅回读 Delta=ON，没有列入上表的全面核查。

## 其他已确认问题

1. Gate 01/02/03、Bitget 01/02/03、Bybit 01 的总杠杆均略高于各自开仓上限，日志持续拒绝新开仓；这是运行中的风控拦截。Gate 03 另有可用保证金约 1,545 USDT，低于 2,000 的账户开仓解锁阈值。
2. Gate 持仓有下列交易所风险限额超限。依据私有 position 的 abs(value) 与 risk_limit 比较；此次未调整杠杆或平仓。

| 环境 | 合约 | 当前名义价值 USDT | Gate risk_limit USDT |
| --- | --- | ---: | ---: |
| gate_fr_arb01 | ARIA_USDT | 46,908.98 | 20,000.00 |
| gate_fr_arb02 | MYX_USDT | 74,348.06 | 70,000.00 |
| gate_fr_arb02 | AIN_USDT | 12,117.26 | 5,000.00 |
| gate_fr_arb02 | GRIFFAIN_USDT | 74,557.81 | 70,000.00 |
| gate_fr_arb02 | IDOL_USDT | 146,816.93 | 70,000.00 |
| gate_fr_arb02 | ARIA_USDT | 39,748.82 | 20,000.00 |
| gate_fr_arb02 | TAG_USDT | 52,763.97 | 50,000.00 |

Gate 02 的 NAORIS 和 ON 还进入 risk_limit-2,000 USDT 的预留缓冲区，但未超过 risk_limit 本身。

3. Bitget 共用 Redis 键 bitget_position_tier_cache:USDT-FUTURES 不存在，三个 pre_trade 均持续报告 tier cache 刷新失败，未发现运行中的 bitget_position_tier_sidecar.py。刷新失败时保留已有内存状态；缺少币种限额的开仓路径会拒单，不能声称所有内存缓存已经失效。
4. Gate 02 BEAT 的策略账本差额约 +10.72 USDT，当前实际净多约 12.01 USDT；小于此前 ALPINE/ESPORTS，未扩大成大额未对冲仓。原因没有进一步逐笔回溯。
5. Bybit 有小额历史资产不在当前 Viz 策略资产行中，钱包 usdValue 合计约 47.04 USDT，最大单项 ZRO 约 4.90 USDT；未把这些当作零，也未自动清理。部分历史零碎币抵押开关关闭，当前主要持仓的抵押检查没有发现大额关闭项。
6. 独立 fr_monitor 没有常驻进程、18180 未监听；本次临时运行 --once --dry-run 完成检查，不发送通知。Bybit pre_trade 另报本地通知服务器连接被拒绝。没有启动任何常驻告警服务。

## 对账与检查边界

- 除 Binance 04 活跃交易时的瞬时差额外，采样交易所余额/持仓与 Viz 的已估值行没有超过 1 USDT 的偏差；没有发现新的 ALPINE 量级大额遗留敞口。
- Binance 04 初次查询 AVA 时约 100 USDT 的变化发生在非原子采样中；14:23:46 复核，AVA 实际显示净敞口 -0.95 USDT，策略账差约 -0.96 USDT，先前 100 USDT 未持续。
- Gate 合约数量统一使用 X-Gate-Size-Decimal: 1，并以实时 contract catalog 的 quanto_multiplier 转成基础币，未默认乘数为 1。
- 代表性行情检查覆盖 JP Binance/Gate/Bitget/OKX 现货和合约的 BTCUSDT、ETHUSDT，8 路均新鲜；这不等于逐个在线币种的行情完整性证明。
- 订单检查同时读取交易所当前挂单与现有 fr_monitor 的 RocksDB 最近窗口。fr_monitor 首次临时读取遭遇诊断进程 1,024 个 FD 软上限；仅提高此次诊断子进程 FD 上限后重跑成功，未发现窗口内订单停滞告警。它不能证明窗口以前的全部历史订单都已结束。
- Gate 05 已停止，交易所余额、持仓和挂单均为零。Binance 01 已停止，但原凭据查询返回 -2015，账户资金/持仓无法核实，未记成零。
- 一些钱包零碎币、股票代币没有 Viz 报价，保留在 reconciliation_summary.json 的 unpriced_assets 中。Gate 的 SLVON/TSLAX/CRCLX 及 Bybit 的 EUR/HFT 也触发集中度巡检缺价提示。

## 证据文件

- jp-meta-elvpn_audit.jsonl、sg_audit.jsonl：进程、快照、交易所账户/持仓/挂单和脱敏日志样本。
- reconciliation_summary.json：逐盘子数量对账和风险限额摘要。
- fr_monitor_once.json：提高本次诊断进程 FD 上限后的单次只读检查。
- followup_exchange_evidence.jsonl：Binance 03 手续费成交证据与 Gate 合约乘数。
- binance03_recent_diagnostics.jsonl、binance03_exchange_limits.jsonl：拒单统计、公开限额、账户交易限制指标和 Redis 参数。
- delta_final_summary.json、*_delta_enable.jsonl：全部 11 个账户 Delta 状态和变更回执。
- final_selected_snapshots.jsonl：14:23 最后复核。

## 后续核对：Bitget 方案与 Binance 03 私有 symbol 限额

原 Bitget 处理方案确实已存在：`scripts/lib/bitget_tier_pool.py` 维护共享环境池，
`start_bitget_position_tier_sidecar.sh` 启动 sidecar，分批查询交易所公开档位并写 Redis，
pre_trade 读取缓存并按杠杆选取 symbol 限额。2026-10-18 三套 FR 启动日志均为
active_symbols=34、cached_symbols=34、missing=0、leverage_fallbacks=1。

当前 Redis 的 pool 注册表与 USDT 缓存整个键均不存在；未发现 sidecar 活进程。
PM2 dump 中保留的是历史 online 配置，不能证明实际仍在运行；sidecar 最后日志时间为
2026-10-17。故此处为共享缓存生产链路中断，不能表述成只缺少几个 symbol。
进程刷新失败时保留旧内存快照，不能据 Redis 整键缺失断言所有运行中 symbol 的内存
限额也已经清空。本轮没有恢复 Bitget pool/sidecar 或重启 Bitget 交易进程。
证据：`bitget_cache_followup.json`。

Binance 03 进一步读取账户 `/papi/v1/um/symbolConfig`（942行）、positionRisk
（54行）和 leverageBracket（1078行）。AGLD 名义价值约 51,081.69 USDT，账户
symbol 上限 50,000；AWE 60,284.25 / 60,000；USUAL 50,043.22 / 50,000。
它们确实超出账户私有 symbol 限额，不能使用公开档位数百万 USDT 的最大值替代。
这些限仓问题与 51042 的具体触发条件不能直接画等号：AEVO 等尚有额度的 symbol
同样出现 51042。前文“频控”仅指交易所文本和本地未退避现象，具体业务规则仍未证实。
账户私有数据及逐 symbol 计算见
`../binance_fr_arb03_republish_20261103/account_symbol_limits.jsonl` 和
`../binance_fr_arb03_republish_20261103/position_limit_comparison.json`。
