# 隔离核心分配登记(jp-meta-elvpn / sg)

最后更新:2026-08-16。**部署、迁移、下线任何绑核进程时,请同步更新本表。**

## jp-meta-elvpn(ip-172-31-35-228,c7i.metal-24xl)

CPU 布局:`0-5` housekeeping(OS、SSH、PM2、系统服务),`6-47` 隔离
(isolcpus/nohz_full/rcu_nocbs),`48-95` 超线程 sibling 已因 `nosmt=force` 下线。
内核默认 `irqaffinity=0-5`,irqbalance 关闭;现场 NIC 数据面 IRQ **不**落在 0-5,
而是一卡一核(见下表 46/47)。ENA 管理中断(`ena-mgmnt`)仍在 `0-5`。
详细内核参数见 `jp-meta-elvpn_hfq_low_latency_tuning_20260618.md`。

| 核 | 进程 | 备注 |
|----|------|------|
| 5 | spread_bbo_zmq_pub(binance-futures) | 绑在 housekeeping 边缘核 |
| 6 | fusion_factor_pub(binance-futures) | |
| 7 | model_1m_pub ×10 + model_pub ×3 | 13 个低频进程共享堆叠 |
| 8 | spread_pbs binance-margin | |
| 9 | spread_pbs binance-futures bookticker | BBO 专核 |
| 10 | spread_pbs gate-both | |
| 11 | spread_pbs bitget-both | 忙时单核饱和,候选拆分(margin/futures 两进程) |
| 12 | spread_pbs okex-both | |
| 13 | depth_pub_general | 本机 8 路 depth25（BN/OKX/Bitget/Gate × margin+futures）；**不含 Bybit**（Bybit 在 sg）。pm2 `dp_general` |
| 14 | spread_pbs binance-futures market | 原 depth_pub binance-both 腾出；trade/incremental/derivatives |
| 15 | persist_manager ×N | intra-arb01、okex_mm_alpha、fr_arb03/04、bitget_fr_arb02、gate_fr_arb01/02 等堆叠 |
| 16 | account_monitor(binance-intra-arb01) | |
| 17 | trade_signal(binance-intra-arb01) | |
| 18 | pre_trade(binance-intra-arb01) | |
| 19 | trade_engine(binance-intra-arb01) | 单线程，只需一核；`TRADE_ENGINE_IPC_CORE` 已废弃 |
| 20 | account_monitor(okex_mm_alpha) | 公共进程之后第一套是 binance-intra；okex MM 从 20 起 |
| 21 | trade_signal(okex_mm_alpha) | |
| 22 | pre_trade(okex_mm_alpha) | |
| 23 | trade_engine(okex_mm_alpha) | 单线程 |
| 24-26 | (空) | |
| 27 | account_monitor(binance_mm_alpha) | |
| 28 | trade_signal(binance_mm_alpha) | |
| 29 | pre_trade(binance_mm_alpha) | |
| 30 | trade_engine(binance_mm_alpha) | 单线程；原 31 号 te-ipc 核已回收 |
| 31-45 | (空) | |
| 46 | NIC IRQ: ens41 全部 Tx-Rx 队列(16) | 默认路由/主网卡;禁止再绑用户进程 |
| 47 | NIC IRQ: ens42 全部 Tx-Rx 队列(16) | 第二块网卡;禁止再绑用户进程。原 pred_rnn_infer 已下线 |

异常待查:

- `binance_mm_alpha/persist_manager` 启动参数为 `--core 21`,实际 affinity mask 是 `15`,
  与参数不符(与其它 persist_manager 堆叠在 15)。

未绑核、跑在 housekeeping 0-5 的交易/数据栈(截至本次盘点):
binance_fr_arb03/04、gate_fr_arb01/02、bitget_fr_arb02、okex_fr_arb01、
okex-intra-arb01 全套、trade_flow_feature ×8、rolling_metrics ×5、fusion_factor_1m、
persist_center、predict_file 及各类 viz/config/dashboard 服务。
`okex_mm_alpha` 的 persist_manager 与其它 persist 一起堆叠在 15。
其中 fr_arb / okex-intra 的 trade_engine 与 housekeeping 上的系统服务同核,
数据面 NIC IRQ 已迁到 46/47,不再与它们抢硬中断。如在意调度抖动仍可迁入空闲隔离核。

NIC IRQ 策略(jp-meta,2026-08-16 现场):

- 一网卡一核:`ens41` → 46,`ens42` → 47;每卡 16 条 combined 队列的 `smp_affinity` 全部钉在该核。
  隔离段末尾两核专放 IRQ;systemd `pin-aws-ena-irq@ens41/ens42`。
- 目的:把硬中断/NAPI 从 housekeeping 和交易核清出去,两卡互不抢同一 IRQ 核。
- 约束:46/47 只做 IRQ,不跑 spread/trade/persist。队列数未按核裁剪,RSS 在单核上串行;
  是否够用看该核 `%soft`/ksoftirqd,而不是看队列个数。busy_poll 收包时 IRQ 核 CPU 可以很低
  (中断仍在响,包已被用户态抽走)。

L3 说明:c7i.metal-24xl 的 L3 为全芯片共享(`shared_cpu_list=0-47`),
跨核没有 L3 惩罚;"8 核一组"的分组只是部署约定。

## sg(SSH: `sg`,ip-172-31-7-123,c7a.4xlarge,apse1-az3)

CPU 布局:`0-7` housekeeping(承担全部 NIC IRQ),`8-15` 隔离;AMD 实例无 SMT,全部为物理核。
主机调优记录见 `sg_hfq_low_latency_tuning_20260816.md`。

| 核 | 进程 | 备注 |
|----|------|------|
| 8 | spread_pbs bybit-both(market 角色) | trade/incremental/derivatives |
| 9 | spread_pbs bybit-both(bookticker 角色) | BBO 专核 |
| 10 | (空) | 空闲隔离核 |
| 11 | account_monitor_bybit(bybit-intra-arb01) | |
| 12 | trade_signal(bybit-intra-arb01) | |
| 13 | pre_trade(bybit-intra-arb01) | |
| 14 | trade_engine(bybit-intra-arb01) | 单线程，只需一核；`TRADE_ENGINE_IPC_CORE` 已废弃 |
| 15 | (空) | 原 te-ipc 核已回收 |

未绑核、跑在 housekeeping 0-7 的热路径进程(截至本次盘点):
mm_bybit_alpha 全套(trade_engine/trade_signal/pre_trade/account_monitor/persist_manager)、
bybit-intra-arb02 的 trade_signal/pre_trade/account_monitor、depth_pub、若干 persist_manager。
这些与全部 NIC IRQ 同在 0-7 竞争;隔离核现空 10 与 15,如需整理绑核可先用这两核,再考虑扩容。
