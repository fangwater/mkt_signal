# 隔离核心分配登记(jp-meta-elvpn / sg)

最后更新:2026-08-16。**部署、迁移、下线任何绑核进程时,请同步更新本表。**

## jp-meta-elvpn(ip-172-31-35-228,c7i.metal-24xl)

CPU 布局:`0-5` housekeeping(承担全部 NIC IRQ、系统服务),`6-47` 隔离
(isolcpus/nohz_full/rcu_nocbs),`48-95` 超线程 sibling 已因 `nosmt=force` 下线。
详细内核参数见 `jp-meta-elvpn_hfq_low_latency_tuning_20260618.md`。

| 核 | 进程 | 备注 |
|----|------|------|
| 5 | spread_bbo_zmq_pub(binance-futures) | 绑在 housekeeping 边缘核 |
| 6 | fusion_factor_pub(binance-futures) | |
| 7 | model_1m_pub ×10 + model_pub ×3 | 13 个低频进程共享堆叠 |
| 8 | spread_pbs binance-margin | |
| 9 | spread_pbs binance-futures | 现为 full 角色;拆分后为 bookticker 专核 |
| 10 | spread_pbs gate-both | |
| 11 | spread_pbs bitget-both | 忙时单核饱和,候选拆分(margin/futures 两进程) |
| 12 | spread_pbs okex-both | |
| 13 | depth_pub okex-both | |
| 14 | depth_pub binance-both | |
| 15 | persist_manager ×6 | intra-arb01、fr_arb03/04、bitget_fr_arb02、gate_fr_arb01/02 堆叠 |
| 16 | account_monitor(binance-intra-arb01) | |
| 17 | trade_signal(binance-intra-arb01) | |
| 18 | pre_trade(binance-intra-arb01) | |
| 19 | trade_engine(binance-intra-arb01)主线程 | |
| 20 | trade_engine te-ipc 线程 | `--ipc-core 20`,busy-spin |
| 21 | period_pbs | |
| 22-23 | (空) | 预留:binance-intra-arb01 的 L3 组 buffer |
| **24** | **(空,已规划)** | **binance-futures spread_pbs market 角色,拆分待执行** |
| 25-26 | (空) | |
| 27 | account_monitor(binance_mm_alpha) | |
| 28 | trade_signal(binance_mm_alpha) | |
| 29 | pre_trade(binance_mm_alpha) | |
| 30 | trade_engine(binance_mm_alpha)主线程 | |
| 31 | trade_engine te-ipc 线程 | `--ipc-core 31` |
| 32-46 | (空) | 共 15 核 |
| 47 | pred_rnn_infer(model_pub predict_rnn_layer) | |

异常待查:

- `binance_mm_alpha/persist_manager` 启动参数为 `--core 21`,实际 affinity mask 是 `15`,
  与参数不符(与其它 persist_manager 堆叠在 15)。

未绑核、跑在 housekeeping 0-5 的交易/数据栈(截至本次盘点):
binance_fr_arb03/04、gate_fr_arb01/02、bitget_fr_arb02、okex_fr_arb01、
okex-intra-arb01 全套、trade_flow_feature ×8、rolling_metrics ×5、fusion_factor_1m、
persist_center、predict_file 及各类 viz/config/dashboard 服务。
其中 fr_arb / okex-intra 的 trade_engine 与 NIC IRQ 同核竞争,如在意其延迟可迁入空闲隔离核。

L3 说明:c7i.metal-24xl 的 L3 为全芯片共享(`shared_cpu_list=0-47`),
跨核没有 L3 惩罚;"8 核一组"的分组只是部署约定。

## sg(SSH: `sg`,ip-172-31-7-123,c7a.4xlarge,apse1-az3)

CPU 布局:`0-7` housekeeping(承担全部 NIC IRQ),`8-15` 隔离;AMD 实例无 SMT,全部为物理核。
主机调优记录见 `sg_hfq_low_latency_tuning_20260816.md`。

| 核 | 进程 | 备注 |
|----|------|------|
| 8 | spread_pbs bybit-both(market 角色) | trade/incremental/derivatives |
| 9 | spread_pbs bybit-both(bookticker 角色) | BBO 专核 |
| 10 | (空) | 唯一空闲隔离核 |
| 11 | account_monitor_bybit(bybit-intra-arb01) | |
| 12 | trade_signal(bybit-intra-arb01) | |
| 13 | pre_trade(bybit-intra-arb01) | |
| 14 | trade_engine(bybit-intra-arb01)主线程 | |
| 15 | trade_engine te-ipc 线程 | `--ipc-core 15`,busy-spin |

未绑核、跑在 housekeeping 0-7 的热路径进程(截至本次盘点):
mm_bybit_alpha 全套(trade_engine/trade_signal/pre_trade/account_monitor/persist_manager)、
bybit-intra-arb02 的 trade_signal/pre_trade/account_monitor、depth_pub、若干 persist_manager。
这些与全部 NIC IRQ 同在 0-7 竞争;隔离核只剩 1 个空位,如需整理绑核需先扩容实例或做取舍。
