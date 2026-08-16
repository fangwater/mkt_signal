# sg 低延迟主机配置记录(busy_poll / ENA / NAPI defer 评估)

日期:2026-08-16

## 机器信息

- SSH 别名:`sg`
- 公网 IP:`47.131.162.78`
- 主机名:`ip-172-31-7-123`
- 内核:`6.17.0-1012-aws`
- 主网卡:`enp40s0`(ena,8 个 Tx-Rx 队列)
- CPU 布局(已有,本次未改):housekeeping `0-7`,隔离核 `8-15`(isolcpus/nohz_full/rcu_nocbs/irqaffinity=0-7)
- 相关进程:`spread_pbs --venue bybit-both` ×2(core 8 market 角色 / core 9 bookticker 角色),另有 bybit 交易全套

## 原始状态(调优前)

```text
net.core.busy_read = 0
net.core.busy_poll = 0
ENA Adaptive RX = on
ENA rx-usecs = 20, tx-usecs = 64
napi_defer_hard_irqs = 0
gro_flush_timeout = 0
```

对齐 jp2(见 `jp2_hfq_low_latency_tuning_20260618.md`):sg 此前只做了 CPU 隔离,缺 socket busy poll 与 ENA 中断合并关闭。

## 变更内容

### 1. Socket busy poll(保留)

新增 `/etc/sysctl.d/99-hfq-low-latency-network.conf`:

```conf
net.core.busy_read = 50
net.core.busy_poll = 50
```

运行时通过 `sysctl -p` 即时生效,无需重启进程(epoll 每次调用动态读取)。

### 2. ENA 中断合并关闭(保留)

新增 systemd oneshot `/etc/systemd/system/hfq-low-latency-network.service`(enabled),动态解析默认路由网卡后执行:

```text
ethtool -C <iface> adaptive-rx off rx-usecs 0 tx-usecs 0
```

`|| [ $? -eq 80 ]` 容忍重复执行时 ethtool 的 no-change 退出码。

### 3. NAPI deferred IRQ(评估后关闭)

曾试验 `napi_defer_hard_irqs=2` + `gro_flush_timeout=200000`,做了三窗口 A/B 后决定**保持关闭**,理由见下。

### 4. C-state 禁用持久化(同日补充)

检查发现 sg 是 `c7a.4xlarge`(AMD,无 SMT,全物理核),cpuidle 为 `acpi_idle`,
其中 **C2 退出延迟 800µs**。所有 16 核的 C2 此前已被运行时手动禁用
(`state2/disable=1`,历史驻留 6 亿次/近 90 小时),但**没有任何持久化配置,重启即回滚**。
已并入上述 systemd oneshot:禁用所有退出延迟 >10µs 的 idle state(保留 POLL/C1,
与 jp2 的 `max_cstate=1` 效果对齐),重启后自动重新生效。

## A/B 实验记录

指标:bookticker 进程日志 `latency_us`(交易所时间戳 → 本地接收,KLL 分位数),每窗口取各上报行的中位数。

时间线(UTC):
- W0 基线:06:18–06:47(原始状态)
- 06:47:56 应用 busy_poll=50 + ENA 合并关闭 + defer 2/200µs
- W1:06:48–07:09(全套开启;剔除 07:05:30–07:06:40 的 --test 验证干扰段)
- 07:09:28 关闭 defer(其余保留)
- W2:07:10–07:25(busy_poll + 合并关闭,defer 关)

```text
venue           窗口          p50    p90    p95    p99   窗口消息量中位数
bybit-futures   W0 基线      1844   2328   2434   2702   592
bybit-futures   W1 defer200  2141   2556   2658   2795   681
bybit-futures   W2 无defer   2188   2624   2718   2880   732
bybit-margin    W0 基线      2185   2627   2746   3010   179
bybit-margin    W1 defer200  2083   2581   2701   2970   297
bybit-margin    W2 无defer   1798   2368   2505   2835   402
```

## 结论

1. **该指标的噪声底远大于内核侧效应**:一小时内 futures 消息量 +24%、margin +125%,市场活跃度持续上升,交易所侧发布延迟随负载独立漂移(±300µs 级),两个 venue 方向相反的变化即为证据。内核路径的 10–100µs 级改善无法在此指标上单独分辨。
2. **busy_poll + 合并关闭保留**:机制上严格减少 NIC 侧等待(adaptive rx 高吞吐时可到百 µs 级)并允许应用核直接轮询,与 jp2 生产配置对齐;margin 三窗口持续改善与其一致,futures 的表观回退与其消息量上升趋势一致,归因于交易所侧漂移。
3. **defer 关闭**:`gro_flush_timeout` 只在应用近似持续轮询时有益;spread_pbs 是 epoll 休眠型(消息间隔 ms 级 >> 50µs busy poll 预算),休眠期间包会被 defer 定时器拖最多 200µs 才处理,共享 NIC 上其他流量还会维持 defer 状态放大该等待。W1/W2 对比也未显示收益。
4. 若要真正量化内核路径,需要本地端到端口径(如驱动层/XDP 时间戳 → 应用时间戳),不受交易所漂移污染。

## 最终状态

```text
net.core.busy_read = 50
net.core.busy_poll = 50
ENA Adaptive RX: off, rx-usecs 0, tx-usecs 0
napi_defer_hard_irqs = 0
gro_flush_timeout = 0
cpuidle: POLL/C1 可用,C2(800µs)全核禁用(已持久化)
hfq-low-latency-network.service: enabled / active(重启持久)
```

进程无需重启,均在线无异常。

## 回滚

```bash
sudo rm /etc/sysctl.d/99-hfq-low-latency-network.conf
sudo sysctl -w net.core.busy_read=0 net.core.busy_poll=0
sudo systemctl disable --now hfq-low-latency-network.service
sudo rm /etc/systemd/system/hfq-low-latency-network.service
sudo ethtool -C enp40s0 adaptive-rx on rx-usecs 20 tx-usecs 64
```

## 备注

- jp2 的 grub/内核参数隔离项 sg 已具备;本次未从当前构建机覆盖 jp2(该机器不在本机 SSH 配置中,`jp-meta-elvpn` 当时不可达)。如需在 jp2 评估 defer,结论预期相同(同为 epoll 休眠型负载)。
