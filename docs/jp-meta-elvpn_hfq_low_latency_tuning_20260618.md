# jp-meta-elvpn 低延迟主机配置记录

日期：2026-06-18

> 2026-08-16 更新：`net.core.busy_poll` 由 50 调整为 200（`/etc/sysctl.d/99-hfq-low-latency-network.conf`），
> 让 epoll 自旋窗口覆盖静默后首条消息、消除中断唤醒链（约 5-15µs）。
> 实测 spread_pbs 静默期约 70% 时间在睡眠，代价是隔离核静默期 CPU 升至 100%。
> 当前各核分配见 `core_allocation.md`。

## 机器信息

- SSH 别名：`jp-meta-elvpn`
- 公网 IP：`13.115.227.29`
- 登录用户：`ubuntu`
- 主机名：`ip-172-31-35-228`
- 内核：`6.17.0-1017-aws`
- 主网卡：`ens41`
- 网卡驱动：`ena`

## 机器原始状态

这台机器是单 socket、48 个物理核、96 个逻辑 CPU 的实例，原始状态开启了超线程：

```text
CPU(s):                 96
On-line CPU(s) list:    0-95
Thread(s) per core:     2
Core(s) per socket:     48
Socket(s):              1
SMT control:            on
SMT active:             1
```

原始启动参数没有 CPU 隔离：

```text
BOOT_IMAGE=/vmlinuz-6.17.0-1017-aws root=PARTUUID=fe814a98-f7fd-4c3d-9642-c18fe64c6e5d ro console=tty1 console=ttyS0 nvme_core.io_timeout=4294967295 panic=-1
```

原始中断、网卡和低延迟相关状态：

```text
/proc/irq/default_smp_affinity = ffffffff,ffffffff,ffffffff
irqbalance = enabled / active
net.core.busy_read = 0
net.core.busy_poll = 0
ENA Adaptive RX = on
ENA rx-usecs = 20
ENA tx-usecs = 64
intel_idle.max_cstate = 9
cpuidle states 包含 POLL、C1、C1E、C6
```

## 配置目标

参考当前 HFQ 机器的低延迟配置思路，对 `jp-meta-elvpn` 做成交易专用机器：

- 系统、SSH、IRQ、RCU 回调、PM2、Redis/Nginx、dashboard 等只跑 housekeeping 核。
- 交易热路径进程显式绑定到隔离核。
- 关闭超线程，避免 sibling CPU 互相干扰。
- 禁止深 C-state，降低唤醒延迟。
- 网卡关闭 ENA adaptive interrupt moderation，降低包到达延迟。
- 开启 socket busy poll，降低 receive/poll 路径延迟。

## CPU 和 GRUB 修改

最终写入：

```text
/etc/default/grub.d/99-cpu-isolation.cfg
```

内容：

```bash
# Managed CPU isolation for jp-meta-elvpn HFQ-style trading host with SMT disabled.
# Housekeeping: 0-5 (OS, IRQs, RCU callbacks, SSH, PM2, Redis/Nginx, dashboards)
# Isolated: 6-47 (market-data and execution hot paths; pin processes explicitly)
# SMT: disabled via nosmt=force, so sibling CPUs 48-95 are offline after reboot.
GRUB_CMDLINE_LINUX_DEFAULT="$GRUB_CMDLINE_LINUX_DEFAULT nosmt=force isolcpus=nohz,domain,managed_irq,6-47 nohz_full=6-47 rcu_nocbs=6-47 irqaffinity=0-5 kthread_cpus=0-5 intel_idle.max_cstate=1 processor.max_cstate=1"
```

执行过：

```bash
sudo update-grub
sudo reboot
```

说明：

- `nosmt=force`：关闭超线程，重启后 `48-95` 这些 sibling CPU 下线。
- `isolcpus=nohz,domain,managed_irq,6-47`：隔离交易核 `6-47`。
- `nohz_full=6-47`：隔离核进入 full dynticks。
- `rcu_nocbs=6-47`：隔离核的 RCU callback offload 到 housekeeping 核。
- `irqaffinity=0-5`：默认中断只放到 housekeeping 核。
- `kthread_cpus=0-5`：内核线程默认限制到 housekeeping 核。
- `intel_idle.max_cstate=1 processor.max_cstate=1`：只允许浅 C-state，避免 C1E/C6 这类深睡眠。

最终 CPU 布局：

```text
0-5   housekeeping
6-47  交易隔离核，需要业务进程显式 taskset 绑定
48-95 超线程 sibling，因 nosmt=force 下线
```

## IRQ 修改

关闭 `irqbalance`：

```text
irqbalance = disabled / inactive
```

最终默认 IRQ mask：

```text
/proc/irq/default_smp_affinity = 00000000,00000000,0000003f
```

`0000003f` 对应 CPU `0-5`。

这是内核**默认** affinity(新中断、`ena-mgmnt`)。数据面 `ens41`/`ens42` 的 Tx-Rx 队列
后续钉到隔离核 **46 / 47**(一卡一核,全部队列同核),登记见 `docs/core_allocation.md`。
不要把 46/47 再分配给用户进程;也不要把数据面 IRQ 改回 0-5,以免和 housekeeping 抢核。

## 超线程修改

运行时先关闭过 SMT：

```bash
echo off | sudo tee /sys/devices/system/cpu/smt/control
```

随后通过 GRUB 持久化：

```text
nosmt=force
```

最终验证：

```text
On-line CPU(s) list: 0-47
Thread(s) per core: 1
smt_control: forceoff
smt_active: 0
```

## C-State 修改

基于这台机器原始 `intel_idle.max_cstate=9`、可进入 `C1E/C6` 的状态，改为限制深 C-state：

```text
intel_idle.max_cstate=1
processor.max_cstate=1
```

最终验证：

```text
/sys/module/intel_idle/parameters/max_cstate = 1
cpu0 cpuidle states: POLL, C1
```

这次没有使用 `idle=poll`，避免空闲核一直忙等导致监控 CPU 接近满载、功耗和温度余量变差。

## Socket Busy Poll 修改

新增：

```text
/etc/sysctl.d/99-hfq-low-latency-network.conf
```

内容：

```conf
# HFQ low-latency socket polling. Increases CPU work to reduce receive latency.
net.core.busy_read = 50
net.core.busy_poll = 50
```

最终验证：

```text
net.core.busy_read = 50
net.core.busy_poll = 50
```

这个配置会增加 CPU 工作量，换取 socket receive/poll 路径的更低延迟。

## ENA 网卡修改

这台机器主网卡是：

```text
iface = ens41
driver = ena
```

原始 ENA interrupt moderation：

```text
Adaptive RX: on
rx-usecs: 20
tx-usecs: 64
```

新增 systemd oneshot：

```text
/etc/systemd/system/hfq-low-latency-network.service
```

内容：

```ini
[Unit]
Description=HFQ low-latency ENA coalescing tuning
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -lc 'set -euo pipefail; iface=$(/usr/sbin/ip -o route get 1.1.1.1 | /usr/bin/awk '\''{for (i=1; i<=NF; i++) if ($i == "dev") {print $(i+1); exit}}'\''); /usr/sbin/ethtool -C "$iface" adaptive-rx off rx-usecs 0 tx-usecs 0'

[Install]
WantedBy=multi-user.target
```

服务状态：

```text
hfq-low-latency-network.service = enabled / active
```

最终验证：

```text
Adaptive RX: off
rx-usecs: 0
tx-usecs: 0
```

这个配置降低网卡中断合并带来的等待时间，代价是中断频率和 CPU 压力会上升。

## 最终验证结果

最终 `/proc/cmdline`：

```text
BOOT_IMAGE=/vmlinuz-6.17.0-1017-aws root=PARTUUID=fe814a98-f7fd-4c3d-9642-c18fe64c6e5d ro console=tty1 console=ttyS0 nvme_core.io_timeout=4294967295 nosmt=force isolcpus=nohz,domain,managed_irq,6-47 nohz_full=6-47 rcu_nocbs=6-47 irqaffinity=0-5 kthread_cpus=0-5 intel_idle.max_cstate=1 processor.max_cstate=1 panic=-1
```

最终运行态：

```text
CPU(s):                 96
On-line CPU(s) list:    0-47
Thread(s) per core:     1
Core(s) per socket:     48
Socket(s):              1
smt_control:            forceoff
smt_active:             0
nohz_full:              6-47
isolated:               6-47
default IRQ mask:       00000000,00000000,0000003f
irqbalance:             disabled / inactive
intel_idle.max_cstate:  1
busy_read:              50
busy_poll:              50
ENA Adaptive RX:        off
ENA rx-usecs:           0
ENA tx-usecs:           0
```

注意：普通 SSH session 默认只允许 housekeeping 核，所以 `nproc` 返回 `6`。这是预期行为。业务进程需要显式绑定到 `6-47`，例如：

```bash
taskset -c 6 <cmd>
taskset -c 47 <cmd>
```

## 回滚点

如果需要回滚：

1. 编辑或删除 `/etc/default/grub.d/99-cpu-isolation.cfg`。
2. 执行 `sudo update-grub`。
3. 删除或修改 `/etc/sysctl.d/99-hfq-low-latency-network.conf`。
4. 停用 ENA tuning service：

```bash
sudo systemctl disable --now hfq-low-latency-network.service
```

5. 如需恢复自动中断调度：

```bash
sudo systemctl enable --now irqbalance.service
```

6. 重启机器。

每次覆盖 `/etc/default/grub.d/99-cpu-isolation.cfg` 前都创建过时间戳备份：

```text
/etc/default/grub.d/99-cpu-isolation.cfg.bak.<UTC timestamp>
```

## 行情和特征服务部署记录

同日继续在 `jp-meta-elvpn` 上部署行情和特征相关服务。部署顺序为：先启动 `spread_pbs` 行情，再启动三组 `rolling_metrics`，最后启动六个 `trade_flow_feature_pub`。

### 工具链

远端原始状态没有 `node`、`npm`、`pm2`、`pmdaemon`、`cargo`、`rustc`。

已安装：

```text
nvm: v0.40.5
node: v26.3.1
npm: 11.16.0
pm2: 7.0.1
pmdaemon: 0.1.4
```

安装过程中 `node v26.3.1` 需要 `libatomic.so.1`，因此额外安装了系统包：

```text
libatomic1
redis-server
redis-tools
```

`node`、`npm`、`npx`、`pm2`、`pmdaemon` 已放到 `/usr/local/bin` 或软链到 `/usr/local/bin`，保证非交互 SSH 和启动脚本默认 PATH 可直接使用。

Redis 作为 `rolling_metrics` 的配置和输出依赖，已安装并启用：

```text
redis-server: 7.0.15
redis-server.service: active
redis-cli ping: PONG
```

### spread_pbs

部署目录：

```text
/home/ubuntu/spread_pbs/binance-margin
/home/ubuntu/spread_pbs/binance-futures
/home/ubuntu/spread_pbs/gate-both
/home/ubuntu/spread_pbs/bitget-both
/home/ubuntu/spread_pbs/config
```

部署文件包括：

- `spread_pbs` release binary
- `scripts/start_spread_pbs.sh`
- `scripts/stop_spread_pbs.sh`
- `config/mkt_cfg.yaml`
- `config/iceoryx2.toml`
- 每个 venue 自己的 `env.sh`

core 映射：

```text
binance-margin   -> CPU 8
binance-futures  -> CPU 9
gate-both        -> CPU 10
bitget-both      -> CPU 11
```

对应 `env.sh` 中写入：

```bash
export SPREAD_PBS_CORE='<core>'
export PATH="$HOME/.local/bin:$PATH"
```

已启动的 pmdaemon 进程：

```text
spp_bn_mg
spp_bn_fu
spp_gt_bo
spp_bg_bo
```

最终验证：

```text
PID   PSR  CPU   COMM        ARGS
6426  8    11.3  spread_pbs  /home/ubuntu/spread_pbs/binance-margin/spread_pbs --venue binance-margin --core 8
6497  9    34.8  spread_pbs  /home/ubuntu/spread_pbs/binance-futures/spread_pbs --venue binance-futures --core 9
6579  10   42.9  spread_pbs  /home/ubuntu/spread_pbs/gate-both/spread_pbs --venue gate-both --core 10
6664  11   25.8  spread_pbs  /home/ubuntu/spread_pbs/bitget-both/spread_pbs --venue bitget-both --core 11
```

进程实际 affinity：

```text
binance-margin   allowed=8
binance-futures  allowed=9
gate-both        allowed=10
bitget-both      allowed=11
```

启动日志确认 Binance、Gate、Bitget websocket 已连接，`spread_pbs` IPC publisher 已创建。

### rolling_metrics

部署目录：

```text
/home/ubuntu/rolling_metrics/binance-margin-binance-futures
/home/ubuntu/rolling_metrics/gate-margin-gate-futures
/home/ubuntu/rolling_metrics/bitget-margin-bitget-futures
/home/ubuntu/rolling_metrics/bitget-futures-gate-futures
```

每个目录包含：

- `rolling_metrics` release binary
- `scripts/rolling_metrics/start_rolling_metrics.sh`
- `scripts/rolling_metrics/stop_rolling_metrics.sh`
- `scripts/rolling_metrics/print_rolling_metrics_thresholds.py`
- `scripts/rolling_metrics/print_rolling_metrics_params.py`
- `scripts/rolling_metrics/sync_rolling_metrics_params.py`
- `scripts/process_match_lib.sh`
- `env.sh`

`env.sh` 说明：

```bash
# No taskset here: rolling_metrics runs on housekeeping/default affinity unless explicitly launched otherwise.
export PATH="$HOME/.local/bin:$PATH"
```

按当前要求，`rolling_metrics` 跑在 housekeeping/default affinity，不占用 `6-47` 中明确给行情和交易热路径使用的隔离核。

这次从当前基准机同步了 `/home/ubuntu/rolling_metrics` 的脚本、文档和运行目录形态；远端保留重新编译部署的新版 `rolling_metrics` 二进制。Redis 中同步了当前基准机的全部 rolling 参数：

```text
rolling_metrics_params_binance-margin_binance-futures
rolling_metrics_params_gate-margin_gate-futures
rolling_metrics_params_bitget-margin_bitget-futures
rolling_metrics_params_bitget-futures_gate-futures
```

当前启动的三组同所期现 rolling：

```text
rm_bn_mg_bn_fu  /home/ubuntu/rolling_metrics/binance-margin-binance-futures/rolling_metrics --open-venue binance-margin --hedge-venue binance-futures
rm_gt_mg_gt_fu  /home/ubuntu/rolling_metrics/gate-margin-gate-futures/rolling_metrics --open-venue gate-margin --hedge-venue gate-futures
rm_bg_mg_bg_fu  /home/ubuntu/rolling_metrics/bitget-margin-bitget-futures/rolling_metrics --open-venue bitget-margin --hedge-venue bitget-futures
```

最终验证时三组均 online，并已写入 thresholds：

```text
rolling_metrics_thresholds_binance-margin_binance-futures 621 fields
rolling_metrics_thresholds_gate-margin_gate-futures        536 fields
rolling_metrics_thresholds_bitget-margin_bitget-futures   208 fields
```

同时程序会写同 exchange futures-futures 输出 key：

```text
rolling_metrics_thresholds_binance-futures_binance-futures
rolling_metrics_thresholds_gate-futures_gate-futures
rolling_metrics_thresholds_bitget-futures_bitget-futures
```

### trade_flow_feature_pub

部署目录：

```text
/home/ubuntu/trade_flow_feature/binance-margin
/home/ubuntu/trade_flow_feature/binance-futures
/home/ubuntu/trade_flow_feature/gate-margin
/home/ubuntu/trade_flow_feature/gate-futures
/home/ubuntu/trade_flow_feature/bitget-margin
/home/ubuntu/trade_flow_feature/bitget-futures
```

每个目录包含：

- `trade_flow_feature_pub` release binary
- `scripts/start_trade_flow_feature_pub.sh`
- `scripts/stop_trade_flow_feature_pub.sh`
- `scripts/process_match_lib.sh`
- `scripts/print_trade_flow_thresholds.py`
- `config/trade_flow_feature_pub.yaml`
- `config/iceoryx2.toml`
- `env.sh`

`env.sh` 说明：

```bash
# No taskset here: trade_flow_feature_pub runs on housekeeping/default affinity unless explicitly launched otherwise.
export PATH="$HOME/.local/bin:$PATH"
```

按当前要求，`trade_flow_feature_pub` 后续如果启动，默认跑在 housekeeping/default affinity。

这台机器没有部署 `depth_pub`，因此最初使用 `depth_channel: "depth25"` 启动会因为找不到 `depth_pubs/<venue>/depth25` IPC service 而退出。已按当前要求把六个 flow 配置都改为 `none` 模式：

```yaml
depth_channel: "none"
```

日志确认进入 `vol-only mode`，发布服务为 `factor_pub/<venue>/rl_vol`，不再依赖 depth channel。

当前启动的六个 flow：

```text
tff_bn_mg  /home/ubuntu/trade_flow_feature/binance-margin/trade_flow_feature_pub --venue binance-margin
tff_bn_fu  /home/ubuntu/trade_flow_feature/binance-futures/trade_flow_feature_pub --venue binance-futures
tff_gt_mg  /home/ubuntu/trade_flow_feature/gate-margin/trade_flow_feature_pub --venue gate-margin
tff_gt_fu  /home/ubuntu/trade_flow_feature/gate-futures/trade_flow_feature_pub --venue gate-futures
tff_bg_mg  /home/ubuntu/trade_flow_feature/bitget-margin/trade_flow_feature_pub --venue bitget-margin
tff_bg_fu  /home/ubuntu/trade_flow_feature/bitget-futures/trade_flow_feature_pub --venue bitget-futures
```

最终验证六个 flow 均 online，并持续输出 `publish_outcome_10s`，`rl_success` 增长、`rl_fail=0`。

### 最终服务状态快照

最终检查时间：`2026-06-18T10:12:55Z`

```text
spread_pbs:
  spp_bn_mg  PID 6426   PSR 8   /home/ubuntu/spread_pbs/binance-margin/spread_pbs --venue binance-margin --core 8
  spp_bn_fu  PID 6497   PSR 9   /home/ubuntu/spread_pbs/binance-futures/spread_pbs --venue binance-futures --core 9
  spp_gt_bo  PID 6579   PSR 10  /home/ubuntu/spread_pbs/gate-both/spread_pbs --venue gate-both --core 10
  spp_bg_bo  PID 6664   PSR 11  /home/ubuntu/spread_pbs/bitget-both/spread_pbs --venue bitget-both --core 11

rolling_metrics:
  rm_bn_mg_bn_fu  PID 11866  housekeeping/default affinity
  rm_gt_mg_gt_fu  PID 11977  housekeeping/default affinity
  rm_bg_mg_bg_fu  PID 12087  housekeeping/default affinity

trade_flow_feature_pub:
  tff_bn_mg  PID 9946   housekeeping/default affinity, depth_channel=none
  tff_bn_fu  PID 10031  housekeeping/default affinity, depth_channel=none
  tff_gt_mg  PID 10116  housekeeping/default affinity, depth_channel=none
  tff_gt_fu  PID 10201  housekeeping/default affinity, depth_channel=none
  tff_bg_mg  PID 10286  housekeeping/default affinity, depth_channel=none
  tff_bg_fu  PID 10371  housekeeping/default affinity, depth_channel=none
```

## Nginx 部署记录

为后续部署 `binance-intra-arb01`，按当前基准机的同名环境配置了最小 nginx 反代和 stream 转发。此步骤只部署 nginx，不启动 `binance-intra-arb01` 交易进程。

已安装：

```text
nginx: 1.24.0
libnginx-mod-stream: 1.24.0
```

HTTP 反代监听：

```text
0.0.0.0:4191
```

映射文件：

```text
/home/ubuntu/nginx_locations.txt
```

内容按本机 `binance-intra-arb01` 对齐：

```text
/intra/binance-intra-arb01/config   -> http://127.0.0.1:19171/
/intra/binance-intra-arb01/         -> static:$HOME/binance-intra-arb01/www/
/intra/binance-intra-arb01/ws       -> http://127.0.0.1:10180/ws
/intra/binance-intra-arb01/healthz  -> http://127.0.0.1:10180/healthz
/intra/binance-intra-arb01/snapshot -> http://127.0.0.1:10180/snapshot
```

生成的 nginx 配置：

```text
/etc/nginx/sites-available/crypto_proxy_4191.conf
/etc/nginx/sites-enabled/crypto_proxy_4191.conf
```

TCP stream 转发监听：

```text
0.0.0.0:4190      -> 127.0.0.1:6379
127.0.0.1:6342   -> 127.0.0.1:50042
```

其中 `6342 -> 50042` 是为 `binance-intra-arb01` 的 `persist_manager` sync source 预留，和当前基准机一致；它只监听本机回环地址。

stream 映射文件：

```text
/home/ubuntu/nginx_streams.txt
```

生成的 stream 配置：

```text
/etc/nginx/stream.conf
/etc/nginx/stream-enabled/crypto_proxy_stream_4190.conf
```

最终验证：

```text
sudo nginx -t: successful
nginx.service: active
listening: 0.0.0.0:4191
listening: 0.0.0.0:4190
listening: 127.0.0.1:6342
nginx Cpus_allowed_list: 0-5,48-95
```

## Tlen Server 部署记录

按本机当前运行方式，在 `jp-meta-elvpn` 上部署共享 `tlen_config_server`。此步骤只部署配置服务和同步 Redis 配置数据，不启动或修改交易进程。

本机基准：

```text
PM2 name: tlen_config_server_shared
namespace: mkt_signal
cwd: /home/ubuntu/crypto_mkt/mkt_signal
script: /home/ubuntu/crypto_mkt/mkt_signal/scripts/tlen_config_server.py
bind: 0.0.0.0:6322
default_venue: binance-futures
redis: 127.0.0.1:6379/0
```

`jp-meta-elvpn` 部署结果：

```text
dir: /home/ubuntu/tlen_config_shared
config: /home/ubuntu/tlen_config_shared/config/tlen_config_server.env
script: /home/ubuntu/tlen_config_shared/scripts/tlen_config_server.py
PM2 name: tlen_config_server_shared
namespace: tlen_config_shared
status: online
pid: 15492
bind: 0.0.0.0:6322
default_venue: binance-futures
redis: 127.0.0.1:6379/0
python redis package: python3-redis 4.3.4
```

PM2 已执行 `pm2 save`，当前进程列表保存到：

```text
/home/ubuntu/.pm2/dump.pm2
```

同步到 `jp-meta-elvpn` 的 Redis 配置范围：

```text
*:tlen_threshold
*:amount-thresholds
*:factor-plan
*:zscore
```

同步后 key/field 统计：

```text
binance-futures:amount-thresholds   fields=100
binance-futures:factor-plan         fields=100
binance-futures:zscore              fields=1
binance-margin:amount-thresholds    fields=100
binance-margin:zscore               fields=1
binance_futures:tlen_threshold      fields=62
binance_margin:tlen_threshold       fields=360
bitget-futures:amount-thresholds    fields=100
bitget-futures:zscore               fields=1
bitget-margin:amount-thresholds     fields=100
bitget_futures:tlen_threshold       fields=62
bitget_margin:tlen_threshold        fields=328
gate-futures:amount-thresholds      fields=100
gate-margin:amount-thresholds       fields=100
gate-margin:zscore                  fields=1
gate_futures:tlen_threshold         fields=62
gate_margin:tlen_threshold          fields=62
```

本机与 `jp-meta-elvpn` 对上述 Redis hash 做规范化 JSON 后的校验值一致：

```text
sha256: f3e3bb6a2d0075c863b9f29aabc9ab7df3407a994941e4a802e4792ed293167d
```

nginx `4191` 新增映射：

```text
/shared/tlen_config_shared/tlen  -> http://127.0.0.1:6322/
/api/                            -> http://127.0.0.1:6322/api/
/healthz                         -> http://127.0.0.1:6322/healthz
```

其中 `/api/` 与 `/healthz` 是为了兼容 `tlen_config_server` 页面里使用的绝对路径 API。

最终验证：

```text
http://127.0.0.1:6322/                                      code=200
http://127.0.0.1:4191/shared/tlen_config_shared/tlen/        code=200
http://127.0.0.1:4191/api/venues                             code=200
http://127.0.0.1:4191/api/thresholds?venue=binance-futures&config_type=amount_thresholds  code=200
http://127.0.0.1:4191/api/thresholds?venue=binance-futures&config_type=factor_plan        code=200
http://127.0.0.1:4191/api/thresholds?venue=binance-futures&config_type=zscore             code=200
http://127.0.0.1:4191/healthz                                code=200
```

## Binance Intra Arb01 部署记录

部署目录：

```text
/home/ubuntu/binance-intra-arb01
```

部署内容：

```text
env.sh
env.bk.sh
config/
scripts/
intra_scripts/
www/
trade_engine.toml
trade_engine
pre_trade
trade_signal
account_monitor_binance
persist_manager
viz_server
```

顶层二进制均从当前本机 `target/release` 获取并同步到 `jp-meta-elvpn`：

```text
trade_engine              sha256=4d504a19099ffe80e6c21effd6ef7c846a5f0b121c12c9a65725d24f2e48380d
pre_trade                 sha256=8a2f84209b8643f9b83d1b00f3cc6f32b7cc0754f950b2f82d8f3866b080aef1
trade_signal              sha256=e5ca8fb5142b1ff2cc7bb902c339085ede975ae11f214470f33a193ef45cbb21
account_monitor_binance   sha256=a6cf6d3d133e8ea3e01c6bb5519939b0e7e320d86ae65fa8af5c27ac7ec4ed21
persist_manager           sha256=948836cb89a7a36229aa6ec78790277b6246da6f534c687a84f2042e42cf9ff0
viz_server                sha256=11ec9151694f07857279c463b5c9f630a8d4a4086d06131cafa264d291ddf7e3
```

`env.sh` 中 Binance 凭证已替换为本机 `binance_mm_alpha` 的 `BINANCE_API_KEY` / `BINANCE_API_SECRET`。文档不记录任何凭证值。

非敏感 env 配置：

```text
IPC_NAMESPACE=binance_intra_arb01
OPEN_VENUE=binance-margin
HEDGE_VENUE=binance-futures
BINANCE_ACCOUNT_MODE=STANDARD
TRADE_SIGNAL_TLEN_QUERY_MODE=local
ARB_HEDGE_FORCE_TAKER=off
ARB_HEDGE_LAZY_TAKER=on
BINANCE_UM_IP_WHITELIST_MODE=off
PERSIST_SYNC_SOURCE_ID=binance-intra-arb01
PERSIST_SYNC_BIND=127.0.0.1:50042
ENABLE_IPC_FAST_POLL=on
```

核心绑定按连续 L3 分片 `16-23` 部署，当前只使用 `16-21`：

```text
ACCOUNT_MONITOR_CORE=16
TRADE_SIGNAL_CORE=17
PRE_TRADE_CORE=18
TRADE_ENGINE_CORE=19
TRADE_ENGINE_IPC_CORE=20
PERSIST_MANAGER_CORE=21
```

`16-21` 全部属于 L3 `16-23`；`22-23` 留作 buffer。行情分片 `8-15` 不被该环境占用。

trade engine 本地 IP 配置按 `jp-meta-elvpn` 单网卡、未加入 Binance 白名单的状态调整：

```text
trade_engine.toml
local_ips = ["0.0.0.0"]
binance_um_whitelist_ip = "0.0.0.0"
```

同时将 env 中的 `BINANCE_UM_IP_WHITELIST_MODE` 设为 `off`。

配置服务：

```text
PM2 name: intra_config_server_binance-intra-arb01
namespace: binance-intra-arb01
cwd: /home/ubuntu/binance-intra-arb01
status: online
pid: 18126
bind: 0.0.0.0:19171
nginx: /intra/binance-intra-arb01/config -> http://127.0.0.1:19171/
```

本次只启动了 `intra_config_server`。未启动以下交易相关进程：

```text
account_monitor_binance
trade_signal
pre_trade
trade_engine
persist_manager
viz_server
```

同步到 `jp-meta-elvpn` 的 `binance-intra-arb01` Redis 配置 key：

```text
binance-intra-arb01:binance-margin:binance-futures:amount_u_overrides
binance-intra-arb01:binance-margin:binance-futures:pre_trade_risk_params
binance-intra-arb01:funding_rate_thresholds_binance-margin_binance-futures
binance-intra-arb01:intra_unimmr_close_symbols:binance-margin_binance-futures
intra_bwd_trade_symbols:binance
intra_dump_symbols:binance
intra_funding_thresholds_config_binance-margin_binance-futures
intra_fwd_trade_symbols:binance
intra_spread_thresholds_config_binance-margin_binance-futures
intra_strategy_params_binance-margin_binance-futures
intra_vol_gate_symbols:binance-margin_binance-futures
rolling_metrics_params_binance-margin_binance-futures
rolling_metrics_thresholds_binance-margin_binance-futures
```

Redis 配置恢复后，本机快照与 `jp-meta-elvpn` 快照规范化校验一致：

```text
sha256=bccb68d48106ffd71460ce27d923ca76db17bbc009a57f5e363be116c7066b2d
```

config server API 对比范围：

```text
/api/symbol-lists
/api/risk-params
/api/strategy-params
/api/funding-thresholds
/api/rolling-params
/api/spread-thresholds
/api/amount-u
/api/max-pos-u
/api/hedge-offset-limits
/api/open-offset-lower
/api/taker-decision-model
```

本机 `127.0.0.1:19171` 与 `jp-meta-elvpn` `127.0.0.1:19171` 上述 11 个 API 的规范化 JSON 完全一致。

最终验证：

```text
http://127.0.0.1:19171/                                      code=200
http://127.0.0.1:4191/intra/binance-intra-arb01/config/       code=200
http://127.0.0.1:4191/intra/binance-intra-arb01/config/api/strategy-params  code=200
```

## 2026-06-18: 启动 binance-intra-arb01 的 viz、persist_manager、trade_engine

本次在 `jp-meta-elvpn` 的 `/home/ubuntu/binance-intra-arb01` 只启动以下三个进程：

```text
intra_pm_binance_arb01   persist_manager
intra_viz_binance_arb01  viz_server
intra_te_binance_arb01   trade_engine
```

未启动：

```text
account_monitor_binance
trade_signal
pre_trade
```

启动后的进程状态：

```text
PID    CPU  进程
19485  21   persist_manager /home/ubuntu/binance-intra-arb01/persist_manager --core 21
19648  0    viz_server /home/ubuntu/binance-intra-arb01/viz_server
19779  19   trade_engine /home/ubuntu/binance-intra-arb01/trade_engine --exchange binance --core 19 --ipc-core 20
```

`trade_engine` 线程绑定：

```text
TID    CPU  线程
19779  19   trade_engine main
19786  20   te-ipc
```

`trade_engine` 当前两个线程都是 busy polling，CPU 接近两个满核，符合启用 fast poll 后的预期。

端口状态：

```text
127.0.0.1:50042  persist_manager
0.0.0.0:10180    viz_server backend
0.0.0.0:19171    intra config server
0.0.0.0:4191     nginx
127.0.0.1:6342   nginx stream -> persist sync
```

`trade_engine` 日志检查结论：

```text
BINANCE_ACCOUNT_MODE=STANDARD
local_ips=0.0.0.0
binance_um_whitelist_ip=0.0.0.0
feeBurn check response: {"feeBurn":true }
trade_engine initialized
IPC thread started, fast_poll=true
binance UM futures websocket connected
binance spot websocket connected
```

未看到 `panic`、`fatal`、鉴权失败、IP bind 失败或 websocket 连接失败。关键字扫描只命中 whitelist 配置说明和 feeBurn 请求行。

`viz_server` 后端验证：

```text
http://127.0.0.1:10180/healthz   code=200
http://127.0.0.1:10180/snapshot  code=200
```

nginx 静态入口初始返回 `403`，根因是 `/home/ubuntu` 目录没有给 nginx worker 用户遍历权限：

```text
/home/ubuntu  mode=750 owner=ubuntu group=ubuntu
nginx worker user=www-data
```

处理方式：

```text
apt-get install -y acl
setfacl -m u:www-data:x /home/ubuntu
```

这是只给 `www-data` 增加目录 traverse 权限，不增加读 env/key 文件的权限。

修复后验证：

```text
http://127.0.0.1:4191/intra/binance-intra-arb01/            code=200
http://127.0.0.1:4191/intra/binance-intra-arb01/index.html  code=200
```

## 2026-06-18: 启动 binance-intra-arb01 的 pre_trade 与 account_monitor

启动 `pre_trade`：

```text
pmdaemon name: intra_pt_binance_arb01
pid: 20918
command: /home/ubuntu/binance-intra-arb01/pre_trade --open-venue binance-margin --hedge-venue binance-futures --core 18
core: 18
status: online
```

`pre_trade` 日志检查：

```text
ArbOpen leverage guard initialized: targets=31 confirmed=31 failed=0 levels={5: 31}
```

未看到 `panic`、`fatal`、鉴权失败或初始化失败。

首次启动 `account_monitor` 时失败，原因是 `binance_account_monitor` 要求 `trade_engine.toml` 至少提供两路 `local_ips`：

```text
Error: trade_engine config /home/ubuntu/binance-intra-arb01/trade_engine.toml must provide at least 2 local IPs for account monitors
```

这台机器只有单公网出口，且该 IP 未加入 Binance 白名单，因此将 `trade_engine.toml` 从一路 `0.0.0.0` 调整为两路 `0.0.0.0`：

```text
local_ips = ["0.0.0.0", "0.0.0.0"]
binance_um_whitelist_ip = "0.0.0.0"
```

注意：当时已运行的 `trade_engine` 不会因为文件修改自动重载；本次修改主要用于后续启动 `account_monitor` 以及之后重启组件时保持配置一致。

重新启动 `account_monitor`：

```text
pmdaemon name: intra_am_binance_arb01
pid: 21593
command: /home/ubuntu/binance-intra-arb01/account_monitor_binance --core 16
core: 16
status: online
```

`account_monitor` 线程全部绑定在 core `16`：

```text
21593 account_monitor core=16
21599 tokio-runtime-w core=16
21600 tokio-runtime-w core=16
21601 tokio-runtime-w core=16
21602 tokio-runtime-w core=16
```

`account_monitor` 日志检查：

```text
BINANCE_ACCOUNT_MODE=STANDARD
bootstrap standard snapshots emitted 1553 basic account event(s)
bootstrap standard snapshots completed
fapi primary/secondary connecting with local_ip=0.0.0.0
spot ws-api primary/secondary connecting with local_ip=0.0.0.0
spot ws-api user stream subscribe ack status=200
PM forwarder stats: sent=1553, dropped=0
```

延迟复查后：

```text
account_monitor PM forwarder stats: sent=0, dropped=0
account_monitor 未看到 panic/fatal/error/failed/unauthorized/forbidden
pre_trade 未看到 panic/fatal/error/failed/unauthorized/forbidden
```

当前已在线的 `binance-intra-arb01` 进程：

```text
account_monitor  core=16
pre_trade        core=18
trade_engine     core=19 main, core=20 te-ipc
persist_manager  core=21
viz_server       housekeeping
config_server    housekeeping
```

仍未启动：

```text
trade_signal
```

## 2026-06-18: 余额检查与“没有余额”的原因

在 `jp-meta-elvpn:/home/ubuntu/binance-intra-arb01` 使用当前环境变量中的 Binance key 执行只读余额检查。

`scripts/check_balance.py --exchange binance --mode STANDARD --asset USDT` 查询的是 Binance U 本位合约钱包：

```text
endpoint: https://fapi.binance.com/fapi/v2/balance
USDT balance: 7618.85871779
USDT crossWalletBalance: 7618.85871779
USDT crossUnPnl: 2262.10397337
USDT availableBalance: 732.57492564
```

所以合约钱包不是空的。

进一步只读查询 spot 与 cross margin：

```text
SPOT /api/v3/account
USDT free: 0
USDT locked: 0
nonzero_count: 0

CROSS MARGIN /sapi/v1/margin/account
totalAssetOfBtc: 0
totalLiabilityOfBtc: 0
totalNetAssetOfBtc: 0
USDT free: 0
USDT borrowed: 0
USDT interest: 0
USDT locked: 0
USDT netAsset: 0
nonzero_count: 0
```

`account_monitor` 日志也验证了同一件事：

```text
binance_std_um   USDT wallet=7618.85871779
binance_std_spot USDT wallet=0
```

因此当前“没有余额”的原因是资金只在 `binance-futures` 合约腿，`binance-margin`/spot 开仓腿没有余额。当前环境配置是：

```text
OPEN_VENUE=binance-margin
HEDGE_VENUE=binance-futures
BINANCE_ACCOUNT_MODE=STANDARD
```

`viz`/`pre_trade` 风控快照中的表现也一致：

```text
hedge_leg venue=binance-futures spot_equity_usd=7618.85871779 total_equity=9867.51604819
open_leg  venue=binance-margin  spot_equity_usd=0 total_equity=0
```

## 2026-06-18: jp-meta-elvpn 前端汇总为 0 的 pre_trade 修复与替换

现象：

```text
account_monitor 已经推送 Binance futures 钱包、未实现盈亏和仓位；
viz 的 hedge_leg 能看到 binance-futures 资金和仓位；
但是 viz 顶层 total_equity/total_position/total_exposure 和 pre_trade_exposure 仍然是 0。
```

原因：

```text
pre_trade 的 query snapshot response 已经把 Balance/Position/UPL/AccountRisk 写入 MonitorChannel；
但是这条 query_eng_channel 路径没有把 MonitorChannel basic_state cache 标记为 dirty；
所以 basic_state_snapshot() 继续返回启动初始的 0 缓存。
```

本地代码修改：

```text
src/pre_trade/monitor_channel.rs
- MonitorChannel::mark_basic_state_dirty() 从 private 改为 pub(crate)

src/pre_trade/query_eng_channel.rs
- BalanceUpdate snapshot apply 成功后标记 basic_state dirty
- BorrowInterest snapshot apply 成功后标记 basic_state dirty
- PositionUpdate snapshot apply 成功后标记 basic_state dirty
- UnrealizedPnlUpdate snapshot apply 成功后标记 basic_state dirty
- AccountRisk snapshot apply 成功后标记 basic_state dirty
```

本地验证与编译：

```text
cargo fmt --check
cargo build --release --bin pre_trade

new target/release/pre_trade sha256:
f79cbeaa81edbb46fa8b650d9c28eb5aefb6f11469c4f2f5a63d786ca1ebb11f
```

jp-meta-elvpn 替换过程：

```text
target env: jp-meta-elvpn:/home/ubuntu/binance-intra-arb01
stop: ./intra_scripts/stop_intra_pre_trade.sh
backup old binary: pre_trade.bak.20260618133003.8a2f8420
old sha256: 8a2f84209b8643f9b83d1b00f3cc6f32b7cc0754f950b2f82d8f3866b080aef1
install: /home/ubuntu/binance-intra-arb01/pre_trade
start: ./intra_scripts/start_intra_pre_trade.sh
new pid: 23761
core: 18
```

替换后只读验证：

```text
snapshot endpoint:
http://127.0.0.1:10180/snapshot

pre_trade_exposure rows: 7
pre_trade_exposure TOTAL net_usdt: 14336.25462337295

risk top:
total_equity: 9833.14468592
total_position: 45618.538341332256
total_exposure: 45618.538341332256
spot_equity_usd: 7618.858717790001
um_unrealized_usd: 2214.28596813
leverage: 4.639262392493123

hedge_leg total_equity: 9833.14468592
```

结论：

```text
jp-meta-elvpn 前端显示 0 不是 account_monitor 没推仓位，也不是 Binance API 没余额；
核心问题是 pre_trade 对 query snapshot 写入后的 basic_state cache 没有失效。
release 版 pre_trade 已经替换并启动，viz 汇总与 exposure 已恢复非 0。
```

## 2026-06-18: 启动 binance-intra-arb01 trade_signal

启动命令：

```text
cd /home/ubuntu/binance-intra-arb01
./intra_scripts/start_intra_trade_signal.sh
```

启动结果：

```text
PM2 namespace: binance-intra-arb01
process: intra_binance_arb01_trade_signal
pid: 24772
core: 17
status: online
binary: /home/ubuntu/binance-intra-arb01/trade_signal
```

当前热路径进程：

```text
account_monitor  pid=21593 core=16
trade_signal     pid=24772 core=17
pre_trade        pid=23761 core=18
trade_engine     pid=19779 core=19 main, core=20 ipc
persist_manager  pid=19485 core=21
viz_server       pid=19648 housekeeping
```

trade_signal 启动后日志现象：

```text
Redis connected
rolling thresholds loaded
funding thresholds loaded
local_tlen[binance-margin] online_symbols=31 cached_symbols=31 missing=0
mkt_channel[binance-margin<->binance-futures] decision_quote_age_us p50 ~= 1.3ms, p99 ~= 2.0ms
持续输出 ArbOpen / ArbClose signals to 'trade_signal'
```

pre_trade 侧验证：

```text
pre_trade 开始收到 signal：
[arb_open_path pt_receive_minus_generation] p50 ~= 14-16us
[arb_open_path pt_handle_strategy_total] p50 ~= 5us
未在 recent log 中看到 panic/fatal/error/failed/reject/insufficient 等明显异常。
```

trade_engine 侧验证：

```text
trade_engine 仍在线，WS 连接已建立。
recent log 中未看到新的 submit/fill/reject/error 记录。
```

启动后账户/前端状态说明：

```text
只读查询 Binance futures /fapi/v2/account：
totalWalletBalance: 9752.18100921
totalUnrealizedProfit: -0.00488698
totalMarginBalance: 9752.17612223
availableBalance: 9752.15297734
nonzero_positions: 1
XRPUSDT positionAmt=0.1 unrealizedProfit=-0.00488698 notional=0.11570407
```

因此此时 viz 的 `total_position=0`、`total_exposure=0` 基本符合真实账户状态：大额 futures 仓位已在 `13:40-13:42 UTC` 期间被平掉，只剩 XRPUSDT 0.1 的尘埃仓。该变化发生在本次 `trade_signal` 于 `13:45 UTC` 启动之前。

## 2026-06-23: 部署 binance_mm_alpha 到 jp-meta-elvpn

目标：

```text
jp-meta-elvpn:/home/ubuntu/binance_mm_alpha
```

本次只部署二进制、脚本、配置、dashboard 和 nginx mapping；没有启动任何 `binance_mm_alpha` 交易进程。

部署命令：

```bash
FR_DEPLOY_HOST=jp-meta-elvpn \
FR_DEPLOY_KEY=/home/ubuntu/.ssh/aws-jp-aws-hfq.pem \
bash scripts/deploy_mm_binance.sh --env-suffix alpha \
  --local-ip 0.0.0.0 \
  --local-ip 0.0.0.0
```

已同步的顶层二进制：

```text
account_monitor
trade_engine
trade_signal
pre_trade
persist_manager
viz_server
```

远端 `env.sh` 使用本机 `binance_mm_alpha/env.sh` 的 Binance 凭证，且不在本文档记录凭证值。非敏感配置：

```text
IPC_NAMESPACE=binance_mm_alpha
BINANCE_ACCOUNT_MODE=STANDARD
BINANCE_UM_IP_WHITELIST_MODE=off
RUST_LOG=info
```

核心绑定：

```text
ACCOUNT_MONITOR_CORE=27
TRADE_SIGNAL_CORE=28
PRE_TRADE_CORE=29
TRADE_ENGINE_CORE=30
TRADE_ENGINE_IPC_CORE=31
PERSIST_MANAGER_CORE=32
```

`trade_engine.toml`：

```text
local_ips = ["0.0.0.0", "0.0.0.0"]
```

这是 jp-meta-elvpn 当前单网卡、未使用 Binance UM 白名单 IP 模式的配置。`BINANCE_UM_IP_WHITELIST_MODE=off` 与双 `0.0.0.0` 同时保留，满足 account monitor 至少两路 local IP 的启动要求。

nginx mapping 已写入并 reload：

```text
/mm/binance_mm_alpha/config  -> http://127.0.0.1:18132/
/mm/binance_mm_alpha/        -> static:$HOME/binance_mm_alpha/www/
/mm/binance_mm_alpha/ws      -> http://127.0.0.1:10232/ws
/mm/binance_mm_alpha/healthz -> http://127.0.0.1:10232/healthz
/mm/binance_mm_alpha/snapshot -> http://127.0.0.1:10232/snapshot
```

部署后只读校验：

```text
远端二进制存在并有 sha256sum。
start_account_monitor.sh 支持 ACCOUNT_MONITOR_CORE。
start_trade_signal.sh 支持 TRADE_SIGNAL_CORE。
start_mm_pre_trade.sh 支持 PRE_TRADE_CORE。
start_mm_trade_engine.sh 支持 TRADE_ENGINE_CORE / TRADE_ENGINE_IPC_CORE。
start_mm_persist_manager.sh 支持 PERSIST_MANAGER_CORE。
ps 检查没有 /home/ubuntu/binance_mm_alpha 进程，确认未误启动。
```
