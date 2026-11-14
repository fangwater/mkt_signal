# JP 整机失联排查

排查日期：2026-10-18 UTC。目标：jp-meta-elvpn，现场 hostname 为 ip-172-31-35-228.ap-northeast-1.compute.internal，~/spread_pbs 存在。全程仅远端读取，没有启动/停止服务、修改网络或交易配置。使用 aws-marketdata-core-layout skill。

## 结论与边界

已确认原运行周期在 2026-10-17 20:25:20 UTC 附近日志突然中断，之后发生非正常关机；不能仅凭日志尾部给出精确宕机秒数。最符合现有证据的是整机失去响应后经外部强制恢复。内核卡死、硬件/Nitro/EC2 基础设施异常仍不能区分，未取得可归因的 panic 栈或 AWS 控制面证据。

用户确认 2026-10-18 07:45 和 08:08 UTC 两次启动均为人工恢复，不能将其当作自动反复崩溃的证据。

## 时间线

| UTC | 北京时间 | 证据 |
| --- | --- | --- |
| 10-17 20:20:01 | 10-18 04:20:01 | 最后 sysstat 采样：CPU idle 71.18%，available memory 120942800 KiB（115.34 GiB），load1 39.74（96 个逻辑 CPU），blocked 0；NVMe util 6.43%，await 1.15 ms |
| 10-17 20:20:38 | 10-18 04:20:38 | chronyd 报 Forward time jump detected；20:21:21 重新选择 169.254.169.123 |
| 10-17 20:25:20.733 | 10-18 04:25:20.733 | 原 boot journal 最后一条；仍是正常应用 INFO，没有 shutdown 尾部 |
| 10-18 06:39–06:50 | 10-18 14:39–14:50 | 前次排查从多来源探测全部公网 IP 失败；见 ../gate_fr_arb01_down_20261018/REPORT.md 与 ALT_IP_REPORT.md |
| 10-18 07:45:10 | 10-18 15:45:10 | 第一次人工恢复启动；EXT4 recovery，606 orphan inodes deleted，journal unclean/corrupted |
| 10-18 07:47:26 | 10-18 15:47:26 | 这一轮 journal 最后记录，随后至下次启动之间无正常 shutdown 尾部；具体失联时间不能由此精确判定 |
| 10-18 08:08:54 | 10-18 16:08:54 | 第二次人工恢复启动；再次 EXT4 recovery 和 journal unclean |
| 10-18 08:22–08:30 附近 | 10-18 16:22–16:30 附近 | SSH 可用；ps 中没有 trade_engine、pre_trade、account_monitor、spread_pbs、depth_pub 等主要交易/行情进程，基础服务和 cta_web 在运行；这不是订单或交易所持仓审计 |

## 排除与线索

- 故障前最后采样不支持持续 CPU、内存或磁盘吞吐耗尽；采样间隔为 10 分钟，不能排除随后数分钟的瞬时故障。
- 原 boot kernel journal 未找到 OOM kill、panic、soft/hard lockup 或存储 I/O error 的落盘证据。缺失记录不能排除内核或硬件卡死。
- 文件系统 recovery 和 journal unclean 证明非正常结束，不证明磁盘本身损坏。
- chronyd 在 18:31、18:33、19:31、19:32、19:33、20:01、20:20 多次报时钟前跳并暂时无可选时间源。它是额外异常线索，尚无证据证明其造成宕机；对 wall-clock 时间线应保留这一限制。
- 当前实例 IMDS：i-0ad3386d7c168cca4，c7i.metal-24xl；维护 scheduled/history 返回空数组。空数组不足以排除 AWS 基础设施异常。
- 本机与 JP 未发现 aws CLI；JP IMDS iam/info 返回 404，无实例角色入口。本次未取得 CloudWatch/CloudTrail/AWS Health 数据，也未读取凭据文件或 env.sh。
- /sys/fs/pstore、/var/crash 为空；kdump-tools inactive，无法从现有本地材料取得 crash dump。
- 故障前 sa17 标记 Linux 6.17.0-1017-aws；人工恢复后为 7.0.0-1014-aws。记录此差异，不据此断言是内核升级引发事故。

## 继续归因所需证据

对实例 i-0ad3386d7c168cca4（ap-northeast-1）读取 10-17 20:00 至 10-18 08:20 UTC：

1. CloudWatch StatusCheckFailed_System、StatusCheckFailed_Instance、StatusCheckFailed_AttachedEBS，以及 CPU、网络、EBS 指标，区分宿主系统、实例 OS 与存储层异常。
2. CloudTrail 中 StopInstances、StartInstances、RebootInstances 等操作时间，区分故障和人工恢复动作。
3. AWS Health / EC2 scheduled events 与可用的 EC2 console/serial output；必要时由 AWS Support 调查 bare-metal 底层故障。

当前无这些控制面数据，根因必须保留未定。不要把资源正常、无 OOM 日志或空维护事件当作硬件故障的证明。

原始只读采集：boot_and_kernel.txt、resources.txt、journal_tails.txt。仓库既有修改保留，本次仅新增此目录；没有部署，无需变更 CPU/IP 运维台账。
