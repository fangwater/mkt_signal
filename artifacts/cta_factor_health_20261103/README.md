# CTA Kafka / NQ 健康检查与修复

检查对象为 JP 的 Prep01 / Prep02（六频段表）和保持停止的 Spread01 计算程序。
所有推送和下单保持关闭，没有更改额度、NQ条件、因子符号或就绪标准。

确认并修复两个代码问题：

- Kafka enable_partition_eof=true 时，PARTITION_EOF(-191) 被当成消息错误；
  成功消费后旧错误仍保留。现在 EOF 返回正常空轮询，收到可解码的新记录后仅清理 Kafka 错误。
  配置、回放或 IPC 等其他错误继续保留。
- NQ 的盘口状态必须先见 snapshot 才可用，但保留的 Kafka 行情从增量中间开始。
  抽样 BTC/ETH 共113次盘口更新均无snapshot，已有110/111次双边数据。
  现在重建出有效双边报价即可使用；显式不完整snapshot仍使缺失一侧无效。

按用户补充移除 ICX：Prep02 1min 一条、15min 两条规则，连同 NQ 共移除4项计算。
仓库模板及线上配置均更新；线上使用带 revision 校验的 Config 预览/保存接口，
其余规则、金额及全部 enabled=false 保持。合并计算408→404。

其他未就绪输入已单独分类，见 diagnosis.json：60项需要大小单阈值，但工作进程未配置，
共享 Binance Hash 也为空；当前仅有旧应用月2026-10的CSV，未冒用为11月阈值。
部分公式和归一化/排序样本不足。例如15min baseline_040 至少需要360根输入，而被检查
回放只有202根；baseline_104 虽有143根有效原始值，尚未完成后续归一化和排序预热。
这些项保持未知，不伪造原始值、不降低样本要求、不标记强制就绪。

回归测试覆盖 EOF 与真实错误区分、消息载荷/offset保持、无snapshot的三频段NQ预热，
以及不完整snapshot的原有失效行为。Period库19项、主库1448项通过（3项忽略），fmt检查通过。
源码提交44fe891c，本机release构建；两个程序上传显式临时路径，SHA-256核验后原子安装。
只通过环境脚本重启 Prep，CPU17；Spread程序同步替换但保持停止，Config服务及其他进程保护。

ICX私有备份：~/crypto_cta_manager/maintenance-backups/cta_remove_icx_20261103T065600Z/
发布私有备份：~/crypto_cta_manager/maintenance-backups/cta_factor_health_20261103T070200Z/
发布前后配置哈希、所有账户绑定和受保护进程身份核对见 verified.json。

最终回放完成：404项均有输出，94项NQ全部ready，Kafka/仓位表错误均为空；
核验时共288项ready，其余116项按输入和样本要求继续保留未就绪。82个其他受保护进程、
全部运行配置及执行绑定保持。七份推送配置关闭、八条绑定shares=0，下单进程停止。
ICX两份Manager零目标及对应执行目标也已清理，只更新相关两份停止策略；
其他业务发布者可以正常更新自己的仓位定义，校验不把这种正常并发更新当作本操作失败。
线上回执见 verified.json、readiness.json、icx_verified.json、icx_targets_verified.json。
临时发布目录和Kafka诊断程序已删除，私有备份保留。
