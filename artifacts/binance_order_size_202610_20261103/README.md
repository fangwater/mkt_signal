# Binance Futures 2026-11 月度大小单阈值

用户确认此前只补充了 OKX。本次按现有本地维护流程，补充当前 Prep01 / Prep02 的51个
Binance Futures USDT标的，ICX已经移除；不修改OKX、现货阈值及任何因子计划。

读取 /mnt/nfs/30.133_xt_crypto/TardisSource 下每个标的完整2026-10自然月的31个
Tardis原始成交gzip CSV，只读扫描1581个文件、934404516笔成交；每个标的744小时均有样本。
复用现有tardis_order_size，以小时KLL、按时间合并计算P50/P90，price×base amount单位USDT，
用于2026-11。8个worker于81.3秒内完成。CSV及缩略manifest位于本目录；完整逐小时审计
仍在本地calculation目录，不提交中间sketch和重复逐日文件列表。

CSV SHA-256：59eb3dd17a35901589327d2ffebd7c15f4b07cb5fa5393c518684e3b7ad81efc。
使用JP已部署的publish_binance_order_size_thresholds.py，先backfill预演，再备份后原子发布。
共享 binance-futures:amount-thresholds 由0增至51，meta记录2026-10→2026-11及USDT。
逐项读回与CSV一致；OKX的8项、现货表及三市场因子计划逐键比较保持。6322维护接口也可读回。

同时修复CTA读取连接：阈值统一来自同一共享Redis，不另维护TOML阈值副本。
启动校验月份、数量单位、标的及正有限P50/P90；每180秒复查，值变化时按新快照
创建回放，避免将旧分类统计直接当作新阈值结果。--check/配置编辑保持不连接Redis。
读取失败保留最后成功快照并报告错误；缺项不使用默认阈值，也不强制标记因子就绪。

代码提交a0d39e69；主库1450项通过、3项忽略，月度/单位/无效数据回归通过，fmt检查通过。
本机release构建两个计算程序，上传校验SHA-256后原子安装；只重启Prep，CPU17，
Spread程序同步更新但保持停止。所有策略JSON、额度、开关、执行绑定和其他进程保持。
新Prep实际加载51项共享阈值后重新回放，最终核验见worker_verified.json与readiness.json。
所有仓位推送和交易继续关闭。

数据发布私有审计：~/tlen_config_shared/run/binance_order_size_202610_20261103T080500Z/
计算程序私有审计：~/crypto_cta_manager/maintenance-backups/cta_factor_health_bnthreshold_20261103T081500Z/

08:33:52 UTC就绪快照：404项中331项ready，73项未就绪（30个因子名）。
1min 132/134，5min 56/69，15min 143/201；未就绪中58项为15min。
原始值缺失28项全部为15min，另3项标准化值缺失、42项排名有效样本不足。
94项NQ全部就绪，缺大小单阈值的标的数为0。具体名单和状态见
unready_report.md、unready_details.csv / json；不降低样本要求或修改公式。

08:43:54 UTC只读追踪：按当前generation的FactorPoint日志重建排名窗口，
与运行时ready逐项一致。1min的ALGO/ZRX buy_avg_price已达1448/1445个
有效排名样本（要求1440），1min134/134全部就绪；5min仍有13项未达288个。
baseline_040等360根公式只有30个有效排名样本；具体数量、首次原始/标准化
输出时间及连续有效时的最短补齐时间见ranking_samples.json。此前08:33明细
保留为当时快照，不代表后续实时状态。这里只审计，不修改参数、公式或运行进程。
