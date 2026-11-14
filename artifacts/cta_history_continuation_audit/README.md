# CTA 1h 历史合成与 Kafka 衔接

已上传 JP `~/cta-hourly-history/current/data`。
历史范围：2026-10-08 00:00 至2026-11-08 00:00 UTC，34标的×744根，25,296根。
输入数据压缩14,164,890字节；含规则、二进制及校验记录的包29,689,568字节。
原包：`~/cta-hourly-history/rx01_rx03_1h_20261108.tar.gz`。
SHA-256：`bc01bcb710cfa4a0479a68f66f6cb9ffbc6a0adba5e37cd3ec46aa61ec7bd5a3`。

复用维护中的Tardis CSV读取器、原生小时聚合与CTA引擎；25币读L2、9币仅成交，8币有大小单分类。
使用31天导出期的814,247,579个盘口事件、648,517,546笔成交；分类还读取前一个自然月成交。
价格、成交、20档盘口及盘口统计在本地合成，不称为交易所或Tardis官方小时产品。
历史窗口及NQ配置来源、逐文件校验见上传包的`provenance/`、`SHA256SUMS`。

原生导入校验：233个普通因子中232个就绪，32个所需NQ均有744根历史。
DASH `depth_imb_bps1` 最近48根无有效原值：中间价两侧±1bp没有挂单，保持原值为空及正常门槛。
L2文件完整，因子按原公式保留空值及样本门槛。

全清单导入后回放58,118条保留Kafka记录，2026-11-07 09:58:21至2026-11-09 10:24:12 UTC，
生成9,010个截止后小时因子点；最新265项计算中264项就绪，仅上述DASH因子未知。
时间严格递增，无重算已导入小时，无缺失计算；JP公共入口另做单币58,142条全量接续验证。
详细结果：`verification.json`、`full_replay_summary.json`、`full_seed_check.json`。

数据包、匹配worker及工具已上传独立共享目录。本次没有替换线上Prep/Spread二进制或重启它们。
启用该读取路径使用`--hourly-history /home/ubuntu/cta-hourly-history/current/data`或
`CTA_HOURLY_HISTORY_DIR`；历史只预热窗口，不回放仓位或下单。
包是截至Nov8的快照，Kafka最早源小时晚于该截止点时拒绝跨缺口启动，需刷新包。

运行代码：`31bc1a10`。RX03的整个1min下架另见
[`cta_rx03_1min_retirement`](../cta_rx03_1min_retirement/README.md)。
