# ICX清理与RX03执行核验

2026-11-04 12:11–12:13 UTC，按用户要求从JP当前CTA配置直接移除ICX：
Prep02 `last-20 × 1min`删除TD_TI_043一条，`last-20 × 15min`删除TD_TI_027和
baseline_104两条；Prep/Spread的`symbol_groups.json`均删除ICX成员。
`last-20`保留组名，当前19个成员；Prep02为99条规则，九策略身份不变。

操作前确认ICX交易所/Exec仓位为0、账户普通及算法挂单均0，使用既有Config API
先preview，再按expected_revision逐份保存规则，最后preview/save券池定义。
`cleanup.py`默认只读预览，`--execute`才应用，整份前配置和交易所/Exec捕获保留在远端私有备份。
对比每份前后Combination证明仅删除ICX规则，其他开关、100U额度、阈值、模板与绑定保持。

热加载后Prep计算408→404，404项均有观测；Spread仍50项，均有观测。
两个原generation均保持、warming=false，因子/仓位错误为空。未重启进程或重新回放。
部分Prep因子窗口未就绪仍由既有就绪检查处理。
清理的是活动配置，旧审计、历史成交/盈亏和零仓位痕迹保留。

`verified.json`记录12:12 UTC只读核验：

- RX03九个绑定均为`lgq_grid_prep` / grid / shares=1，发布队列空，position_ready=true。
- 交易所全部币种的实际净仓与完整Exec分仓总量一致，ICX仓位0，无ICX执行行。
- 全部37行execution_complete=true，活动批次、在途数量、普通/算法挂单均0。
  12行target_reached、12行target_tolerance、13行exchange_minimum。
- 切换后last-20/top-20/top-5三个1min新策略已确认真实成交20/18/1笔，证明新身份已进入执行和归属。
- 近15分钟检查无ERROR；pre-trade的WARN为DASH极小聚合尾差低于交易所最小下单量，
  trade_engine有一次撤单返回-32080（订单已完成/存在），当前快照已无活动委托或未完成执行行。
- Prep2970444 / CPU17、Spread2970968 / CPU18、RX03 pre-trade3016803、trade_signal3016920、
  trade_engine3936910、account_monitor3685214保持。env.sh、主TOML、source IP、CPU及监听未改。

本地同步删除四份模板/券池定义中的ICX。原始20组输入fixture保留，既有模板验证显式扣除ICX，
验证剩余99条规则、9表和两worker同一券池定义。该定向测试1项通过，release程序`--check`
验证本地计划404项；cargo fmt --check、Python语法及git diff --check通过。
本次仅热更新配置，无程序替换或生产release构建。

远端私有备份：
`~/crypto_cta_manager/maintenance-backups/cta_icx_cleanup_20261104T121113Z/`。
原始账户捕获和整份线上配置不入Git。本目录仅提交操作代码与脱敏验收；local-plan.json不入Git。
