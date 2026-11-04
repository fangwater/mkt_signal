# Prep02 九策略上线与存量分仓

用户授权 commit、push、重新部署；券池名为 `top-5`、`top-20`、`last-20`。
JP Prep02现为三券池×1min/5min/15min九个独立策略；102条规则，空的
`top-20 × 5min`保留身份与零目标。Prep和Spread共用的新配置页支持券池JSON，
Prep01及Spread01继续使用原未限定券池的身份。

## 已应用的切换

- 账户：`binance_exec_rx03`，Binance USDT永续（RapidX执行）。
- 切换点：2026-11-04 **09:47:31.222 UTC**（1793785651222000微秒）。
- 唯一规则直接归组，跨池重复规则等分数量及冻结入场金额，决策时钟迁移。
- 信号发布先暂停，等待活动委托/在途成交排空，再停止RX03 pre-trade与trade_signal。
  两次完整Exec账本捕获一致，交易所账户总数量与Manager全历史重放一致。
- 对22项已完成执行尾差及历史策略/系统尾仓归属差异，使用真实执行数量和完整账户证明，
  同一已完成分钟收盘价结转。Manager只追加一次不可变零和归属记录。
- history、7d、1d、after窗口的账户FIFO及总盈亏保持，策略盈亏之和可加。
  原成交、费用与历史策略盈亏保留；新九策略承接切换后的盈亏。
- 九条Manager绑定沿用原`lgq_grid_prep`、grid、shares=1；九份信号配置均启用，
  每规则100U，开平阈值与执行参数保持线上原值。Prep01 15min原50U及其他参数保持。
- Manager绑定保存不会自动发布物理Exec配置。逐条publish九条绑定，并用现有Exec Config
  DELETE接口退役旧三项；物理strategy_names恰为九项，旧三项在removed索引中，旧目标为零。
  旧Redis配置作为退役记录保留，未使用的Chase分仓仍为null。
- 新九张及退役旧三张信号账本、Exec分仓、Manager目标与绑定安装后，依次通过环境本地
  脚本恢复pre-trade、trade_signal。迁移本身没有制造成交、费用或以补单修正归属。

## 构建与验证

实现提交：`0b890394`、`e4d66eca`、`b77e5da1`，均已推送`arbmm`。
在活跃本地工作区导出已提交源码，使用其Cargo manifest本地release构建；
运行程序的准确来源及SHA-256见`release_manifest.json`。
Prep/Spread/Config来源`f281e5f4`；最终迁移工具及Delist来源`37ea9cae`，后者包含
同期已上线的确认现货尾差修复。发布时arbmm与远端同步，未在生产主机运行Cargo。
SCP上传显式临时路径，逐项SHA-256核对、原子替换；启停只用环境本地脚本。

实现阶段库测试1475通过、3忽略，迁移回归最终5项通过，Config/Delist定向检查通过。
最终只读验收见`verified.json`：

- Prep/Spread均已结束历史回放，实时心跳、观测新鲜度、Manager发布确认、仓位状态正常，
  因子/仓位错误为空；RX03 position_ready=true，九绑定shares=1，无pending publishes。
- Prep 408项共享计算、404项有观测；ICX四项无行情。部分Prep窗口仍未就绪，按既有规则跳过；
  不把历史回放结束当作所有因子就绪。Spread50项均有新鲜就绪观测。
- Prep PID2970444全部线程CPU17，Spread2970968全部线程CPU18；Config2968323，
  RX03 pre-trade3016803、trade_signal3016920；Delist3033546、原SG隧道3033518。
- Manager2097112 / CPU24、RX03 trade_engine3936910、persist3684036、
  account_monitor3685214、Viz3685348、Config3685475的原身份保持。
- env.sh、主TOML、源IP、local_ips、ENI/EIP、路由与监听未改。同期Bitget FR另有独立发布，
  不将其15个进程身份变化误记为本次操作，也不覆盖其更新。
- `browser.mjs`对线上只读服务运行真实Chromium：1440px及390px逐个核对九策略的
  券池/频段、规则数、100U、启用状态及空表，再检查Spread参数；无JS错误或页面溢出，
  前后catalog revision相同，生产写入为0。结果见`browser-verified.json`。

浏览器工具通过临时本机SSH转发访问远端loopback19220；未保存生产参数。
本地浏览器使用已有Chromium与临时依赖目录，未修改生产服务依赖。

## 审计与运维

远端完整私有备份：
`~/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z/`。
含原文件/程序、原始捕获、分仓包、不可变归属应用证据和各阶段日志。
**不要重复安装或再次插入本次切换记录。**

`ops.py`是本次分阶段部署审计助手，依赖远端已有Manager运维模块和私有审计目录；
变更阶段必须显式`--execute`，不是通用的自动启动迁移逻辑。
`verify.py`只读取运行状态与配置，输出脱敏回执至私有审计目录。
大体积源码快照、tar、二进制、原始账户捕获与凭据不入Git。

日常启停继续用各环境`./scripts/*.sh`；Prep/Spread脚本需要`--execute`，
Exec与Delist启停脚本不接收该参数。详见
[迁移契约](../../docs/cta_symbol_group_migration.md)、
[CPU登记](../../docs/core_allocation.md)、
[IP登记](../../docs/jp-meta-elvpn_ip_binding.md)。
