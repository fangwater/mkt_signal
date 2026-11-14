# Delist 当前 CTA 监控映射已修复

2026-11-03 15:33 UTC发布，15:36 UTC验收。目标 `jp-meta-elvpn:~/delist_risk_server`。
只替换监控程序和页面，通过环境本地脚本重启监控及其SG隧道。

旧CTA已退役并由共享Prep/Spread替换，但Delist仍扫描旧RX；前端仅隐藏区域，顶部仍
累计异常。旧程序只读诊断确认RX01/RX03/RX04恰好三条过期错误：`before_mapping.json`。

当前实时监控直接读取共享worker的plan和runtime position_tables，按worker心跳判断
新鲜度，避免把15分钟表的正常决策间隔判成3分钟超时。表缺失、身份/周期错误、开关
未同步及仓位发布失败仍报告异常，风险范围保留配置之外的持仓目标币种。

| 当前组合 | 执行绑定 |
| --- | --- |
| Prep01 1min / 5min / 15min | binance_exec_rx01 |
| Prep02 1min / 5min / 15min | binance_exec_rx03 |
| Spread01 1min | binance_exec_spread01 + okex_exec_trade01 |

七项均enabled=true、stale=false、errors为空，所有绑定active=true、pending=false；
两个worker均plan_matches_config=true。当前Prep为404个唯一任务，Spread为50个。
旧RX目录不再计入实时监控，目录/历史数据保留。配置和执行入口采用当前身份。当前组合
不属于独立旧信号环境，不显示仍要求旧信号目录的备份按钮；本轮未迁移历史归档格式。
OKX下架目录未接入的状态继续单独显示，不作为计算进程故障。

验证与发布：

- 本地同步arbmm，HEAD `6ee63160771cebecf807efd263d92ff465f31b22`；已有未提交改动
  保留，没有stage或提交其他工作。改动前文件在 `source_before_current_mapping/`。
- 26项Delist测试、触及Rust文件格式检查及浏览器检查通过。覆盖7行组合、旧目录不影响
  顶部计数、配置往返、保存隔离/冲突和移动布局。见 `delist_tests.log`、`browser_test.log`。
- 本地release构建成功，上传临时文件后核验SHA-256、备份原文件并原子安装。发布前后
  只读核对真实Manager绑定，见 `staged_mapping.json` / `after_mapping.json`。
- 二进制SHA-256 `a3096488dfad952bfc06f049731ba222ee8f3bd3edfb0688f29764bf0bfa97f5`；
  页面SHA-256 `07def370514238118662d350cc7ad173f408f78149ee697a2de95c0811b1c80e`。
  当前PID580273 / CPU `0-5,48-95`。
- 配置及鉴权哈希保持，原auto-remove/dump/flatten均为开启，回读保持；未新增或关闭
  开关。SG Redis隧道和4191登录入口正常。见 `runtime_after_install.json`。
- CTA计算/执行及Bitget交易进程未由本次重启。同期另有Binance FR03/NAV进程变化，
  回执明确保留，没有误报为全主机PID不变。

远端备份 `~/delist_risk_server/run/current_cta_mapping_20261103T1533Z/`。
重启会使旧登录会话失效，重新登录后读取当前页面。CLI使用空风险书验收映射，不能证明
全部下架风险目录或账户无风险；正式HTTP路径仍使用真实风险书。
