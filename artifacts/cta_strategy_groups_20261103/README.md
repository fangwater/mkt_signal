# CTA 多策略组本地验证

新增必填 `strategy_group`，以组、市场和频段构成 Manager 策略名及持久化仓位身份。
支持 Prep 多组各自 1min / 5min / 15min，Spread 多组各自 1min；相同因子定义共享计算，
不同组独立维护开关、额度、规则和子仓位。新组复制草稿默认关闭仓位发布。

本轮修改仅在本地，未上传多组产物或修改线上配置。生产两个新计算进程按用户要求已停止，
见 `../cta_factor_replace_20261103/user-paused.json`。

验证：

- `cargo test --lib --bin cta-factor-config`：库 1447 通过、3 忽略；HTTP 1 通过。
- 三个新二进制本地 debug 构建通过，`cargo fmt --check`、`git diff --check` 通过。
- `node artifacts/cta_strategy_groups_20261103/verify_browser.mjs`：临时目录、仅本机 HTTP，
  真实 Chromium 验证 Prep/Spread 组切换、复制默认关闭、保存/删除隔离、Manager 名称、
  共享计算数以及 390px 页面无溢出；不接触 Kafka、账户或生产服务。

默认配置仅预置 `prep01` 和 `spread01`；第二组只在隔离测试内创建，没有虚构正式因子清单。
同 ID 的计算参数须一致；组间规则、NQ 门槛和仓位参数可分别设置。
