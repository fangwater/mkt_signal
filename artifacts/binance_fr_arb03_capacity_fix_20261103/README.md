# Binance FR 03 USDT 购买能力与拒单退避修复

目标：`jp-meta-elvpn:/home/ubuntu/binance_fr_arb03`，Binance 原生 Portfolio Margin FR。
2026-11-03 15:32–15:40 UTC 完成停止、发布、恢复基础组件及原先运行的 trade_signal。

## 原因与修复

原面板字段来自 `/papi/v1/account.totalAvailableBalance`，表示账户可用保证金。
现货买单还需要 USDT 现金或可借 USDT。此前账户现金和可借额度均为0，却仍通过
保证金检查。恢复信号时最初30笔现货买单先返回51014（资产不可借入），随后多币
买单返回51042（操作过于频繁）；具体交易所频控阈值和可借额度为0的原因未获说明。
原始诊断见 `../binance_fr_arb03_51042_diagnosis_20261103/evidence.json`。

- 保留现有账户保证金检查，新增 Binance PM FR 的 USDT 现金/可借额度查询；
  复用签名、REST dispatcher 和 query IPC。新查询返回原始的小型资产 JSON，
  不改变现有完整账户快照的基本消息输出。
- 两个查询必须属于同一轮、ID匹配且数值有效，才更新购买能力。首次无快照、
  查询错误、快照超过180秒，均拦截新的 USDT 现货买入。周期由现有60秒定时器驱动。
- 买单按名义金额加0.1%缓冲检查；已提交金额在后续完整快照覆盖前预扣，防止
  批量请求重复使用同一份余额。刷新保留5秒提交结算余量。
- 51014 使借款部分进入60秒冷却，并要求失败后新查询才恢复借款额度；自有现金
  仍可使用。币种方向拒单锁也是60秒。51042 对该账户的现货新开仓使用60秒起、
  上限15分钟的递增冷却，同一批在途拒单不会逐笔放大冷却。
- 检查接入 ArbOpen 信号及提交前；ArbClose、对冲、撤单路径未加此限制。
- 单一风险 IPC 消息新增 `binance_pm_usdt` 状态，producer 与 target Viz 一并更新。
  原面板改为“账户可用保证金”，另列“USDT现金可用”“USDT可借额度”
  “现货买入可用USDT”及暂停原因。保留远端已有币名处理差异，见 `prior_html.diff`。

## 验证与发布

- `cargo check`：pre_trade、trade_engine、viz_server 通过。
- mkt_signal、trade_engine、order_common、viz_common、viz_server 库测试合计
  1,893通过、0失败、3忽略；新增测试覆盖资金为0、过期/错误快照、查询相关性、
  额度预扣、借款恢复、频控递增及51014方向锁。见 `tests.log`。
- `cargo fmt --all --check`、`git diff --check`、页面 JavaScript 语法检查通过。
- 本地活动工作区 `arbmm` 与 `origin/arbmm` 同步，HEAD `6ee63160`。未提交工作区
  状态及本次源码哈希见 `source_state.json`，本次差异见 `task_diff.patch` 与
  `binance_pm_buy_capacity.rs`；没有回滚或提交其他任务的改动。
- 6个 release 二进制全部在本机构建。通过 `publish-jp-meta-fr.sh --skip-build`
  上传16个文件到显式临时目录、逐项核对 SHA-256 并原子安装；没有远端 Cargo 构建。
  HTML单独校验后原子安装。二进制哈希见 `binary_sha256.json`，发布回执见 `publish.log`。
- 通过环境本地脚本停止/启动，停机时核验现货和合约挂单均为空。旧文件备份：
  `~/binance_fr_arb03/run/capacity_fix_20261103T153229Z/`，未额外复制凭据。
- 恢复基础进程、确认完整资金快照后，执行环境本地 `scripts/start_trade_signal.sh binance`。
  `env.sh`、trade_engine.toml、Viz配置哈希保持；源IP仍为 `.231/.231`。
- persist_manager PID592997 / CPU15；trade_engine595521、pre_trade595779、
  account_monitor598066、trade_signal600644、Viz592648、Config590456 为CPU `0-5,48-95`；
  原 dashboard61245 保留。CPU/IP living docs已同步更新。
- 发布后的HTML与上传文件完全一致。Chromium通过SSH连接真实Viz/FR websocket，
  桌面/手机检查通过，无JS异常；见 `browser_result.json`、`desktop.png`、`mobile.png`。

## 验收边界

15:40首次恢复后：信号有ArbOpen与ArbClose，波动率抑制为0；资金快照每分钟更新，
现金/可借/买入可用均为0，状态为 `binance_usdt_buy_capacity_insufficient`。
现货买单在本地拦截。最终15:41:06–15:43:06完整两分钟窗口内51014/51042、资金查询错误、
波动率抑制均为0；风险快照年龄1.07秒，仍有ArbOpen=100、ArbClose=20，净敞口绝对值合计
约172.75 USDT。最终证据见 `final_runtime.json`。
15:41:57交易所现货/合约挂单均为0，见 `open_orders_after.json`。

本次修复的是资金检查和错误退避，没有恢复交易所的可借额度，没有转账、借款、
还款、调整杠杆或人工平仓。只有现金/可借额度恢复，且其他风控允许时，才会继续
对应买入开仓。此前三个币种的账户限仓问题也没有通过本次修改消除。
