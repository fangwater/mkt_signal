# 易达原生交易：SHFE / INE

`crates/cn_futures` 现有 `ctp-trader`（中金所）和 `yida-trader`（上期所 / 上海国际能源交易中心）两个入口，共用一个交易核心。易达直接调用 Hanlin **YDExtendedApi**，没有通过易达的 CTP 兼容接口。行情独立，暂按 CTP 考虑，本次没有接入易达行情。

实现已完成本地单元测试和按真实原生头文件编译的合成 SDK 桥接联测，**尚未连接期货公司柜台，也没有生产部署或端到端性能测量**。

## SDK 依据

用户提供的[官网](https://www.hanlinit.com)有[文档](https://www.hanlinit.com/docs/)和[下载](https://www.hanlinit.com/downloads/)入口，但本次公开访问均要求登录。实现采用公开的 **1.188.78.0 原生开发包参考**：[YDApi_Demo](https://github.com/arriettytrader/YDApi_Demo_1_188_78_0/tree/5b9bbf0618aae254eceaa0d131b70b38c48bc25d)，不是已确认的最新官方版本或已确认适配某家期货公司的生产包。没有使用示例账户配置。

SDK 文件留在仓库外；代码使用原生 `ydApi.h` / `ydDataStruct.h` / `ydDataType.h` 编译 C++ shim，通过 `makeYDExtendedApi` 和 `getYDVersion` 加载 `libyd.so`。运行时严格核对 `1.188.78.0`，其他版本必须先核对头文件 / ABI 并重新验证。

| 文件 | 本次参考 SHA-256 |
| --- | --- |
| `ydApi.h` | `20ff751f8fc68d1139401a50b83fa830c48912a3db25a8721da0b09a5d5793d1` |
| `ydDataStruct.h` | `67924e84ba1d579ba0a068240fb7910b8c3e3d4ab775325820ce9bada5f7b71d` |
| `ydDataType.h` | `22d0ff79445c70bed132634ab60c6dfc15a92a343eeb3ebdc55446acb389aacd` |
| `libyd.so` | `f087dc662dda9978ca3fc81b471f1ccca94292c85ee4e11faa38f5ca256884a1` |

本次读取参考库版本时，库初始化器尝试执行 `/sbin/udevadm` 和 `sudo`，后者未获授权；没有提供提权凭据。版本读取也会执行厂商库初始化代码。常规测试使用合成库，不反复加载真实库或尝试修改系统。正式使用应核对厂商安装要求与期货公司配套版本。

## 构建与配置

```bash
YIDA_SDK_DIR=/absolute/path/to/yida/1.188.78.0 \
  cargo build -p cn_futures --features yida-sdk --bin yida-trader

# 只检查项目配置结构；不加载 SDK、不读凭据、不连接。
cargo run -p cn_futures --bin yida-trader -- \
  --config config/yida_trade.example.toml --check-config
```

以 `config/yida_trade.example.toml` 为起点，在独立环境目录中填写库路径、柜台提供的原生服务器配置路径、日志路径、合约白名单和保守资金预算。凭据从配置指定的 `YIDA_USER_ID`、`YIDA_PASSWORD`、`YIDA_APP_ID`、`YIDA_AUTH_CODE` 环境变量读取，不放入配置、仓库或命令参数。

原生服务器配置仍由期货公司提供。当前代码在加载库前检查以下 `key=value` 配置，其他参数按厂商要求保留：

```ini
# 地址仅为占位，实际使用期货公司提供的交易端点。
TradingServerIP=127.0.0.1
TradingServerPort=1
UDPTrading=no
ConnectTCPMarketData=no
ReceiveUDPMarketData=no
RecalcMode=off
```

首期只实现 TCP 交易；所有易达行情连接关闭。`RecalcMode=off` 避免在缺少行情时假装持续重算保证金 / 浮盈亏；因此 SDK 本地 `Available` / `usable()` 不能当作独立、实时的柜台风险模型。需要按实际保证金、手续费和价格风险配置保守 `cash_budget_per_open_lot`，并在仿真中核对。资金事件只收紧本地预算，完整恢复才建立新基准。

**账户必须专用，且原生 `LoginCount == 1`。** 不能另起 CTP、YDClient 或其他策略独立使用同一账户的资金 / 持仓。flow 锁只防止相同目录被并发使用，不是跨机器的账户锁。恢复时若有 SHFE / INE 之外的持仓、非投机套保账本、期权、组合或不支持的事实类型，会保留原始事件并阻断交易，不做猜测映射。

以下是未来仿真联调入口，本次没有执行这些真实 SDK 连接命令：

```bash
# 目标为本地配置指定的仿真账户、SHFE / INE 合约，仅登录与恢复。
target/debug/yida-trader --config /absolute/path/yida-sim.toml --connect

# 同一目标，额外允许显式 JSON 下单 / 撤单。
target/debug/yida-trader --config /absolute/path/yida-sim.toml --connect --execute
```

默认只检查配置。`--connect` 启动 SDK 并登录；没有 `--execute` 不允许下单和撤单。启动、EOF、`shutdown` 或错误退出均不会自动撤销交易所挂单。

## 一套交易契约

一个业务线程直接执行意图、风控、预占、OMS 和 SDK 提交；SDK 内部线程回调只传递有界队列中的复制事件。异步 JSONL 审计和 stdin 读取在后台。没有策略兼容层，也没有风险到执行之间的 IPC。

| 能力 | 原生映射与约束 |
| --- | --- |
| 普通限价 | `OrderType=YD_ODT_Limit`，提供具体 tick 价格；没有 post-only 保证 |
| FAK / FOK | 原生 `YD_ODT_FAK=1` / `YD_ODT_FOK=3`；默认关闭，需按实际账户 / 合约验证 |
| 开仓 | `YD_OF_Open` |
| 平今 / 平昨 | `YD_OF_CloseToday=3` / `YD_OF_CloseYesterday=4`；SHFE / INE 不直接发送通用 `Close` |
| 通用平仓意图 | 按配置优先级规划今昨子单，所有子单先通过本地风控与预占，再逐笔提交 |
| 跨日期 FOK | 拒绝拆分，两个独立 FOK 不保证父单全部成交或全部撤销 |
| 订单引用 | 先调用 SDK `getNextOrderRef()` 分配正 int，再本地预占 / 审计；`checkOrder` 验证后以 `insertOrder` 发送，保留该引用 |
| 撤单 | `cancelOrder` 使用交易所作用域内的整数 `OrderSysID`；尚无系统号时拒绝发出撤单，等待确认 |
| 权限 | 每次提交重查账户 / 交易所 / 品种 / 合约权限，取最严格值；`CloseOnly` 拒绝开仓 |

未调用会重新分配 OrderRef 的 `checkAndInsertOrder`，避免已经登记的预占和实际订单失去关联。普通 `insertOrder` 在回报前不会更新 SDK 本地冻结；这个间隙由共享交易核心的预占覆盖，SDK `checkOrder` 是附加检查。

stdin 每行一个 JSON。CTP 和易达使用同一 `Intent` 与 `OrderKey` 结构，key 的 session 内容只由各自适配器解释：

```json
{"op":"state"}
{"op":"submit","intent":{"venue":"SHFE","symbol":"cu2612","side":"sell","offset":"close","lots":1,"price_ticks":350000,"time_in_force":"day"}}
{"op":"cancel","key":{"session":"yida:20261109","order_ref":"101"}}
{"op":"reconcile"}
{"op":"shutdown"}
```

这些只是合约 / 价格 / 身份示例；撤单必须使用实际回传 key。原生无月份的品种代号、期权和 TAS 不在首期范围。

`submit_receipt` 包含每个子单的 `key`、开平、手数和结果：`submitted_locally`、`rejected_locally`、`uncertain`、`not_sent`。中途失败也保留已发出子单的身份；后续未发子单释放预占。没有自动重试或自动撤回已经发出的兄弟单。

`checkOrder` 返回失败可以确定尚未发送，因此释放本地预占。`insertOrder` / `cancelOrder` 返回 false、桥接异常或未知发送结果保守视为不确定，保留预占并阻断新增订单。SDK 返回 true 也只表示本地提交，不等于柜台确认或成交。

## 身份、恢复与限制

- 系统订单号按 `(venue, OrderSysID)` 关联，成交按交易所、合约、交易日、TradeID 和方向去重。允许恢复 `OrderRef=0` 的外部手工订单，SHFE / INE 的同号订单不会覆盖。恢复订单没有策略归属；未知成交保留原始证据后要求对账。
- 原始审计保留原生时间、错误码、整数订单 / 成交身份、厂商可提供的交易所原始 ID，以及 `YDTrade.Commission`。共享持仓只随事实成交改变，不把累计委托成交量编造成成交。
- `notifyFinishInit` 只说明 API 可调用，**不说明订单 / 成交已经追平**。每次登录必须等待 `notifyCaughtUp`；在该回调中复制原生账户、目录、分日期持仓、`findOrders` 和 `findTrades`，业务线程校验委托成交量与事实成交量、原生平仓冻结与恢复委托的预占一致后整体恢复。读取指针不跨 FFI 保留；`find*` 不进入逐单热路径。
- 快照中若插入独立实时回调、出现不支持的事实、遗漏尚未确定结果的本地订单或对账失败，就不解除阻断。`notifyCaughtUp` 不是每笔不确定出站请求最终处置的证明；`findOrders` 只包含交易所已接受的委托，不能用查不到来推断拒绝。重启前应保留和审查审计，不能用新进程抹掉发送不确定性。
- `reconcile` 主动断开以触发新登录 / 追平，断线保留原有预占。跨交易日、柜台切换、持续成交中的重放顺序和 SDK 查询的线程保证仍需柜台仿真验证。按交易日重启并完成恢复；不按自然午夜重置仓位。
- `notifyTradingSegment` 的参数没有被解释成“连续交易中”。首期不提供已验证的实时交易时段门控，使用原生权限、合约有效期和 `checkOrder` / 柜台验证；本地不能保证休市时提前拒绝。后续接入可靠交易状态源后再启用相应门控。
- 权限 / 账户变化会阻断新增订单并要求恢复；可靠队列溢出通过队列外故障标记使进程停下，不静默继续。SDK `ServerRestarted` 回调会触发 `startDestroy`，避免厂商默认直接退出进程；之后需要新建实例并恢复。
- SDK 生命周期遵循异步 `startDestroy` → `notifyAfterApiDestroy`，不直接 `delete api`。若 5 秒未完成销毁，保留回调对象到进程退出，避免释放仍可能被调用的地址；已启动的动态库保持映射，避免 SDK 尾部线程返回到卸载代码。
- 日志是后台 JSONL，100 ms 刷盘，尚未接现有 RocksDB / uniform_orders；提交前入日志队列不等于掉电持久化。初版也没有自动做市、追单、实时保证金计算或端到端延迟承诺。

## 本地验证

```bash
cargo fmt -p cn_futures --check
cargo test -p cn_futures
YIDA_SDK_DIR=/absolute/path/to/yida/1.188.78.0 \
  cargo test -p cn_futures --features yida-sdk
YIDA_SDK_DIR=/absolute/path/to/yida/1.188.78.0 \
  cargo clippy -p cn_futures --all-targets --features yida-sdk -- -D warnings

python3 crates/cn_futures/tests/yida_bridge_smoke.py \
  --sdk-dir /absolute/path/to/yida/1.188.78.0 \
  --binary target/debug/yida-trader
```

合成 SDK 按实际头文件生成未使用接口 stub，验证真正的 Rust / C++ 边界：原生生命周期、追平、手工引用 0、交易所同号隔离、平今平昨拆单、FAK / FOK、重复成交、系统号撤单、同步 / 异步拒单、撤单失败、发送不确定以及重连恢复。它不连接网络，也不能替代真实柜台仿真。CTP 的既有测试和合成桥接联测同时回归。
