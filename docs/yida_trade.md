# 易达原生交易：SHFE / INE

最后更新：2026-11-10 UTC。

`crates/cn_futures` 现有 `ctp-trader`（中金所）和 `yida-trader`（上期所 / 上海国际能源交易中心）两个入口，共用一个交易核心。易达直接调用 Hanlin **YDExtendedApi**，没有通过易达的 CTP 兼容接口。行情独立，暂按 CTP 考虑，本次没有接入易达行情。

实现已完成本地单元测试、按真实原生头文件编译的合成 SDK 桥接联测，以及官网公共仿真的真实 SDK 联调。TCP / UDP / XTCP 在 SHFE `cu2612`、INE `sc2612` 上均已验证限价报撤单、限价 FAK / FOK 开平仓、原生市价 Day / FAK / FOK 开平仓、事实成交和重连对账。**没有生产部署或端到端性能测量**。

## SDK 依据

以用户提供的 **`ydClient_1_502_53_182.tgz`** 为当前配套 SDK，使用其中 `ydAPI_c++/include/`、`ydAPI_c++/linux64/yd.so` 和 233 页的 **1.502** 编程指南。官网[下载页](https://www.hanlinit.com/downloads/)及[文档页](https://www.hanlinit.com/docs/)匿名访问需要登录；没有据此确认官网最新版本，也没有确认某家期货公司柜台允许的 API 版本范围。此前单独提供的 132 页指南适用于 1.386，不作为本次 ABI 依据。公共仿真的服务器配置独立生成；官网仿真授权参数只从环境变量注入，不复制到仓库。

SDK 文件留在仓库外；代码使用配套的 `ydApi.h` / `ydDataStruct.h` / `ydDataType.h` 编译 C++ shim，通过 `makeYDExtendedApi` 和 `getYDVersion` 加载 `yd.so`。运行时严格核对 **1.502.53.182**，其他版本必须先核对头文件 / ABI 并重新验证。只维护当前这一套桥接和内部事件结构。

| 文件 | SHA-256 |
| --- | --- |
| `ydClient_1_502_53_182.tgz` | `e0376895c536a2a2cdd1e79595b08d9d4d003ef7c3941cd68f497882130a4e70` |
| `include/ydApi.h` | `95dc94d85a3a07e57af833babfeaed5cd6cdd22dac5c0e3d90166939871d8392` |
| `include/ydDataStruct.h` | `49a9156b9bcf054ddb4f11a930753c6eb592723c4c18f6141e556c66a575844a` |
| `include/ydDataType.h` | `f070673c1a9eb5ff5b7037bbbbef30912a86c5a180d744ae6e7734d89bee7cbf` |
| `linux64/yd.so` | `ac2e0863feba711a91ffcd6f91c22d7ba80bee9ecbcd17cd4ec5f63c2003f16a` |

真实 SDK 初始化器会尝试执行 `/sbin/udevadm` 和 `sudo`。本地仿真登录时，`udevadm` 不存在且 `sudo` 提示需要口令，没有提供提权凭据或修改系统权限；该测试柜台仍允许登录。`--sdk-version` 同样会执行厂商库初始化代码。正式使用应核对厂商安装要求与期货公司配套版本。

## 构建与配置

```bash
YIDA_SDK_DIR=/absolute/path/to/ydClient/ydAPI_c++/include \
  cargo build --release -p cn_futures --features yida-sdk --bin yida-trader

# 只检查项目配置结构；不加载 SDK、不读凭据、不连接。
cargo run -p cn_futures --bin yida-trader -- \
  --config config/yida_trade.example.toml --check-config
```

以 `config/yida_trade.example.toml` 为起点，在独立环境目录中填写库路径、柜台提供的原生服务器配置路径、日志路径、合约白名单和保守资金预算。`trading_protocol` 必须显式填写 `tcp`、`udp` 或 `xtcp`，示例选择 `xtcp`。凭据从配置指定的 `YIDA_USER_ID`、`YIDA_PASSWORD`、`YIDA_APP_ID`、`YIDA_AUTH_CODE` 环境变量读取，不放入配置、仓库或命令参数。

原生服务器配置仍由期货公司提供。当前代码在加载库前检查以下 `key=value` 配置，其他参数按厂商要求保留：

```ini
# 地址仅为占位，实际使用期货公司提供的交易端点。
TradingServerIP=127.0.0.1
TradingServerPort=1
TradingProtocol=XTCP
XTCPTradingType=automatic
ConnectTCPMarketData=no
ReceiveUDPMarketData=no
RecalcMode=off
```

原生 `TradingProtocol` 必须与项目 `trading_protocol` 一致，移除旧 `UDPTrading`。库加载前会校验配置，SDK 启动前还会用 `getConfig` 再核对协议。切换 UDP 时两处分别填写 `udp` / `UDP`，并设置 `UDPTradingType=sf`、`exa`、`yusnic` 或 `socket`；不接受可能静默降级的 `automatic`。XTCP 要显式选择 `XTCPTradingType=automatic` 或 `socket`。`socket` 用于普通内核网络，不能据此宣称达到厂商硬件加速性能。

UDP / XTCP 发单仍由 SDK `insertOrder` / `cancelOrder` 在调用线程完成；回报始终通过 TCP。恢复时读取柜台系统参数 `UDPTradingPort` / `XTCPTradingPort`，端口为 0 或缺失时禁止报撤单。XTCP 还必须收到 `YD_AE_XTCPTradeConnected`。未开通、未连上或已断开时不主动调用 SDK 报撤单；已有可用连接断开后，阻断新下单，需要连接恢复后显式 `reconcile`。`state` 返回 `trading_protocol`、`transport_ready` 和综合的 `can_trade`；`transport_status` 记录状态和柜台下发端口。

厂商 SDK 在 XTCP 断线时自身可以退回 TCP，未提供原子“只允许 XTCP”发送接口。桥接在每笔 SDK 调用前检查连接状态，但无法消除检查与 SDK 内部断线处理之间的竞态，不能承诺从未发生 TCP 回退。必须在配套柜台上验证连接事件、重连及实际网络发单路径。

交易运行时关闭所有易达行情连接，联调的盘口与涨跌停价通过独立的只读行情探针取得。`RecalcMode=off` 避免在缺少行情时假装持续重算保证金 / 浮盈亏；因此 SDK 本地 `Available` / `usable()` 不能当作独立、实时的柜台风险模型。需要按实际保证金、手续费和价格风险配置保守 `cash_budget_per_open_lot`，并在仿真中核对。资金事件只收紧本地预算，完整恢复才建立新基准。

`idle_sleep_us=0` 使用忙轮询。可在项目配置里指定 `business_core` 绑定单个业务核；后台输入、审计和 SDK 先启动，再绑定当前业务线程，避免后台工作继承该单核亲和性。使用此选项时，原生配置必须明确给出 `TCPTradingCPUID`、`TimerCPUID`，XTCP 还需给出 `XTCPTradingCPUID`，且都与业务核不同。实际 CPU 编号由部署机器决定，示例没有硬编码。`TradingServerTimeout=-1` 可使 SDK 回报线程忙轮询；这会额外占用一个核，需按机器配置。尚无绑核后的真实柜台延迟指标。

**账户必须专用，且追平后的原生 `LoginCount == 1`。** 不能另起 CTP、YDClient 或其他策略独立使用同一账户的资金 / 持仓。flow 锁只防止相同目录被并发使用，不是跨机器的账户锁。默认恢复时若有 SHFE / INE 之外的持仓、非投机套保账本、期权、组合或不支持的事实类型，会保留原始事件并阻断交易，不做猜测映射。下述公共仿真模式只允许把该仿真柜台的既有期权仓位作为只读账户记录保留。

### 公共仿真柜台

用户于 2026-11-10 提供的三张截图（本地 `ds/yd/`）补充了[官网开发测试环境](https://www.hanlinit.com/docs/dev-environments/)的信息。截图中的普通柜台地址是 `118.190.175.212`，柜台版本为 `1.502.55`。与当前实现有关的端口如下，其他市场不能直接使用本适配器：

| 市场 | TCP 登录 / 回报 | TCP 行情 | UDP 报单 | XTCP 报单 | 当前原生易达范围 |
| --- | --- | --- | --- | --- | --- |
| 上期所 / 能源所 | 41600 | 41601 | 41602 | 41603 | SHFE / INE 期货 |
| 中金所 | 40600 | 40601 | 40602 | 40603 | 尚未接入；不能只修改端口切换市场 |

无论选择 UDP 还是 XTCP，`TradingServerPort` 都填写对应市场的 TCP 登录端口；需要按公网 / NAT 端口覆盖时，另外设置 `UDPTradingServerPort` / `XTCPTradingServerPort`。截图的配置示范采用 TCP，实际测试 UDP / XTCP 时仍须修改两处协议配置并选择相应发送器。

SDK 版本与柜台版本有不同用途：本地桥接严格匹配 SDK **1.502.53.182** 的 ABI；柜台则检查 API 的前三段 `1.502.53` 是否位于 `MinApiVersion` 与 `MaxApiVersion` 之间，第四段紧急补丁不参与该范围比较。按默认范围，截图的 `1.502.55` 柜台可以接受此 SDK；实际设置以登录结果及柜台系统参数为准，无需因为截图中的柜台版本而改动本地 SDK 版本校验。

截图给出公共仿真账户及原生 API 的登录授权参数，原生授权码与 ydCTP 的授权码不同。只从环境变量注入所选仿真账户及原生 API 凭据，不复制到仓库或命令参数。公共账户可能有其他使用者；不能跳过 `LoginCount == 1` 检查。截图说明每个账户预置 50 亿元权益，并已有各交易所多空双向持仓，因此必须完整恢复账户事实后再选择测试合约；不能假定空仓，或为了通过小额测试风控而清零 / 忽略初始持仓。超出 `max_position_lots_per_side` 的初始仓位可能阻止继续开仓。

截图说明期货使用前一交易日日初数据。先从柜台目录确认合约、交易日、tick、有效期与现有持仓，再填写合约白名单和价格；`cu2612` / `sc2612` 已在本次公共仿真中确认并联调，后续运行仍须重新核对。测试环境中的交易日也可能与自然日或回放数据日期不同，不能直接用本机日期替代。截图里的 `RecalcMode=auto`、`ConnectTCPMarketData=yes` 是全功能 SDK 配置，当前独立交易运行时仍要求 `off` / `no`，并使用显式保守资金预算。

公共仿真需显式设置 `public_simulation=true`，默认值为 `false`。此模式只接受 `118.190.175.212:41600`，原生配置限定为已知连接参数，禁止高可用或其他目标地址参数；SDK 创建后还会用 `getConfig` 核对实际加载的目标。必须设置 `max_order_lots=1`、`max_working_lots<=2`，追平后仍要求单连接。公共柜台实际报告 `Test=no`，所以此模式依据已确认的官网公开端点进行限定，不把该参数当作公共仿真标识。

公共仿真的已有投机期权持仓在完整快照校验成功后保存为 `read_only_positions`，同时保留原始审计，`state` 和 `reconciled` 返回这些记录；它们不进入期货下单持仓账本。合约白名单、C++ 提交检查和订单解析仍只接受 SHFE / INE 期货；期权委托 / 成交、未知冻结或其他不支持的账户事实仍会阻断交易。完整恢复失败时保留上一次已确认的只读持仓，不用部分快照覆盖。

2026-11-10 使用配套 SDK、release 二进制完成下表六组真实公共仿真测试，每组单笔 1 手，包含 11 笔报单、1 笔撤单和 10 笔事实成交。最终六组合计 **66 笔报单、6 笔撤单、60 笔成交回报**，其中 36 笔为原生市价 Day / FAK / FOK 的开平仓成交。测试前两个目标合约都没有持仓；每组结束后主动重连、用原生完整快照核对：测试持仓恢复、其他 10 条期货持仓及 20 条只读期权持仓不变，本次订单全部终结。最终账户快照没有期货挂单。

| 协议 | SHFE `cu2612` | INE `sc2612` |
| --- | --- | --- |
| TCP | 限价报撤、限价 FAK/FOK、市价 Day/FAK/FOK 开平、对账通过 | 限价报撤、限价 FAK/FOK、市价 Day/FAK/FOK 开平、对账通过 |
| UDP | 限价报撤、限价 FAK/FOK、市价 Day/FAK/FOK 开平、对账通过 | 限价报撤、限价 FAK/FOK、市价 Day/FAK/FOK 开平、对账通过 |
| XTCP | 限价报撤、限价 FAK/FOK、市价 Day/FAK/FOK 开平、对账通过 | 限价报撤、限价 FAK/FOK、市价 Day/FAK/FOK 开平、对账通过 |

对 SHFE 的 UDP / XTCP 两组使用不展开载荷的系统调用跟踪，分别确认 11 次 72 字节报单、1 次 40 字节撤单发往 `41602` / `41603`；这些测试实际使用了对应协议端口。测试使用普通内核网络 `socket` 发送器，未测硬件加速和端到端延迟。

真实 SDK 联调入口如下：

```bash
# 目标为本地配置指定的仿真账户、SHFE / INE 合约，仅登录与恢复。
target/release/yida-trader --config /absolute/path/yida-sim.toml --connect

# 同一目标，额外允许显式 JSON 下单 / 撤单。
target/release/yida-trader --config /absolute/path/yida-sim.toml --connect --execute
```

可复用的实际联调脚本为 `crates/cn_futures/tests/yida_public_sim_smoke.py`。它从环境变量读取凭据，默认只登录观察；`--execute` 要求 release 二进制和调用方明确提供三个限价的整数 ticks。执行前验证目标合约为空仓，结束时仅处理本次订单和新增的一手仓位，再完整对账。额外提供 `--market` 时，再测试原生市价 Day / FAK / FOK 各一笔开仓和平今。审计、stdout、stderr 与 `summary.json` 放在显式指定的新输出目录中。每次执行必须重新确认实际合约与价格，不能照搬历史测试价格。

```bash
python3 crates/cn_futures/tests/yida_public_sim_smoke.py \
  --sdk-root /absolute/path/to/ydClient/ydAPI_c++ \
  --binary target/release/yida-trader --output-dir /absolute/path/to/new-audit-dir \
  --protocol xtcp --venue SHFE --symbol cu2612
# 额外提供 --passive-price-ticks N --buy-price-ticks N --sell-price-ticks N --execute
# 同时验证原生市价时再加 --market
```

默认只检查配置。`--connect` 启动 SDK 并登录；没有 `--execute` 不允许下单和撤单。启动、EOF、`shutdown` 或错误退出均不会自动撤销交易所挂单。

## 一套交易契约

一个业务线程直接执行意图、风控、预占、OMS 和 SDK 提交；SDK 内部线程回调只传递有界队列中的复制事件。异步 JSONL 审计和 stdin 读取在后台。没有策略兼容层，也没有风险到执行之间的 IPC。

| 能力 | 原生映射与约束 |
| --- | --- |
| 普通限价 | `OrderType=YD_ODT_Limit`，提供具体 tick 价格；没有 post-only 保证 |
| FAK / FOK | 原生 `YD_ODT_FAK=1` / `YD_ODT_FOK=3`；默认关闭，需按实际账户 / 合约验证 |
| 原生市价 | `price_type="market"`，保留 Day / FAK / FOK；分别映射 `YD_ODT_Limit=0` / `YD_ODT_FAK=1` / `YD_ODT_FOK=3`，并设置对应交易所的 `YD_EOA_SHFE_MarketPrice` / `YD_EOA_INE_MarketPrice=1` |
| 开仓 | `YD_OF_Open` |
| 平今 / 平昨 | `YD_OF_CloseToday=3` / `YD_OF_CloseYesterday=4`；SHFE / INE 不直接发送通用 `Close` |
| 通用平仓意图 | 按配置优先级规划今昨子单，所有子单先通过本地风控与预占，再逐笔提交 |
| 跨日期 FOK | 拒绝拆分，两个独立 FOK 不保证父单全部成交或全部撤销 |
| 订单引用 | 先调用 SDK `getNextOrderRef(0, true)` 分配组 0 的正 int，再本地预占 / 审计；`checkOrder` 验证后以 `insertOrder` 发送，保留该引用 |
| 撤单 | `cancelOrder` 使用交易所作用域内的 64 位 `LongOrderSysID`；尚无系统号时拒绝发出撤单，等待确认 |
| 权限 | 每次提交重查账户 / 交易所 / 品种 / 合约权限，取最严格值；`CloseOnly` 拒绝开仓 |

SHFE / INE 的市价按配套指南 7.1.1.1 使用 `ExchangeOrderAttribute`，不发送其他交易所采用的 `YD_ODT_Market=2`。输入的 `price_ticks` 必须为 0，表示没有投资者限价；C++ 提交时从原生合约初始化数据中取买单涨停价 / 卖单跌停价填入 `Price`，满足柜台的价格风控，同时始终保留市价属性。此字段不构成市价成交价格承诺，也不把市价降级成限价 FAK。公共仿真实测将该字段留 0 会收到 `YD_ERROR_PriceOutOfLimit=40`；缺少有效原生涨跌停数据时在本地拒绝（9903），不发送。该数据来自初始化，无需开启实时行情连接。

市价使用独立的 `MinMarketOrderVolume` / `MaxMarketOrderVolume`，数量必须是最小量的整数倍；规则缺失时不借用限价规则。任一方向存在未结订单时，反向市价会被自成交检查拒绝；存在未结市价时也拒绝反向限价。普通限价仍按价格判断。CTP 入口本次没有启用市价。原生指令和传输协议是两件事：此前 FAK / FOK 就已直接使用原生 SDK；本次没有比较不同订单类型的端到端延迟，不能据此宣称市价更快。

未调用会重新分配 OrderRef 的 `checkAndInsertOrder`，避免已经登记的预占和实际订单失去关联。普通 `insertOrder` 在回报前不会更新 SDK 本地冻结；这个间隙由共享交易核心的预占覆盖，SDK `checkOrder` 是附加检查。

stdin 每行一个 JSON。CTP 和易达使用同一 `Intent` 与 `OrderKey` 结构，key 的 session 内容只由各自适配器解释：

```json
{"op":"state"}
{"op":"submit","intent":{"venue":"SHFE","symbol":"cu2612","side":"sell","offset":"close","lots":1,"price_ticks":350000,"time_in_force":"day"}}
{"op":"submit","intent":{"venue":"SHFE","symbol":"cu2612","side":"buy","offset":"open","lots":1,"price_type":"market","price_ticks":0,"time_in_force":"fak"}}
{"op":"cancel","key":{"session":"yida:20261109","order_ref":"101"}}
{"op":"reconcile"}
{"op":"shutdown"}
```

`price_type` 省略时为 `limit`。这些只是合约 / 价格 / 身份示例；撤单必须使用实际回传 key。原生无月份的品种代号、期权和 TAS 不在首期范围。

`submit_receipt` 包含每个子单的 `key`、开平、手数和结果：`submitted_locally`、`rejected_locally`、`uncertain`、`not_sent`。中途失败也保留已发出子单的身份；后续未发子单释放预占。没有自动重试或自动撤回已经发出的兄弟单。

`checkOrder` 返回失败可以确定尚未发送，因此释放本地预占。`insertOrder` / `cancelOrder` 返回 false、桥接异常或未知发送结果保守视为不确定，保留预占并阻断新增订单。SDK 返回 true 也只表示本地提交，不等于柜台确认或成交。

本地发送成功后，如果 `response_timeout_ms` 内没有收到该订单的首次柜台 / 交易所回报，或撤单请求发出后未收到订单终态，就保留预占并阻断新订单，记录含 `request.operation=submit/cancel` 的 `trading_blocked`，不自动重发。这也覆盖 UDP 成功发出但丢包的情况。超时后即使收到迟到回报也不自动解除阻断，需要完整对账。成功对账后以柜台的当前订单状态恢复撤单等待，不自动重发撤单。此 SDK 没有旧 `notifyFailedOrder` 回调；柜台和交易所拒单都通过 `notifyOrder` 的拒绝状态处理。

按配套指南 7.1.2.4，`notifyFailedCancelOrder` 与发出的撤单在数量和内容上不保证严格对应，因此仅作为审计证据；即使编号看起来匹配，也不据此清除 `cancel_pending` 或释放预占。订单终态或完整恢复负责更新订单状态。原始审计保留 `OrderType` 与 `ExchangeOrderAttribute`，订单解析与恢复保留市价 / 限价类型；仅对市价委托允许零价格，事实成交仍须为有限正价。解析器拒绝不一致的状态 / 错误码 / 数量、非有限价格、无效标识与不支持的市场事实；原始回报先入审计队列再解析，失败时阻断新交易。恢复快照中发生权限变更也会使该快照失效。

## 身份、恢复与限制

- 系统订单号按 `(venue, LongOrderSysID)` 关联，成交按交易所、合约、交易日、`LongTradeID` 和方向去重，Rust / C++ 边界保留完整 64 位编号。32 位 `OrderSysID` / `TradeID` 仅保留在原始审计中；不拿它们作去重或撤单身份。允许恢复 `OrderRef=0` 的外部手工订单，SHFE / INE 的同号订单不会覆盖。仅支持报单组 0、普通成交和有对应委托的成交，其他事实保留证据后阻断。恢复订单没有策略归属；未知成交保留原始证据后要求对账。
- 原始审计保留原生时间、错误码、整数订单 / 成交身份、厂商可提供的交易所原始 ID，以及 `YDTrade.Commission`。共享持仓只随事实成交改变，不把累计委托成交量编造成成交。
- `notifyFinishInit` 只说明 API 可调用，**不说明订单 / 成交已经追平**。每次登录必须等待 `notifyCaughtUp`；在该回调中复制原生账户、目录、分日期持仓、`findOrders` 和 `findTrades`，业务线程校验委托成交量与事实成交量、原生平仓冻结与恢复委托的预占一致后整体恢复。读取指针不跨 FFI 保留；`find*` 不进入逐单热路径。
- 追平前 SDK 还会重放没有交易所系统号的历史拒单。仅在完整快照开始前校验并保留这类原始证据，不给它们分配策略归属，不修改持仓或释放当前预占；快照中插入实时拒单仍会使恢复失败。运行期间未知拒单仍需对账。
- 追平前的 `notifyExtendedAccount` 会回放历史资金及连接数，实际观察到 `LoginCount=0` 的旧值。这些回报先入审计，只有追平快照建立可用资金和单连接基准；已恢复后的实时账户更新仍完整校验并只收紧预算。
- TCP 回报通道重连时，SDK 可能保留独立 XTCP 连接，不再次回调 `XTCPTradeConnected`。仅在确实收到 XTCP 断开或柜台切换时清除 XTCP 连接事实；TCP 断开期间仍通过登录 / 追平门控禁止发送。XTCP 新连接也可能晚于 `notifyCaughtUp`，`reconciled` 只表示账户快照就绪，实际可交易性以 `state.can_trade` 为准。
- 快照中若插入独立实时回调、出现不支持的事实、遗漏尚未确定结果的本地订单或对账失败，就不解除阻断。`notifyCaughtUp` 不是每笔不确定出站请求最终处置的证明；`findOrders` 只包含交易所已接受的委托，不能用查不到来推断拒绝。重启前应保留和审查审计，不能用新进程抹掉发送不确定性。
- `reconcile` 主动断开以触发新登录 / 追平，断线保留原有预占。跨交易日、柜台切换、持续成交中的重放顺序和 SDK 查询的线程保证仍需柜台仿真验证。按交易日重启并完成恢复；不按自然午夜重置仓位。
- `notifyTradingSegment` 的参数没有被解释成“连续交易中”。首期不提供已验证的实时交易时段门控，使用原生权限、合约有效期和 `checkOrder` / 柜台验证；本地不能保证休市时提前拒绝。后续接入可靠交易状态源后再启用相应门控。
- 权限 / 账户变化会阻断新增订单并要求恢复；可靠队列溢出通过队列外故障标记使进程停下，不静默继续。SDK `ServerRestarted` 回调会触发 `startDestroy`，避免厂商默认直接退出进程；之后需要新建实例并恢复。
- SDK 生命周期遵循异步 `startDestroy` → `notifyAfterApiDestroy`，不直接 `delete api`。若 5 秒未完成销毁，保留回调对象到进程退出，避免释放仍可能被调用的地址；已启动的动态库保持映射，避免 SDK 尾部线程返回到卸载代码。
- 日志是后台 JSONL，100 ms 刷盘，尚未接现有 RocksDB / uniform_orders；提交前入日志队列不等于掉电持久化。初版也没有自动做市、追单、实时保证金计算或端到端延迟承诺。

## 本地验证

2026-11-10 验证结果：CTP + 易达双 SDK feature 的 76 项常规单元测试通过；默认忽略的 CTP 官方库离线构造 / 销毁测试单独指定配套动态库后也通过。release 二进制通过 27 个易达合成 SDK 场景（含联调脚本拒绝触碰已有一手持仓的保护）、CTP 桥接回归及上述 6 组实际公共仿真测试，`cargo fmt`、双 SDK `clippy -D warnings` 和本地 release 构建通过。订单解析覆盖两市场、双方向、三种开平、五种状态的 60 个组合，以及无效身份 / 数量 / 价格 / 错误状态等拒绝路径。

```bash
cargo fmt -p cn_futures --check
cargo test -p cn_futures
YIDA_SDK_DIR=/absolute/path/to/ydClient/ydAPI_c++/include \
  cargo test -p cn_futures --features yida-sdk
YIDA_SDK_DIR=/absolute/path/to/ydClient/ydAPI_c++/include \
  cargo clippy -p cn_futures --all-targets --features yida-sdk -- -D warnings

python3 crates/cn_futures/tests/yida_bridge_smoke.py \
  --sdk-dir /absolute/path/to/ydClient/ydAPI_c++/include \
  --binary target/debug/yida-trader
```

合成 SDK 按配套 1.502.53.182 头文件生成未使用接口 stub，覆盖 TCP / UDP / XTCP 三种配置，验证原生生命周期、追平、手工引用 0、交易所同号隔离、超 32 位订单 / 成交编号及相同截断编号的隔离、平今平昨拆单、FAK / FOK、重复成交、64 位系统号撤单、同步 / 异步拒单、发送不确定以及重连恢复。额外覆盖三协议 × 两交易所原生市价 Day / FAK / FOK 映射、零价格市价回报与正价事实成交、独立数量规则、市价自成交检查与恢复、缺少原生涨跌停数据时不发送、无系统号历史拒单重放、只读模式、只许平仓权限、多连接账户保护、终态先于事实成交、部分成交部分撤单、冲突重复成交、畸形委托回报、UDP / XTCP 未开通、XTCP 未连上和断线、下单与撤单无回报超时、不能关联的撤单错误审计，以及对账后恢复。在允许使用至少两个 CPU 的 Linux 上，还验证业务线程单核亲和性以及后台输入 / 审计线程保留原有亲和性。它不连接网络、不验证实际协议报文，也不能替代真实柜台仿真。CTP 的既有单元测试和合成桥接联测同时回归。
