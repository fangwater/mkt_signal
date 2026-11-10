# CTP 中金所交易实现

`crates/cn_futures` 提供一个独立的国内期货交易核心和 `ctp-trader` 入口。CTP 接入直接调用上期技术官方 **6.7.11 Trader API**；“交易网关”在这里就是 SDK 适配模块，不是额外进程。行情暂不接入，调用方在同一个业务线程中通过 `SingleLoop::submit` / `cancel` 提交交易意图。现有加密交易策略的兼容不在本次范围内，不增加策略兼容层。

当前完成的是可编译、可本地测试的第一版交易接入，**没有连接真实期货公司，没有经过中金所仿真账户或实盘联调**。易达 SHFE / INE 原生交易初版见 [易达接入说明](yida_trade.md)，共用 `cn_futures` 核心，已通过本地合成 SDK 桥接联测与官网公共仿真。

## 已实现范围

- CFFEX 期货：IF、IH、IC、IM、TS、TF、T、TL，必须显式配置当前有效合约；仅支持投机仓。期权、组合、套利与套保仓不在首期范围。
- `ReqAuthenticate` → `ReqUserLogin` → `ReqSettlementInfoConfirm` → 合约、资金、委托、成交、持仓查询。
- 限价下单、撤单，以及 SDK 对应的 FAK / FOK 参数映射；FAK / FOK 默认关闭，需在真实仿真柜台逐项验证后启用。
- 订单引用、原始前置 / 会话身份、交易所订单号与成交身份关联；异步业务拒单映射回原始请求，不以重连后的当前会话替代原始身份。
- 一个业务线程负责风控、预占、委托、今昨仓、成交去重。官方 SDK 的网络 / 回调线程只向有界队列投递固定结构体。
- 订单数量、逐侧仓位、全部未成委托、账户报撤单频率、本地资金预算、自成交、合约活跃性与连续交易状态检查。
- 每 60 秒完整刷新合约规则；完整快照遗漏或标记不活跃的合约立即失去新下单权限。每 5 秒查询 CNY 可用资金，只能收紧本地预算。
- 启动 / 重连保留交易阻断，使用两轮无并发回报且完全一致的委托、成交和持仓查询进行快照收敛。超过次数、查询错误或超时不进入可交易状态。
- 后台 RocksDB 归一化订单／事实成交写盘、JSONL 交易审计、凭据脱敏、有界可靠事件故障标记，以及 flow 目录排他锁。不在启动或退出时自动撤销账户已有订单。

## Maker / taker 基础能力

当前接口接收调用方给定的限价和有效方式，没有独立的 maker / taker 自动执行算法：

| 用法     | 已有能力                                                   | 边界                                                                   |
| -------- | ---------------------------------------------------------- | ---------------------------------------------------------------------- |
| 挂单     | 普通限价 GFD（`day`）、撤单、部分成交与剩余冻结管理        | 没有 post-only 保证；普通限价单若穿过盘口，也可能立即成交              |
| 主动成交 | 使用可成交限价；也可使用 FAK（余量撤销）或 FOK（全成或撤） | 价格由调用方给定，不自动取对手价；FAK / FOK 默认关闭，待仿真验证后开启 |
| 自动执行 | 调用方可依据回报显式下单或撤单                             | 尚未实现盘口定价、撤单重挂、自动追价、超时补单或自动滑点保护价计算     |

所有报单当前都映射到 CTP 限价指令，没有发送无价格约束的市价单。指定的限价是订单的成交价格边界；它不意味着本模块已经实现按盘口计算保护价的算法。maker / taker 的实际成交角色也不能仅由 `day` / `fak` / `fok` 推定。

## 代码边界

| 文件                                       | 职责                                             |
| ------------------------------------------ | ------------------------------------------------ |
| `crates/cn_futures/src/model.rs`       | 重导出共享交易契约，另有适配器风控验证         |
| `crates/cn_futures/src/core.rs`        | 单线程状态所有者、预占、成交核销、直接 SDK 调用  |
| `crates/cn_futures/src/ctp/session.rs` | 请求关联、认证登录、查询限速、快照收敛与周期刷新 |
| `crates/cn_futures/src/ctp/sdk.rs`     | Rust SDK 生命周期、回调入口、原始请求上下文      |
| `crates/cn_futures/cpp/ctp_bridge.cpp` | 按官方头文件编译的 C++ / Rust 桥接与原生字段映射 |
| `crates/cn_futures/src/runtime.rs`     | 同一业务 loop、后台输入 / 审计、明确的交易开关   |
| `config/ctp_trade.example.toml`            | 不包含凭据的配置示例                             |

默认 Cargo feature 不依赖 SDK；`ctp-sdk` feature 编译真实 C++ 桥接。没有官方 SDK 时仍可运行核心与生命周期测试，但网络接入会明确报错，不会换用其他柜台或模拟实现。

## 官方 SDK 与构建

将匹配的以下组件放在仓库外，头文件和动态库必须来自同一个官方版本：

```text
ThostFtdcTraderApi.h
ThostFtdcUserApiStruct.h
ThostFtdcUserApiDataType.h
libthosttraderapi_se.so
```

`CTP_SDK_DIR` 指向头文件所在目录。运行时 `sdk_library` 指向该配套动态库，二进制通过 `dlopen` 加载，不把本机 SDK 路径编进 ELF 的 RPATH。当前桥接针对 Linux x86-64、官方 6.7.11 ABI，加载其他版本会拒绝启动；升级 SDK 应同步核实头文件、工厂参数和虚表，再重新编译验证。

```bash
CTP_SDK_DIR=/absolute/path/to/ctp/6.7.11 \
  cargo build -p cn_futures --features ctp-sdk --bin ctp-trader
```

不连接网络的检查：

```bash
cargo run -p cn_futures --bin ctp-trader -- \
  --config config/ctp_trade.example.toml --check-config

CTP_SDK_DIR=/absolute/path/to/ctp/6.7.11 \
  cargo run -p cn_futures --features ctp-sdk --bin ctp-trader -- \
  --sdk-version /absolute/path/to/ctp/6.7.11/libthosttraderapi_se.so
```

默认只校验配置，不读取凭据、不创建 SDK 对象、不连接前置。`--sdk-version` 只加载库并读取版本，不调用 `Init`。

本地构建验证采用 [vnpy_ctp 的公开镜像](https://github.com/vnpy/vnpy_ctp/tree/1be9bfb5292208e5c258cc769c90be4abe83f9a9/vnpy_ctp/api) 所附官方 6.7.11 头文件和 `.so`；没有使用 vnpy 的 Python 封装或运行时。动态库离线返回 `v6.7.11_20250617 16:22:00.10369`，并已验证创建 / 释放对象。SDK 未放入仓库。正式接入可使用上期技术官方下载的相同版本组件，并与期货公司确认柜台兼容性。

该次验证所用 `libthosttraderapi_se.so` SHA-256：

```text
ad4da95953ea5d0f4ed28ac746462ddba1f3434d0df1727919ae0d97591b4085
```

## 仿真连接配置

复制示例到环境目录后，在本地填写 SDK 路径、独立 flow / journal 路径、前置、BrokerID、合约白名单、账户预算和风控参数。示例前置是 `127.0.0.1:1` 占位地址，没有写死任何公共测试柜台。

凭据只从配置指定的环境变量读取：`CTP_USER_ID`、`CTP_PASSWORD`、`CTP_APP_ID`、`CTP_AUTH_CODE`。InvestorID 若与 UserID 不同，配置独立的 `investor_id_env`。不要将实际值放到示例、聊天、命令输出或提交中。

`production_mode` 是官方 6.7.11 工厂方法的参数：`false` 选择测评 API，`true` 选择生产 API。必须按前置提供方要求配置；这个布尔值本身不能证明某个地址是仿真环境。

以下仅说明**本地仿真**的操作入口，不是已执行的连接或生产部署步骤：

```bash
# 目标为配置中的仿真账户及 CFFEX 合约。
# 连接、认证、登录、确认当日结算并查询；不允许下单或撤单。
target/debug/ctp-trader --config /absolute/path/ctp-sim.toml --connect

# 同一仿真目标，额外允许 stdin 明确提交的下单 / 撤单命令。
target/debug/ctp-trader --config /absolute/path/ctp-sim.toml --connect --execute
```

`--connect` 会调用结算确认，不能将其描述为完全不改变柜台状态。`--execute` 只开启明确命令入口，不会自动生成策略订单。生产环境仍应使用 env-local 启停脚本和仓库既有发布规范；本次没有部署脚本、远程操作或生产二进制发布。

中金所联调优先申请期货公司的 CTP 仿真账户、交易前置和程序化接入权限；公共 SimNow 的最新中金所品种开放情况未在本次验证中确认，不假设其支持 IF / T。

## 命令与同 loop 集成

stdin 是人工仿真联调入口，输入在线程外读取；调用方直接使用核心 API，无需经 stdin、HTTP 或 IPC。所有命令都是单行 JSON，未知字段 / 指令会拒绝。

```json
{"op":"state"}
{"op":"submit","intent":{"venue":"CFFEX","symbol":"IF2612","side":"buy","offset":"open","lots":1,"price_ticks":20000,"time_in_force":"day"}}
{"op":"cancel","key":{"session":"ctp:7:11","order_ref":"101"}}
{"op":"reconcile"}
{"op":"shutdown"}
```

合约、价格和订单身份均为示例；撤单应使用实际回传的完整 `key`。`price_ticks` 是整数价格档位，实际限价等于 `price_ticks × PriceTick`。核心目前接受 CFFEX `open` / `close`，不把上期所的平今 / 平昨指令映射直接套到中金所。CFFEX 通用平仓的今昨仓核销顺序通过 `close_priority` 显式配置，需在实际柜台验证。

命令统一返回 `submit_receipt`；`children` 中逐项保留实际订单 key、开平、手数、结果和错误。CTP 不拆日期，只有一个子项；子项 `submitted_locally` 仅说明 SDK 同步返回成功。它不等于柜台确认，更不等于成交。持仓只随事实成交改变；撤单请求不释放冻结量；撤单终结中已报告但尚未收到事实成交的数量继续占用，直至成交事实到达或完成对账。

`reconcile` 在没有控制查询占用时启动；成功启动后暂停新交易，重新查询并收敛。stdin EOF 或 `shutdown` 会退出进程，**不自动撤销交易所挂单**。flow 目录不能被两个实例共用，同一账户的风险额度也不能由其他独立进程重复使用。

## 风控、恢复与持久化的实际限制

- `cash_budget_per_open_lot` 是显式配置的本地保守资金预算，**不是已经从柜台查询并计算的实际保证金率**。应根据真实保证金、手续费与缓冲配置；它不替代柜台保证金风控。首期没有实现实时保证金模型、强平风控或策略资金分配。
- 成交开仓后，已消耗的本地预算继续保留；周期资金查询只收紧预算。只有完整重对账可以恢复基准，避免把陈旧资金快照错误用作新增额度。
- SDK 私有流使用 RESUME，公共状态流使用 RESTART。新连接会清除旧交易状态许可，只有实际收到的连续交易状态才允许新下单；未收到状态时不会人为放开。
- 两轮查询一致是一种保守收敛检查，**不是 CTP 提供的原子快照 / 全序屏障**。需要在真实仿真柜台验证重放、查询与在途事件的顺序，尤其是持续成交、断线期间成交和跨交易日场景。不可据此宣称生产恢复已获验证。
- 交易证据先进入可靠审计队列，再尝试解析。未知订单、非数字强平身份、无法关联的成交均保留原始事件并阻断新交易，等待查询核对，不丢弃或杜撰策略归属。
- 审计包含交易事实与发往 SDK 前的本地订单身份；后台约每 100 ms flush / sync。**它不保证报单前已刷盘或掉电零丢失**。入口 / 审计故障会显式阻断；恢复时须以柜台事实查询补齐。
- JSONL 是首期独立原生审计存储，尚未接入统一订单数据库、账户监控和 Viz；执行入口与现有加密交易策略独立，不包含策略兼容层。设计展示页仍是静态演示，不控制此运行时。
- 单业务线程不等于已完成绑核、SDK 线程亲和性或端到端性能优化；本次没有给出微秒延迟或 p99 指标。后续应按设计页的边界实测。

## 订单持久化

交易回报直接进入 `persist_manager` 的嵌入式 RocksDB，未引入 IPC。发送前落盘选择、恢复屏障、原生身份、去重与 `cn_order_export` 读取方式见[国内订单持久化](cn_order_persistence.md)。桥接联测前须构建与交易程序同目录的 exporter。

## 验证

```bash
cargo test -p cn_futures
CTP_SDK_DIR=/absolute/path/to/ctp/6.7.11 \
  cargo test -p cn_futures --features ctp-sdk

CTP_SDK_DIR=/absolute/path/to/ctp/6.7.11 \
CTP_TEST_SDK_LIBRARY=/absolute/path/to/ctp/6.7.11/libthosttraderapi_se.so \
  cargo test -p cn_futures --features ctp-sdk \
    official_sdk_constructs_and_releases_without_connecting -- --ignored

python3 crates/cn_futures/tests/ctp_bridge_smoke.py \
  --sdk-dir /absolute/path/to/ctp/6.7.11 --binary target/debug/ctp-trader
```

最后一项用官方头文件生成独立的**测试假库**，驱动实际 Rust / C++ 桥接和业务 loop，验证认证、两轮查询、限价撤单、FAK/FOK、重复成交、平仓、异步拒单和审计。假库不发起网络连接，不是运行时备用后端，不能代替中金所仿真联调。
