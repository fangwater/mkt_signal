# CTP 国内期货行情与交易

最后更新：2026-11-11

**运行地点限制：禁止在当前开发机和所有内网机器运行 CTP，包括实盘、仿真、只读、离线 SDK、连接探针及执行 CTP 的测试/基准。** 本机仅可编辑、静态检查、格式化和编译；本页运行、SDK 版本查询及测试命令仅可在用户明确指定并授权的外部主机执行。历史本机测试记录不构成继续运行的授权，完整规则见 [AGENTS.md](../AGENTS.md#ctp-execution-location-restriction)。

`crates/cn_futures` 直接使用上期技术官方 **CTP 6.7.11**，提供 `ctp-trader` 和 `ctp-md`。交易核心、风控、订单状态和持久化与易达共用；SDK 网络线程只投递有界原始事件，业务状态由单一线程维护。不使用第三方 CTP 兼容柜台作为运行时替代。

支持 CFFEX（中金所）、SHFE（上期所）、INE（能源中心）、DCE（大商所）、CZCE（郑商所）、GFEX（广期所）的普通投机期货。仅显式配置并由柜台确认有效的合约可报新单。期权、组合、套利和套保账本不在本次范围；已有易达期权功能保持其独立边界。

本地单元测试和合成 SDK 联测不能证明柜台已接受实盘登录或订单。2026-11-11 的创元连接记录见后文；截至该记录，所有入口均拒绝连接，尚未验证创元实盘行情、交易、手续费或账户查询权限。

## 合约与交易所语义

每个 `[[symbols]]` 必须显式填写 `venue` 和柜台原生 `symbol`。例如 `CFFEX/IF2612`、`SHFE/rb2701`、`INE/sc2701`、`DCE/m2701`、`CZCE/SR701`、`GFEX/lc2701`；这些仅是测试样本，不是当前主力合约或推荐交易标的。

郑商所保留三位交割月代码，不自动改写成四位；其他交易所保留原生大小写。合约存在性、活跃状态、到期日、乘数、tick size、限价单最小和最大手数均读取官方 `ReqQryInstrument`。不维护产品乘数常量表，不用 `1` 填补缺失乘数。完整快照遗漏或标记不活跃的合约失去新报单权限；另须实际收到交易所连续交易状态，不能人为放开。

- SHFE / INE 使用独立的平今、平昨指令；通用 `close` 由共享核心按实际今昨仓和 `close_priority` 拆分，逐子单返回结果、预占仓位并跟踪事实成交。
- CFFEX / DCE / CZCE / GFEX 使用通用平仓，不向它们发送上期所式日期指令。今昨仓在各市场账本内分别核算。
- 下单和撤单的 `ExchangeID` 来自意图中的交易所，不再写死 CFFEX；撤单保留原始 FrontID、SessionID、OrderRef 与 OrderSysID。
- 支持限价 GFD、撤单及 FAK/FOK 参数映射。FAK/FOK 默认关闭，柜台能否接受取决于具体交易所、品种及账户，须在相应仿真逐项验证。CTP 市价单仍不开放。
- 普通限价单没有 post-only 保证。maker/taker 角色取决于实际成交，不由有效方式推定。没有自动定价、追价或撤单重挂策略。

## 账户、手续费与保证金

`ReqQryTradingAccount` 的 **44 个数值字段**全部进入当前账户快照，包括权益 `balance`、可用资金 `available`、占用/冻结保证金、冻结资金、手续费及冻结手续费、持仓/平仓盈亏、出入金、可取资金、交割保证金、特殊产品字段和期权市值。`state` 同时返回 `account`、`settlement_confirmed` 和交易核心状态；尚未查询或已断线时账户快照不可用。账户更新每 5 秒查询一次，交易核心仅据此收紧已有资金预算，只有完整对账可恢复额度。

合约元数据包含 `contract_multiple`、`expiry_date`、`price_tick`、数量约束及有效状态。每个活跃白名单合约逐项查询：

1. `ReqQryInstrumentCommissionRate`：开仓、平仓、平今分别保留按金额与按手费率，两项相加。
2. `ReqQryInstrumentMarginRate`：保留多头与空头按金额/按手参数。
3. 若 `IsRelative=true`，另查 `ReqQryExchangeMarginRate`，将经纪商加收部分与交易所基础参数相加，得到绝对投资者保证金率。缺少基础参数不能当作已知保证金率。

费率响应按请求 ID、交易所、合约和投资者范围核对。柜台若以产品代码返回费率，只接受已查询到的该合约 `ProductID`，不按字符串猜产品。空结果、重复歧义、错误或不匹配均不能放开交易。查询间隔至少 1 秒；合约与费率在每 60 秒规则刷新时组成一个完整的新快照，完成后整体替换。

估算费用为 `手数 × (价格 × 合约乘数 × 按金额费率 + 按手费用)`；保证金按多空各自参数使用相同金额/手数结构计算。`CommissionRate::estimate` 与 `MarginRate::estimate` 提供显式估算方法；未区分日期的通用 `close` 按普通平仓与平今两者较高费用估算，只有明确的日期指令才使用对应费率。开仓资金预占取本地每手保守预算与“查询所得保证金 + 开仓手续费”两者较大值，不因查询而放松预算。它不代替柜台组合保证金、账户风险或强平计算。

**费率估算不是柜台已收的逐笔费用。** CTP 成交回报没有独立逐笔手续费字段，事实成交的费用保留未知；不能把账户累计手续费分摊或虚构成某一笔成交的事实费用。

## 只读查询、结算确认与恢复

默认只校验配置，不读取凭据、不创建 SDK 对象、不连接网络。`--connect` 认证、登录并查询结算确认状态、合约、费率、资金、委托、成交和持仓，默认不确认结算，不下单、不撤单。只有显式 `--confirm-settlement` 或配置 `confirm_settlement=true` 才允许调用结算确认。`--execute` 另行允许人工下单/撤单指令；当日结算未确认时即使带此开关，新交易仍被阻断。

启动及重连保留交易阻断，重新认证和登录，并要求两轮无并发委托/成交回报且内容一致的委托、成交、持仓查询。原生查询间没有原子快照屏障，这只是保守收敛检查。SHFE/INE 按 PositionDate 合并今昨仓；其他交易所使用 Position 与 TodayPosition，不误用开盘时的 YdPosition 代替剩余昨仓。

SDK 注册主/备用前置并负责传输重连。传输或控制响应超时后，运行时阻断交易，Release 原对象并记录所有已交付的尾部事件，再按退避时间重建连接、重新登录和对账。旧业务请求身份保留，不将旧会话拒单归给新会话。明确的认证/登录错误、歧义事件、审计故障等不会被当作网络超时无限重试。退出、重连均不自动撤销交易所挂单。

回报队列和审计故障显式阻断；未知订单、非数字交易所身份、强制平仓等事实先保留原始证据，再对账，不杜撰策略归属。订单和成交进入嵌入式 RocksDB，JSONL 为可读审计。存储、掉电与恢复边界见 [国内订单持久化](cn_order_persistence.md)。

## 持续行情服务

`ctp-md` 使用官方 MdApi，不创建 TraderApi。它连接配置中的多个前置，登录后订阅显式合约，并持续向 stdout 与私有 journal 输出同一套 JSON 行消息；`tick` 中包含五档买卖价量、最新价、成交量/额、持仓量、涨跌停价、柜台交易日/业务日期/更新时间及本机微秒接收时间。行情权限只有一档时其余档位按原始可用值保留，不补造五档。CTP 无效浮点哨兵转换成 `null`。

断线立即清除登录和订阅完成状态，输出 `gap=true`。重连必须重新登录并重新订阅全部配置合约，`connection_generation` 随会话增加。连接、登录或订阅长时间未完成时重建 SDK 并退避重试；已经完成订阅的连接不会因休市无 tick 就被判定掉线。明确的错误密码响应停止进程，不循环提交密码。

原生回调只投递固定结构体到有界队列，后台线程写 journal 和 stdout；队列溢出或输出失败会显式结束不完整流。SIGINT/SIGTERM 或指定运行时长结束后释放 SDK 并排空输出。带 `--duration-seconds` 的测试若结束时未完成登录和订阅，返回失败。收到行情回调也不一定表示实时更新，需检查原始日期时间与本机接收时间。

行情服务不包含自动交易策略，也未添加现有加密市场行情 IPC 的兼容层。库中的 `market_data::Tick` 为同进程消费者提供原生行情契约；进程消费者可读取 stdout 的 JSON 行。

## CPU、NUMA 与热路径

交易和行情配置现在都必须包含 `[placement]`；旧私有配置需补齐后才能使用。配置加载只检查字段范围、非空和 CPU 集合互斥，不改变 CPU 或内存策略。只有实际连接运行时才检查外部主机拓扑并应用绑定：

```toml
[placement]
numa_node = 0
business_cpu = 0
sdk_cpus = [1]
background_cpus = [2]
```

CPU 编号仅为占位。业务、SDK、后台三个域必须位于同一指定 NUMA 节点，属于进程原本允许的 CPU 集合，且不同域不能共享 SMT 物理核。行情与交易若同时运行，需分别分配物理核；示例配置没有替实际主机预留 CPU。SDK Create/Init 和重建期间临时切至 SDK CPU 集合，使其工作线程继承绑定；首次回调再次强制校验并绑定。业务线程内的普通同步回调保留业务 CPU，Create/Init 内的同步回调保留 SDK CPU，避免破坏后续工作线程的继承。Journal、RocksDB 后台线程、stdin 与行情输出线程继承后台 CPU 集合。

运行时强制 `MPOL_BIND | MPOL_F_STATIC_NODES`，新分配页受指定节点策略约束。节点须在 `Mems_allowed_list` 中，宿主机必须允许 `get_mempolicy`、`set_mempolicy` 和绑核系统调用。拓扑、权限、绑定或临时切核后恢复失败都会返回错误；回调内失败会使主循环报错退出，没有自动放宽策略。该策略不负责迁移已经分配的旧页或保证 SDK 文件映射页全部位于指定节点。

高频委托、成交、报撤响应和订单/成交查询使用 **328 字节**紧凑事件；低频账户、合约及费率响应保留 **928 字节**完整事件，放入预分配池。两类事件进入同一个有界 MPSC 队列，保留入队顺序；审计持有池槽引用，直到后台序列化完成才释放。官方 81 字节错误消息字段保留完整容量。以上为编译期布局大小，不是延迟测量。

报撤单固定账户/合约字段在冷路径准备不可变模板，每次请求复制模板并覆盖动态字段，避免每单构造多份 `CString`，也不依赖 SDK 保持输入结构不变。白名单外已有订单可冷路径准备仅供撤单的模板，不因此获得报新单权限。请求 ID 使用每页 1024 项的直接索引；`preallocated_requests` 默认 16384，范围 1024–2000000，只决定初始容量，历史仍可增长且保留迟到响应关联。重连移交关联表，不克隆全表。

两份示例均设 `idle_sleep_us=0`，空闲时忙轮询，占用业务 CPU；可设 1–1000 微秒，仅在空闲时休眠。行情业务线程只校验并复制固定 `TickRecord`，字符串、无效数值转换、JSON 与脱敏放在输出线程。输出每批最多处理当前已就绪的 64 条消息，不等待凑批；stdout 每批 flush，文件约每 100 毫秒 flush，退出时同步文件并传播失败。

共享 OMS 和归一化仍有字符串分配，不能宣称整条链路零分配。本轮只做静态审阅、格式化、编译和 Clippy；新增回归用例仅编译，CTP 测试、SDK、进程及性能基准均未执行。真实延迟与吞吐待用户指定并授权外部主机后验证，不能套用易达历史结果，见[性能说明](cn_futures_performance.md)。

## SDK 与本地构建

SDK 必须在仓库外，头文件和动态库均使用匹配的 Linux x86-64 官方 6.7.11 版本：

```text
ThostFtdcTraderApi.h
ThostFtdcMdApi.h
ThostFtdcUserApiStruct.h
ThostFtdcUserApiDataType.h
libthosttraderapi_se.so
libthostmduserapi_se.so
```

运行时动态加载配置中的库，并拒绝不匹配的版本。`production_mode` 是 SDK 工厂的生产/测评选项，应与提供方要求一致。

```bash
CTP_SDK_DIR=/absolute/path/to/ctp/6.7.11 \
  cargo build --release -p cn_futures --features ctp-sdk --bin ctp-trader --bin ctp-md
cargo build --release -p persist_manager --features embedded --bin cn_order_export

# 以下即使不连接网络，也仅限用户指定并授权的外部主机
target/release/ctp-trader --config config/ctp_trade.example.toml --check-config
target/release/ctp-md --config config/ctp_market_data.example.toml
target/release/ctp-md --sdk-version /absolute/path/libthostmduserapi_se.so
```

历史验证使用 [vnpy_ctp 固定提交所附官方 SDK](https://github.com/vnpy/vnpy_ctp/tree/1be9bfb5292208e5c258cc769c90be4abe83f9a9/vnpy_ctp/api)，没有使用其 Python 运行时。交易库 SHA-256 为 `ad4da95953ea5d0f4ed28ac746462ddba1f3434d0df1727919ae0d97591b4085`；行情库为 `9c6d321f3d880e1c91ec225cc46950f8aa1502f43fc9f41d1d92f10ee04d190a`。历史离线版本查询记录为 `v6.7.11_20250617 16:22:00.10369`；本轮没有加载 SDK 或重做版本查询。

## 配置与命令

无凭据示例为 `config/ctp_trade.example.toml` 与 `config/ctp_market_data.example.toml`。交易示例的 `front`/`fallback_fronts`、行情示例的 `fronts` 均是本地占位地址，运行前须复制到私有目录填写。交易每合约配置本地保守资金预算及交易所；行情只配置订阅。两进程使用独立的 flow/journal，不能共享 SDK 状态目录。

交易凭据从配置指定的 `CTP_USER_ID`、`CTP_PASSWORD`、`CTP_APP_ID`、`CTP_AUTH_CODE` 读取；InvestorID 不同时使用 `investor_id_env`。MdApi 只使用账号密码及 BrokerID，没有交易端的 `ReqAuthenticate`，不能用行情登录证明交易端 AppID/AuthCode 授权有效。

```bash
# 目标为私有配置中显式填写的柜台和合约；只读交易端资料查询
target/release/ctp-trader --config /absolute/private/trade.toml --connect

# 同一显式行情目标：登录、订阅并持续输出行情；20 秒未就绪会失败
target/release/ctp-md --config /absolute/private/md.toml --connect --duration-seconds 20
```

交易 stdin 接收单行 JSON。`state` 返回完整账户及合约规则；`reconcile` 重做规则/费率与账户对账；`shutdown` 退出而不撤单。EOF 同样退出。

```json
{"op":"state"}
{"op":"reconcile"}
{"op":"shutdown"}
```

在已获授权的测试柜台，开启 `--execute` 后可显式提交：

```json
{"op":"submit","intent":{"venue":"SHFE","symbol":"rb2701","side":"buy","offset":"open","lots":1,"price_ticks":3500,"time_in_force":"day"}}
{"op":"cancel","key":{"session":"ctp:7:11","order_ref":"101"}}
```

合约、档位及订单身份仅为示例，实际限价等于 `price_ticks × PriceTick`，撤单必须使用真实回传的完整 key。逐子单回执中的 `submitted_locally` 只表示 SDK 同步接收，不表示柜台接受或已成交。

## 创元生产资料与首次连接记录

完整资料位于本机仓库外：

- `~/.config/mkt_signal/chuangyuan_ctp/access.md`：生产通知、线路、端口及私有接入资料。
- `~/.config/mkt_signal/chuangyuan_ctp/env.sh`：凭据与接入变量，文件权限 `600`；目录权限 `700`。

密码沿用会话中先前提供的值，尚未核对是否为最新短信密码。实际账号、密码和授权码不复制到仓库文档、示例或提交中。`source env.sh` 只导出变量，不连接柜台；地址、BrokerID 等仍须填写到对应私有 TOML，不会自动覆盖程序配置。

2026-11-11 北京时间 17:16–17:17，本机逐一测试 8 个行情入口及 8 个交易入口，TCP 全部返回 `ECONNREFUSED`；官方 MdApi 对两组端口各取一个代表入口，等待 15 秒均无 `OnFrontConnected`，没有提交登录、订阅、结算确认或订单请求。此时为收盘后时段，但不能据此确定拒绝原因。完整结果位于私有 `probes/20261111T091644Z/summary.json`。

最初的短时探针 `scripts/ctp_md_probe.cpp` 仍可用于独立排查；它在断线时退出，不承担持续行情服务的重连职责。持续运行使用 `ctp-md`。

## 验证命令（仅限获准的外部主机）

合成 SDK smoke 脚本需要 Python 3.11 或更新版本。独立 placement 文件只含 `numa_node`、`business_cpu`、`sdk_cpus`、`background_cpus` 四个顶层字段，不加 `[placement]` 表头。被忽略的官方 SDK 构造/释放用例同样需要通过 `CTP_TEST_PLACEMENT_CONFIG` 指向该文件，并通过 `CTP_TEST_SDK_LIBRARY` 指向官方库。

```bash
CTP_SDK_DIR=/absolute/ctp \
  cargo test -p cn_futures --features ctp-sdk
cargo test -p persist_common
cargo test -p persist_manager --features embedded --lib cn_futures

# 官方头文件 ABI + 合成交易 SDK：每个市场独立验证真实 Rust/C++ 路径与 RocksDB 导出
# 先复制并按获准外部主机拓扑修改 config/ctp_placement.example.toml
python3 crates/cn_futures/tests/ctp_bridge_smoke.py \
  --sdk-dir /absolute/ctp --binary target/release/ctp-trader --venue CZCE --symbol SR701 \
  --placement-config /absolute/private/ctp_placement.toml
# 同样覆盖 CFFEX/IF2612、SHFE/rb2701、INE/sc2701、DCE/m2701、GFEX/lc2701
python3 crates/cn_futures/tests/ctp_bridge_smoke.py \
  --sdk-dir /absolute/ctp --binary target/release/ctp-trader --unconfirmed-settlement \
  --placement-config /absolute/private/ctp_placement.toml
python3 crates/cn_futures/tests/ctp_md_bridge_smoke.py \
  --sdk-dir /absolute/ctp --binary target/release/ctp-md \
  --placement-config /absolute/private/ctp_placement.toml
```

交易联测覆盖认证、只读结算状态查询、账户字段、合约乘数、手续费、相对保证金率合并、两轮对账、限价撤单、FAK/FOK、重复成交、平仓、异步拒单及持久化导出。未确认结算的联测要求账户/规则仍可读，但交易被阻断且没有结算确认或订单请求。行情联测覆盖五档、断线 gap、重新登录/订阅和错误密码只尝试一次。这些合成 SDK 不发起网络连接，不能代替真实柜台仿真或实盘验证。


### 扩展实现后的只读复测

2026-11-11 北京时间 18:12（UTC 10:12），使用新构建的 `ctp-trader` 和 `ctp-md`，覆盖六交易所合约样本，配置 `confirm_settlement=false` 且未开启 `--execute`。行情运行 20 秒，交易查询运行约 30 秒后正常退出。

16 个 TCP 入口仍全部拒绝连接。行情进程进行了带退避的重建尝试，最终因限时内未就绪返回失败；交易进程同样记录超时重连，未完成认证、登录或账户对账，账户快照为空。行情回调为 0。没有结算确认、下单或撤单；这次不能验证实盘账户、费用或订阅权限。

完整报告位于私有 `probes/20261111T101205Z/summary.json`。官方 SDK 已持久保存至本机 `~/.local/share/ctp/6.7.11/`；私有 `trade.toml` / `md.toml` 及 env 中的 `CTP_TRADE_CONFIG` / `CTP_MD_CONFIG` 可用于柜台开放后的复测，未部署远程服务。
