# 国内期货统一行情：cn-futures-pbs

最后更新：2026-11-13 UTC。

`crates/cn_futures_pbs` 提供一个 `cn-futures-pbs` 入口。`--zmq`、`--ctp`、`--colo` 互斥选择来源；连接和裸解析留在适配器内，输出共用一个 `MarketDataMsg` 和 Iceoryx2 IPC 发布层。行情是完整五档快照，不拆 Crypto 的 ticker、spread、trade 等消息种类。

| 来源／环境 | 当前状态 |
| --- | --- |
| `--zmq`：**公司内网通联转发** | 已接入并实测六个期货主行情频道到 IPC |
| `--ctp`：官方 CTP MdApi | 复用现有 Feed 与严格 CPU／NUMA 放置，已编译；未在本机执行或实测 |
| `--colo`：托管机房 | 参数保留，待提供实际协议、授权与端点；当前明确报未实现 |
| 公网接入 | 后续环境，当前没有可用的公网适配器或默认地址 |
| 易达 SDK 自带行情 | 只作审计备份参考，本轮不接入，不增加 `--yd` |

当前 `--zmq` 是公司内部转发服务的接法，不是通联的通用公网接口。colo、公网、公司内网是部署与来源的区别，不建立平行版本的内部消息、类型或 Redis／IPC 契约；未来适配器继续输出同一个 `MarketDataMsg`。不同来源必须使用独立的 `service_root`，不能同时占用同一发布服务。

## 公司内网通联来源

- 公司数据目录：[tonglian-future-quote-L2](http://10.61.20.64:5201/market/dataset/tonglian-future-quote-L2)。这是网页入口。
- 实际行情地址：`tcp://10.61.20.12:13005`，ZeroMQ SUB，单 frame。
- 当前配置：`config/cn_futures_pbs_company_intranet.example.toml`，显式 `access_scope="company_intranet"`，只接受私有／本地 IPv4。
- 消息：`<topic> || <CSV payload>`。topic 内的 `*` 是普通字符；订阅前缀为完整的 `<topic> || `。
- 依据：数据目录附件 `FUTURES_DOWNSTREAM_GUIDE.md`、`MDL959-SDK_.pdf`，以及既有历史 MSG CSV schema。上期所／能源所 CSV 字段顺序不能直接照抄 PDF 中的二进制结构声明。

接收器只订阅六个期货主行情：

| topic | 交易所 | 当前字段数 |
| --- | --- | ---: |
| `mdl.21.1.*` | CFFEX | 43 |
| `mdl.22.1.*` | SHFE | 43 |
| `mdl.22.3.*` | INE | 43 |
| `mdl.23.1.*` | CZCE | 59 |
| `mdl.24.1.*` | DCE | 59 |
| `mdl.26.1.*` | GFEX | 59 |

上游有 16 个频道，其余组合、成交量统计、十笔委托和额度频道不进入此主行情 IPC；不能把累计量差、统计或组合行情充作逐笔成交。解析器按固定位置借用字节切片，使用栈上字段槽位；没有 JSON、CSV 对象、HashMap、消息堆分配或中间存储。当前所需字段之后的新增尾部字段被跳过；原 frame 的所有权始终在调用方，IPC 不承诺保留完整原始正文。

## 通用消息与 IPC

唯一的 `#[repr(C)] MarketDataMsg` 当前为 **328 字节**，定义于 `src/msg.rs`，为两侧直接使用的无指针 POD：

- 接收时间 `received_us`、交易所时间 `exchange_ts_us`，均为 Unix 微秒；原生 `trading_day` 和 `action_day` 单独保留。
- `publisher_epoch`、连续性段 `source_generation`、每个 venue 独立的 IPC `sequence`，以及可用时的供应商 `source_sequence`。
- 原生大小写合约（最多 63 字节）和交易所名；不借用 Crypto venue 编号，不扩写郑商所三位月份码。
- 最新价、累计成交量（手）、原始成交额、持仓量，买卖各五档价格／数量。
- 缺失浮点为 NaN，缺失整数数量／供应商序号为 `u64::MAX`，未知日期／交易所时间为 0。显式零量仍是零，不填前值，不做价格／数量缩放或默认合约乘数。

夜盘时间用 **ActionDay + UpdateTime + Asia/Shanghai UTC+8** 计算，不拿结算日 TradDay 或本机日期补造。CTP 缺失 ActionDay 时保留未知时间，不猜日期。数量与金额不作任何合约乘数换算；历史回放的只读 PostgreSQL 乘数来源保持不变。

服务名为：

```text
<service_root>/<CFFEX|SHFE|INE|CZCE|DCE|GFEX>/l2
```

例如 `cn_futures_pbs/company_intranet/SHFE/l2`。每个服务只有一个发布者；下游用相同的 `MarketDataMsg` 类型打开 Iceoryx2 publish/subscribe 服务即可直接读 loaned sample。当前历史队列为 1，订阅缓冲上限 2048，最多 16 个订阅者，64 个节点。运行环境与下游应使用相同的 `iceoryx2.toml`；沿用现有环境 cwd 配置约定，不跨环境混用共享内存目录。

TCP 连接／断开、畸形单帧或误用 multipart 会生成 `SOURCE_RESET` 标记，并以空 symbol 向六个 venue 发布。正常退出或可处理的源错误也尝试发布失效标记。消费端先检查 `flags & SOURCE_RESET`，撤销旧的就绪／窗口连续性，再处理后续完整快照。`publisher_epoch` 变化表示进程代际，IPC 序号能检测本地下游漏收；供应商 SeqNo 不作为跨频道连续序号使用。

上游 PUB/SUB 不持久化、不确认、不补发。TCP 重连不代表历史缺口被修复，完整盘口快照也不能补齐丢失的交易流／因子历史。即使未观察到连接事件，网络或慢消费者仍可能丢消息，适配器不能证明上游完整。下游仍需根据来源时间、接收时间、代际和交易日执行新鲜度／就绪门控；不能把行情就绪当作账户可交易。此入口不发布交易目标或下单指令。

## 使用

```bash
cargo build --release -p cn_futures_pbs --bin cn-futures-pbs
target/release/cn-futures-pbs --zmq \
  --config config/cn_futures_pbs_company_intranet.example.toml --check-config

# 公司内网只读订阅；实际运行时改成环境自己的 service_root。
target/release/cn-futures-pbs --zmq \
  --config config/cn_futures_pbs_company_intranet.example.toml

# 有限时长的接收测试；默认不自动退出。
target/release/cn-futures-pbs --zmq \
  --config /absolute/env/cn_futures_pbs.toml --seconds 10 --max-messages 1000
```

CTP 只允许在用户明确指定、授权的**外部主机**执行。当前开发机和所有内网机器不得加载 SDK、连接、探测端口或运行 CTP 测试／离线 SDK。这里只编译：

```bash
CTP_SDK_DIR=/absolute/official/ctp-6.7.11/include \
  cargo check -p cn_futures_pbs --features ctp-sdk
```

授权外部环境使用 `config/cn_futures_pbs_ctp.example.toml` 指向现有官方 CTP 行情配置，通过 `--ctp` 启动带 `ctp-sdk` 的同一个程序。账号权限、合约订阅和原来的严格 CPU／NUMA 配置继续由 CTP Feed 校验，没有复制一套 CTP 登录／重连状态机。该路径尚缺外部主机的运行验收。

易达 SDK `1.502.55.35` 的 `YDMarketData` 确实提供订阅和单档 BidPrice／AskPrice／BidVolume／AskVolume。本轮仿真探针收到的是这类 BBO，不能当作五档。按本次范围，它只作为审计备份参考，保持易达交易入口的行情连接关闭，不为此增加采集器。

## 验证

默认测试仅覆盖本 crate 的 SDK-free 路径，不运行 `cn_futures` 或 CTP 混合测试：

```bash
cargo fmt -p cn_futures_pbs --check
cargo test -p cn_futures_pbs --locked
cargo clippy -p cn_futures_pbs --all-targets --locked -- -D warnings

# 明确授权的公司内网只读采样，发布到唯一测试命名空间：
CN_FUTURES_LIVE_ZMQ_ENDPOINT=tcp://10.61.20.12:13005 \
  cargo test -p cn_futures_pbs --test zmq_ipc \
  live_company_feed_reaches_all_six_ipc_venues -- --ignored --nocapture
```

2026-11-13 已完成六家真实消息样本解析、夜盘日期、缺失档位／显式零量、郑商所订单笔数与大商所推导量不混作盘口量、64 位数量、畸形／截断输入、未知尾字段和本地 ZMQ→IPC 的验证。6,000 次真实样本解析的 Rust 堆分配计数为 0。公司内网→测试 IPC 联调六家均收到了数据，第一轮计数按上述 venue 顺序为 `23/129/1/137/171/12`；不是交易量或丢包率测量。CTP 只完成静态编译，colo／公网尚未验证。
