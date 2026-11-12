# Exchange Public API Cache

最后更新：2026-11-12（Gate FR 缺失标记价补价；Binance PM FR 最低借款额与返还现金检查）

2026-11-12，Gate FR `pre_trade` 增加持仓缺失标记价的异步 REST 兜底（代码，尚未发布）。
默认连续缺价 30 秒后，直连官方无鉴权
`GET /api/v4/futures/usdt/contracts/<CONTRACT>`，只接受名称匹配且有限、正数的
`mark_price`，写入现有 mark 表并刷新账户估值；不使用 `last` 或现货价格代替。
`PRE_TRADE_GATE_MARK_REST_AFTER_SECS` 可设为 5–3600 秒，默认 30；每 5 秒检查，
每轮最多 8 次串行请求、间隔 100ms、超时 5 秒，失败后该币至少等 30 秒再试。
429/503 全局退避至少 60 秒，并遵守数值型 `Retry-After`。仅针对 Gate FR
现货/合约配对的实际持仓（含两腿净额为零），无合约持仓且目录无对应合约的资产
继续使用既有现货集中度补价。持仓消失或行情先补齐时跳过请求/回写。
此兜底修复缺价，不把已有标记价的更新间隔当作缺价；Gate ticker 可因价格不变而不推送。
不依赖 `trade_signal`，不携带账户凭据、不跟随重定向、不新增 Nginx 缓存或白名单。
官方契约：[Get a single contract](https://www.gate.com/docs/developers/apiv4/en/#get-a-single-contract)。

2026-11-12，Binance PM FR 的开仓检查由账户容量任务调度（当前每60秒，组件自身最短间隔30秒），
通过现有交易引擎查询路由读取
`GET /sapi/v1/margin/allAssets`，只提取USDT行的`userMinBorrow`，有效期180秒。
目录在交易引擎内缩成原始USDT单行后进入现有IPC，避免完整目录超过4096字节消息上限。
缺失、重复、非正/非有限最低额和过期规则禁止需要借款的新买单；现金足够的订单
不依赖这项规则。该查询沿用签名REST客户端、环境账户和交易源IP，直连官方
`api.binance.com`；不进入公共共享缓存，不新增Nginx端点白名单或监听。
2026-11-12只读实查USDT的`userMinBorrow=5`，运行代码不硬编码此值。
本段记录代码行为，发布状态见`docs/delist_risk_server.md`。
最低借款检查同时读取余额响应的`crossMarginLocked`，并把近期已提交买单的预留额
计入可能返还的现金上界，防止多档订单在前一笔借款/撤单后重复按旧的零现金放行。
冻结额缺失或无效不得按零处理；现有手续费缓冲以内不能证明纯现金支持的买单也要
通过最低借款检查。余额、可借额度和最低额查询均维持上述直连签名路由。


2026-11-09，CTA 配置预览/保存、Prep/Spread 命令行配置导入复用官方公共只读
`GET /fapi/v1/exchangeInfo` 完整目录，只保留 Binance `TRADING` 标的；不使用
账户凭据，不影响 Exec 由 Manager 独占的订单规则刷新。复用专用
`BINANCE_FUTURES_PUBLIC_API_BASE`（默认 `https://fapi.binance.com`），配置服务
进程内成功目录最长复用 60 秒，HTTP 超时 15 秒，无自动重试、代理回退或重定向；
目录不可用时拒绝本次编辑，不据此清空配置。不新增 Nginx 监听、缓存或白名单，
不向因子运行层增加行情完整性校验；未部署。本公共端点契约见
[Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)。

2026-11-09，CTA Prep/Spread 的 1h NQ 启动和新代次回放增加一次性历史价格预热，
复用 Binance 官方公共 `GET /fapi/v1/klines`，固定 `interval=1h`，仅取第一条 Kafka
源小时之前的已闭合 K 线 close。默认每币 239 根，每页最多 499 根；单进程串行且请求
起始间隔至少 1 秒，HTTP 超时 15 秒。429/418 停止该代次本轮后续请求，不立即重试；
网络、HTTP 或数据错误记录到预热状态，不降低样本门槛、不伪造价格。
复用专用 `BINANCE_FUTURES_PUBLIC_API_BASE`，默认 `https://fapi.binance.com`；
不读取私有接口基址或账户凭据，不附加签名或 API key，不跟随重定向，也不在配置代理
失败时自动直连。没有新增 Nginx 缓存、端口、白名单或部署；历史种子仅保存在各计算
代次目录供审计，不作为主机共享行情缓存。官方契约沿用下文 Binance K 线消费者条目。

2026-11-06 07:26 UTC，`okex_mm_83777` 新账户只读观察栈恢复，环境本地公共
利率缓存 PID413204 再次监听 `127.0.0.1:28901`。继续只请求下述官方公共端点，
不使用账户凭据；缓存策略、端点 allowlist、TTL 和 Nginx 配置保持。

2026-11-06 06:54 UTC，`okex_mm_83777` 替换私有账户凭据后全部停止，包含环境本地
`127.0.0.1:28901` 利率缓存；待私有账户 IP 白名单和 UID 验证后再恢复观察。
公共缓存策略、端点 allowlist、TTL 和 Nginx 配置保持。

2026-11-06，JP `okex_mm_83777` 观察模式使用本机 `127.0.0.1:28901` 利率缓存；
该缓存直接向 OKX 官方公共 `GET /api/v5/public/interest-rate-loan-quota` 请求，
每小时一次，不使用账户凭据，不新增 Nginx 公共缓存或代理路径。

2026-11-05 Delist 风险服务接入 OKX 官方公共只读来源，均直接访问官方 HTTPS，
不新增 Nginx 路由、监听或缓存：`GET /api/v5/public/instruments?instType=SPOT|SWAP`
用于完整目录和下架日程；`GET /api/v5/support/announcements?annType=announcements-delistings&page=N`
用于公告列表，`Accept-Language: en-US`；正文仅取 `https://www.okx.com/help/` 官方页面，
拒绝重定向与缺失正文。沿用每日目录/日程/公告周期，每次公告最多10页，分页间隔500ms，
无立即失败重试；429/503遵守 `Retry-After`（最低60秒），业务限流50011/50013共享60秒冷却。
不发送任何 `OK-ACCESS-*` 头或账户凭据；私有/交易请求不进入此路径。
官方契约：[Instruments](https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments)、
[Announcements](https://www.okx.com/docs-v5/en/#announcement-get-announcements)。
公共公告结果受请求IP地区限制；成功解析不表示全球覆盖。公告、正文/抽取和各市场目录/日程分别记录健康，
失败不会被解释为空且健康。运行时样本与最终发布验收见 `docs/delist_risk_server.md`。

2026-11-05 Gate FR 集中度风控增加现货估值补价：对同账户 Gate 现货/合约配对中，
持有现货、没有合约持仓且缺合约 mark 的资产，异步直连官方无鉴权
`GET /api/v4/spot/tickers?currency_pair=<ASSET>_USDT`。仅使用有效买一/卖一中间价，
不使用可能陈旧的 `last`；默认每 10 秒刷新，每轮最多 16 次请求，串行间隔 100ms，
超时 5 秒，按轮转覆盖待补价资产。进程内估值最长保留 30 秒，请求失败即使该资产
报价失效；429 遵守数值型 `Retry-After`，缺省全局退避 60 秒。
补价仅供集中度检查，不写合约 mark 表或下单报价。缺价资产保留已有告警/dump 状态，
其他有价资产继续检查；不修改阈值。`gate_fr_arb02` / `gate_fr_arb01` 已分别于
02:53 / 03:41 UTC 发布并恢复信号；不新增 Nginx 白名单、缓存监听或代理，不携带账户凭据。
Gate 公共共享缓存仍未部署。
官方契约：[List spot tickers](https://www.gate.com/docs/developers/apiv4/en/#list-tickers)。

2026-11-03 15:18 UTC恢复Bitget仓位档位sidecar至独立目录
`~/bitget_position_tier_sidecar`，替换已不存在的服务器仓库路径。公共请求仍显式使用
`http://127.0.0.1:28902` 和既有 `GET /api/v2/mix/market/query-position-lever` 白名单，
TTL、缓存键及Nginx出口不变。注册Bitget FR01/02/03，当前22个在线币种一次性补全后
恢复原有3币/20秒、单币30分钟冷却的常驻轮询；不新增API或代理私有/写接口。

2026-11-03 15:01 UTC，跨所账户当前查询优先在同一公开Funding缓存及预算内生效；
当前区间费率/标记价完整后释放，查询停止后自动失效。未更改90次/5分钟预算、
3秒请求间隔、并发上限、WAF/429退避、出口或官方端点，无新代理或接口。
启动至14:50 UTC区间覆盖完整，估算金额与两所及策略合计一致。

2026-11-03 Manager 公开 Funding 调度修复：累计查询首次先提供最近所需覆盖，
再扩展更早历史；已覆盖区间不再被旧历史的loading状态阻塞；长历史分页之间释放
加载名额。请求预算、并发上限、WAF/429退避、原出口和官方端点保持；未添加
缓存代理、白名单、公开端点或私有请求。OKX 仍使用自身结算费率及已确认的
结算前1分钟标记价，不混用 Binance 价格。当前账户只累计查询区间内的结算。

2026-11-03 修复 Manager 的 OKX 历史成交K线确认延迟处理：官方
`GET /api/v5/market/history-candles` 可对时间已结束的分钟暂时返回 `confirm=0`。
保留同页已确认K线，未确认分钟不写缓存、不参与净值估值；查询返回等待状态，
后续只补该缺口。畸形数据仍报错。沿用既有专用出口、请求预算和官方直连，
不增加缓存代理、白名单、交易请求或未确认价格回退。

2026-11-02 审核 CTA Manager 的 OKX 公开估值接入：历史成交 K 线使用
`GET /api/v5/market/history-candles`，公开已结算 Funding 使用
`GET /api/v5/public/funding-rate-history`，估算所需的历史标记价格分钟使用
`GET /api/v5/market/history-mark-price-candles`。全部直连官方 HTTPS，
无鉴权，不新增 Nginx 共享缓存端口或白名单。K 线沿用 Manager 已校验的专用
出口、交易 IP 排除和滚动请求预算；Funding 走原有公开费率客户端与全局退避，
不调用账户收入或私有接口。上述解析已随JP Manager产物部署；2026-11-03对
官方BTC-USDT-SWAP样本核验10根连续已收盘1m K线、3次公开已结算费率及
结算前完整分钟的标记价。K线合约张数与基础币成交量字段分别处理，未复用Binance价格。
公开样本/已部署源码指纹：`artifacts/cta_spread_data_audit_20261103/`。本组统一使用
预估Funding，不开启实际账单采集；没有新增缓存代理、接口白名单或私有请求。

2026-10-30 审核 FR 启动抵押检查 `scripts/check_fr_collateral.py`：所有账户查询、
抵押开关写入及回读均绑定部署环境的交易源 IP，直连官方 HTTPS。辅助查询
Gate `/api/v4/unified/currency_discount_tiers` 和 Bitget
`/api/v3/account/custom-collateral-coins` 同样直连；此工具携带账户签名，
不进入公共缓存，不新增 Nginx 代理或白名单。

2026-10-29 审核原生 OKX 统一账户 Exec 的现货接入：CTA Manager 每 60 秒直连官方
`GET /api/v5/public/instruments?instType=SPOT`，为 `okex-margin` 发布完整 USDT
现货规则；交易引擎沿用同一公共端点获取 WS `instIdCode`。不新增本机缓存代理或
Nginx 白名单。私有账户查询、挂单查询及撤单继续签名直连并绑定账户交易源 IP。

2026-10-13 审核一次性运维工具 `scripts/neutralize_rapidx_binance.py`：
Binance 实时 `/fapi/v1/depth` 直连官方 HTTPS，不进入缓存；RapidX 的所有
账户、规则、订单查询及写请求均签名直连，不新增代理或缓存白名单。

2026-10-13 06:49 UTC 已在 RX01 部署 CTA Spread 信号观察页；独立只读后台通过官方公共
`GET /api/v3/klines` 补现货 1m 图表，默认直连 `https://api.binance.com`；
不增加 Nginx 白名单，不调用私有接口。每次最多 1000 根，只补缺失分钟并刷新
末两根，单线程最多 4 请求/秒，遇 418/429 全局退避 60–900 秒；
本地 SQLite 缓存行情。可用 `BINANCE_SPOT_PUBLIC_API_BASE` 指定公共基址，
不得复用交易凭据或私有 API 代理。
官方契约：[Spot Klines](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data)。

## 状态

本文是交易所公共 REST API 的主机级共享缓存规范。Bitget 是首个已接入的交易所。
截至最后更新时间，`jp-meta-elvpn` 已部署 Bitget Nginx 缓存，当前监听
`127.0.0.1:28902`。Bitget 当前已部署的长驻公共 REST 调用已完整切换到该代理：
`bitget_fr_arb01`、`bitget_fr_arb02` 的 `trade_signal`、`fr_signal_dashboard`、
`pre_trade`，`spread_pbs/bitget-both`，以及 `bitget_position_tier_sidecar` 和
`delist_risk_server`。`trade_engine`、账户监控及所有签名/写请求继续直连交易所。

Gate 公共 REST 缓存尚未接入，详见“已审计、暂缓实施的项目”。不要把其中的候选方案
视为已部署状态。

后续 Binance、Gate、OKX、Bybit 等交易所需要公共 API 缓存时，继续更新本文，
不要为每个交易所创建互相独立、规则不一致的说明文档。

## 目标与边界

- 一台机器上的所有进程和所有本地源 IP 共享同一份缓存。
- 每个交易所使用独立的本地监听端口、缓存区和严格的公共 GET 路径白名单。
- Nginx 回源使用主机默认路由，不配置 `proxy_bind`，不继承调用进程的源 IP。
- 本地监听只绑定 `127.0.0.1`，不向其他机器或公网提供代理服务。
- 缓存 key 必须包含完整 URI 和查询参数。调用方应固定查询参数顺序，以提高跨进程命中率。
- 开启 `proxy_cache_lock`，同一个 key 失效时只允许一个请求回源。
- 只缓存 HTTP 200；429、HTTP 鉴权错误和上游错误不得写入缓存。
- 纯 Nginx 不解析 JSON 业务码。HTTP 200 中的交易所业务错误仍可能被缓存，响应体校验和告警必须保留在应用侧。
- 不提供过期数据兜底。缓存过期后的上游失败应返回失败，由应用保留其最后一次有效状态并按原有风险逻辑处理。
- 私有账户查询、订单、撤单、转账、借还币和任何签名请求不得经过该缓存。
- 实时 ticker、盘口、成交、标记价格和当前资金费率不得使用分钟级缓存。

缓存是每台主机独立的故障域。`jp-meta-elvpn` 和 `sg` 如果都需要缓存，应分别部署，
不跨地域共享缓存。

## 应用配置约定

每个交易所使用仅限公共 REST 的独立基址，例如：

```bash
export BITGET_PUBLIC_API_BASE='http://127.0.0.1:28902'
export BINANCE_PUBLIC_API_BASE='http://127.0.0.1:<待分配端口>'
export GATE_PUBLIC_API_BASE='http://127.0.0.1:<待分配端口>'
```

`BITGET_PUBLIC_API_BASE` 已由公共 Bitget 调用支持；未设置时默认值仍是
`https://api.bitget.com`，方便未部署本地代理的开发和其他主机直接访问。

不要把现有 `BITGET_API_BASE`、`BINANCE_API_BASE` 或 `GATE_API_BASE` 整体改成本地代理；
这些变量也被私有和写接口使用。公共基址只能用于本文白名单内的无鉴权 GET 请求。
不要通过 `/etc/hosts` 劫持交易所域名。

应用不应在本地代理失败时自动绕过代理直连，否则多个进程会在代理或交易所异常时重新形成
请求风暴。长驻进程应保留最后一次有效值、记录代理错误，并保持原有开仓/平仓风险语义。

## Bitget 首期清单

Bitget 官方文档中的这些接口按 IP 限速；当前相关接口的公开限额为每秒 10 或 20 次。
主机共享缓存可消除多个策略实例从不同本地源 IP 重复查询同一份平台级数据的问题。

| 路径 | 当前仓库用途 | TTL | 说明 |
| --- | --- | ---: | --- |
| `GET /api/v3/market/margin-loans` | `trade_signal` 借贷日利率和可借额度 | 5m | key 必须保留 `coin`、`level` 及其他查询参数；当前调用未传 `level`，使用 Bitget 默认等级 |
| `GET /api/v3/market/history-fund-rate` | `trade_signal` 历史资金费率和结算周期推断 | 5m | 在资金费率结算后，最新历史记录最多延迟一个 TTL 可见 |
| `GET /api/v3/market/instruments` | 交易规则、数量精度、产品状态、下架/交割时间 | 60s | 状态和规则直接影响可交易性，不使用 5m TTL |
| `GET /api/v2/spot/public/symbols` | 旧版现货规则、状态和 `offTime` | 60s | 旧版兼容路径；迁移前继续列入白名单 |
| `GET /api/v2/public/annoucements` | 下架公告发现 | 60s | 官方路径拼写就是 `annoucements`；cursor、语言和公告类型都属于 cache key |
| `GET /api/v2/mix/market/contracts` | 旧版合约规则和状态脚本 | 60s | 旧版兼容路径；优先逐步统一到 v3 instruments |
| `GET /api/v2/mix/market/query-position-lever` | 仓位风险档位 sidecar | 5m | key 必须区分 `symbol` 和 `productType` |

主要调用位置：

- `crates/trade_signal/src/rate_fetcher.rs`
- `src/common/exchange_info.rs`
- `src/common/min_qty_table.rs`
- `crates/signal_common/src/min_qty_table.rs`
- `src/common/delist_schedule.rs`
- `src/common/bitget_announcement.rs`
- `src/mkt_pub/cfg.rs`
- `scripts/bitget_position_tier_sidecar.py`
- `scripts/set_cross_align.py`

一次性运维脚本中的公共查询也可以复用代理，但只有明确改用
`BITGET_PUBLIC_API_BASE` 的公共调用才能进入缓存。同一脚本内的签名请求必须继续使用
`BITGET_API_BASE` 直连交易所。

### Bitget 特殊注意事项

`margin-loans` 的响应同时包含 `dailyInterest`、`limit`、主子账户额度和平台剩余额度。
5 分钟缓存不仅缓存利率，也会让停止借贷或额度归零最多延迟 5 分钟被应用看到。
该延迟不应阻断 dump 平仓，但可能影响普通开仓资格判断，部署验收时必须保留这一风险记录。

Bitget 当前的成功响应带 Cloudflare `Set-Cookie`。Nginx 默认不会缓存带
`Set-Cookie` 的响应，所以代理必须同时配置：

```nginx
proxy_ignore_headers Set-Cookie;
proxy_hide_header Set-Cookie;
```

官方参考：

- [Get Margin Loan](https://www.bitget.com/api-doc/uta/public/Get-Margin-Loans)
- [Get Funding Rate History](https://www.bitget.com/api-doc/uta/public/Get-History-Funding-Rate)
- [Market Data / Get Instruments](https://www.bitget.com/docs/catalog/market/market-data)
- [Get Announcements](https://www.bitget.com/zh-CN/api-doc/classic/common/notice/Get-All-Notices)
- [Get Contract Config](https://www.bitget.com/api-doc/classic/contract/market/Get-All-Symbols-Contracts)
- [Get Position Tier](https://www.bitget.com/zh-CN/api-doc/classic/contract/position/Get-Query-Position-Lever)

## Bitget Nginx 模板

当前主配置文件是 `/etc/nginx/conf.d/bitget_public_api_cache.conf`，公共代理参数放在
`/etc/nginx/snippets/bitget_public_cache.inc`。对应仓库文件是
`config/nginx/bitget_public_api_cache.conf` 和 `config/nginx/bitget_public_cache.inc`，
由 `scripts/setup_public_api_cache.sh` 以默认 dry-run、显式 `--execute` 的方式安装。
以下配置没有 `proxy_bind`，Nginx 将通过主机默认路由访问 Bitget。

主配置：

```nginx
proxy_cache_path /var/cache/nginx/public_api/bitget
    levels=1:2
    keys_zone=bitget_public:16m
    max_size=128m
    inactive=1h
    use_temp_path=off;

map $upstream_status $bitget_public_skip_store {
    default 1;
    200     0;
}

server {
    listen 127.0.0.1:28902;
    server_name _;

    access_log /var/log/nginx/bitget_public_cache_access.log;

    location ~ ^/api/v3/market/(margin-loans|history-fund-rate)$ {
        include /etc/nginx/snippets/bitget_public_cache.inc;
        proxy_cache_valid 200 5m;
    }

    location = /api/v2/mix/market/query-position-lever {
        include /etc/nginx/snippets/bitget_public_cache.inc;
        proxy_cache_valid 200 5m;
    }

    location ~ ^/(api/v3/market/instruments|api/v2/spot/public/symbols|api/v2/public/annoucements|api/v2/mix/market/contracts)$ {
        include /etc/nginx/snippets/bitget_public_cache.inc;
        proxy_cache_valid 200 60s;
    }

    location / {
        return 404;
    }
}
```

公共代理参数：

```nginx
limit_except GET {
    deny all;
}

proxy_pass https://api.bitget.com;
proxy_http_version 1.1;
proxy_set_header Host api.bitget.com;
proxy_set_header Connection "";
proxy_set_header Accept-Encoding "";
proxy_set_header Content-Length "";
proxy_pass_request_body off;

# 公共代理绝不向上游转发 Bitget 凭据。
proxy_set_header ACCESS-KEY "";
proxy_set_header ACCESS-SIGN "";
proxy_set_header ACCESS-TIMESTAMP "";
proxy_set_header ACCESS-PASSPHRASE "";

proxy_ssl_server_name on;
proxy_ssl_name api.bitget.com;

proxy_connect_timeout 3s;
proxy_read_timeout 5s;
proxy_send_timeout 5s;
proxy_next_upstream off;

proxy_cache bitget_public;
proxy_cache_key "bitget|$request_method|$uri|$args";
proxy_cache_lock on;
proxy_cache_lock_timeout 10s;
proxy_cache_lock_age 10s;
proxy_cache_use_stale off;
proxy_no_cache $bitget_public_skip_store;

proxy_ignore_headers Set-Cookie;
proxy_hide_header Set-Cookie;
add_header X-Public-API-Cache $upstream_cache_status always;
```

配置目录应由 Nginx 用户写入：

```bash
sudo install -d -o www-data -g www-data -m 0750 /var/cache/nginx/public_api/bitget
sudo nginx -t
sudo systemctl reload nginx
```

加载 Nginx 配置不会让现有应用自动使用缓存。必须先让相关公共调用支持
`BITGET_PUBLIC_API_BASE`，再逐个部署和重启对应进程。

`scripts/deploy_fr_bitget.sh` 和 `scripts/spread_pbs/deploy_spread_pbs.sh` 会在发布
Bitget 环境时幂等维护 `env.sh` 中的托管块：

```bash
# BEGIN managed: Bitget public API cache
export BITGET_PUBLIC_API_BASE='http://127.0.0.1:28902'
# END managed: Bitget public API cache
```

该操作不读取、不重写 `BITGET_API_BASE` 或任何凭据。即使使用 `--bin` 发布，也会维护
这个公共基址，避免二进制已支持代理但环境仍遗漏 URL。

## jp-meta-elvpn 当前部署

- Nginx：`127.0.0.1:28902`，缓存目录 `/var/cache/nginx/public_api/bitget`。
- `bitget_fr_arb01`、`bitget_fr_arb02`：`env.sh` 设置
  `BITGET_PUBLIC_API_BASE=http://127.0.0.1:28902`；`trade_signal`、
  `fr_signal_dashboard` 和 `pre_trade` 均已重发并验证进程环境。
- `spread_pbs/bitget-both`：同样使用该公共基址拉取启动期 instruments；进程继续固定
  CPU 11，WebSocket 行情连接继续从 `172.31.46.90` 发出。
- `bitget_position_tier_sidecar`：PM2 参数显式设置同一本地 `--base-url`。
- `delist_risk_server`：PM2 环境设置同一本地公共基址，启动时的 Bitget symbols、
  instruments 和 announcements 请求已经过代理。
- Nginx 没有 `proxy_bind`。调用进程即使绑定策略私网源地址，访问本地监听后也由 Nginx
  通过主机默认路由统一回源。
- `trade_engine` 和 `account_monitor` 的私有账户、仓位、订单、撤单及其他签名查询没有
  改用代理；进程即使继承 `BITGET_PUBLIC_API_BASE`，这些代码路径也不会读取它。

## 验证

部署前先确认目标主机和本地监听端口未被占用。部署后至少验证：

```bash
hostname -f
ss -lntp | grep 28902
curl -sS -D - -o /dev/null 'http://127.0.0.1:28902/api/v3/market/margin-loans?coin=GRT&category=MARGIN'
curl -sS -D - -o /dev/null 'http://127.0.0.1:28902/api/v3/market/margin-loans?coin=GRT&category=MARGIN'
```

第一次应为 `X-Public-API-Cache: MISS`，第二次应为 `HIT`。不同 `coin`、`level`、
`category`、`symbol`、分页参数必须产生不同缓存项。还要验证：

- 白名单外路径返回 404。
- 非 GET 方法被拒绝。
- 429 和 5xx 不会成为后续请求的 HIT。
- 监听地址仅为 `127.0.0.1:28902`。
- Nginx 到 Bitget 的连接使用主机默认出口，没有固定到任一策略 IP。
- 两个不同策略实例请求同一 key 时，共享同一个 HIT/MISS 状态。
- 应用日志中的 Bitget 429 消失，且 dump 平仓信号不依赖缓存成功。

## 增加其他交易所

增加 Binance、Gate 或其他交易所前，先完成以下检查并更新本文：

1. 从官方文档确认接口无需鉴权、只读、限速维度和最新响应语义。
2. 在仓库中列出全部调用方和调用频率，确认是否真的存在跨进程重复请求。
3. 根据数据变化速度和风险语义确定 TTL，不要统一套用 5 分钟。
4. 检查响应中的 `Set-Cookie`、`Cache-Control`、`Vary` 和账户/等级相关字段。
5. 使用独立缓存 zone、本地端口、上游 Host/SNI 和严格路径白名单。
6. 增加对应的 `<EXCHANGE>_PUBLIC_API_BASE`，不要复用私有 API 基址。
7. 记录部署主机、配置路径、端口、缓存目录、白名单、TTL 和当前部署状态。

## Bybit 下架风险公共接口

`delist_risk_server` 在 JP 接入以下无鉴权只读来源；当前直接访问官方 HTTPS，
**未部署 Bybit Nginx 缓存**，也未改用私有 API 基址：

| 来源 | 调用频率 | 分页／正文 |
| --- | --- | --- |
| `GET /v5/announcements/index`，`locale=en-US&type=delistings` | 启动及每 24h | 每页 20 条，覆盖至少近 37 天 |
| `GET /v5/market/instruments-info`，`category=spot` | 启动、每 24h、00:00 UTC | Spot 无分页；完整目录用于已下架判断 |
| 同一 instruments 路径，`category=linear&limit=1000` | 同上及每 24h 合约下架查询 | 全部 cursor 页，仅 USDT 永续；`deliveryTime` 是永续下架时间 |
| `https://announcements.bybit.com/` 官方公告正文 | 每次公告列表刷新 | 从 `__NEXT_DATA__` 提取正文；按内容指纹复用 LLM 结果 |

目前是单个监控服务的日级请求，暂不新增本地代理端口或 TTL。后续共享缓存若覆盖
这些路径，建议公告和 instruments 使用 60s TTL，完整查询参数必须进入 cache key；
私有账户、订单及任何签名或写请求仍禁止经过缓存。

官方接口契约：[Get Announcement](https://bybit-exchange.github.io/docs/v5/announcement)、
[Get Instruments Info](https://bybit-exchange.github.io/docs/v5/market/instrument)。

## 已审计、暂缓实施的项目

### Binance CTA 分钟 K 线消费者

`scripts/cta_special_signal_history.py` 在 CTA 部署环境中采集
`GET /fapi/v1/klines`，只发送无鉴权公共 GET，固定 `interval=1m&limit=499`。
首次每币三页补齐最近 24h（1440 根闭合分钟加当前分钟），随后默认每 20s 更新
最后两根及缺失分钟，最大并发为 2。429/418 在整个消费者共享 60..900s 退避，
遵守 `Retry-After`；失败保留本地已存 K 线、显示错误，不伪造缺口。

这个消费者使用专用 `BINANCE_FUTURES_PUBLIC_API_BASE`，未设置时默认
`https://fapi.binance.com`。它不读取私有 API 基址，不附带账户凭据，配置代理后失败也不绕过代理。
当前变更只增加消费者，**没有部署 Binance Nginx 缓存或分配主机缓存端口**。
当前形成中的分钟每 20s 更新，因此没有设置分钟级缓存 TTL。
未来接入主机缓存时只允许此公共 GET、完整查询参数进入 key，并重新评估当前分钟的 TTL；
私有查询和交易写接口继续直连。

官方契约：[USD-M Futures market data](https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)。

以下项目于 2026-09-09 UTC 在 `jp-meta-elvpn` 完成只读审计，但没有修改 Gate
代码或配置。继续实施前需要重新确认应用显式公共基址的接入方式；当前不使用
`/etc/hosts`、透明 TLS 代理、iptables 重定向或全局 `HTTPS_PROXY` 劫持硬编码的
交易所 HTTPS 请求。

### Gate funding 与借贷历史

- `gate_fr_arb01`、`gate_fr_arb02` 的 `trade_signal` 当前各自每 5 秒请求一次
  `GET /api/v4/futures/usdt/contracts`，合计约 24 次/分钟。一次响应约 1.28 MB，
  按持续运行估算约为 44 GB/天的上游响应流量。
- 同时刻抽样中，`GET /api/v4/futures/usdt/tickers` 与 `contracts` 都返回 990 个合约，
  共同合约的当前 funding rate 无差异；`tickers` 响应约 0.47 MB。候选优化是让当前
  funding 补充使用 `tickers`，并对该精确路径使用约 5 秒的主机级共享缓存。它不能使用
  5 分钟 TTL。
- 两个 Gate `trade_signal` 和两个 `fr_signal_dashboard` 还会在整点重复拉取历史数据。
  一次审计窗口内合计约有 255 次 `GET /api/v4/futures/usdt/funding_rate` 和 249 次
  `GET /api/v4/unified/history_loan_rate`，这两类历史数据可考虑 5 分钟共享缓存，cache key
  必须包含完整查询参数。
- `GET /api/v4/unified/history_loan_rate` 是公共接口。对同一币种的签名与未签名请求实测
  返回完全一致；如后续接入公共缓存，应停止签名或由代理剥离所有 Gate 鉴权头。
- `GET /api/v4/unified/estimate_rate` 是私有鉴权接口，不得缓存或经过公共代理。Gate
  账户、仓位、订单、撤单、借还币和其他签名请求也必须继续直连。
- Gate 目前没有 `GATE_PUBLIC_API_BASE` 接入、Nginx 监听端口或缓存配置，上述内容均为
  待评估方案，不是部署记录。

## 回滚

应用侧先恢复公共基址直连并逐个重启相关进程；确认不再访问本地监听后，再移除或禁用
Nginx server 配置并执行 `nginx -t` 和 reload。回滚不得修改私有 API 基址、账户凭据、
交易源 IP 或交易进程的其他网络设置。

## Manager 混合现货/合约估值

CTA Manager 的现货理论估值新增 Binance 公共 `GET /api/v3/klines`，与
原有 `GET /fapi/v1/klines` 共用 Manager 的请求权重预算、专用非交易出口绑定和
仅补缺失分钟的本地缓存。现货使用裸币名缓存键，合约使用完整交易对缓存键。
两者仍直连 Binance，不新增 Nginx 路由或把签名 RapidX `sym/info` 纳入公共缓存。
此次仅代码接入，未变更线上代理部署。
