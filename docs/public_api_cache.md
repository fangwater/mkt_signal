# Exchange Public API Cache

最后更新：2026-09-09 UTC

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

## 已审计、暂缓实施的项目

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
