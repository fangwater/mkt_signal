# sh_dev 常驻多服务穿透

最后更新：2026-11-25

本机主动建立到腾讯云 `sh_dev` 的反向 SSH TCP 连接，云上的服务通过
`127.0.0.1` 访问本机或公司局域网内的已授权服务。按用户确认的范围，
隧道入口仅监听回环地址。2026-11-16 用户另行授权 CTA 策略页面通过腾讯云
公网 IP 访问配置面板，由云端 Nginx 的 80 端口代理页面和受授权保护的配置 API；
其余服务仍按原范围访问。
HTTP、HTTP/2、gRPC、WebSocket 和 Redis
均按原生 TCP 字节流转发，无需改成 HTTP 代理协议。

## 当前映射

| 服务名 | 腾讯云内的入口 | 内网目标 | 用途 |
| --- | --- | --- | --- |
| `history-grpc` | `127.0.0.1:18312` | 本机 `127.0.0.1:18312` | 国内期货历史分钟 gRPC |
| `market-redis` | `127.0.0.1:13005` | 公司 `10.61.20.11:9380` | 通联二级 feeder 的 Redis Pub/Sub，供 Viz 估值 |
| `dominant` | `127.0.0.1:18313` | 本机 `127.0.0.1:18313` | 主力合约 HTTP 查询 |
| `mcp` | `127.0.0.1:8780` | 本机 `127.0.0.1:8780` | MCP HTTP 服务 |
| `cta-config` | `127.0.0.1:19401` | 本机 `127.0.0.1:19401` | CTA 01 配置面板 |
| `cta-config-02` | `127.0.0.1:19402` | 本机 `127.0.0.1:19402` | CTA 02 配置面板（2026-11-25） |

历史 gRPC 地址为 `http://127.0.0.1:18312`。远端 Viz 在 `127.0.0.1:13005`
通过 RESP2 订阅 Redis，上期所频道为 `mdl.22.1.*`，其中 `*` 是频道名的一部分。
本机 CTA 直接连接二级 feeder。公司主行情解析规则见 [cn-futures-pbs](cn_futures_pbs.md)。

主力与 MCP 在云端按原地址访问：

```bash
curl 'http://127.0.0.1:18313/api/v1/dominant'
curl 'http://127.0.0.1:18313/api/v1/dominant/RB?start=2026-11-01&end=2026-11-13'
curl 'http://127.0.0.1:8780/api/cn_dominant_contracts?product=RB'
```

最初搭建隧道时，主力服务尚未发布；2026-11-16 的本次验收已实际查询到
2026-11-16 的 80 个品种记录。隧道部署不负责发布主力表或修改盘后处理结果。

## 常驻与连接隔离

本机运行环境为 `/home/fanghaizhou/sh_dev_service_tunnel`，权限 0700。
每个服务独立运行一个 SSH 进程与 systemd user 单元：

```text
sh_dev_service_tunnel@history-grpc.service
sh_dev_service_tunnel@market-redis.service
sh_dev_service_tunnel@dominant.service
sh_dev_service_tunnel@mcp.service
sh_dev_service_tunnel@cta-config.service
sh_dev_service_tunnel@cta-config-02.service
```

六个实例均已启用开机启动，用户已开启 linger，退出终端后继续运行。
每个服务使用独立 TCP 连接，历史批量传输与实时行情的 TCP 流控彼此隔离；
它们仍共享主机和线路带宽，不承诺带宽预留。单服务故障或重启不重启其他服务。

SSH 每 15 秒发送保活，连续 3 次无响应后退出；systemd 5 秒后重新启动，
每次连接超时 10 秒，不限制重试次数。`ExitOnForwardFailure=yes` 保证远端
监听建立失败时退出重试。此处的自动恢复是重新建立连接，不重放已中断的
RPC 或行情消息。本机、SSH 链路以及目标业务服务需要在线。

旧 `~/sh_dev_dominant_tunnel` 的单连接服务已停用并移除自启单元，旧环境
保留历史配置和日志，新环境接管原 18313／8780 入口。

## 管理入口

所有启停均使用本机环境脚本；不需要进入腾讯云操作这些客户端进程：

```bash
cd /home/fanghaizhou/sh_dev_service_tunnel
./scripts/local_service_tunnel.sh status
./scripts/local_service_tunnel.sh status --service history-grpc
./scripts/local_service_tunnel.sh restart --service history-grpc
./scripts/local_service_tunnel.sh stop --service market-zmq
./scripts/local_service_tunnel.sh start --service market-zmq

# 不带 --service 时，start / stop / restart 操作全部已配置服务。
./scripts/local_service_tunnel.sh start

# 有意中断指定连接并检查自动恢复，同时核对其他服务 PID 不变。
./scripts/local_service_tunnel.sh check-reconnect --service history-grpc
```

配置文件为 `config/tunnel.toml`，每个映射显式填写 `name`、`local_host`、
`local_port` 和 `remote_port`。`local_host` 支持本机回环和局域网 IPv4，
每个云端监听端口唯一。日志独立位于 `logs/<服务名>.log`。
`status` 反映 SSH 进程状态。业务可用性须验证实际 RPC、HTTP 查询或 Redis
订阅与有效行情。

私有 OpenSSH 配置为 `~/.config/mkt_signal/dev_tunnel/ssh_config`，专用密钥
为 `~/.ssh/mkt-sh-dev-tunnel-ed25519`，权限 0600；保留既有 known_hosts
信任并启用严格主机密钥检查。仓库不保存原始 SSH 地址或密钥。

## 新增服务

1. 在环境 `config/tunnel.toml` 新增一个有唯一名称、云端端口的映射。
2. 在云端专用账户的 SSH `PermitListen` 和 authorized_keys 的
   `permitlisten` 同时加入 `127.0.0.1:<云端端口>`；变更前保留原文件，
   用 `sshd -t` 校验并核对原管理用户策略后重载 SSH。
3. 在本机环境运行 `./scripts/local_service_tunnel.sh install`，为配置中的
   实例启用自启，再运行 `start --service <服务名>`。
4. 检查云端监听仅为回环地址，并验证真实业务请求与断线恢复。

删除服务时先通过环境脚本停止对应实例，再移除其自启、配置和云端许可。
单纯删配置不会自动停止已运行的 SSH 连接。

专用无 shell 账户为 `mkt-dev-tunnel`。云端 SSH 限制位于
`/etc/ssh/mkt-dev-tunnel.conf`，由主配置末尾 Include 加载，只允许
上述六个回环端口的 remote forwarding，禁用密码、交互会话、代理、X11
和 Unix socket 转发。专用账户 `GatewayPorts=no`，原 `ubuntu` 的有效
`GatewayPorts=yes` 保持不变。CTA 网页另由 Nginx 提供公网 80 监听，
未将 SSH 转发改为公网监听，也未改其他服务的范围。

## CTA 公网配置面板

云端环境为 `/home/ubuntu/cta_cn_futures_public`，入口是
`http://<腾讯云公网IP>/cta-cn-futures/<实例>/`（2026-11-25 起按实例分路径，`/cta-cn-futures/` 跳转到 `01`）。
Nginx 用 `map` 把 `01 / 02` 分别转发到云端回环 19401 / 19402，并提供页面切换用的 `instances.json`；
新增实例需同时扩展 `map`、路径正则、`instances.json`、隧道映射和 SSH `PermitListen`。
线上文件末尾 include Manager 的 `/etc/nginx/snippets/cn-futures-manager.conf`，配置源已同步保留。
以下为首次上线时的说明，路径前缀均已加上实例段。按用户最终要求复用 cta-prep 配置页面，
Nginx 对外提供 `/cta-cn-futures/`、`GET/POST /cta-cn-futures/api/config` 和
`POST /cta-cn-futures/api/preview`，转发时移除前缀。根路径跳转到该子路径，
省略末尾斜杠时自动补齐。配置读取无需授权，校验和保存要求专用
Bearer 授权，跨域写入被拒绝。页面编辑单份手数、开平仓参数及因子 JSON；
不显示状态卡片、仓位看板、账户阈值或币种券池。

Nginx 配置源为 `config/cta_cn_futures_public.nginx.conf`，云端安装位置为
`/etc/nginx/sites-available/cta-cn-futures`。页面由本机 worker 使用
`web/cta_factor_config/index.html` 提供，云端不再保留独立看板源码。
`nginx.service` 已启用开机启动，API 禁止缓存，代理目标固定为云端回环 19401。
使用云端环境脚本管理：

```bash
ssh sh_dev /home/ubuntu/cta_cn_futures_public/scripts/cta_cn_futures_public.sh status
ssh sh_dev /home/ubuntu/cta_cn_futures_public/scripts/cta_cn_futures_public.sh check
ssh sh_dev /home/ubuntu/cta_cn_futures_public/scripts/cta_cn_futures_public.sh reload --execute
```

公网首页、普通读取、授权预览及不改值保存均已实际验证。未授权校验/保存返回 401，
跨域写入返回 403，过期 revision 返回 409；原 `/state`、`/targets` 和账户 API
均不对外转发。独立的只读看板源码已删除，运行环境旧页面仅作私有备份保留。
原 `cta-targets` 实例已停用并移除自启，`cta-config` 使用同一个 19401 端口，
无需扩大 SSH PermitListen。原四条服务独立连接保持不变。

直接打开子路径即可查看当前配置，无需访问专用链接。校验或保存需要授权，
专用访问凭据仅保存在本机 worker 的私有数据目录中；使用收据中的
`access-link.txt`，链接片段不发送给 Nginx，页面读取后会清除片段并记住授权。
原授权保持有效，旧的根路径授权链接也能跳转进入。
浏览器已实际验证跳转后读取、校验、保存及重新加载，根路径 `/api/*` 不再转发。
此次只重载云端 Nginx，策略进程和五条隧道均保持运行。
详情见 [CTA 运维说明](cta_cn_futures.md)。

私有收据指针为 `data/ops/cta_public_latest.txt`，包含发布校验、SSH 配置备份位置、
公网 URL、浏览器截图和接口验收；最新配置面板收据指针为
`data/ops/cta_config_panel_latest.txt`。仓库不记录原始 SSH 地址或凭据。
子路径发布与验收的私有收据指针为 `data/ops/cta_config_route_latest.txt`。

## 验收结果

2026-11-16 本次发布验证：

- 四个云端端口均实际监听 `127.0.0.1`，四条 SSH 连接分别运行。
- 主力 HTTP 返回 200、80 个品种；MCP RB 查询返回 200、1761 条记录。
- 经云端入口的真实 gRPC `GetDataState` 返回最新交易日 `20261116`；
  `GetMinuteBars(SHFE, rb2701, 20261113)` 返回 345 根分钟 bar。
- 经云端入口的 ZeroMQ ZMTP 握手成功；2 秒观察期收到 0 条行情。
  本次只能确认协议连接，尚未完成盘中连续行情验收。
- 强制结束历史 gRPC 隧道后，systemd 自动恢复；另外三条连接 PID 不变，
  恢复后的真实 gRPC 再次查询成功。
- 配置边界与独立连接命令的 2 项 SDK-free 测试通过。

私有收据指针为 `data/ops/service_tunnel_latest.txt`，其中包含发布前备份、
SSH 策略变更、协议验收和自动恢复结果，均 Git 忽略。

## CTA 预热与实时衔接

此隧道可承载长连接，当前 18312 的 `GetMinuteBars` 本身仍是普通 unary RPC，
尚未提供服务端连续流或按游标重放。CTA 连续预热应在 `market_replay` 的
当前 `CnFuturesMarket` 契约中补齐流式读取、来源标识、固定截止点和续传游标，
客户端按持久化进度恢复，不依赖一次 TCP 连接永久不掉线。

建议云端 CTA 先连接 ZMQ 并将新行情记录到有界内存缓冲及可恢复的本地
日志，再读取历史至一个已封闭分钟的确定截止点。历史、追赶与实时必须进入
相同的期货聚合／因子引擎，检查交接的覆盖、重叠和缺口后才开放信号。
交易日、夜盘、休市以及快照差分沿用共享 `cn_futures_core`，不复写一套。
PUB/SUB 不补发，缓冲或本地日志不能找回采集前、上游或断线中丢失的事件；
缺口须由具备相同输入口径的历史源补齐，不能通过切换实时通道掩盖。

历史 OHLCV 分钟数据只能预热其实际覆盖的依赖。当前分钟查询不含完整五档、
盘口因子输入或大小单等字段，不能据此声称所有 Prep 因子都已预热；应按实际
因子清单扩展统一输入并验证历史与实时口径，原生 CTP 与通联来源也需分别核对。

信号按手数输出：每个独立规则多头 +1 手、空头 -1 手、空仓 0 手，保持
完整绝对目标及规则账本。多规则贡献是否叠加到多手应在策略配置中明确。
这省去按名义金额除以价格计算入场数量；执行侧仍使用行情价格、tick、
合约乘数、保证金和上期所今昨仓规则。预热阶段只恢复因子窗口，不生成
历史交易或重建历史仓位。当前部署的 CTP 观察服务没有因此开启下单。
