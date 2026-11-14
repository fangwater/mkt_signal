# gate_fr_arb01 页面不可达排查

- 排查时间：2026-10-18 06:39–06:43 UTC。
- 目标：JP `jp-meta-elvpn` / `jp-meta`，预期目录 `/home/ubuntu/gate_fr_arb01`，Gate FR 页面 `http://13.115.227.29:4191/fr/gate_fr_arb01`。
- **修复：N。当前没有验证可用的访问地址，尚未恢复 HTTP 200。**
- **已确认故障层级：公网 TCP 连接无法建立，VPN SSH 在收到服务端 banner 前关闭。最终根因未确定。** 不能据此断言 nginx/viz 崩溃、主机宕机或安全组配置错误。
- 本次仅做读取和连通性探测。未重启服务或主机，未修改 nginx、防火墙、网络、交易目标、限额、限速、订单或资金；未读取远端 `env.sh`，未输出密钥。

## 分支与操作依据

- 首先读取仓库 `AGENTS.md`；使用 `aws-marketdata-core-layout` 技能核对 JP 双 ENI 布局。
- 本地分支为 `arbmm`，提交 `970fafe5`。成功执行 `git fetch origin arbmm` 后，`HEAD...origin/arbmm` 为 `0 0`。
- 工作树已有大量无关修改，均保留；本次只新增本报告目录内的排查产物。未构建、部署或提交代码。

## 连接证据

HTTP 请求均禁用代理，并设置 8 秒连接超时、12 秒总超时；这些是诊断请求超时，与交易停盘阈值无关。以下三处均成功执行了 curl；远端跳板与 SG 本身可以 SSH 登录。

| curl 执行位置 | 目标 | 结果 | 响应体 |
| --- | --- | --- | --- |
| 本地工作机，源私网地址 `172.16.30.38` | `13.115.227.29:4191/fr/gate_fr_arb01` | exit 28，约 8.002 秒 TCP 建连超时，`http_code=000` | 0 字节 |
| `jp-vpn` 跳板 | 同上 | exit 28，约 8.003 秒 TCP 建连超时，`http_code=000` | 0 字节 |
| `sg`，实际 hostname `ip-172-31-7-123.ap-southeast-1.compute.internal` | 同上 | exit 28，约 8.002 秒 TCP 建连超时，`http_code=000` | 0 字节 |

`000` 是 curl 未收到 HTTP 响应的标记，不是服务器返回的 HTTP 状态码。未收到 404、502 或其他 HTTP 响应，因此尚未到达可诊断具体 location/upstream 响应的阶段。

SSH 与其他路径：

- 本地 ping `10.61.20.5`：3/3 成功，平均约 0.365 ms。只证明该地址有 ICMP 响应，不能证明其后 JP SSH 服务或 nginx 正常。
- `ssh jp-meta-elvpn` 多次重试：TCP 建立，未收到服务端 SSH banner，约 5.07 秒后对端关闭。详细输出为 `kex_exchange_identification: Connection closed by remote host` / `Connection closed by 10.61.20.5 port 10022`，exit 255。独立 socket 探测同样得到 `EOF before banner`。
- `ssh jp-meta`：`13.115.227.29:22` TCP 建连超时，exit 255。
- 经 `jp-vpn` 和 `sg` 跳转到 `jp-meta`：早期 SSH 尝试报 `Connection timed out during banner exchange`；最后一轮采集在 20 秒总预算内未完成。该 ProxyJump 错误本身不能证明目标 TCP 已连接；SG 上直接 socket 测试证实目标 `:22` 建连超时。
- 经 `jp-vpn` 到 `jp-meta-elvpn` 也报 banner exchange timeout；跳板自身到 `10.61.20.5` 的路由走默认公网网关，不具备本地办公室的 VPN 路径。
- 本地 JP IdentityFile 存在且可读。所有 JP 登录均未进入身份认证阶段，未观察到 `Permission denied`，没有证据指向密钥认证失败。
- 按现有 IP 台账，另测 ens42 的 `52.69.209.108:22/:4191`：本地与 SG 均 TCP 超时；ens41 的 FR 地址 `52.69.78.134:22` 从本地也超时。这些是台账地址的连通性测试，未通过实时 AWS 查询核实当前 EIP 归属。

可复核原始记录：

- [http_probes.json](http_probes.json)：三来源 curl 的 UTC 时间、退出码、耗时、状态码和错误。
- [tcp_probes.json](tcp_probes.json)：本地 VPN banner 与公网/备用地址 TCP 探测。
- [ssh_probes.json](ssh_probes.json)：最后一轮四条 SSH 路径的结果。

## 清单完成情况与限制

| 检查项 | 结果 |
| --- | --- |
| JP SSH、替代路径、重试 | 已检查；未成功登录 JP，无法读取其 hostname、负载或确认环境目录实时存在 |
| nginx 是否监听 `0.0.0.0:4191`、systemd 状态 | 因 JP 登录失败，未能现场核验 |
| `/etc/nginx/sites-enabled/crypto_proxy_4191.conf` 的 FR location | 未能读取现场配置，未假定历史配置仍有效 |
| `gate_fr_arb01` viz_server / pmdaemon 状态 | 未能现场核验；未运行任何 raw pmdaemon/PM2 启停命令 |
| JP loopback `:4191` 和 viz upstream curl | 未执行；不能把本地工作机的 localhost 当作 JP localhost |
| 4191 是公网还是 VPN-only | 文档设计明确为共享公网入口，见下述依据；当前 SG/防火墙政策未核验 |
| AWS 安全组、NACL、EIP、实例状态检查 | 无现成 AWS CLI、标准凭据文件或 AWS 凭据环境配置，未取得控制面状态；未尝试修改 |
| JP iptables / ufw / 路由 | 无法登录，未能读取 |
| nginx error / viz crash / OOM 日志 | 无法登录，未能读取；不据此推断崩溃或高负载 |
| UI 服务重启及 HTTP 200 验证 | 未执行重启；没有可达管理路径，也没有确认服务停止的证据；公网复测仍超时 |

本地文档依据（均不能替代本次现场核验）：

- `docs/delist_risk_server.md:558` 明确写明 `4191 is a shared public front`，且不能重生成整个 server 配置以免丢失其他路径。
- `docs/jp-meta-elvpn_hfq_low_latency_tuning_20260618.md:574` 记录历史监听为 `0.0.0.0:4191`，并列出 nginx 配置路径。
- `docs/fr_monitor.md:15` 和 `config/fr_monitor.toml:96` 记录 `gate_fr_arb01` 的 Viz 为 `http://127.0.0.1:20121`，来自 2026-10-08 核验；尚未证明现场 nginx 当前仍映射到此端口。
- `docs/jp-meta-elvpn_ip_binding.md` 记录本次探测的主网卡/备用网卡公网地址。

## 后续定位入口与残留风险

- 需要先取得可用的 JP 主机管理入口，或从 AWS 控制台/已配置的带外通道读取实例状态检查、ENI/EIP 关联、4191/22 对应 SG 与 NACL。当前材料不足以决定重启实例、开放端口或修改路由。
- 三处公网来源失败，加上 SSH 同时异常，支持优先检查主机与网络可达性；仍不能区分实例异常、资源耗尽、主机防火墙、AWS 过滤/路由或 VPN 转发后端问题。
- 入口恢复后，先读取 nginx 状态/监听/FR location、环境内 UI 脚本、viz 进程及近期日志；按实时配置分别 curl JP loopback `:4191` 和实际 upstream，并检查正文非空。
- 只有确认 UI 服务停止且重启是明确修复时，才通过 `/home/ubuntu/gate_fr_arb01/scripts/start_fr_viz_server.sh` 等现场现有 UI 脚本恢复；若仅 nginx 故障，则针对 nginx 处理并先检查配置。不能重启整套交易环境来试错。
- 恢复后需重新从公网来源验证原 URL 返回 HTTP 200 和有效 HTML/JSON。现阶段没有可验证的临时访问方式，SSH 隧道同样依赖管理入口恢复。
- 交易进程、行情、账户和订单当前状态均未取得，不能由页面/SSH 不可达推断交易已停止或仍健康。
