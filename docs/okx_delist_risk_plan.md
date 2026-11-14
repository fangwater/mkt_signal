# OKX 下架风险接入执行计划

编写时间：2026-11-05 UTC。执行模型：Sol 6.1。

## 用户目标与授权范围

用户确认 CTA 页面中的“风险源待接入”与 OKX 风险源缺失有关，要求编写计划并交给 Sol 6.1 执行。按本计划完成实现、验证和 `jp-meta-elvpn:~/delist_risk_server` 发布；不是只做调研或再次写计划。

将 OKX 公告、完整交易品种目录和官方下架日程接入现有 Delist 服务，使 OKX/Binance CTA Spread 按各自市场准确判断风险。保留已经上线的 `/delist/` FR 日常检查首页及 `/delist/?view=cta` CTA 独立页面，保留备份、看板、配置入口。不得再次重做页面布局。

本任务不调整交易参数、币种池、仓位、订单、账户模式或杠杆；不为 CTA 新增自动移除、dump、flatten 行为。现有 FR 行为和通知范围应保持。

## 开工前重新确认

1. 阅读 `/home/fanghaizhou/mkt_signal/AGENTS.md`，执行 `git status --short`、`git diff --stat`；工作区可能有其他人的改动，仅修改和提交本任务文件。
2. 生产操作只能来自 `/home/fanghaizhou/mkt_signal` 中与 `origin/arbmm` 同步的 `arbmm`。重新核对线上进程、实际二进制、启动脚本及配置差异，不依赖下面的历史观察。
3. 只读检查现有服务风险簿、日志、来源状态及实际 CTA 市场范围。不要打印或复制 `env.sh`、API key、密码、数据库连接串或 cookie 到输出、仓库和聊天。
4. 阅读 `docs/delist_risk_server.md` 和 `docs/public_api_cache.md`。使用新公共端点之前先确认官方契约和调用策略，并更新后者；不新增公共代理，不让私有/写请求进入缓存。

## 已确认的代码缺口

| 位置 | 现状及执行要点 |
| --- | --- |
| `src/common/exchange_info.rs` | `fetch_listing_index` 只接入 Binance、Bitget、Gate、Bybit；没有 OKX。`ListingIndex` 的查找、原生符号保存、状态和完整快照语义需要一并核对。 |
| `src/common/delist_accounts.rs` | `build_account_views_for_venues` 的 coverage 交易所白名单没有 `okex`，并要求各 venue 有目录。不能仅添加白名单就宣称接入完成。 |
| `src/common/delist_schedule.rs` | 已有 OKX SPOT/SWAP provider，通过官方 instruments 的 `expTime` 获取下架日程，应复用而非重新实现一套。 |
| `src/bin/delist_risk_server.rs` | `ingest_schedule_venues` 尚未注册 OKX；`refresh_announcements` 尚未抓取 OKX 公告。来源状态、持久化和周期任务也要接通。 |
| `src/common/announcement_llm.rs` | 抽取提示中的 exchange 枚举目前没有 `okex`；检查解析校验、市场归属及其他 exchange 白名单。 |
| `src/bin/delist_risk_server/cta_mapping.rs` | `signal_risk_views` 已按每条腿计算并聚合 `risk_by_venue`；保留这个结构及已知 Binance 风险，不混合两所品种目录。 |
| `web/delist_risk/index.html` | 已认识 OKX 名称和 venue，但 CTA 只给聚合的“风险源待接入”文案；如需说明缺哪条腿，只做局部风险文案改进。 |

2026-11-05 只读观察：线上风险簿公告来源仅有 Binance、Gate、Bitget、Bybit，没有 OKX。此观察不能代替开工和发布时的重新验证。

注意：现有 `--cta-mapping-check` 路径使用空的 `RiskBook` 和 `ListingIndex::default()`。它能检查账户映射，**不能用于证明真实风险覆盖或线上下架判断正常**。

## 实施步骤

### 1. 确认官方数据契约，保存可重放样本

- 查阅 OKX 官方 API 文档及官方下架公告页面。instruments 基础入口为 `GET https://www.okx.com/api/v5/public/instruments`，按实际所需市场分别请求 SPOT、SWAP；核实响应业务码、字段含义、市场过滤、分页/完整性和限速。
- 找到可持续使用的官方公告列表和正文来源，确认分类、语言、发布日期、分页和去重身份。优先官方明确支持的接口；如使用官方网页结构，记录出处及正文解析依据。不要编造公告 API，也不要把 WAF/登录页当成空结果。
- 核实 `state`、`expTime` 对正常交易、预上线、暂停交易、计划下架、已下架的准确语义。暂时停牌不能随意解释为永久下架。
- 保存脱敏的正常、异常及公告原始样本供测试，记录文档链接、采集时间及预期结果。不得把“当前无下架公告”当成未实现公告接入的替代品。
- 使用受限请求频率、超时、分页上限及失败退避；遵守官方 429 行为。不得重置现有扫描周期或触发批量历史消息推送。

### 2. 接入 OKX 目录和下架日程

- 复用现有公共 HTTP 和解析设施，必要时将现有 OKX instruments 抓取抽为共用 helper，避免为目录和日程重复维护同一外部契约。
- 覆盖部署拥有的 `okex-margin` USDT 现货和 `okex-futures` USDT 线性永续；确认实际账户范围，不擅自加入币本位、交割、期权或其他保证金币种。
- 解决 `BTC-USDT-SWAP`、`BTC-USDT` 与策略内部 `BTCUSDT`/现金目标之间的市场内映射。保留原生 instrument ID，匹配必须 venue-scoped；不要全局改写 Binance/Gate/Bitget/Bybit 的符号规则。
- OKX `live` 等状态在 OKX 路径本地映射，避免为了接入而改变其他交易所的状态含义。未来下架时间产生 pending 风险；未知状态不能静默变成正常。
- 只有完整、成功、校验通过的目录可以用于“目录中缺失 => 已移除”的判断。空响应、业务错误、截断、解析失败和过期/失败数据不得清空有效目录或批量制造下架。
- 接通现有 OKX schedule provider，明确秒/毫秒转换及现有微秒时间戳边界，按各市场写入现有 RiskBook 和来源状态。
- 保证 OKX 上游失败不会让其余四所已有效的监控失效。当前 `refresh_listings` 存在整批发布逻辑；检查新增来源的失败隔离，只做必要且有测试支撑的改动。

### 3. 接入公告发现、正文、抽取和持久化

- 按现有 `bybit_announcement.rs`、Bitget/Gate/Binance 模式增加 OKX 公告适配，复用 `RawAnnouncement`、正文归档、去重、重试、LLM 预算及既有持久化。
- 正文抓取仅访问经过校验的官方 URL；正文缺失、分页不完整、HTTP/WAF/业务码失败必须记录异常，不得显示成功且零公告。
- 扩充 LLM 提示及所有必要校验中的 `okex` 支持，维持现有单一 JSON/IPC/Redis 契约，不增设版本化内部路径、类型或 key。
- 提取实际交易下架和开仓限制及其独立截止时间，保留原公告链接、原生标的与时间依据。单独借贷关闭、抵押资格调整或还款通知仍不能冒充交易下架；个别交易对移除不能扩散成全币种下架。
- 接入现有轮询、来源健康状态、数据库恢复和最近公告查询。重启后已成功处理的公告不应再次抽取或重复告警。
- 检查 `notifications.rs` 的作用域；本次不要把 CTA/OKX 事件擅自发送给仅用于 FR 的通知通道，也不要发送额外测试消息。保持现有通知配置及语义。

### 4. 补全覆盖判断和必要的显示

- 以真实目录与来源状态为依据启用 OKX coverage；不能仅把 `uncovered` 字符串改成 `ok`。区分未接入、读取失败、未就绪/不完整、已接入无风险。
- 每条腿独立判断、再聚合：OKX 异常不吞掉 Binance 已知风险；Binance 异常也不吞掉 OKX 已知风险。两条腿可靠且无风险时才能给出整体正常结论。
- 公告读取失败与目录可用是不同事实，不能把任何一方成功当成另一方成功。复用/扩充当前来源状态表达必要信息，不引入第二套风险模型。
- 页面继续保持 FR 首页和 CTA 独立页。只在需要时展示明确的来源/市场失败说明，保留链接、备份、筛选和折叠/自动刷新行为。

## 测试与验收

使用离线样本为新增行为增加有意义的测试，至少包含：

1. OKX 正常 SPOT/SWAP、未来 `expTime`、已到期、未知/暂停状态、非目标市场过滤及符号映射；两种市场不得互相污染。
2. 完整目录中确实缺失标的能识别；空/错误/不完整/失败/过期目录不能制造下架或宣称健康。单所失败不得破坏其他已覆盖交易所。
3. 公告正文解析、分页边界、去重、抽取失败重试和重启恢复；现货交易对移除、永续 reduce-only 与最终下架分别命中；借贷/抵押通知不误报。
4. CTA 两条腿全部正常、OKX 风险、Binance 风险、单腿来源缺失/失败、两腿同时异常，已知风险不丢失；不触发 CTA 自动交易动作。
5. 前端仍默认 FR，CTA 独立地址可刷新；备份/看板/配置入口可见；320–1440px 无页面横向溢出，公告和敞口展开状态在刷新后保持。

先运行聚焦测试；共享解析、风控状态和符号匹配有改动时按 AGENTS 要求扩大到 `cargo test`。至少执行 `cargo fmt --check`、`cargo check --bin delist_risk_server`、相关库/二进制测试、现有 `scripts/tests/delist_exposure_browser.py` 和必要的 CTA 浏览器回归。Rust 提交前运行 `cargo fmt`，避免纳入无关格式化差异。

浏览器环境可先检查现有 Playwright 与 Chromium 是否仍可用：此前使用 `PYTHONPATH=/tmp/cta-panel-browser-tools`，Chromium 位于 `/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome`，库路径为 `/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu:/tmp/playwright-libs/usr/lib/x86_64-linux-gnu`。这些路径只是线索，不能假定一直有效。

上线验收必须有实际官方来源成功证据及真实风险覆盖结果，不使用空目录的 mapping-check 代替。优先复用正常鉴权路径查看 `/status`、`/announcements`、`/cta-strategies` 与目录持久化结果；没有可用会话时继续完成其他工作，再如实说明缺少的鉴权验证，不能降低鉴权或伪造结果。当前无真实下架也可以验收采集，但风险命中必须由离线样本测试证明。

## 发布与回滚

1. 更新 `docs/delist_risk_server.md` 和 `docs/public_api_cache.md` 的 living 文档及最后更新时间，记录来源、市场范围、频率、覆盖语义和实际验收结果。CPU/IP 未变时不无故修改对应库存；发生变化则同次更新。
2. 只提交本任务文件并验证暂存范围；同步 `origin/arbmm`。生产 Rust release 必须在本地维护工作区构建；如有其他人的并发改动，按仓库政策隔离构建来源并记录源码指纹，不能把无关未提交改动带上线。
3. 操作前说明目标为 `jp-meta-elvpn:~/delist_risk_server`，涉及 OKX USDT 现货/线性永续风险监控，是否重启服务及预期影响。
4. 备份线上二进制、实际脚本和需要替换的页面；保留私有配置及已存在的运维差异。不要整套覆盖远端 `env.sh`、启动参数或 Nginx 配置。
5. 将本地 release 的精确产物 `scp` 到明确临时路径，核验 SHA-256 后原子安装。仅通过环境内 `scripts/stop_delist_risk_server.sh` / `scripts/start_delist_risk_server.sh` 完成必要重启；不远端编译，不用裸 `pm2`/`pmdaemon` 控制进程。
6. 核对来源成功/失败状态、数据时间、公告归档、CTA 双腿结果、FR 原有监控、受保护页面鉴权及进程状态。记录发布前后非目标服务 PID/监听/CPU/IP 的必要证据。
7. 若出现错误 coverage、误报大面积下架、其他交易所风险源退化或现有功能回归，使用备份精确回滚本任务产物并通过环境脚本恢复；保留错误证据，不回退其他人的变更。

## 执行交付

完成后报告：实现范围、官方契约与样本、测试结果、提交号、发布产物 SHA-256、线上验收和真实限制。若外部公告来源确实阻塞，保留明确异常状态，交付已完成且可验证的部分并说明缺口，不将只接目录包装成“OKX 公告监控已完整接入”。
