# Viz 模块

## 职责概述
- 汇集账户、行情与资金费率数据，形成面向前端可直接消费的快照。
- 通过 Iceoryx 订阅各类内部服务推送的消息，并维护本地共享状态。
- 周期性采样状态，计算价差信号阈值、资金费率等补充信息。
- 提供基于 Axum 的 HTTP/WS 服务，将最新快照按需推送给可视化客户端。

## 模块结构
- `config`：定义 `viz.toml` 结构，包括 HTTP、数据源、采样与阈值配置，并提供异步加载入口。
- `state`：维护共享状态 `SharedState`，管理价格表、账户敞口、资金费率缓存以及最新更新时间；负责解析账户事件、行情/资金费率更新并重算敞口与 PnL。
- `subscribers`：基于 Iceoryx2 建立订阅者，接入账户 (`signal_pubs/...`) 与衍生品行情源，解析消息后调用 `SharedState` 更新；同时附带资金费率 resample 订阅用于调试验证。
- `sampler`：按配置周期从 `SharedState` 抽取数据，构建 `model::Snapshot`，计算基差、资金费率预测占位值、未实现盈亏，并根据 `send_if_changed` 控制是否去重广播。
- `server`：封装 `WsHub` 广播器与 HTTP 服务启动逻辑，对外提供 `/healthz` 和可配置 WebSocket 路径，支持可选的 token 校验。
- `model`：定义前端订阅的快照结构，包括账户汇总、价格队列、资金费率、阈值因子、PnL 以及数据陈旧标记。

## 数据流
1. `viz_server` 入口读取 `VIZ_CFG`（默认为 `config/viz.toml`）并构造 `SharedState`。
2. 在 `tokio::LocalSet` 上启动订阅任务：
   - 账户订阅解析 `AccountUpdateBalance`/`AccountPosition`/`AccountUpdatePosition`，维护现货余额、UM 持仓并触发敞口重算。
   - 衍生品订阅解析 `MarkPrice`、`IndexPrice`、`FundingRate`，同步价格表与资金费率缓存。
   - 资金费率 resample 订阅目前仅验证解码，为后续扩展预留。
3. `Sampler` 每 `sampling.interval_ms` 构建 `Snapshot`：
   - 按需过滤 `symbols`，未配置时输出全部已知交易对。
   - 基于 mark/index 价格计算基差，附带配置阈值。
   - 将 `SharedState` 中的资金费率流数据与 PnL 计算结果写入快照。
   - 若启用 `send_if_changed`，会缓存上一次 JSON 并在快照未变化时跳过广播。
   - 根据账户/价格更新时间与当前时间比较，生成 5 秒超时的陈旧标记。
4. `WsHub` 将快照 JSON 通过广播 channel 分发到所有 WebSocket 连接；HTTP 服务提供心跳接口并在握手时执行可选 token 校验。

## 数据模型要点
- `Snapshot`：包含毫秒时间戳 `ts`、`account` 汇总、`prices`/`funding`/`factors` 列表、`pnl` 与 `stale` 标志。
- `AccountSummary`：汇总总权益、绝对敞口与逐资产敞口 (`ExposureManager` 计算)。
- `PriceItem`：记录 mark/index 价、来源时间戳，缺失时填 `None`/`0`。
- `FactorItem`：以价差 `basis` 结合配置阈值，供前端判定开/平仓条件。
- `FundingItem`：结合实时 funding stream 的值；预测与借贷利率暂返回占位。
- `StaleFlags`：当 5 秒内未收到账户或价格更新时置位，方便前端做可用性提示。

## 配置字段（`config/viz.toml`）
- `[http]`：监听地址、端口、WebSocket 路径；`cors_origins` 和 `auth_token` 可选。
- `[sources.account]`：账户消息的 Iceoryx 服务名，可选显示标签与负载上限。
- `[sources.derivatives]`：衍生品行情服务名与负载上限。
- `[sampling]`：快照间隔（毫秒）、`send_if_changed`（去重开关）、`symbols`（白名单过滤）。
- `[thresholds]`：基差开/平仓阈值，`Sampler` 会写回 `FactorItem` 供 UI 对比。

## 运行与调试建议
- 需要在当前主机安装并运行 Iceoryx 路由，确保配置的服务名与生产发布端一致。
- WebSocket 客户端需连接 `ws://{bind}:{port}{ws_path}` 并在 `auth_token` 启用时附带 `?token=...`。
- `send_if_changed=true` 时如需确认快照推送可通过 `/healthz` 判断服务是否存活，再结合日志调试。
- 若需扩展额外行情来源，可在 `subscribers` 中增加新的 listener，并在 `SharedState` 引入对应缓存与采样逻辑。
