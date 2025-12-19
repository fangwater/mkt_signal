# Viz 模块

## 职责概述
- 作为 WebSocket relay：订阅 Iceoryx 的 pre-trade resample（以及可选的 funding resample）并直接转发给前端。
- 提供基于 Axum 的 HTTP/WS 服务，将消息广播给所有连接的客户端。

## 模块结构
- `config`：定义 `viz.toml` 结构（当前仅使用 `[http]`）。
- `subscribers`：基于 Iceoryx2 建立订阅者，接入 pre-trade resample（以及可选的 funding resample），收到后直接广播给 WebSocket 客户端。
- `server`：封装 `WsHub` 广播器与 HTTP 服务启动逻辑，对外提供 `/healthz` 和可配置 WebSocket 路径，支持可选的 token 校验。

## 数据流
1. `viz_server` 入口读取 `VIZ_CFG`（默认为 `config/viz.toml`）并创建 `WsHub`。
2. 在 `tokio::LocalSet` 上启动订阅任务：
   - pre-trade resample：`viz_pubs/pre_trade_exposure|pre_trade_risk`，收到后直接广播给 WebSocket 客户端。
   - funding resample（可选）：`signal_pubs/fr_resample_msg`，收到后广播给 WebSocket 客户端。
3. `WsHub` 将 JSON 通过广播 channel 分发到所有 WebSocket 连接；HTTP 服务提供心跳接口并在握手时执行可选 token 校验。

## 数据模型要点
- `pre_trade_exposure` / `pre_trade_risk`：`viz_pubs/` 的重采样消息，`viz_server` 直接转发。

## 配置字段（`config/viz.toml`）
- `[http]`：监听地址、端口、WebSocket 路径；`cors_origins` 和 `auth_token` 可选。

## 运行与调试建议
- 需要在当前主机安装并运行 Iceoryx 路由，确保配置的服务名与生产发布端一致。
- WebSocket 客户端需连接 `ws://{bind}:{port}{ws_path}` 并在 `auth_token` 启用时附带 `?token=...`。
- `send_if_changed=true` 时如需确认快照推送可通过 `/healthz` 判断服务是否存活，再结合日志调试。
- 若需扩展额外行情来源，可在 `subscribers` 中增加新的 listener，并在 `SharedState` 引入对应缓存与采样逻辑。
