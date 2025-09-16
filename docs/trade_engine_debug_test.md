# Trade Engine 测试与调试指南（Debug 模式）

本文档说明如何在 Debug 模式下运行交易引擎、发送测试订单并订阅回包，同时开启更详细的调试日志。

## 1. 环境准备
- Rust 工具链（nightly 非必需）。
- 币安 API Key/Secret（UM 合约权限）。
- 配置文件：`config/trade_engine.toml`
  - 默认使用 PAPI UM 接口（`base_url = "https://papi.binance.com"`，endpoint 为 `/papi/v1/um/...`）。
  - 如需 FAPI，请调整 `base_url` 与映射（可联系维护者改造 `trade_type_mapping.rs`）。

## 2. 账户配置（2 选 1）
- 写入配置文件：编辑 `config/trade_engine.toml`
  ```toml
  [accounts]
  [[accounts.keys]]
  name = "acct_1"
  key = "YOUR_API_KEY"
  secret = "YOUR_SECRET"
  ```
- 或使用环境变量（推荐临时测试）：
  ```bash
  export BINANCE_API_KEY=你的Key
  export BINANCE_API_SECRET=你的Secret
  export BINANCE_API_NAME=env  # 可选
  ```

## 3. 启动引擎（Debug 模式 + 调试日志）
- 在 Debug 模式下运行，并打开 debug 级别日志：
  ```bash
  RUST_LOG=debug cargo run --bin trade_engine
  ```
- 关键调试日志覆盖：
  - 请求解析：收到 Iceoryx 二进制、头部字段、参数长度。
  - 映射与分发：请求类型 → endpoint/method/weight；构造的参数数量。
  - HTTP 过程：选择的本地 IP/账户、响应状态码、用量 header、body 长度。
  - 回包封装：msg_type、status、body_format（0 原始/1 typed/2 错误）、体长度。

说明：引擎采用忙轮询（无 sleep），以降低延迟；Debug 模式下 CPU 使用可能较高，属预期。

## 4. 订阅响应（可选）
- 新开终端订阅回包：
  ```bash
  export ORDER_RESP_SERVICE="order_resps/binance"
  RUST_LOG=debug cargo run --bin demo_subscribe_resps
  ```
- 输出内容包含统一头部字段与 body 格式：
  - `body_format=1`：typed 二进制（例如 UM 下单已实现 typed 封装）。
  - `body_format=0`：原始 HTTP body（字符串或字节）。
  - `body_format=2`：错误 typed（包含错误码与错误信息）。

- 发送 Demo 已写死参数，无需设置环境变量：
  - 服务名：`order_reqs/binance`
  - 交易对：`SOLUSDT`
  - 数量：`0.03`
  - 方向：`SELL`，`positionSide=SHORT`
  - 运行：
    ```bash
    RUST_LOG=debug cargo run --bin demo_send_um_market_short
    # 运行后在终端内：按 Enter 发送一单，输入 q 回车退出
    ```
- 再启动引擎（或先启动引擎亦可）：
  ```bash
  RUST_LOG=debug cargo run --bin trade_engine
  ```
- DEMO 发送最小参数集：`symbol/side=SELL/type=MARKET/quantity/(positionSide)/newClientOrderId`
- 引擎自动补齐：`timestamp/recvWindow`，并完成签名。
- 该 DEMO 为交互式，Publisher 常驻，按 Enter 才会发送，符合 Iceoryx pub/sub 的使用习惯。

注意：实盘会真实下单，请先小额测试；确认账户持仓模式（单向/双向）与最小下单量/精度符合交易所规则。

## 6. 参数与响应格式说明
- 请求参数格式：query-string（`k=v&k2=v2`），不支持 JSON。重复 key 仅保留最后一个。
- 统一响应头（小端序，32 字节）：
  - `u32 msg_type`（错误时为 ErrorResponse）
  - `i64 local_recv_time`
  - `i64 client_order_id`
  - `u32 exchange`
  - `u16 status`
  - `u16 body_format`（0 原始/1 typed/2 错误）
  - `u32 body_length`
  - 紧随 `body_length` 字节 body。

## 7. 常见问题
- 未收到响应：检查 `ORDER_REQ_SERVICE`/`ORDER_RESP_SERVICE` 与配置一致；确认账户权限与 IP 出口设置有效。
- 429/418：日志会提示并按配置进行冷却/回退；可适当调低请求速率或增加本地 IP。
- 需要更多 typed 回包：提供接口的 JSON 示例，维护者将补齐对应的解析封装。
