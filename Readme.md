关于不同市场的websocket处理

币安
https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/websocket-api-general-info

单次连接API有效期仅为24小时;预计在24小时标记后断开连接。

Websocket服务器每3分钟发送一个ping消息。
如果 websocket 服务器在10分钟内没有收到来自连接的pong frame，则连接将断开。
当客户收到ping消息，必需尽快回复pong消息，同时payload需要和ping消息一致。
未经请求的pong消息是被允许的，但是不会保证连接不断开。对于这些pong消息，建议payload为空。

okex
https://www.okx.com/docs-v5/zh/#overview-websocket-overview
如果出现网络问题，系统会自动断开连接
如果连接成功后30s未订阅或订阅后30s内服务器未向用户推送数据，系统会自动断开连接
为了保持连接有效且稳定，建议您进行以下操作：
1. 每次接收到消息后，用户设置一个定时器，定时N秒，N 小于30。
2. 如果定时器被触发（N 秒内没有收到新消息），发送字符串 'ping'。
3. 期待一个文字字符串'pong'作为回应。如果在N秒内未收到，请发出错误或重新连接。

bybit
Due to network complexity, your may get disconnected at any time. Please follow the instructions below to ensure that you receive WebSocket messages on time:
Keep connection alive by sending the heartbeat packet
Reconnect as soon as possible if disconnected

// req_id is a customised ID, which is optional
ws.send(JSON.stringify({"req_id": "100001", "op": "ping"}));

{
    "req_id": "test",
    "op": "pong",
    "args": [
        "1675418560633"
    ],
    "conn_id": "cfcb4ocsvfriu23r3er0-1b"
}

To avoid network or program issues, we recommend that you send the ping heartbeat packet every 20 seconds to maintain the WebSocket connection.

因此需要考虑stream的复用问题，我需要三个stream二进制文件，分别处理
对于binance-futures_stream
1、按照需求增加ping handler 
2、在local维护一个计时器 如果大于180s + delay(delay设置为3s,防止clock误差) 没有收到服务端的ping消息，则重启对应batch的websocket

对于okex-swap_stream
1、对于每个batch的websocket，维护一个定时器，定时器时间为15s的倒计时
2、如果收到新消息，则刷新定时器
3、如果count down后发送字符串ping，等待pong
4、记录这个过程，如果没有成功的返回字符串pong 则重新连接这个websocket，等待时间也是N

对于bybit_stream 
以2s为重启的base，方便整除，维护一个本地的local定时器，N为2s的次数
1、N/10 == 0, 即20s，发送一个ping消息
2、在N+1 即2s内，必须要收到pong消息
记录所有的ping pong事件

从设计的角度考虑
首先3个交易所都要求实现一个本地的计时器
对于币安，这个计时器需要每隔一段时间，检查是否有来自服务器的ping消息 3min，以及发出的pong消息有没有被及时回复 10min
且payload要和ping消息一致

on-msg 不做任何事情
on-ping 收到服务端ping消息触发，ping倒计时恢复到3min，回复pong消息，和ping的payload一致
on-pong 币安不会ping服务端，不会有pong消息
on-ping-timeout 3min没有收到ping消息，重启websocket
on-pong-timeout 不需要处理

维护ping-timer需要本地计时器检查

对于okex 维护一个 收消息倒计时 no-msg-timer 的变量 初始值20s
ping-timer pong-timer
on-msg 每收到一条消息，no-msg-timer恢复到20s，ping-timer，pong-timer不处理
on-no-msg-timeout 发送ping消息，ping-timer设置为20s，开始倒计时，pong-timer仍然不处理
on-ping okex不会ping客户端，client不会收到ping，不会触发，打印log
on-ping-timeout ping消息发出后倒计时结束，重启websocket
on-pong 收到服务端的pong消息，ping-timer恢复到20ms的倒计时状态
on-pong-timeout 服务端不需要响应pong消息，因此不需要处理


对于bybit 按照官方推荐的方式，设置固定的heatbeat
pong-timer初使设定为40s 跳过第一个round
ping-timer 倒计时，还有多久需要发送ping消息
pong-timer 倒计时，表示最迟到
on-ping 不存在
on-ping-timeout 重启
on-pong 收到ping消息对应的pong，检查是否匹配。匹配成功ping-timer重置为20s
on-pong-timeout 客户端不会主动发送pong 因此不需要

整体逻辑如下
1、启动，ping-timer设置为40s，启动计时器
2、40s结束，触发on-ping-timeout，发送ping消息，记录在cache中，开始倒计时pong-timer
分为两种情况
3、在pong-timer结束之前，存在两种情况
pong-timer在倒计时结束前，收到对应的pong，pong-timer停止计时，ping-timer重新计时
pong-timer到技术结束，没有收到对应的pong 直接重启websocket 恢复到开始


6月8日新增
1、由于okex和bybit的pong回复不在pong消息，需要parser后才知道字端

解决快照问题
    const BASE_URL: &str = "https://data-api.binance.vision"; // 币安rest api
    const ENDPOINT: &str = "/api/v3/depth"; // 获取深度 
是spot的请求方法 limit = 1000
用curl测试连通性:

用future的 看数据是否一致
https://fapi.binance.com
/fapi/v1/depth


增加币安期货行情对应的现货行情
1、对于所有在币安期货出现的symbol(xx-usdt)，是否有对应的现货symbol
2、作为spot组进行订阅

binance-futures 
binance
okex-swap
okex
bybit
bybit-spot


1、binance-futures 
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream-for-All-market
futures Mark Price Stream for All market 只需要1分钟频率的
futures Kline/Candlestick Streams 1m 只需要1分钟频率的
futures Liquidation Order Streams 

spot Kline/Candlestick Streams 1m 只需要1分钟频率的

futures Mark Price Stream for All market
futures Kline/Candlestick Streams 1m
spot Kline/Candlestick Streams 1m





我的一个需求，我需要获取交易所到我收到行情的时间差。因此我需要在manager中管理这一点。











## 本地 IP 绑定的 WebSocket 连接（tokio + tokio-tungstenite）

项目已支持在 tokio 环境下以“指定本地 IP”建立 WebSocket 连接，兼容 `ws://` 与 `wss://`。

- 入口函数：`src/connection/connection.rs` 中 `WsConnector::connect_with_local_ip(url, sub_msg, local_ip)`。
- 实现方式：
  - 使用 `TcpSocket::bind((local_ip, 0))` 绑定本地 IP 后 `connect` 到目标地址；
  - `ws://` 走 `MaybeTlsStream::Plain` → `client_async`；
  - `wss://` 先用 `native-tls`/`tokio-native-tls` 做 TLS 握手（SNI 为 URL 的域名），再包裹为 `MaybeTlsStream::NativeTls` → `client_async`；
  - 最终类型与 `connect_async` 保持一致：`WebSocketStream<MaybeTlsStream<TcpStream>>`。

### Demo：绑定本地 IP 连接并打印消息

已提供最小演示程序：`src/bin/ws_bind_ip_demo.rs`

构建：

```
cargo build --release
```

运行示例（Binance 现货 trade 流，按需调整订阅 JSON）：

```
target/release/ws_bind_ip_demo \
  --url wss://stream.binance.com:9443/ws \
  --local-ip 192.168.1.10 \
  --json '{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}' \
  --print 5
```

通用回显服务测试（验证通路）：

```
target/release/ws_bind_ip_demo \
  --url wss://echo.websocket.events \
  --json '"hello"' \
  --print 1
```

参数说明：

- `--url`：WebSocket 地址（支持 ws/wss）。
- `--local-ip`：本地绑定 IP；`0.0.0.0` 或空表示不强制绑定。
- `--json`：连接后立即发送的 JSON 文本。
- `--print`：打印收到的消息条数后退出。
- `--timeout-secs`：整体超时秒数。

注意事项：

- 本地 IP 必须存在于本机网卡；确保与目标地址族（IPv4/IPv6）匹配。
- `wss://` 场景 TLS 握手使用 URL 的域名作为 SNI（不要用 IP）。
- 生产使用建议加入重试、超时与清晰日志（本项目内已实现基础重试与日志）。


现在处理资金费率策略的一个辅助工具。
这个工具需要监听盘口最优价格。即最佳的ask和bid价格，以及维护最有买卖的量、
最后一次更新时间等信息。


这个盘口价格，对于币安、bybit、和okex，都是来自spread msg，这已经在上游
处理过了。需要的是构建一个symbol-->盘口struct的维护。symbol也来自msg。如果没有就创建。


注意，在更新的时候，需要对比最近一次的时间。如果是旧的消息，就drop掉。

把这个维护最优的逻辑，单独维护在一个rs里面。


我现在需要在src下增加一个新的模块，我要编写一个tokio单线程的进程。叫trade_engine专门用于下单。trade
▌engine通过iceyx sub一个通道，这个通道会推送下单的event。然后，需要有自己的toml配置，重点编写基础架
▌构的部分。这个eng的目标是，可以配置多个ip。我现在需要你实现，可以配置rest地址，支持多个api发送请
▌求。我的关键是，我需要一个分派工具，处理限制。限制有账号频率限制，每个下单请求回报将包含一个X-MBX-
▌ORDER-COUNT-(intervalNum)(intervalLetter)的头，其中包含当前账户已用的下单限制数量。
▌被拒绝或不成功的下单并不保证回报中包含以上头内容。
▌下单频率限制是基于每个账户计数的。
▌统一账户下单频率限制为1200/min。但我希望是配置化的。如果监测到账号限制，则打印log warn。其次是ip，
▌这是关键，每个请求将包含一个X-MBX-USED-权重-(intervalNum)(intervalLetter)的头，其中包含当前IP所有请
▌求的已使用权重。
▌每个路由都有一个"权重"，该权重确定每个接口计数的请求数。较重的接口和对多个交易对进行操作的接口将具
▌有较重的"权重"。
▌收到429时，您有责任作为API退回而不向其发送更多的请求。
▌如果屡次违反速率限制和/或在收到429后未能退回，将导致API的IP被禁(http状态418)。
▌频繁违反限制，封禁时间会逐渐延长 ，对于重复违反者，将会被封从2分钟到3天。
▌访问限制是基于IP的，而不是API Key
▌统一账户IP访问频率限制为6000/min。 你需要裁决合适的ip进行请求下发。HTTP 4XX 错误码用于指示错误的请
▌求内容、行为、格式。
▌HTTP 403 错误码表示违反WAF限制(Web应用程序防火墙)。
▌HTTP 429 错误码表示警告访问频次超限，即将被封IP
▌HTTP 418 表示收到429后继续访问，于是被封了。
▌HTTP 5XX 错误码用于指示Binance服务侧的问题。
▌如果返回内容里包含了报错信息 "Request occur unknown error."，请稍后重试请求。
▌HTTP 503 表示三种可能：
▌如果返回内容里包含了报错信息 "Unknown error, please check your request or try again later."，则表示
▌API服务端已经向业务核心提交了请求但未能获取响应，特别需要注意的是其不代表请求失败，而是未知。很可能
▌已经得到了执行，也有可能执行失败，需要做进一步确认。
▌如果返回内容里包含了报错信息 "Service Unavailable."，则表示本次API请求失败。这种情况下可能是服务暂
▌不可用，您需要稍后重试。
▌如果返回内容里包含了报错信息 "Internal error; unable to process your request. Please try again."，
▌则表示本次API请求失败。这种情况下您如果需要的话可以选择立即重试。 同时做好处理。