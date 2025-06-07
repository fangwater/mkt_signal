关于不同市场的websocket处理

币安
https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/websocket-api-general-info

单次连接API有效期仅为24小时;预计在 24 小时标记后断开连接。

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










