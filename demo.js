const WebSocket = require('ws');

// Binance WebSocket 地址
// const wsUrl = 'wss://fstream.binance.com/ws';
const wsUrl = 'wss://stream.bybit.com/v5/public/linear';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl);

// 要订阅的币种列表
const symbols = ['BTCUSDT'];

// 存储ping发送时间
let pingSentTime = null;

ws.on('open', () => {
  console.log('WebSocket 连接已建立');

  // 构造订阅消息（多个币种）
  // const subscribeMsg = {
  //   method: "SUBSCRIBE",
  //   params: symbols.map(symbol => `${symbol}@depth@100ms`),
  //   id: 1
  // };
  const subscribeMsg = {
    op: "subscribe",
    args: symbols.map(symbol => `orderbook.500.${symbol}`),
  };

  // 发送订阅请求
  ws.send(JSON.stringify(subscribeMsg));
  console.log('订阅请求已发送:', subscribeMsg);

  // 每20秒发送一次ping消息
  setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      const pingMsg = {"req_id": "b9640434-1234-5678-9012-345678901234", "op": "ping"};
      pingSentTime = new Date();
      ws.send(JSON.stringify(pingMsg));
      console.log(`发送ping消息: ${JSON.stringify(pingMsg)}, 发送时间: ${pingSentTime.toISOString()}`);
    }
  }, 20000); // 20秒 = 20000毫秒
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  //不打印topic: 'orderbook.500.BTCUSDT'的消息
  if (message.topic !== 'orderbook.500.BTCUSDT') {  
    console.log('收到消息:', message);
  }
});

ws.on('pong', (data) => {
  const pongTime = new Date();
  console.log('收到pong消息');
  console.log('pong消息体:', data.toString());
  if (pingSentTime) {
    console.log(`ping发出时间: ${pingSentTime.toISOString()}`);
    console.log(`pong接收时间: ${pongTime.toISOString()}`);
    console.log(`往返时间: ${pongTime.getTime() - pingSentTime.getTime()}ms`);
  }
});

ws.on('error', (err) => {
  console.error('WebSocket 错误:', err);
});

ws.on('close', () => {
  console.log('WebSocket 连接已关闭');
});