const WebSocket = require('ws');

// Binance WebSocket 地址
// const wsUrl = 'wss://fstream.binance.com/ws';
const wsUrl = 'wss://data-stream.binance.vision/ws';
// const wsUrl = 'wss://stream.bybit.com/v5/public/linear';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl);

// 要订阅的币种列表
const symbols = ['btcusdt'];

ws.on('open', () => {
  console.log('WebSocket 连接已建立');

  // 构造订阅消息（多个币种）
  const subscribeMsg = {
    method: "SUBSCRIBE",
    params: symbols.map(symbol => `${symbol}@depth@100ms`),
    id: 1
  };
  // const subscribeMsg = {
  //   method: "SUBSCRIBE",
  //   params: symbols.map(symbol => `orderbook.500.${symbol}`),
  //   id: 1
  // };

  // 发送订阅请求
  ws.send(JSON.stringify(subscribeMsg));
  console.log('订阅请求已发送:', subscribeMsg);
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  console.log('收到消息:', message);
});

ws.on('error', (err) => {
  console.error('WebSocket 错误:', err);
});

ws.on('close', () => {
  console.log('WebSocket 连接已关闭');
});