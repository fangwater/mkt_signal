const WebSocket = require('ws');

// {
//     topic: 'orderbook.200.BTCUSDT',
//     ts: 1750401065888,
//     type: 'delta',
//     data: {
//       s: 'BTCUSDT',
//       b: [
//         [Array], [Array],
//         [Array], [Array],
//         [Array], [Array],
//         [Array]
//       ],
//       a: [
//         [Array], [Array],
//         [Array], [Array],
//         [Array], [Array],
//         [Array], [Array],
//         [Array]
//       ],
//       u: 1359144,
//       seq: 77689879390
//     },
//     cts: 1750401065875
//   }

// Binance WebSocket 地址
//wss://stream.bybit.com/v5/public/spot
const wsUrl = 'wss://stream.bybit.com/v5/public/linear';
// const wsUrl = 'wss://stream.bybit.com/v5/public/spot';
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
    args: symbols.map(symbol => `tickers.BTCUSDT`),
        // args: symbols.map(symbol => `orderbook.500.SOLUSDT`),
  };
  // 发送订阅请求
  ws.send(JSON.stringify(subscribeMsg));
  console.log('订阅请求已发送:', subscribeMsg);
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  console.log('收到消息:', message);
});
