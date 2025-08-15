const WebSocket = require('ws');

// OKEx WebSocket 地址
const wsUrl = 'wss://ws.okx.com:8443/ws/v5/public';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl);

// 要订阅的币种列表和频道
// const symbols = ['BTC-USDT'];
const symbols = ["BTC-USDT-SWAP"];
const channel = "mark-price"; // 可选: tickers, books, trades, candle1m 等

// 存储ping发送时间
let pingSentTime = null;

ws.on('open', () => {
  console.log('OKEx WebSocket 连接已建立');

  // 构建OKEx订阅消息
  const args = symbols.map(symbol => ({
    channel: channel,
    instId: symbol
  }));
  
  const subscribeMsg = {
    op: "subscribe",
    args: args
  };

  // 发送订阅请求
  ws.send(JSON.stringify(subscribeMsg));
  console.log('订阅请求已发送:', subscribeMsg);

  // 每20秒发送一次ping消息
  setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      const pingMsg = "ping";
      pingSentTime = new Date();
      ws.send(pingMsg);
      console.log(`发送ping消息: ${pingMsg}, 发送时间: ${pingSentTime.toISOString()}`);
    }
  }, 20000); // 20秒 = 20000毫秒
});

ws.on('message', (data) => {
  const dataStr = data.toString();
  try {
    const message = JSON.parse(dataStr);
    console.log('收到消息:', message);
  } catch (e) {
    console.log('收到非JSON消息:', dataStr);
  }
});

ws.on('error', (err) => {
  console.error('WebSocket 错误:', err);
});

ws.on('close', () => {
  console.log('WebSocket 连接已关闭');
});