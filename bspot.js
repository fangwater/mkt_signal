const WebSocket = require('ws');

// Binance-spot WebSocket 地址
// const wsUrl_spot = 'wss://data-stream.binance.vision/ws';
const wsUrl_futures = 'wss://fstream.binance.com/ws';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl_futures, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  }
});

// 要订阅的币种列表和频道
const symbols = ['BTCUSDT'];
const channel = 'depth@0ms';
// const channel = 'trade';

// 存储ping发送时间
let pingSentTime = null;

ws.on('open', () => {
  console.log('Binance-spot WebSocket 连接已建立');

  const subscribeMsg = {
    method: "SUBSCRIBE",
    params: symbols.map(symbol => `${symbol.toLowerCase()}@${channel}`),
    id: 1
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

ws.on('pong', (data) => {
  console.log('收到pong消息(on pong)');
  console.log('pong消息体:', data.toString());
});

ws.on('message', (data) => {
  const dataStr = data.toString();
  
  // 处理pong消息
  if (dataStr === 'pong') {
    const pongTime = new Date();
    console.log('收到pong消息(on message)');
    if (pingSentTime) {
      console.log(`ping发出时间: ${pingSentTime.toISOString()}`);
      console.log(`pong接收时间: ${pongTime.toISOString()}`);
      console.log(`往返时间: ${pongTime.getTime() - pingSentTime.getTime()}ms`);
    }
    return;
  }

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