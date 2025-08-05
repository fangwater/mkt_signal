const WebSocket = require('ws');

// Binance-spot WebSocket 地址
const wsUrl_spot = 'wss://data-stream.binance.vision/ws';
// const wsUrl_futures = 'wss://fstream.binance.com/ws';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl_spot, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  }
});

// 要订阅的币种列表和频道
const symbols = ['FLMUSDT'];
// const channel = 'depth@0ms';
// const channel = 'trade';
const channel = 'depth@100ms';


// 存储ping发送时间
let pingSentTime = null;
let prevE = 0;

ws.on('open', () => {
  console.log('Binance-spot WebSocket 连接已建立');

  const subscribeMsg_a = {
    method: "SUBSCRIBE",
    params: symbols.map(symbol => `${symbol.toLowerCase()}@${channel}`),
    id: 1
  };
  const subscribeMsg_b = {
    method: "SUBSCRIBE",
    params: ["btcusdt@depth5@100ms"],
    id: 1
  };
  const subscribeMsg_c = {
    method: "SUBSCRIBE",
    params: ["!markPrice@arr"],
    id: 1
  };
  // 发送订阅请求
  ws.send(JSON.stringify(subscribeMsg_a));
  console.log('订阅请求已发送:', subscribeMsg_a);
});

ws.on('pong', (data) => {
  console.log('收到pong消息(on pong)');
  console.log('pong消息体:', data.toString());
});

ws.on('message', (data) => {
  const dataStr = data.toString();
  
  try {
    const message = JSON.parse(dataStr);
    console.log(message);
    //E时ms时间戳，对比和当前时间，计算延迟
    if (prevE == 0){
      prevE = message.E;
    }else{
      console.log('E时间戳差:', message.E - prevE);
      prevE = message.E;
    }
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