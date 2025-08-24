const WebSocket = require('ws');

// OKEx WebSocket 地址
const wsUrl = 'wss://ws.okx.com:8443/ws/v5/public';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl);

// 要订阅的币种列表和频道
// const symbols = ['BTC-USDT'];
const symbols = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"];
const channel = "books"; // 可选: tickers, books, trades, candle1m 等

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

  // OKEx衍生品统一订阅消息 - 整合4种数据类型
  const okexPerpsUnifiedSubscribe = {
    "op": "subscribe",
    "args": [
      // 标记价格 mark-price
      {
        "channel": "mark-price",
        "instId": "BTC-USDT-SWAP"
      },
      {
        "channel": "mark-price", 
        "instId": "ETH-USDT-SWAP"
      },
      // 指数价格 index-tickers
      {
        "channel": "index-tickers",
        "instId": "BTC-USD"
      },
      {
        "channel": "index-tickers",
        "instId": "ETH-USD" 
      },
      // 资金费率 funding-rate
      {
        "channel": "funding-rate",
        "instId": "BTC-USDT-SWAP"
      },
      {
        "channel": "funding-rate",
        "instId": "ETH-USDT-SWAP"
      },
      // 强平信息 liquidation-orders 
      {
        "channel": "liquidation-orders",
        "instType": "SWAP"
      }
    ]
  };

  // 发送统一订阅请求
  ws.send(JSON.stringify(okexPerpsUnifiedSubscribe));
  console.log('OKEx衍生品统一订阅请求已发送 - 包含标记价格、指数价格、资金费率、强平信息');

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
    
    // 根据频道类型处理不同的消息
    if (message.arg && message.arg.channel) {
      const channel = message.arg.channel;
      switch(channel) {
        case 'mark-price':
          console.log('收到标记价格数据:', message);
          break;
        case 'index-tickers':  
          console.log('收到指数价格数据:', message);
          break;
        case 'funding-rate':
          console.log('收到资金费率数据:', message);
          break;
        case 'liquidation-orders':
          console.log('收到强平信息数据:', message);
          break;
        default:
          console.log('收到其他消息:', message);
      }
    } else {
      console.log('收到系统消息:', message);
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