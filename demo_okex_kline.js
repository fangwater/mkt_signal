const WebSocket = require('ws');

// OKEx WebSocket business 地址 (K线数据使用business端点)
const wsUrl = 'wss://ws.okx.com:8443/ws/v5/business';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl);

// 要订阅的币种列表 
const symbols = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"];
const channel = "candle1m"; // 1分钟K线

// 存储ping发送时间
let pingSentTime = null;

ws.on('open', () => {
  console.log('OKEx Business WebSocket 连接已建立');

  // 构建OKEx K线订阅消息
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
  console.log('K线订阅请求已发送:', JSON.stringify(subscribeMsg, null, 2));

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
    
    // 检查是否是K线数据
    if (message.arg && message.arg.channel === "candle1m") {
      console.log('=== K线数据 ===');
      console.log('交易对:', message.arg.instId);
      console.log('频道:', message.arg.channel);
      
      if (message.data && message.data.length > 0) {
        const klineData = message.data[0];
        console.log('K线数据结构:', klineData);
        
        // 解析K线数据: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        if (Array.isArray(klineData) && klineData.length >= 9) {
          const [ts, open, high, low, close, volume, volCcy, volCcyQuote, confirm] = klineData;
          
          console.log(`时间戳: ${ts} (${new Date(parseInt(ts)).toISOString()})`);
          console.log(`开盘价: ${open}`);
          console.log(`最高价: ${high}`);
          console.log(`最低价: ${low}`);
          console.log(`收盘价: ${close}`);
          console.log(`成交量: ${volume}`);
          console.log(`确认状态: ${confirm} (1=已确认/封闭K线, 0=未确认)`);
          
          // 计算封闭时间戳 (开始时间)
          const timestamp = parseInt(ts);
          const closedTimestamp = Math.floor(timestamp / 60000) * 60000;
          console.log(`封闭时间戳: ${closedTimestamp} (${new Date(closedTimestamp).toISOString()})`);
          
          if (confirm === "1") {
            console.log('*** 这是一个封闭的K线 ***');
          }
        }
      }
      console.log('==================');
    } else if (dataStr.eq && dataStr.eq("pong")) {
      // 收到pong消息
      if (pingSentTime) {
        const pongReceivedTime = new Date();
        const latency = pongReceivedTime - pingSentTime;
        console.log(`收到pong消息, 延迟: ${latency}ms`);
      }
    } else {
      console.log('收到其他消息:', message);
    }
  } catch (e) {
    if (dataStr === "pong") {
      // 收到pong消息
      if (pingSentTime) {
        const pongReceivedTime = new Date();
        const latency = pongReceivedTime - pingSentTime;
        console.log(`收到pong消息, 延迟: ${latency}ms`);
      }
    } else {
      console.log('收到非JSON消息:', dataStr);
    }
  }
});

ws.on('error', (err) => {
  console.error('WebSocket 错误:', err);
});

ws.on('close', () => {
  console.log('WebSocket 连接已关闭');
});