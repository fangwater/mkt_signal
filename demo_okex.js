const WebSocket = require('ws');

// OKEx WebSocket 地址
const wsUrl = 'wss://ws.okx.com:8443/ws/v5/public';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl);

// 要订阅的币种列表和频道
// const symbols = ['BTC-USDT'];
const symbols = ["BTC-USDT-SWAP"];
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

  const x =  {
      "args": [
        {
          "channel": "books",
          "instId": "BTC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ETH-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ETHW-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ETHFI-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BNB-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "LTC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SOL-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SOLV-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BCH-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "XRP-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "LUNA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "AVAX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "TRX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ETC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "DOT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "LINK-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ADA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "DOGE-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SHIB-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "UNI-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "XLM-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "XTZ-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CRO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BAT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "XAUT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BABY-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "USTC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "COMP-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "AAVE-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "FIL-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ZRX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ALGO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "NEO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "IOTA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "MKR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "IOST-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ATOM-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ZIL-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "THETA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "QTUM-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ONT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "USDC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "YFI-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CRV-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BAND-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "DGB-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "RVN-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SCR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ICX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "HBAR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ENJ-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SNX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "RENDER-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "LRC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "RSR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SUSHI-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "TRB-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "MANA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "EGLD-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CVC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "NMR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "KSM-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "UMA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "FLM-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "STORJ-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "AXS-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "STX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ZENT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CELO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "NEAR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "PROMPT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "AVAAI-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "GRT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BNT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CHZ-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "1INCH-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "TRUMP-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "JST-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "SAND-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "RAY-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ICP-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "FLOW-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "GALA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CVX-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "AR-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ARB-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ARKM-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ARC-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "MINA-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "YGG-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "WAXP-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "LPT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "TON-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "ONDO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "LDO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "BRETT-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "MEW-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "AERO-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "CORE-USDT-SWAP"
        },
        {
          "channel": "books",
          "instId": "APE-USDT-SWAP"
        }
      ],
      "op": "subscribe"
    };

  // 发送订阅请求
  ws.send(JSON.stringify(x));
  console.log('订阅请求已发送');

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