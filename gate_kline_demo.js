const WebSocket = require('ws');

const ws = new WebSocket('wss://api.gateio.ws/ws/v4/');

ws.on('open', function open() {
    console.log('Connected to Gate.io');

    // 订阅 BTC_USDT 1分钟K线
    const msg = {
        time: Math.floor(Date.now() / 1000),
        channel: "spot.candlesticks",
        event: "subscribe",
        payload: ["1m", "BTC_USDT"]
    };

    ws.send(JSON.stringify(msg));
    console.log('Sent subscribe:', JSON.stringify(msg, null, 2));
});

ws.on('message', function message(data) {
    const msg = data.toString();
    const parsed = JSON.parse(msg);
    console.log('Received:', JSON.stringify(parsed, null, 2));
});

ws.on('error', function error(err) {
    console.error('WebSocket error:', err);
});

ws.on('close', function close() {
    console.log('Connection closed');
});

// 15秒后关闭
setTimeout(() => {
    console.log('Closing connection...');
    ws.close();
    process.exit(0);
}, 15000);
