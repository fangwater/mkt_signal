const WebSocket = require('ws');

const ws = new WebSocket('wss://api.gateio.ws/ws/v4/');

ws.on('open', function open() {
    console.log('Connected to Gate.io');

    // 订阅多个交易对的 ticker
    const msg = {
        time: Math.floor(Date.now() / 1000),
        channel: "spot.tickers",
        event: "subscribe",
        payload: ["BTC_USDT", "ETH_USDT", "SOL_USDT"]
    };

    ws.send(JSON.stringify(msg));
    console.log('Sent subscribe:', JSON.stringify(msg, null, 2));
});

ws.on('message', function message(data) {
    const msg = data.toString();
    const parsed = JSON.parse(msg);
    console.log('Received:', JSON.stringify(parsed, null, 2));
});

ws.on('ping', function ping() {
    console.log('Received protocol ping, auto-replying pong');
});

ws.on('error', function error(err) {
    console.error('WebSocket error:', err);
});

ws.on('close', function close() {
    console.log('Connection closed');
});

// 应用层心跳 - 每25秒
setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
        const pingMsg = {
            time: Math.floor(Date.now() / 1000),
            channel: "spot.ping"
        };
        ws.send(JSON.stringify(pingMsg));
        console.log('Sent app ping');
    }
}, 25000);

// 30秒后关闭
setTimeout(() => {
    console.log('Closing connection...');
    ws.close();
    process.exit(0);
}, 30000);
