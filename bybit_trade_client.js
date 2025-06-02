const net = require('net');
// {
//     topic: 'publicTrade.ETHUSDT',
//     type: 'snapshot',
//     ts: 1747326980818,
//     data: [
//       {
//         T: 1747326980816,
//         s: 'ETHUSDT',
//         S: 'Sell',
//         v: '0.02',
//         p: '2559.42',
//         L: 'MinusTick',
//         i: '131005ed-94b5-54b2-a1ab-01f213bd2721',
//         BT: false,
//         RPI: false
//       }
//     ]
//   }
// 连接的socket路径
const SOCKET_PATH = './bybit_trade.sock';

const client = new net.Socket();

// 禁用Nagle算法
client.setNoDelay(true);

// 用于存储未完整处理的数据
let buffer = Buffer.alloc(0);

// 用于存储已经出现的symbol
const symbolSet = new Set();

// 从topic中提取symbol
function extractSymbolFromTopic(topic) {
    // topic格式: 'orderbook.500.BTCUSDT'
    const parts = topic.split('.');
    if (parts.length === 2) {
        return parts[1];
    }
    return null;
}

// 解析并统计symbol
function parseAndCountSymbol(jsonData) {
    if (jsonData.topic) {
        const symbol = extractSymbolFromTopic(jsonData.topic);
        if (symbol && !symbolSet.has(symbol)) {
            symbolSet.add(symbol);
            console.log(`发现新的交易对，当前共有 ${symbolSet.size} 个不同的交易对`);
        }
    }
}

client.connect(SOCKET_PATH, () => {});

client.on('data', (data) => {
    buffer = Buffer.concat([buffer, data]);
    
    while (buffer.length >= 8) {
        const messageLength = buffer.readUInt32LE(0);
        const totalLength = 8 + messageLength;
        
        if (buffer.length < totalLength) {
            break;
        }
        
        const message = buffer.slice(8, totalLength);
        
        try {
            const jsonData = JSON.parse(message);
            parseAndCountSymbol(jsonData);
        } catch (e) {}
        
        buffer = buffer.slice(totalLength);
    }
});

client.on('close', () => {});

client.on('error', (err) => {});

process.on('SIGINT', () => {
    client.destroy();
    process.exit();
}); 