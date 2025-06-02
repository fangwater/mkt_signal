const net = require('net');
// {
//     topic: 'orderbook.500.ETHUSDT',
//     type: 'delta',
//     ts: 1747326048744,
//     data: {
//       s: 'ETHUSDT',
//       b: [
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array]
//       ],
//       a: [
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array], [Array], [Array],
//         [Array], [Array]
//       ],
//       u: 32417677,
//       seq: 263467746352
//     },
//     cts: 1747326048742
//   }
// 连接的socket路径
const SOCKET_PATH = './bybit_inc.sock';

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
    if (parts.length === 3) {
        return parts[2];
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

client.connect(SOCKET_PATH, () => {
    console.log(`已成功连接到 Unix Socket: ${SOCKET_PATH}`);
});

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

client.on('close', () => {
    console.log('连接已关闭');
    console.log('尝试在3秒后重新连接...');
    setTimeout(() => {
        console.log('正在尝试重新连接...');
        client.connect(SOCKET_PATH);
    }, 3000);
});

client.on('error', (err) => {
    console.error('连接错误:', err.message);
    if (err.code === 'ECONNREFUSED') {
        console.log('无法连接到 Socket 文件，请确保服务端正在运行');
    }
});

process.on('SIGINT', () => {
    client.destroy();
    process.exit();
}); 