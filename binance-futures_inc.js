const net = require('net');

// 连接的socket路径，需要与Rust服务端的socket_path一致
const SOCKET_PATH = './binance-futures_inc.sock';

const client = new net.Socket();

// 禁用Nagle算法
client.setNoDelay(true);

// 用于存储未完整处理的数据
let buffer = Buffer.alloc(0);

// 用于存储已经出现的instId
const instIdSet = new Set();

// {
//     e: 'depthUpdate',
//     E: 1747392073714,
//     s: 'BTCUSDT',
//     U: 69006236185,
//     u: 69006236200,
//     b: [
//       [ '103752.41000000', '2.87406000' ],
//       [ '103740.60000000', '0.00094000' ],
//       [ '42000.00000000', '3.24792000' ]
//     ],
//     a: [
//       [ '103777.26000000', '0.02893000' ],
//       [ '104369.58000000', '0.01929000' ]
//     ]
//   }
function parseAndCountInstId(jsonData) {
    if (jsonData.e === 'depthUpdate') {
        if (jsonData.s && !instIdSet.has(jsonData.s)) {
            instIdSet.add(jsonData.s);
            console.log(`发现新的交易对，当前共有 ${instIdSet.size} 个不同的交易对`);
        }
    }
}

client.connect(SOCKET_PATH, () => {
    console.log('已连接到服务器');
});

client.on('data', (data) => {
    // 将新数据追加到现有缓冲区
    buffer = Buffer.concat([buffer, data]);
    
    // 持续处理数据直到缓冲区不足以构成一个完整消息
    while (buffer.length >= 8) { // 至少需要8字节来读取长度和填充
        const messageLength = buffer.readUInt32LE(0);
        const totalLength = 8 + messageLength; // 头部8字节 + 消息体
        
        // 检查是否有足够的数据构成完整消息
        if (buffer.length < totalLength) {
            break;
        }
        
        // 提取消息内容
        const message = buffer.slice(8, totalLength);
        
        try {
            const jsonData = JSON.parse(message);
            parseAndCountInstId(jsonData);
        } catch (e) {
            console.error('解析数据错误:', e);
        }
        
        // 更新buffer，移除已处理的数据
        buffer = buffer.slice(totalLength);
    }
});

client.on('close', () => {
    console.log('连接已关闭');
});

client.on('error', (err) => {
    console.error('连接错误:', err);
});

// 处理程序退出
process.on('SIGINT', () => {
    console.log('正在关闭客户端...');
    client.destroy();
    process.exit();
}); 