const net = require('net');

// 连接的socket路径，需要与Rust服务端的socket_path一致
const SOCKET_PATH = './okex-swap_trade.sock';

const client = new net.Socket();

// 禁用Nagle算法
client.setNoDelay(true);

// 用于存储未完整处理的数据
let buffer = Buffer.alloc(0);

// 用于存储已经出现的instId
const instIdSet = new Set();

// 解析并统计instId
function parseAndCountInstId(jsonData) {
    if (jsonData.data && Array.isArray(jsonData.data)) {
        jsonData.data.forEach(trade => {
            if (trade.instId && !instIdSet.has(trade.instId)) {
                instIdSet.add(trade.instId);
                console.log(`发现新的交易对，当前共有 ${instIdSet.size} 个不同的交易对`);
            }
        });
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