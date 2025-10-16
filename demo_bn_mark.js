const WebSocket = require('ws');

// Binance Futures WebSocket 地址
const wsUrl_futures = 'wss://fstream.binance.com/ws';

// 创建 WebSocket 连接
const ws = new WebSocket(wsUrl_futures, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  } 
});

// 统计变量
let symbolSet = new Set();
let pushCount = 0;
let lastPushTime = Date.now();
let symbolChangeCount = 0;
let lastSymbolSetSize = 0;
let previousPushSymbols = new Set(); // 保存上一次推送的symbols用于比较 

ws.on('open', () => {
  console.log('Binance Futures WebSocket 连接已建立');
  console.log('开始监控markPrice数据流...\n');

  // 订阅所有标记价格更新
  const subscribeMsg = {
    method: "SUBSCRIBE",
    params: ["!markPrice@arr@1s"],
    id: 1
  };

  ws.send(JSON.stringify(subscribeMsg));
  console.log('已订阅markPrice数据流:', subscribeMsg.params);
  console.log('================================================\n');
});

ws.on('message', (data) => {
  const dataStr = data.toString();
  
  try {
    const message = JSON.parse(dataStr);
    
    // 检查是否是markPrice数组数据
    if (Array.isArray(message)) {
      pushCount++;
      const currentTime = Date.now();
      const timeDiff = currentTime - lastPushTime;
      lastPushTime = currentTime;

      console.log(`\n=== 第${pushCount}次推送 (间隔: ${timeDiff}ms) ===`);
      console.log(`数组长度: ${message.length}`);
      
      // 统计当前推送中的symbols
      const currentPushSymbols = new Set();
      message.forEach(item => {
        if (item.s) {
          currentPushSymbols.add(item.s);
          symbolSet.add(item.s);
        }
      });

      console.log(`当前推送symbol数量: ${currentPushSymbols.size}`);
      console.log(`累计发现symbol数量: ${symbolSet.size}`);
      
      // 与上一次推送比较symbol变化
      if (pushCount > 1) {
        const addedSymbols = new Set([...currentPushSymbols].filter(s => !previousPushSymbols.has(s)));
        const removedSymbols = new Set([...previousPushSymbols].filter(s => !currentPushSymbols.has(s)));
        
        if (addedSymbols.size > 0 || removedSymbols.size > 0) {
          console.log('\n📊 与上次推送的差异:');
          if (addedSymbols.size > 0) {
            console.log(`✅ 新增symbols (${addedSymbols.size}个): ${Array.from(addedSymbols).sort().join(', ')}`);
          }
          if (removedSymbols.size > 0) {
            console.log(`❌ 消失symbols (${removedSymbols.size}个): ${Array.from(removedSymbols).sort().join(', ')}`);
          }
          symbolChangeCount++;
        } else {
          console.log('✓ 与上次推送symbols完全一致');
        }
      } else {
        console.log('首次推送，无法比较');
      }
      
      // 检查累计symbol集合是否有变化
      if (symbolSet.size !== lastSymbolSetSize) {
        const newSymbols = symbolSet.size - lastSymbolSetSize;
        console.log(`🔄 累计symbol变化: 新增 ${newSymbols} 个 (总计: ${symbolSet.size})`);
        lastSymbolSetSize = symbolSet.size;
      }

      // 显示前5个symbol作为示例
      const sampleSymbols = Array.from(currentPushSymbols).slice(0, 5);
      console.log(`示例symbols: ${sampleSymbols.join(', ')}${currentPushSymbols.size > 5 ? '...' : ''}`);
      
      // 保存当前推送的symbols供下次比较
      previousPushSymbols = new Set(currentPushSymbols);
      
      // 每10次推送显示详细统计
      if (pushCount % 10 === 0) {
        console.log('\n📋 统计汇总:');
        console.log(`- 累计推送次数: ${pushCount}`);
        console.log(`- 累计symbol数量: ${symbolSet.size}`);
        console.log(`- symbol变化次数: ${symbolChangeCount}`);
        console.log(`- 平均推送间隔: ~${timeDiff}ms`);
        console.log(`- 最近一次推送symbols总数: ${currentPushSymbols.size}`);
      }
    } else {
      // 处理订阅确认等其他消息
      console.log('收到确认消息:', message);
    }
  } catch (e) {
    console.log('收到非JSON消息:', dataStr);
  }
});

ws.on('error', (err) => {
  console.error('WebSocket 错误:', err);
});

ws.on('close', () => {
  console.log('\n🔌 WebSocket 连接已关闭');
  console.log('\n📈 最终统计:');
  console.log(`- 总推送次数: ${pushCount}`);
  console.log(`- 总symbol数量: ${symbolSet.size}`);
  console.log(`- symbol变化次数: ${symbolChangeCount}`);
  console.log(`- 所有symbols: ${Array.from(symbolSet).sort().join(', ')}`);
}); 
