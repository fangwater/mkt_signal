#! /bin/bash
ln -s /home/crypto_mkt/symbol_server/exchange/binance-futures.sock ./binance-futures-symbol.sock
ln -s /home/crypto_mkt/symbol_server/exchange/okex-swap.sock ./okex-swap-symbol.sock
ln -s /home/crypto_mkt/symbol_server/exchange/bybit.sock ./bybit-symbol.sock

# 设置日志级别
export RUST_LOG=info

# 存储所有进程ID的数组
declare -a PIDS=()

# 创建logs目录（如果不存在）
mkdir -p logs

# 清理函数
cleanup() {
    echo "正在关闭所有进程..."
    for pid in "${PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            kill -TERM $pid
        fi
    done
    exit 0
}

# 注册信号处理
trap cleanup SIGINT SIGTERM

# 首先进行release编译
echo "开始release编译..."
cargo build --release

# 检查编译是否成功
if [ $? -ne 0 ]; then
    echo "编译失败，请检查错误"
    exit 1
fi

echo "编译完成，开始启动进程..."
# symbol server位于当前目录的symbol_server/exchange

# 启动所有进程
./target/release/stream --exchange binance-futures --channel inc --symbol-socket ./binance-futures-symbol.sock --monitor-ip=38.55.198.59 > logs/binance-futures-inc.log 2>&1 &
PIDS+=($!)

./target/release/stream --exchange binance-futures --channel trade --symbol-socket ./binance-futures-symbol.sock --monitor-ip=38.55.198.59 > logs/binance-futures-trade.log 2>&1 &
PIDS+=($!)

./target/release/stream --exchange okex-swap --channel inc --symbol-socket ./okex-swap-symbol.sock --monitor-ip=38.55.198.59 > logs/okex-swap-inc.log 2>&1 &
PIDS+=($!)

./target/release/stream --exchange okex-swap --channel trade --symbol-socket ./okex-swap-symbol.sock --monitor-ip=38.55.198.59 > logs/okex-swap-trade.log 2>&1 &
PIDS+=($!)

./target/release/stream --exchange bybit --channel inc --symbol-socket ./bybit-symbol.sock --monitor-ip=38.55.198.59 > logs/bybit-inc.log 2>&1 &
PIDS+=($!)

./target/release/stream --exchange bybit --channel trade --symbol-socket ./bybit-symbol.sock --monitor-ip=38.55.198.59 > logs/bybit-trade.log 2>&1 &
PIDS+=($!)

echo "所有进程已启动，进程ID: ${PIDS[@]}"
echo "日志文件保存在 logs 目录下"
echo "按 Ctrl+C 停止所有进程"

# 等待所有进程
wait

