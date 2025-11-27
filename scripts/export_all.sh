#!/bin/bash
# 导出所有持久化数据到 Parquet 文件
# 用法: ./export_all.sh [输出目录] [API地址] [limit]

set -e

OUTPUT_DIR="${1:-./export_data}"
API_BASE="${2:-http://localhost:8088}"
LIMIT="${3:-100000}"  # 默认导出最大 10万 条

# 如果目录已存在则删除旧的
if [ -d "$OUTPUT_DIR" ]; then
    echo "删除旧目录: $OUTPUT_DIR"
    rm -rf "$OUTPUT_DIR"
fi

mkdir -p "$OUTPUT_DIR"

echo "导出目录: $OUTPUT_DIR"
echo "API 地址: $API_BASE"
echo "每类最大条数: $LIMIT"
echo ""

# 导出信号
echo "=== 导出信号 ==="
for kind in signals_arb_open signals_arb_hedge signals_arb_cancel signals_arb_close; do
    echo -n "  $kind ... "
    if curl -sf "${API_BASE}/signals/${kind}/export?limit=${LIMIT}" -o "${OUTPUT_DIR}/${kind}.parquet"; then
        size=$(ls -lh "${OUTPUT_DIR}/${kind}.parquet" | awk '{print $5}')
        echo "OK ($size)"
    else
        echo "FAILED or EMPTY"
        rm -f "${OUTPUT_DIR}/${kind}.parquet"
    fi
done

# 导出订单更新
echo ""
echo "=== 导出订单更新 ==="
echo -n "  order_updates ... "
if curl -sf "${API_BASE}/order_updates/export?limit=${LIMIT}" -o "${OUTPUT_DIR}/order_updates.parquet"; then
    size=$(ls -lh "${OUTPUT_DIR}/order_updates.parquet" | awk '{print $5}')
    echo "OK ($size)"
else
    echo "FAILED or EMPTY"
    rm -f "${OUTPUT_DIR}/order_updates.parquet"
fi

# 导出成交更新
echo ""
echo "=== 导出成交更新 ==="
echo -n "  trade_updates ... "
if curl -sf "${API_BASE}/trade_updates/export?limit=${LIMIT}" -o "${OUTPUT_DIR}/trade_updates.parquet"; then
    size=$(ls -lh "${OUTPUT_DIR}/trade_updates.parquet" | awk '{print $5}')
    echo "OK ($size)"
else
    echo "FAILED or EMPTY"
    rm -f "${OUTPUT_DIR}/trade_updates.parquet"
fi

echo ""
echo "=== 导出完成 ==="
ls -lh "$OUTPUT_DIR"
