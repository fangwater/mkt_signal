#!/usr/bin/env python3
"""
xarb 的 symbol 导出（v2）：
  - 输出与 scripts/export_symbol_data_v2.py 一致的“最终汇总 parquet”（NEW 订单 + outcome + 关联信号字段）
  - 支持跨所两条腿符号（如 FIL-USDT-SWAP / FILUSDT）：内部已做 normalize + strategy_id 关联

用法:
  python scripts/export_xarb_symbol_data.py --dir export_data --symbol filusdt --output FILUSDT_order.parquet
"""

from export_symbol_data_v2 import main


if __name__ == "__main__":
    main()

