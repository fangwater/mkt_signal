"""Numeric Bybit orderLinkId for REST ops that must reach unmatched persist.

Live mkt_signal client ids are i64: strategy_id << 32 | seq. Account-monitor
drops non-i64 orderLinkId before pre_trade/persist, so flatten/exit scripts
must emit a decimal id in a reserved strategy namespace that no live strategy
uses. pre_trade then fails strategy match and publishes trade_updates_unmatched.
"""

from __future__ import annotations

import time
from typing import Optional

# i32::MAX. Live arb/mm strategy ids sit well below this.
EXTERNAL_ORDER_STRATEGY_ID = 0x7FFFFFFF


def make_external_order_link_id(seq: int, *, now_ms: Optional[int] = None) -> str:
    if seq < 1:
        raise ValueError("seq must be >= 1")
    timestamp_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    packed_seq = ((timestamp_ms & 0xFFFFFFFF) + seq) & 0xFFFFFFFF
    packed = (EXTERNAL_ORDER_STRATEGY_ID << 32) | packed_seq
    return str(packed)
