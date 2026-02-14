#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 trade_flow_feature 的金额阈值（只走本目录 JSON 文件）。

本地文件（固定）:
  ./trade_flow_thresholds.json

Redis key:
  {venue}:{symbol}:amount-threshold

Redis value(JSON):
  {
    "symbol": "...",
    "medium_notional_threshold": ...,
    "large_notional_threshold": ...
  }

模式:
  1) 默认（sync）: 读取本目录 JSON，同步到 Redis（包含删除 Redis 多余 symbol）。
  2) --load: 从 Redis 拉取当前 venue 阈值，写回本目录 JSON。

示例（sync）:
  python scripts/sync_trade_flow_thresholds.py --venue binance-futures

示例（load）:
  python scripts/sync_trade_flow_thresholds.py \
    --venue binance-futures \
    --load
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

AMOUNT_THRESHOLD_SUFFIX = "amount-threshold"
LOCAL_JSON_FILENAME = "trade_flow_thresholds.json"
VENUE_DIR_REGEX = re.compile(r"^[a-z0-9]+-(?:futures|margin|spot|swap|perp|perpetual)$")


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


@dataclass(frozen=True)
class ThresholdRow:
    symbol: str
    medium_notional_threshold: float
    large_notional_threshold: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync trade_flow_feature amount-threshold entries via local JSON file"
    )
    p.add_argument("--venue", help="venue，例如 binance-futures；省略时尝试按当前目录名推断")
    p.add_argument(
        "--load",
        action="store_true",
        help="从 Redis 拉取当前 venue 的阈值，写入本目录 JSON 文件",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印计划，不实际写入 Redis 或本地文件",
    )
    p.add_argument("--redis-host", default="127.0.0.1")
    p.add_argument("--redis-port", type=int, default=6379)
    p.add_argument("--redis-db", type=int, default=0)
    p.add_argument("--redis-username", default=None)
    p.add_argument("--redis-password", default=None)
    p.add_argument(
        "--redis-prefix",
        default="",
        help="可选 key 前缀（与 Rust RedisSettings.prefix 含义一致）",
    )
    return p.parse_args()


def parse_optional_f64(raw: Any, field: str) -> Optional[float]:
    if raw is None:
        return None
    try:
        value = float(raw)
    except Exception as exc:
        raise ValueError(f"{field} 不是合法数值: {raw!r}") from exc
    if not _is_finite(value):
        raise ValueError(f"{field} 必须是有限数值: {raw!r}")
    return value


def infer_venue_from_cwd() -> Optional[str]:
    name = Path.cwd().name.lower()
    if VENUE_DIR_REGEX.fullmatch(name):
        return name
    return None


def parse_required_f64(raw: Any, field: str) -> float:
    value = parse_optional_f64(raw, field)
    if value is None:
        raise ValueError(f"缺少字段: {field}")
    return value


def validate_row(row: ThresholdRow) -> None:
    if not row.symbol.strip():
        raise ValueError("symbol 不能为空")
    if not _is_finite(row.medium_notional_threshold) or not _is_finite(row.large_notional_threshold):
        raise ValueError(f"symbol={row.symbol}: medium_notional_threshold/large_notional_threshold 必须是有限数值")
    if row.medium_notional_threshold <= 0.0 or row.large_notional_threshold <= 0.0:
        raise ValueError(f"symbol={row.symbol}: medium_notional_threshold/large_notional_threshold 必须 > 0")
    if row.medium_notional_threshold > row.large_notional_threshold:
        raise ValueError(f"symbol={row.symbol}: medium_notional_threshold 不能大于 large_notional_threshold")


def json_file_path() -> Path:
    return Path.cwd() / LOCAL_JSON_FILENAME


def build_row_from_dict(symbol: str, obj: Dict[str, Any]) -> ThresholdRow:
    medium_notional_threshold = parse_required_f64(obj.get("medium_notional_threshold"), f"{symbol}.medium_notional_threshold")
    large_notional_threshold = parse_required_f64(obj.get("large_notional_threshold"), f"{symbol}.large_notional_threshold")
    row = ThresholdRow(
        symbol=symbol.strip(),
        medium_notional_threshold=medium_notional_threshold,
        large_notional_threshold=large_notional_threshold,
    )
    validate_row(row)
    return row


def load_rows_from_json(path: Path) -> List[ThresholdRow]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"读取或解析 JSON 失败: {path} ({exc})") from exc
    return parse_rows_payload(payload)


def parse_rows_payload(payload: Any) -> List[ThresholdRow]:
    rows: List[ThresholdRow] = []

    # 格式 A: [{"symbol":"BTCUSDT","medium_notional_threshold":...,"large_notional_threshold":...}, ...]
    if isinstance(payload, list):
        for idx, item in enumerate(payload):
            if not isinstance(item, dict):
                raise SystemExit(f"输入数组第 {idx} 项不是对象")
            raw_symbol = item.get("symbol")
            if not isinstance(raw_symbol, str) or not raw_symbol.strip():
                raise SystemExit(f"输入数组第 {idx} 项缺少有效 symbol")
            rows.append(build_row_from_dict(raw_symbol, item))
        return dedup_rows(rows)

    if not isinstance(payload, dict):
        raise SystemExit("输入 JSON 必须是对象或数组")

    # 格式 B: {"BTCUSDT": {"medium_notional_threshold":...,"large_notional_threshold":...}, ...}
    # 格式 C: {"symbol":"BTCUSDT","medium_notional_threshold":...,"large_notional_threshold":...}
    if "medium_notional_threshold" in payload and "large_notional_threshold" in payload:
        raw_symbol = payload.get("symbol")
        if not isinstance(raw_symbol, str) or not raw_symbol.strip():
            raise SystemExit("单对象格式需要包含 symbol 字段")
        return [build_row_from_dict(raw_symbol, payload)]

    for symbol, value in payload.items():
        if not isinstance(symbol, str) or not symbol.strip():
            raise SystemExit(f"对象 key 不是有效 symbol: {symbol!r}")
        if not isinstance(value, dict):
            raise SystemExit(f"symbol={symbol} 的值不是对象")
        rows.append(build_row_from_dict(symbol, value))
    return dedup_rows(rows)


def dedup_rows(rows: List[ThresholdRow]) -> List[ThresholdRow]:
    dedup: Dict[str, ThresholdRow] = {}
    for row in rows:
        dedup[row.symbol] = row
    return [dedup[sym] for sym in sorted(dedup.keys())]


def logical_key(venue: str, symbol: str, suffix: str) -> str:
    return f"{venue}:{symbol}:{suffix}"


def with_prefix(prefix: str, key: str) -> str:
    return f"{prefix}{key}" if prefix else key


def parse_symbol_from_key(
    full_key: str,
    prefix: str,
    venue: str,
    suffix: str,
) -> Optional[str]:
    key = full_key[len(prefix) :] if prefix and full_key.startswith(prefix) else full_key
    parts = key.split(":")
    if len(parts) != 3:
        return None
    key_venue, symbol, key_suffix = parts
    if key_venue != venue or key_suffix != suffix:
        return None
    return symbol


def encode_value(row: ThresholdRow) -> str:
    payload: Dict[str, Any] = {
        "symbol": row.symbol,
        "medium_notional_threshold": row.medium_notional_threshold,
        "large_notional_threshold": row.large_notional_threshold,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _decode_maybe_bytes(v: Any) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", "ignore")
    return str(v)


def _is_finite(v: float) -> bool:
    # 避免引入 math.isfinite 的额外浮点对象判断差异
    return not (v != v or v == float("inf") or v == float("-inf"))


def format_num(v: Optional[float]) -> str:
    if v is None:
        return "-"
    text = f"{v:.10f}".rstrip("0").rstrip(".")
    return text or "0"


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("缺少 redis 包，请先安装: pip install redis")
    kwargs: Dict[str, Any] = {
        "host": args.redis_host,
        "port": args.redis_port,
        "db": args.redis_db,
        "password": args.redis_password,
        "decode_responses": False,
    }
    if args.redis_username:
        kwargs["username"] = args.redis_username
    return redis.Redis(**kwargs)


def row_to_json_obj(row: ThresholdRow) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "medium_notional_threshold": row.medium_notional_threshold,
        "large_notional_threshold": row.large_notional_threshold,
    }
    return out


def parse_row_from_redis_payload(
    symbol_from_key: str,
    payload: Dict[str, Any],
) -> Optional[ThresholdRow]:
    medium_notional_threshold = parse_optional_f64(payload.get("medium_notional_threshold"), f"{symbol_from_key}.medium_notional_threshold")
    large_notional_threshold = parse_optional_f64(payload.get("large_notional_threshold"), f"{symbol_from_key}.large_notional_threshold")
    if medium_notional_threshold is None or large_notional_threshold is None:
        return None
    symbol_raw = payload.get("symbol")
    symbol = symbol_from_key
    if isinstance(symbol_raw, str) and symbol_raw.strip():
        symbol = symbol_raw.strip()
    row = ThresholdRow(
        symbol=symbol,
        medium_notional_threshold=medium_notional_threshold,
        large_notional_threshold=large_notional_threshold,
    )
    validate_row(row)
    return row


def load_from_redis(args: argparse.Namespace, rds) -> int:
    venue = args.venue.strip()
    prefix = args.redis_prefix or ""
    pattern = with_prefix(prefix, f"{venue}:*:{AMOUNT_THRESHOLD_SUFFIX}")
    keys = rds.keys(pattern)
    rows: Dict[str, ThresholdRow] = {}
    parse_errors = 0

    for item in keys:
        full_key = _decode_maybe_bytes(item)
        symbol = parse_symbol_from_key(full_key, prefix, venue, AMOUNT_THRESHOLD_SUFFIX)
        if not symbol:
            continue
        raw = rds.get(full_key)
        if raw is None:
            continue
        text = _decode_maybe_bytes(raw)
        try:
            payload = json.loads(text)
        except Exception:
            parse_errors += 1
            continue
        if not isinstance(payload, dict):
            parse_errors += 1
            continue
        try:
            row = parse_row_from_redis_payload(symbol, payload)
        except Exception:
            parse_errors += 1
            continue
        if row is None:
            parse_errors += 1
            continue
        rows[row.symbol] = row

    out = {sym: row_to_json_obj(rows[sym]) for sym in sorted(rows.keys())}
    out_path = json_file_path()

    print(
        f"load plan: venue={venue} rows={len(out)} parse_errors={parse_errors} "
        f"redis={args.redis_host}:{args.redis_port}/{args.redis_db} prefix={prefix!r} "
        f"file={out_path}"
    )
    if args.dry_run:
        print("[dry-run] local json preview:")
        print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    out_path.write_text(
        json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"done: wrote {len(out)} rows to {out_path}")
    return 0


def sync_from_local_file(args: argparse.Namespace, rds) -> int:
    venue = args.venue.strip()
    prefix = args.redis_prefix or ""
    in_path = json_file_path()
    if not in_path.exists():
        raise SystemExit(f"未找到本地 JSON 文件: {in_path}")

    rows = load_rows_from_json(in_path)
    rows_sorted = sorted(rows, key=lambda x: x.symbol)

    print(
        f"sync plan: venue={venue} rows={len(rows_sorted)} "
        f"redis={args.redis_host}:{args.redis_port}/{args.redis_db} prefix={prefix!r} "
        f"file={in_path}"
    )

    upsert_count = 0
    for row in rows_sorted:
        key = with_prefix(prefix, logical_key(venue, row.symbol, AMOUNT_THRESHOLD_SUFFIX))
        payload = encode_value(row)
        if args.dry_run:
            print(f"[dry-run] SET {key} {payload}")
        else:
            rds.set(key, payload)
        upsert_count += 1

    # Sync semantics: delete Redis keys that are not present in local file.
    wanted_symbols = {row.symbol for row in rows_sorted}
    pattern = with_prefix(prefix, f"{venue}:*:{AMOUNT_THRESHOLD_SUFFIX}")
    keys_to_delete: List[str] = []
    for item in rds.keys(pattern):
        key = _decode_maybe_bytes(item)
        symbol = parse_symbol_from_key(key, prefix, venue, AMOUNT_THRESHOLD_SUFFIX)
        if symbol is None:
            continue
        if symbol not in wanted_symbols:
            keys_to_delete.append(key)

    delete_count = 0
    for key in sorted(set(keys_to_delete)):
        if args.dry_run:
            print(f"[dry-run] DEL {key}")
        else:
            rds.delete(key)
        delete_count += 1

    print(f"done: upsert={upsert_count} delete={delete_count}")
    return 0


def main() -> int:
    args = parse_args()
    venue = (args.venue or "").strip()
    if not venue:
        inferred = infer_venue_from_cwd()
        if inferred:
            venue = inferred
            print(f"[INFO] 未提供 --venue，基于目录推断: {venue}", file=sys.stderr)
    if not venue:
        raise SystemExit("需要 --venue，或在目录名使用 <exchange>-<market> 以自动推断（如 binance-futures）")
    args.venue = venue

    rds = connect_redis(args)
    if args.load:
        return load_from_redis(args, rds)
    return sync_from_local_file(args, rds)


if __name__ == "__main__":
    raise SystemExit(main())
