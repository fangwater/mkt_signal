#!/usr/bin/env python3
"""
Health-check ONNX model artifacts served by model_manager.

Checks performed per symbol:
1) GET /api/models/{model_name}/model_onnx/{symbol}
2) Verify non-empty payload
3) Verify feature-dim response header (optional)
4) Optionally keep downloaded .onnx artifacts for offline inspection
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List


DEFAULT_BASE_URL = "http://127.0.0.1:6300"
DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_WORKERS = 8
DEFAULT_FEATURE_DIM_HEADER = "x-model-feature-dim"


def now_ms() -> int:
    return int(time.time() * 1000)


def build_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def sanitize_filename(name: str) -> str:
    out = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def fetch_symbols(base_url: str, model_name: str, timeout_sec: float) -> List[str]:
    url = build_url(
        base_url,
        f"/api/models/{urllib.parse.quote(model_name, safe='')}/symbols",
    )
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    out: List[str] = []
    seen = set()
    for item in payload.get("items", []):
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    out.sort()
    return out


def fetch_one_onnx(
    base_url: str,
    model_name: str,
    symbol: str,
    out_dir: str,
    timeout_sec: float,
    feature_dim_header: str,
    require_feature_dim: bool,
) -> Dict[str, Any]:
    model_q = urllib.parse.quote(model_name, safe="")
    symbol_q = urllib.parse.quote(symbol, safe="")
    url = build_url(base_url, f"/api/models/{model_q}/model_onnx/{symbol_q}")
    out_path = os.path.join(out_dir, f"{sanitize_filename(symbol)}.onnx")

    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read()
            header_raw = resp.headers.get(feature_dim_header, "")

        if not payload:
            return {
                "symbol": symbol,
                "status": "empty_payload",
                "detail": "response body is empty",
                "url": url,
            }

        feature_dim = None
        if header_raw.strip():
            try:
                feature_dim = int(header_raw.strip())
            except ValueError:
                return {
                    "symbol": symbol,
                    "status": "bad_feature_dim",
                    "detail": f"invalid header {feature_dim_header}={header_raw!r}",
                    "url": url,
                }
            if feature_dim <= 0:
                return {
                    "symbol": symbol,
                    "status": "bad_feature_dim",
                    "detail": f"non-positive header {feature_dim_header}={feature_dim}",
                    "url": url,
                }
        elif require_feature_dim:
            return {
                "symbol": symbol,
                "status": "missing_feature_dim",
                "detail": f"missing header {feature_dim_header}",
                "url": url,
            }

        with open(out_path, "wb") as f:
            f.write(payload)

        return {
            "symbol": symbol,
            "status": "ok",
            "onnx_bytes": len(payload),
            "feature_dim": feature_dim,
            "onnx_path": out_path,
            "url": url,
        }
    except urllib.error.HTTPError as exc:
        return {
            "symbol": symbol,
            "status": "fetch_error",
            "detail": f"HTTP {exc.code}: {exc.reason}",
            "url": url,
        }
    except urllib.error.URLError as exc:
        return {
            "symbol": symbol,
            "status": "fetch_error",
            "detail": f"URL error: {exc.reason}",
            "url": url,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "symbol": symbol,
            "status": "fetch_error",
            "detail": f"{type(exc).__name__}: {exc}",
            "url": url,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check model_manager ONNX artifact health per symbol",
    )
    parser.add_argument("--model-name", required=True, help="model name in model_manager")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="model_manager base url")
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=DEFAULT_TIMEOUT_SEC,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="parallel workers",
    )
    parser.add_argument(
        "--feature-dim-header",
        default=DEFAULT_FEATURE_DIM_HEADER,
        help="response header storing feature dim",
    )
    parser.add_argument(
        "--allow-missing-feature-dim",
        action="store_true",
        help="do not fail when feature-dim header is absent",
    )
    parser.add_argument(
        "--only-symbol",
        action="append",
        default=[],
        help="check only selected symbol(s), repeatable",
    )
    parser.add_argument(
        "--work-dir",
        default="",
        help="directory to store downloaded .onnx files (temp dir when empty)",
    )
    parser.add_argument(
        "--keep-onnx",
        action="store_true",
        help="keep downloaded .onnx files",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="optional path to write full json report",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0")
    if args.timeout_sec <= 0:
        raise SystemExit("--timeout-sec must be > 0")

    own_temp_dir = False
    if args.work_dir.strip():
        work_dir = os.path.abspath(args.work_dir.strip())
        os.makedirs(work_dir, exist_ok=True)
    else:
        own_temp_dir = True
        work_dir = tempfile.mkdtemp(prefix="model_onnx_health_")

    model_dir = os.path.join(work_dir, sanitize_filename(args.model_name.strip()))
    os.makedirs(model_dir, exist_ok=True)

    started_ms = now_ms()
    print(
        f"[info] start model_onnx health check: model={args.model_name} base_url={args.base_url} "
        f"workers={args.workers} timeout={args.timeout_sec}s work_dir={model_dir}",
        flush=True,
    )

    try:
        symbols = fetch_symbols(args.base_url, args.model_name, args.timeout_sec)
    except Exception as exc:  # noqa: BLE001
        print(f"[fatal] fetch symbols failed: {type(exc).__name__}: {exc}")
        if own_temp_dir and not args.keep_onnx:
            shutil.rmtree(work_dir, ignore_errors=True)
        return 3

    if args.only_symbol:
        allow = {s.strip() for s in args.only_symbol if s.strip()}
        symbols = [s for s in symbols if s in allow]
    if not symbols:
        print("[fatal] symbol list is empty after filters")
        if own_temp_dir and not args.keep_onnx:
            shutil.rmtree(work_dir, ignore_errors=True)
        return 3

    print(f"[info] symbols={len(symbols)}", flush=True)
    require_feature_dim = not args.allow_missing_feature_dim
    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                fetch_one_onnx,
                args.base_url,
                args.model_name,
                s,
                model_dir,
                args.timeout_sec,
                args.feature_dim_header,
                require_feature_dim,
            ): s
            for s in symbols
        }
        done = 0
        total = len(symbols)
        for fut in as_completed(futures):
            symbol = futures[fut]
            done += 1
            try:
                res = fut.result()
            except Exception as exc:  # noqa: BLE001
                res = {
                    "symbol": symbol,
                    "status": "fetch_error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            results.append(res)
            detail = str(res.get("detail", "")).strip()
            suffix = f" detail={detail}" if detail else ""
            print(f"[{done}/{total}] {symbol}: {res.get('status', 'unknown')}{suffix}", flush=True)

    results.sort(key=lambda x: str(x.get("symbol", "")))
    counts: Dict[str, int] = {}
    for item in results:
        status = str(item.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1

    elapsed_ms = now_ms() - started_ms
    print("[summary] status counts:", flush=True)
    for status in sorted(counts.keys()):
        print(f"  - {status}: {counts[status]}", flush=True)
    print(f"[summary] elapsed_ms={elapsed_ms}", flush=True)

    bad = [r for r in results if r.get("status") != "ok"]
    if bad:
        print("[summary] problematic symbols:", flush=True)
        for r in bad:
            print(
                f"  - {r.get('symbol', '?')}: {r.get('status', 'unknown')} {r.get('detail', '')}".rstrip(),
                flush=True,
            )

    report = {
        "model_name": args.model_name,
        "base_url": args.base_url,
        "feature_dim_header": args.feature_dim_header,
        "allow_missing_feature_dim": args.allow_missing_feature_dim,
        "workers": args.workers,
        "timeout_sec": args.timeout_sec,
        "elapsed_ms": elapsed_ms,
        "counts": counts,
        "results": results,
    }
    if args.json_out.strip():
        out_path = os.path.abspath(args.json_out.strip())
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
        print(f"[summary] json report: {out_path}", flush=True)

    if own_temp_dir and not args.keep_onnx:
        shutil.rmtree(work_dir, ignore_errors=True)
    else:
        print(f"[summary] kept onnx files under: {model_dir}", flush=True)

    return 0 if not bad else 4


if __name__ == "__main__":
    raise SystemExit(main())
