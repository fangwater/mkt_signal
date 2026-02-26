#!/usr/bin/env python3
"""
Health-check tl2cgen model shared libraries served by model_manager.

What this script does:
1) Fetch symbol list for a model from model_manager.
2) Download each symbol's `model_so` binary.
3) In an isolated subprocess, load the `.so` via ctypes and call:
   - `get_num_feature()`
   - `predict(zeros, 0)`
4) Classify each symbol as:
   - ok
   - non_finite
   - bad_dim
   - segfault / signal_crash
   - fetch_error / check_error / timeout

Why subprocess isolation:
If one `.so` segfaults, only that subprocess dies; the parent script continues
and reports exactly which symbol crashed.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List


DEFAULT_BASE_URL = "http://127.0.0.1:6300"
DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_WORKERS = 8


class Entry(ctypes.Union):
    _fields_ = [("missing", ctypes.c_int), ("fvalue", ctypes.c_float)]


def now_ms() -> int:
    return int(time.time() * 1000)


def build_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def http_get_json(url: str, timeout_sec: float) -> Dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload = resp.read()
    return json.loads(payload.decode("utf-8"))


def http_get_bytes(url: str, timeout_sec: float) -> bytes:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return resp.read()


def fetch_symbols(base_url: str, model_name: str, timeout_sec: float) -> List[str]:
    model_q = urllib.parse.quote(model_name, safe="")
    url = build_url(base_url, f"/api/models/{model_q}/symbols")
    payload = http_get_json(url, timeout_sec)
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


def sanitize_filename(name: str) -> str:
    out = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def check_single_so(so_path: str, symbol: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "symbol": symbol,
        "so_path": so_path,
        "status": "check_error",
    }
    try:
        lib = ctypes.CDLL(so_path)

        lib.get_num_feature.restype = ctypes.c_int
        n_features = int(lib.get_num_feature())
        result["n_features"] = n_features
        if n_features <= 0:
            result["status"] = "bad_dim"
            result["detail"] = f"invalid get_num_feature={n_features}"
            return result

        arr = (Entry * n_features)()
        for i in range(n_features):
            arr[i].fvalue = 0.0

        lib.predict.argtypes = [ctypes.POINTER(Entry), ctypes.c_int]
        lib.predict.restype = ctypes.c_float
        predict_value = float(lib.predict(arr, 0))
        result["predict"] = predict_value

        if not math.isfinite(predict_value):
            result["status"] = "non_finite"
            result["detail"] = f"predict={predict_value}"
            return result

        result["status"] = "ok"
        return result
    except Exception as exc:  # noqa: BLE001
        result["status"] = "check_error"
        result["detail"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc(limit=3)
        return result


def check_single_so_with_arity(so_path: str, symbol: str, predict_arity: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "symbol": symbol,
        "so_path": so_path,
        "predict_arity": predict_arity,
        "status": "check_error",
    }
    try:
        lib = ctypes.CDLL(so_path)

        lib.get_num_feature.restype = ctypes.c_int
        n_features = int(lib.get_num_feature())
        result["n_features"] = n_features
        if n_features <= 0:
            result["status"] = "bad_dim"
            result["detail"] = f"invalid get_num_feature={n_features}"
            return result

        arr = (Entry * n_features)()
        for i in range(n_features):
            arr[i].fvalue = 0.0

        if predict_arity == 2:
            lib.predict.argtypes = [ctypes.POINTER(Entry), ctypes.c_int]
            lib.predict.restype = ctypes.c_float
            predict_value = float(lib.predict(arr, 0))
        elif predict_arity == 3:
            lib.predict.argtypes = [ctypes.POINTER(Entry), ctypes.c_int, ctypes.c_int]
            lib.predict.restype = ctypes.c_float
            predict_value = float(lib.predict(arr, 0, 0))
        else:
            result["status"] = "check_error"
            result["detail"] = f"unsupported predict_arity={predict_arity}, expected 2 or 3"
            return result

        result["predict"] = predict_value
        if not math.isfinite(predict_value):
            result["status"] = "non_finite"
            result["detail"] = f"predict={predict_value}"
            return result

        result["status"] = "ok"
        return result
    except Exception as exc:  # noqa: BLE001
        result["status"] = "check_error"
        result["detail"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc(limit=3)
        return result


def run_check_subprocess(
    script_path: str,
    so_path: str,
    symbol: str,
    timeout_sec: float,
    predict_arity: int,
) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        script_path,
        "--check-one",
        "--so-path",
        so_path,
        "--symbol",
        symbol,
        "--predict-arity",
        str(predict_arity),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "symbol": symbol,
            "so_path": so_path,
            "predict_arity": predict_arity,
            "status": "timeout",
            "detail": f"subprocess timeout after {timeout_sec:.1f}s",
        }

    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    parsed: Dict[str, Any] = {}
    if stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = {}

    if proc.returncode == 0:
        if parsed:
            parsed.setdefault("symbol", symbol)
            parsed.setdefault("so_path", so_path)
            return parsed
        return {
                "symbol": symbol,
                "so_path": so_path,
                "predict_arity": predict_arity,
                "status": "ok",
                "detail": "no json output, returncode=0",
            }

    if proc.returncode < 0:
        sig_num = -proc.returncode
        sig_name = signal.Signals(sig_num).name if sig_num in signal.valid_signals() else "UNKNOWN"
        status = "segfault" if sig_num == signal.SIGSEGV else "signal_crash"
        return {
            "symbol": symbol,
            "so_path": so_path,
            "predict_arity": predict_arity,
            "status": status,
            "signal": sig_num,
            "detail": f"terminated by signal {sig_num} ({sig_name})",
            "stderr": stderr,
        }

    if proc.returncode == 139:
        return {
            "symbol": symbol,
            "so_path": so_path,
            "predict_arity": predict_arity,
            "status": "segfault",
            "detail": "subprocess exited 139",
            "stderr": stderr,
        }

    if parsed:
        parsed.setdefault("symbol", symbol)
        parsed.setdefault("so_path", so_path)
        parsed.setdefault("returncode", proc.returncode)
        if stderr and "stderr" not in parsed:
            parsed["stderr"] = stderr
        return parsed

    return {
        "symbol": symbol,
        "so_path": so_path,
        "predict_arity": predict_arity,
        "status": "check_error",
        "detail": f"subprocess returncode={proc.returncode}",
        "stderr": stderr,
    }


def fetch_one_so(
    base_url: str,
    model_name: str,
    symbol: str,
    out_dir: str,
    timeout_sec: float,
) -> Dict[str, Any]:
    model_q = urllib.parse.quote(model_name, safe="")
    symbol_q = urllib.parse.quote(symbol, safe="")
    url = build_url(base_url, f"/api/models/{model_q}/model_so/{symbol_q}")
    so_name = f"{sanitize_filename(symbol)}.so"
    so_path = os.path.join(out_dir, so_name)

    try:
        binary = http_get_bytes(url, timeout_sec)
        if not binary:
            return {
                "symbol": symbol,
                "status": "fetch_error",
                "detail": "empty response body",
                "url": url,
            }
        with open(so_path, "wb") as f:
            f.write(binary)
        return {
            "symbol": symbol,
            "status": "fetched",
            "so_path": so_path,
            "so_bytes": len(binary),
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
        description="Check model_manager model_so health per symbol (segfault/non-finite/dim)",
    )
    parser.add_argument(
        "--model-name",
        required=False,
        help="model name in model_manager, e.g. binance-futures-mm-xgb-test",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"model_manager base url (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=DEFAULT_TIMEOUT_SEC,
        help=f"HTTP and subprocess timeout in seconds (default: {DEFAULT_TIMEOUT_SEC})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"concurrency for fetch/check (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--work-dir",
        default="",
        help="directory to store downloaded .so files; temp dir if empty",
    )
    parser.add_argument(
        "--keep-so",
        action="store_true",
        help="keep downloaded .so files (default: cleaned up when work-dir is empty)",
    )
    parser.add_argument(
        "--only-symbol",
        action="append",
        default=[],
        help="check only selected symbol(s), repeatable",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="optional path to write full json report",
    )
    parser.add_argument(
        "--predict-arity",
        type=int,
        default=2,
        help="predict function arity to call: 2 or 3 (default: 2)",
    )
    parser.add_argument(
        "--check-one",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--so-path",
        default="",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--symbol",
        default="",
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def run_check_one_mode(args: argparse.Namespace) -> int:
    if not args.so_path:
        print(
            json.dumps(
                {
                    "symbol": args.symbol,
                    "status": "check_error",
                    "detail": "--so-path is required in --check-one mode",
                },
                ensure_ascii=True,
            )
        )
        return 2

    result = check_single_so_with_arity(
        args.so_path,
        args.symbol or os.path.basename(args.so_path),
        int(args.predict_arity),
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    status = result.get("status", "check_error")
    if status == "ok":
        return 0
    if status == "non_finite":
        return 11
    if status == "bad_dim":
        return 12
    return 10


def main() -> int:
    args = parse_args()
    if args.check_one:
        return run_check_one_mode(args)

    if not args.model_name:
        print("--model-name is required", file=sys.stderr)
        return 2
    if args.workers <= 0:
        print("--workers must be > 0", file=sys.stderr)
        return 2
    if args.timeout_sec <= 0:
        print("--timeout-sec must be > 0", file=sys.stderr)
        return 2
    if args.predict_arity not in (2, 3):
        print("--predict-arity must be 2 or 3", file=sys.stderr)
        return 2

    model_name = args.model_name.strip()
    if not model_name:
        print("--model-name must not be empty", file=sys.stderr)
        return 2

    own_temp_dir = False
    if args.work_dir.strip():
        work_dir = os.path.abspath(args.work_dir.strip())
        os.makedirs(work_dir, exist_ok=True)
    else:
        own_temp_dir = True
        work_dir = tempfile.mkdtemp(prefix="model_so_health_")

    work_model_dir = os.path.join(work_dir, sanitize_filename(model_name))
    os.makedirs(work_model_dir, exist_ok=True)

    started_ms = now_ms()
    print(
        f"[info] start model_so health check: model={model_name} base_url={args.base_url} "
        f"workers={args.workers} timeout={args.timeout_sec}s predict_arity={args.predict_arity} "
        f"work_dir={work_model_dir}",
        flush=True,
    )

    try:
        symbols = fetch_symbols(args.base_url, model_name, args.timeout_sec)
    except Exception as exc:  # noqa: BLE001
        print(f"[fatal] fetch symbols failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        if own_temp_dir and not args.keep_so:
            shutil.rmtree(work_dir, ignore_errors=True)
        return 3

    if args.only_symbol:
        allow = {s.strip() for s in args.only_symbol if s.strip()}
        symbols = [s for s in symbols if s in allow]

    if not symbols:
        print("[fatal] symbol list is empty after filters", file=sys.stderr)
        if own_temp_dir and not args.keep_so:
            shutil.rmtree(work_dir, ignore_errors=True)
        return 3

    print(f"[info] symbols={len(symbols)}", flush=True)

    script_path = os.path.abspath(__file__)
    results: List[Dict[str, Any]] = []

    def process_symbol(symbol: str) -> Dict[str, Any]:
        fetch_res = fetch_one_so(
            base_url=args.base_url,
            model_name=model_name,
            symbol=symbol,
            out_dir=work_model_dir,
            timeout_sec=args.timeout_sec,
        )
        if fetch_res.get("status") != "fetched":
            return fetch_res
        so_path = str(fetch_res["so_path"])
        check_res = run_check_subprocess(
            script_path=script_path,
            so_path=so_path,
            symbol=symbol,
            timeout_sec=args.timeout_sec,
            predict_arity=args.predict_arity,
        )
        check_res["so_bytes"] = fetch_res.get("so_bytes", 0)
        return check_res

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_symbol, s): s for s in symbols}
        done_count = 0
        total = len(symbols)
        for fut in as_completed(futures):
            symbol = futures[fut]
            done_count += 1
            try:
                res = fut.result()
            except Exception as exc:  # noqa: BLE001
                res = {
                    "symbol": symbol,
                    "status": "check_error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            results.append(res)
            status = res.get("status", "unknown")
            detail = str(res.get("detail", "")).strip()
            suffix = f" detail={detail}" if detail else ""
            print(f"[{done_count}/{total}] {symbol}: {status}{suffix}", flush=True)

    results.sort(key=lambda x: str(x.get("symbol", "")))

    counts: Dict[str, int] = {}
    for r in results:
        status = str(r.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1

    elapsed_ms = now_ms() - started_ms
    print("[summary] status counts:", flush=True)
    for status in sorted(counts.keys()):
        print(f"  - {status}: {counts[status]}", flush=True)
    print(f"[summary] elapsed_ms={elapsed_ms}", flush=True)

    bad_statuses = {
        "fetch_error",
        "check_error",
        "timeout",
        "bad_dim",
        "non_finite",
        "signal_crash",
        "segfault",
    }
    bad = [r for r in results if r.get("status") in bad_statuses]
    if bad:
        print("[summary] problematic symbols:", flush=True)
        for r in bad:
            symbol = r.get("symbol", "?")
            status = r.get("status", "unknown")
            detail = r.get("detail", "")
            print(f"  - {symbol}: {status} {detail}".rstrip(), flush=True)

    report = {
        "model_name": model_name,
        "base_url": args.base_url,
        "workers": args.workers,
        "timeout_sec": args.timeout_sec,
        "predict_arity": args.predict_arity,
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

    if own_temp_dir and not args.keep_so:
        shutil.rmtree(work_dir, ignore_errors=True)
    else:
        print(f"[summary] kept so files under: {work_model_dir}", flush=True)

    if counts.get("segfault", 0) > 0 or counts.get("signal_crash", 0) > 0:
        return 5
    if any(status != "ok" for status in counts):
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
