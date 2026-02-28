#!/usr/bin/env python3
"""
Config-driven factor pool pipeline:
1) Pull model metadata/artifacts from model_manager
2) Read source H5 and rebuild by model factor order
3) Run XGBoost inference in Python
4) Run ONNX inference in Rust (onnx_csv_predict)
5) Compare outputs and export metrics
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import tempfile
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import numpy as np


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run factor-pool rebuild + XGB/ONNX compare pipeline from TOML config"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/factor_pool_pipeline.toml"),
        help="pipeline TOML config path",
    )
    return parser.parse_args()


def require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("missing dependency pandas, please run: pip install pandas") from exc
    try:
        import tables  # noqa: F401
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "missing dependency tables (PyTables), please run: pip install tables"
        ) from exc
    return pd


def require_xgboost():
    try:
        import xgboost as xgb
    except ModuleNotFoundError as exc:
        raise RuntimeError("missing dependency xgboost, please run: pip install xgboost") from exc
    return xgb


def load_toml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"config file not found: {path}")
    with path.open("rb") as f:
        data = tomllib.load(f)
    if not isinstance(data, dict):
        raise RuntimeError(f"invalid TOML root type: {type(data).__name__}")
    return data


def get_table(cfg: dict[str, Any], key: str) -> dict[str, Any]:
    value = cfg.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"config.{key} must be a table")
    return value


def get_str(tbl: dict[str, Any], key: str, default: str = "", required: bool = False) -> str:
    value = tbl.get(key, default)
    if not isinstance(value, str):
        raise RuntimeError(f"config.{key} must be string")
    out = value.strip()
    if required and not out:
        raise RuntimeError(f"config.{key} must not be empty")
    return out


def get_int(tbl: dict[str, Any], key: str, default: int) -> int:
    value = tbl.get(key, default)
    if not isinstance(value, int):
        raise RuntimeError(f"config.{key} must be int")
    return value


def get_float(tbl: dict[str, Any], key: str, default: float) -> float:
    value = tbl.get(key, default)
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise RuntimeError(f"config.{key} must be float")
    return value


def build_url(base_url: str, path: str, query: dict[str, str] | None = None) -> str:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if query:
        return f"{url}?{urllib.parse.urlencode(query)}"
    return url


def http_get_json(url: str, timeout_sec: float) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json", "Accept-Encoding": "identity"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {url}; {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error for {url}: {exc.reason}") from exc

    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise RuntimeError(f"unexpected json type for {url}: {type(obj).__name__}")
    return obj


def list_models(base_url: str, timeout_sec: float) -> list[str]:
    payload = http_get_json(build_url(base_url, "/api/models"), timeout_sec)
    out: list[str] = []
    for item in payload.get("items", []):
        model_name = str(item.get("model_name", "")).strip()
        if model_name:
            out.append(model_name)
    return out


def fetch_symbol_detail(
    base_url: str,
    model_name: str,
    symbol: str,
    timeout_sec: float,
    group_key: str,
) -> dict[str, Any] | None:
    model_q = urllib.parse.quote(model_name, safe="")
    symbol_q = urllib.parse.quote(symbol, safe="")
    query = {"group_key": group_key} if group_key else None
    url = build_url(base_url, f"/api/models/{model_q}/symbols/{symbol_q}", query=query)

    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {url}; {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error for {url}: {exc.reason}") from exc

    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise RuntimeError(f"unexpected json type for {url}: {type(obj).__name__}")
    return obj


def resolve_model_name(
    base_url: str,
    model_name_arg: str,
    symbol: str,
    timeout_sec: float,
    group_key: str,
) -> tuple[str, dict[str, Any]]:
    if model_name_arg:
        detail = fetch_symbol_detail(base_url, model_name_arg, symbol, timeout_sec, group_key)
        if detail is None:
            raise RuntimeError(f"symbol {symbol} not found in model {model_name_arg}")
        return model_name_arg, detail

    candidates: list[tuple[str, dict[str, Any]]] = []
    for model_name in list_models(base_url, timeout_sec):
        detail = fetch_symbol_detail(base_url, model_name, symbol, timeout_sec, group_key)
        if detail is None:
            continue
        candidates.append((model_name, detail))

    if not candidates:
        raise RuntimeError(f"no model found for symbol={symbol}")
    if len(candidates) > 1:
        names = ", ".join(name for name, _ in candidates)
        raise RuntimeError(
            f"multiple models found for symbol={symbol}: {names}; please set model.model_name"
        )
    return candidates[0]


def fetch_model_json(
    base_url: str,
    model_name: str,
    symbol: str,
    timeout_sec: float,
) -> str:
    model_q = urllib.parse.quote(model_name, safe="")
    symbol_q = urllib.parse.quote(symbol, safe="")
    model_url = build_url(base_url, f"/api/models/{model_q}/model/{symbol_q}")

    model_payload = http_get_json(model_url, timeout_sec)
    payload = model_payload.get("payload") or {}
    model_json = payload.get("model_json")
    if not isinstance(model_json, str) or not model_json.strip():
        raise RuntimeError(f"empty payload.model_json from {model_url}")
    return model_json


def normalize_h5_key(key: str) -> str:
    return key if key.startswith("/") else f"/{key}"


def resolve_h5_key(pd_mod: Any, h5_path: Path, h5_key: str) -> str:
    with pd_mod.HDFStore(str(h5_path), mode="r") as store:
        keys = list(store.keys())
    if not keys:
        raise RuntimeError(f"input h5 has no keys: {h5_path}")

    if h5_key.strip():
        k = normalize_h5_key(h5_key.strip())
        if k not in keys:
            raise RuntimeError(f"h5 key not found: key={h5_key!r} available={keys[:10]}")
        return k

    if len(keys) == 1:
        return keys[0]
    if "/data" in keys:
        return "/data"
    raise RuntimeError(
        f"multiple h5 keys found, set input.h5_key explicitly: available={keys[:10]}"
    )


def load_rebuilt_df(
    pd_mod: Any,
    h5_path: Path,
    h5_key: str,
    factor_order: list[str],
    head: int,
) -> tuple[str, Any]:
    read_key = resolve_h5_key(pd_mod, h5_path, h5_key)
    kwargs: dict[str, Any] = {"key": read_key}
    if head > 0:
        kwargs["stop"] = int(head)
    df = pd_mod.read_hdf(str(h5_path), **kwargs)
    if df.empty:
        raise RuntimeError(f"input h5 has no rows: {h5_path} key={read_key}")

    missing = [name for name in factor_order if name not in df.columns]
    if missing:
        raise RuntimeError(f"h5 missing model factor columns: first_missing={missing[:20]}")

    return read_key, df.loc[:, factor_order].copy()


def write_rebuilt_h5(
    pd_mod: Any,
    rebuilt_df: Any,
    output_h5: Path,
    output_key: str,
    meta: dict[str, Any],
) -> str:
    output_h5.parent.mkdir(parents=True, exist_ok=True)
    key = normalize_h5_key(output_key)
    rebuilt_df.to_hdf(str(output_h5), key=key, mode="w", format="table")
    with pd_mod.HDFStore(str(output_h5), mode="a") as store:
        storer = store.get_storer(key)
        for k, v in meta.items():
            setattr(storer.attrs, k, v)
    return key


def normalize_predictions(raw: np.ndarray, expected_rows: int, side: str) -> np.ndarray:
    arr = np.asarray(raw)
    if arr.ndim == 1:
        out = arr
    elif arr.ndim == 2 and arr.shape[1] == 1:
        out = arr[:, 0]
    else:
        raise RuntimeError(f"{side} output shape not supported: {arr.shape}")
    if int(out.shape[0]) != int(expected_rows):
        raise RuntimeError(
            f"{side} prediction row mismatch: got={out.shape[0]}, expected={expected_rows}"
        )
    return np.asarray(out, dtype=np.float64)


def write_feature_csv(rebuilt_df: Any, feature_csv: Path) -> None:
    feature_csv.parent.mkdir(parents=True, exist_ok=True)
    rebuilt_df.to_csv(feature_csv, index=False, float_format="%.17g")


def write_xgb_pred_csv(xgb_pred: np.ndarray, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["idx", "xgb"])
        for idx, value in enumerate(xgb_pred):
            writer.writerow([idx, f"{float(value):.17g}"])


def run_rust_onnx_predict(
    rust_cfg_path: Path,
    feature_csv: Path,
    onnx_pred_csv: Path,
    feature_dim: int,
    base_url: str,
    model_name: str,
    symbol: str,
    timeout_sec: float,
    rust_tbl: dict[str, Any],
) -> None:
    mode = get_str(rust_tbl, "mode", default="cargo_run")
    cargo_bin = get_str(rust_tbl, "cargo_bin", default="cargo")
    bin_name = get_str(rust_tbl, "bin_name", default="onnx_csv_predict")
    binary_path = get_str(rust_tbl, "binary_path", default="target/release/onnx_csv_predict")
    log_every = get_int(rust_tbl, "log_every", default=50_000)

    rust_cfg_path.parent.mkdir(parents=True, exist_ok=True)
    rust_cfg_path.write_text(
        "\n".join(
            [
                f"model_manager_base_url = {json.dumps(base_url)}",
                f"model_name = {json.dumps(model_name)}",
                f"symbol = {json.dumps(symbol)}",
                f"model_manager_timeout_sec = {float(timeout_sec)}",
                f"input_csv = {json.dumps(str(feature_csv))}",
                f"output_csv = {json.dumps(str(onnx_pred_csv))}",
                "has_header = true",
                'delimiter = ","',
                f"expected_feature_dim = {int(feature_dim)}",
                f"log_every = {int(log_every)}",
                f"model_onnx_path = {json.dumps(get_str(rust_tbl, 'model_onnx_path', default='/api/models/{model_name}/model_onnx/{symbol}'))}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    if mode == "cargo_run":
        cmd = [cargo_bin, "run", "--release", "--bin", bin_name, "--", "--config", str(rust_cfg_path)]
    elif mode == "binary":
        cmd = [binary_path, "--config", str(rust_cfg_path)]
    else:
        raise RuntimeError(f"unsupported rust_onnx.mode: {mode}")

    proc = subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "rust onnx inference failed:\n"
            f"cmd={' '.join(cmd)}\n"
            f"exit_code={proc.returncode}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    if proc.stdout.strip():
        print(proc.stdout.strip())
    if proc.stderr.strip():
        print(proc.stderr.strip())


def load_onnx_pred_csv(path: Path) -> np.ndarray:
    if not path.is_file():
        raise FileNotFoundError(f"onnx prediction csv not found: {path}")

    values: list[float] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "idx" not in (reader.fieldnames or []) or "onnx" not in (reader.fieldnames or []):
            raise RuntimeError(f"onnx prediction csv header invalid: {path}")
        for row_no, row in enumerate(reader):
            raw_idx = row.get("idx", "")
            raw_val = row.get("onnx", "")
            try:
                idx = int(raw_idx)
            except ValueError as exc:
                raise RuntimeError(f"invalid idx at row={row_no}: {raw_idx!r}") from exc
            if idx != row_no:
                raise RuntimeError(
                    f"onnx idx mismatch at row={row_no}: got idx={idx}, expect={row_no}"
                )
            try:
                values.append(float(raw_val))
            except ValueError as exc:
                raise RuntimeError(f"invalid onnx value at row={row_no}: {raw_val!r}") from exc
    return np.asarray(values, dtype=np.float64)


def safe_pearson(x: np.ndarray, y: np.ndarray) -> float | None:
    if x.size < 2 or y.size < 2:
        return None
    x_std = float(np.std(x))
    y_std = float(np.std(y))
    if x_std == 0.0 or y_std == 0.0:
        return None
    corr = np.corrcoef(x, y)
    return float(corr[0, 1])


def compute_metrics(
    xgb_pred: np.ndarray,
    onnx_pred: np.ndarray,
    rel_eps: float,
    top_k: int,
) -> dict[str, Any]:
    diff = onnx_pred - xgb_pred
    abs_diff = np.abs(diff)
    rel_abs_diff = abs_diff / np.maximum(np.abs(xgb_pred), rel_eps)

    quantiles = [0.5, 0.9, 0.95, 0.99]
    abs_q = {f"p{int(q * 100)}": float(np.quantile(abs_diff, q)) for q in quantiles}
    rel_q = {f"p{int(q * 100)}": float(np.quantile(rel_abs_diff, q)) for q in quantiles}

    order = np.argsort(-abs_diff)
    top_indices = order[: max(0, top_k)]
    top_errors = [
        {
            "idx": int(i),
            "xgb_pred": float(xgb_pred[i]),
            "onnx_pred": float(onnx_pred[i]),
            "abs_diff": float(abs_diff[i]),
            "rel_abs_diff": float(rel_abs_diff[i]),
        }
        for i in top_indices
    ]

    mae = float(np.mean(abs_diff)) if abs_diff.size else 0.0
    rmse = float(math.sqrt(float(np.mean(np.square(diff))))) if diff.size else 0.0
    return {
        "n_rows": int(xgb_pred.shape[0]),
        "mae": mae,
        "rmse": rmse,
        "max_abs_diff": float(np.max(abs_diff)) if abs_diff.size else 0.0,
        "mean_rel_abs_diff": float(np.mean(rel_abs_diff)) if rel_abs_diff.size else 0.0,
        "abs_diff_quantile": abs_q,
        "rel_abs_diff_quantile": rel_q,
        "pearson_corr": safe_pearson(xgb_pred, onnx_pred),
        "top_errors": top_errors,
    }


def write_compare_csv(
    path: Path,
    xgb_pred: np.ndarray,
    onnx_pred: np.ndarray,
    rel_eps: float,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    abs_diff = np.abs(onnx_pred - xgb_pred)
    rel_abs_diff = abs_diff / np.maximum(np.abs(xgb_pred), rel_eps)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["idx", "xgb", "onnx", "abs_diff", "rel_abs_diff"])
        for idx, (x_val, o_val, a_val, r_val) in enumerate(
            zip(xgb_pred, onnx_pred, abs_diff, rel_abs_diff)
        ):
            writer.writerow(
                [
                    idx,
                    f"{float(x_val):.17g}",
                    f"{float(o_val):.17g}",
                    f"{float(a_val):.17g}",
                    f"{float(r_val):.17g}",
                ]
            )


def main() -> int:
    args = parse_args()
    cfg = load_toml(args.config.expanduser().resolve())

    model_tbl = get_table(cfg, "model")
    input_tbl = get_table(cfg, "input")
    output_tbl = get_table(cfg, "output")
    runtime_tbl = get_table(cfg, "runtime")
    rust_tbl = get_table(cfg, "rust_onnx")

    base_url = get_str(model_tbl, "base_url", required=True)
    model_name_arg = get_str(model_tbl, "model_name", default="")
    symbol = get_str(model_tbl, "symbol", default="BTCUSDT", required=True).upper()
    group_key = get_str(model_tbl, "group_key", default="")
    timeout_sec = get_float(model_tbl, "timeout_sec", default=15.0)
    if timeout_sec <= 0:
        raise RuntimeError("model.timeout_sec must be > 0")

    h5_path = Path(get_str(input_tbl, "h5_path", required=True)).expanduser().resolve()
    h5_key = get_str(input_tbl, "h5_key", default="")
    head = get_int(input_tbl, "head", default=0)
    if head < 0:
        raise RuntimeError("input.head must be >= 0")
    if not h5_path.is_file():
        raise FileNotFoundError(f"input.h5_path not found: {h5_path}")

    work_dir = Path(get_str(output_tbl, "work_dir", required=True)).expanduser().resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    def resolve_output(name: str, default_name: str) -> Path:
        raw = get_str(output_tbl, name, default=default_name)
        p = Path(raw).expanduser()
        return p.resolve() if p.is_absolute() else (work_dir / p).resolve()

    rebuilt_h5 = resolve_output("rebuilt_h5", "rebuilt_model_factors.h5")
    rebuilt_h5_key = get_str(output_tbl, "rebuilt_h5_key", default="data")
    feature_csv = resolve_output("feature_csv", "rebuilt_features.csv")
    xgb_pred_csv = resolve_output("xgb_pred_csv", "xgb_pred.csv")
    onnx_pred_csv = resolve_output("onnx_pred_csv", "onnx_pred.csv")
    compare_csv = resolve_output("compare_csv", "pred_compare.csv")
    metrics_json = resolve_output("metrics_json", "metrics.json")
    rust_config_path = resolve_output("rust_config_path", "onnx_csv_predict.toml")

    xgb_nthread = get_int(runtime_tbl, "xgb_nthread", default=1)
    rel_eps = get_float(runtime_tbl, "rel_eps", default=1e-8)
    top_k = get_int(runtime_tbl, "top_k", default=10)
    if xgb_nthread <= 0:
        raise RuntimeError("runtime.xgb_nthread must be > 0")
    if rel_eps <= 0:
        raise RuntimeError("runtime.rel_eps must be > 0")
    if top_k < 0:
        raise RuntimeError("runtime.top_k must be >= 0")

    model_name, symbol_detail = resolve_model_name(
        base_url=base_url,
        model_name_arg=model_name_arg,
        symbol=symbol,
        timeout_sec=timeout_sec,
        group_key=group_key,
    )
    factors = symbol_detail.get("factors")
    if not isinstance(factors, list) or not factors:
        raise RuntimeError(f"empty factors list: model={model_name} symbol={symbol}")
    factor_order = [str(x) for x in factors]

    model_json = fetch_model_json(
        base_url=base_url,
        model_name=model_name,
        symbol=symbol,
        timeout_sec=timeout_sec,
    )

    pd_mod = require_pandas()
    source_key, rebuilt_df = load_rebuilt_df(
        pd_mod=pd_mod,
        h5_path=h5_path,
        h5_key=h5_key,
        factor_order=factor_order,
        head=head,
    )
    written_key = write_rebuilt_h5(
        pd_mod=pd_mod,
        rebuilt_df=rebuilt_df,
        output_h5=rebuilt_h5,
        output_key=rebuilt_h5_key,
        meta={
            "model_name": model_name,
            "symbol": symbol,
            "source_h5": str(h5_path),
            "source_key": source_key,
            "factor_count": int(rebuilt_df.shape[1]),
            "factor_order_json": json.dumps(list(rebuilt_df.columns), ensure_ascii=False),
        },
    )
    write_feature_csv(rebuilt_df, feature_csv)

    samples = np.asarray(rebuilt_df.to_numpy(dtype=np.float32, copy=True), dtype=np.float32, order="C")
    xgb = require_xgboost()
    with tempfile.TemporaryDirectory(prefix="factor_pool_xgb_") as tmp_dir:
        xgb_path = Path(tmp_dir) / "model.json"
        xgb_path.write_text(model_json, encoding="utf-8")
        booster = xgb.Booster()
        booster.load_model(str(xgb_path))
        booster.set_param({"nthread": int(xgb_nthread)})
        xgb_pred = booster.predict(xgb.DMatrix(samples))
    xgb_pred = normalize_predictions(xgb_pred, expected_rows=samples.shape[0], side="xgb")
    write_xgb_pred_csv(xgb_pred, xgb_pred_csv)

    run_rust_onnx_predict(
        rust_cfg_path=rust_config_path,
        feature_csv=feature_csv,
        onnx_pred_csv=onnx_pred_csv,
        feature_dim=samples.shape[1],
        base_url=base_url,
        model_name=model_name,
        symbol=symbol,
        timeout_sec=timeout_sec,
        rust_tbl=rust_tbl,
    )
    onnx_pred = load_onnx_pred_csv(onnx_pred_csv)
    onnx_pred = normalize_predictions(onnx_pred, expected_rows=samples.shape[0], side="onnx")

    if int(xgb_pred.shape[0]) != int(onnx_pred.shape[0]):
        raise RuntimeError(
            f"prediction length mismatch: xgb={xgb_pred.shape[0]} onnx={onnx_pred.shape[0]}"
        )

    write_compare_csv(compare_csv, xgb_pred=xgb_pred, onnx_pred=onnx_pred, rel_eps=rel_eps)
    metrics = compute_metrics(xgb_pred=xgb_pred, onnx_pred=onnx_pred, rel_eps=rel_eps, top_k=top_k)

    payload = {
        "model_name": model_name,
        "symbol": symbol,
        "source_h5": str(h5_path),
        "source_h5_key": source_key,
        "rebuilt_h5": str(rebuilt_h5),
        "rebuilt_h5_key": written_key,
        "feature_csv": str(feature_csv),
        "xgb_pred_csv": str(xgb_pred_csv),
        "onnx_pred_csv": str(onnx_pred_csv),
        "compare_csv": str(compare_csv),
        "factor_count": int(rebuilt_df.shape[1]),
        "factor_order": factor_order,
        "runtime": {
            "xgb_nthread": xgb_nthread,
            "rel_eps": rel_eps,
            "top_k": top_k,
            "rust_onnx": rust_tbl,
        },
        "metrics": metrics,
    }
    metrics_json.parent.mkdir(parents=True, exist_ok=True)
    metrics_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"config={args.config.expanduser().resolve()}")
    print(f"model_name={model_name}")
    print(f"symbol={symbol}")
    print(f"rows={int(rebuilt_df.shape[0])}")
    print(f"factor_count={int(rebuilt_df.shape[1])}")
    print(f"rebuilt_h5={rebuilt_h5}")
    print(f"feature_csv={feature_csv}")
    print(f"xgb_pred_csv={xgb_pred_csv}")
    print(f"onnx_pred_csv={onnx_pred_csv}")
    print(f"compare_csv={compare_csv}")
    print(f"metrics_json={metrics_json}")
    print(f"mae={metrics['mae']:.6g}")
    print(f"rmse={metrics['rmse']:.6g}")
    print(f"max_abs_diff={metrics['max_abs_diff']:.6g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
