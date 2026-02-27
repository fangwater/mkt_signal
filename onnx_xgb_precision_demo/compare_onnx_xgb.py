#!/usr/bin/env python3
"""Compare inference consistency between ONNXRuntime and native XGBoost."""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    import onnxruntime as ort
    import xgboost as xgb


DEFAULT_THRESHOLDS = "1e-8,1e-7,1e-6,1e-5,1e-4,1e-3"


@dataclass
class ModelArtifacts:
    onnx_path: Path
    xgb_json_path: Path
    feature_dim_hint: int | None
    source_desc: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare prediction deltas between ONNXRuntime and native XGBoost "
            "with large random samples."
        )
    )
    source = parser.add_argument_group("model source")
    source.add_argument("--onnx-path", type=Path, help="local onnx file path")
    source.add_argument("--xgb-path", type=Path, help="local xgboost json file path")
    source.add_argument(
        "--base-url",
        default="http://127.0.0.1:6300",
        help="model_manager base url for remote fetch mode",
    )
    source.add_argument("--model-name", help="model name in model_manager")
    source.add_argument("--symbol", help="symbol in model_manager")
    source.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/tmp/onnx_xgb_precision_demo"),
        help="where fetched artifacts are stored",
    )
    source.add_argument(
        "--timeout-sec", type=float, default=15.0, help="HTTP timeout seconds"
    )

    runtime = parser.add_argument_group("runtime")
    runtime.add_argument(
        "--feature-dim",
        type=int,
        help="override feature dimension if model metadata is unavailable",
    )
    runtime.add_argument(
        "--allow-dim-mismatch",
        action="store_true",
        help="continue even if xgb/onnx inferred feature dims differ",
    )
    runtime.add_argument(
        "--ort-intra-threads",
        type=int,
        default=1,
        help="onnxruntime intra-op threads",
    )
    runtime.add_argument(
        "--ort-inter-threads",
        type=int,
        default=1,
        help="onnxruntime inter-op threads",
    )
    runtime.add_argument(
        "--xgb-nthread",
        type=int,
        default=1,
        help="xgboost inference threads",
    )

    sample = parser.add_argument_group("samples")
    sample.add_argument(
        "--n-samples",
        type=int,
        default=100_000,
        help="number of samples for random input generation",
    )
    sample.add_argument("--seed", type=int, default=42, help="RNG seed")
    sample.add_argument(
        "--dist",
        choices=["normal", "uniform", "laplace"],
        default="normal",
        help="random distribution",
    )
    sample.add_argument(
        "--value-scale",
        type=float,
        default=1.0,
        help="multiply generated random values by this scale",
    )
    sample.add_argument(
        "--sample-npy",
        type=Path,
        help="optional npy file with shape [N, F], overrides random generation",
    )

    output = parser.add_argument_group("output")
    output.add_argument(
        "--thresholds",
        default=DEFAULT_THRESHOLDS,
        help="comma separated absolute-error thresholds",
    )
    output.add_argument(
        "--rel-eps",
        type=float,
        default=1e-8,
        help="epsilon for relative error denominator",
    )
    output.add_argument("--top-k", type=int, default=5, help="show largest deltas")
    output.add_argument("--output-json", type=Path, help="save metrics as json")

    args = parser.parse_args()
    validate_args(args, parser)
    return args


def require_onnxruntime():
    try:
        import onnxruntime as ort
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "missing dependency onnxruntime, please run: pip install onnxruntime"
        ) from exc
    return ort


def require_xgboost():
    try:
        import xgboost as xgb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "missing dependency xgboost, please run: pip install xgboost"
        ) from exc
    return xgb


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    local_mode = args.onnx_path is not None or args.xgb_path is not None
    remote_mode = args.model_name is not None or args.symbol is not None

    if args.n_samples <= 0:
        parser.error("--n-samples must be > 0")

    if local_mode:
        if args.onnx_path is None or args.xgb_path is None:
            parser.error("local mode requires both --onnx-path and --xgb-path")
        return

    if remote_mode:
        if args.model_name is None or args.symbol is None:
            parser.error("remote mode requires both --model-name and --symbol")
        return

    parser.error(
        "must provide either local paths (--onnx-path + --xgb-path) "
        "or remote info (--model-name + --symbol)"
    )


def sanitize_file_token(text: str) -> str:
    allowed = []
    for ch in text:
        if ch.isalnum() or ch in "-_.":
            allowed.append(ch)
        else:
            allowed.append("_")
    return "".join(allowed)


def load_artifacts(args: argparse.Namespace) -> ModelArtifacts:
    if args.onnx_path is not None and args.xgb_path is not None:
        onnx_path = args.onnx_path.expanduser().resolve()
        xgb_path = args.xgb_path.expanduser().resolve()
        if not onnx_path.is_file():
            raise FileNotFoundError(f"onnx file not found: {onnx_path}")
        if not xgb_path.is_file():
            raise FileNotFoundError(f"xgb json file not found: {xgb_path}")
        return ModelArtifacts(
            onnx_path=onnx_path,
            xgb_json_path=xgb_path,
            feature_dim_hint=args.feature_dim,
            source_desc="local_files",
        )

    assert args.model_name is not None
    assert args.symbol is not None
    base_url = args.base_url.rstrip("/")
    model_name_q = urllib.parse.quote(args.model_name, safe="")
    symbol_q = urllib.parse.quote(args.symbol, safe="")

    model_url = f"{base_url}/api/models/{model_name_q}/model/{symbol_q}"
    onnx_url = f"{base_url}/api/models/{model_name_q}/model_onnx/{symbol_q}"

    model_payload = http_get_json(model_url, timeout_sec=args.timeout_sec)
    payload = model_payload.get("payload") or {}
    model_json_text = payload.get("model_json")
    if not isinstance(model_json_text, str) or not model_json_text.strip():
        raise RuntimeError(f"model payload missing non-empty payload.model_json: {model_url}")

    metadata = payload.get("metadata") or {}
    feature_dim_hint = args.feature_dim
    if feature_dim_hint is None:
        feature_dim_hint = as_optional_int(metadata.get("feature_dim"))

    onnx_bytes, onnx_headers = http_get_binary(onnx_url, timeout_sec=args.timeout_sec)
    if not onnx_bytes:
        raise RuntimeError(f"downloaded empty onnx bytes from: {onnx_url}")

    if feature_dim_hint is None:
        feature_dim_hint = as_optional_int(onnx_headers.get("x-model-feature-dim"))

    cache_dir = args.cache_dir.expanduser().resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{sanitize_file_token(args.model_name)}.{sanitize_file_token(args.symbol)}"
    xgb_json_path = cache_dir / f"{stem}.model.json"
    onnx_path = cache_dir / f"{stem}.model.onnx"
    xgb_json_path.write_text(model_json_text, encoding="utf-8")
    onnx_path.write_bytes(onnx_bytes)

    return ModelArtifacts(
        onnx_path=onnx_path,
        xgb_json_path=xgb_json_path,
        feature_dim_hint=feature_dim_hint,
        source_desc=f"model_manager:{base_url}",
    )


def http_get_json(url: str, timeout_sec: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "Accept-Encoding": "identity"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} on {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"failed to connect {url}: {exc}") from exc
    try:
        obj = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid json response from {url}: {exc}") from exc
    if not isinstance(obj, dict):
        raise RuntimeError(f"unexpected json type from {url}: {type(obj).__name__}")
    return obj


def http_get_binary(url: str, timeout_sec: float) -> tuple[bytes, dict[str, str]]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/octet-stream", "Accept-Encoding": "identity"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            payload = response.read()
            headers = {k.lower(): v for k, v in response.headers.items()}
            return payload, headers
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} on {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"failed to connect {url}: {exc}") from exc


def as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            return None
    return None


def build_ort_session(
    onnx_path: Path, intra_threads: int, inter_threads: int
) -> "ort.InferenceSession":
    ort = require_onnxruntime()
    opts = ort.SessionOptions()
    opts.intra_op_num_threads = intra_threads
    opts.inter_op_num_threads = inter_threads
    return ort.InferenceSession(
        str(onnx_path), sess_options=opts, providers=["CPUExecutionProvider"]
    )


def infer_onnx_feature_dim(session: "ort.InferenceSession") -> int | None:
    inputs = session.get_inputs()
    if not inputs:
        return None
    shape = inputs[0].shape
    if not shape:
        return None
    for dim in reversed(shape):
        if isinstance(dim, int) and dim > 0:
            return dim
    return None


def infer_onnx_static_batch(session: "ort.InferenceSession") -> int | None:
    inputs = session.get_inputs()
    if not inputs:
        return None
    shape = inputs[0].shape
    if not shape:
        return None
    batch = shape[0]
    if isinstance(batch, int) and batch > 0:
        return batch
    return None


def resolve_feature_dim(
    preferred: int | None,
    xgb_dim: int | None,
    onnx_dim: int | None,
    allow_mismatch: bool,
) -> int:
    candidates: dict[str, int] = {}
    if preferred is not None and preferred > 0:
        candidates["preferred"] = preferred
    if xgb_dim is not None and xgb_dim > 0:
        candidates["xgb"] = xgb_dim
    if onnx_dim is not None and onnx_dim > 0:
        candidates["onnx"] = onnx_dim

    if not candidates:
        raise RuntimeError("cannot resolve feature dim from args/xgb/onnx")

    values = set(candidates.values())
    feature_dim = next(iter(values))
    if len(values) > 1 and not allow_mismatch:
        joined = ", ".join(f"{k}={v}" for k, v in candidates.items())
        raise RuntimeError(
            "feature dim mismatch detected "
            f"({joined}). pass --allow-dim-mismatch only if intentional."
        )
    if "preferred" in candidates:
        feature_dim = candidates["preferred"]
    elif "xgb" in candidates:
        feature_dim = candidates["xgb"]
    elif "onnx" in candidates:
        feature_dim = candidates["onnx"]
    return feature_dim


def load_samples(args: argparse.Namespace, n_samples: int, feature_dim: int) -> np.ndarray:
    if args.sample_npy is not None:
        sample_path = args.sample_npy.expanduser().resolve()
        if not sample_path.is_file():
            raise FileNotFoundError(f"sample npy file not found: {sample_path}")
        data = np.load(sample_path)
        if data.ndim != 2:
            raise RuntimeError(f"sample npy must be 2D, got shape={data.shape}")
        if data.shape[1] != feature_dim:
            raise RuntimeError(
                f"sample npy feature dim mismatch: data.shape[1]={data.shape[1]} "
                f"feature_dim={feature_dim}"
            )
        if data.shape[0] < n_samples:
            raise RuntimeError(
                f"sample npy has insufficient rows: rows={data.shape[0]} requested={n_samples}"
            )
        return np.asarray(data[:n_samples], dtype=np.float32, order="C")

    rng = np.random.default_rng(args.seed)
    if args.dist == "normal":
        values = rng.normal(0.0, 1.0, size=(n_samples, feature_dim))
    elif args.dist == "uniform":
        values = rng.uniform(-1.0, 1.0, size=(n_samples, feature_dim))
    else:
        values = rng.laplace(0.0, 1.0, size=(n_samples, feature_dim))
    values = values * args.value_scale
    return np.asarray(values, dtype=np.float32, order="C")


def normalize_predictions(raw: np.ndarray, expected_rows: int, side: str) -> np.ndarray:
    arr = np.asarray(raw)
    if arr.ndim == 1:
        out = arr
    elif arr.ndim == 2 and arr.shape[1] == 1:
        out = arr[:, 0]
    else:
        raise RuntimeError(f"{side} output shape not supported for single-target regression: {arr.shape}")
    if out.shape[0] != expected_rows:
        raise RuntimeError(
            f"{side} prediction row mismatch: got={out.shape[0]} expected={expected_rows}"
        )
    return np.asarray(out, dtype=np.float64)


def run_onnx_predict(
    session: "ort.InferenceSession",
    input_name: str,
    output_name: str,
    samples: np.ndarray,
    static_batch: int | None,
) -> tuple[np.ndarray, float]:
    n_rows = int(samples.shape[0])

    # Dynamic batch model: run one-shot for max throughput.
    if static_batch is None:
        t0 = time.perf_counter_ns()
        raw = session.run([output_name], {input_name: samples})[0]
        predict_us = (time.perf_counter_ns() - t0) / 1_000.0
        pred = normalize_predictions(raw, expected_rows=n_rows, side="onnx")
        return pred, predict_us

    # Static batch model: run in fixed-size chunks. Tail is padded then cropped.
    if static_batch <= 0:
        raise RuntimeError(f"invalid static batch from onnx input shape: {static_batch}")

    collected: list[np.ndarray] = []
    t0 = time.perf_counter_ns()
    for start in range(0, n_rows, static_batch):
        end = min(start + static_batch, n_rows)
        chunk = samples[start:end]
        real_rows = int(chunk.shape[0])
        if real_rows < static_batch:
            pad = np.zeros((static_batch, samples.shape[1]), dtype=samples.dtype)
            pad[:real_rows, :] = chunk
            chunk = pad

        raw = session.run([output_name], {input_name: chunk})[0]
        pred = normalize_predictions(raw, expected_rows=static_batch, side="onnx")
        collected.append(pred[:real_rows])

    predict_us = (time.perf_counter_ns() - t0) / 1_000.0
    onnx_pred = np.concatenate(collected, axis=0) if collected else np.empty((0,), dtype=np.float64)
    if onnx_pred.shape[0] != n_rows:
        raise RuntimeError(
            f"onnx prediction row mismatch after chunking: got={onnx_pred.shape[0]} expected={n_rows}"
        )
    return onnx_pred, predict_us


def run_comparison(args: argparse.Namespace, artifacts: ModelArtifacts) -> dict[str, Any]:
    xgb = require_xgboost()
    booster = xgb.Booster()
    booster.load_model(str(artifacts.xgb_json_path))
    booster.set_param({"nthread": args.xgb_nthread})
    xgb_dim = as_optional_int(booster.num_features())

    session = build_ort_session(
        artifacts.onnx_path,
        intra_threads=args.ort_intra_threads,
        inter_threads=args.ort_inter_threads,
    )
    ort_inputs = session.get_inputs()
    ort_outputs = session.get_outputs()
    if len(ort_inputs) != 1:
        raise RuntimeError(f"expected exactly one ONNX input, got {len(ort_inputs)}")
    if len(ort_outputs) != 1:
        raise RuntimeError(f"expected exactly one ONNX output, got {len(ort_outputs)}")

    onnx_dim = infer_onnx_feature_dim(session)
    onnx_static_batch = infer_onnx_static_batch(session)
    feature_dim = resolve_feature_dim(
        preferred=artifacts.feature_dim_hint,
        xgb_dim=xgb_dim,
        onnx_dim=onnx_dim,
        allow_mismatch=args.allow_dim_mismatch,
    )

    samples = load_samples(args, n_samples=args.n_samples, feature_dim=feature_dim)
    n_rows = int(samples.shape[0])

    t0 = time.perf_counter_ns()
    xgb_pred = booster.predict(xgb.DMatrix(samples))
    xgb_predict_us = (time.perf_counter_ns() - t0) / 1_000.0
    xgb_pred = normalize_predictions(xgb_pred, expected_rows=n_rows, side="xgb")

    input_name = ort_inputs[0].name
    output_name = ort_outputs[0].name
    onnx_pred, onnx_predict_us = run_onnx_predict(
        session=session,
        input_name=input_name,
        output_name=output_name,
        samples=samples,
        static_batch=onnx_static_batch,
    )

    return compute_metrics(
        xgb_pred=xgb_pred,
        onnx_pred=onnx_pred,
        thresholds=parse_thresholds(args.thresholds),
        rel_eps=args.rel_eps,
        top_k=args.top_k,
        meta={
            "source": artifacts.source_desc,
            "xgb_json_path": str(artifacts.xgb_json_path),
            "onnx_path": str(artifacts.onnx_path),
            "feature_dim": feature_dim,
            "xgb_feature_dim": xgb_dim,
            "onnx_feature_dim": onnx_dim,
            "onnx_input_shape": ort_inputs[0].shape,
            "onnx_static_batch": onnx_static_batch,
            "n_samples": n_rows,
            "dist": args.dist if args.sample_npy is None else "sample_npy",
            "seed": args.seed if args.sample_npy is None else None,
            "value_scale": args.value_scale if args.sample_npy is None else None,
            "sample_npy": str(args.sample_npy.expanduser().resolve()) if args.sample_npy else None,
            "xgb_predict_us": xgb_predict_us,
            "onnx_predict_us": onnx_predict_us,
            "xgb_predict_qps": n_rows / (xgb_predict_us / 1_000_000.0),
            "onnx_predict_qps": n_rows / (onnx_predict_us / 1_000_000.0),
        },
    )


def parse_thresholds(raw: str) -> list[float]:
    thresholds: list[float] = []
    for token in raw.split(","):
        s = token.strip()
        if not s:
            continue
        value = float(s)
        if value <= 0:
            raise RuntimeError(f"threshold must be > 0, got {value}")
        thresholds.append(value)
    if not thresholds:
        raise RuntimeError("no valid thresholds parsed")
    return sorted(set(thresholds))


def safe_pearson(x: np.ndarray, y: np.ndarray) -> float:
    x_std = float(np.std(x))
    y_std = float(np.std(y))
    if x_std == 0.0 or y_std == 0.0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def safe_r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_mean = float(np.mean(y_true))
    ss_tot = float(np.sum((y_true - y_mean) ** 2))
    if ss_tot == 0.0:
        return float("nan")
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    return 1.0 - (ss_res / ss_tot)


def compute_metrics(
    xgb_pred: np.ndarray,
    onnx_pred: np.ndarray,
    thresholds: list[float],
    rel_eps: float,
    top_k: int,
    meta: dict[str, Any],
) -> dict[str, Any]:
    diff = onnx_pred - xgb_pred
    abs_diff = np.abs(diff)
    rel_abs_diff = abs_diff / np.maximum(np.abs(xgb_pred), rel_eps)

    quantiles = {
        "p50": float(np.quantile(abs_diff, 0.50)),
        "p90": float(np.quantile(abs_diff, 0.90)),
        "p95": float(np.quantile(abs_diff, 0.95)),
        "p99": float(np.quantile(abs_diff, 0.99)),
        "p999": float(np.quantile(abs_diff, 0.999)),
    }

    threshold_result = []
    total = int(abs_diff.shape[0])
    for thr in thresholds:
        count = int(np.sum(abs_diff <= thr))
        threshold_result.append(
            {
                "threshold": thr,
                "count_leq": count,
                "ratio_leq": count / total,
            }
        )

    k = max(0, min(top_k, total))
    top_errors: list[dict[str, Any]] = []
    if k > 0:
        idx = np.argpartition(abs_diff, -k)[-k:]
        idx = idx[np.argsort(abs_diff[idx])[::-1]]
        for i in idx:
            top_errors.append(
                {
                    "index": int(i),
                    "xgb_pred": float(xgb_pred[i]),
                    "onnx_pred": float(onnx_pred[i]),
                    "diff": float(diff[i]),
                    "abs_diff": float(abs_diff[i]),
                    "rel_abs_diff": float(rel_abs_diff[i]),
                }
            )

    return {
        "meta": meta,
        "metrics": {
            "mae": float(np.mean(abs_diff)),
            "rmse": float(np.sqrt(np.mean(diff**2))),
            "max_abs_diff": float(np.max(abs_diff)),
            "mean_abs_rel_diff": float(np.mean(rel_abs_diff)),
            "max_abs_rel_diff": float(np.max(rel_abs_diff)),
            "pearson_corr": safe_pearson(xgb_pred, onnx_pred),
            "r2": safe_r2(xgb_pred, onnx_pred),
            "abs_diff_quantiles": quantiles,
            "thresholds": threshold_result,
        },
        "top_errors": top_errors,
    }


def print_report(result: dict[str, Any]) -> None:
    meta = result["meta"]
    metrics = result["metrics"]
    print("=== ONNX vs XGBoost Precision Report ===")
    print(f"source:            {meta['source']}")
    print(f"xgb_json_path:     {meta['xgb_json_path']}")
    print(f"onnx_path:         {meta['onnx_path']}")
    print(
        f"feature_dim:       {meta['feature_dim']} "
        f"(xgb={meta['xgb_feature_dim']}, onnx={meta['onnx_feature_dim']})"
    )
    print(f"onnx_input_shape:  {meta['onnx_input_shape']}")
    print(f"onnx_static_batch: {meta['onnx_static_batch']}")
    print(f"n_samples:         {meta['n_samples']}")
    print(f"sample_dist:       {meta['dist']}")
    if meta["seed"] is not None:
        print(f"seed/value_scale:  {meta['seed']} / {meta['value_scale']}")
    else:
        print(f"sample_npy:        {meta['sample_npy']}")

    print(
        f"xgb_predict_us:    {meta['xgb_predict_us']:.2f} "
        f"(qps={meta['xgb_predict_qps']:.2f})"
    )
    print(
        f"onnx_predict_us:   {meta['onnx_predict_us']:.2f} "
        f"(qps={meta['onnx_predict_qps']:.2f})"
    )

    print(f"mae:               {metrics['mae']:.12g}")
    print(f"rmse:              {metrics['rmse']:.12g}")
    print(f"max_abs_diff:      {metrics['max_abs_diff']:.12g}")
    print(f"mean_abs_rel_diff: {metrics['mean_abs_rel_diff']:.12g}")
    print(f"max_abs_rel_diff:  {metrics['max_abs_rel_diff']:.12g}")
    print(f"pearson_corr:      {metrics['pearson_corr']:.12g}")
    print(f"r2:                {metrics['r2']:.12g}")

    q = metrics["abs_diff_quantiles"]
    print(
        "abs_diff_quantile: "
        f"p50={q['p50']:.12g}, p90={q['p90']:.12g}, "
        f"p95={q['p95']:.12g}, p99={q['p99']:.12g}, p999={q['p999']:.12g}"
    )

    print("threshold_pass:")
    for row in metrics["thresholds"]:
        print(
            f"  <= {row['threshold']:.1e}: "
            f"{row['count_leq']} / {meta['n_samples']} ({row['ratio_leq']:.4%})"
        )

    print("top_errors:")
    for row in result["top_errors"]:
        print(
            f"  idx={row['index']} abs_diff={row['abs_diff']:.12g} "
            f"rel={row['rel_abs_diff']:.12g} "
            f"xgb={row['xgb_pred']:.12g} onnx={row['onnx_pred']:.12g}"
        )


def main() -> None:
    args = parse_args()
    artifacts = load_artifacts(args)
    result = run_comparison(args, artifacts)
    print_report(result)
    if args.output_json is not None:
        output_path = args.output_json.expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"saved_json:        {output_path}")


if __name__ == "__main__":
    main()
