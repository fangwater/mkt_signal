#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
from pathlib import Path

import numpy as np
import tables


DEFAULT_MODEL_DIR = Path("/home/ubuntu/model_data/binance_futures_direction_model")
DEFAULT_PRED_DIR = Path(
    "/home/ubuntu/google-drive/arbmm_lgq/predict_signal/"
    "signal_binanceswap_symbol100_twapchg_predict_202602_202603_202604131150"
)
DEFAULT_OUTPUT_DIR = Path("/tmp/y_pred_scaling_analysis")
QUANTILES = [0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sample prediction files and compare y_pred distributions before and "
            "after scaling by 1e8."
        )
    )
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--pred-dir", type=Path, default=DEFAULT_PRED_DIR)
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260415)
    parser.add_argument("--scale", type=float, default=1e8)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for analysis outputs.",
    )
    return parser.parse_args()


def discover_model_bases(model_dir: Path) -> list[str]:
    return sorted(
        path.name[: -len("_model.pkl")]
        for path in model_dir.glob("*_model.pkl")
        if path.name.endswith("_model.pkl")
    )


def decode_labels(values: np.ndarray) -> list[str]:
    decoded: list[str] = []
    for value in values.tolist():
        if isinstance(value, (bytes, np.bytes_)):
            decoded.append(value.decode("utf-8"))
        else:
            decoded.append(str(value))
    return decoded


def read_hdf_column(h5_path: Path, column_name: str) -> np.ndarray:
    with tables.open_file(h5_path, mode="r") as h5:
        group = h5.get_node("/df")
        nblocks = int(group._v_attrs.nblocks)
        for block_idx in range(nblocks):
            items_path = f"/df/block{block_idx}_items"
            values_path = f"/df/block{block_idx}_values"
            if not h5.__contains__(items_path) or not h5.__contains__(values_path):
                continue
            items = decode_labels(h5.get_node(items_path).read())
            if column_name not in items:
                continue
            values = np.asarray(h5.get_node(values_path).read())
            col_idx = items.index(column_name)
            if values.ndim == 1:
                if col_idx != 0:
                    raise ValueError(
                        f"{h5_path} column {column_name} is stored in unexpected shape"
                    )
                return values.astype(np.float64, copy=False)
            return values[:, col_idx].astype(np.float64, copy=False)
    raise KeyError(f"{column_name} not found in {h5_path}")


def unique_ratio(values: np.ndarray) -> float:
    return float(np.unique(values).size) / float(values.size)


def summarize(values: np.ndarray, prefix: str) -> dict[str, float]:
    quantile_values = np.quantile(values, QUANTILES)
    result = {
        f"{prefix}_count": float(values.size),
        f"{prefix}_mean": float(values.mean()),
        f"{prefix}_std": float(values.std()),
        f"{prefix}_min": float(values.min()),
        f"{prefix}_max": float(values.max()),
        f"{prefix}_unique_ratio": unique_ratio(values),
    }
    for q, qv in zip(QUANTILES, quantile_values):
        result[f"{prefix}_q{str(q).replace('.', '_')}"] = float(qv)
    return result


def analyze_symbol(base_name: str, pred_dir: Path, scale: float) -> dict[str, float | str]:
    pred_path = pred_dir / f"{base_name}_predictions.h5"
    y_pred = read_hdf_column(pred_path, "y_pred")
    scaled = y_pred * scale
    scaled_int = np.rint(scaled).astype(np.int64)

    result: dict[str, float | str] = {
        "base_name": base_name,
        "symbol": base_name.split("_", 1)[0],
        "scale": scale,
        "pred_path": str(pred_path),
        "float_scale_sign_match_ratio": float(np.mean(np.signbit(y_pred) == np.signbit(scaled))),
        "int_scale_sign_match_ratio": float(np.mean(np.sign(y_pred) == np.sign(scaled_int))),
        "float_scale_corr": float(np.corrcoef(y_pred, scaled)[0, 1]),
        "int_scale_corr": float(np.corrcoef(y_pred, scaled_int.astype(np.float64))[0, 1]),
        "scaled_back_max_abs_err": float(np.max(np.abs(y_pred - scaled / scale))),
        "rounded_back_max_abs_err": float(np.max(np.abs(y_pred - scaled_int / scale))),
        "rounded_back_mean_abs_err": float(np.mean(np.abs(y_pred - scaled_int / scale))),
        "rounded_zero_ratio": float(np.mean(scaled_int == 0)),
        "rounded_unique_ratio": unique_ratio(scaled_int),
    }
    result.update(summarize(y_pred, "orig"))
    result.update(summarize(scaled, "scaled_float"))
    result.update(summarize(scaled_int.astype(np.float64), "scaled_int"))
    return result


def write_csv(rows: list[dict[str, float | str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    rows: list[dict[str, float | str]],
    sample_bases: list[str],
    output_path: Path,
    scale: float,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    float_corrs = [float(row["float_scale_corr"]) for row in rows]
    int_corrs = [float(row["int_scale_corr"]) for row in rows]
    rounded_errs = [float(row["rounded_back_max_abs_err"]) for row in rows]
    rounded_zero_ratios = [float(row["rounded_zero_ratio"]) for row in rows]
    orig_q95 = [float(row["orig_q0_95"]) for row in rows]
    orig_q99 = [float(row["orig_q0_99"]) for row in rows]
    scaled_int_q95 = [float(row["scaled_int_q0_95"]) for row in rows]
    scaled_int_q99 = [float(row["scaled_int_q0_99"]) for row in rows]

    lines = [
        f"sample_size={len(sample_bases)}",
        f"sample_bases={','.join(sample_bases)}",
        f"scale={scale}",
        (
            "float_scale_preservation="
            "multiplying by a positive constant preserves order and shape exactly; "
            "the observed correlations are expected to be 1.0."
        ),
        f"float_scale_corr_min={min(float_corrs):.12f}",
        f"float_scale_corr_max={max(float_corrs):.12f}",
        f"int_scale_corr_min={min(int_corrs):.12f}",
        f"int_scale_corr_max={max(int_corrs):.12f}",
        f"rounded_back_max_abs_err_max={max(rounded_errs):.12e}",
        f"rounded_zero_ratio_min={min(rounded_zero_ratios):.6f}",
        f"rounded_zero_ratio_max={max(rounded_zero_ratios):.6f}",
        f"orig_q95_range=[{min(orig_q95):.12e}, {max(orig_q95):.12e}]",
        f"orig_q99_range=[{min(orig_q99):.12e}, {max(orig_q99):.12e}]",
        f"scaled_int_q95_range=[{min(scaled_int_q95):.3f}, {max(scaled_int_q95):.3f}]",
        f"scaled_int_q99_range=[{min(scaled_int_q99):.3f}, {max(scaled_int_q99):.3f}]",
    ]
    output_path.write_text("\n".join(lines) + "\n")


def main() -> int:
    args = parse_args()
    model_dir = args.model_dir.expanduser().resolve()
    pred_dir = args.pred_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    model_bases = discover_model_bases(model_dir)
    if args.sample_size > len(model_bases):
        raise SystemExit(
            f"sample-size {args.sample_size} exceeds available symbols {len(model_bases)}"
        )

    rng = random.Random(args.seed)
    sample_bases = sorted(rng.sample(model_bases, args.sample_size))
    rows = [analyze_symbol(base_name, pred_dir, args.scale) for base_name in sample_bases]

    csv_path = output_dir / "per_symbol_stats.csv"
    summary_path = output_dir / "summary.txt"
    sample_path = output_dir / "sample_symbols.txt"

    write_csv(rows, csv_path)
    write_summary(rows, sample_bases, summary_path, args.scale)
    sample_path.write_text("\n".join(sample_bases) + "\n")

    print(f"sample_bases={sample_bases}")
    print(f"per_symbol_stats={csv_path}")
    print(f"summary={summary_path}")

    for row in rows:
        print(
            f"{row['base_name']}: "
            f"orig_q95={float(row['orig_q0_95']):.12e} "
            f"orig_q99={float(row['orig_q0_99']):.12e} "
            f"scaled_int_q95={float(row['scaled_int_q0_95']):.3f} "
            f"scaled_int_q99={float(row['scaled_int_q0_99']):.3f} "
            f"int_corr={float(row['int_scale_corr']):.12f} "
            f"rounded_zero_ratio={float(row['rounded_zero_ratio']):.6f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
