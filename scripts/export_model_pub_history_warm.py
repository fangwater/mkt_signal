#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import tables


DEFAULT_MODEL_DIR = Path("/home/ubuntu/model_data/binance_futures_direction_model")
DEFAULT_PRED_DIR = Path(
    "/home/ubuntu/google-drive/arbmm_lgq/predict_signal/"
    "signal_binanceswap_symbol100_twapchg_predict_202602_202603_202604131150"
)
DEFAULT_OUTPUT_DIR = Path(
    "/home/ubuntu/model_pub/binance_futures_direction_model/history_ylabel"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build model_pub score_rolling warm-history files from prediction y_pred "
            "using evenly spaced quantiles."
        )
    )
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--pred-dir", type=Path, default=DEFAULT_PRED_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--column", default="y_pred")
    parser.add_argument("--num-quantiles", type=int, default=200)
    parser.add_argument(
        "--include-endpoints",
        action="store_true",
        help="Use quantiles including 0 and 1. Default uses midpoint quantiles.",
    )
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def discover_model_bases(model_dir: Path) -> list[str]:
    return sorted(
        path.name[: -len("_model.pkl")]
        for path in model_dir.glob("*_model.pkl")
        if path.name.endswith("_model.pkl")
    )


def decode_labels(values: np.ndarray) -> list[str]:
    labels: list[str] = []
    for value in values.tolist():
        if isinstance(value, (bytes, np.bytes_)):
            labels.append(value.decode("utf-8"))
        else:
            labels.append(str(value))
    return labels


def read_fixed_hdf_column(h5_path: Path, column_name: str) -> np.ndarray:
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
                        f"{h5_path} column {column_name} stored in unexpected 1D block"
                    )
                return values.astype(np.float64, copy=False)
            if values.ndim != 2:
                raise ValueError(
                    f"{h5_path} column {column_name} stored in unsupported shape {values.shape}"
                )
            return values[:, col_idx].astype(np.float64, copy=False)

    raise KeyError(f"{column_name} not found in {h5_path}")


def build_quantile_grid(num_quantiles: int, include_endpoints: bool) -> np.ndarray:
    if num_quantiles <= 0:
        raise ValueError("num_quantiles must be > 0")
    if include_endpoints:
        if num_quantiles == 1:
            return np.array([0.5], dtype=np.float64)
        return np.linspace(0.0, 1.0, num_quantiles, dtype=np.float64)
    step = 1.0 / num_quantiles
    start = step / 2.0
    end = 1.0 - start
    return np.linspace(start, end, num_quantiles, dtype=np.float64)


def write_warm_file(output_path: Path, values: np.ndarray, overwrite: bool) -> None:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output already exists: {output_path}")
    text = "".join(f"{value:.12g}\n" for value in values)
    output_path.write_text(text)


def main() -> int:
    args = parse_args()
    model_dir = args.model_dir.expanduser().resolve()
    pred_dir = args.pred_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    model_bases = discover_model_bases(model_dir)
    if not model_bases:
        raise SystemExit(f"No *_model.pkl files found in {model_dir}")

    quantiles = build_quantile_grid(args.num_quantiles, args.include_endpoints)
    written = 0
    missing_predictions: list[str] = []

    summary_lines = [
        f"model_dir={model_dir}",
        f"pred_dir={pred_dir}",
        f"output_dir={output_dir}",
        f"column={args.column}",
        f"num_quantiles={args.num_quantiles}",
        f"include_endpoints={args.include_endpoints}",
    ]

    for base_name in model_bases:
        pred_path = pred_dir / f"{base_name}_predictions.h5"
        if not pred_path.exists():
            missing_predictions.append(base_name)
            continue

        scores = read_fixed_hdf_column(pred_path, args.column)
        warm_values = np.quantile(scores, quantiles)
        symbol = base_name.split("_", 1)[0].lower()
        output_path = output_dir / f"{symbol}-ylabel.txt"
        write_warm_file(output_path, warm_values, args.overwrite)

        written += 1
        summary_lines.append(
            f"{base_name} rows={scores.size} warm_samples={warm_values.size} file={output_path.name}"
        )
        print(
            f"written {base_name} rows={scores.size} warm_samples={warm_values.size} -> {output_path}"
        )

    summary_lines.append(f"written={written}")
    summary_lines.append(f"missing_predictions={len(missing_predictions)}")
    if missing_predictions:
        summary_lines.append("missing_prediction_bases=" + ",".join(missing_predictions))
    (output_dir / "_summary.txt").write_text("\n".join(summary_lines) + "\n")

    print(
        f"summary written={written} missing_predictions={len(missing_predictions)} output_dir={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
