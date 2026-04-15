#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import tables


DEFAULT_MODEL_DIR = Path("/home/ubuntu/model_data/binance_futures_direction_model")
DEFAULT_PRED_DIR = Path(
    "/home/ubuntu/google-drive/arbmm_lgq/predict_signal/"
    "signal_binanceswap_symbol100_twapchg_predict_202602_202603_202604131150"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan model_data symbols, read the matching prediction HDF5 files, "
            "and export only the ylabel column."
        )
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Directory containing <base>_model.pkl files.",
    )
    parser.add_argument(
        "--pred-dir",
        type=Path,
        default=DEFAULT_PRED_DIR,
        help="Directory containing <base>_predictions.h5 files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write the exported ylabel files. Defaults to --model-dir.",
    )
    parser.add_argument(
        "--source-column",
        default="y_true",
        help="Prediction HDF5 column to export. Defaults to y_true.",
    )
    parser.add_argument(
        "--output-column",
        default="ylabel",
        help="Column name to use in the exported file. Defaults to ylabel.",
    )
    parser.add_argument(
        "--format",
        choices=("h5", "txt"),
        default="h5",
        help="Export format. H5 keeps the values as a named column.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional symbol filter, for example BTCUSDT ETHUSDT.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing export files.",
    )
    return parser.parse_args()


def discover_model_bases(model_dir: Path) -> list[str]:
    return sorted(
        path.name[: -len("_model.pkl")]
        for path in model_dir.glob("*_model.pkl")
        if path.name.endswith("_model.pkl")
    )


def normalize_symbol_filters(symbols: Iterable[str] | None) -> set[str] | None:
    if not symbols:
        return None
    return {symbol.strip().upper() for symbol in symbols if symbol.strip()}


def decode_labels(values: np.ndarray) -> list[str]:
    labels: list[str] = []
    for value in values.tolist():
        if isinstance(value, (bytes, np.bytes_)):
            labels.append(value.decode("utf-8"))
        else:
            labels.append(str(value))
    return labels


def extract_fixed_hdf_column(h5_path: Path, source_column: str) -> np.ndarray:
    with tables.open_file(h5_path, mode="r") as h5:
        group = h5.get_node("/df")
        nblocks = int(group._v_attrs.nblocks)
        for block_idx in range(nblocks):
            items_path = f"/df/block{block_idx}_items"
            values_path = f"/df/block{block_idx}_values"
            if not h5.__contains__(items_path) or not h5.__contains__(values_path):
                continue

            block_items = decode_labels(h5.get_node(items_path).read())
            if source_column not in block_items:
                continue

            values = np.asarray(h5.get_node(values_path).read())
            col_idx = block_items.index(source_column)
            if values.ndim == 1:
                if col_idx != 0:
                    raise ValueError(
                        f"{h5_path} stores {source_column} in an unexpected 1D block."
                    )
                return values
            if values.ndim != 2:
                raise ValueError(
                    f"{h5_path} stores {source_column} in unsupported shape {values.shape}."
                )
            return values[:, col_idx]

    raise KeyError(f"{source_column} not found in {h5_path}")


def extract_column(h5_path: Path, source_column: str) -> np.ndarray:
    try:
        return extract_fixed_hdf_column(h5_path, source_column)
    except Exception:
        df = pd.read_hdf(h5_path, key="df")
        if source_column not in df.columns:
            raise KeyError(f"{source_column} not found in {h5_path}")
        return df[source_column].to_numpy()


def write_output(
    values: np.ndarray,
    output_path: Path,
    output_format: str,
    output_column: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "h5":
        pd.DataFrame({output_column: values}).to_hdf(output_path, key="df", mode="w")
        return

    pd.Series(values, name=output_column).to_csv(
        output_path,
        index=False,
        header=False,
    )


def build_output_path(
    output_dir: Path,
    base_name: str,
    output_column: str,
    output_format: str,
) -> Path:
    return output_dir / f"{base_name}_{output_column}.{output_format}"


def main() -> int:
    args = parse_args()
    model_dir = args.model_dir.expanduser().resolve()
    pred_dir = args.pred_dir.expanduser().resolve()
    output_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir is not None
        else model_dir
    )
    symbol_filters = normalize_symbol_filters(args.symbols)

    model_bases = discover_model_bases(model_dir)
    if not model_bases:
        raise SystemExit(f"No *_model.pkl files found in {model_dir}")

    exported = 0
    skipped_existing = 0
    missing_predictions: list[str] = []
    failed_exports: list[str] = []

    print(f"model_dir={model_dir}")
    print(f"pred_dir={pred_dir}")
    print(f"discovered_model_symbols={len(model_bases)}")

    for base_name in model_bases:
        symbol = base_name.split("_", 1)[0].upper()
        if symbol_filters is not None and symbol not in symbol_filters:
            continue

        pred_path = pred_dir / f"{base_name}_predictions.h5"
        if not pred_path.exists():
            missing_predictions.append(base_name)
            continue

        output_path = build_output_path(
            output_dir=output_dir,
            base_name=base_name,
            output_column=args.output_column,
            output_format=args.format,
        )
        if output_path.exists() and not args.overwrite:
            skipped_existing += 1
            print(f"skip_exists {output_path}")
            continue

        try:
            values = extract_column(pred_path, args.source_column)
            write_output(
                values=values,
                output_path=output_path,
                output_format=args.format,
                output_column=args.output_column,
            )
            exported += 1
            print(f"exported {base_name} rows={len(values)} -> {output_path}")
        except Exception as exc:
            failed_exports.append(base_name)
            print(f"failed {base_name}: {exc}")

    print(
        "summary "
        f"exported={exported} "
        f"skipped_existing={skipped_existing} "
        f"missing_predictions={len(missing_predictions)} "
        f"failed={len(failed_exports)}"
    )
    if missing_predictions:
        print("missing_prediction_bases=" + ",".join(missing_predictions))
    if failed_exports:
        print("failed_bases=" + ",".join(failed_exports))

    return 0 if not failed_exports else 1


if __name__ == "__main__":
    raise SystemExit(main())
