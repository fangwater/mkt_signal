#!/usr/bin/env python3
"""Compare preserved CN Rust formulas with the authoritative Python source."""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path
import subprocess
import sys
import warnings

import numpy as np
import pandas as pd

from generate_cn_factor_review import (
    FORMULA_REPAIRS,
    SOURCE_PATH,
    SUBSTITUTIONS,
    analyze,
)


ROOT = Path(__file__).resolve().parents[1]
CHECKPOINTS = (199, 399, 799)
RELATIVE_TOLERANCE = 1e-8


def deterministic_frame(rows: int = 800) -> pd.DataFrame:
    index = np.arange(rows)
    phase = index.astype(float)
    close = 100.0 + phase * 0.002 + np.sin(phase * 0.17) * 0.3
    open_ = close - np.cos(phase * 0.11) * 0.08
    high = np.maximum(open_, close) + 0.2 + (index % 7) * 0.01
    low = np.minimum(open_, close) - 0.2 - (index % 5) * 0.01
    volume = 20.0 + index % 13
    buy_volume = volume * (0.42 + (index % 5) * 0.025)
    sell_volume = volume - buy_volume
    amount = close * volume * 10.0
    buy_amount = close * buy_volume * 10.0 * 1.001
    sell_amount = amount - buy_amount
    count = 5.0 + index % 9
    buy_count = 2.0 + index % 4
    sell_count = count - buy_count

    data = {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
        "avg_amount": amount / count,
        "count": count,
        "trade_time": phase * 1_000.0,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buy_amount": buy_amount,
        "sell_amount": sell_amount,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "large_order": amount * 0.45,
        "medium_order": amount * 0.35,
        "small_order": amount * 0.20,
        "large_buy": buy_amount * 0.45,
        "large_sell": sell_amount * 0.45,
        "medium_buy": buy_amount * 0.35,
        "medium_sell": sell_amount * 0.35,
        "small_buy": buy_amount * 0.20,
        "small_sell": sell_amount * 0.20,
        "vwap": amount / volume / 10.0,
        "buy_vwap": buy_amount / buy_volume / 10.0,
        "sell_vwap": sell_amount / sell_volume / 10.0,
        "net_buy_amount": buy_amount - sell_amount,
        "net_buy_volume": buy_volume - sell_volume,
        "net_buy_pct": (buy_volume - sell_volume) / volume,
        "net_buy_large": buy_amount * 0.45 - sell_amount * 0.45,
        "net_buy_medium": buy_amount * 0.35 - sell_amount * 0.35,
        "net_buy_small": buy_amount * 0.20 - sell_amount * 0.20,
    }
    for level in range(20):
        data[f"bid{level}p"] = close - 0.5 - level * (
            0.08 + (index % 3) * 0.005
        )
        data[f"bid{level}v"] = (
            10.0 + level * 3.0 + (index % (level + 3)) * 0.7
        )
        data[f"ask{level}p"] = close + 0.5 + level * (
            0.09 + (index % 4) * 0.004
        )
        data[f"ask{level}v"] = (
            8.0 + level * 2.0 + (index % (level + 4)) * 0.6
        )
    return pd.DataFrame(data)


def load_python_source():
    spec = importlib.util.spec_from_file_location("cn_factor_source", SOURCE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SOURCE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def rust_values() -> dict[tuple[int, str], float]:
    result = subprocess.run(
        [
            "cargo",
            "test",
            "--quiet",
            "--lib",
            "factor_pub::cn_features::app::tests::dump_deterministic_factor_values",
            "--",
            "--ignored",
            "--nocapture",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    values = {}
    for line in (result.stdout + "\n" + result.stderr).splitlines():
        if not line.startswith("CN_FACTOR_AUDIT\t"):
            continue
        _, checkpoint, name, value = line.split("\t")
        values[(int(checkpoint), name)] = float(value)
    if len(values) != 632 * len(CHECKPOINTS):
        raise RuntimeError(f"expected 1896 Rust audit values, found {len(values)}")
    return values


def main() -> int:
    names, dependencies = analyze(SOURCE_PATH.read_text())
    candidates = [
        name
        for name in names
        if dependencies[name].depth <= 5
        and name not in SUBSTITUTIONS
        and name not in FORMULA_REPAIRS
    ]
    if len(candidates) != 499:
        raise RuntimeError(f"expected 499 preserved formulas, found {len(candidates)}")

    frame = deterministic_frame()
    source = load_python_source()
    rust = rust_values()
    mismatches = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for name in candidates:
            output = np.asarray(getattr(source, name)(frame))
            if output.shape != (len(frame),):
                raise RuntimeError(f"{name} returned shape {output.shape}")
            for checkpoint in CHECKPOINTS:
                python_value = float(output[checkpoint])
                rust_value = rust[(checkpoint, name)]
                if math.isnan(python_value) and math.isnan(rust_value):
                    continue
                tolerance = RELATIVE_TOLERANCE * max(
                    1.0, abs(python_value), abs(rust_value)
                )
                if (
                    not math.isfinite(python_value)
                    or not math.isfinite(rust_value)
                    or abs(python_value - rust_value) > tolerance
                ):
                    mismatches.append(
                        (name, checkpoint, python_value, rust_value, tolerance)
                    )

    if mismatches:
        for row in mismatches:
            print("mismatch", *row, sep="\t", file=sys.stderr)
        return 1
    print(
        f"parity passed: {len(candidates)} preserved factors x "
        f"{len(CHECKPOINTS)} checkpoints"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
