#!/usr/bin/env python3

"""
Single entrypoint for Rust-vs-Python factor alignment.

Modes:
    auto   - use full reference comparison when pandas is available, otherwise
             fall back to the numpy subset comparator for the 38 newly added factors.
    baseline - compare all currently implemented baseline factors against the
             Python reference; uses a local pandas shim when pandas is unavailable.
    opv   - compare the selected OPV factors against the Python reference; uses
            a local pandas shim when pandas is unavailable.
    full   - require pandas and compare all fusion factors that have a callable
             implementation in final_factor_pool_update20260123.py.
    subset - compare only the 38 newly added fusion factors using numpy only.

Usage:
    python3 scripts/factor_test_compare.py /tmp/factor_test_output.json
    python3 scripts/factor_test_compare.py /tmp/factor_test_output.json --mode full
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import re
import sys
import types
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parent.parent
FACTOR_POOL_PATH = REPO_ROOT / "final_factor_pool_update20260123.py"
VALIDATION_ALLOWLIST_PATH = REPO_ROOT / "scripts" / "factor_validation_allowlist.txt"
FACTOR_NAME_PATTERN = re.compile(
    r"\b(?:factor_\d{3}|baseline_\d{3}|factor_trades_\d{3}|TD_[A-Z]{2}_\d{3}|TP_VPI_\d{3})\b"
)

SUBSET_FACTORS = [
    "factor_002",
    "factor_008",
    "factor_012",
    "factor_020",
    "factor_026",
    "factor_028",
    "factor_036",
    "factor_040",
    "factor_042",
    "factor_043",
    "factor_049",
    "factor_053",
    "factor_058",
    "factor_059",
    "factor_062",
    "factor_064",
    "factor_067",
    "factor_068",
    "factor_069",
    "factor_070",
    "factor_073",
    "factor_079",
    "factor_080",
    "factor_085",
    "factor_091",
    "factor_093",
    "factor_095",
    "factor_103",
    "factor_111",
    "factor_113",
    "factor_114",
    "factor_115",
    "factor_120",
    "factor_122",
    "factor_125",
    "factor_129",
    "factor_130",
    "factor_159",
]

OPV_FACTORS = [
    "TD_TI_001", "TD_TI_002", "TD_TI_005", "TD_TI_006", "TD_TI_008", "TD_TI_009",
    "TD_TI_010", "TD_TI_011", "TD_TI_013", "TD_TI_014", "TD_TI_015", "TD_TI_016",
    "TD_TI_017", "TD_TI_018", "TD_TI_019", "TD_TI_020", "TD_TI_021", "TD_TI_022",
    "TD_TI_023", "TD_TI_024", "TD_TI_025", "TD_TI_026", "TD_TI_027", "TD_TI_028",
    "TD_TI_030", "TD_TI_031", "TD_TI_032", "TD_TI_033", "TD_TI_034", "TD_TI_035",
    "TD_TI_036", "TD_TI_037", "TD_TI_038", "TD_TI_039", "TD_TI_040", "TD_TI_041",
    "TD_TI_043", "TD_TI_044", "TD_TI_045",
    "TD_MT_001", "TD_MT_002", "TD_MT_003", "TD_MT_005", "TD_MT_007", "TD_MT_008",
    "TD_MT_010", "TD_MT_011", "TD_MT_013", "TD_MT_014", "TD_MT_015", "TD_MT_016",
    "TD_MT_017", "TD_MT_018", "TD_MT_019", "TD_MT_021", "TD_MT_022", "TD_MT_025",
    "TD_MT_027", "TD_MT_028", "TD_MT_029", "TD_MT_030", "TD_MT_032", "TD_MT_034",
    "TD_MT_035", "TD_MT_036", "TD_MT_037", "TD_MT_038", "TD_MT_039", "TD_MT_040",
    "TD_MT_042",
    "TP_VPI_001", "TP_VPI_002", "TP_VPI_003", "TP_VPI_004", "TP_VPI_005", "TP_VPI_006",
    "TP_VPI_008", "TP_VPI_009", "TP_VPI_011", "TP_VPI_012", "TP_VPI_013", "TP_VPI_014",
    "TP_VPI_015", "TP_VPI_016", "TP_VPI_017", "TP_VPI_018", "TP_VPI_019",
    "TD_VI_001", "TD_VI_002", "TD_VI_005", "TD_VI_009", "TD_VI_010", "TD_VI_011",
    "TD_VI_012", "TD_VI_013", "TD_VI_020", "TD_VI_021", "TD_VI_022", "TD_VI_023",
    "TD_VI_024", "TD_VI_025", "TD_VI_026", "TD_VI_027", "TD_VI_028", "TD_VI_029",
    "TD_PT_001", "TD_PT_002", "TD_PT_003", "TD_PT_004", "TD_PT_009", "TD_PT_010",
    "TD_PT_024", "TD_PT_025", "TD_PT_026", "TD_PT_027",
    "TD_CI_003", "TD_CI_007", "TD_CI_008", "TD_CI_010",
    "TD_PR_001", "TD_PR_002", "TD_PR_005", "TD_PR_006", "TD_PR_007", "TD_PR_008",
    "TD_PR_010", "TD_PR_011", "TD_PR_012", "TD_PR_013", "TD_PR_014", "TD_PR_015",
    "TD_PR_016", "TD_PR_017",
]

ABS_TOL = 1e-9
REL_TOL = 1e-6


def load_validation_allowlist() -> set[str]:
    text = VALIDATION_ALLOWLIST_PATH.read_text()
    return set(FACTOR_NAME_PATTERN.findall(text))


VALIDATION_ALLOWLIST = load_validation_allowlist()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_path", nargs="?", default="/tmp/factor_test_output.json")
    parser.add_argument(
        "--mode",
        choices=["auto", "baseline", "opv", "full", "subset"],
        default="auto",
        help="alignment mode",
    )
    return parser.parse_args()


def load_scenarios(path: str):
    with open(path, "r") as f:
        return json.load(f)["scenarios"]


def pandas_available() -> bool:
    return importlib.util.find_spec("pandas") is not None


def normalize_py_value(value: float) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    if not math.isfinite(float(value)):
        return 0.0
    return float(value)


def _to_numpy(value) -> np.ndarray:
    if isinstance(value, FakeSeries):
        return value.values
    return np.asarray(value, dtype=float)


def _normalize_ufunc_kwarg(value):
    if isinstance(value, FakeSeries):
        return value.values
    if isinstance(value, tuple):
        return tuple(_normalize_ufunc_kwarg(item) for item in value)
    if isinstance(value, list):
        return [_normalize_ufunc_kwarg(item) for item in value]
    return value


class FakeSeriesIloc:
    def __init__(self, series: "FakeSeries"):
        self.series = series

    def __getitem__(self, idx):
        return self.series.values[idx]

    def __setitem__(self, idx, value):
        self.series.values[idx] = value


class FakeSeries:
    __array_priority__ = 1000

    def __init__(self, data=None, index=None, dtype=None):
        if data is None:
            size = 0 if index is None else len(index)
            arr = np.full(size, np.nan, dtype=float if dtype is None else dtype)
        else:
            arr = np.asarray(data if not isinstance(data, FakeSeries) else data.values)
            if arr.ndim == 0:
                arr = np.asarray([arr.item()])
            if dtype is not None:
                arr = arr.astype(dtype)
            elif arr.dtype.kind != "b":
                arr = arr.astype(float)
        self.values = np.asarray(arr)
        self.index = np.arange(self.values.shape[0]) if index is None else np.asarray(index)
        self.iloc = FakeSeriesIloc(self)

    def __array__(self, dtype=None):
        if dtype is None:
            return self.values
        return self.values.astype(dtype)

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        if method != "__call__":
            return NotImplemented
        raw_inputs = [x.values if isinstance(x, FakeSeries) else x for x in inputs]
        raw_kwargs = {key: _normalize_ufunc_kwarg(value) for key, value in kwargs.items()}
        with np.errstate(all="ignore"):
            result = getattr(ufunc, method)(*raw_inputs, **raw_kwargs)
        if isinstance(result, tuple):
            return tuple(FakeSeries(x, index=self.index) if isinstance(x, np.ndarray) else x for x in result)
        if isinstance(result, np.ndarray):
            return FakeSeries(result, index=self.index)
        return result

    def __len__(self):
        return len(self.values)

    def __iter__(self):
        return iter(self.values)

    def __getitem__(self, item):
        out = self.values[item]
        if isinstance(item, slice):
            return FakeSeries(out)
        return out

    def to_numpy(self):
        return self.values.copy()

    def copy(self):
        return FakeSeries(self.values.copy(), index=self.index.copy())

    @property
    def shape(self):
        return self.values.shape

    def rolling(self, window, min_periods=None, center=False, **kwargs):
        if center not in (False, 0):
            raise NotImplementedError("FakeSeries.rolling does not support center=True")
        return FakeRollingSeries(self, window, min_periods)

    def diff(self, periods=1):
        out = np.full_like(self.values, np.nan, dtype=float)
        if periods <= 0:
            return FakeSeries(out)
        out[periods:] = self.values[periods:] - self.values[:-periods]
        return FakeSeries(out)

    def pct_change(self, periods=1):
        out = np.full_like(self.values, np.nan, dtype=float)
        if periods <= 0:
            return FakeSeries(out)
        prev = self.values[:-periods]
        curr = self.values[periods:]
        mask = np.isfinite(prev) & np.isfinite(curr) & (np.abs(prev) > 1e-12)
        out[periods:][mask] = (curr[mask] - prev[mask]) / prev[mask]
        return FakeSeries(out)

    def shift(self, periods=1):
        out = np.full_like(self.values, np.nan, dtype=float)
        if periods >= 0:
            if periods < len(self.values):
                out[periods:] = self.values[: len(self.values) - periods]
        else:
            periods = -periods
            if periods < len(self.values):
                out[: len(self.values) - periods] = self.values[periods:]
        return FakeSeries(out)

    def cumsum(self):
        out = np.full_like(self.values, np.nan, dtype=float)
        acc = 0.0
        for i, value in enumerate(self.values):
            if np.isfinite(value):
                acc += value
                out[i] = acc
            else:
                out[i] = np.nan
        return FakeSeries(out, index=self.index)

    def ewm(self, com=None, span=None, adjust=True):
        return FakeEwmSeries(self, com=com, span=span, adjust=adjust)

    def where(self, cond, other):
        cond_arr = np.asarray(cond, dtype=bool)
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(np.where(cond_arr, self.values, other_arr))

    def clip(self, lower=None, upper=None):
        arr = self.values.copy()
        if lower is not None:
            arr = np.maximum(arr, lower)
        if upper is not None:
            arr = np.minimum(arr, upper)
        return FakeSeries(arr)

    def abs(self):
        return FakeSeries(np.abs(self.values))

    def fillna(self, value):
        arr = self.values.copy()
        arr[~np.isfinite(arr)] = value
        return FakeSeries(arr)

    def replace(self, to_replace, value):
        arr = self.values.copy()
        if isinstance(to_replace, (list, tuple, np.ndarray)):
            mask = np.zeros(arr.shape, dtype=bool)
            for item in to_replace:
                if isinstance(item, float) and math.isnan(item):
                    mask |= ~np.isfinite(arr)
                else:
                    mask |= arr == item
            arr[mask] = value
        else:
            arr[arr == to_replace] = value
        return FakeSeries(arr, index=self.index)

    def astype(self, dtype):
        return FakeSeries(self.values.astype(dtype), index=self.index)

    def _binop(self, other, op):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        with np.errstate(all="ignore"):
            return FakeSeries(op(self.values, other_arr), index=self.index)

    def __add__(self, other):
        return self._binop(other, np.add)

    def __radd__(self, other):
        return self._binop(other, np.add)

    def __sub__(self, other):
        return self._binop(other, np.subtract)

    def __rsub__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(np.subtract(other_arr, self.values))

    def __mul__(self, other):
        return self._binop(other, np.multiply)

    def __rmul__(self, other):
        return self._binop(other, np.multiply)

    def __truediv__(self, other):
        return self._binop(other, np.divide)

    def __rtruediv__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(np.divide(other_arr, self.values))

    def __pow__(self, other):
        return self._binop(other, np.power)

    def __neg__(self):
        return FakeSeries(-self.values, index=self.index)

    def __abs__(self):
        return FakeSeries(np.abs(self.values), index=self.index)

    def __gt__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(self.values > other_arr, index=self.index)

    def __ge__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(self.values >= other_arr, index=self.index)

    def __lt__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(self.values < other_arr, index=self.index)

    def __le__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(self.values <= other_arr, index=self.index)

    def __eq__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(self.values == other_arr, index=self.index)

    def __ne__(self, other):
        other_arr = _to_numpy(other) if isinstance(other, (FakeSeries, np.ndarray, list, tuple)) else other
        return FakeSeries(self.values != other_arr, index=self.index)

    def __and__(self, other):
        other_arr = np.asarray(_to_numpy(other), dtype=bool)
        return FakeSeries(np.asarray(self.values, dtype=bool) & other_arr, index=self.index)

    def __rand__(self, other):
        return self.__and__(other)

    def __or__(self, other):
        other_arr = np.asarray(_to_numpy(other), dtype=bool)
        return FakeSeries(np.asarray(self.values, dtype=bool) | other_arr, index=self.index)

    def __ror__(self, other):
        return self.__or__(other)


class FakeRollingSeries:
    def __init__(self, series: FakeSeries, window: int, min_periods=None):
        self.series = series
        self.window = int(window)
        self.min_periods = self.window if min_periods is None else int(min_periods)

    def _iter_windows(self):
        values = self.series.values
        out = []
        for i in range(values.shape[0]):
            start = max(0, i + 1 - self.window)
            tail = values[start : i + 1]
            valid = tail[np.isfinite(tail)]
            if valid.shape[0] < self.min_periods:
                out.append(np.nan)
            else:
                out.append(valid)
        return out

    def mean(self):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = tail.mean()
        return FakeSeries(out)

    def std(self, ddof=1):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = 0.0 if tail.shape[0] <= ddof else tail.std(ddof=ddof)
        return FakeSeries(out)

    def var(self, ddof=1):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = 0.0 if tail.shape[0] <= ddof else tail.var(ddof=ddof)
        return FakeSeries(out)

    def min(self):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = tail.min()
        return FakeSeries(out)

    def max(self):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = tail.max()
        return FakeSeries(out)

    def sum(self):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = tail.sum()
        return FakeSeries(out)

    def quantile(self, q):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                out[i] = np.quantile(tail, q, method="linear")
        return FakeSeries(out)

    def apply(self, func, raw=False):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                arg = tail if raw else FakeSeries(tail)
                out[i] = func(arg)
        return FakeSeries(out)

    def cov(self, other):
        other_arr = _to_numpy(other)
        out = np.full(self.series.values.shape[0], np.nan)
        for i in range(self.series.values.shape[0]):
            start = max(0, i + 1 - self.window)
            x = self.series.values[start : i + 1]
            y = other_arr[start : i + 1]
            mask = np.isfinite(x) & np.isfinite(y)
            x = x[mask]
            y = y[mask]
            if x.shape[0] < self.min_periods:
                continue
            out[i] = 0.0 if x.shape[0] < 2 else np.cov(x, y, ddof=1)[0, 1]
        return FakeSeries(out)

    def corr(self, other):
        other_arr = _to_numpy(other)
        out = np.full(self.series.values.shape[0], np.nan)
        for i in range(self.series.values.shape[0]):
            start = max(0, i + 1 - self.window)
            x = self.series.values[start : i + 1]
            y = other_arr[start : i + 1]
            mask = np.isfinite(x) & np.isfinite(y)
            x = x[mask]
            y = y[mask]
            if x.shape[0] < self.min_periods:
                continue
            if x.shape[0] < 2 or np.std(x) <= 1e-12 or np.std(y) <= 1e-12:
                out[i] = 0.0
            else:
                out[i] = np.corrcoef(x, y)[0, 1]
        return FakeSeries(out)

    def rank(self):
        out = np.full(self.series.values.shape[0], np.nan)
        for i, tail in enumerate(self._iter_windows()):
            if isinstance(tail, np.ndarray):
                last = tail[-1]
                lt = np.sum(tail < last)
                eq = np.sum(tail == last)
                out[i] = np.nan if eq == 0 else lt + (eq + 1.0) / 2.0
        return FakeSeries(out)


class FakeEwmSeries:
    def __init__(self, series: FakeSeries, com=None, span=None, adjust=True):
        self.series = series
        self.adjust = adjust
        if com is not None:
            self.alpha = 1.0 / (1.0 + float(com))
        elif span is not None:
            self.alpha = 2.0 / (float(span) + 1.0)
        else:
            raise ValueError("FakeEwmSeries requires com or span")

    def mean(self):
        values = self.series.values.astype(float)
        out = np.full(values.shape[0], np.nan)
        if values.shape[0] == 0:
            return FakeSeries(out, index=self.series.index)
        alpha = self.alpha
        decay = 1.0 - alpha
        if self.adjust:
            weighted_sum = values[0]
            weight = 1.0
            out[0] = values[0]
            for i in range(1, values.shape[0]):
                v = values[i]
                if np.isfinite(v):
                    weighted_sum = v + decay * weighted_sum
                    weight = 1.0 + decay * weight
                    out[i] = weighted_sum / weight
        else:
            ema = values[0]
            out[0] = values[0]
            for i in range(1, values.shape[0]):
                v = values[i]
                if np.isfinite(v):
                    ema = alpha * v + decay * ema
                    out[i] = ema
        return FakeSeries(out, index=self.series.index)


class FakeDataFrame:
    def __init__(self, data, index=None):
        if isinstance(data, dict):
            self._data = {k: (v if isinstance(v, FakeSeries) else FakeSeries(v, index=index)) for k, v in data.items()}
            size = len(next(iter(self._data.values())).values) if self._data else 0
            self.index = np.arange(size) if index is None else np.asarray(index)
        else:
            arr = np.asarray(data, dtype=float)
            if arr.ndim == 1:
                arr = arr.reshape(-1, 1)
            self._data = {i: FakeSeries(arr[:, i], index=index) for i in range(arr.shape[1])}
            self.index = np.arange(arr.shape[0]) if index is None else np.asarray(index)

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._data[key]
        if isinstance(key, (list, tuple)):
            return FakeDataFrame({k: self._data[k] for k in key}, index=self.index)
        raise KeyError(key)

    def __setitem__(self, key, value):
        self._data[key] = value if isinstance(value, FakeSeries) else FakeSeries(value, index=self.index)

    def __len__(self):
        return len(self.index)

    def __getattr__(self, item):
        try:
            return self._data[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def max(self, axis=0):
        if axis == 1:
            arr = np.column_stack([series.values for series in self._data.values()])
            return FakeSeries(np.nanmax(arr, axis=1), index=self.index)
        raise NotImplementedError

    def min(self, axis=0):
        if axis == 1:
            arr = np.column_stack([series.values for series in self._data.values()])
            return FakeSeries(np.nanmin(arr, axis=1), index=self.index)
        raise NotImplementedError

    def sum(self, axis=0):
        if axis == 1:
            arr = np.column_stack([series.values for series in self._data.values()])
            return FakeSeries(np.nansum(arr, axis=1), index=self.index)
        raise NotImplementedError


def build_fake_pandas_module():
    mod = types.ModuleType("pandas")
    mod.Series = FakeSeries
    mod.DataFrame = FakeDataFrame
    return mod


def build_depth_arrays(input_data):
    depth_bids = np.asarray(input_data["depth_bids"], dtype=float)
    depth_asks = np.asarray(input_data["depth_asks"], dtype=float)
    bidp = depth_bids[:, :, 0]
    bidv = depth_bids[:, :, 1]
    askp = depth_asks[:, :, 0]
    askv = depth_asks[:, :, 1]
    return bidp, bidv, askp, askv


def rolling_mean(arr: np.ndarray, window: int, min_periods: int | None = None) -> np.ndarray:
    if min_periods is None:
        min_periods = window
    out = np.full(arr.shape[0], np.nan)
    for i in range(arr.shape[0]):
        start = max(0, i + 1 - window)
        tail = arr[start : i + 1]
        if tail.shape[0] < min_periods or np.any(~np.isfinite(tail)):
            continue
        out[i] = tail.mean()
    return out


def rolling_std(
    arr: np.ndarray,
    window: int,
    min_periods: int | None = None,
    ddof: int = 1,
) -> np.ndarray:
    if min_periods is None:
        min_periods = window
    out = np.full(arr.shape[0], np.nan)
    for i in range(arr.shape[0]):
        start = max(0, i + 1 - window)
        tail = arr[start : i + 1]
        if tail.shape[0] < min_periods or np.any(~np.isfinite(tail)):
            continue
        if tail.shape[0] <= ddof:
            out[i] = 0.0
        else:
            out[i] = tail.std(ddof=ddof)
    return out


def rolling_quantile(arr: np.ndarray, window: int, q: float) -> np.ndarray:
    out = np.full(arr.shape[0], np.nan)
    for i in range(arr.shape[0]):
        if i + 1 < window:
            continue
        tail = arr[i + 1 - window : i + 1]
        tail = tail[np.isfinite(tail)]
        if tail.size == 0:
            continue
        out[i] = np.quantile(tail, q, method="linear")
    return out


def pct_change(arr: np.ndarray, periods: int) -> np.ndarray:
    out = np.full(arr.shape[0], np.nan)
    for i in range(periods, arr.shape[0]):
        prev = arr[i - periods]
        curr = arr[i]
        if np.isfinite(prev) and np.isfinite(curr) and abs(prev) > 1e-12:
            out[i] = (curr - prev) / prev
    return out


def sample_skew(values: np.ndarray) -> float:
    vals = values[np.isfinite(values)]
    n = vals.size
    if n < 3:
        return np.nan
    mean = vals.mean()
    m2 = np.mean((vals - mean) ** 2)
    if abs(m2) <= 1e-12:
        return 0.0
    m3 = np.mean((vals - mean) ** 3)
    g1 = m3 / (m2**1.5)
    return math.sqrt(n * (n - 1)) / (n - 2) * g1


def harmonic_mean(values: np.ndarray) -> float:
    vals = np.asarray(values, dtype=float)
    if np.any(~np.isfinite(vals)) or np.any(vals <= 0.0):
        return np.nan
    return vals.size / np.sum(1.0 / vals)


def weighted_harmonic(values: np.ndarray) -> float:
    vals = np.asarray(values, dtype=float)
    weights = np.arange(1, vals.size + 1, dtype=float)
    if np.any(~np.isfinite(vals)) or np.any(vals <= 0.0):
        return np.nan
    return vals.size / np.sum(weights / vals)


def compute_subset_factor_last(
    name: str,
    bidp: np.ndarray,
    bidv: np.ndarray,
    askp: np.ndarray,
    askv: np.ndarray,
):
    bidv20 = bidv[:, :20].sum(axis=1)
    askv20 = askv[:, :20].sum(axis=1)
    bidv10 = bidv[:, :10].sum(axis=1)
    askv10 = askv[:, :10].sum(axis=1)
    bidv5 = bidv[:, :5].sum(axis=1)
    askv5 = askv[:, :5].sum(axis=1)
    bidv3 = bidv[:, :3].sum(axis=1)
    askv3 = askv[:, :3].sum(axis=1)

    if name == "factor_002":
        return askv20[-1] / bidv20[-1] if abs(bidv20[-1]) > 1e-12 else np.nan
    if name == "factor_008":
        return np.std(bidp[-1, :10] * bidv[-1, :10] - askp[-1, :10] * askv[-1, :10])
    if name == "factor_012":
        bid_value = np.sum(bidp[-1, :20] * bidv[-1, :20])
        ask_value = np.sum(askp[-1, :20] * askv[-1, :20])
        den = bid_value + ask_value
        return (bid_value - ask_value) / den if abs(den) > 1e-12 else np.nan
    if name == "factor_020":
        ask_vwap20 = np.divide(
            np.sum(askp[:, :20] * askv[:, :20], axis=1),
            askv20,
            out=np.full(askv20.shape, np.nan),
            where=np.abs(askv20) > 1e-12,
        )
        roll = rolling_mean(ask_vwap20, 5, 1)
        return roll[-1] - roll[-2]
    if name == "factor_026":
        bottom17 = askv20[-1] - askv3[-1]
        return askv3[-1] / bottom17 if abs(bottom17) > 1e-12 else np.nan
    if name == "factor_028":
        return sample_skew(askv[-1, :20])
    if name == "factor_036":
        avg_ask15 = askp[:, :15].mean(axis=1)
        ma = rolling_mean(avg_ask15, 300, 300)[-1]
        std = rolling_std(avg_ask15, 300, 300, 1)[-1]
        return (avg_ask15[-1] - ma) / std if np.isfinite(std) and abs(std) > 1e-12 else np.nan
    if name == "factor_040":
        return np.std(bidv[-1, :10])
    if name == "factor_042":
        return bidv5[-1] / askv5[-1] if abs(askv5[-1]) > 1e-12 else np.nan
    if name == "factor_043":
        total = bidv20[-1] + askv20[-1]
        top1 = bidv[-1, 0] + askv[-1, 0]
        return top1 / total if abs(total) > 1e-12 else np.nan
    if name == "factor_049":
        mean_bid5 = bidp[:, :5].mean(axis=1)
        return rolling_std(mean_bid5, 30, 30, 1)[-1]
    if name == "factor_053":
        top5 = bidv5[-1] + askv5[-1]
        top1 = bidv[-1, 0] + askv[-1, 0]
        return top1 / top5 if abs(top5) > 1e-12 else np.nan
    if name == "factor_058":
        return bidv5[-1]
    if name == "factor_059":
        den = askv5[-1] + bidv5[-1]
        return askv5[-1] / den if abs(den) > 1e-12 else np.nan
    if name == "factor_062":
        weights = bidv[-1, :5] + askv[-1, :5]
        spreads = askp[-1, :5] - bidp[-1, :5]
        den = weights.sum()
        return np.dot(spreads, weights) / den if abs(den) > 1e-12 else np.nan
    if name == "factor_064":
        return np.max(askv[-1, :10])
    if name == "factor_067":
        return bidv10[-1] - askv10[-1]
    if name == "factor_068":
        return bidv10[-1] / askv10[-1] if abs(askv10[-1]) > 1e-12 else np.nan
    if name == "factor_069":
        return np.sqrt(bidv[-1, :5]).sum()
    if name == "factor_070":
        return np.sqrt(askv[-1, :5]).sum()
    if name == "factor_073":
        return np.std(bidv[-1, :5])
    if name == "factor_079":
        den = askv20[-1] + bidv20[-1]
        return askv20[-1] / den if abs(den) > 1e-12 else np.nan
    if name == "factor_080":
        combined_level9 = bidv[:, 9] + askv[:, 9]
        return rolling_std(combined_level9, 3, 3, 1)[-1]
    if name == "factor_085":
        return np.cov(bidv[-1, :10], askv[-1, :10])[0, 1]
    if name == "factor_091":
        ask_std = np.std(askv[-1, :10])
        return np.std(bidv[-1, :10]) / ask_std if abs(ask_std) > 1e-12 else np.nan
    if name == "factor_093":
        avg_ask5 = askp[:, :5].mean(axis=1)
        return rolling_quantile(avg_ask5, 360, 0.5)[-1]
    if name == "factor_095":
        return np.median(bidv[-1, :20])
    if name == "factor_103":
        return harmonic_mean(askv[-1, :10])
    if name == "factor_111":
        return rolling_std(bidp[:, 9], 3, 3, 1)[-1] ** 2
    if name == "factor_113":
        bid_pct = np.vstack([pct_change(bidp[:, i], 30) for i in range(10)])
        return weighted_harmonic(bid_pct[:, -1])
    if name == "factor_114":
        ask_pct_mean = np.vstack([pct_change(askp[:, i], 30) for i in range(10)]).mean(axis=0)
        roll = np.full(ask_pct_mean.shape[0], np.nan)
        for i in range(ask_pct_mean.shape[0]):
            if i + 1 < 600:
                continue
            tail = ask_pct_mean[i + 1 - 600 : i + 1]
            if np.any(~np.isfinite(tail)):
                continue
            roll[i] = tail.sum()
        return roll[-1] - roll[-2]
    if name == "factor_115":
        bid_vwap20 = np.divide(
            np.sum(bidp[:, :20] * bidv[:, :20], axis=1),
            bidv20,
            out=np.full(bidv20.shape, np.nan),
            where=np.abs(bidv20) > 1e-12,
        )
        ask_vwap20 = np.divide(
            np.sum(askp[:, :20] * askv[:, :20], axis=1),
            askv20,
            out=np.full(askv20.shape, np.nan),
            where=np.abs(askv20) > 1e-12,
        )
        diff = bid_vwap20 - ask_vwap20
        diff_pct = pct_change(diff, 1)
        return rolling_mean(diff_pct, 60, 60)[-1]
    if name == "factor_120":
        return np.median(bidv[-1, :10])
    if name == "factor_122":
        ask15 = askv[-1, :15].sum()
        ask3 = askv[-1, :3].sum()
        return ask3 / ask15 if abs(ask15) > 1e-12 else np.nan
    if name == "factor_125":
        prices = np.median(np.concatenate([bidp[:, :20], askp[:, :20]], axis=1), axis=1)
        pct15_val = pct_change(prices, 15)[-1]
        if not np.isfinite(pct15_val):
            return np.nan
        period = 5 if pct15_val < 0.0 else 20
        return pct_change(prices, period)[-1]
    if name == "factor_129":
        return weighted_harmonic(bidv[-1, :10])
    if name == "factor_130":
        return weighted_harmonic(askv[-1, :10])
    if name == "factor_159":
        return bidp[-1, :10].mean() - askp[-1, :10].mean()
    raise KeyError(name)


def run_subset_compare(scenarios) -> int:
    subset_names = [name for name in SUBSET_FACTORS if name in VALIDATION_ALLOWLIST]
    failed = False
    for scenario_name, scenario_data in scenarios.items():
        bidp, bidv, askp, askv = build_depth_arrays(scenario_data["input"])
        rust = scenario_data["fusion_factors"]
        mismatches = []

        for name in subset_names:
            rv = rust.get(name)
            pv = compute_subset_factor_last(name, bidp, bidv, askp, askv)
            if rv is None and np.isnan(pv):
                continue
            pv = normalize_py_value(pv)
            if rv is None:
                mismatches.append((name, rv, pv, float("inf")))
                continue
            rv = float(rv)
            abs_diff = abs(rv - pv)
            rel_diff = abs_diff / max(abs(rv), abs(pv), 1e-10)
            if abs_diff > ABS_TOL and rel_diff >= REL_TOL:
                mismatches.append((name, rv, pv, rel_diff))

        print(f"{scenario_name}: compared={len(subset_names)} mismatches={len(mismatches)}")
        for name, rv, pv, rel in mismatches:
            print(f"  {name}: rust={rv} py={pv} rel={rel:.6e}")
        failed = failed or bool(mismatches)

    return 1 if failed else 0


def build_dataframe_full(input_data, pd):
    df_dict = {}
    for field_name, values in input_data.items():
        if field_name not in ["depth_bids", "depth_asks"]:
            df_dict[field_name] = values
    df = pd.DataFrame(df_dict)

    depth_bids = input_data["depth_bids"]
    depth_asks = input_data["depth_asks"]
    for i in range(20):
        df[f"bid{i}p"] = [bar[i][0] if i < len(bar) else 0.0 for bar in depth_bids]
        df[f"bid{i}v"] = [bar[i][1] if i < len(bar) else 0.0 for bar in depth_bids]
        df[f"ask{i}p"] = [bar[i][0] if i < len(bar) else 0.0 for bar in depth_asks]
        df[f"ask{i}v"] = [bar[i][1] if i < len(bar) else 0.0 for bar in depth_asks]
    return df


def load_factor_pool(use_fake_pandas: bool = False):
    prev_pandas = sys.modules.get("pandas")
    if use_fake_pandas:
        sys.modules["pandas"] = build_fake_pandas_module()
    spec = importlib.util.spec_from_file_location("factor_pool", str(FACTOR_POOL_PATH))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    try:
        spec.loader.exec_module(mod)
        return mod
    finally:
        if use_fake_pandas:
            if prev_pandas is None:
                sys.modules.pop("pandas", None)
            else:
                sys.modules["pandas"] = prev_pandas


def resolve_python_func(pool, rust_name):
    if rust_name == "factor_097":
        return _safe_factor_097
    func = getattr(pool, rust_name, None)
    if func is not None and callable(func):
        return func
    return None


def _safe_factor_097(df):
    ratios = []
    for i in range(10):
        bid = np.asarray(df[f"bid{i}v"].values, dtype=float)
        ask = np.asarray(df[f"ask{i}v"].values, dtype=float)
        level = np.full_like(bid, np.nan, dtype=float)
        mask = np.isfinite(bid) & np.isfinite(ask) & (np.abs(ask) > 1e-12)
        level[mask] = bid[mask] / ask[mask]
        ratios.append(level)
    return np.nanmean(np.asarray(ratios, dtype=float), axis=0)


def run_full_compare(scenarios) -> int:
    import pandas as pd

    pool = load_factor_pool()
    failed = False

    for scenario_name, scenario_data in scenarios.items():
        df = build_dataframe_full(scenario_data["input"], pd)
        rust_factors = scenario_data["fusion_factors"]
        mismatches = []
        compared = 0
        skipped = 0
        excluded = 0

        for rust_name in sorted(rust_factors.keys()):
            if rust_name not in VALIDATION_ALLOWLIST:
                excluded += 1
                continue
            rust_val = rust_factors[rust_name]
            if rust_val is None:
                continue
            func = resolve_python_func(pool, rust_name)
            if func is None:
                skipped += 1
                continue

            compared += 1
            try:
                result = func(df)
                if hasattr(result, "values"):
                    result = result.values
                py_val = normalize_py_value(float(result[-1]))
            except Exception as exc:
                mismatches.append((rust_name, float(rust_val), f"PY_ERROR: {exc}", float("inf")))
                continue

            rust_float = float(rust_val)
            abs_diff = abs(rust_float - py_val)
            rel_diff = abs_diff / max(abs(rust_float), abs(py_val), 1e-10)
            if abs_diff > ABS_TOL and rel_diff >= REL_TOL:
                mismatches.append((rust_name, rust_float, py_val, rel_diff))

        print(
            f"{scenario_name}: compared={compared} mismatches={len(mismatches)} "
            f"skipped_no_python={skipped} excluded_not_allowlisted={excluded}"
        )
        for rust_name, rust_val, py_val, rel_diff in mismatches[:50]:
            print(f"  {rust_name}: rust={rust_val} py={py_val} rel={rel_diff:.6e}")
        failed = failed or bool(mismatches)

    return 1 if failed else 0


def run_baseline_compare(scenarios) -> int:
    use_fake_pandas = not pandas_available()
    if use_fake_pandas:
        pd = build_fake_pandas_module()
    else:
        import pandas as pd

    pool = load_factor_pool(use_fake_pandas=use_fake_pandas)
    failed = False

    for scenario_name, scenario_data in scenarios.items():
        df = build_dataframe_full(scenario_data["input"], pd)
        rust_factors = scenario_data["fusion_factors"]
        baseline_names = sorted(
            name
            for name, value in rust_factors.items()
            if name.startswith("baseline_") and name in VALIDATION_ALLOWLIST and value is not None
        )
        mismatches = []

        for rust_name in baseline_names:
            rust_val = rust_factors[rust_name]
            func = resolve_python_func(pool, rust_name)
            if func is None:
                mismatches.append((rust_name, float(rust_val), "PY_MISSING", float("inf")))
                continue

            try:
                result = func(df)
                if isinstance(result, FakeSeries):
                    result = result.values
                elif hasattr(result, "values"):
                    result = result.values
                py_val = normalize_py_value(float(np.asarray(result, dtype=float)[-1]))
            except Exception as exc:
                mismatches.append((rust_name, float(rust_val), f"PY_ERROR: {exc}", float("inf")))
                continue

            rust_float = float(rust_val)
            abs_diff = abs(rust_float - py_val)
            rel_diff = abs_diff / max(abs(rust_float), abs(py_val), 1e-10)
            if abs_diff > ABS_TOL and rel_diff >= REL_TOL:
                mismatches.append((rust_name, rust_float, py_val, rel_diff))

        print(
            f"{scenario_name}: compared={len(baseline_names)} mismatches={len(mismatches)} pandas_shim={use_fake_pandas}"
        )
        for rust_name, rust_val, py_val, rel_diff in mismatches[:80]:
            print(f"  {rust_name}: rust={rust_val} py={py_val} rel={rel_diff:.6e}")
        failed = failed or bool(mismatches)

    return 1 if failed else 0


def run_opv_compare(scenarios) -> int:
    use_fake_pandas = not pandas_available()
    if use_fake_pandas:
        pd = build_fake_pandas_module()
    else:
        import pandas as pd

    pool = load_factor_pool(use_fake_pandas=use_fake_pandas)
    failed = False

    for scenario_name, scenario_data in scenarios.items():
        df = build_dataframe_full(scenario_data["input"], pd)
        rust_factors = scenario_data["fusion_factors"]
        names = [
            name
            for name in OPV_FACTORS
            if name in VALIDATION_ALLOWLIST and rust_factors.get(name) is not None
        ]
        mismatches = []

        for rust_name in names:
            rust_val = rust_factors[rust_name]
            func = resolve_python_func(pool, rust_name)
            if func is None:
                mismatches.append((rust_name, float(rust_val), "PY_MISSING", float("inf")))
                continue

            try:
                result = func(df)
                if isinstance(result, FakeSeries):
                    result = result.values
                elif hasattr(result, "values"):
                    result = result.values
                py_val = normalize_py_value(float(np.asarray(result, dtype=float)[-1]))
            except Exception as exc:
                mismatches.append((rust_name, float(rust_val), f"PY_ERROR: {exc}", float("inf")))
                continue

            rust_float = float(rust_val)
            abs_diff = abs(rust_float - py_val)
            rel_diff = abs_diff / max(abs(rust_float), abs(py_val), 1e-10)
            if abs_diff > ABS_TOL and rel_diff >= REL_TOL:
                mismatches.append((rust_name, rust_float, py_val, rel_diff))

        print(
            f"{scenario_name}: compared={len(names)} mismatches={len(mismatches)} pandas_shim={use_fake_pandas}"
        )
        for rust_name, rust_val, py_val, rel_diff in mismatches[:120]:
            print(f"  {rust_name}: rust={rust_val} py={py_val} rel={rel_diff:.6e}")
        failed = failed or bool(mismatches)

    return 1 if failed else 0


def main() -> int:
    args = parse_args()
    scenarios = load_scenarios(args.json_path)

    mode = args.mode
    if mode == "auto":
        mode = "full" if pandas_available() else "subset"

    if mode == "full" and not pandas_available():
        print("pandas is not available; full mode cannot run in this environment", file=sys.stderr)
        return 2

    print(f"mode={mode}")
    if mode == "baseline":
        return run_baseline_compare(scenarios)
    if mode == "opv":
        return run_opv_compare(scenarios)
    if mode == "full":
        return run_full_compare(scenarios)
    return run_subset_compare(scenarios)


if __name__ == "__main__":
    raise SystemExit(main())
