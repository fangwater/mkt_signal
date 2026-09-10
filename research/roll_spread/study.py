"""Second-level fixed-pair roll-spread research from causal BBO parquet.

Daily dominant tables decide each old/new pair and effective roll date. Prices
come only from contract-level backtest_1s BBO, with no HFQ or continuous price.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Callable, Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq


CN_EXCHANGES = ("ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce")
CME_PRODUCTS = (
    ("CME", "ES"), ("CME", "NQ"), ("CME", "RTY"), ("CBOT", "YM"),
    ("COMEX", "GC"), ("NYMEX", "CL"),
)


@dataclass(frozen=True)
class StudyPaths:
    """Explicit, read-only sources."""

    cn_roll_root: Path
    cme_roll_root: Path
    cn_backtest_root: Path
    cme_backtest_root: Path


@dataclass(frozen=True)
class StudyParameters:
    """Pre-specified high-frequency measurement choices."""

    pre_roll_days: int = 10
    include_roll_day: bool = False
    smoothing_seconds: int = 60
    min_seconds_per_minute: int = 30
    path_bins: int = 100

    def __post_init__(self) -> None:
        if self.pre_roll_days <= 0 or self.smoothing_seconds <= 0:
            raise ValueError("pre_roll_days and smoothing_seconds must be positive")
        if not 1 <= self.min_seconds_per_minute <= self.smoothing_seconds:
            raise ValueError("min_seconds_per_minute must be within the smoothing window")
        if self.path_bins < 2:
            raise ValueError("path_bins must be at least two")


def _require_columns(frame: pd.DataFrame, names: Iterable[str], label: str) -> None:
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise ValueError(f"{label} lacks required columns: {missing}")


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def _cn_delivery_month(contract_id: str, as_of: pd.Timestamp) -> pd.Timestamp | pd.NaT:
    """Resolve YYMM/YMM only to assert that the new contract is farther dated."""

    match = re.fullmatch(r"[A-Za-z]+(\d{3,4})", str(contract_id).strip())
    if not match:
        return pd.NaT
    code = match.group(1)
    if len(code) == 4:
        year, month = 2000 + int(code[:2]), int(code[2:])
        return pd.Timestamp(year=year, month=month, day=1) if 1 <= month <= 12 else pd.NaT
    month = int(code[1:])
    if not 1 <= month <= 12:
        return pd.NaT
    candidates = [
        pd.Timestamp(year=year, month=month, day=1)
        for year in range(as_of.year - 10, as_of.year + 21)
        if year % 10 == int(code[0])
    ]
    if not candidates:
        return pd.NaT
    lower_bound = as_of.normalize().replace(day=1) - pd.DateOffset(months=1)
    viable = [candidate for candidate in candidates if candidate >= lower_bound]
    return min(viable or candidates, key=lambda candidate: abs(candidate - as_of))


def _cme_delivery_month(contract_id: str, _: pd.Timestamp) -> pd.Timestamp | pd.NaT:
    match = re.search(r"(\d{4})-(\d{2})$", str(contract_id).strip())
    if not match:
        return pd.NaT
    year, month = (int(value) for value in match.groups())
    return pd.Timestamp(year=year, month=month, day=1) if 1 <= month <= 12 else pd.NaT


def _backtest_contract_id(contract_id: str, exchange: str) -> str:
    """Map only Zhengzhou's YYYYMM dominant identifier to its YMM quote identifier."""

    contract = str(contract_id).strip()
    if exchange != "xzce":
        return contract
    match = re.fullmatch(r"([A-Za-z]+)(\d{4})", contract)
    if not match:
        return contract
    product, yymm = match.groups()
    return f"{product}{yymm[1:]}"


def _finish_events(
    events: pd.DataFrame,
    maturity_parser: Callable[[str, pd.Timestamp], pd.Timestamp | pd.NaT],
) -> pd.DataFrame:
    events = events.copy()
    events["event_date"] = pd.to_datetime(events["event_date"], errors="coerce")
    events = events.dropna(subset=["event_date", "old_contract", "new_contract"])
    for column in ("old_contract", "new_contract"):
        events[column] = events[column].astype(str).str.strip()
    events = events[(events["old_contract"] != "") & (events["new_contract"] != "")].copy()
    events["old_delivery_month"] = [
        maturity_parser(contract, day)
        for contract, day in zip(events["old_contract"], events["event_date"], strict=True)
    ]
    events["new_delivery_month"] = [
        maturity_parser(contract, day)
        for contract, day in zip(events["new_contract"], events["event_date"], strict=True)
    ]
    events = events[events["new_delivery_month"].gt(events["old_delivery_month"])].copy()
    events["product_key"] = events["market"] + ":" + events["exchange"] + ":" + events["product"]
    events = events.sort_values(["market", "exchange", "product", "event_date"]).reset_index(drop=True)
    events["event_id"] = [
        f"{key}:{day:%Y%m%d}:{index}"
        for index, (key, day) in enumerate(zip(events["product_key"], events["event_date"], strict=True))
    ]
    return events


def _extract_roll_events(
    dominant: pd.DataFrame,
    *,
    market: str,
    exchange: str,
    product_column: str,
    date_column: str,
    contract_column: str,
    roll_column: str,
    maturity_parser: Callable[[str, pd.Timestamp], pd.Timestamp | pd.NaT],
) -> tuple[pd.DataFrame, dict[tuple[str, str, str], pd.DatetimeIndex]]:
    """Freeze old/new contracts from an existing main-contract switch table."""

    _require_columns(
        dominant,
        [product_column, date_column, contract_column, roll_column],
        f"{market} dominant table",
    )
    selected = dominant[[product_column, date_column, contract_column, roll_column]].copy()
    selected.columns = ["product", "event_date", "new_contract", "roll_flag"]
    selected["event_date"] = pd.to_datetime(selected["event_date"], errors="coerce")
    selected["product"] = selected["product"].astype(str).str.strip()
    selected["new_contract"] = selected["new_contract"].fillna("").astype(str).str.strip()
    selected = selected.dropna(subset=["event_date"]).sort_values(["product", "event_date"]).reset_index(drop=True)
    calendars = {
        (market, exchange, product): pd.DatetimeIndex(group["event_date"].drop_duplicates().sort_values())
        for product, group in selected.groupby("product", sort=False)
    }
    selected["old_contract"] = selected.groupby("product", sort=False)["new_contract"].shift()
    events = selected[selected["roll_flag"].map(_truthy)].copy()
    events["market"], events["exchange"] = market, exchange
    return _finish_events(
        events[["market", "exchange", "product", "event_date", "old_contract", "new_contract"]],
        maturity_parser,
    ), calendars


def load_roll_events(paths: StudyPaths) -> tuple[pd.DataFrame, dict[tuple[str, str, str], pd.DatetimeIndex]]:
    """Load both markets' roll events. No EOD reference price is read."""

    event_frames: list[pd.DataFrame] = []
    calendars: dict[tuple[str, str, str], pd.DatetimeIndex] = {}
    for exchange in CN_EXCHANGES:
        path = paths.cn_roll_root / f"{exchange}_dominant.csv"
        if not path.is_file():
            continue
        events, exchange_calendars = _extract_roll_events(
            pd.read_csv(path, dtype=str),
            market="CN",
            exchange=exchange,
            product_column="product_id",
            date_column="trad_day",
            contract_column="instrument_id",
            roll_column="roll_flag",
            maturity_parser=_cn_delivery_month,
        )
        event_frames.append(events)
        calendars.update(exchange_calendars)
    for exchange, product in CME_PRODUCTS:
        path = paths.cme_roll_root / exchange / product / "dominant.csv"
        if not path.is_file():
            continue
        events, product_calendars = _extract_roll_events(
            pd.read_csv(path, dtype=str),
            market="CME",
            exchange=exchange,
            product_column="product",
            date_column="trading_day",
            contract_column="contract_id",
            roll_column="roll_flag",
            maturity_parser=_cme_delivery_month,
        )
        event_frames.append(events)
        calendars.update(product_calendars)
    if not event_frames:
        raise ValueError("no dominant tables were found")
    return pd.concat(event_frames, ignore_index=True), calendars


def select_events(
    events: pd.DataFrame,
    *,
    recent_start: pd.Timestamp | None = None,
    latest_per_product: bool = False,
    markets: tuple[str, ...] = ("CN", "CME"),
) -> pd.DataFrame:
    """Choose a declared scope; sampling happens only when explicitly requested."""

    selected = events[events["market"].isin(markets)].copy()
    if recent_start is not None:
        selected = selected[selected["event_date"].ge(pd.Timestamp(recent_start))]
    if latest_per_product:
        selected = selected.sort_values("event_date").groupby("product_key", as_index=False).tail(1)
    return selected.sort_values(["market", "exchange", "product", "event_date"]).reset_index(drop=True)


def plan_backtest_jobs(
    events: pd.DataFrame,
    calendars: dict[tuple[str, str, str], pd.DatetimeIndex],
    paths: StudyPaths,
    parameters: StudyParameters = StudyParameters(),
) -> pd.DataFrame:
    """Expand each event into the fixed pre-roll trading-day parquet paths."""

    rows: list[dict[str, object]] = []
    end_offset = 1 if parameters.include_roll_day else 0
    for event in events.itertuples(index=False):
        calendar = calendars.get((event.market, event.exchange, event.product))
        if calendar is None:
            continue
        location = calendar.get_indexer([event.event_date])
        if len(location) != 1 or location[0] < 0:
            continue
        root = paths.cn_backtest_root if event.market == "CN" else paths.cme_backtest_root
        for relative_day in range(-parameters.pre_roll_days, end_offset):
            day_location = int(location[0]) + relative_day
            if day_location < 0:
                continue
            trading_day = calendar[day_location]
            rows.append(
                {
                    "event_id": event.event_id,
                    "market": event.market,
                    "exchange": event.exchange,
                    "product": event.product,
                    "product_key": event.product_key,
                    "event_date": event.event_date,
                    "old_contract": event.old_contract,
                    "new_contract": event.new_contract,
                    "relative_day": relative_day,
                    "trading_day": trading_day,
                    "source_path": root / event.exchange / event.product / f"{trading_day:%Y%m%d}.parquet",
                }
            )
    return pd.DataFrame(
        rows,
        columns=[
            "event_id", "market", "exchange", "product", "product_key", "event_date",
            "old_contract", "new_contract", "relative_day", "trading_day", "source_path",
        ],
    )


def _read_pair_seconds(path: Path, old_contract: str, new_contract: str) -> pd.DataFrame:
    """Read BBO and inner-join the two fixed legs only at identical seconds."""

    table = pq.read_table(
        path,
        columns=["contract_id", "ts", "bid0p", "ask0p"],
        filters=[("contract_id", "in", [old_contract, new_contract])],
        use_threads=True,
    )
    quotes = table.to_pandas()
    if quotes.empty:
        return pd.DataFrame()
    if quotes.duplicated(["contract_id", "ts"]).any():
        raise ValueError(f"duplicate contract_id/ts in {path}")
    sides = []
    for prefix, contract in (("old", old_contract), ("new", new_contract)):
        side = quotes.loc[quotes["contract_id"].eq(contract), ["ts", "bid0p", "ask0p"]]
        sides.append(side.rename(columns={"bid0p": f"{prefix}_bid", "ask0p": f"{prefix}_ask"}))
    pair = sides[0].merge(sides[1], on="ts", how="inner", validate="one_to_one")
    if pair.empty:
        return pair
    valid = np.ones(len(pair), dtype=bool)
    for prefix in ("old", "new"):
        bid, ask = pair[f"{prefix}_bid"], pair[f"{prefix}_ask"]
        valid &= np.isfinite(bid) & np.isfinite(ask) & bid.gt(0) & ask.ge(bid)
    pair = pair.loc[valid].sort_values("ts").reset_index(drop=True)
    if pair.empty:
        return pair
    pair["old_mid"] = (pair["old_bid"] + pair["old_ask"]) / 2.0
    pair["new_mid"] = (pair["new_bid"] + pair["new_ask"]) / 2.0
    pair["mid_spread"] = pair["old_mid"] - pair["new_mid"]
    pair["mid_spread_bps"] = 10_000.0 * np.log(pair["old_mid"] / pair["new_mid"])
    pair["sell_old_buy_new_spread"] = pair["old_bid"] - pair["new_ask"]
    pair["buy_old_sell_new_spread"] = pair["old_ask"] - pair["new_bid"]
    return pair


def smooth_seconds_to_minutes(
    seconds: pd.DataFrame,
    parameters: StudyParameters = StudyParameters(),
) -> pd.DataFrame:
    """Trailing causal rolling mean, then the last valid value of each minute."""

    if seconds.empty:
        return pd.DataFrame()
    measures = [
        "mid_spread", "mid_spread_bps",
        "sell_old_buy_new_spread", "buy_old_sell_new_spread",
    ]
    indexed = seconds.set_index(pd.to_datetime(seconds["ts"], unit="s", utc=True))[measures]
    smoothed = indexed.rolling(
        f"{parameters.smoothing_seconds}s",
        min_periods=parameters.min_seconds_per_minute,
    ).mean()
    per_minute = smoothed.resample("1min").last()
    per_minute["observed_seconds"] = indexed["mid_spread"].resample("1min").count()
    per_minute["minute_ts"] = pd.Series(seconds["ts"].to_numpy(), index=indexed.index).resample("1min").last()
    per_minute = per_minute[per_minute["observed_seconds"].ge(parameters.min_seconds_per_minute)]
    per_minute = per_minute.dropna(subset=measures).reset_index(names="minute_utc")
    per_minute["minute_ts"] = per_minute["minute_ts"].astype("int64")
    return per_minute


def build_minute_spreads(
    jobs: pd.DataFrame,
    parameters: StudyParameters = StudyParameters(),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Produce smoothed minute spreads and an audit for every planned parquet."""

    minute_frames: list[pd.DataFrame] = []
    audit_rows: list[dict[str, object]] = []
    for job in jobs.itertuples(index=False):
        audit = {
            "event_id": job.event_id, "market": job.market, "exchange": job.exchange,
            "product": job.product, "event_date": job.event_date, "relative_day": job.relative_day,
            "trading_day": job.trading_day, "source_path": str(job.source_path),
        }
        if not job.source_path.is_file():
            audit_rows.append(audit | {"status": "missing_parquet", "pair_seconds": 0, "minute_rows": 0})
            continue
        quote_old_contract = _backtest_contract_id(job.old_contract, job.exchange)
        quote_new_contract = _backtest_contract_id(job.new_contract, job.exchange)
        seconds = _read_pair_seconds(job.source_path, quote_old_contract, quote_new_contract)
        if seconds.empty:
            audit_rows.append(audit | {"status": "missing_pair_quotes", "pair_seconds": 0, "minute_rows": 0})
            continue
        minutes = smooth_seconds_to_minutes(seconds, parameters)
        if minutes.empty:
            audit_rows.append(audit | {"status": "insufficient_minute_coverage", "pair_seconds": len(seconds), "minute_rows": 0})
            continue
        for column in (
            "event_id", "market", "exchange", "product", "product_key", "event_date",
            "old_contract", "new_contract", "relative_day", "trading_day",
        ):
            minutes[column] = getattr(job, column)
        minute_frames.append(minutes)
        audit_rows.append(audit | {"status": "ok", "pair_seconds": len(seconds), "minute_rows": len(minutes)})
    columns = [
        "event_id", "market", "exchange", "product", "product_key", "event_date", "old_contract", "new_contract",
        "relative_day", "trading_day", "minute_utc", "minute_ts", "observed_seconds", "mid_spread", "mid_spread_bps",
        "sell_old_buy_new_spread", "buy_old_sell_new_spread",
    ]
    minutes = pd.concat(minute_frames, ignore_index=True) if minute_frames else pd.DataFrame(columns=columns)
    return minutes[columns], pd.DataFrame(
        audit_rows,
        columns=[
            "event_id", "market", "exchange", "product", "event_date", "relative_day",
            "trading_day", "source_path", "status", "pair_seconds", "minute_rows",
        ],
    )


def summarize_event_trends(minutes: pd.DataFrame) -> pd.DataFrame:
    """Fit raw, state and momentum continuation statistics for every event."""

    rows: list[dict[str, object]] = []
    for event_id, group in minutes.groupby("event_id", sort=False):
        group = group.sort_values("minute_ts").reset_index(drop=True)
        values = group["mid_spread_bps"].to_numpy()
        if len(values) < 2:
            continue
        first = group.iloc[0]
        middle = len(values) // 2
        initial_state_sign = float(np.sign(values[0]))
        first_half_change = values[middle] - values[0]
        second_half_change = values[-1] - values[middle]
        momentum_sign = float(np.sign(first_half_change))
        raw_slope = np.polyfit(np.arange(len(values)), values, deg=1)[0] * 1_000.0
        rows.append(
            {
                "event_id": event_id, "market": first["market"], "exchange": first["exchange"],
                "product": first["product"], "product_key": first["product_key"], "event_date": first["event_date"],
                "old_contract": first["old_contract"], "new_contract": first["new_contract"], "minute_rows": len(group),
                "first_mid_spread_bps": values[0], "last_mid_spread_bps": values[-1],
                "change_mid_spread_bps": values[-1] - values[0],
                "slope_bps_per_1000_trading_minutes": raw_slope,
                "initial_state": (
                    "backwardation" if initial_state_sign > 0 else "contango" if initial_state_sign < 0 else "flat"
                ),
                "state_continuation_change_bps": (
                    initial_state_sign * (values[-1] - values[0]) if initial_state_sign else np.nan
                ),
                "state_continuation_slope_bps_per_1000_trading_minutes": (
                    initial_state_sign * raw_slope if initial_state_sign else np.nan
                ),
                "first_half_change_bps": first_half_change,
                "second_half_change_bps": second_half_change,
                "momentum_continuation_change_bps": (
                    momentum_sign * second_half_change if momentum_sign else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def summarize_market_trends(trends: pd.DataFrame) -> pd.DataFrame:
    """Equal-event descriptive summaries, including causal state and momentum tests."""

    rows: list[dict[str, object]] = []
    for market, group in trends.groupby("market", sort=True):
        state = group["state_continuation_change_bps"].dropna()
        momentum = group["momentum_continuation_change_bps"].dropna()
        rows.append(
            {
                "market": market,
                "events": len(group),
                "mean_raw_slope_bps_per_1000_trading_minutes": group[
                    "slope_bps_per_1000_trading_minutes"
                ].mean(),
                "median_raw_slope_bps_per_1000_trading_minutes": group[
                    "slope_bps_per_1000_trading_minutes"
                ].median(),
                "positive_raw_slope_share": (group["slope_bps_per_1000_trading_minutes"] > 0).mean(),
                "state_events": len(state),
                "mean_state_continuation_change_bps": state.mean(),
                "median_state_continuation_change_bps": state.median(),
                "positive_state_continuation_share": (state > 0).mean(),
                "momentum_events": len(momentum),
                "mean_momentum_continuation_change_bps": momentum.mean(),
                "median_momentum_continuation_change_bps": momentum.median(),
                "positive_momentum_continuation_share": (momentum > 0).mean(),
            }
        )
    return pd.DataFrame(rows)


def normalized_progress_path(minutes: pd.DataFrame, parameters: StudyParameters = StudyParameters()) -> pd.DataFrame:
    """Equal-weight paths interpolated onto an exact 0..100% event-progress grid."""

    event_bins: list[pd.DataFrame] = []
    progress_grid = np.linspace(0.0, 1.0, parameters.path_bins + 1)
    for event_id, group in minutes.groupby("event_id", sort=False):
        group = group.sort_values("minute_ts").copy()
        if len(group) < 2:
            continue
        progress = np.arange(len(group)) / (len(group) - 1)
        delta = group["mid_spread_bps"].to_numpy() - group["mid_spread_bps"].iloc[0]
        event_bins.append(
            pd.DataFrame(
                {
                    "event_id": event_id,
                    "market": group["market"].iloc[0],
                    "progress_bin": np.arange(parameters.path_bins + 1),
                    "delta_mid_spread_bps": np.interp(progress_grid, progress, delta),
                }
            )
        )
    if not event_bins:
        return pd.DataFrame(columns=["market", "progress_bin", "mean_delta_mid_spread_bps", "events"])
    return (
        pd.concat(event_bins, ignore_index=True)
        .groupby(["market", "progress_bin"], as_index=False)
        .agg(mean_delta_mid_spread_bps=("delta_mid_spread_bps", "mean"), events=("event_id", "nunique"))
    )


def render_progress_path(summary: pd.DataFrame, output_path: Path) -> None:
    figure, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True, constrained_layout=True)
    for axis, market in zip(axes, ("CN", "CME"), strict=True):
        selected = summary[summary["market"].eq(market)].sort_values("progress_bin")
        axis.plot(selected["progress_bin"], selected["mean_delta_mid_spread_bps"], color="#007A78", linewidth=1.7)
        axis.axhline(0, color="#4A4A4A", linewidth=1)
        axis.set_title(f"{market}: fixed-pair pre-roll minute path")
        axis.set_xlabel("Event progress (equal trading-minute bins)")
        axis.grid(axis="y", alpha=0.25)
    axes[0].set_ylabel("Mean change in mid spread (bp)")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def render_trend_distribution(trends: pd.DataFrame, output_path: Path) -> None:
    figure, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True, constrained_layout=True)
    for axis, market in zip(axes, ("CN", "CME"), strict=True):
        values = trends.loc[trends["market"].eq(market), "slope_bps_per_1000_trading_minutes"]
        axis.hist(values, bins=min(30, max(8, len(values) // 2)), color="#C23B22", alpha=0.85)
        axis.axvline(0, color="#4A4A4A", linewidth=1)
        axis.set_title(f"{market}: event trend distribution")
        axis.set_xlabel("Mid-spread slope (bp / 1,000 trading minutes)")
        axis.grid(axis="y", alpha=0.25)
    axes[0].set_ylabel("Events")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def run_high_frequency_study(
    paths: StudyPaths,
    output_dir: Path,
    *,
    recent_start: pd.Timestamp | None = None,
    latest_per_product: bool = False,
    parameters: StudyParameters = StudyParameters(),
) -> dict[str, pd.DataFrame]:
    """Run a declared scope and write minute-level research artifacts."""

    all_events, calendars = load_roll_events(paths)
    events = select_events(all_events, recent_start=recent_start, latest_per_product=latest_per_product)
    jobs = plan_backtest_jobs(events, calendars, paths, parameters)
    minutes, audit = build_minute_spreads(jobs, parameters)
    trends = summarize_event_trends(minutes)
    market_summary = summarize_market_trends(trends)
    progress = normalized_progress_path(minutes, parameters)
    output_dir.mkdir(parents=True, exist_ok=True)
    events.to_csv(output_dir / "events.csv", index=False)
    jobs.assign(source_path=jobs["source_path"].astype(str)).to_csv(output_dir / "planned_backtest_files.csv", index=False)
    minutes.to_parquet(output_dir / "minute_spreads.parquet", index=False)
    audit.to_csv(output_dir / "backtest_file_audit.csv", index=False)
    trends.to_csv(output_dir / "event_trends.csv", index=False)
    market_summary.to_csv(output_dir / "market_trend_summary.csv", index=False)
    progress.to_csv(output_dir / "normalized_progress_path.csv", index=False)
    render_progress_path(progress, output_dir / "minute_spread_path.png")
    render_trend_distribution(trends, output_dir / "event_trend_distribution.png")
    return {
        "events": events, "jobs": jobs, "minutes": minutes, "audit": audit,
        "trends": trends, "market_summary": market_summary, "progress": progress,
    }
