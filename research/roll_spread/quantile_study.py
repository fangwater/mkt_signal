"""CFFEX fixed-pair BBO spread distributions, aggregated from seconds to minutes and days."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from research.roll_spread.study import (
    StudyPaths,
    _backtest_contract_id,
    _read_pair_seconds,
    load_roll_events,
    plan_backtest_jobs,
    select_events,
)


SPREAD_MEASURES = (
    "mid_spread",
    "mid_spread_bps",
    "sell_old_buy_new_spread",
    "buy_old_sell_new_spread",
    "sell_old_buy_new_spread_bps",
    "buy_old_sell_new_spread_bps",
)

CFFEX_PRODUCT_NAMES = {
    "IC": "中证500股指期货",
    "IF": "沪深300股指期货",
    "IH": "上证50股指期货",
    "IM": "中证1000股指期货",
    "T": "10年期国债期货",
    "TF": "5年期国债期货",
    "TL": "30年期国债期货",
    "TS": "2年期国债期货",
}

DISTRIBUTION_STATISTICS = ("mean", "p10", "p20", "p50", "p80", "p90")


def _opposite_statistic(statistic: str) -> str:
    return {"mean": "mean", "p10": "p90", "p20": "p80", "p50": "p50", "p80": "p20", "p90": "p10"}[statistic]


@dataclass(frozen=True)
class QuantileParameters:
    """Parameters for causal second-to-minute aggregation, not a rolling smoother."""

    pre_roll_days: int = 10
    include_roll_day: bool = False
    min_seconds_per_minute: int = 30
    quantiles: tuple[float, ...] = (0.10, 0.20, 0.50, 0.80, 0.90)

    def __post_init__(self) -> None:
        if self.pre_roll_days <= 0:
            raise ValueError("pre_roll_days must be positive")
        if self.min_seconds_per_minute <= 0:
            raise ValueError("min_seconds_per_minute must be positive")
        if not self.quantiles or any(not 0 < value < 1 for value in self.quantiles):
            raise ValueError("quantiles must be inside (0, 1)")
        if tuple(sorted(set(self.quantiles))) != self.quantiles:
            raise ValueError("quantiles must be sorted and unique")


def _quantile_label(value: float) -> str:
    return f"p{int(round(value * 100)):02d}"


def _distribution_columns(measures: Iterable[str], parameters: QuantileParameters) -> list[str]:
    columns: list[str] = []
    for measure in measures:
        columns.append(f"{measure}_mean")
        columns.extend(f"{measure}_{_quantile_label(quantile)}" for quantile in parameters.quantiles)
    return columns


def minute_spread_distribution(
    seconds: pd.DataFrame,
    parameters: QuantileParameters = QuantileParameters(),
) -> pd.DataFrame:
    """Save the empirical distribution of each valid same-second spread within every minute."""

    if seconds.empty:
        return pd.DataFrame()
    seconds = seconds.copy()
    seconds["minute_utc"] = pd.to_datetime(seconds["ts"], unit="s", utc=True).dt.floor("min")
    grouped = seconds.groupby("minute_utc", sort=True)
    result = grouped["ts"].agg(pair_seconds="size", minute_ts="max").reset_index()
    for measure in SPREAD_MEASURES:
        means = grouped[measure].mean().rename(f"{measure}_mean").reset_index()
        quantiles = grouped[measure].quantile(parameters.quantiles).unstack()
        quantiles.columns = [f"{measure}_{_quantile_label(value)}" for value in quantiles.columns]
        result = result.merge(means, on="minute_utc", validate="one_to_one")
        result = result.merge(quantiles.reset_index(), on="minute_utc", validate="one_to_one")
    result = result.loc[result["pair_seconds"].ge(parameters.min_seconds_per_minute)].copy()
    result["minute_ts"] = result["minute_ts"].astype("int64")
    return result.reset_index(drop=True)


def daily_spread_distribution(
    seconds: pd.DataFrame,
    parameters: QuantileParameters = QuantileParameters(),
) -> dict[str, float | int]:
    """Compute one trading-day distribution from all paired seconds in that parquet day."""

    if seconds.empty:
        return {"pair_seconds": 0}
    result: dict[str, float | int] = {
        "pair_seconds": len(seconds),
        "first_ts": int(seconds["ts"].iloc[0]),
        "last_ts": int(seconds["ts"].iloc[-1]),
    }
    for measure in SPREAD_MEASURES:
        values = seconds[measure]
        result[f"{measure}_mean"] = float(values.mean())
        for quantile in parameters.quantiles:
            result[f"{measure}_{_quantile_label(quantile)}"] = float(values.quantile(quantile))
    return result


def _add_executable_bps(seconds: pd.DataFrame) -> pd.DataFrame:
    """Express both executable BBO directions in bp so products can be compared."""

    seconds = seconds.copy()
    seconds["sell_old_buy_new_spread_bps"] = 10_000.0 * np.log(seconds["old_bid"] / seconds["new_ask"])
    seconds["buy_old_sell_new_spread_bps"] = 10_000.0 * np.log(seconds["old_ask"] / seconds["new_bid"])
    return seconds


def build_quantile_distributions(
    jobs: pd.DataFrame,
    parameters: QuantileParameters = QuantileParameters(),
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read each fixed pair once and emit both minute and trading-day distributions."""

    minute_frames: list[pd.DataFrame] = []
    daily_rows: list[dict[str, object]] = []
    audit_rows: list[dict[str, object]] = []
    metadata_columns = (
        "event_id", "market", "exchange", "product", "product_key", "event_date",
        "old_contract", "new_contract", "relative_day", "trading_day",
    )
    for job in jobs.itertuples(index=False):
        audit: dict[str, object] = {
            "event_id": job.event_id, "market": job.market, "exchange": job.exchange,
            "product": job.product, "event_date": job.event_date, "relative_day": job.relative_day,
            "trading_day": job.trading_day, "source_path": str(job.source_path),
        }
        if not job.source_path.is_file():
            audit_rows.append(audit | {"status": "missing_parquet", "pair_seconds": 0, "minute_rows": 0})
            continue
        quote_old = _backtest_contract_id(job.old_contract, job.exchange)
        quote_new = _backtest_contract_id(job.new_contract, job.exchange)
        seconds = _read_pair_seconds(job.source_path, quote_old, quote_new)
        if seconds.empty:
            audit_rows.append(audit | {"status": "missing_pair_quotes", "pair_seconds": 0, "minute_rows": 0})
            continue
        seconds = _add_executable_bps(seconds)
        minutes = minute_spread_distribution(seconds, parameters)
        if minutes.empty:
            audit_rows.append(audit | {"status": "insufficient_minute_coverage", "pair_seconds": len(seconds), "minute_rows": 0})
            continue
        metadata = {name: getattr(job, name) for name in metadata_columns}
        for name, value in metadata.items():
            minutes[name] = value
        minute_frames.append(minutes)
        daily_rows.append(metadata | daily_spread_distribution(seconds, parameters))
        audit_rows.append(audit | {"status": "ok", "pair_seconds": len(seconds), "minute_rows": len(minutes)})

    distribution_columns = _distribution_columns(SPREAD_MEASURES, parameters)
    minute_columns = [
        *metadata_columns, "minute_utc", "minute_ts", "pair_seconds", *distribution_columns,
    ]
    daily_columns = [
        *metadata_columns, "pair_seconds", "first_ts", "last_ts", *distribution_columns,
    ]
    minutes = pd.concat(minute_frames, ignore_index=True) if minute_frames else pd.DataFrame(columns=minute_columns)
    daily = pd.DataFrame(daily_rows, columns=daily_columns)
    audit = pd.DataFrame(
        audit_rows,
        columns=[
            "event_id", "market", "exchange", "product", "event_date", "relative_day",
            "trading_day", "source_path", "status", "pair_seconds", "minute_rows",
        ],
    )
    return minutes[minute_columns], daily[daily_columns], audit


def event_daily_quantile_trends(daily: pd.DataFrame) -> pd.DataFrame:
    """Measure raw, state and first-half-momentum continuation for every daily quantile path."""

    rows: list[dict[str, object]] = []
    statistics = DISTRIBUTION_STATISTICS
    for event_id, group in daily.groupby("event_id", sort=False):
        group = group.sort_values("relative_day").reset_index(drop=True)
        if len(group) < 2:
            continue
        midpoint = len(group) // 2
        first = group.iloc[0]
        for statistic in statistics:
            values = group[f"mid_spread_bps_{statistic}"].to_numpy(dtype=float)
            initial_sign = float(np.sign(values[0]))
            first_half_change = values[midpoint] - values[0]
            second_half_change = values[-1] - values[midpoint]
            momentum_sign = float(np.sign(first_half_change))
            slope = np.polyfit(group["relative_day"], values, deg=1)[0]
            rows.append(
                {
                    "event_id": event_id, "market": first["market"], "exchange": first["exchange"],
                    "product": first["product"], "product_key": first["product_key"],
                    "event_date": first["event_date"], "old_contract": first["old_contract"],
                    "new_contract": first["new_contract"], "statistic": statistic, "trading_days": len(group),
                    "first_value_bps": values[0], "last_value_bps": values[-1],
                    "change_bps": values[-1] - values[0], "slope_bps_per_trading_day": slope,
                    "initial_state": "backwardation" if initial_sign > 0 else "contango" if initial_sign < 0 else "flat",
                    "state_continuation_bps": initial_sign * (values[-1] - values[0]) if initial_sign else np.nan,
                    "first_half_change_bps": first_half_change,
                    "second_half_change_bps": second_half_change,
                    "momentum_continuation_bps": momentum_sign * second_half_change if momentum_sign else np.nan,
                }
            )
    return pd.DataFrame(rows)


def product_quantile_summary(trends: pd.DataFrame) -> pd.DataFrame:
    """Identify products whose daily quantile paths differ from the CFFEX aggregate."""

    rows: list[dict[str, object]] = []
    for (product, statistic), group in trends.groupby(["product", "statistic"], sort=True):
        state = group["state_continuation_bps"].dropna()
        momentum = group["momentum_continuation_bps"].dropna()
        rows.append(
            {
                "product": product, "product_name": CFFEX_PRODUCT_NAMES.get(product, product),
                "statistic": statistic, "events": len(group),
                "mean_change_bps": group["change_bps"].mean(),
                "median_change_bps": group["change_bps"].median(),
                "mean_slope_bps_per_trading_day": group["slope_bps_per_trading_day"].mean(),
                "median_slope_bps_per_trading_day": group["slope_bps_per_trading_day"].median(),
                "positive_slope_share": (group["slope_bps_per_trading_day"] > 0).mean(),
                "mean_state_continuation_bps": state.mean(),
                "median_state_continuation_bps": state.median(),
                "positive_state_continuation_share": (state > 0).mean(),
                "mean_momentum_continuation_bps": momentum.mean(),
                "median_momentum_continuation_bps": momentum.median(),
                "positive_momentum_continuation_share": (momentum > 0).mean(),
            }
        )
    return pd.DataFrame(rows)


def event_execution_screen(daily: pd.DataFrame) -> pd.DataFrame:
    """Screen first-half momentum against later p50 two-leg BBO, without claiming a fill backtest."""

    rows: list[dict[str, object]] = []
    for event_id, group in daily.groupby("event_id", sort=False):
        group = group.sort_values("relative_day").reset_index(drop=True)
        if len(group) < 2:
            continue
        midpoint = len(group) // 2
        first, entry, exit_quote = group.iloc[0], group.iloc[midpoint], group.iloc[-1]
        signal = float(np.sign(entry["mid_spread_bps_p50"] - first["mid_spread_bps_p50"]))
        if signal > 0:
            entry_bps = entry["buy_old_sell_new_spread_bps_p50"]
            exit_bps = exit_quote["sell_old_buy_new_spread_bps_p50"]
            gross_screen_bps = exit_bps - entry_bps
            direction = "long_old_short_new"
        elif signal < 0:
            entry_bps = entry["sell_old_buy_new_spread_bps_p50"]
            exit_bps = exit_quote["buy_old_sell_new_spread_bps_p50"]
            gross_screen_bps = entry_bps - exit_bps
            direction = "short_old_long_new"
        else:
            entry_bps = np.nan
            exit_bps = np.nan
            gross_screen_bps = np.nan
            direction = "flat"
        rows.append(
            {
                "event_id": event_id, "market": first["market"], "exchange": first["exchange"],
                "product": first["product"], "event_date": first["event_date"],
                "old_contract": first["old_contract"], "new_contract": first["new_contract"],
                "signal_direction": direction, "entry_relative_day": entry["relative_day"],
                "exit_relative_day": exit_quote["relative_day"], "entry_p50_bbo_spread_bps": entry_bps,
                "exit_p50_bbo_spread_bps": exit_bps, "gross_p50_bbo_screen_bps": gross_screen_bps,
            }
        )
    return pd.DataFrame(rows)


def product_execution_summary(screen: pd.DataFrame) -> pd.DataFrame:
    """Product-level descriptive screen for the p50 BBO direction selected at the midpoint."""

    rows: list[dict[str, object]] = []
    for product, group in screen.groupby("product", sort=True):
        values = group["gross_p50_bbo_screen_bps"].dropna()
        rows.append(
            {
                "product": product, "product_name": CFFEX_PRODUCT_NAMES.get(product, product),
                "events": len(group), "signalled_events": len(values),
                "mean_gross_p50_bbo_screen_bps": values.mean(),
                "median_gross_p50_bbo_screen_bps": values.median(),
                "positive_gross_p50_bbo_screen_share": (values > 0).mean(),
            }
        )
    return pd.DataFrame(rows)


def add_directional_mark_columns(daily: pd.DataFrame) -> pd.DataFrame:
    """Create comparable long and short liquidation marks from their respective BBO sides."""

    result = daily.copy()
    for statistic in DISTRIBUTION_STATISTICS:
        result[f"long_old_short_new_mark_bps_{statistic}"] = result[
            f"sell_old_buy_new_spread_bps_{statistic}"
        ]
        opposite = _opposite_statistic(statistic)
        result[f"short_old_long_new_mark_bps_{statistic}"] = -result[
            f"buy_old_sell_new_spread_bps_{opposite}"
        ]
    return result


def normalized_daily_statistic_path(daily: pd.DataFrame, measure_prefix: str = "mid_spread_bps") -> pd.DataFrame:
    """Equal-event daily paths after subtracting each event's own first trading day."""

    rows: list[dict[str, object]] = []
    statistics = DISTRIBUTION_STATISTICS
    for event_id, group in daily.groupby("event_id", sort=False):
        group = group.sort_values("relative_day")
        for statistic in statistics:
            values = group[f"{measure_prefix}_{statistic}"].to_numpy(dtype=float)
            for relative_day, value in zip(group["relative_day"], values, strict=True):
                rows.append(
                    {
                        "event_id": event_id, "statistic": statistic, "relative_day": relative_day,
                        "delta_bps": value - values[0],
                    }
                )
    if not rows:
        return pd.DataFrame(columns=["statistic", "relative_day", "mean_delta_bps", "median_delta_bps", "events"])
    return (
        pd.DataFrame(rows)
        .groupby(["statistic", "relative_day"], as_index=False)
        .agg(
            mean_delta_bps=("delta_bps", "mean"),
            median_delta_bps=("delta_bps", "median"),
            events=("event_id", "nunique"),
        )
    )


def render_daily_statistic_panels(path: pd.DataFrame, output_path: Path, title: str) -> None:
    """Render independent small daily-path panels for mean and the five requested quantiles."""

    figure, axes = plt.subplots(2, 3, figsize=(7.2, 4.7), constrained_layout=True)
    figure.suptitle(title, fontsize=13)
    colors = {"mean": "#264653", "p10": "#457B9D", "p20": "#2A9D8F", "p50": "#1D3557", "p80": "#E76F51", "p90": "#9B2226"}
    relative_days = sorted(path["relative_day"].unique())
    for axis, statistic in zip(axes.flat, DISTRIBUTION_STATISTICS, strict=True):
        selected = path[path["statistic"].eq(statistic)].sort_values("relative_day")
        axis.plot(selected["relative_day"], selected["mean_delta_bps"], color=colors[statistic], linewidth=1.7)
        axis.axhline(0, color="#4A4A4A", linewidth=0.8)
        axis.set_title(statistic.upper())
        axis.set_xticks(relative_days[::2])
        axis.grid(axis="y", alpha=0.25)
    for axis in axes[1, :]:
        axis.set_xlabel("Relative trading day")
    for axis in axes[:, 0]:
        axis.set_ylabel("Mean delta (bp)")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def render_product_mid_spread_rolls(daily: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    """Render small per-product panels with every roll event overlaid and explicitly labelled."""

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_rows: list[dict[str, object]] = []
    for product, product_daily in daily.groupby("product", sort=True):
        events = [
            group.sort_values("relative_day")
            for _, group in product_daily.groupby("event_id", sort=False)
        ]
        figure, axes = plt.subplots(2, 3, figsize=(5.8, 4.1), constrained_layout=True)
        figure.suptitle(f"{product}: fixed-pair daily mid-spread rolls", fontsize=10)
        colors = plt.get_cmap("tab10")(np.arange(len(events)))
        labels = [
            f"{event['event_date'].iloc[0]:%Y-%m-%d}: {event['old_contract'].iloc[0]} -> {event['new_contract'].iloc[0]}"
            for event in events
        ]
        for axis, statistic in zip(axes.flat, DISTRIBUTION_STATISTICS, strict=True):
            for event, color, label in zip(events, colors, labels, strict=True):
                values = event[f"mid_spread_bps_{statistic}"].to_numpy(dtype=float)
                axis.plot(event["relative_day"], values - values[0], color=color, linewidth=1.1, label=label)
            axis.axhline(0, color="#4A4A4A", linewidth=0.7)
            axis.set_title(statistic.upper(), fontsize=9)
            axis.set_xticks([-10, -8, -6, -4, -2])
            axis.tick_params(labelsize=7)
            axis.grid(axis="y", alpha=0.25)
        axes[0, 0].legend(fontsize=5.6, frameon=False, loc="best")
        for axis in axes[1, :]:
            axis.set_xlabel("Relative day", fontsize=7)
        for axis in axes[:, 0]:
            axis.set_ylabel("Delta bp", fontsize=7)
        output_path = output_dir / f"ccfx_{product}_mid_spread_rolls.png"
        figure.savefig(output_path, dpi=160)
        plt.close(figure)
        manifest_rows.append(
            {
                "product": product,
                "product_name": CFFEX_PRODUCT_NAMES.get(product, product),
                "events": len(events),
                "image_file": output_path.name,
                "rolls": " | ".join(labels),
            }
        )
    return pd.DataFrame(manifest_rows)


def directional_execution_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Compare fixed long-spread and short-spread p-quantile BBO screens after the midpoint."""

    rows: list[dict[str, object]] = []
    for event_id, group in daily.groupby("event_id", sort=False):
        group = group.sort_values("relative_day").reset_index(drop=True)
        if len(group) < 2:
            continue
        entry, exit_quote = group.iloc[len(group) // 2], group.iloc[-1]
        for statistic in DISTRIBUTION_STATISTICS:
            long_gross = (
                exit_quote[f"sell_old_buy_new_spread_bps_{statistic}"]
                - entry[f"buy_old_sell_new_spread_bps_{statistic}"]
            )
            short_gross = (
                entry[f"sell_old_buy_new_spread_bps_{statistic}"]
                - exit_quote[f"buy_old_sell_new_spread_bps_{statistic}"]
            )
            rows.append(
                {
                    "event_id": event_id, "product": entry["product"],
                    "product_name": CFFEX_PRODUCT_NAMES.get(entry["product"], entry["product"]),
                    "event_date": entry["event_date"], "statistic": statistic,
                    "entry_relative_day": entry["relative_day"], "exit_relative_day": exit_quote["relative_day"],
                    "long_old_short_new_gross_bps": long_gross,
                    "short_old_long_new_gross_bps": short_gross,
                }
            )
    return pd.DataFrame(rows)


def product_directional_execution_summary(screen: pd.DataFrame) -> pd.DataFrame:
    """Summarize both trade directions for every product and within-day statistic."""

    rows: list[dict[str, object]] = []
    for (product, statistic), group in screen.groupby(["product", "statistic"], sort=True):
        long_values = group["long_old_short_new_gross_bps"]
        short_values = group["short_old_long_new_gross_bps"]
        rows.append(
            {
                "product": product, "product_name": CFFEX_PRODUCT_NAMES.get(product, product),
                "statistic": statistic, "events": len(group),
                "median_long_old_short_new_gross_bps": long_values.median(),
                "positive_long_old_short_new_share": (long_values > 0).mean(),
                "median_short_old_long_new_gross_bps": short_values.median(),
                "positive_short_old_long_new_share": (short_values > 0).mean(),
            }
        )
    return pd.DataFrame(rows)


def run_ccfx_quantile_study(
    paths: StudyPaths,
    output_dir: Path,
    *,
    recent_start: pd.Timestamp = pd.Timestamp("2026-01-01"),
    parameters: QuantileParameters = QuantileParameters(),
) -> dict[str, pd.DataFrame]:
    """Run the CFFEX-only daily quantile study and persist every aggregation layer."""

    events, calendars = load_roll_events(paths)
    events = select_events(events, recent_start=recent_start)
    events = events.loc[(events["market"].eq("CN")) & (events["exchange"].eq("ccfx"))].copy()
    jobs = plan_backtest_jobs(events, calendars, paths, parameters)  # Duck types the shared pre-roll fields.
    minutes, daily, audit = build_quantile_distributions(jobs, parameters)
    daily = add_directional_mark_columns(daily)
    trends = event_daily_quantile_trends(daily)
    product_summary = product_quantile_summary(trends)
    execution_screen = event_execution_screen(daily)
    execution_summary = product_execution_summary(execution_screen)
    mid_path = normalized_daily_statistic_path(daily)
    long_path = normalized_daily_statistic_path(daily, "long_old_short_new_mark_bps")
    short_path = normalized_daily_statistic_path(daily, "short_old_long_new_mark_bps")
    directional_screen = directional_execution_summary(daily)
    directional_summary = product_directional_execution_summary(directional_screen)
    product_roll_manifest = render_product_mid_spread_rolls(daily, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    events.to_csv(output_dir / "events.csv", index=False)
    jobs.assign(source_path=jobs["source_path"].astype(str)).to_csv(output_dir / "planned_backtest_files.csv", index=False)
    minutes.to_parquet(output_dir / "minute_spread_distribution.parquet", index=False)
    daily.to_csv(output_dir / "daily_spread_distribution.csv", index=False)
    audit.to_csv(output_dir / "backtest_file_audit.csv", index=False)
    trends.to_csv(output_dir / "event_daily_quantile_trends.csv", index=False)
    product_summary.to_csv(output_dir / "product_quantile_summary.csv", index=False)
    execution_screen.to_csv(output_dir / "event_execution_screen.csv", index=False)
    execution_summary.to_csv(output_dir / "product_execution_summary.csv", index=False)
    mid_path.to_csv(output_dir / "daily_mid_spread_path.csv", index=False)
    long_path.to_csv(output_dir / "daily_long_old_short_new_path.csv", index=False)
    short_path.to_csv(output_dir / "daily_short_old_long_new_path.csv", index=False)
    directional_screen.to_csv(output_dir / "event_directional_execution_screen.csv", index=False)
    directional_summary.to_csv(output_dir / "product_directional_execution_summary.csv", index=False)
    product_roll_manifest.to_csv(output_dir / "product_mid_spread_roll_manifest.csv", index=False)
    render_daily_statistic_panels(mid_path, output_dir / "ccfx_mid_spread_panels.png", "Mid spread: old contract minus new contract")
    render_daily_statistic_panels(long_path, output_dir / "ccfx_long_old_short_new_panels.png", "Long old / short new: liquidation BBO mark")
    render_daily_statistic_panels(short_path, output_dir / "ccfx_short_old_long_new_panels.png", "Short old / long new: negative cover BBO mark")
    return {
        "events": events, "jobs": jobs, "minutes": minutes, "daily": daily, "audit": audit,
        "trends": trends, "product_summary": product_summary, "execution_screen": execution_screen,
        "execution_summary": execution_summary, "mid_path": mid_path, "long_path": long_path,
        "short_path": short_path, "directional_screen": directional_screen,
        "directional_summary": directional_summary, "product_roll_manifest": product_roll_manifest,
    }
