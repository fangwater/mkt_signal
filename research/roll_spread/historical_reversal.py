"""Historical fixed-pair roll-spread reversal study from causal 1-second BBO.

The event date and fixed old/new pair come from the dominant-contract switch
table.  Each pre-roll day is reduced to a daily mean mid spread and the final
observed two-leg BBO.  This deliberately keeps the historical study small:
there is no HFQ, continuous price, or intraday quantile export here.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from math import ceil, comb
from pathlib import Path
import zlib

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from research.roll_spread.study import (
    StudyParameters,
    StudyPaths,
    _backtest_contract_id,
    _read_pair_seconds,
    load_roll_events,
    plan_backtest_jobs,
    select_events,
)


# Products existed in the 2020--present history and have a liquid main-contract
# sequence. Newer products with materially shorter histories stay out of this
# pre-specified universe rather than being compared as though they had six years.
HISTORICAL_CN_UNIVERSE: dict[str, tuple[str, ...]] = {
    "ccfx": ("IC", "IF", "IH", "IM", "T", "TF", "TL", "TS"),
    "xsge": ("AG", "AL", "AU", "BU", "CU", "FU", "HC", "NI", "PB", "RB", "RU", "SN", "SP", "SS", "ZN"),
    "xsie": ("BC", "LU", "NR", "SC"),
}

CN_PRODUCT_NAMES = {
    "IC": "中证500股指期货", "IF": "沪深300股指期货", "IH": "上证50股指期货",
    "IM": "中证1000股指期货", "T": "10年期国债期货", "TF": "5年期国债期货",
    "TL": "30年期国债期货", "TS": "2年期国债期货",
    "AG": "白银期货", "AL": "铝期货", "AU": "黄金期货", "BU": "沥青期货",
    "CU": "铜期货", "FU": "燃料油期货", "HC": "热轧卷板期货", "NI": "镍期货",
    "PB": "铅期货", "RB": "螺纹钢期货", "RU": "天然橡胶期货", "SN": "锡期货",
    "SP": "纸浆期货", "SS": "不锈钢期货", "ZN": "锌期货",
    "BC": "国际铜期货", "LU": "低硫燃料油期货", "NR": "20号胶期货", "SC": "原油期货",
}

# Continuation / reversal only; validated as a two-slot categorical pair.
PATH_CONTINUATION_COLOR = "#2a78d6"
PATH_REVERSAL_COLOR = "#eb6834"
PATH_AXIS_COLOR = "#52514e"
PATH_GRID_COLUMNS = 8


@dataclass(frozen=True)
class HistoricalParameters:
    """Causal pre-roll signal and execution choices shared by every product."""

    pre_roll_days: int = 10
    signal_relative_day: int = -6
    exit_relative_day: int = -1
    min_pair_seconds_per_day: int = 60

    def __post_init__(self) -> None:
        if self.pre_roll_days != 10:
            raise ValueError("this study defines its two five-day windows using pre_roll_days=10")
        if self.signal_relative_day != -6 or self.exit_relative_day != -1:
            raise ValueError("this study defines the causal signal at t=-6 and exit at t=-1")
        if self.min_pair_seconds_per_day <= 0:
            raise ValueError("min_pair_seconds_per_day must be positive")


def select_historical_cn_events(events: pd.DataFrame, start: str | pd.Timestamp = "2020-01-01") -> pd.DataFrame:
    """Select the declared liquid CN universe, preserving every qualifying roll event."""

    selected = select_events(events, recent_start=pd.Timestamp(start), markets=("CN",))
    allowed = pd.Series(
        [
            (exchange, product)
            for exchange, products in HISTORICAL_CN_UNIVERSE.items()
            for product in products
        ],
        dtype="object",
    )
    keys = pd.MultiIndex.from_frame(selected[["exchange", "product"]])
    selected = selected[keys.isin(pd.MultiIndex.from_tuples(allowed.tolist()))].copy()
    return selected.sort_values(["exchange", "product", "event_date"]).reset_index(drop=True)


def _daily_mark_from_seconds(seconds: pd.DataFrame) -> dict[str, float | int]:
    """Summarize one trading day and retain its last synchronized executable BBO."""

    last = seconds.iloc[-1]
    return {
        "pair_seconds": len(seconds),
        "last_pair_ts": int(last["ts"]),
        "daily_mid_mean_bps": float(seconds["mid_spread_bps"].mean()),
        # A long old/short new position is opened by buying old and selling new,
        # then liquidated by selling old and buying new.
        "long_entry_cost_bps": float(10_000.0 * np.log(last["old_ask"] / last["new_bid"])),
        "long_liquidation_bps": float(10_000.0 * np.log(last["old_bid"] / last["new_ask"])),
    }


def _summarize_job(job: object, parameters: HistoricalParameters) -> tuple[dict[str, object] | None, dict[str, object]]:
    """Read one parquet safely so a single malformed day remains visible in the audit."""

    base = {
        name: getattr(job, name)
        for name in (
            "event_id", "market", "exchange", "product", "product_key", "event_date",
            "old_contract", "new_contract", "relative_day", "trading_day",
        )
    }
    audit = base | {"source_path": str(job.source_path)}
    if not job.source_path.is_file():
        return None, audit | {"status": "missing_parquet", "pair_seconds": 0}
    try:
        seconds = _read_pair_seconds(
            job.source_path,
            _backtest_contract_id(job.old_contract, job.exchange),
            _backtest_contract_id(job.new_contract, job.exchange),
        )
    except Exception as error:  # Keep a full historical run auditable despite isolated bad files.
        return None, audit | {"status": f"read_error:{type(error).__name__}", "pair_seconds": 0}
    if len(seconds) < parameters.min_pair_seconds_per_day:
        return None, audit | {"status": "insufficient_pair_seconds", "pair_seconds": len(seconds)}
    mark = base | _daily_mark_from_seconds(seconds)
    return mark, audit | {"status": "ok", "pair_seconds": len(seconds)}


def build_historical_daily_marks(
    jobs: pd.DataFrame,
    parameters: HistoricalParameters = HistoricalParameters(),
    workers: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reduce each planned fixed-pair day to one mid mark plus its final BBO mark."""

    iterator = jobs.itertuples(index=False)
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            outcomes = list(executor.map(lambda job: _summarize_job(job, parameters), iterator))
    else:
        outcomes = [_summarize_job(job, parameters) for job in iterator]
    marks = [mark for mark, _ in outcomes if mark is not None]
    audit = [row for _, row in outcomes]
    mark_columns = [
        "event_id", "market", "exchange", "product", "product_key", "event_date", "old_contract", "new_contract",
        "relative_day", "trading_day", "pair_seconds", "last_pair_ts", "daily_mid_mean_bps",
        "long_entry_cost_bps", "long_liquidation_bps",
    ]
    audit_columns = mark_columns[:10] + ["source_path", "status", "pair_seconds"]
    daily = pd.DataFrame(marks, columns=mark_columns).sort_values(
        ["exchange", "product", "event_date", "relative_day"]
    ).reset_index(drop=True)
    return daily, pd.DataFrame(audit, columns=audit_columns)


def _binomial_two_sided(k: int, n: int) -> float:
    """Exact two-sided sign/binomial test for p=0.5 without a SciPy dependency."""

    if n <= 0:
        return np.nan
    tail = sum(comb(n, value) for value in range(0, min(k, n - k) + 1)) / (2 ** n)
    return min(1.0, 2.0 * tail)


def _bh_qvalues(values: pd.Series) -> pd.Series:
    """Benjamini-Hochberg FDR adjusted p-values, retaining missing values."""

    result = pd.Series(np.nan, index=values.index, dtype=float)
    valid = values.dropna().sort_values()
    if valid.empty:
        return result
    total = len(valid)
    adjusted = np.empty(total, dtype=float)
    running = 1.0
    for position in range(total - 1, -1, -1):
        running = min(running, valid.iloc[position] * total / (position + 1))
        adjusted[position] = running
    result.loc[valid.index] = adjusted
    return result


def _sign_flip_mean_pvalue(values: pd.Series, simulations: int = 100_000) -> float:
    """Two-sided sign-flip test of the aligned late-window mean, with a stable seed.

    The direction-count test asks whether reversals are more common. This second
    statistic asks whether their average *magnitude* departs from zero. The
    null is sign symmetry of event-level late moves, and the fixed seed makes
    the Monte Carlo estimate reproducible in the notebook and command line.
    """

    sample = values.dropna().to_numpy(dtype=float)
    if len(sample) == 0:
        return np.nan
    observed = abs(float(sample.mean()))
    seed = zlib.crc32(sample.tobytes())
    generator = np.random.default_rng(seed)
    exceedances = 0
    remaining = simulations
    while remaining:
        batch = min(10_000, remaining)
        signs = generator.integers(0, 2, size=(batch, len(sample)), dtype=np.int8) * 2 - 1
        exceedances += int((np.abs((signs * sample).mean(axis=1)) >= observed).sum())
        remaining -= batch
    return (1 + exceedances) / (1 + simulations)


def build_event_reversal_statistics(
    daily: pd.DataFrame,
    parameters: HistoricalParameters = HistoricalParameters(),
) -> pd.DataFrame:
    """Measure whether the early five-day mid move continues or reverses late in the roll."""

    rows: list[dict[str, object]] = []
    required_days = set(range(-parameters.pre_roll_days, parameters.exit_relative_day + 1))
    for event_id, group in daily.groupby("event_id", sort=False):
        group = group.sort_values("relative_day").drop_duplicates("relative_day", keep="last")
        if set(group["relative_day"]) != required_days:
            continue
        indexed = group.set_index("relative_day")
        first, entry, exit_day = (
            indexed.loc[-parameters.pre_roll_days],
            indexed.loc[parameters.signal_relative_day],
            indexed.loc[parameters.exit_relative_day],
        )
        early_change = float(entry["daily_mid_mean_bps"] - first["daily_mid_mean_bps"])
        late_change = float(exit_day["daily_mid_mean_bps"] - entry["daily_mid_mean_bps"])
        early_sign = int(np.sign(early_change))
        late_sign = int(np.sign(late_change))
        if early_sign == 0 or late_sign == 0:
            outcome = "flat"
        elif early_sign == late_sign:
            outcome = "continuation"
        else:
            outcome = "reversal"
        if early_sign > 0:
            direction = "多旧空新"
            follow_pnl = float(exit_day["long_liquidation_bps"] - entry["long_entry_cost_bps"])
            contrarian_pnl = float(entry["long_liquidation_bps"] - exit_day["long_entry_cost_bps"])
        elif early_sign < 0:
            direction = "空旧多新"
            follow_pnl = float(entry["long_liquidation_bps"] - exit_day["long_entry_cost_bps"])
            contrarian_pnl = float(exit_day["long_liquidation_bps"] - entry["long_entry_cost_bps"])
        else:
            direction, follow_pnl, contrarian_pnl = "无信号", np.nan, np.nan
        rows.append(
            {
                "event_id": event_id,
                "market": first["market"], "exchange": first["exchange"], "product": first["product"],
                "product_name": CN_PRODUCT_NAMES.get(first["product"], first["product"]),
                "event_date": first["event_date"], "old_contract": first["old_contract"], "new_contract": first["new_contract"],
                "early_mid_change_bps_t10_to_t6": early_change,
                "late_mid_change_bps_t6_to_t1": late_change,
                "aligned_late_mid_change_bps": early_sign * late_change if early_sign else np.nan,
                "early_direction": direction, "outcome": outcome,
                "follow_trend_gross_bbo_pnl_bps": follow_pnl,
                "contrarian_gross_bbo_pnl_bps": contrarian_pnl,
            }
        )
    return pd.DataFrame(rows)


def _summarize_event_group(group: pd.DataFrame) -> dict[str, object]:
    """Return a product or exchange level reversal and BBO opportunity summary."""

    directional = group[group["outcome"].isin(["reversal", "continuation"])].copy()
    total = len(group)
    eligible = len(directional)
    reversals = int(directional["outcome"].eq("reversal").sum())
    continuations = int(directional["outcome"].eq("continuation").sum())
    follow = group["follow_trend_gross_bbo_pnl_bps"].dropna()
    contra = group["contrarian_gross_bbo_pnl_bps"].dropna()
    return {
        "events_complete": total,
        "directional_events": eligible,
        "reversal_events": reversals,
        "continuation_events": continuations,
        "reversal_rate": reversals / eligible if eligible else np.nan,
        "reversal_pvalue": _binomial_two_sided(reversals, eligible),
        "mean_aligned_late_mid_change_bps": directional["aligned_late_mid_change_bps"].mean(),
        "median_aligned_late_mid_change_bps": directional["aligned_late_mid_change_bps"].median(),
        "aligned_mean_signflip_pvalue": _sign_flip_mean_pvalue(directional["aligned_late_mid_change_bps"]),
        "follow_events": len(follow),
        "follow_mean_gross_bbo_pnl_bps": follow.mean(),
        "follow_median_gross_bbo_pnl_bps": follow.median(),
        "follow_win_rate": (follow > 0).mean(),
        "follow_win_pvalue": _binomial_two_sided(int((follow > 0).sum()), len(follow)),
        "contrarian_events": len(contra),
        "contrarian_mean_gross_bbo_pnl_bps": contra.mean(),
        "contrarian_median_gross_bbo_pnl_bps": contra.median(),
        "contrarian_win_rate": (contra > 0).mean(),
        "contrarian_win_pvalue": _binomial_two_sided(int((contra > 0).sum()), len(contra)),
    }


def product_reversal_summary(event_statistics: pd.DataFrame) -> pd.DataFrame:
    """Summarize all products and label only FDR-controlled reversal/continuation evidence."""

    rows: list[dict[str, object]] = []
    for (exchange, product), group in event_statistics.groupby(["exchange", "product"], sort=True):
        rows.append(
            {
                "exchange": exchange,
                "product": product,
                "product_name": CN_PRODUCT_NAMES.get(product, product),
            }
            | _summarize_event_group(group)
        )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["reversal_qvalue"] = _bh_qvalues(result["reversal_pvalue"])
    result["aligned_mean_signflip_qvalue"] = _bh_qvalues(result["aligned_mean_signflip_pvalue"])
    result["follow_win_qvalue"] = _bh_qvalues(result["follow_win_pvalue"])
    result["contrarian_win_qvalue"] = _bh_qvalues(result["contrarian_win_pvalue"])
    result["trend_conclusion"] = np.select(
        [
            result["reversal_qvalue"].lt(0.10) & result["reversal_rate"].gt(0.50),
            result["reversal_qvalue"].lt(0.10) & result["reversal_rate"].lt(0.50),
        ],
        ["显著反转（FDR 10%）", "显著延续（FDR 10%）"],
        default="未显著",
    )
    result["magnitude_conclusion"] = np.select(
        [
            result["aligned_mean_signflip_qvalue"].lt(0.10) & result["mean_aligned_late_mid_change_bps"].lt(0),
            result["aligned_mean_signflip_qvalue"].lt(0.10) & result["mean_aligned_late_mid_change_bps"].gt(0),
        ],
        ["显著回转幅度（FDR 10%）", "显著延续幅度（FDR 10%）"],
        default="未显著",
    )
    return result.sort_values(["exchange", "product"]).reset_index(drop=True)


def exchange_reversal_summary(event_statistics: pd.DataFrame) -> pd.DataFrame:
    """A pooled exchange view, kept separate from the product-level inference."""

    rows = []
    for exchange, group in event_statistics.groupby("exchange", sort=True):
        rows.append({"exchange": exchange} | _summarize_event_group(group))
    return pd.DataFrame(rows)


def aligned_mid_paths(daily: pd.DataFrame, event_statistics: pd.DataFrame) -> pd.DataFrame:
    """Align all paths to the initial five-day direction so a late reversal is visually comparable."""

    indexed = event_statistics.set_index("event_id")
    signs = indexed["early_mid_change_bps_t10_to_t6"].map(np.sign)
    rows: list[dict[str, object]] = []
    for event_id, group in daily.groupby("event_id", sort=False):
        direction = signs.get(event_id, 0.0)
        if direction == 0:
            continue
        group = group.sort_values("relative_day")
        baseline = group.iloc[0]["daily_mid_mean_bps"]
        outcome = str(indexed.at[event_id, "outcome"])
        for row in group.itertuples(index=False):
            rows.append(
                {
                    "event_id": event_id, "exchange": row.exchange, "product": row.product,
                    "product_name": CN_PRODUCT_NAMES.get(row.product, row.product),
                    "event_date": row.event_date, "old_contract": row.old_contract,
                    "new_contract": row.new_contract, "outcome": outcome,
                    "relative_day": row.relative_day,
                    "aligned_mid_delta_bps": direction * (row.daily_mid_mean_bps - baseline),
                }
            )
    return pd.DataFrame(rows)


def _path_grid_shape(n_events: int, columns: int = PATH_GRID_COLUMNS) -> tuple[int, int]:
    """Keep a compact eight-column page, shrinking only when a product has fewer rolls."""

    if n_events <= 0:
        raise ValueError("n_events must be positive")
    cols = min(columns, n_events)
    return ceil(n_events / cols), cols


def _roll_panel_title(event_date: object, old_contract: object, new_contract: object) -> str:
    """Label one panel with the roll date and the frozen old -> new pair."""

    return f"{pd.Timestamp(event_date):%Y-%m-%d}\n{old_contract}->{new_contract}"


def render_product_aligned_mid_paths(paths: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    """Render one small-multiple page per product, with every roll in its own labelled panel."""

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, object]] = []
    for (exchange, product), group in paths.groupby(["exchange", "product"], sort=True):
        events = [(event_id, event.sort_values("relative_day")) for event_id, event in group.groupby("event_id", sort=True)]
        n_events = len(events)
        rows, cols = _path_grid_shape(n_events)
        header_inches = 0.42
        figure, axes = plt.subplots(
            rows,
            cols,
            figsize=(0.40 + 0.92 * cols, header_inches + 0.78 * rows),
            sharex=True,
            sharey=True,
            squeeze=False,
            facecolor="#fcfcfb",
        )
        y_max = float(np.nanmax(np.abs(group["aligned_mid_delta_bps"].to_numpy(dtype=float))))
        y_lim = max(5.0, 1.08 * y_max)
        roll_labels: list[str] = []
        for index, (_, event) in enumerate(events):
            axis = axes[index // cols, index % cols]
            axis.set_facecolor("#fcfcfb")
            first = event.iloc[0]
            color = PATH_REVERSAL_COLOR if first["outcome"] == "reversal" else PATH_CONTINUATION_COLOR
            axis.plot(event["relative_day"], event["aligned_mid_delta_bps"], color=color, linewidth=1.15, solid_capstyle="round")
            axis.axhline(0.0, color=PATH_AXIS_COLOR, linewidth=0.5)
            axis.axvline(-6, color=PATH_AXIS_COLOR, linewidth=0.5, alpha=0.45)
            axis.set_xlim(-10.3, -0.7)
            axis.set_ylim(-y_lim, y_lim)
            axis.set_xticks([-10, -6, -1])
            title = _roll_panel_title(first["event_date"], first["old_contract"], first["new_contract"])
            axis.set_title(title, fontsize=5.6, pad=1.6, color="#0b0b0b")
            axis.tick_params(labelsize=5.2, length=2, colors=PATH_AXIS_COLOR)
            axis.grid(axis="y", color="#ececec", linewidth=0.5)
            for spine in axis.spines.values():
                spine.set_color("#d0d5dd")
                spine.set_linewidth(0.5)
            roll_labels.append(title.replace("\n", " "))
        for index in range(n_events, rows * cols):
            axes[index // cols, index % cols].set_visible(False)
        for axis in axes[:, 0]:
            if axis.get_visible():
                axis.set_ylabel("bp", fontsize=6, color=PATH_AXIS_COLOR)
        last_row = (n_events - 1) // cols
        for axis in axes[last_row, :]:
            if axis.get_visible():
                axis.set_xlabel("t", fontsize=6, color=PATH_AXIS_COLOR)
        figure.suptitle(
            f"{exchange.upper()} {product}  {n_events} rolls",
            fontsize=9,
            x=0.012,
            y=0.995,
            ha="left",
            va="top",
            color="#0b0b0b",
        )
        figure.legend(
            handles=[
                Line2D([0], [0], color=PATH_CONTINUATION_COLOR, lw=1.5, label="Continuation"),
                Line2D([0], [0], color=PATH_REVERSAL_COLOR, lw=1.5, label="Reversal"),
            ],
            loc="upper right",
            fontsize=6.5,
            frameon=False,
            ncol=2,
            bbox_to_anchor=(0.995, 0.995),
            borderaxespad=0.0,
        )
        top = 1.0 - header_inches / figure.get_figheight()
        figure.subplots_adjust(left=0.045, right=0.995, top=top - 0.01, bottom=0.07, wspace=0.18, hspace=0.92)
        filename = f"{exchange}_{product}_aligned_mid_paths.png"
        figure.savefig(output_dir / filename, dpi=150, facecolor=figure.get_facecolor())
        plt.close(figure)
        manifest.append(
            {
                "exchange": exchange, "product": product,
                "product_name": CN_PRODUCT_NAMES.get(product, product), "image_file": filename,
                "events": n_events, "rolls": " | ".join(roll_labels),
            }
        )
    return pd.DataFrame(manifest)


def render_reversal_summary(summary: pd.DataFrame, output_path: Path) -> None:
    """Render one compact product comparison; colours encode inference rather than exchange."""

    plot = summary.copy().sort_values(["exchange", "reversal_rate", "product"])
    colors = {"显著反转（FDR 10%）": "#b54708", "显著延续（FDR 10%）": "#007f73", "未显著": "#98a2b3"}
    exchanges = [exchange for exchange in HISTORICAL_CN_UNIVERSE if exchange in set(plot["exchange"])]
    figure, axes = plt.subplots(len(exchanges), 1, figsize=(8.6, 6.4), sharey=True, constrained_layout=True)
    if len(exchanges) == 1:
        axes = [axes]
    for axis, exchange in zip(axes, exchanges, strict=True):
        group = plot[plot["exchange"].eq(exchange)].reset_index(drop=True)
        bars = axis.bar(
            range(len(group)), group["reversal_rate"],
            color=group["trend_conclusion"].map(colors),
        )
        axis.axhline(0.5, color="#475467", linewidth=0.8, linestyle="--")
        axis.set_ylim(0, 1)
        axis.set_ylabel(f"{exchange.upper()}\nreversal rate", fontsize=8)
        axis.set_xticks(range(len(group)), group["product"], fontsize=8)
        for bar, row in zip(bars, group.itertuples(index=False), strict=True):
            axis.text(
                bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.025,
                f"n={row.directional_events}", ha="center", va="bottom", fontsize=6,
            )
    axes[0].set_title("2020-present: fixed-pair early/late direction reversal", fontsize=11)
    axes[0].plot([], [], color="#475467", linestyle="--", label="Random baseline 50%")
    axes[0].legend(loc="upper right", fontsize=7, frameon=False)
    figure.savefig(output_path, dpi=150)
    plt.close(figure)


def default_paths() -> StudyPaths:
    """Return the explicit read-only sources used by the Chinese-futures study."""

    return StudyPaths(
        cn_roll_root=Path("/mnt/nvme-raid0-28t/fanghaizhou/cn_roll_replay/2019_20260826/result_no_rollback"),
        cme_roll_root=Path("/mnt/nvme-raid0-28t/fanghaizhou/cme_roll_replay"),
        cn_backtest_root=Path("/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/backtest_1s"),
        cme_backtest_root=Path("/mnt/hdd-raid5-72t/liang_torch/cme_futures_data/backtest_1s"),
    )


def run_historical_reversal_study(
    output_dir: Path,
    *,
    start: str = "2020-01-01",
    workers: int = 4,
    paths: StudyPaths | None = None,
    parameters: HistoricalParameters = HistoricalParameters(),
) -> dict[str, pd.DataFrame]:
    """Run and write a complete auditable historical study for the declared CN universe."""

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = paths or default_paths()
    events, calendars = load_roll_events(paths)
    selected_events = select_historical_cn_events(events, start)
    jobs = plan_backtest_jobs(
        selected_events,
        calendars,
        paths,
        StudyParameters(pre_roll_days=parameters.pre_roll_days, include_roll_day=False),
    )
    daily, audit = build_historical_daily_marks(jobs, parameters, workers)
    event_statistics = build_event_reversal_statistics(daily, parameters)
    product_summary = product_reversal_summary(event_statistics)
    exchange_summary = exchange_reversal_summary(event_statistics)
    paths_frame = aligned_mid_paths(daily, event_statistics)
    manifest = render_product_aligned_mid_paths(paths_frame, output_dir / "product_paths")
    render_reversal_summary(product_summary, output_dir / "product_reversal_rate.png")
    selected_events.to_csv(output_dir / "selected_events.csv", index=False)
    jobs.drop(columns="source_path").to_csv(output_dir / "planned_days.csv", index=False)
    daily.to_csv(output_dir / "daily_mid_and_eod_bbo.csv", index=False)
    audit.to_csv(output_dir / "daily_mark_audit.csv", index=False)
    event_statistics.to_csv(output_dir / "event_reversal_statistics.csv", index=False)
    product_summary.to_csv(output_dir / "product_reversal_summary.csv", index=False)
    exchange_summary.to_csv(output_dir / "exchange_reversal_summary.csv", index=False)
    paths_frame.to_csv(output_dir / "aligned_mid_paths.csv", index=False)
    manifest.to_csv(output_dir / "product_path_manifest.csv", index=False)
    return {
        "selected_events": selected_events, "jobs": jobs, "daily": daily, "audit": audit,
        "event_statistics": event_statistics, "product_summary": product_summary,
        "exchange_summary": exchange_summary, "aligned_paths": paths_frame, "manifest": manifest,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="2020-present Chinese futures roll-spread reversal study")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    result = run_historical_reversal_study(args.output_dir, start=args.start, workers=args.workers)
    print(result["product_summary"].to_string(index=False))


if __name__ == "__main__":
    main()
