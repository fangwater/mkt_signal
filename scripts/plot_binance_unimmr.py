#!/usr/bin/env python3
"""Plot Binance unified-account uniMMR and export threshold-crossing data."""

import argparse
import csv
import math
import re
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


ACCOUNT_RISK_MARKER = "Binance AccountRisk:"
FIELD_PATTERN = re.compile(r"\b([a-z_]+)=([^\s]+)")
NUMERIC_FIELDS = (
    "adj_eq_usd",
    "actual_eq_usd",
    "maint_margin_usd",
    "initial_margin_usd",
    "margin_ratio",
    "calc_margin_ratio",
    "diff",
)
SENTINEL_UNIMMR = 99_999_999.0


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Plot Binance AccountRisk uniMMR and export the plotted samples and "
            "downward threshold crossings."
        )
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("/home/ubuntu/.pmdaemon/logs/fr_am_bn_arb02-error.log"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "artifacts/binance_fr_arb02_unimmr_last_24h_excl_last_3h.png"
        ),
    )
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--exclude-recent-hours", type=float, default=3.0)
    parser.add_argument("--threshold", type=float, default=2.0)
    return parser.parse_args()


def parse_samples(log_path):
    samples = []
    with log_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if ACCOUNT_RISK_MARKER not in line:
                continue
            fields = dict(FIELD_PATTERN.findall(line))
            if "ts" not in fields or "margin_ratio" not in fields:
                continue
            try:
                sample = {
                    "ts_ms": int(fields["ts"]),
                    "scope": fields.get("scope", ""),
                }
                for field in NUMERIC_FIELDS:
                    sample[field] = float(fields[field])
            except (KeyError, ValueError):
                continue
            samples.append(sample)
    return samples


def utc_datetime(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def utc_text(ts_ms):
    return utc_datetime(ts_ms).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def select_window(samples, hours, exclude_recent_hours):
    if not samples:
        raise SystemExit("no Binance AccountRisk samples found")
    if hours <= 0:
        raise SystemExit("--hours must be positive")
    if exclude_recent_hours < 0 or exclude_recent_hours >= hours:
        raise SystemExit("--exclude-recent-hours must satisfy 0 <= value < --hours")

    anchor_ms = samples[-1]["ts_ms"]
    window_start_ms = anchor_ms - round(hours * 3_600_000)
    window_end_ms = anchor_ms - round(exclude_recent_hours * 3_600_000)
    selected = [
        sample
        for sample in samples
        if window_start_ms <= sample["ts_ms"] < window_end_ms
    ]
    if not selected:
        raise SystemExit("no samples in the requested window")
    return selected, window_start_ms, window_end_ms


def find_downward_crossings(samples, threshold):
    crossings = []
    previous = None
    for sample in samples:
        value = sample["margin_ratio"]
        if value >= SENTINEL_UNIMMR:
            previous = None
            continue
        if (
            previous is not None
            and previous["margin_ratio"] > threshold
            and value <= threshold
        ):
            crossings.append(
                {
                    "event_index": len(crossings) + 1,
                    "threshold": threshold,
                    "previous_ts_ms": previous["ts_ms"],
                    "previous_timestamp_utc": utc_text(previous["ts_ms"]),
                    "previous_margin_ratio": previous["margin_ratio"],
                    "trigger_ts_ms": sample["ts_ms"],
                    "trigger_timestamp_utc": utc_text(sample["ts_ms"]),
                    "trigger_margin_ratio": value,
                }
            )
        previous = sample
    return crossings


def output_stem(output_path):
    return output_path.with_suffix("")


def write_samples_csv(output_path, samples, threshold):
    path = Path(f"{output_stem(output_path)}_samples.csv")
    fieldnames = [
        "ts_ms",
        "timestamp_utc",
        "scope",
        *NUMERIC_FIELDS,
        "is_no_margin_sentinel",
        "at_or_below_threshold",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for sample in samples:
            is_sentinel = sample["margin_ratio"] >= SENTINEL_UNIMMR
            writer.writerow(
                {
                    "ts_ms": sample["ts_ms"],
                    "timestamp_utc": utc_text(sample["ts_ms"]),
                    "scope": sample["scope"],
                    **{field: sample[field] for field in NUMERIC_FIELDS},
                    "is_no_margin_sentinel": is_sentinel,
                    "at_or_below_threshold": (
                        not is_sentinel and sample["margin_ratio"] <= threshold
                    ),
                }
            )
    return path


def write_crossings_csv(output_path, crossings):
    path = Path(f"{output_stem(output_path)}_threshold_crossings.csv")
    fieldnames = [
        "event_index",
        "threshold",
        "previous_ts_ms",
        "previous_timestamp_utc",
        "previous_margin_ratio",
        "trigger_ts_ms",
        "trigger_timestamp_utc",
        "trigger_margin_ratio",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(crossings)
    return path


def find_sentinel_runs(samples):
    runs = []
    run_start = None
    run_end = None
    for sample in samples:
        if sample["margin_ratio"] >= SENTINEL_UNIMMR:
            if run_start is None:
                run_start = sample["ts_ms"]
            run_end = sample["ts_ms"]
        elif run_start is not None:
            runs.append((run_start, run_end))
            run_start = None
            run_end = None
    if run_start is not None:
        runs.append((run_start, run_end))
    return runs


def plot(output_path, samples, crossings, window_start_ms, window_end_ms, args):
    times = [utc_datetime(sample["ts_ms"]) for sample in samples]
    raw_values = [sample["margin_ratio"] for sample in samples]
    values = [
        value if value < SENTINEL_UNIMMR else math.nan for value in raw_values
    ]
    finite_points = [
        (time, value)
        for time, value in zip(times, values)
        if math.isfinite(value)
    ]
    if not finite_points:
        raise SystemExit("no finite uniMMR samples in the requested window")

    finite_times = [point[0] for point in finite_points]
    finite_values = [point[1] for point in finite_points]
    min_value = min(finite_values)
    max_value = max(finite_values)
    min_idx = finite_values.index(min_value)
    max_idx = finite_values.index(max_value)
    sentinel_count = sum(value >= SENTINEL_UNIMMR for value in raw_values)

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(14.52, 7.14), dpi=120)
    fig.patch.set_facecolor("#f5f6f7")
    ax.set_facecolor("#ffffff")

    line_color = "#277d9a"
    span = max_value - min_value
    padding = max(span * 0.12, 0.1)
    lower = max(0.0, min(min_value, args.threshold) - padding)
    upper = max(max_value, args.threshold) + padding
    ax.set_ylim(lower, upper)
    ax.axhspan(lower, args.threshold, color="#d84a4a", alpha=0.055, zorder=0)
    ax.axhline(
        args.threshold,
        color="#c83e3e",
        linestyle="--",
        linewidth=1.2,
        label=f"Strategy close threshold = {args.threshold:g}",
        zorder=2,
    )
    ax.plot(
        times,
        values,
        color=line_color,
        linewidth=1.25,
        label="Binance uniMMR",
        zorder=3,
    )
    ax.fill_between(times, values, lower, color=line_color, alpha=0.09, zorder=1)

    sentinel_runs = find_sentinel_runs(samples)
    for index, (start_ms, end_ms) in enumerate(sentinel_runs):
        if index == len(sentinel_runs) - 1 and end_ms == samples[-1]["ts_ms"]:
            end_ms = window_end_ms
        start = utc_datetime(start_ms)
        end = utc_datetime(end_ms)
        ax.axvspan(start, end, color="#aeb7bf", alpha=0.18, zorder=0)
        mid = start + (end - start) / 2
        ax.text(
            mid,
            upper - padding * 0.15,
            "No maintenance margin\nuniMMR = 99,999,999 (sentinel)",
            ha="center",
            va="top",
            fontsize=9.5,
            color="#626b73",
            bbox=dict(
                boxstyle="round,pad=0.35",
                fc="white",
                ec="#c9ced3",
                alpha=0.92,
            ),
            zorder=4,
        )

    if crossings:
        crossing_times = [utc_datetime(row["trigger_ts_ms"]) for row in crossings]
        crossing_values = [row["trigger_margin_ratio"] for row in crossings]
        ax.scatter(
            crossing_times,
            crossing_values,
            s=28,
            color="#d92525",
            edgecolor="white",
            linewidth=0.6,
            label=f"Downward crossings ({len(crossings)})",
            zorder=5,
        )

    title = (
        "Binance FR Arb02 - Unified Account Margin Ratio "
        f"(T-{args.hours:g}h to T-{args.exclude_recent_hours:g}h)"
    )
    fig.text(
        0.0395,
        0.965,
        title,
        ha="left",
        va="top",
        fontsize=18,
        color="#2c2c2c",
    )
    start_text = utc_datetime(window_start_ms).strftime("%Y-%m-%d %H:%M:%S")
    end_text = utc_datetime(window_end_ms).strftime("%Y-%m-%d %H:%M:%S")
    fig.text(
        0.0395,
        0.927,
        f"Window: {start_text} to {end_text} UTC  |  {len(samples):,} samples  |  "
        f"{len(finite_values):,} finite, {sentinel_count:,} no-margin sentinel  |  "
        f"{len(crossings)} downward crossings at {args.threshold:g}",
        ha="left",
        va="top",
        fontsize=10.5,
        color="#626b73",
    )

    ax.set_xlabel("Time (UTC)", fontsize=11)
    ax.set_ylabel("margin_ratio (Binance uniMMR)", fontsize=11)
    locator = mdates.AutoDateLocator(minticks=7, maxticks=10, tz=timezone.utc)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%m-%d\n%H:%M", tz=timezone.utc)
    )
    ax.tick_params(axis="both", labelsize=10)
    ax.grid(True, color="#d5dde3", linewidth=0.7, alpha=0.85)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#c7cdd2")
    ax.spines["bottom"].set_color("#c7cdd2")
    ax.set_xlim(utc_datetime(window_start_ms), utc_datetime(window_end_ms))
    ax.legend(loc="upper left", frameon=True, framealpha=0.92, fontsize=9.5)

    for name, point_idx, color, offset in (
        ("MIN", min_idx, "#23865f", (12, 22)),
        ("MAX", max_idx, "#c83e3e", (12, -48)),
    ):
        point_time = finite_times[point_idx]
        point_value = finite_values[point_idx]
        ax.scatter([point_time], [point_value], s=72, color=color, zorder=6)
        ax.annotate(
            f"{name}  {point_value:.6f}\n"
            f"{point_time.strftime('%Y-%m-%d %H:%M:%S')} UTC",
            xy=(point_time, point_value),
            xytext=offset,
            textcoords="offset points",
            fontsize=10.5,
            color=color,
            bbox=dict(
                boxstyle="round,pad=0.45",
                fc="white",
                ec="#c9ced3",
                alpha=0.95,
            ),
            arrowprops=dict(arrowstyle="-", color=color, lw=1.25),
            zorder=7,
        )

    fig.subplots_adjust(left=0.0395, right=0.932, bottom=0.09, top=0.90)
    fig.savefig(output_path, dpi=120)
    plt.close(fig)
    return min_value, max_value, sentinel_count


def main():
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    all_samples = parse_samples(args.log)
    samples, window_start_ms, window_end_ms = select_window(
        all_samples, args.hours, args.exclude_recent_hours
    )
    crossings = find_downward_crossings(samples, args.threshold)
    samples_path = write_samples_csv(args.output, samples, args.threshold)
    crossings_path = write_crossings_csv(args.output, crossings)
    min_value, max_value, sentinel_count = plot(
        args.output,
        samples,
        crossings,
        window_start_ms,
        window_end_ms,
        args,
    )

    print(args.output)
    print(samples_path)
    print(crossings_path)
    print(
        f"samples={len(samples)} finite={len(samples) - sentinel_count} "
        f"sentinel={sentinel_count} min={min_value:.6f} max={max_value:.6f} "
        f"downward_crossings={len(crossings)}"
    )
    for crossing in crossings:
        print(
            f"{crossing['event_index']:02d} "
            f"{crossing['trigger_timestamp_utc']} "
            f"{crossing['previous_margin_ratio']:.6f} -> "
            f"{crossing['trigger_margin_ratio']:.6f}"
        )


if __name__ == "__main__":
    main()
