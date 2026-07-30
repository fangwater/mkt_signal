use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use polars::prelude::*;
use serde::Serialize;

const UNIFORM_FILE: &str = "uniform_orders.parquet";
const UNMATCHED_FILE: &str = "trade_updates_unmatched.parquet";

#[derive(Debug, Parser)]
#[command(
    name = "trade_history_reconcile",
    about = "Compare local persisted Binance fills with exchange trades exported from PostgreSQL",
    long_about = "Compare a UTC millisecond interval from crypto_nav_manager trade CSV exports with \
local order_export parquet files. Local quantity is uniform_orders.amount_update plus only the \
cumulative-delta fills from trade_updates_unmatched whose client_order_id is not already represented \
in uniform_orders."
)]
struct Args {
    /// Directory containing uniform_orders.parquet and trade_updates_unmatched.parquet.
    #[arg(long)]
    local_dir: PathBuf,

    /// PG trade CSV file or directory. Repeat for multiple files/directories.
    #[arg(long = "pg-trades", required = true)]
    pg_inputs: Vec<PathBuf>,

    /// Inclusive interval start in Unix milliseconds.
    #[arg(long)]
    start_ms: i64,

    /// Inclusive interval end in Unix milliseconds.
    #[arg(long)]
    end_ms: i64,

    /// Directory for summary.json, groups.csv, and unmatched_orders.csv.
    #[arg(long, default_value = "trade_history_reconcile_report")]
    report_dir: PathBuf,

    /// Absolute base-quantity tolerance per market/symbol/side group.
    #[arg(long, default_value_t = 1e-8)]
    qty_epsilon: f64,

    /// Return success even when quantity mismatches are found.
    #[arg(long, default_value_t = false)]
    allow_mismatch: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Market {
    Spot,
    Swap,
}

impl Market {
    fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Swap => "swap",
        }
    }

    fn from_sid(raw: &str) -> Result<Self> {
        match raw {
            "1" => Ok(Self::Spot),
            "0" => Ok(Self::Swap),
            _ => bail!("unsupported PG sid {raw:?}; expected 0 or 1"),
        }
    }

    fn from_venue(raw: &str) -> Result<Self> {
        match raw {
            "BinanceMargin" => Ok(Self::Spot),
            "BinanceFutures" => Ok(Self::Swap),
            _ => bail!(
                "unsupported local trading_venue {raw:?}; expected BinanceMargin or BinanceFutures"
            ),
        }
    }
}

impl fmt::Display for Market {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct GroupKey {
    market: Market,
    symbol: String,
    side: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct OrderKey {
    market: Market,
    symbol: String,
    order_id: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TradeKey {
    market: Market,
    symbol: String,
    trade_id: String,
}

#[derive(Clone, Debug)]
struct PgTradeFingerprint {
    order_key: OrderKey,
    group: GroupKey,
    qty_bits: u64,
}

#[derive(Clone, Debug)]
struct PgOrder {
    group: GroupKey,
    qty: f64,
    trade_count: usize,
}

#[derive(Debug, Default)]
struct PgData {
    orders: BTreeMap<OrderKey, PgOrder>,
    trade_count: usize,
    duplicate_rows: usize,
}

#[derive(Debug, Default)]
struct UniformGroup {
    qty: f64,
    event_count: usize,
    client_order_ids: BTreeSet<i64>,
}

#[derive(Clone, Copy, Debug)]
struct Observation {
    ts_us: i64,
    cumulative_qty: f64,
}

#[derive(Debug)]
struct UnmatchedSeries {
    group: GroupKey,
    client_order_ids: BTreeSet<i64>,
    observations: Vec<Observation>,
}

#[derive(Debug)]
struct UnmatchedOrder {
    group: GroupKey,
    qty: f64,
    event_count: usize,
    baseline_observed: bool,
    client_order_ids: BTreeSet<i64>,
}

#[derive(Debug, Default)]
struct QuantityStats {
    qty: f64,
    event_count: usize,
    order_count: usize,
}

#[derive(Debug, Serialize)]
struct GroupReport {
    market: String,
    symbol: String,
    side: String,
    pg_trade_count: usize,
    pg_order_count: usize,
    pg_qty: f64,
    pg_uniform_expected_qty: f64,
    uniform_event_count: usize,
    uniform_order_count: usize,
    uniform_qty: f64,
    unmatched_event_count: usize,
    unmatched_order_count: usize,
    unmatched_qty: f64,
    unmatched_only_order_count: usize,
    unmatched_only_qty: f64,
    unmatched_represented_order_count: usize,
    local_qty: f64,
    total_qty_diff: f64,
    uniform_expected_diff: f64,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct UnmatchedOrderReport {
    market: String,
    symbol: String,
    order_id: String,
    local_side: String,
    pg_side: String,
    local_qty: f64,
    pg_qty: Option<f64>,
    qty_diff: Option<f64>,
    local_event_count: usize,
    pg_trade_count: usize,
    baseline_observed: bool,
    represented_in_uniform: bool,
    client_order_ids: String,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct Summary {
    start_ms: i64,
    end_ms: i64,
    qty_epsilon: f64,
    pg_trade_count: usize,
    pg_order_count: usize,
    pg_duplicate_rows: usize,
    uniform_fill_event_count: usize,
    uniform_order_count: usize,
    unmatched_fill_event_count: usize,
    unmatched_order_count: usize,
    unmatched_represented_order_count: usize,
    unmatched_only_order_count: usize,
    unmatched_orders_with_pre_start_baseline: usize,
    group_count: usize,
    mismatched_group_count: usize,
    mismatched_unmatched_order_count: usize,
    pg_qty: f64,
    uniform_qty: f64,
    unmatched_raw_qty: f64,
    unmatched_represented_qty: f64,
    unmatched_qty: f64,
    local_qty: f64,
    quantity_diff: f64,
    aligned: bool,
    limitation: &'static str,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let (start_us, end_us) = validate_args(&args)?;

    let pg_files = collect_pg_files(&args.pg_inputs)?;
    let pg = load_pg_trades(&pg_files, args.start_ms, args.end_ms)?;
    let uniform = load_uniform_orders(
        &args.local_dir.join(UNIFORM_FILE),
        start_us,
        end_us,
        args.qty_epsilon,
    )?;
    let unmatched = load_unmatched_orders(
        &args.local_dir.join(UNMATCHED_FILE),
        start_us,
        end_us,
        args.qty_epsilon,
    )?;

    let groups = build_group_reports(&pg, &uniform, &unmatched, args.qty_epsilon);
    let unmatched_reports = build_unmatched_reports(&pg, &uniform, &unmatched, args.qty_epsilon);
    let summary = build_summary(
        &args,
        &pg,
        &uniform,
        &unmatched,
        &groups,
        &unmatched_reports,
    );

    fs::create_dir_all(&args.report_dir)
        .with_context(|| format!("create report directory {}", args.report_dir.display()))?;
    write_group_report(&args.report_dir.join("groups.csv"), &groups)?;
    write_unmatched_report(
        &args.report_dir.join("unmatched_orders.csv"),
        &unmatched_reports,
    )?;
    write_summary(&args.report_dir.join("summary.json"), &summary)?;

    println!(
        "aligned={} pg_trades={} pg_qty={:.12} uniform_qty={:.12} unmatched_raw_qty={:.12} unmatched_only_qty={:.12} local_qty={:.12} diff={:.12}",
        summary.aligned,
        summary.pg_trade_count,
        summary.pg_qty,
        summary.uniform_qty,
        summary.unmatched_raw_qty,
        summary.unmatched_qty,
        summary.local_qty,
        summary.quantity_diff,
    );
    println!(
        "groups={} mismatched_groups={} unmatched_orders={} mismatched_unmatched_orders={} report_dir={}",
        summary.group_count,
        summary.mismatched_group_count,
        summary.unmatched_order_count,
        summary.mismatched_unmatched_order_count,
        args.report_dir.display(),
    );

    if !summary.aligned && !args.allow_mismatch {
        bail!(
            "trade history mismatch: {} group(s) differ; inspect {}",
            summary.mismatched_group_count,
            args.report_dir.display()
        );
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(i64, i64)> {
    if args.start_ms < 0 || args.end_ms < 0 {
        bail!("start_ms and end_ms must be non-negative");
    }
    if args.end_ms < args.start_ms {
        bail!(
            "end_ms {} is earlier than start_ms {}",
            args.end_ms,
            args.start_ms
        );
    }
    if !args.qty_epsilon.is_finite() || args.qty_epsilon < 0.0 {
        bail!("qty_epsilon must be finite and non-negative");
    }
    let start_us = args
        .start_ms
        .checked_mul(1_000)
        .context("start_ms overflows microseconds")?;
    let end_us = args
        .end_ms
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(999))
        .context("end_ms overflows inclusive microsecond bound")?;
    Ok((start_us, end_us))
}

fn collect_pg_files(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = BTreeSet::new();
    for input in inputs {
        if input.is_file() {
            files.insert(input.clone());
            continue;
        }
        if input.is_dir() {
            for entry in fs::read_dir(input)
                .with_context(|| format!("read PG trades directory {}", input.display()))?
            {
                let path = entry
                    .with_context(|| format!("read entry below {}", input.display()))?
                    .path();
                let is_trade_csv = path.is_file()
                    && path.extension().and_then(|value| value.to_str()) == Some("csv")
                    && path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .is_some_and(|name| name.starts_with("trades_"));
                if is_trade_csv {
                    files.insert(path);
                }
            }
            continue;
        }
        bail!("PG trades input does not exist: {}", input.display());
    }
    if files.is_empty() {
        bail!("no trades_*.csv files found in --pg-trades inputs");
    }
    Ok(files.into_iter().collect())
}

fn load_pg_trades(files: &[PathBuf], start_ms: i64, end_ms: i64) -> Result<PgData> {
    let mut data = PgData::default();
    let mut seen = BTreeMap::<TradeKey, PgTradeFingerprint>::new();

    for path in files {
        let mut reader = csv::Reader::from_path(path)
            .with_context(|| format!("open PG trade CSV {}", path.display()))?;
        let headers = reader
            .headers()
            .with_context(|| format!("read CSV header {}", path.display()))?
            .clone();
        let sid_idx = header_index(&headers, "sid")?;
        let symbol_idx = header_index(&headers, "symbol")?;
        let trade_id_idx = header_index(&headers, "id")?;
        let order_id_idx = header_index(&headers, "orderId")?;
        let side_idx = header_index(&headers, "side")?;
        let qty_idx = header_index(&headers, "qty")?;
        let ts_idx = header_index(&headers, "ts")?;

        for (row_index, result) in reader.records().enumerate() {
            let row =
                result.with_context(|| format!("read {} row {}", path.display(), row_index + 2))?;
            let ts = parse_i64(csv_value(&row, ts_idx, "ts")?, "ts")?;
            if ts < start_ms || ts > end_ms {
                continue;
            }

            let market = Market::from_sid(csv_value(&row, sid_idx, "sid")?)?;
            let symbol = normalize_symbol(csv_value(&row, symbol_idx, "symbol")?)?;
            let side = normalize_side(csv_value(&row, side_idx, "side")?)?;
            let trade_id = nonempty(csv_value(&row, trade_id_idx, "id")?, "id")?;
            let order_id = nonempty(csv_value(&row, order_id_idx, "orderId")?, "orderId")?;
            let qty = parse_nonnegative_f64(csv_value(&row, qty_idx, "qty")?, "qty")?;
            if qty == 0.0 {
                continue;
            }

            let group = GroupKey {
                market,
                symbol: symbol.clone(),
                side,
            };
            let order_key = OrderKey {
                market,
                symbol: symbol.clone(),
                order_id: order_id.to_string(),
            };
            let trade_key = TradeKey {
                market,
                symbol,
                trade_id: trade_id.to_string(),
            };
            let fingerprint = PgTradeFingerprint {
                order_key: order_key.clone(),
                group: group.clone(),
                qty_bits: qty.to_bits(),
            };
            if let Some(previous) = seen.get(&trade_key) {
                if previous.order_key != fingerprint.order_key
                    || previous.group != fingerprint.group
                    || previous.qty_bits != fingerprint.qty_bits
                {
                    bail!(
                        "conflicting duplicate PG trade market={} symbol={} trade_id={}",
                        trade_key.market,
                        trade_key.symbol,
                        trade_key.trade_id
                    );
                }
                data.duplicate_rows += 1;
                continue;
            }
            seen.insert(trade_key, fingerprint);

            let order = data.orders.entry(order_key).or_insert_with(|| PgOrder {
                group: group.clone(),
                qty: 0.0,
                trade_count: 0,
            });
            if order.group != group {
                bail!(
                    "PG order has inconsistent side: market={} symbol={} order_id={}",
                    order.group.market,
                    order.group.symbol,
                    order_id
                );
            }
            order.qty += qty;
            order.trade_count += 1;
            data.trade_count += 1;
        }
    }
    Ok(data)
}

fn header_index(headers: &csv::StringRecord, name: &str) -> Result<usize> {
    headers
        .iter()
        .position(|header| header == name)
        .with_context(|| format!("PG trade CSV is missing column {name:?}"))
}

fn csv_value<'a>(row: &'a csv::StringRecord, index: usize, name: &str) -> Result<&'a str> {
    row.get(index)
        .with_context(|| format!("CSV row is missing column {name:?}"))
}

fn load_uniform_orders(
    path: &Path,
    start_us: i64,
    end_us: i64,
    epsilon: f64,
) -> Result<BTreeMap<GroupKey, UniformGroup>> {
    let df = read_parquet(path)?;
    let update_ts = i64_col(&df, "update_ts")?;
    let symbol = str_col(&df, "symbol")?;
    let venue = str_col(&df, "trading_venue")?;
    let side = str_col(&df, "side")?;
    let amount_update = f64_col(&df, "amount_update")?;
    let client_order_id = i64_col(&df, "client_order_id")?;
    let mut groups = BTreeMap::<GroupKey, UniformGroup>::new();

    for row in 0..df.height() {
        let ts = required_i64(update_ts, row, "update_ts")?;
        if ts < start_us || ts > end_us {
            continue;
        }
        let qty = required_f64(amount_update, row, "amount_update")?;
        if qty < -epsilon {
            bail!("negative uniform amount_update at row {row}: {qty}");
        }
        if qty <= epsilon {
            continue;
        }
        let key = GroupKey {
            market: Market::from_venue(required_str(venue, row, "trading_venue")?)?,
            symbol: normalize_symbol(required_str(symbol, row, "symbol")?)?,
            side: normalize_side(required_str(side, row, "side")?)?,
        };
        let group = groups.entry(key).or_default();
        group.qty += qty;
        group.event_count += 1;
        group
            .client_order_ids
            .insert(required_i64(client_order_id, row, "client_order_id")?);
    }
    Ok(groups)
}

fn load_unmatched_orders(
    path: &Path,
    start_us: i64,
    end_us: i64,
    epsilon: f64,
) -> Result<BTreeMap<OrderKey, UnmatchedOrder>> {
    let df = read_parquet(path)?;
    let event_time = i64_col(&df, "event_time")?;
    let trade_time = i64_col(&df, "trade_time")?;
    let symbol = str_col(&df, "symbol")?;
    let order_id = i64_col(&df, "order_id")?;
    let client_order_id = i64_col(&df, "client_order_id")?;
    let side = str_col(&df, "side")?;
    let venue = str_col(&df, "trading_venue")?;
    let cumulative = f64_col(&df, "cumulative_filled_quantity")?;
    let mut series = BTreeMap::<OrderKey, UnmatchedSeries>::new();

    for row in 0..df.height() {
        let trade_ts = required_i64(trade_time, row, "trade_time")?;
        let event_ts = required_i64(event_time, row, "event_time")?;
        let ts_us = if trade_ts > 0 { trade_ts } else { event_ts };
        if ts_us > end_us {
            continue;
        }
        let qty = required_f64(cumulative, row, "cumulative_filled_quantity")?;
        if qty < -epsilon {
            bail!("negative unmatched cumulative quantity at row {row}: {qty}");
        }

        let market = Market::from_venue(required_str(venue, row, "trading_venue")?)?;
        let normalized_symbol = normalize_symbol(required_str(symbol, row, "symbol")?)?;
        let group = GroupKey {
            market,
            symbol: normalized_symbol.clone(),
            side: normalize_side(required_str(side, row, "side")?)?,
        };
        let key = OrderKey {
            market,
            symbol: normalized_symbol,
            order_id: required_i64(order_id, row, "order_id")?.to_string(),
        };
        let entry = series
            .entry(key.clone())
            .or_insert_with(|| UnmatchedSeries {
                group: group.clone(),
                client_order_ids: BTreeSet::new(),
                observations: Vec::new(),
            });
        if entry.group != group {
            bail!(
                "unmatched order has inconsistent side: market={} symbol={} order_id={}",
                key.market,
                key.symbol,
                key.order_id
            );
        }
        entry
            .client_order_ids
            .insert(required_i64(client_order_id, row, "client_order_id")?);
        entry.observations.push(Observation {
            ts_us,
            cumulative_qty: qty,
        });
    }

    let mut orders = BTreeMap::new();
    for (key, mut values) in series {
        values
            .observations
            .sort_by_key(|observation| observation.ts_us);
        if let Some(summary) = summarize_unmatched_series(values, start_us, end_us, epsilon) {
            orders.insert(key, summary);
        }
    }
    Ok(orders)
}

fn summarize_unmatched_series(
    values: UnmatchedSeries,
    start_us: i64,
    end_us: i64,
    epsilon: f64,
) -> Option<UnmatchedOrder> {
    let mut baseline_qty = 0.0_f64;
    let mut end_qty = 0.0_f64;
    let mut baseline_observed = false;
    let mut event_count = 0;

    for observation in values.observations {
        if observation.ts_us < start_us {
            baseline_qty = baseline_qty.max(observation.cumulative_qty);
            baseline_observed = true;
        } else if observation.ts_us <= end_us {
            end_qty = end_qty.max(observation.cumulative_qty);
            event_count += 1;
        }
    }
    end_qty = end_qty.max(baseline_qty);
    let qty = end_qty - baseline_qty;
    if event_count == 0 || qty <= epsilon {
        return None;
    }
    Some(UnmatchedOrder {
        group: values.group,
        qty,
        event_count,
        baseline_observed,
        client_order_ids: values.client_order_ids,
    })
}

fn unmatched_is_represented(
    order: &UnmatchedOrder,
    uniform: &BTreeMap<GroupKey, UniformGroup>,
) -> bool {
    uniform.get(&order.group).is_some_and(|group| {
        order
            .client_order_ids
            .iter()
            .any(|client_order_id| group.client_order_ids.contains(client_order_id))
    })
}

fn build_group_reports(
    pg: &PgData,
    uniform: &BTreeMap<GroupKey, UniformGroup>,
    unmatched: &BTreeMap<OrderKey, UnmatchedOrder>,
    epsilon: f64,
) -> Vec<GroupReport> {
    let mut pg_groups = BTreeMap::<GroupKey, QuantityStats>::new();
    let unmatched_only_keys = unmatched
        .iter()
        .filter(|(_, order)| !unmatched_is_represented(order, uniform))
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    let mut pg_uniform_expected = BTreeMap::<GroupKey, f64>::new();
    for (key, order) in &pg.orders {
        let stats = pg_groups.entry(order.group.clone()).or_default();
        stats.qty += order.qty;
        stats.event_count += order.trade_count;
        stats.order_count += 1;
        if !unmatched_only_keys.contains(key) {
            *pg_uniform_expected.entry(order.group.clone()).or_default() += order.qty;
        }
    }

    let mut unmatched_groups = BTreeMap::<GroupKey, QuantityStats>::new();
    let mut unmatched_only_groups = BTreeMap::<GroupKey, QuantityStats>::new();
    let mut unmatched_represented_counts = BTreeMap::<GroupKey, usize>::new();
    for order in unmatched.values() {
        let stats = unmatched_groups.entry(order.group.clone()).or_default();
        stats.qty += order.qty;
        stats.event_count += order.event_count;
        stats.order_count += 1;
        if unmatched_is_represented(order, uniform) {
            *unmatched_represented_counts
                .entry(order.group.clone())
                .or_default() += 1;
        } else {
            let only = unmatched_only_groups
                .entry(order.group.clone())
                .or_default();
            only.qty += order.qty;
            only.event_count += order.event_count;
            only.order_count += 1;
        }
    }

    let mut keys = BTreeSet::new();
    keys.extend(pg_groups.keys().cloned());
    keys.extend(uniform.keys().cloned());
    keys.extend(unmatched_groups.keys().cloned());

    keys.into_iter()
        .map(|key| {
            let pg_stats = pg_groups.get(&key);
            let uniform_stats = uniform.get(&key);
            let unmatched_stats = unmatched_groups.get(&key);
            let unmatched_only_stats = unmatched_only_groups.get(&key);
            let pg_qty = pg_stats.map_or(0.0, |stats| stats.qty);
            let uniform_qty = uniform_stats.map_or(0.0, |stats| stats.qty);
            let unmatched_qty = unmatched_stats.map_or(0.0, |stats| stats.qty);
            let unmatched_only_qty = unmatched_only_stats.map_or(0.0, |stats| stats.qty);
            let local_qty = uniform_qty + unmatched_only_qty;
            let total_qty_diff = local_qty - pg_qty;
            let uniform_expected = pg_uniform_expected.get(&key).copied().unwrap_or(0.0);
            let unmatched_represented_order_count =
                unmatched_represented_counts.get(&key).copied().unwrap_or(0);
            GroupReport {
                market: key.market.to_string(),
                symbol: key.symbol,
                side: key.side,
                pg_trade_count: pg_stats.map_or(0, |stats| stats.event_count),
                pg_order_count: pg_stats.map_or(0, |stats| stats.order_count),
                pg_qty,
                pg_uniform_expected_qty: uniform_expected,
                uniform_event_count: uniform_stats.map_or(0, |stats| stats.event_count),
                uniform_order_count: uniform_stats.map_or(0, |stats| stats.client_order_ids.len()),
                uniform_qty,
                unmatched_event_count: unmatched_stats.map_or(0, |stats| stats.event_count),
                unmatched_order_count: unmatched_stats.map_or(0, |stats| stats.order_count),
                unmatched_qty,
                unmatched_only_order_count: unmatched_only_stats
                    .map_or(0, |stats| stats.order_count),
                unmatched_only_qty,
                unmatched_represented_order_count,
                local_qty,
                total_qty_diff,
                uniform_expected_diff: uniform_qty - uniform_expected,
                status: if total_qty_diff.abs() <= epsilon {
                    "MATCH"
                } else {
                    "MISMATCH"
                },
            }
        })
        .collect()
}

fn build_unmatched_reports(
    pg: &PgData,
    uniform: &BTreeMap<GroupKey, UniformGroup>,
    unmatched: &BTreeMap<OrderKey, UnmatchedOrder>,
    epsilon: f64,
) -> Vec<UnmatchedOrderReport> {
    unmatched
        .iter()
        .map(|(key, local)| {
            let pg_order = pg.orders.get(key);
            let pg_qty = pg_order.map(|order| order.qty);
            let qty_diff = pg_qty.map(|qty| local.qty - qty);
            let represented_in_uniform = unmatched_is_represented(local, uniform);
            let status = match pg_order {
                None => "MISSING_PG",
                Some(order) if order.group != local.group => "FIELD_MISMATCH",
                Some(_) if qty_diff.is_some_and(|diff| diff.abs() > epsilon) => "QTY_MISMATCH",
                Some(_) => "MATCH",
            };
            UnmatchedOrderReport {
                market: key.market.to_string(),
                symbol: key.symbol.clone(),
                order_id: key.order_id.clone(),
                local_side: local.group.side.clone(),
                pg_side: pg_order
                    .map(|order| order.group.side.clone())
                    .unwrap_or_default(),
                local_qty: local.qty,
                pg_qty,
                qty_diff,
                local_event_count: local.event_count,
                pg_trade_count: pg_order.map_or(0, |order| order.trade_count),
                baseline_observed: local.baseline_observed,
                represented_in_uniform,
                client_order_ids: local
                    .client_order_ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join("|"),
                status,
            }
        })
        .collect()
}

fn build_summary(
    args: &Args,
    pg: &PgData,
    uniform: &BTreeMap<GroupKey, UniformGroup>,
    unmatched: &BTreeMap<OrderKey, UnmatchedOrder>,
    groups: &[GroupReport],
    unmatched_reports: &[UnmatchedOrderReport],
) -> Summary {
    let pg_qty = pg.orders.values().map(|order| order.qty).sum::<f64>();
    let uniform_qty = uniform.values().map(|group| group.qty).sum::<f64>();
    let unmatched_raw_qty = unmatched.values().map(|order| order.qty).sum::<f64>();
    let unmatched_represented_qty = unmatched
        .values()
        .filter(|order| unmatched_is_represented(order, uniform))
        .map(|order| order.qty)
        .sum::<f64>();
    let unmatched_qty = unmatched_raw_qty - unmatched_represented_qty;
    let local_qty = uniform_qty + unmatched_qty;
    let unmatched_represented_order_count = unmatched
        .values()
        .filter(|order| unmatched_is_represented(order, uniform))
        .count();
    let mismatched_group_count = groups
        .iter()
        .filter(|group| group.status != "MATCH")
        .count();
    let mismatched_unmatched_order_count = unmatched_reports
        .iter()
        .filter(|order| order.status != "MATCH")
        .count();

    Summary {
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        qty_epsilon: args.qty_epsilon,
        pg_trade_count: pg.trade_count,
        pg_order_count: pg.orders.len(),
        pg_duplicate_rows: pg.duplicate_rows,
        uniform_fill_event_count: uniform.values().map(|group| group.event_count).sum(),
        uniform_order_count: uniform
            .values()
            .map(|group| group.client_order_ids.iter().copied())
            .flatten()
            .collect::<BTreeSet<_>>()
            .len(),
        unmatched_fill_event_count: unmatched.values().map(|order| order.event_count).sum(),
        unmatched_order_count: unmatched.len(),
        unmatched_represented_order_count,
        unmatched_only_order_count: unmatched.len() - unmatched_represented_order_count,
        unmatched_orders_with_pre_start_baseline: unmatched
            .values()
            .filter(|order| order.baseline_observed)
            .count(),
        group_count: groups.len(),
        mismatched_group_count,
        mismatched_unmatched_order_count,
        pg_qty,
        uniform_qty,
        unmatched_raw_qty,
        unmatched_represented_qty,
        unmatched_qty,
        local_qty,
        quantity_diff: local_qty - pg_qty,
        aligned: mismatched_group_count == 0,
        limitation: "uniform_orders has no exchange order_id or trade_id, so matched fills are reconciled only by market, symbol, side, and total base quantity",
    }
}

fn write_group_report(path: &Path, rows: &[GroupReport]) -> Result<()> {
    let mut writer =
        csv::Writer::from_path(path).with_context(|| format!("create {}", path.display()))?;
    for row in rows {
        writer
            .serialize(row)
            .with_context(|| format!("write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))
}

fn write_unmatched_report(path: &Path, rows: &[UnmatchedOrderReport]) -> Result<()> {
    let mut writer =
        csv::Writer::from_path(path).with_context(|| format!("create {}", path.display()))?;
    for row in rows {
        writer
            .serialize(row)
            .with_context(|| format!("write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))
}

fn write_summary(path: &Path, summary: &Summary) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    serde_json::to_writer_pretty(file, summary).with_context(|| format!("write {}", path.display()))
}

fn read_parquet(path: &Path) -> Result<DataFrame> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    ParquetReader::new(file)
        .finish()
        .with_context(|| format!("read parquet {}", path.display()))
}

fn str_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name:?}"))?
        .str()
        .with_context(|| format!("parquet column {name:?} must be string"))
}

fn i64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name:?}"))?
        .i64()
        .with_context(|| format!("parquet column {name:?} must be int64"))
}

fn f64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    df.column(name)
        .with_context(|| format!("missing parquet column {name:?}"))?
        .f64()
        .with_context(|| format!("parquet column {name:?} must be float64"))
}

fn required_str<'a>(column: &'a StringChunked, row: usize, name: &str) -> Result<&'a str> {
    column
        .get(row)
        .with_context(|| format!("parquet column {name:?} is null at row {row}"))
}

fn required_i64(column: &Int64Chunked, row: usize, name: &str) -> Result<i64> {
    column
        .get(row)
        .with_context(|| format!("parquet column {name:?} is null at row {row}"))
}

fn required_f64(column: &Float64Chunked, row: usize, name: &str) -> Result<f64> {
    let value = column
        .get(row)
        .with_context(|| format!("parquet column {name:?} is null at row {row}"))?;
    if !value.is_finite() {
        bail!("parquet column {name:?} is not finite at row {row}: {value}");
    }
    Ok(value)
}

fn normalize_symbol(raw: &str) -> Result<String> {
    let symbol = raw
        .trim()
        .chars()
        .filter(|character| !matches!(character, '-' | '_' | '/'))
        .collect::<String>()
        .to_ascii_uppercase();
    if symbol.is_empty() {
        bail!("empty symbol");
    }
    Ok(symbol)
}

fn normalize_side(raw: &str) -> Result<String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok("BUY".to_string()),
        "SELL" => Ok("SELL".to_string()),
        _ => bail!("unsupported side {raw:?}"),
    }
}

fn nonempty<'a>(raw: &'a str, field: &str) -> Result<&'a str> {
    let value = raw.trim();
    if value.is_empty() {
        bail!("empty {field}");
    }
    Ok(value)
}

fn parse_i64(raw: &str, field: &str) -> Result<i64> {
    raw.trim()
        .parse::<i64>()
        .with_context(|| format!("parse {field} value {raw:?} as i64"))
}

fn parse_nonnegative_f64(raw: &str, field: &str) -> Result<f64> {
    let value = raw
        .trim()
        .parse::<f64>()
        .with_context(|| format!("parse {field} value {raw:?} as f64"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(anyhow!("{field} must be finite and non-negative: {value}"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(side: &str) -> GroupKey {
        GroupKey {
            market: Market::Spot,
            symbol: "BTCUSDT".to_string(),
            side: side.to_string(),
        }
    }

    #[test]
    fn unmatched_cumulative_uses_pre_window_baseline_and_deduplicates() {
        let values = UnmatchedSeries {
            group: group("BUY"),
            client_order_ids: BTreeSet::from([7]),
            observations: vec![
                Observation {
                    ts_us: 900,
                    cumulative_qty: 1.0,
                },
                Observation {
                    ts_us: 1_100,
                    cumulative_qty: 1.5,
                },
                Observation {
                    ts_us: 1_200,
                    cumulative_qty: 1.5,
                },
                Observation {
                    ts_us: 1_300,
                    cumulative_qty: 2.0,
                },
            ],
        };
        let result = summarize_unmatched_series(values, 1_000, 2_000, 1e-8).unwrap();
        assert!((result.qty - 1.0).abs() < 1e-12);
        assert_eq!(result.event_count, 3);
        assert!(result.baseline_observed);
    }

    #[test]
    fn group_report_adds_uniform_and_unmatched_quantities() {
        let order_key = OrderKey {
            market: Market::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: "42".to_string(),
        };
        let mut pg = PgData::default();
        pg.orders.insert(
            order_key.clone(),
            PgOrder {
                group: group("BUY"),
                qty: 3.0,
                trade_count: 2,
            },
        );
        let uniform = BTreeMap::from([(
            group("BUY"),
            UniformGroup {
                qty: 2.0,
                event_count: 1,
                client_order_ids: BTreeSet::from([1]),
            },
        )]);
        let unmatched = BTreeMap::from([(
            order_key,
            UnmatchedOrder {
                group: group("BUY"),
                qty: 1.0,
                event_count: 1,
                baseline_observed: false,
                client_order_ids: BTreeSet::from([2]),
            },
        )]);

        let rows = build_group_reports(&pg, &uniform, &unmatched, 1e-8);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "MATCH");
        assert!((rows[0].local_qty - 3.0).abs() < 1e-12);
    }

    #[test]
    fn group_report_detects_quantity_mismatch() {
        let mut pg = PgData::default();
        pg.orders.insert(
            OrderKey {
                market: Market::Spot,
                symbol: "BTCUSDT".to_string(),
                order_id: "42".to_string(),
            },
            PgOrder {
                group: group("SELL"),
                qty: 1.0,
                trade_count: 1,
            },
        );
        let uniform = BTreeMap::from([(
            group("SELL"),
            UniformGroup {
                qty: 0.9,
                event_count: 1,
                client_order_ids: BTreeSet::from([1]),
            },
        )]);

        let rows = build_group_reports(&pg, &uniform, &BTreeMap::new(), 1e-8);
        assert_eq!(rows[0].status, "MISMATCH");
        assert!((rows[0].total_qty_diff + 0.1).abs() < 1e-12);
    }

    #[test]
    fn group_report_does_not_double_count_unmatched_already_in_uniform() {
        let order_key = OrderKey {
            market: Market::Spot,
            symbol: "BTCUSDT".to_string(),
            order_id: "42".to_string(),
        };
        let mut pg = PgData::default();
        pg.orders.insert(
            order_key.clone(),
            PgOrder {
                group: group("BUY"),
                qty: 2.0,
                trade_count: 1,
            },
        );
        let uniform = BTreeMap::from([(
            group("BUY"),
            UniformGroup {
                qty: 2.0,
                event_count: 1,
                client_order_ids: BTreeSet::from([7]),
            },
        )]);
        let unmatched = BTreeMap::from([(
            order_key,
            UnmatchedOrder {
                group: group("BUY"),
                qty: 2.0,
                event_count: 1,
                baseline_observed: false,
                client_order_ids: BTreeSet::from([7]),
            },
        )]);

        let rows = build_group_reports(&pg, &uniform, &unmatched, 1e-8);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "MATCH");
        assert!((rows[0].local_qty - 2.0).abs() < 1e-12);
        assert_eq!(rows[0].unmatched_represented_order_count, 1);
        assert_eq!(rows[0].unmatched_only_order_count, 0);
    }
}
