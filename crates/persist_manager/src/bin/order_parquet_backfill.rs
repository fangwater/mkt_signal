use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, BytesMut};
use clap::Parser;
use log::info;
use order_common::{ExecutionType, OrderStatus, OrderType, Side, TimeInForce, TradingVenue};
use persist_common::{SignalBbo, SignalBboLeg, SIGNAL_BBO_BINARY_LEN};
use persist_manager::sync::center_source_cf_name;
use persist_manager::RocksDbStore;
use polars::prelude::*;

const CF_UNIFORM_ORDERS: &str = "uniform_orders";
const CF_ORDER_UPDATES_UNMATCHED: &str = "order_updates_unmatched";
const CF_TRADE_UPDATES_UNMATCHED: &str = "trade_updates_unmatched";
const SIGNAL_BBO_PARQUET_COLUMNS: [&str; 12] = [
    "signal_open_venue",
    "signal_open_ts",
    "signal_open_bid_price",
    "signal_open_bid_qty",
    "signal_open_ask_price",
    "signal_open_ask_qty",
    "signal_hedge_venue",
    "signal_hedge_ts",
    "signal_hedge_bid_price",
    "signal_hedge_bid_qty",
    "signal_hedge_ask_price",
    "signal_hedge_ask_qty",
];
const STANDARD_CFS: [&str; 3] = [
    CF_UNIFORM_ORDERS,
    CF_ORDER_UPDATES_UNMATCHED,
    CF_TRADE_UPDATES_UNMATCHED,
];

#[derive(Parser, Debug)]
#[command(
    name = "order_parquet_backfill",
    about = "Import order_export parquet files into a persist sync center RocksDB"
)]
struct Args {
    /// Directory containing standard order_export parquet files.
    #[arg(long)]
    data_dir: PathBuf,

    /// Source id used by persist_sync_collector/read_server, e.g. okex-intra-arb01.
    #[arg(long)]
    source_id: String,

    /// Center RocksDB path, e.g. data/persist_sync_center.
    #[arg(long)]
    db_dir: PathBuf,

    /// Optional target CF filter. Repeat to import multiple CFs. Defaults to all 3 standard CFs.
    #[arg(long = "cf", value_parser = parse_cf_name)]
    cf_names: Vec<String>,

    /// Max records per RocksDB write batch.
    #[arg(long, default_value_t = 10_000)]
    batch_rows: usize,

    /// Log progress every N input rows per file.
    #[arg(long, default_value_t = 50_000)]
    progress_rows: usize,

    /// Overwrite existing keys instead of counting them as duplicates and skipping.
    #[arg(long, default_value_t = false)]
    overwrite: bool,

    /// Use RocksDB sync writes.
    #[arg(long, default_value_t = false)]
    sync_writes: bool,
}

#[derive(Debug, Default, Clone)]
struct ImportStats {
    rows: usize,
    inserted: usize,
    overwritten: usize,
    duplicates: usize,
    skipped: usize,
}

impl ImportStats {
    fn add(&mut self, other: &Self) {
        self.rows += other.rows;
        self.inserted += other.inserted;
        self.overwritten += other.overwritten;
        self.duplicates += other.duplicates;
        self.skipped += other.skipped;
    }
}

#[derive(Debug)]
struct ImportJob {
    cf_name: String,
    center_cf: String,
    path: PathBuf,
}

fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = Args::parse();
    run(args)
}

fn run(args: Args) -> Result<()> {
    if !args.data_dir.is_dir() {
        return Err(anyhow!(
            "--data-dir must be an existing directory: {}",
            args.data_dir.display()
        ));
    }
    if args.source_id.trim().is_empty() {
        return Err(anyhow!("--source-id must not be empty"));
    }
    if args.batch_rows == 0 {
        return Err(anyhow!("--batch-rows must be greater than 0"));
    }

    let jobs = build_jobs(&args)?;
    let target_cfs = jobs
        .iter()
        .map(|job| job.center_cf.clone())
        .collect::<Vec<_>>();
    let tuning = persist_manager::default_tuning();
    let store = RocksDbStore::open_with_existing_cfs_and_tuning(
        &args.db_dir.to_string_lossy(),
        &target_cfs,
        args.sync_writes,
        &tuning,
    )
    .with_context(|| format!("failed to open center db {}", args.db_dir.display()))?;

    info!(
        "order parquet backfill start data_dir={} source_id={} db_dir={} cfs={} overwrite={} batch_rows={}",
        args.data_dir.display(),
        args.source_id,
        args.db_dir.display(),
        jobs.iter()
            .map(|job| job.cf_name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        args.overwrite,
        args.batch_rows
    );

    let mut total = ImportStats::default();
    for job in jobs {
        let stats = import_job(&store, &job, &args)?;
        info!(
            "order parquet backfill file done cf={} center_cf={} file={} rows={} inserted={} overwritten={} duplicates={} skipped={}",
            job.cf_name,
            job.center_cf,
            job.path.display(),
            stats.rows,
            stats.inserted,
            stats.overwritten,
            stats.duplicates,
            stats.skipped
        );
        total.add(&stats);
    }

    info!(
        "order parquet backfill complete rows={} inserted={} overwritten={} duplicates={} skipped={}",
        total.rows, total.inserted, total.overwritten, total.duplicates, total.skipped
    );
    Ok(())
}

fn build_jobs(args: &Args) -> Result<Vec<ImportJob>> {
    let cf_names = if args.cf_names.is_empty() {
        STANDARD_CFS.iter().map(|cf| (*cf).to_string()).collect()
    } else {
        args.cf_names.clone()
    };

    let mut jobs = Vec::with_capacity(cf_names.len());
    for cf_name in cf_names {
        let path = resolve_parquet_path(&args.data_dir, &cf_name, args.cf_names.len() == 1)?;
        let center_cf = center_source_cf_name(args.source_id.trim(), &cf_name);
        jobs.push(ImportJob {
            cf_name,
            center_cf,
            path,
        });
    }
    Ok(jobs)
}

fn resolve_parquet_path(
    data_dir: &Path,
    cf_name: &str,
    allow_single_fallback: bool,
) -> Result<PathBuf> {
    let standard = data_dir.join(format!("{cf_name}.parquet"));
    if standard.is_file() {
        return Ok(standard);
    }

    if allow_single_fallback {
        let mut parquet_files = std::fs::read_dir(data_dir)
            .with_context(|| format!("failed to read {}", data_dir.display()))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
            .collect::<Vec<_>>();
        parquet_files.sort();
        if parquet_files.len() == 1 {
            return Ok(parquet_files.remove(0));
        }
    }

    Err(anyhow!(
        "parquet file for cf={} not found at {}",
        cf_name,
        standard.display()
    ))
}

fn import_job(store: &RocksDbStore, job: &ImportJob, args: &Args) -> Result<ImportStats> {
    info!(
        "order parquet backfill file start cf={} center_cf={} file={}",
        job.cf_name,
        job.center_cf,
        job.path.display()
    );

    let file =
        File::open(&job.path).with_context(|| format!("failed to open {}", job.path.display()))?;
    let df = ParquetReader::new(file)
        .finish()
        .with_context(|| format!("failed to read parquet {}", job.path.display()))?;

    match job.cf_name.as_str() {
        CF_UNIFORM_ORDERS => import_uniform_orders(store, job, args, &df),
        CF_ORDER_UPDATES_UNMATCHED => import_order_updates(store, job, args, &df),
        CF_TRADE_UPDATES_UNMATCHED => import_trade_updates(store, job, args, &df),
        _ => Err(anyhow!("unsupported cf: {}", job.cf_name)),
    }
}

fn import_uniform_orders(
    store: &RocksDbStore,
    job: &ImportJob,
    args: &Args,
    df: &DataFrame,
) -> Result<ImportStats> {
    let cols = UniformColumns::new(df)?;
    let mut stats = ImportStats::default();
    let mut writes = Vec::with_capacity(args.batch_rows);

    for row in 0..df.height() {
        stats.rows += 1;
        match encode_uniform_order_row(&cols, row) {
            Ok((key, value)) => queue_write(store, job, args, &mut stats, &mut writes, key, value)?,
            Err(err) => {
                stats.skipped += 1;
                if stats.skipped <= 10 {
                    log::warn!(
                        "order parquet backfill skip cf={} file={} row={} err={err:#}",
                        job.cf_name,
                        job.path.display(),
                        row
                    );
                }
            }
        }
        maybe_log_progress(job, args, &stats);
    }
    flush_writes(store, &mut writes)?;
    Ok(stats)
}

fn import_order_updates(
    store: &RocksDbStore,
    job: &ImportJob,
    args: &Args,
    df: &DataFrame,
) -> Result<ImportStats> {
    let cols = OrderUpdateColumns::new(df)?;
    let mut stats = ImportStats::default();
    let mut writes = Vec::with_capacity(args.batch_rows);

    for row in 0..df.height() {
        stats.rows += 1;
        match encode_order_update_row(&cols, row) {
            Ok((key, value)) => queue_write(store, job, args, &mut stats, &mut writes, key, value)?,
            Err(err) => {
                stats.skipped += 1;
                if stats.skipped <= 10 {
                    log::warn!(
                        "order parquet backfill skip cf={} file={} row={} err={err:#}",
                        job.cf_name,
                        job.path.display(),
                        row
                    );
                }
            }
        }
        maybe_log_progress(job, args, &stats);
    }
    flush_writes(store, &mut writes)?;
    Ok(stats)
}

fn import_trade_updates(
    store: &RocksDbStore,
    job: &ImportJob,
    args: &Args,
    df: &DataFrame,
) -> Result<ImportStats> {
    let cols = TradeUpdateColumns::new(df)?;
    let mut stats = ImportStats::default();
    let mut writes = Vec::with_capacity(args.batch_rows);

    for row in 0..df.height() {
        stats.rows += 1;
        match encode_trade_update_row(&cols, row) {
            Ok((key, value)) => queue_write(store, job, args, &mut stats, &mut writes, key, value)?,
            Err(err) => {
                stats.skipped += 1;
                if stats.skipped <= 10 {
                    log::warn!(
                        "order parquet backfill skip cf={} file={} row={} err={err:#}",
                        job.cf_name,
                        job.path.display(),
                        row
                    );
                }
            }
        }
        maybe_log_progress(job, args, &stats);
    }
    flush_writes(store, &mut writes)?;
    Ok(stats)
}

fn queue_write(
    store: &RocksDbStore,
    job: &ImportJob,
    args: &Args,
    stats: &mut ImportStats,
    writes: &mut Vec<(String, Vec<u8>, Vec<u8>)>,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<()> {
    if store.get(&job.center_cf, &key)?.is_some() {
        if !args.overwrite {
            stats.duplicates += 1;
            return Ok(());
        }
        stats.overwritten += 1;
    } else {
        stats.inserted += 1;
    }

    writes.push((job.center_cf.clone(), key, value));
    if writes.len() >= args.batch_rows {
        flush_writes(store, writes)?;
    }
    Ok(())
}

fn flush_writes(store: &RocksDbStore, writes: &mut Vec<(String, Vec<u8>, Vec<u8>)>) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    store.put_many(writes)?;
    writes.clear();
    Ok(())
}

fn maybe_log_progress(job: &ImportJob, args: &Args, stats: &ImportStats) {
    if args.progress_rows == 0 || stats.rows % args.progress_rows != 0 {
        return;
    }
    info!(
        "order parquet backfill progress cf={} file={} rows={} inserted={} overwritten={} duplicates={} skipped={}",
        job.cf_name,
        job.path.display(),
        stats.rows,
        stats.inserted,
        stats.overwritten,
        stats.duplicates,
        stats.skipped
    );
}

struct UniformColumns<'a> {
    key: &'a StringChunked,
    recv_ts_us: &'a Int64Chunked,
    symbol: &'a StringChunked,
    create_ts: &'a Int64Chunked,
    update_ts: &'a Int64Chunked,
    signal_ts: &'a Int64Chunked,
    submit_ts: &'a Int64Chunked,
    local_ts: &'a Int64Chunked,
    mkt_ts: &'a Int64Chunked,
    client_order_id: &'a Int64Chunked,
    trading_venue: &'a StringChunked,
    order_type: &'a StringChunked,
    side: &'a StringChunked,
    price: &'a Float64Chunked,
    price_offset: &'a Float64Chunked,
    amount_init: &'a Float64Chunked,
    amount_update: &'a Float64Chunked,
    status: &'a StringChunked,
    from_key: &'a StringChunked,
    from_key_hex: &'a StringChunked,
    signal_bbo: Option<SignalBboColumns<'a>>,
    bbo_spread: &'a StringChunked,
}

impl<'a> UniformColumns<'a> {
    fn new(df: &'a DataFrame) -> Result<Self> {
        Ok(Self {
            key: str_col(df, "key")?,
            recv_ts_us: i64_col(df, "recv_ts_us")?,
            symbol: str_col(df, "symbol")?,
            create_ts: i64_col(df, "create_ts")?,
            update_ts: i64_col(df, "update_ts")?,
            signal_ts: i64_col(df, "signal_ts")?,
            submit_ts: i64_col(df, "submit_ts")?,
            local_ts: i64_col(df, "local_ts")?,
            mkt_ts: i64_col(df, "mkt_ts")?,
            client_order_id: i64_col(df, "client_order_id")?,
            trading_venue: str_col(df, "trading_venue")?,
            order_type: str_col(df, "order_type")?,
            side: str_col(df, "side")?,
            price: f64_col(df, "price")?,
            price_offset: f64_col(df, "price_offset")?,
            amount_init: f64_col(df, "amount_init")?,
            amount_update: f64_col(df, "amount_update")?,
            status: str_col(df, "status")?,
            from_key: str_col(df, "from_key")?,
            from_key_hex: str_col(df, "from_key_hex")?,
            signal_bbo: SignalBboColumns::new_optional(df)?,
            bbo_spread: str_col(df, "bbo_spread")?,
        })
    }
}

struct SignalBboColumns<'a> {
    open: SignalBboLegColumns<'a>,
    hedge: SignalBboLegColumns<'a>,
}

impl<'a> SignalBboColumns<'a> {
    fn new_optional(df: &'a DataFrame) -> Result<Option<Self>> {
        let present = SIGNAL_BBO_PARQUET_COLUMNS
            .iter()
            .filter(|name| df.column(name).is_ok())
            .count();
        if present == 0 {
            return Ok(None);
        }
        if present != SIGNAL_BBO_PARQUET_COLUMNS.len() {
            return Err(anyhow!(
                "signal_bbo parquet columns must be absent or complete (found {present}/{})",
                SIGNAL_BBO_PARQUET_COLUMNS.len()
            ));
        }

        Ok(Some(Self {
            open: SignalBboLegColumns::new(df, "signal_open")?,
            hedge: SignalBboLegColumns::new(df, "signal_hedge")?,
        }))
    }

    fn value(&self, row: usize) -> Result<Option<SignalBbo>> {
        Ok(SignalBbo::new(
            self.open.value(row, "signal_open")?,
            self.hedge.value(row, "signal_hedge")?,
        ))
    }
}

struct SignalBboLegColumns<'a> {
    venue: &'a StringChunked,
    ts: &'a Int64Chunked,
    bid_price: &'a Float64Chunked,
    bid_qty: &'a Float64Chunked,
    ask_price: &'a Float64Chunked,
    ask_qty: &'a Float64Chunked,
}

impl<'a> SignalBboLegColumns<'a> {
    fn new(df: &'a DataFrame, prefix: &str) -> Result<Self> {
        Ok(Self {
            venue: str_col(df, &format!("{prefix}_venue"))?,
            ts: i64_col(df, &format!("{prefix}_ts"))?,
            bid_price: f64_col(df, &format!("{prefix}_bid_price"))?,
            bid_qty: f64_col(df, &format!("{prefix}_bid_qty"))?,
            ask_price: f64_col(df, &format!("{prefix}_ask_price"))?,
            ask_qty: f64_col(df, &format!("{prefix}_ask_qty"))?,
        })
    }

    fn value(&self, row: usize, prefix: &str) -> Result<Option<SignalBboLeg>> {
        let venue = self.venue.get(row);
        let ts = self.ts.get(row);
        let bid_price = self.bid_price.get(row);
        let bid_qty = self.bid_qty.get(row);
        let ask_price = self.ask_price.get(row);
        let ask_qty = self.ask_qty.get(row);
        let present = [
            venue.is_some(),
            ts.is_some(),
            bid_price.is_some(),
            bid_qty.is_some(),
            ask_price.is_some(),
            ask_qty.is_some(),
        ]
        .into_iter()
        .filter(|value| *value)
        .count();

        if present == 0 {
            return Ok(None);
        }
        if present != 6 {
            return Err(anyhow!(
                "{prefix} columns must be all null or all populated at row={row}"
            ));
        }

        let venue = parse_venue_code(venue.expect("presence checked"))?;
        SignalBboLeg::checked(
            venue,
            ts.expect("presence checked"),
            bid_price.expect("presence checked"),
            bid_qty.expect("presence checked"),
            ask_price.expect("presence checked"),
            ask_qty.expect("presence checked"),
        )
        .map(Some)
        .ok_or_else(|| anyhow!("invalid {prefix} BBO values at row={row}"))
    }
}

struct OrderUpdateColumns<'a> {
    key: &'a StringChunked,
    ts_us: &'a Int64Chunked,
    event_time: &'a Int64Chunked,
    symbol: &'a StringChunked,
    order_id: &'a Int64Chunked,
    client_order_id: &'a Int64Chunked,
    client_order_id_str: &'a StringChunked,
    side: &'a StringChunked,
    order_type: &'a StringChunked,
    time_in_force: &'a StringChunked,
    price: &'a Float64Chunked,
    quantity: &'a Float64Chunked,
    cumulative_filled_quantity: &'a Float64Chunked,
    status: &'a StringChunked,
    raw_status: &'a StringChunked,
    execution_type: &'a StringChunked,
    raw_execution_type: &'a StringChunked,
    trading_venue: &'a StringChunked,
}

impl<'a> OrderUpdateColumns<'a> {
    fn new(df: &'a DataFrame) -> Result<Self> {
        Ok(Self {
            key: str_col(df, "key")?,
            ts_us: i64_col(df, "ts_us")?,
            event_time: i64_col(df, "event_time")?,
            symbol: str_col(df, "symbol")?,
            order_id: i64_col(df, "order_id")?,
            client_order_id: i64_col(df, "client_order_id")?,
            client_order_id_str: str_col(df, "client_order_id_str")?,
            side: str_col(df, "side")?,
            order_type: str_col(df, "order_type")?,
            time_in_force: str_col(df, "time_in_force")?,
            price: f64_col(df, "price")?,
            quantity: f64_col(df, "quantity")?,
            cumulative_filled_quantity: f64_col(df, "cumulative_filled_quantity")?,
            status: str_col(df, "status")?,
            raw_status: str_col(df, "raw_status")?,
            execution_type: str_col(df, "execution_type")?,
            raw_execution_type: str_col(df, "raw_execution_type")?,
            trading_venue: str_col(df, "trading_venue")?,
        })
    }
}

struct TradeUpdateColumns<'a> {
    key: &'a StringChunked,
    ts_us: &'a Int64Chunked,
    event_time: &'a Int64Chunked,
    trade_time: &'a Int64Chunked,
    symbol: &'a StringChunked,
    order_id: &'a Int64Chunked,
    client_order_id: &'a Int64Chunked,
    side: &'a StringChunked,
    price: &'a Float64Chunked,
    is_maker: &'a BooleanChunked,
    trading_venue: &'a StringChunked,
    cumulative_filled_quantity: &'a Float64Chunked,
    order_status: &'a StringChunked,
}

impl<'a> TradeUpdateColumns<'a> {
    fn new(df: &'a DataFrame) -> Result<Self> {
        Ok(Self {
            key: str_col(df, "key")?,
            ts_us: i64_col(df, "ts_us")?,
            event_time: i64_col(df, "event_time")?,
            trade_time: i64_col(df, "trade_time")?,
            symbol: str_col(df, "symbol")?,
            order_id: i64_col(df, "order_id")?,
            client_order_id: i64_col(df, "client_order_id")?,
            side: str_col(df, "side")?,
            price: f64_col(df, "price")?,
            is_maker: bool_col(df, "is_maker")?,
            trading_venue: str_col(df, "trading_venue")?,
            cumulative_filled_quantity: f64_col(df, "cumulative_filled_quantity")?,
            order_status: str_col(df, "order_status")?,
        })
    }
}

fn encode_uniform_order_row(cols: &UniformColumns<'_>, row: usize) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = required_str(cols.key, row, "key")?.as_bytes().to_vec();
    let symbol = required_str(cols.symbol, row, "symbol")?;
    if symbol.len() > u16::MAX as usize {
        return Err(anyhow!("symbol too long: {} bytes", symbol.len()));
    }
    let from_key = from_key_bytes(cols, row)?;
    let bbo_spread = required_str(cols.bbo_spread, row, "bbo_spread")?;
    if bbo_spread.len() > u16::MAX as usize {
        return Err(anyhow!("bbo_spread too long: {} bytes", bbo_spread.len()));
    }
    let signal_bbo = cols
        .signal_bbo
        .as_ref()
        .map(|columns| columns.value(row))
        .transpose()?;

    let signal_bbo_len = usize::from(cols.signal_bbo.is_some()) * SIGNAL_BBO_BINARY_LEN;
    let mut buf = BytesMut::with_capacity(
        160 + symbol.len() + from_key.len() + bbo_spread.len() + signal_bbo_len,
    );
    buf.put_i64_le(required_i64(cols.recv_ts_us, row, "recv_ts_us")?);
    buf.put_u16_le(symbol.len() as u16);
    buf.put_slice(symbol.as_bytes());
    buf.put_i64_le(required_i64(cols.create_ts, row, "create_ts")?);
    buf.put_i64_le(required_i64(cols.update_ts, row, "update_ts")?);
    buf.put_i64_le(required_i64(cols.signal_ts, row, "signal_ts")?);
    buf.put_i64_le(required_i64(cols.submit_ts, row, "submit_ts")?);
    buf.put_i64_le(required_i64(cols.local_ts, row, "local_ts")?);
    buf.put_i64_le(required_i64(cols.mkt_ts, row, "mkt_ts")?);
    buf.put_i64_le(required_i64(cols.client_order_id, row, "client_order_id")?);
    buf.put_u8(parse_venue_code(required_str(
        cols.trading_venue,
        row,
        "trading_venue",
    )?)?);
    buf.put_u8(parse_order_type_code(required_str(
        cols.order_type,
        row,
        "order_type",
    )?)?);
    buf.put_u8(parse_side_code(required_str(cols.side, row, "side")?)?);
    buf.put_f64_le(required_f64(cols.price, row, "price")?);
    buf.put_f64_le(required_f64(cols.price_offset, row, "price_offset")?);
    buf.put_f64_le(required_f64(cols.amount_init, row, "amount_init")?);
    buf.put_f64_le(required_f64(cols.amount_update, row, "amount_update")?);
    buf.put_u8(parse_uniform_status_code(required_str(
        cols.status,
        row,
        "status",
    )?)?);
    buf.put_u32_le(from_key.len() as u32);
    buf.put_slice(&from_key);
    buf.put_u16_le(bbo_spread.len() as u16);
    buf.put_slice(bbo_spread.as_bytes());
    if cols.signal_bbo.is_some() {
        buf.put_slice(&SignalBbo::encode_optional(signal_bbo.flatten()));
    }
    Ok((key, buf.to_vec()))
}

fn encode_order_update_row(
    cols: &OrderUpdateColumns<'_>,
    row: usize,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = required_str(cols.key, row, "key")?.as_bytes().to_vec();
    let symbol = required_str(cols.symbol, row, "symbol")?;
    let raw_status = required_str(cols.raw_status, row, "raw_status")?;
    let raw_execution_type = required_str(cols.raw_execution_type, row, "raw_execution_type")?;

    let mut buf =
        BytesMut::with_capacity(192 + symbol.len() + raw_status.len() + raw_execution_type.len());
    buf.put_i64_le(required_i64(cols.ts_us, row, "ts_us")?);
    buf.put_i64_le(required_i64(cols.event_time, row, "event_time")?);
    put_string(&mut buf, symbol);
    buf.put_i64_le(required_i64(cols.order_id, row, "order_id")?);
    buf.put_i64_le(required_i64(cols.client_order_id, row, "client_order_id")?);
    put_opt_string(&mut buf, cols.client_order_id_str.get(row));
    buf.put_u8(parse_side_code(required_str(cols.side, row, "side")?)?);
    buf.put_u8(parse_order_type_code(required_str(
        cols.order_type,
        row,
        "order_type",
    )?)?);
    buf.put_u8(parse_time_in_force_code(required_str(
        cols.time_in_force,
        row,
        "time_in_force",
    )?)?);
    buf.put_f64_le(required_f64(cols.price, row, "price")?);
    buf.put_f64_le(required_f64(cols.quantity, row, "quantity")?);
    buf.put_f64_le(required_f64(
        cols.cumulative_filled_quantity,
        row,
        "cumulative_filled_quantity",
    )?);
    buf.put_u8(parse_order_update_status_code(required_str(
        cols.status,
        row,
        "status",
    )?)?);
    put_string(&mut buf, raw_status);
    buf.put_u8(parse_execution_type_code(required_str(
        cols.execution_type,
        row,
        "execution_type",
    )?)?);
    put_string(&mut buf, raw_execution_type);
    buf.put_u8(parse_venue_code(required_str(
        cols.trading_venue,
        row,
        "trading_venue",
    )?)?);
    Ok((key, buf.to_vec()))
}

fn encode_trade_update_row(
    cols: &TradeUpdateColumns<'_>,
    row: usize,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = required_str(cols.key, row, "key")?.as_bytes().to_vec();
    let symbol = required_str(cols.symbol, row, "symbol")?;

    let mut buf = BytesMut::with_capacity(128 + symbol.len());
    buf.put_i64_le(required_i64(cols.ts_us, row, "ts_us")?);
    buf.put_i64_le(required_i64(cols.event_time, row, "event_time")?);
    buf.put_i64_le(required_i64(cols.trade_time, row, "trade_time")?);
    put_string(&mut buf, symbol);
    buf.put_i64_le(required_i64(cols.order_id, row, "order_id")?);
    buf.put_i64_le(required_i64(cols.client_order_id, row, "client_order_id")?);
    buf.put_u8(parse_side_code(required_str(cols.side, row, "side")?)?);
    buf.put_f64_le(required_f64(cols.price, row, "price")?);
    buf.put_u8(required_bool(cols.is_maker, row, "is_maker")? as u8);
    buf.put_u8(parse_venue_code(required_str(
        cols.trading_venue,
        row,
        "trading_venue",
    )?)?);
    buf.put_f64_le(required_f64(
        cols.cumulative_filled_quantity,
        row,
        "cumulative_filled_quantity",
    )?);
    if let Some(status) = cols.order_status.get(row) {
        buf.put_u8(1);
        buf.put_u8(parse_order_update_status_code(status)?);
    } else {
        buf.put_u8(0);
    }
    Ok((key, buf.to_vec()))
}

fn from_key_bytes(cols: &UniformColumns<'_>, row: usize) -> Result<Vec<u8>> {
    let hex_value = required_str(cols.from_key_hex, row, "from_key_hex")?;
    if !hex_value.is_empty() {
        return hex::decode(hex_value)
            .with_context(|| format!("invalid from_key_hex at row={row}"));
    }
    Ok(required_str(cols.from_key, row, "from_key")?
        .as_bytes()
        .to_vec())
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32_le(value.len() as u32);
    buf.put_slice(value.as_bytes());
}

fn put_opt_string(buf: &mut BytesMut, value: Option<&str>) {
    if let Some(value) = value {
        buf.put_u8(1);
        put_string(buf, value);
    } else {
        buf.put_u8(0);
    }
}

fn str_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    df.column(name)
        .with_context(|| format!("missing column {name}"))?
        .str()
        .with_context(|| format!("column {name} must be string"))
}

fn i64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    df.column(name)
        .with_context(|| format!("missing column {name}"))?
        .i64()
        .with_context(|| format!("column {name} must be int64"))
}

fn f64_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    df.column(name)
        .with_context(|| format!("missing column {name}"))?
        .f64()
        .with_context(|| format!("column {name} must be float64"))
}

fn bool_col<'a>(df: &'a DataFrame, name: &str) -> Result<&'a BooleanChunked> {
    df.column(name)
        .with_context(|| format!("missing column {name}"))?
        .bool()
        .with_context(|| format!("column {name} must be bool"))
}

fn required_str<'a>(col: &'a StringChunked, row: usize, name: &str) -> Result<&'a str> {
    col.get(row)
        .ok_or_else(|| anyhow!("column {name} is null at row={row}"))
}

fn required_i64(col: &Int64Chunked, row: usize, name: &str) -> Result<i64> {
    col.get(row)
        .ok_or_else(|| anyhow!("column {name} is null at row={row}"))
}

fn required_f64(col: &Float64Chunked, row: usize, name: &str) -> Result<f64> {
    let value = col
        .get(row)
        .ok_or_else(|| anyhow!("column {name} is null at row={row}"))?;
    if !value.is_finite() {
        return Err(anyhow!("column {name} is not finite at row={row}: {value}"));
    }
    Ok(value)
}

fn required_bool(col: &BooleanChunked, row: usize, name: &str) -> Result<bool> {
    col.get(row)
        .ok_or_else(|| anyhow!("column {name} is null at row={row}"))
}

fn parse_cf_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if STANDARD_CFS.iter().any(|cf| *cf == trimmed) {
        return Ok(trimmed.to_string());
    }
    Err(anyhow!("unsupported cf: {raw}"))
}

fn parse_venue_code(raw: &str) -> Result<u8> {
    let venue = match raw {
        "BinanceMargin" => TradingVenue::BinanceMargin,
        "BinanceFutures" => TradingVenue::BinanceFutures,
        "OkexMargin" => TradingVenue::OkexMargin,
        "OkexFutures" => TradingVenue::OkexFutures,
        "BybitMargin" => TradingVenue::BybitMargin,
        "BybitFutures" => TradingVenue::BybitFutures,
        "BitgetMargin" => TradingVenue::BitgetMargin,
        "BitgetFutures" => TradingVenue::BitgetFutures,
        "GateMargin" => TradingVenue::GateMargin,
        "GateFutures" => TradingVenue::GateFutures,
        "AsterMargin" => TradingVenue::AsterMargin,
        "AsterFutures" => TradingVenue::AsterFutures,
        "HyperliquidMargin" => TradingVenue::HyperliquidMargin,
        "HyperliquidFutures" => TradingVenue::HyperliquidFutures,
        _ => return Err(anyhow!("unknown trading_venue: {raw}")),
    };
    Ok(venue.to_u8())
}

fn parse_side_code(raw: &str) -> Result<u8> {
    Side::from_str(raw)
        .map(Side::to_u8)
        .ok_or_else(|| anyhow!("unknown side: {raw}"))
}

fn parse_order_type_code(raw: &str) -> Result<u8> {
    OrderType::from_str(raw)
        .map(OrderType::to_u8)
        .ok_or_else(|| anyhow!("unknown order_type: {raw}"))
}

fn parse_time_in_force_code(raw: &str) -> Result<u8> {
    TimeInForce::from_str(raw)
        .map(TimeInForce::to_u8)
        .ok_or_else(|| anyhow!("unknown time_in_force: {raw}"))
}

fn parse_uniform_status_code(raw: &str) -> Result<u8> {
    OrderStatus::from_str(raw)
        .map(OrderStatus::to_u8)
        .ok_or_else(|| anyhow!("unknown status: {raw}"))
}

fn parse_order_update_status_code(raw: &str) -> Result<u8> {
    let status = OrderStatus::from_str(raw).ok_or_else(|| anyhow!("unknown status: {raw}"))?;
    Ok(match status {
        OrderStatus::New => 0,
        OrderStatus::PartiallyFilled => 1,
        OrderStatus::Filled => 2,
        OrderStatus::Canceled => 3,
        OrderStatus::Expired => 4,
        OrderStatus::ExpiredInMatch => 5,
    })
}

fn parse_execution_type_code(raw: &str) -> Result<u8> {
    let execution_type =
        ExecutionType::from_str(raw).ok_or_else(|| anyhow!("unknown execution_type: {raw}"))?;
    Ok(match execution_type {
        ExecutionType::New => 0,
        ExecutionType::Canceled => 1,
        ExecutionType::Replaced => 2,
        ExecutionType::Rejected => 3,
        ExecutionType::Trade => 4,
        ExecutionType::Expired => 5,
        ExecutionType::TradePrevention => 6,
    })
}
