//! Bounded, read-only source-time extraction for investigating late TAS prints.
//! No filtering, calendar inference, or correction is applied to source records.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Datelike, Utc};
use clap::Parser;
use cme_tas_replay::daily_inventory::{event_time, Event};
use cme_tas_replay::product::{encode_all_key, product_cf_name, quote_last_merge};
use cme_tas_replay::{
    decode_cme_trade, decode_period_status, PeriodStatus, CF_REPLAY_META, KIND_CME_TRADE,
    MISSING_EXCH_HMS_NS, MISSING_PRICE, PERIOD_META_PREFIX, RIC_LEN,
};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use serde::Serialize;
use std::{fs::OpenOptions, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    year: u16,
    #[arg(long)]
    product: String,
    #[arg(long, value_delimiter = ',')]
    rics: Vec<String>,
    /// Inclusive source Date-Time, UTC nanoseconds.
    #[arg(long)]
    start_ns: u64,
    /// Exclusive source Date-Time, UTC nanoseconds.
    #[arg(long)]
    end_ns: u64,
    #[arg(long, default_value_t = 1_000_000)]
    max_rows: u64,
}
#[derive(Serialize)]
struct Row {
    product: String,
    ric: String,
    published_ns: u64,
    exchange_hms_ns: Option<u64>,
    recovered_event_ns: u64,
    price_e9: Option<i64>,
    volume: u32,
    aggressor: u8,
    part: u16,
    seq: u32,
}
fn opts() -> Options {
    let mut o = Options::default();
    o.set_merge_operator_associative("quote_last", quote_last_merge);
    o.set_max_open_files(128);
    o.set_skip_stats_update_on_db_open(true);
    o
}
fn main() -> Result<()> {
    let a = Args::parse();
    ensure!(
        a.start_ns < a.end_ns && a.max_rows > 0 && !a.rics.is_empty(),
        "invalid scan range"
    );
    for ns in [a.start_ns, a.end_ns - 1] {
        let date = DateTime::<Utc>::from_timestamp((ns / 1_000_000_000) as i64, 0)
            .context("source timestamp outside supported range")?;
        ensure!(
            date.year() == i32::from(a.year),
            "scan must stay inside the selected source year"
        );
    }
    ensure!(
        cme_roll_metadata::exchange(&a.product).is_some(),
        "unsupported product"
    );
    ensure!(!a.output.exists(), "output already exists");
    let name = product_cf_name(a.year, &a.product)?;
    let db = DB::open_cf_descriptors_read_only(
        &opts(),
        &a.rocksdb_dir,
        ["default", CF_REPLAY_META, &name].map(|n| ColumnFamilyDescriptor::new(n, opts())),
        false,
    )?;
    let meta = db
        .cf_handle(CF_REPLAY_META)
        .context("missing replay_meta")?;
    let mut matching = Vec::new();
    for item in db.iterator_cf(&meta, IteratorMode::Start) {
        let (key, value) = item?;
        if let Some(raw) = key.strip_prefix(PERIOD_META_PREFIX.as_bytes()) {
            let period = std::str::from_utf8(raw)?;
            if period.starts_with(&format!("{}-", a.year)) {
                ensure!(
                    decode_period_status(&value)? == PeriodStatus::Done,
                    "source period incomplete"
                );
                matching.push(period.to_owned());
            }
        }
    }
    ensure!(matching.len() == 1, "missing/ambiguous done source year");
    if let Some(parent) = a.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let partial = a.output.with_extension("csv.partial");
    let mut writer = csv::Writer::from_writer(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?,
    );
    // Explicit header also preserves a valid empty extract.
    writer.write_record([
        "product",
        "ric",
        "published_ns",
        "exchange_hms_ns",
        "recovered_event_ns",
        "price_e9",
        "volume",
        "aggressor",
        "part",
        "seq",
    ])?;
    let cf = db.cf_handle(&name).context("missing product partition")?;
    let mut count = 0;
    let mut rics = a.rics.clone();
    rics.sort();
    rics.dedup();
    ensure!(rics.len() == a.rics.len(), "duplicate RIC");
    for ric in &rics {
        let start = encode_all_key(KIND_CME_TRADE, ric, a.start_ns, 0, 0)?;
        let mut iter = db.raw_iterator_cf(&cf);
        iter.seek(start);
        while iter.valid() {
            let key = iter.key().context("missing iterator key")?;
            if !key.starts_with(&start[..1 + RIC_LEN]) {
                break;
            }
            let r = decode_cme_trade(iter.value().context("missing iterator value")?)?;
            if r.ts_utc_ns >= a.end_ns {
                break;
            }
            ensure!(
                r.ts_utc_ns >= a.start_ns && &r.ric == ric,
                "source key/value mismatch"
            );
            count += 1;
            ensure!(
                count <= a.max_rows,
                "scan exceeds --max-rows; partial file retained"
            );
            let e = Event {
                ric: r.ric.clone(),
                published_ns: r.ts_utc_ns,
                exchange_hms_ns: (r.exch_hms_ns != MISSING_EXCH_HMS_NS).then_some(r.exch_hms_ns),
                price_e9: (r.price != MISSING_PRICE).then_some(r.price),
                volume: r.volume as u64,
            };
            let row = Row {
                product: a.product.clone(),
                ric: r.ric,
                published_ns: r.ts_utc_ns,
                exchange_hms_ns: e.exchange_hms_ns,
                recovered_event_ns: event_time(&e)?,
                price_e9: e.price_e9,
                volume: r.volume,
                aggressor: r.aggressor,
                part: u16::from_be_bytes(key[25..27].try_into()?),
                seq: u32::from_be_bytes(key[27..31].try_into()?),
            };
            // Serialize as a tuple to avoid emitting a second automatic header.
            writer.serialize((
                &row.product,
                &row.ric,
                row.published_ns,
                row.exchange_hms_ns,
                row.recovered_event_ns,
                row.price_e9,
                row.volume,
                row.aggressor,
                row.part,
                row.seq,
            ))?;
            iter.next();
        }
        iter.status()?;
    }
    writer.flush()?;
    drop(writer);
    std::fs::rename(partial, &a.output)?;
    let receipt = serde_json::json!({"period":matching[0], "product":a.product, "rics":rics,
        "start_ns":a.start_ns, "end_ns":a.end_ns, "rows":count, "source_time_bounded":true,
        "session_validated":false, "scope":"unmodified source prints; recovered date is diagnostic"});
    std::fs::write(
        a.output.with_extension("json"),
        serde_json::to_vec_pretty(&receipt)?,
    )?;
    eprintln!("{receipt}");
    Ok(())
}
