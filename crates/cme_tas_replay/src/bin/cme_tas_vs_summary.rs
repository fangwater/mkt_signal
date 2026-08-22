//! Compare RocksDB `cme_trade` bars with source summarised 1-minute OHLC.
//!
//! Printable trades only. Special-only Summary minutes are expected to be
//! approximate (volume without OHLC). Unknown Type / bad numbers panic.

use anyhow::{bail, Context, Result};
use clap::Parser;
use cme_tas_replay::{
    compare_priced_minute, decode_cme_special, decode_cme_trade, decode_ric, encode_key,
    key_ts_utc_ns, parse_date_time_ns, parse_price_e9, parse_volume, synthesize_1min_bars,
    CompareVerdict, SlimTrade, SynthBar, CF_CME_SPECIAL, CF_CME_TRADE, KEY_LEN, MISSING_PRICE,
    RIC_LEN,
};
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use log::{error, info};
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "cme_tas_vs_summary")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_vs_summary.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompareConfig {
    rocksdb_dir: PathBuf,
    summary_path: PathBuf,
    rics: Vec<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    /// Secondary instance so this tool never takes the replay lock.
    #[serde(default = "default_secondary_dir")]
    secondary_dir: PathBuf,
}

fn default_secondary_dir() -> PathBuf {
    PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb.secondary")
}

fn open_rocksdb_secondary(primary: &Path, secondary: &Path) -> Result<DB> {
    if !primary.exists() {
        bail!("rocksdb {} does not exist", primary.display());
    }
    if let Some(parent) = secondary.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create secondary parent {}", parent.display()))?;
    }
    let mut db_opts = Options::default();
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);
    db_opts.set_max_open_files(-1);
    let names = DB::list_cf(&db_opts, primary)
        .with_context(|| format!("list column families {}", primary.display()))?;
    if !names.iter().any(|name| name == CF_CME_TRADE) {
        bail!(
            "rocksdb {} has no {CF_CME_TRADE} column family",
            primary.display()
        );
    }
    let db = DB::open_cf_as_secondary(&db_opts, primary, secondary, &names).with_context(|| {
        format!(
            "open rocksdb secondary {} from {}",
            secondary.display(),
            primary.display()
        )
    })?;
    db.try_catch_up_with_primary()
        .context("catch up rocksdb secondary")?;
    Ok(db)
}

fn scan_cf(
    db: &DB,
    cf_name: &str,
    ric: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    decode: fn(&[u8]) -> Result<SlimTrade>,
) -> Result<Vec<SlimTrade>> {
    let Some(cf) = db.cf_handle(cf_name) else {
        if cf_name == CF_CME_SPECIAL {
            return Ok(Vec::new());
        }
        bail!("column family {cf_name} missing");
    };
    let start_key = encode_key(ric, start_ns.unwrap_or(0), 0, 0)?;
    let iter = db.iterator_cf(cf, IteratorMode::From(&start_key, Direction::Forward));
    let mut rows = Vec::new();
    for item in iter {
        let (key, value) = item.with_context(|| format!("scan {cf_name}"))?;
        if key.len() != KEY_LEN {
            bail!("{cf_name} key length {} is not {KEY_LEN}", key.len());
        }
        let key_ric = decode_ric(&key[..RIC_LEN])?;
        if key_ric != ric {
            break;
        }
        let ts = key_ts_utc_ns(&key)?;
        if end_ns.is_some_and(|end| ts >= end) {
            break;
        }
        let rec = decode(&value)?;
        if rec.ric != ric {
            bail!("{cf_name} value ric {} does not match key {ric}", rec.ric);
        }
        if rec.ts_utc_ns != ts {
            bail!(
                "{cf_name} value ts {} does not match key {ts} for {ric}",
                rec.ts_utc_ns
            );
        }
        rows.push(rec);
    }
    Ok(rows)
}

fn scan_trades(
    db: &DB,
    ric: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<Vec<SlimTrade>> {
    scan_cf(db, CF_CME_TRADE, ric, start_ns, end_ns, decode_cme_trade)
}

fn scan_specials(
    db: &DB,
    ric: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<Vec<SlimTrade>> {
    scan_cf(
        db,
        CF_CME_SPECIAL,
        ric,
        start_ns,
        end_ns,
        decode_cme_special,
    )
}

fn cell<'a>(record: &'a StringRecord, headers: &BTreeMap<String, usize>, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|&idx| record.get(idx))
        .map(str::trim)
        .unwrap_or("")
}

fn required<'a>(
    record: &'a StringRecord,
    headers: &BTreeMap<String, usize>,
    name: &str,
) -> Result<&'a str> {
    let value = cell(record, headers, name);
    if value.is_empty() {
        bail!("unhandled empty Summary field {name:?}");
    }
    Ok(value)
}

fn load_summary_bars(
    path: &Path,
    wanted: &BTreeMap<String, ()>,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<BTreeMap<(String, u64), SynthBar>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(8 * 1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(decoder);
    let header = reader.headers().context("read Summary header")?.clone();
    let mut headers = BTreeMap::new();
    for (idx, name) in header.iter().enumerate() {
        headers.insert(name.to_string(), idx);
    }
    for name in ["#RIC", "Date-Time", "Type"] {
        if !headers.contains_key(name) {
            bail!("Summary header missing {name}");
        }
    }

    let mut bars = BTreeMap::new();
    let mut seen_wanted = 0u64;
    for record in reader.records() {
        let record = record.with_context(|| format!("read Summary row from {}", path.display()))?;
        let ric = required(&record, &headers, "#RIC")?;
        if !wanted.contains_key(ric) {
            if seen_wanted > 0 {
                if let Some(last) = wanted.keys().next_back() {
                    if ric.as_bytes() > last.as_bytes() {
                        break;
                    }
                }
            }
            continue;
        }
        seen_wanted += 1;
        let event_type = required(&record, &headers, "Type")?;
        if event_type != "Intraday 1Min" {
            bail!("unhandled Summary Type {event_type:?} for {ric}");
        }
        let ts = parse_date_time_ns(required(&record, &headers, "Date-Time")?)?;
        if start_ns.is_some_and(|start| ts < start) {
            continue;
        }
        if end_ns.is_some_and(|end| ts >= end) {
            continue;
        }
        let open = cell(&record, &headers, "Open");
        let high = cell(&record, &headers, "High");
        let low = cell(&record, &headers, "Low");
        let last = cell(&record, &headers, "Last");
        let volume = cell(&record, &headers, "Volume");
        let no_trades = cell(&record, &headers, "No. Trades");
        let priced = !open.is_empty() || !high.is_empty() || !low.is_empty() || !last.is_empty();
        if !priced && volume.is_empty() {
            continue;
        }
        if priced && (open.is_empty() || high.is_empty() || low.is_empty() || last.is_empty()) {
            bail!("Summary {ric} {ts} has a partial OHLC");
        }
        let bar = SynthBar {
            ric: ric.to_string(),
            minute_utc_ns: ts,
            open: if priced {
                parse_price_e9(open)?
            } else {
                MISSING_PRICE
            },
            high: if priced {
                parse_price_e9(high)?
            } else {
                MISSING_PRICE
            },
            low: if priced {
                parse_price_e9(low)?
            } else {
                MISSING_PRICE
            },
            last: if priced {
                parse_price_e9(last)?
            } else {
                MISSING_PRICE
            },
            volume: if volume.is_empty() {
                0
            } else {
                u64::from(parse_volume(volume)?)
            },
            no_trades: if no_trades.is_empty() {
                0
            } else {
                parse_volume(no_trades)?
            },
        };
        if bars.insert((ric.to_string(), ts), bar).is_some() {
            bail!("duplicate Summary minute {ric} {ts}");
        }
    }
    Ok(bars)
}

fn compare_ric(
    db: &DB,
    summary: &BTreeMap<(String, u64), SynthBar>,
    ric: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<()> {
    let trades = scan_trades(db, ric, start_ns, end_ns)?;
    let specials = scan_specials(db, ric, start_ns, end_ns)?;
    let synth = synthesize_1min_bars(&trades);
    let special_volume_by_minute = {
        let mut map = BTreeMap::<u64, u64>::new();
        for rec in &specials {
            *map.entry(cme_tas_replay::minute_left_edge_ns(rec.ts_utc_ns))
                .or_insert(0) += u64::from(rec.volume);
        }
        map
    };
    let mut n_exact = 0usize;
    let mut n_approx = 0usize;
    let mut n_mismatch = 0usize;
    let mut n_missing = 0usize;
    let mut n_volume_only = 0usize;

    let mut summary_minutes: BTreeMap<u64, &SynthBar> = BTreeMap::new();
    for ((got, minute), bar) in summary {
        if got == ric {
            summary_minutes.insert(*minute, bar);
        }
    }

    let mut synth_minutes: BTreeMap<u64, &SynthBar> = BTreeMap::new();
    for bar in &synth {
        synth_minutes.insert(bar.minute_utc_ns, bar);
    }

    let minutes: Vec<u64> = synth_minutes
        .keys()
        .copied()
        .chain(summary_minutes.keys().copied())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    for minute in minutes {
        match (synth_minutes.get(&minute), summary_minutes.get(&minute)) {
            (Some(left), Some(right)) => {
                if right.open == MISSING_PRICE {
                    n_volume_only += 1;
                    n_approx += 1;
                    info!(
                        "{ric} {} volume-only summary volume={} synth_volume={} special_volume={} synth_trades={}",
                        minute_label(minute),
                        right.volume,
                        left.volume,
                        special_volume_by_minute.get(&minute).copied().unwrap_or(0),
                        left.no_trades
                    );
                    continue;
                }
                let special_vol = special_volume_by_minute.get(&minute).copied().unwrap_or(0);
                let row = compare_priced_minute(left, right, special_vol);
                match row.verdict {
                    CompareVerdict::Exact => n_exact += 1,
                    CompareVerdict::Approximate => {
                        n_approx += 1;
                        info!(
                            "{ric} {} volume differs by specials special_volume={special_vol}",
                            minute_label(minute)
                        );
                    }
                    CompareVerdict::Mismatch => {
                        n_mismatch += 1;
                        info!(
                            "{ric} {} leftover priced {:?}",
                            minute_label(minute),
                            row.deltas
                                .iter()
                                .map(|d| format!(
                                    "{} synth={:?} summary={:?}",
                                    d.name, d.synth, d.summary
                                ))
                                .collect::<Vec<_>>()
                        );
                    }
                    CompareVerdict::MissingSide => unreachable!(),
                }
            }
            (Some(left), None) => {
                n_missing += 1;
                info!(
                    "{ric} {} synth-only volume={} trades={}",
                    minute_label(minute),
                    left.volume,
                    left.no_trades
                );
            }
            (None, Some(right)) => {
                if right.open == MISSING_PRICE {
                    n_volume_only += 1;
                    n_approx += 1;
                    info!(
                        "{ric} {} summary-only special volume={} trades={}",
                        minute_label(minute),
                        right.volume,
                        right.no_trades
                    );
                } else {
                    n_missing += 1;
                    info!(
                        "{ric} {} summary-only priced volume={} trades={}",
                        minute_label(minute),
                        right.volume,
                        right.no_trades
                    );
                }
            }
            (None, None) => {}
        }
    }

    let priced_summary = summary_minutes
        .values()
        .filter(|bar| bar.open != MISSING_PRICE)
        .count();
    println!(
        "cme_tas_vs_summary ric={ric} trades={} specials={} synth_minutes={} summary_minutes={} priced_summary={priced_summary} exact={n_exact} special_volume={n_approx} leftover={n_mismatch} volume_only_summary={n_volume_only} missing_side={n_missing}",
        trades.len(),
        specials.len(),
        synth.len(),
        summary_minutes.len()
    );
    if n_missing > 0 {
        bail!("{ric} has {n_missing} priced minutes missing on one side");
    }
    if n_mismatch > 0 {
        bail!("{ric} has {n_mismatch} leftover priced OHLC/Volume mismatches after Special accounting");
    }
    Ok(())
}

fn minute_label(ns: u64) -> String {
    let secs = (ns / 1_000_000_000) as i64;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ns.to_string())
}

fn run(config: &CompareConfig) -> Result<()> {
    if config.rics.is_empty() {
        bail!("cme_tas_vs_summary requires at least one RIC");
    }
    let start_ns = config
        .start
        .as_deref()
        .map(parse_date_time_ns)
        .transpose()?;
    let end_ns = config.end.as_deref().map(parse_date_time_ns).transpose()?;
    let wanted: BTreeMap<String, ()> = config.rics.iter().cloned().map(|r| (r, ())).collect();
    info!(
        "cme_tas_vs_summary rocksdb={} summary={} rics={:?}",
        config.rocksdb_dir.display(),
        config.summary_path.display(),
        config.rics
    );
    let db = open_rocksdb_secondary(&config.rocksdb_dir, &config.secondary_dir)?;
    let summary = load_summary_bars(&config.summary_path, &wanted, start_ns, end_ns)?;
    for ric in &config.rics {
        compare_ric(&db, &summary, ric, start_ns, end_ns)?;
    }
    Ok(())
}

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("cme_tas_vs_summary=info"),
    )
    .format_timestamp_secs()
    .init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config).unwrap_or_else(|err| {
        panic!("read compare config {}: {err}", args.config.display());
    });
    let config: CompareConfig = toml::from_str(&content).unwrap_or_else(|err| {
        panic!("parse compare config {}: {err}", args.config.display());
    });
    if let Err(err) = run(&config) {
        error!("cme_tas_vs_summary panic: {err:?}");
        panic!("{err:?}");
    }
}
