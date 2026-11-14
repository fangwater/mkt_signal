//! Bounded per-RIC endpoint coverage, not a full database verification scan.
use anyhow::{bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ll2_1min::{decode_ll2_minute, ll2_latest_merge};
use cme_tas_replay::raw_ll2::raw_cf;
use cme_tas_replay::raw_ll2_io::Config;
use cme_tas_replay::{encode_ric, period_meta_key, CF_REPLAY_META, MISSING_PRICE};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    normalized_db: Option<PathBuf>,
    #[arg(long)]
    normalized_inventory: Option<PathBuf>,
    #[arg(long)]
    output_root: PathBuf,
}
fn open(path: &Path, normalized: bool) -> Result<DB> {
    let mut opts = Options::default();
    opts.set_max_open_files(65536);
    let names = DB::list_cf(&opts, path)?;
    let desc = names.into_iter().map(|n| {
        let mut o = Options::default();
        if normalized && n.starts_with("p:") {
            o.set_merge_operator_associative("ll2_latest", ll2_latest_merge);
        }
        ColumnFamilyDescriptor::new(n, o)
    });
    Ok(DB::open_cf_descriptors_read_only(&opts, path, desc, false)?)
}
fn endpoint(db: &DB, name: &str, ric: &str, start: u64, end: u64, raw: bool) -> Result<Value> {
    let Some(cf) = db.cf_handle(name) else {
        return Ok(Value::Null);
    };
    let prefix = encode_ric(ric)?;
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(&prefix);
    let mut iter = db.raw_iterator_cf(&cf);
    let mut points = Vec::new();
    for (ts, last) in [(start, false), (end - 1, true)] {
        key[16..].copy_from_slice(&ts.to_be_bytes());
        if last {
            iter.seek_for_prev(key);
        } else {
            iter.seek(key);
        }
        iter.status()?;
        let Some(k) = iter.key() else {
            return Ok(Value::Null);
        };
        if k.len() != 24 || k[..16] != prefix {
            return Ok(Value::Null);
        }
        let bucket = u64::from_be_bytes(k[16..].try_into()?);
        if bucket < start || bucket >= end {
            return Ok(Value::Null);
        }
        let v = decode_ll2_minute(iter.value().context("endpoint value")?)?;
        let known = if raw {
            let q = db.cf_handle(&format!("q:{name}")).context("quality CF")?;
            let b = db.get_cf(&q, k)?.context("endpoint quality")?;
            u64::from_le_bytes(b.as_slice().try_into()?)
        } else {
            (1u64 << 61) - 1
        };
        let mut valid_depth = 0;
        for l in 0..10 {
            let mask = 27u64 << (l * 6); // bid/ask price + size, matching baseline donors
            let prices = [v.bid_prices[l], v.ask_prices[l]];
            let sizes = [v.bid_sizes[l], v.ask_sizes[l]];
            if known & mask != mask
                || prices.iter().any(|x| *x == MISSING_PRICE || *x <= 0)
                || sizes.iter().any(|x| *x == MISSING_PRICE || *x < 0)
                || v.ask_prices[l] < v.bid_prices[l]
            {
                break;
            }
            valid_depth += 1;
        }
        points.push(
            json!({"bucket_utc_ns":bucket,"source_utc_ns":v.source_ts_utc_ns,
            "known":known,"ten_level_fields_known":known & ((1u64<<60)-1) == (1u64<<60)-1,
            "valid_depth_levels":valid_depth}),
        );
    }
    Ok(json!({"first":points[0],"last":points[1]}))
}
fn range(period: &str) -> Result<(u64, u64)> {
    let (a, b) = period.split_once('_').context("period")?;
    let ns = |s: &str| -> Result<u64> {
        Ok(chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")?
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp() as u64
            * 1_000_000_000)
    };
    Ok((ns(a)?, ns(b)?))
}
fn dump_csv(path: &Path, rows: &[Value]) -> Result<()> {
    let Some(first) = rows.first() else {
        fs::write(path, "")?;
        return Ok(());
    };
    let columns: Vec<_> = first.as_object().unwrap().keys().cloned().collect();
    let mut writer = csv::Writer::from_path(path)?;
    writer.write_record(&columns)?;
    for row in rows {
        writer.write_record(columns.iter().map(|c| match &row[c] {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            v => v.to_string(),
        }))?;
    }
    writer.flush()?;
    Ok(())
}
fn run() -> Result<()> {
    let args = Args::parse();
    let cfg: Config = toml::from_str(&fs::read_to_string(&args.config)?)?;
    if cfg.periods.len() != 1 {
        bail!("choose one source period")
    }
    let period = &cfg.periods[0];
    let (start, end) = range(period)?;
    let year = period[..4].parse()?;
    let inv: Value = serde_json::from_slice(&fs::read(&cfg.inventory)?)?;
    let p = inv["packages"]
        .as_array()
        .context("packages")?
        .iter()
        .find(|p| p["period"] == *period)
        .context("source period")?;
    let db = open(&cfg.rocksdb_dir, false)?;
    let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    let watermark = db
        .get_cf(&meta, period_meta_key(period)?)?
        .context("watermark")?;
    if watermark != b"done" {
        bail!("source period is not write-complete")
    }
    let audit: Value = serde_json::from_slice(
        &db.get_cf(&meta, format!("audit:{period}"))?
            .context("audit")?,
    )?;
    let norm = args
        .normalized_db
        .as_ref()
        .map(|p| open(p, true))
        .transpose()?;
    let norm_inv: Option<Value> = args
        .normalized_inventory
        .as_ref()
        .map(|p| -> Result<Value> { Ok(serde_json::from_slice(&fs::read(p)?)?) })
        .transpose()?;
    let mut norm_records = BTreeMap::new();
    if let Some(inv) = &norm_inv {
        for p in inv["packages"].as_array().context("normalized packages")? {
            if p["period"] != *period {
                continue;
            }
            for values in p["products"]
                .as_object()
                .context("normalized products")?
                .values()
            {
                for r in values["active_rics"].as_array().context("active_rics")? {
                    norm_records.insert(r["ric"].as_str().context("RIC")?.to_string(), r.clone());
                }
            }
        }
    }
    let mut normalized_watermark = Value::Null;
    if let Some(db) = &norm {
        let m = db.cf_handle(CF_REPLAY_META).context("normalized meta")?;
        if let Some(s) = db.get_cf(&m, period_meta_key(period)?)? {
            normalized_watermark = String::from_utf8(s)?.into();
        }
    }
    let mut rows = Vec::new();
    let mut products = Vec::new();
    for (product, values) in p["products"].as_object().context("products")? {
        let mut group = Vec::new();
        for r in values["active_rics"].as_array().context("RICs")? {
            let ric = r["ric"].as_str().context("RIC")?;
            let one = endpoint(&db, &raw_cf(year, product, 60)?, ric, start, end, true)?;
            let fifteen = endpoint(&db, &raw_cf(year, product, 15)?, ric, start, end, true)?;
            let n = norm
                .as_ref()
                .map(|d| endpoint(d, &raw_cf(year, product, 60)?, ric, start, end, false))
                .transpose()?
                .unwrap_or(Value::Null);
            let mut row = json!({"product":product,"ric":ric,"source_write_complete":true,
                "source_message_count":r["count"],"source_start_utc":r["start"],"source_end_utc":r["end"],
                "raw_15s_present":!fifteen.is_null(),"raw_1min_present":!one.is_null(),
                "raw_snapshot_present":!fifteen.is_null() || !one.is_null(),
                "raw_minute_derivable":!fifteen.is_null() || !one.is_null(),
                "configured_frequency_present":if audit["frequency"] == 15 {!fifteen.is_null()} else {!one.is_null()},
                "normalized_1min_present":!n.is_null(),
                "normalized_report_count":norm_records.get(ric).map(|v|v["count"].clone()).unwrap_or(json!(0))});
            for (label, point) in [
                ("raw_15s", fifteen),
                ("raw_1min", one),
                ("normalized_1min", n),
            ] {
                for (suffix, field) in [("first_bucket_ns", "first"), ("last_bucket_ns", "last")] {
                    row[format!("{label}_{suffix}")] = point[field]["bucket_utc_ns"].clone();
                    row[format!("{label}_{}_bucket_utc", field)] = point[field]["bucket_utc_ns"]
                        .as_u64()
                        .and_then(|ns| {
                            chrono::DateTime::from_timestamp(
                                (ns / 1_000_000_000) as i64,
                                (ns % 1_000_000_000) as u32,
                            )
                        })
                        .map(|t| Value::String(t.to_rfc3339()))
                        .unwrap_or(Value::Null);
                }
                row[format!("{label}_last_known_mask")] = point["last"]["known"].clone();
                row[format!("{label}_last_10_level_fields_known")] =
                    point["last"]["ten_level_fields_known"].clone();
                row[format!("{label}_first_valid_depth_levels")] =
                    point["first"]["valid_depth_levels"].clone();
                row[format!("{label}_last_valid_depth_levels")] =
                    point["last"]["valid_depth_levels"].clone();
            }
            group.push(row);
        }
        let count = |key: &str| group.iter().filter(|r| r[key] == true).count();
        products.push(json!({"product":product,"source_contracts":group.len(),
            "raw_15s_contracts":count("raw_15s_present"),"raw_1min_contracts":count("raw_1min_present"),"raw_minute_derivable_contracts":count("raw_minute_derivable"),
            "normalized_1min_contracts":count("normalized_1min_present"),
            "raw_1min_snapshot_rows":audit["snapshot_counts"][raw_cf(year,product,60)?],
            "raw_15s_snapshot_rows":audit["snapshot_counts"][raw_cf(year,product,15)?],
            "raw_last_10_level_fields_known_contracts":count("raw_15s_last_10_level_fields_known"),
            "raw_additional_contracts":group.iter().filter(|r|r["raw_snapshot_present"] == true && r["normalized_1min_present"] != true).count(),
            "raw_ric_list":group.iter().filter(|r|r["raw_snapshot_present"] == true).map(|r|r["ric"].as_str().unwrap()).collect::<Vec<_>>().join(" ") }));
        rows.extend(group);
    }
    fs::create_dir_all(&args.output_root)?;
    dump_csv(&args.output_root.join("contracts.csv"), &rows)?;
    dump_csv(&args.output_root.join("products.csv"), &products)?;
    let completed: Vec<_> = rows
        .iter()
        .filter(|r| r["configured_frequency_present"] == true)
        .cloned()
        .collect();
    dump_csv(
        &args.output_root.join("raw_completed_contracts.csv"),
        &completed,
    )?;
    let additional: Vec<_> = rows
        .iter()
        .filter(|r| r["raw_snapshot_present"] == true && r["normalized_1min_present"] != true)
        .cloned()
        .collect();
    dump_csv(
        &args.output_root.join("raw_additional_contracts.csv"),
        &additional,
    )?;
    fs::write(
        args.output_root.join("contracts.json"),
        serde_json::to_vec_pretty(&rows)?,
    )?;
    let count = |key: &str| rows.iter().filter(|r| r[key] == true).count();
    let summary = json!({"measured_at":chrono::Utc::now().to_rfc3339(),"period":period,"raw_rocksdb":cfg.rocksdb_dir,
        "watermark":"done","full_scan_verification":audit["full_scan_verification"],"source_messages":audit["census"]["messages"],
        "source_products":products.len(),"source_contracts":rows.len(),"raw_15s_contracts":count("raw_15s_present"),
        "raw_1min_contracts":count("raw_1min_present"),"raw_minute_derivable_contracts":count("raw_minute_derivable"),"normalized_watermark":normalized_watermark,
        "normalized_1min_contracts":count("normalized_1min_present"),
        "raw_last_10_level_fields_known_contracts":count("raw_15s_last_10_level_fields_known"),
        "raw_last_valid_10_level_contracts":rows.iter().filter(|r|r["raw_15s_last_valid_depth_levels"] == 10).count(),
        "raw_additional_contracts":rows.iter().filter(|r|r["raw_snapshot_present"] == true && r["normalized_1min_present"] != true).count(),
        "no_raw_snapshots_contracts":rows.iter().filter(|r|r["raw_1min_present"] != true && r["raw_15s_present"] != true).count(),
        "snapshot_counts":audit["snapshot_counts"],"notes":[
            "Source-complete means all Report messages were parsed and the writer completed, not a dense trading-session grid.",
            "Only first and last snapshot per RIC/frequency were sought; no full row scan.",
            "Last ten-level initialization and valid-depth counts describe the last snapshot only, not every historical snapshot.",
            "Expired contracts may end with explicitly empty fields; endpoint checks do not measure historical fill rate."]});
    fs::write(
        args.output_root.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("cme_ll2_contract_status: {e:#}");
        std::process::exit(1);
    }
}
