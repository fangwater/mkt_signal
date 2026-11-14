//! Export selected normalised Quote fields from the existing US-stock RocksDB.

use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use clap::Parser;
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use usstock_replay::bin_msg::{UsStockSchemaMsg, UsStockSourceRowMsg};

const CF_SOURCE_SCHEMA: &str = "source_schema";
const CF_PREFIX: &str = "us_stock:";
const KEY_LEN: usize = 18;

#[derive(Debug, Parser)]
#[command(name = "usstock_export_quotes")]
#[command(about = "Export normalised Quotes from US-stock RocksDB by RIC and UTC range")]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    #[arg(long)]
    period: String,
    #[arg(long, required = true, num_args = 1..)]
    ric: Vec<String>,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long)]
    output_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct Header {
    date_time: usize,
    event_type: usize,
    buyer_id: usize,
    bid_price: usize,
    bid_size: usize,
    seller_id: usize,
    ask_price: usize,
    ask_size: usize,
    qualifiers: usize,
    seq_no: usize,
    exch_time: usize,
}

impl Header {
    fn from_names(names: &[String]) -> Result<Self> {
        let fields: BTreeMap<&str, usize> = names
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect();
        let required = |name: &str| {
            fields
                .get(name)
                .copied()
                .ok_or_else(|| anyhow!("source schema missing {name}"))
        };
        required("#RIC")?;
        Ok(Self {
            date_time: required("Date-Time")?,
            event_type: required("Type")?,
            buyer_id: required("Buyer ID")?,
            bid_price: required("Bid Price")?,
            bid_size: required("Bid Size")?,
            seller_id: required("Seller ID")?,
            ask_price: required("Ask Price")?,
            ask_size: required("Ask Size")?,
            qualifiers: required("Qualifiers")?,
            seq_no: required("Seq. No.")?,
            exch_time: required("Exch Time")?,
        })
    }

    fn cell<'a>(&self, cells: &'a [String], index: usize) -> &'a str {
        cells.get(index).map(String::as_str).unwrap_or("")
    }
}

#[derive(Debug, Serialize)]
struct Quote {
    ric: String,
    ts: String,
    exch_time: String,
    buyer_id: String,
    bid_price: String,
    bid_size: String,
    seller_id: String,
    ask_price: String,
    ask_size: String,
    qualifiers: String,
    seq_no: String,
}

#[derive(Debug, Serialize)]
struct Output {
    period: String,
    start: String,
    end: String,
    quote_rows: BTreeMap<String, u64>,
    output_files: BTreeMap<String, PathBuf>,
}

fn parse_utc_ns(raw: &str) -> Result<u64> {
    let timestamp = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse UTC timestamp {raw:?}"))?
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp outside nanosecond range {raw:?}"))?;
    u64::try_from(timestamp).map_err(|_| anyhow!("negative UTC timestamp {raw:?}"))
}

fn key_at(ts_ns: u64) -> [u8; KEY_LEN] {
    let mut key = [0_u8; KEY_LEN];
    key[..8].copy_from_slice(&ts_ns.to_be_bytes());
    key
}

fn key_timestamp(key: &[u8]) -> Result<u64> {
    if key.len() != KEY_LEN {
        bail!("expected {KEY_LEN}-byte source key, got {}", key.len());
    }
    Ok(u64::from_be_bytes(key[..8].try_into()?))
}

fn open_read_only(path: &PathBuf, cfs: Vec<String>) -> Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    options.set_max_open_files(256);
    let all = DB::list_cf(&options, path)
        .with_context(|| format!("list column families in {}", path.display()))?;
    for cf in &cfs {
        if !all.iter().any(|name| name == cf) {
            bail!("column family {cf} is absent from {}", path.display());
        }
    }
    DB::open_cf_for_read_only(&options, path, cfs, false)
        .with_context(|| format!("open RocksDB {} read-only", path.display()))
}

fn decode_cells(value: &[u8]) -> Result<Vec<String>> {
    UsStockSourceRowMsg::from_bytes(value.to_vec())?.cells()
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rics: BTreeSet<String> = args.ric.into_iter().collect();
    if rics.is_empty() || rics.iter().any(|ric| ric.is_empty()) {
        bail!("--ric must contain one or more nonempty RICs");
    }
    let start_ns = parse_utc_ns(&args.start)?;
    let end_ns = parse_utc_ns(&args.end)?;
    if end_ns <= start_ns {
        bail!("--end must be after --start");
    }
    fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("create {}", args.output_dir.display()))?;

    let mut cf_names = vec!["default".to_string(), CF_SOURCE_SCHEMA.to_string()];
    cf_names.extend(rics.iter().map(|ric| format!("{CF_PREFIX}{ric}")));
    let db = open_read_only(&args.rocksdb_dir, cf_names)?;
    let schema_cf = db
        .cf_handle(CF_SOURCE_SCHEMA)
        .ok_or_else(|| anyhow!("missing {CF_SOURCE_SCHEMA} handle"))?;
    let schema_key = format!("header:{}", args.period);
    let schema = db
        .get_cf(schema_cf, schema_key.as_bytes())?
        .ok_or_else(|| anyhow!("missing schema {schema_key}"))?;
    let header = Header::from_names(&UsStockSchemaMsg::from_bytes(schema.to_vec())?.headers()?)?;

    let mut quote_rows = BTreeMap::new();
    let mut output_files = BTreeMap::new();
    let start_key = key_at(start_ns);
    for ric in &rics {
        let cf_name = format!("{CF_PREFIX}{ric}");
        let cf = db
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow!("missing {cf_name} handle"))?;
        let path = args.output_dir.join(format!("{ric}.quotes.jsonl"));
        let mut writer = BufWriter::new(
            File::create(&path).with_context(|| format!("create {}", path.display()))?,
        );
        let mut count = 0_u64;
        for item in db.iterator_cf(cf, IteratorMode::From(&start_key, Direction::Forward)) {
            let (key, value) = item?;
            if key_timestamp(&key)? >= end_ns {
                break;
            }
            let cells = decode_cells(&value)?;
            if header.cell(&cells, header.event_type) != "Quote" {
                continue;
            }
            let quote = Quote {
                ric: ric.clone(),
                ts: header.cell(&cells, header.date_time).to_string(),
                exch_time: header.cell(&cells, header.exch_time).to_string(),
                buyer_id: header.cell(&cells, header.buyer_id).to_string(),
                bid_price: header.cell(&cells, header.bid_price).to_string(),
                bid_size: header.cell(&cells, header.bid_size).to_string(),
                seller_id: header.cell(&cells, header.seller_id).to_string(),
                ask_price: header.cell(&cells, header.ask_price).to_string(),
                ask_size: header.cell(&cells, header.ask_size).to_string(),
                qualifiers: header.cell(&cells, header.qualifiers).to_string(),
                seq_no: header.cell(&cells, header.seq_no).to_string(),
            };
            serde_json::to_writer(&mut writer, &quote)?;
            writer.write_all(b"\n")?;
            count += 1;
        }
        writer.flush()?;
        quote_rows.insert(ric.clone(), count);
        output_files.insert(ric.clone(), path);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&Output {
            period: args.period,
            start: args.start,
            end: args.end,
            quote_rows,
            output_files,
        })?
    );
    Ok(())
}
