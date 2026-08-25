//! Print causally non-inferable Trade examples directly from US-stock RocksDB.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use rocksdb::{IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use usstock_replay::bin_msg::{UsStockSchemaMsg, UsStockSourceRowMsg};

const SCALE: i128 = 1_000_000_000;
const SCHEMA_CF: &str = "source_schema";
const CF_PREFIX: &str = "us_stock:";

#[derive(Debug, Parser)]
#[command(name = "usstock_inference_samples")]
#[command(about = "Read non-inferable Trade examples from US-stock RocksDB")]
struct Args {
    #[arg(long, default_value = "config/usstock_inference_samples.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    rocksdb_dir: PathBuf,
    period: String,
    ric: String,
    max_samples_per_class: usize,
    max_scanned_rows: u64,
    #[serde(default)]
    output_json: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy)]
struct Level {
    price: i64,
    size: i64,
}

#[derive(Debug, Clone)]
struct Bbo {
    ts: String,
    source_row: u64,
    bid: Level,
    ask: Level,
    bid_venue: String,
    ask_venue: String,
}

#[derive(Debug, Serialize)]
struct Sample {
    class: String,
    trade_ts: String,
    trade_source_row: u64,
    trade_venue: String,
    trade_price: String,
    trade_volume: String,
    qualifiers: String,
    prior_quote_ts: String,
    prior_quote_source_row: u64,
    prior_bid_venue: String,
    prior_bid_price: String,
    prior_bid_size: String,
    prior_ask_venue: String,
    prior_ask_price: String,
    prior_ask_size: String,
}

#[derive(Debug, Serialize)]
struct Output {
    ric: String,
    max_scanned_rows: u64,
    scanned_rows: u64,
    complete_quote_rows: u64,
    inside_prior_spread: Vec<Sample>,
    outside_prior_bbo: Vec<Sample>,
}

#[derive(Debug, Clone)]
struct Header {
    date_time: usize,
    event_type: usize,
    venue: usize,
    price: usize,
    volume: usize,
    bid_venue: usize,
    bid_price: usize,
    bid_size: usize,
    ask_venue: usize,
    ask_price: usize,
    ask_size: usize,
    qualifiers: usize,
}

impl Header {
    fn from_names(names: &[String]) -> Result<Self> {
        let by_name: BTreeMap<&str, usize> = names
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect();
        let required = |name: &str| -> Result<usize> {
            by_name
                .get(name)
                .copied()
                .ok_or_else(|| anyhow!("source schema missing {name}"))
        };
        required("#RIC")?;
        Ok(Self {
            date_time: required("Date-Time")?,
            event_type: required("Type")?,
            venue: required("Ex/Cntrb.ID")?,
            price: required("Price")?,
            volume: required("Volume")?,
            bid_venue: required("Buyer ID")?,
            bid_price: required("Bid Price")?,
            bid_size: required("Bid Size")?,
            ask_venue: required("Seller ID")?,
            ask_price: required("Ask Price")?,
            ask_size: required("Ask Size")?,
            qualifiers: required("Qualifiers")?,
        })
    }

    fn cell<'a>(&self, cells: &'a [String], index: usize) -> &'a str {
        cells.get(index).map(String::as_str).unwrap_or("")
    }
}

fn parse_e9(raw: &str, field: &str) -> Result<Option<i64>> {
    if raw.is_empty() {
        return Ok(None);
    }
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 9
    {
        bail!("invalid {field} {raw:?}");
    }
    let whole = whole.parse::<i128>()?;
    let mut fraction_padded = fraction.to_string();
    while fraction_padded.len() < 9 {
        fraction_padded.push('0');
    }
    let fraction = if fraction_padded.is_empty() {
        0
    } else {
        fraction_padded.parse::<i128>()?
    };
    let scaled = whole
        .checked_mul(SCALE)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| anyhow!("{field} {raw:?} overflow"))?;
    Ok(Some(i64::try_from(scaled)?))
}

fn positive_level(price: &str, size: &str, side: &str) -> Result<Option<Level>> {
    match (parse_e9(price, side)?, parse_e9(size, side)?) {
        (Some(price), Some(size)) if price > 0 && size > 0 => Ok(Some(Level { price, size })),
        _ => Ok(None),
    }
}

fn format_e9(value: i64) -> String {
    let whole = value / 1_000_000_000;
    let fraction = value.unsigned_abs() % 1_000_000_000;
    if fraction == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{fraction:09}")
            .trim_end_matches('0')
            .to_string()
    }
}

fn decode_source_row(value: &[u8]) -> Result<Vec<String>> {
    UsStockSourceRowMsg::from_bytes(value.to_vec())?.cells()
}

fn open_read_only(primary: &Path, cfs: Vec<String>) -> Result<DB> {
    if !primary.is_dir() {
        bail!("rocksdb_dir {} is not a directory", primary.display());
    }
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    options.set_max_open_files(256);
    let all_names = DB::list_cf(&options, primary)
        .with_context(|| format!("list column families {}", primary.display()))?;
    for name in &cfs {
        if !all_names.iter().any(|candidate| candidate == name) {
            bail!(
                "column family {name} does not exist in {}",
                primary.display()
            );
        }
    }
    DB::open_cf_for_read_only(&options, primary, cfs, false)
        .with_context(|| format!("open read-only {}", primary.display()))
}

fn source_row_from_key(key: &[u8]) -> Result<u64> {
    if key.len() != 18 {
        bail!("expected 18-byte source-row key, got {}", key.len());
    }
    Ok(u64::from_be_bytes(key[10..18].try_into()?))
}

fn sample(class: &str, header: &Header, cells: &[String], source_row: u64, bbo: &Bbo) -> Sample {
    Sample {
        class: class.to_string(),
        trade_ts: header.cell(cells, header.date_time).to_string(),
        trade_source_row: source_row,
        trade_venue: header.cell(cells, header.venue).to_string(),
        trade_price: header.cell(cells, header.price).to_string(),
        trade_volume: header.cell(cells, header.volume).to_string(),
        qualifiers: header.cell(cells, header.qualifiers).to_string(),
        prior_quote_ts: bbo.ts.clone(),
        prior_quote_source_row: bbo.source_row,
        prior_bid_venue: bbo.bid_venue.clone(),
        prior_bid_price: format_e9(bbo.bid.price),
        prior_bid_size: format_e9(bbo.bid.size),
        prior_ask_venue: bbo.ask_venue.clone(),
        prior_ask_price: format_e9(bbo.ask.price),
        prior_ask_size: format_e9(bbo.ask.size),
    }
}

fn run(config: &Config) -> Result<Output> {
    if config.ric.is_empty() || config.max_samples_per_class == 0 || config.max_scanned_rows == 0 {
        bail!("ric, max_samples_per_class, and max_scanned_rows must be set");
    }
    let cf_name = format!("{CF_PREFIX}{}", config.ric);
    let db = open_read_only(
        &config.rocksdb_dir,
        vec![
            "default".to_string(),
            SCHEMA_CF.to_string(),
            cf_name.clone(),
        ],
    )?;
    let schema_cf = db
        .cf_handle(SCHEMA_CF)
        .ok_or_else(|| anyhow!("missing {SCHEMA_CF} CF"))?;
    let schema_key = format!("header:{}", config.period);
    let schema = db
        .get_cf(schema_cf, schema_key.as_bytes())?
        .ok_or_else(|| anyhow!("missing schema for period {}", config.period))?;
    let header = Header::from_names(&UsStockSchemaMsg::from_bytes(schema.to_vec())?.headers()?)?;
    let stock_cf = db
        .cf_handle(&cf_name)
        .ok_or_else(|| anyhow!("missing column family {cf_name}"))?;
    let mut output = Output {
        ric: config.ric.clone(),
        max_scanned_rows: config.max_scanned_rows,
        scanned_rows: 0,
        complete_quote_rows: 0,
        inside_prior_spread: Vec::new(),
        outside_prior_bbo: Vec::new(),
    };
    let mut prior_bbo: Option<Bbo> = None;
    for item in db.iterator_cf(stock_cf, IteratorMode::Start) {
        if output.scanned_rows >= config.max_scanned_rows {
            break;
        }
        let (key, value) = item?;
        output.scanned_rows += 1;
        let source_row = source_row_from_key(&key)?;
        let cells = decode_source_row(&value)?;
        match header.cell(&cells, header.event_type) {
            "Quote" => {
                let bid = positive_level(
                    header.cell(&cells, header.bid_price),
                    header.cell(&cells, header.bid_size),
                    "bid",
                )?;
                let ask = positive_level(
                    header.cell(&cells, header.ask_price),
                    header.cell(&cells, header.ask_size),
                    "ask",
                )?;
                if let (Some(bid), Some(ask)) = (bid, ask) {
                    if bid.price < ask.price {
                        output.complete_quote_rows += 1;
                        prior_bbo = Some(Bbo {
                            ts: header.cell(&cells, header.date_time).to_string(),
                            source_row,
                            bid,
                            ask,
                            bid_venue: header.cell(&cells, header.bid_venue).to_string(),
                            ask_venue: header.cell(&cells, header.ask_venue).to_string(),
                        });
                    }
                }
            }
            "Trade" => {
                let Some(bbo) = prior_bbo.as_ref() else {
                    continue;
                };
                let Some(price) = parse_e9(header.cell(&cells, header.price), "trade price")?
                else {
                    continue;
                };
                if price > bbo.bid.price
                    && price < bbo.ask.price
                    && output.inside_prior_spread.len() < config.max_samples_per_class
                {
                    output.inside_prior_spread.push(sample(
                        "inside_prior_spread",
                        &header,
                        &cells,
                        source_row,
                        bbo,
                    ));
                } else if (price < bbo.bid.price || price > bbo.ask.price)
                    && output.outside_prior_bbo.len() < config.max_samples_per_class
                {
                    output.outside_prior_bbo.push(sample(
                        "outside_prior_bbo",
                        &header,
                        &cells,
                        source_row,
                        bbo,
                    ));
                }
            }
            _ => {}
        }
        if output.inside_prior_spread.len() >= config.max_samples_per_class
            && output.outside_prior_bbo.len() >= config.max_samples_per_class
        {
            break;
        }
    }
    Ok(output)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let text = fs::read_to_string(&args.config)
        .with_context(|| format!("read {}", args.config.display()))?;
    let config: Config =
        toml::from_str(&text).with_context(|| format!("parse {}", args.config.display()))?;
    let json = serde_json::to_string_pretty(&run(&config)?)?;
    if let Some(path) = &config.output_json {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let mut output =
            File::create(path).with_context(|| format!("create {}", path.display()))?;
        output.write_all(json.as_bytes())?;
        output.write_all(b"\n")?;
    }
    println!("{json}");
    Ok(())
}
