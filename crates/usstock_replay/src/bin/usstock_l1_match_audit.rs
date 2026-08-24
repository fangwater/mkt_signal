//! Read-only audit of whether venue-local L1 reflects subsequent trade size.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::PathBuf;

const SCALE: i128 = 1_000_000_000;

#[derive(Debug, Parser)]
#[command(name = "usstock_l1_match_audit")]
#[command(about = "Audit venue-local L1 decrements after US-stock TAS trades")]
struct Args {
    #[arg(long, default_value = "config/usstock_l1_match_audit.toml")]
    config: PathBuf,
    #[arg(long)]
    quote_qualifier_contains: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    input_path: PathBuf,
    ric: String,
    max_source_rows: u64,
    quote_qualifier_contains: String,
    #[serde(default)]
    output_json: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy)]
struct Level {
    price: i64,
    size: i64,
}

#[derive(Debug, Clone)]
struct VenueBook {
    bid: Option<Level>,
    ask: Option<Level>,
}

impl VenueBook {
    fn empty() -> Self {
        Self {
            bid: None,
            ask: None,
        }
    }

    fn side(&self, side: Side) -> Option<Level> {
        match side {
            Side::Bid => self.bid,
            Side::Ask => self.ask,
        }
    }

    fn set_side(&mut self, side: Side, level: Option<Level>) {
        match side {
            Side::Bid => self.bid = level,
            Side::Ask => self.ask = level,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingDecrement {
    price: i64,
    before_size: i64,
    summed_volume: i64,
    trades: u64,
}

#[derive(Debug, Default, Serialize)]
struct Stats {
    quote_qualifier_contains: String,
    source_rows: u64,
    ric_rows: u64,
    quote_rows_total: u64,
    quote_rows_used: u64,
    quote_side_updates: u64,
    quote_side_without_venue: u64,
    quote_side_incomplete: u64,
    trade_rows: u64,
    trade_without_venue: u64,
    trade_without_price_or_volume: u64,
    trade_row_l1_missing: u64,
    trade_row_l1_at_bid: u64,
    trade_row_l1_at_ask: u64,
    trade_row_l1_locked: u64,
    trade_row_l1_inside_spread: u64,
    trade_row_l1_outside_or_one_sided: u64,
    trade_without_prior_venue_book: u64,
    trade_at_prior_bid: u64,
    trade_at_prior_ask: u64,
    trade_at_locked_book: u64,
    trade_not_at_prior_best: u64,
    pending_buckets_resolved: u64,
    exact_decrement_buckets: u64,
    decrement_smaller_than_trade_volume: u64,
    decrement_larger_than_trade_volume: u64,
    quote_size_increased: u64,
    quote_price_changed_or_cleared: u64,
    pending_buckets_at_end: u64,
    pending_trade_volume_at_end_e9: String,
    qualifiers_with_aggrs_sid1: u64,
}

struct Audit {
    books: BTreeMap<String, VenueBook>,
    pending: BTreeMap<(String, Side), PendingDecrement>,
    stats: Stats,
}

impl Audit {
    fn new() -> Self {
        Self {
            books: BTreeMap::new(),
            pending: BTreeMap::new(),
            stats: Stats::default(),
        }
    }

    fn on_quote_side(&mut self, venue: &str, side: Side, after: Option<Level>) {
        if let Some(pending) = self.pending.remove(&(venue.to_string(), side)) {
            self.stats.pending_buckets_resolved += 1;
            match after {
                Some(level) if level.price == pending.price => {
                    let decrement = pending.before_size - level.size;
                    if decrement == pending.summed_volume {
                        self.stats.exact_decrement_buckets += 1;
                    } else if decrement < 0 {
                        self.stats.quote_size_increased += 1;
                    } else if decrement < pending.summed_volume {
                        self.stats.decrement_smaller_than_trade_volume += 1;
                    } else {
                        self.stats.decrement_larger_than_trade_volume += 1;
                    }
                }
                _ => self.stats.quote_price_changed_or_cleared += 1,
            }
        }
        let book = self
            .books
            .entry(venue.to_string())
            .or_insert_with(VenueBook::empty);
        book.set_side(side, after);
        self.stats.quote_side_updates += 1;
    }

    fn on_trade(&mut self, venue: &str, price: i64, volume: i64) {
        let Some(book) = self.books.get(venue) else {
            self.stats.trade_without_prior_venue_book += 1;
            return;
        };
        let bid = book.side(Side::Bid);
        let ask = book.side(Side::Ask);
        let matching_side = match (bid, ask) {
            (Some(bid), Some(ask)) if price == bid.price && price == ask.price => {
                self.stats.trade_at_locked_book += 1;
                return;
            }
            (Some(bid), _) if price == bid.price => {
                self.stats.trade_at_prior_bid += 1;
                Some((Side::Bid, bid))
            }
            (_, Some(ask)) if price == ask.price => {
                self.stats.trade_at_prior_ask += 1;
                Some((Side::Ask, ask))
            }
            _ => {
                self.stats.trade_not_at_prior_best += 1;
                None
            }
        };
        let Some((side, level)) = matching_side else {
            return;
        };
        let key = (venue.to_string(), side);
        match self.pending.get_mut(&key) {
            Some(pending) if pending.price == level.price && pending.before_size == level.size => {
                pending.summed_volume += volume;
                pending.trades += 1;
            }
            Some(_) => {
                // A quote update should have resolved this bucket. Do not merge
                // across a changed venue book; leave the existing audit result intact.
                self.pending.insert(
                    key,
                    PendingDecrement {
                        price: level.price,
                        before_size: level.size,
                        summed_volume: volume,
                        trades: 1,
                    },
                );
            }
            None => {
                self.pending.insert(
                    key,
                    PendingDecrement {
                        price: level.price,
                        before_size: level.size,
                        summed_volume: volume,
                        trades: 1,
                    },
                );
            }
        }
    }

    fn observe_trade_row_l1(&mut self, price: i64, bid: Option<i64>, ask: Option<i64>) {
        match (bid, ask) {
            (None, None) => self.stats.trade_row_l1_missing += 1,
            (Some(bid), Some(ask)) if price == bid && price == ask => {
                self.stats.trade_row_l1_locked += 1
            }
            (Some(bid), _) if price == bid => self.stats.trade_row_l1_at_bid += 1,
            (_, Some(ask)) if price == ask => self.stats.trade_row_l1_at_ask += 1,
            (Some(bid), Some(ask)) if price > bid && price < ask => {
                self.stats.trade_row_l1_inside_spread += 1
            }
            _ => self.stats.trade_row_l1_outside_or_one_sided += 1,
        }
    }

    fn finish(mut self) -> Stats {
        self.stats.pending_buckets_at_end = self.pending.len() as u64;
        let pending_volume: i128 = self
            .pending
            .values()
            .map(|pending| i128::from(pending.summed_volume))
            .sum();
        self.stats.pending_trade_volume_at_end_e9 = pending_volume.to_string();
        self.stats
    }
}

#[derive(Debug, Clone)]
struct Header {
    ric: usize,
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
    fn from_record(record: &StringRecord) -> Result<Self> {
        let mut by_name = BTreeMap::new();
        for (index, name) in record.iter().enumerate() {
            by_name.insert(name.trim().to_string(), index);
        }
        let required = |name: &str| -> Result<usize> {
            by_name
                .get(name)
                .copied()
                .ok_or_else(|| anyhow!("TAS header missing {name}"))
        };
        required("Date-Time")?;
        Ok(Self {
            ric: required("#RIC")?,
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

    fn cell<'a>(&self, row: &'a StringRecord, index: usize) -> &'a str {
        row.get(index).map(str::trim).unwrap_or("")
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

fn positive_l1(raw: &str, field: &str) -> Result<Option<i64>> {
    Ok(parse_e9(raw, field)?.filter(|value| *value > 0))
}

fn quote_level(
    venue: &str,
    price_raw: &str,
    size_raw: &str,
    stats: &mut Stats,
) -> Result<Option<(String, Option<Level>)>> {
    if venue.is_empty() {
        if !price_raw.is_empty() || !size_raw.is_empty() {
            stats.quote_side_without_venue += 1;
        }
        return Ok(None);
    }
    let price = parse_e9(price_raw, "quote price")?;
    let size = parse_e9(size_raw, "quote size")?;
    match (price, size) {
        (Some(0), Some(0)) => Ok(Some((venue.to_string(), None))),
        (Some(price), Some(size)) if price > 0 && size > 0 => {
            Ok(Some((venue.to_string(), Some(Level { price, size }))))
        }
        (None, None) => Ok(None),
        _ => {
            stats.quote_side_incomplete += 1;
            Ok(None)
        }
    }
}

fn process_row(
    audit: &mut Audit,
    header: &Header,
    row: &StringRecord,
    target_ric: &str,
    quote_qualifier_contains: &str,
) -> Result<()> {
    let ric = header.cell(row, header.ric);
    if ric != target_ric {
        return Ok(());
    }
    audit.stats.ric_rows += 1;
    let qualifiers = header.cell(row, header.qualifiers);
    if qualifiers.contains("AGGRS_SID1") {
        audit.stats.qualifiers_with_aggrs_sid1 += 1;
    }
    match header.cell(row, header.event_type) {
        "Quote" => {
            audit.stats.quote_rows_total += 1;
            if quote_qualifier_contains != "*" && !qualifiers.contains(quote_qualifier_contains) {
                return Ok(());
            }
            audit.stats.quote_rows_used += 1;
            if let Some((venue, level)) = quote_level(
                header.cell(row, header.bid_venue),
                header.cell(row, header.bid_price),
                header.cell(row, header.bid_size),
                &mut audit.stats,
            )? {
                audit.on_quote_side(&venue, Side::Bid, level);
            }
            if let Some((venue, level)) = quote_level(
                header.cell(row, header.ask_venue),
                header.cell(row, header.ask_price),
                header.cell(row, header.ask_size),
                &mut audit.stats,
            )? {
                audit.on_quote_side(&venue, Side::Ask, level);
            }
        }
        "Trade" => {
            audit.stats.trade_rows += 1;
            let venue = header.cell(row, header.venue);
            let price = parse_e9(header.cell(row, header.price), "trade price")?;
            let volume = parse_e9(header.cell(row, header.volume), "trade volume")?;
            match (venue.is_empty(), price, volume) {
                (true, _, _) => audit.stats.trade_without_venue += 1,
                (false, Some(price), Some(volume)) if price > 0 && volume > 0 => {
                    audit.observe_trade_row_l1(
                        price,
                        positive_l1(header.cell(row, header.bid_price), "trade row bid")?,
                        positive_l1(header.cell(row, header.ask_price), "trade row ask")?,
                    );
                    audit.on_trade(venue, price, volume)
                }
                _ => audit.stats.trade_without_price_or_volume += 1,
            }
        }
        _ => {}
    }
    Ok(())
}

fn run(config: &Config) -> Result<Stats> {
    if config.ric.is_empty() {
        bail!("ric must not be empty");
    }
    if config.max_source_rows == 0 {
        bail!("max_source_rows must be positive");
    }
    let file = File::open(&config.input_path)
        .with_context(|| format!("open {}", config.input_path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(16 * 1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);
    let header_row = reader
        .records()
        .next()
        .ok_or_else(|| anyhow!("{} is empty", config.input_path.display()))??;
    let header = Header::from_record(&header_row)?;
    let mut audit = Audit::new();
    audit.stats.quote_qualifier_contains = config.quote_qualifier_contains.clone();
    for result in reader.records() {
        if audit.stats.source_rows >= config.max_source_rows {
            break;
        }
        let row = result?;
        audit.stats.source_rows += 1;
        process_row(
            &mut audit,
            &header,
            &row,
            &config.ric,
            &config.quote_qualifier_contains,
        )?;
    }
    Ok(audit.finish())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let text = fs::read_to_string(&args.config)
        .with_context(|| format!("read {}", args.config.display()))?;
    let mut config: Config =
        toml::from_str(&text).with_context(|| format!("parse {}", args.config.display()))?;
    if let Some(quote_qualifier_contains) = args.quote_qualifier_contains {
        config.quote_qualifier_contains = quote_qualifier_contains;
    }
    let stats = run(&config)?;
    let json = serde_json::to_string_pretty(&stats)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_books_do_not_cross_and_exact_ask_decrement_matches() {
        let mut audit = Audit::new();
        audit.on_quote_side(
            "DEX",
            Side::Ask,
            Some(Level {
                price: 100_000_000_000,
                size: 10_000_000_000,
            }),
        );
        audit.on_quote_side(
            "BAT",
            Side::Ask,
            Some(Level {
                price: 100_000_000_000,
                size: 10_000_000_000,
            }),
        );
        audit.on_trade("DEX", 100_000_000_000, 1_000_000_000);
        audit.on_quote_side(
            "BAT",
            Side::Ask,
            Some(Level {
                price: 100_000_000_000,
                size: 9_000_000_000,
            }),
        );
        assert_eq!(audit.stats.pending_buckets_resolved, 0);
        audit.on_quote_side(
            "DEX",
            Side::Ask,
            Some(Level {
                price: 100_000_000_000,
                size: 9_000_000_000,
            }),
        );
        assert_eq!(audit.stats.exact_decrement_buckets, 1);
    }

    #[test]
    fn prior_bid_marks_sell_and_missing_book_is_not_defaulted() {
        let mut audit = Audit::new();
        audit.on_trade("DEX", 99_000_000_000, 1_000_000_000);
        assert_eq!(audit.stats.trade_without_prior_venue_book, 1);
        audit.on_quote_side(
            "DEX",
            Side::Bid,
            Some(Level {
                price: 99_000_000_000,
                size: 5_000_000_000,
            }),
        );
        audit.on_trade("DEX", 99_000_000_000, 1_000_000_000);
        assert_eq!(audit.stats.trade_at_prior_bid, 1);
    }

    #[test]
    fn explicit_zero_pair_clears_only_named_venue_side() {
        let mut stats = Stats::default();
        let clear = quote_level("DEX", "0", "0", &mut stats).unwrap();
        assert!(matches!(clear, Some((_, None))));
        let missing_venue = quote_level("", "0", "0", &mut stats).unwrap();
        assert!(missing_venue.is_none());
        assert_eq!(stats.quote_side_without_venue, 1);
    }
}
