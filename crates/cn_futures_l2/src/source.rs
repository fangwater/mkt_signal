//! Locate Tonglian future-main-quote files and parse L2 snapshots.

use anyhow::{bail, Context, Result};
use chrono::{Days, NaiveDate, NaiveTime, TimeZone};
use chrono_tz::Asia::Shanghai;
use csv::{ReaderBuilder, StringRecord};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use zip::ZipArchive;

use crate::codec::QUEUE_LEVELS;
use crate::events::{QueueSnapshot, Snapshot};
use crate::session::Exchange;
use crate::universe::{is_maintained_product, product_id};

pub const DEFAULT_LOOKBACK_DAYS: u64 = 16;
pub const COMM_L2_LOOKBACK_DAYS: u64 = 1;
pub const DEFAULT_OVERLAP_CUT: &str = "2025-11-03";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Layer {
    CommL2,
    Msg,
}

#[derive(Clone, Debug)]
pub struct LocatedFile {
    pub path: PathBuf,
    pub layer: Layer,
    pub directory_day: NaiveDate,
    pub kind: FileKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    Future,
    OrderQueue,
}

#[derive(Clone, Debug)]
struct Columns {
    instrument: usize,
    trad_day: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    volume: Option<usize>,
    turnover: Option<usize>,
    last_price: Option<usize>,
    open_int: Option<usize>,
    sequence: Option<usize>,
    bid_prices: [Option<usize>; 5],
    bid_sizes: [Option<usize>; 5],
    ask_prices: [Option<usize>; 5],
    ask_sizes: [Option<usize>; 5],
    is_combo: bool,
}

#[derive(Clone, Debug)]
struct QueueColumns {
    instrument: usize,
    trad_day: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    sequence: Option<usize>,
    bid_price: Option<usize>,
    ask_price: Option<usize>,
    bid_qty: [Option<usize>; QUEUE_LEVELS],
    ask_qty: [Option<usize>; QUEUE_LEVELS],
}

impl Columns {
    fn from_header(header: &StringRecord) -> Option<Self> {
        let indexes: HashMap<String, usize> = header
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name.trim_start_matches('\u{feff}')
                        .trim()
                        .to_ascii_lowercase(),
                    index,
                )
            })
            .collect();
        let required = |names: &[&str]| names.iter().find_map(|name| indexes.get(*name).copied());
        let optional = |names: &[&str]| required(names);
        let instrument = required(&["instruid", "instrument_id"])?;
        let trad_day = required(&["tradday", "trad_day"])?;
        let level = |prefix: &str, suffix: &str| {
            let mut slots = [None; 5];
            for (index, slot) in slots.iter_mut().enumerate() {
                let name = format!("{prefix}{}{suffix}", index + 1);
                *slot = indexes.get(&name).copied();
            }
            slots
        };
        Some(Self {
            instrument,
            trad_day,
            action_day: optional(&["actionday", "action_day"]),
            update_time: optional(&["updatetime", "update_time"]),
            volume: optional(&["volume"]),
            turnover: optional(&["turnover"]),
            last_price: optional(&["lastprice", "last_price"]),
            open_int: optional(&["openint", "open_int"]),
            sequence: optional(&["seqno", "seq_no"]),
            bid_prices: level("bidprice", ""),
            bid_sizes: level("bidvolume", ""),
            ask_prices: level("askprice", ""),
            ask_sizes: level("askvolume", ""),
            is_combo: ["cmbtypeid", "firstinstruid", "secondinstruid"]
                .iter()
                .any(|name| indexes.contains_key(*name)),
        })
    }
}

impl QueueColumns {
    fn from_header(header: &StringRecord) -> Option<Self> {
        let indexes: HashMap<String, usize> = header
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name.trim_start_matches('\u{feff}')
                        .trim()
                        .to_ascii_lowercase(),
                    index,
                )
            })
            .collect();
        let required = |names: &[&str]| names.iter().find_map(|name| indexes.get(*name).copied());
        let optional = |names: &[&str]| required(names);
        let instrument = required(&["instruid", "instrument_id"])?;
        let trad_day = required(&["tradday", "trad_day"])?;
        if optional(&["bidorderqty1"]).is_none() && optional(&["askorderqty1"]).is_none() {
            return None;
        }
        let qty = |prefix: &str| {
            let mut slots = [None; QUEUE_LEVELS];
            for (index, slot) in slots.iter_mut().enumerate() {
                let name = format!("{prefix}{}", index + 1);
                *slot = indexes.get(&name).copied();
            }
            slots
        };
        Some(Self {
            instrument,
            trad_day,
            action_day: optional(&["actionday", "action_day"]),
            update_time: optional(&["updatetime", "update_time"]),
            sequence: optional(&["seqno", "seq_no"]),
            bid_price: optional(&["bidprice"]),
            ask_price: optional(&["askprice"]),
            bid_qty: qty("bidorderqty"),
            ask_qty: qty("askorderqty"),
        })
    }
}

pub fn parse_day(value: &str) -> Result<NaiveDate> {
    let compact = compact_day(value).ok_or_else(|| anyhow::anyhow!("invalid date: {value:?}"))?;
    NaiveDate::parse_from_str(&compact, "%Y%m%d").context("parse date")
}

pub fn compact_day(value: &str) -> Option<String> {
    let compact = value.trim().replace('-', "");
    (compact.len() == 8 && compact.bytes().all(|byte| byte.is_ascii_digit())).then_some(compact)
}

fn parse_directory_day(name: &str) -> Option<NaiveDate> {
    compact_day(name).and_then(|value| NaiveDate::parse_from_str(&value, "%Y%m%d").ok())
}

fn list_directories(
    root: &Path,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let mut directories = Vec::new();
    if !root.is_dir() {
        return Ok(directories);
    }
    for entry in fs::read_dir(root).with_context(|| format!("read {root:?}"))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(day) = parse_directory_day(&name.to_string_lossy()) else {
            continue;
        };
        if day >= start && day <= end {
            directories.push((day, entry.path()));
        }
    }
    directories.sort_by_key(|(day, _)| *day);
    Ok(directories)
}

pub fn resolve_layer(
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
) -> (Option<PathBuf>, Option<PathBuf>) {
    match (l2_root, msg_root, overlap_cut) {
        (Some(l2), Some(_), Some(cut)) if trad_day <= cut => (Some(l2.to_path_buf()), None),
        (Some(_), Some(msg), Some(_)) => (None, Some(msg.to_path_buf())),
        (l2, msg, _) => (l2.map(Path::to_path_buf), msg.map(Path::to_path_buf)),
    }
}

pub fn source_files_for_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<LocatedFile>> {
    let (layer_l2, layer_msg) = resolve_layer(trad_day, l2_root, msg_root, overlap_cut);
    let mut files = Vec::new();
    if let Some(root) = layer_l2 {
        let start = trad_day
            .checked_sub_days(Days::new(COMM_L2_LOOKBACK_DAYS))
            .unwrap_or(trad_day);
        for (day, directory) in list_directories(&root.join(exchange.as_str()), start, trad_day)? {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let lower = entry.file_name().to_string_lossy().to_ascii_lowercase();
                if lower.starts_with("future_")
                    && (lower.ends_with(".zip") || lower.ends_with(".csv"))
                {
                    files.push(LocatedFile {
                        path: entry.path(),
                        layer: Layer::CommL2,
                        directory_day: day,
                        kind: FileKind::Future,
                    });
                }
            }
        }
    }
    if let Some(root) = layer_msg {
        let start = trad_day
            .checked_sub_days(Days::new(lookback_days))
            .unwrap_or(trad_day);
        let expected = exchange.msg_stem();
        for (day, directory) in list_directories(&root, start, trad_day)? {
            let path = directory.join(expected);
            if path.is_file() {
                files.push(LocatedFile {
                    path,
                    layer: Layer::Msg,
                    directory_day: day,
                    kind: FileKind::Future,
                });
            }
        }
    }
    files.sort_by(|left, right| {
        (left.layer as u8, left.directory_day, &left.path).cmp(&(
            right.layer as u8,
            right.directory_day,
            &right.path,
        ))
    });
    Ok(files)
}

pub fn source_queue_files_for_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<LocatedFile>> {
    if !exchange.has_order_queue() {
        return Ok(Vec::new());
    }
    let (layer_l2, layer_msg) = resolve_layer(trad_day, l2_root, msg_root, overlap_cut);
    let mut files = Vec::new();
    if let (Some(root), Some(prefix)) = (layer_l2, exchange.comm_l2_queue_prefix()) {
        let start = trad_day
            .checked_sub_days(Days::new(COMM_L2_LOOKBACK_DAYS))
            .unwrap_or(trad_day);
        for (day, directory) in list_directories(&root.join(exchange.as_str()), start, trad_day)? {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let lower = entry.file_name().to_string_lossy().to_ascii_lowercase();
                if lower.starts_with(prefix) && (lower.ends_with(".zip") || lower.ends_with(".csv"))
                {
                    files.push(LocatedFile {
                        path: entry.path(),
                        layer: Layer::CommL2,
                        directory_day: day,
                        kind: FileKind::OrderQueue,
                    });
                }
            }
        }
    }
    if let (Some(root), Some(expected)) = (layer_msg, exchange.msg_queue_stem()) {
        let start = trad_day
            .checked_sub_days(Days::new(lookback_days))
            .unwrap_or(trad_day);
        for (day, directory) in list_directories(&root, start, trad_day)? {
            let path = directory.join(expected);
            if path.is_file() {
                files.push(LocatedFile {
                    path,
                    layer: Layer::Msg,
                    directory_day: day,
                    kind: FileKind::OrderQueue,
                });
            }
        }
    }
    files.sort_by(|left, right| {
        (left.layer as u8, left.directory_day, &left.path).cmp(&(
            right.layer as u8,
            right.directory_day,
            &right.path,
        ))
    });
    Ok(files)
}

fn cell<'a>(record: &'a StringRecord, index: Option<usize>) -> &'a str {
    index.and_then(|value| record.get(value)).unwrap_or("")
}

fn parse_float(value: &str) -> Option<f64> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    text.parse::<f64>().ok().filter(|price| price.is_finite())
}

fn parse_update_time(value: &str) -> Option<NaiveTime> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M:%S"))
        .ok()
}

fn sequence(value: &str, source_row: u32) -> i64 {
    value
        .trim()
        .parse::<i64>()
        .or_else(|_| value.trim().parse::<f64>().map(|value| value as i64))
        .unwrap_or(source_row as i64)
}

fn levels(record: &StringRecord, indexes: [Option<usize>; 5]) -> [Option<f64>; 5] {
    let mut out = [None; 5];
    for (slot, index) in out.iter_mut().zip(indexes) {
        *slot = parse_float(cell(record, index));
    }
    out
}

fn queue_levels(
    record: &StringRecord,
    indexes: [Option<usize>; QUEUE_LEVELS],
) -> [Option<f64>; QUEUE_LEVELS] {
    let mut out = [None; QUEUE_LEVELS];
    for (slot, index) in out.iter_mut().zip(indexes) {
        *slot = parse_float(cell(record, index)).filter(|value| *value != 0.0);
    }
    out
}

fn row_event(
    row: &StringRecord,
    instrument_idx: usize,
    trad_day_idx: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    trad_day: NaiveDate,
) -> Option<(String, String, NaiveDate, chrono::DateTime<chrono_tz::Tz>)> {
    let instrument = cell(row, Some(instrument_idx)).trim();
    let product = product_id(instrument)?;
    if !is_maintained_product(&product) {
        return None;
    }
    let trad_key = compact_day(cell(row, Some(trad_day_idx)))?;
    let row_day = NaiveDate::parse_from_str(&trad_key, "%Y%m%d").ok()?;
    if row_day != trad_day {
        return None;
    }
    let action_key = action_day.and_then(|index| compact_day(cell(row, Some(index))))?;
    let action_day = NaiveDate::parse_from_str(&action_key, "%Y%m%d").ok()?;
    let clock = parse_update_time(cell(row, update_time))?;
    let event_time = Shanghai
        .from_local_datetime(&action_day.and_time(clock))
        .single()?;
    Some((instrument.to_owned(), product, row_day, event_time))
}

fn collect_snapshots<R: Read>(
    reader: R,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<Snapshot>,
) -> Result<()> {
    let mut csv = ReaderBuilder::new().flexible(true).from_reader(reader);
    let header = csv.headers()?.clone();
    let Some(columns) = Columns::from_header(&header) else {
        return Ok(());
    };
    if columns.is_combo {
        return Ok(());
    }
    for (offset, row) in csv.records().enumerate() {
        let row = row?;
        let source_row = (offset + 1) as u32;
        let instrument = cell(&row, Some(columns.instrument)).trim();
        let Some(product) = product_id(instrument) else {
            continue;
        };
        if !is_maintained_product(&product) {
            continue;
        }
        let Some(trad_key) = compact_day(cell(&row, Some(columns.trad_day))) else {
            continue;
        };
        let row_day = NaiveDate::parse_from_str(&trad_key, "%Y%m%d")?;
        if row_day != trad_day {
            continue;
        }
        let Some(action_key) = columns
            .action_day
            .and_then(|index| compact_day(cell(&row, Some(index))))
        else {
            continue;
        };
        let Some(action_day) = NaiveDate::parse_from_str(&action_key, "%Y%m%d").ok() else {
            continue;
        };
        let Some(clock) = parse_update_time(cell(&row, columns.update_time)) else {
            continue;
        };
        let Some(event_time) = Shanghai
            .from_local_datetime(&action_day.and_time(clock))
            .single()
        else {
            continue;
        };
        out.push(Snapshot {
            trad_day: row_day,
            instrument_id: instrument.to_owned(),
            product_id: product,
            event_time,
            seq: sequence(cell(&row, columns.sequence), source_row),
            source_row,
            file_order,
            volume: parse_float(cell(&row, columns.volume)),
            turnover: parse_float(cell(&row, columns.turnover)),
            last_price: parse_float(cell(&row, columns.last_price)),
            open_int: parse_float(cell(&row, columns.open_int)),
            bid_prices: levels(&row, columns.bid_prices),
            bid_sizes: levels(&row, columns.bid_sizes),
            ask_prices: levels(&row, columns.ask_prices),
            ask_sizes: levels(&row, columns.ask_sizes),
        });
    }
    Ok(())
}

pub fn read_located_file(
    located: &LocatedFile,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<Snapshot>,
) -> Result<()> {
    if located
        .path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
        let mut archive = ZipArchive::new(file)?;
        for index in 0..archive.len() {
            let member = archive.by_index(index)?;
            let name = member.name().to_ascii_lowercase();
            if !name.ends_with(".csv")
                || name.contains("futcmb")
                || name.contains("futtqs")
                || name.contains("futorderq")
            {
                continue;
            }
            collect_snapshots(member, trad_day, file_order, out)?;
        }
        return Ok(());
    }
    let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
    collect_snapshots(BufReader::new(file), trad_day, file_order, out)
}

fn collect_queues<R: Read>(
    reader: R,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<QueueSnapshot>,
) -> Result<()> {
    let mut csv = ReaderBuilder::new().flexible(true).from_reader(reader);
    let header = csv.headers()?.clone();
    let Some(columns) = QueueColumns::from_header(&header) else {
        return Ok(());
    };
    for (offset, row) in csv.records().enumerate() {
        let row = row?;
        let source_row = (offset + 1) as u32;
        let Some((instrument, product, row_day, event_time)) = row_event(
            &row,
            columns.instrument,
            columns.trad_day,
            columns.action_day,
            columns.update_time,
            trad_day,
        ) else {
            continue;
        };
        let snap = QueueSnapshot {
            trad_day: row_day,
            instrument_id: instrument,
            product_id: product,
            event_time,
            seq: sequence(cell(&row, columns.sequence), source_row),
            source_row,
            file_order,
            bid_price: parse_float(cell(&row, columns.bid_price)).filter(|value| *value > 0.0),
            ask_price: parse_float(cell(&row, columns.ask_price)).filter(|value| *value > 0.0),
            bid_qty: queue_levels(&row, columns.bid_qty),
            ask_qty: queue_levels(&row, columns.ask_qty),
        };
        if snap.has_side() {
            out.push(snap);
        }
    }
    Ok(())
}

pub fn read_located_queue_file(
    located: &LocatedFile,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<QueueSnapshot>,
) -> Result<()> {
    if located
        .path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
        let mut archive = ZipArchive::new(file)?;
        for index in 0..archive.len() {
            let member = archive.by_index(index)?;
            let name = member.name().to_ascii_lowercase();
            if !name.ends_with(".csv") {
                continue;
            }
            collect_queues(member, trad_day, file_order, out)?;
        }
        return Ok(());
    }
    let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
    collect_queues(BufReader::new(file), trad_day, file_order, out)
}

pub fn load_trad_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<Snapshot>> {
    let files = source_files_for_day(
        exchange,
        trad_day,
        l2_root,
        msg_root,
        overlap_cut,
        lookback_days,
    )?;
    let mut snapshots = Vec::new();
    for (file_order, located) in files.iter().enumerate() {
        read_located_file(located, trad_day, file_order as u32, &mut snapshots)?;
    }
    Ok(snapshots)
}

pub fn load_queue_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<QueueSnapshot>> {
    let files = source_queue_files_for_day(
        exchange,
        trad_day,
        l2_root,
        msg_root,
        overlap_cut,
        lookback_days,
    )?;
    let mut snapshots = Vec::new();
    for (file_order, located) in files.iter().enumerate() {
        read_located_queue_file(located, trad_day, file_order as u32, &mut snapshots)?;
    }
    Ok(snapshots)
}

pub fn iter_calendar_days(start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
    if end < start {
        bail!("end precedes start");
    }
    let mut days = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        days.push(cursor);
        cursor = cursor
            .succ_opt()
            .ok_or_else(|| anyhow::anyhow!("date overflow after {cursor}"))?;
    }
    Ok(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_cut_picks_one_layer() {
        let cut = NaiveDate::from_ymd_opt(2025, 11, 3).unwrap();
        let l2 = Path::new("/l2");
        let msg = Path::new("/msg");
        let before = resolve_layer(cut, Some(l2), Some(msg), Some(cut));
        let after = resolve_layer(
            NaiveDate::from_ymd_opt(2025, 11, 4).unwrap(),
            Some(l2),
            Some(msg),
            Some(cut),
        );
        assert_eq!(before, (Some(l2.to_path_buf()), None));
        assert_eq!(after, (None, Some(msg.to_path_buf())));
    }
}
