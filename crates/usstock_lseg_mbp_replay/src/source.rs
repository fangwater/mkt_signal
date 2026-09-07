use crate::codec::{parse_decimal_e9, parse_hms_ns};
use crate::model::{
    summary_index, LevelAction, LevelDelta, LogicalMessage, MessageClass, Side, SummaryDelta,
    SummaryField, ENTRY_FIDS_OLD, ENTRY_TIME_NS_FID, SUMMARY_FIDS,
};
use anyhow::{anyhow, bail, Context, Result};
use csv::StringRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

const HEADER: [&str; 14] = [
    "#RIC",
    "Domain",
    "Date-Time",
    "GMT Offset",
    "Type",
    "MsgClass/FID number",
    "UpdateType/Action",
    "FID Name",
    "FID Value",
    "FID Enum String",
    "PE Code",
    "Template Number",
    "Key/Msg Sequence Number",
    "Number of FIDs",
];

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Census {
    pub period: String,
    pub physical_rows: u64,
    pub messages: u64,
    pub capped: bool,
    pub messages_by_ric: BTreeMap<String, u64>,
    pub msg_classes: BTreeMap<String, u64>,
    pub update_types: BTreeMap<String, u64>,
    pub summary_masks: BTreeMap<String, u64>,
    pub messages_without_summary: u64,
    pub map_actions: BTreeMap<String, u64>,
    pub map_entries: u64,
    pub book_images: u64,
    pub refresh_without_entries: u64,
    pub add_existing: u64,
    pub update_missing: u64,
    pub delete_missing: u64,
    pub final_depth_by_ric: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
struct RawFid {
    number: u16,
    name: String,
    value: String,
    enum_value: String,
}

#[derive(Debug)]
struct RawEntry {
    action: String,
    key: String,
    declared: usize,
    fids: Vec<RawFid>,
}

#[derive(Debug)]
struct MessageBuilder {
    ric: String,
    ts_utc_ns: u64,
    source_row: u64,
    source_sequence: Option<u64>,
    gmt_offset_minutes: i16,
    message_class: MessageClass,
    update_type: String,
    pe_code: u16,
    template_number: Option<u16>,
    summary_declared: Option<usize>,
    summary_fids: Vec<RawFid>,
    entries: Vec<RawEntry>,
    current_entry: Option<usize>,
}

impl MessageBuilder {
    fn from_outer(record: &StringRecord, source_row: u64) -> Result<Self> {
        if field(record, 1) != "Market By Price" || field(record, 4) != "Raw" {
            bail!("source row {source_row} is not a Market By Price Raw outer row");
        }
        if parse_usize(field(record, 13), "outer Number of FIDs", source_row)? != 0 {
            bail!("source row {source_row} outer Number of FIDs is not zero");
        }
        let ric = field(record, 0).to_string();
        if ric.is_empty() || ric.len() > 255 || !ric.is_ascii() {
            bail!("invalid RIC at source row {source_row}: {ric:?}");
        }
        Ok(Self {
            ric,
            ts_utc_ns: parse_utc_ns(field(record, 2))
                .with_context(|| format!("source row {source_row} Date-Time"))?,
            source_row,
            source_sequence: parse_optional_u64(
                field(record, 12),
                "Key/Msg Sequence Number",
                source_row,
            )?,
            gmt_offset_minutes: parse_gmt_offset_minutes(field(record, 3))
                .with_context(|| format!("source row {source_row} GMT Offset"))?,
            message_class: MessageClass::parse(field(record, 5))
                .with_context(|| format!("source row {source_row}"))?,
            update_type: field(record, 6).to_string(),
            pe_code: parse_u16(field(record, 10), "PE Code", source_row)?,
            template_number: parse_optional_u16(field(record, 11), "Template Number", source_row)?,
            summary_declared: None,
            summary_fids: Vec::new(),
            entries: Vec::new(),
            current_entry: None,
        })
    }

    fn push_child(&mut self, record: &StringRecord, source_row: u64) -> Result<()> {
        match field(record, 4) {
            "Summary" => {
                if self.summary_declared.is_some() {
                    bail!("source row {source_row} repeats Summary within one message");
                }
                self.current_entry = None;
                self.summary_declared = Some(parse_usize(
                    field(record, 13),
                    "Summary Number of FIDs",
                    source_row,
                )?);
            }
            "MapEntry" => {
                let declared =
                    parse_usize(field(record, 13), "MapEntry Number of FIDs", source_row)?;
                self.entries.push(RawEntry {
                    action: field(record, 6).to_string(),
                    key: field(record, 12).to_string(),
                    declared,
                    fids: Vec::with_capacity(declared),
                });
                self.current_entry = Some(self.entries.len() - 1);
            }
            "FID" => {
                let raw = RawFid {
                    number: parse_u16(field(record, 5), "FID number", source_row)?,
                    name: field(record, 7).to_string(),
                    value: field(record, 8).to_string(),
                    enum_value: field(record, 9).to_string(),
                };
                if let Some(index) = self.current_entry {
                    self.entries[index].fids.push(raw);
                } else if self.summary_declared.is_some() {
                    self.summary_fids.push(raw);
                } else {
                    bail!("source row {source_row} FID is outside Summary or MapEntry");
                }
            }
            other => bail!("source row {source_row} has unsupported child type {other:?}"),
        }
        Ok(())
    }

    fn finish(self) -> Result<LogicalMessage> {
        let summary = match self.summary_declared {
            Some(declared) => {
                if declared != self.summary_fids.len() {
                    bail!(
                        "RIC {} source row {} Summary declares {declared} FIDs but has {}",
                        self.ric,
                        self.source_row,
                        self.summary_fids.len()
                    );
                }
                Some(parse_summary(self.summary_fids).with_context(|| {
                    format!("RIC {} source row {} Summary", self.ric, self.source_row)
                })?)
            }
            None => {
                if !self.summary_fids.is_empty() {
                    bail!("message without Summary contains Summary FIDs");
                }
                None
            }
        };
        let entries = self
            .entries
            .into_iter()
            .enumerate()
            .map(|(index, raw)| {
                parse_entry(raw).with_context(|| {
                    format!(
                        "RIC {} source row {} MapEntry {}",
                        self.ric, self.source_row, index
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if self.message_class == MessageClass::Status {
            if self.source_sequence.is_some() || summary.is_some() || !entries.is_empty() {
                bail!("STATUS must be a standalone message with an empty source sequence");
            }
        } else if self.source_sequence.is_none() {
            bail!("REFRESH/UPDATE message is missing its source sequence");
        }
        Ok(LogicalMessage {
            ric: self.ric,
            ts_utc_ns: self.ts_utc_ns,
            source_row: self.source_row,
            source_sequence: self.source_sequence,
            gmt_offset_minutes: self.gmt_offset_minutes,
            message_class: self.message_class,
            update_type: self.update_type,
            pe_code: self.pe_code,
            template_number: self.template_number,
            summary,
            entries,
        })
    }
}

fn parse_summary(raw_fields: Vec<RawFid>) -> Result<SummaryDelta> {
    let mut slots: Vec<Option<SummaryField>> = vec![None; SUMMARY_FIDS.len()];
    for raw in raw_fields {
        let index = summary_index(raw.number)
            .ok_or_else(|| anyhow!("unknown Summary FID {}:{}", raw.number, raw.name))?;
        let expected_name = SUMMARY_FIDS[index].1;
        if raw.name != expected_name {
            bail!(
                "Summary FID {} has name {:?}, expected {:?}",
                raw.number,
                raw.name,
                expected_name
            );
        }
        if slots[index].is_some() {
            bail!("duplicate Summary FID {}", raw.number);
        }
        slots[index] = Some(SummaryField {
            fid: raw.number,
            value: (!raw.value.is_empty()).then_some(raw.value),
            enum_value: (!raw.enum_value.is_empty()).then_some(raw.enum_value),
        });
    }
    Ok(SummaryDelta {
        fields: slots.into_iter().flatten().collect(),
    })
}

fn parse_entry(raw: RawEntry) -> Result<LevelDelta> {
    let action = LevelAction::parse(&raw.action)?;
    if raw.declared != raw.fids.len() {
        bail!(
            "{} declares {} FIDs but has {}",
            raw.action,
            raw.declared,
            raw.fids.len()
        );
    }
    let (key_price, key_side) = parse_entry_key(&raw.key)?;
    if action == LevelAction::Delete {
        if raw.declared != 0 {
            bail!("DELETE must have zero child FIDs");
        }
        return Ok(LevelDelta {
            action,
            side: key_side,
            price_e9: key_price,
            no_ord: None,
            acc_size: None,
            level_time_ms: None,
            level_time_msp: None,
            level_date: None,
            level_time_ns: None,
        });
    }
    if raw.declared != 7 && raw.declared != 8 {
        bail!("ADD/UPDATE must have the historical 7- or 8-FID layout");
    }
    let mut fields = HashMap::with_capacity(raw.fids.len());
    for field in raw.fids {
        let expected = ENTRY_FIDS_OLD
            .iter()
            .find(|(fid, _)| *fid == field.number)
            .copied()
            .or_else(|| (field.number == ENTRY_TIME_NS_FID.0).then_some(ENTRY_TIME_NS_FID))
            .ok_or_else(|| anyhow!("unknown MapEntry FID {}:{}", field.number, field.name))?;
        if field.name != expected.1 {
            bail!(
                "MapEntry FID {} has name {:?}, expected {:?}",
                field.number,
                field.name,
                expected.1
            );
        }
        if field.value.is_empty() {
            bail!(
                "MapEntry FID {}:{} has an empty value",
                field.number,
                field.name
            );
        }
        if fields.insert(field.number, field).is_some() {
            bail!("duplicate MapEntry FID {}", expected.0);
        }
    }
    for (fid, name) in ENTRY_FIDS_OLD {
        if !fields.contains_key(&fid) {
            bail!("MapEntry missing FID {fid}:{name}");
        }
    }
    let has_ns = fields.contains_key(&ENTRY_TIME_NS_FID.0);
    if has_ns != (raw.declared == 8) {
        bail!("MapEntry field count and LV_TIM_NS presence disagree");
    }
    let price = parse_decimal_e9(value(&fields, 3427)?)?;
    if price != key_price {
        bail!("MapEntry key price and ORDER_PRC disagree");
    }
    let side_raw = value(&fields, 3428)?.parse::<u8>()?;
    let side = Side::try_from(side_raw)?;
    if side != key_side {
        bail!("MapEntry key side and ORDER_SIDE disagree");
    }
    let side_enum = &fields[&3428].enum_value;
    if (side == Side::Bid && side_enum != "BID") || (side == Side::Ask && side_enum != "ASK") {
        bail!("ORDER_SIDE value and enum disagree: {side_raw}/{side_enum:?}");
    }
    for field in fields.values() {
        if field.number != 3428 && !field.enum_value.is_empty() {
            bail!("unexpected enum string on MapEntry FID {}", field.number);
        }
    }
    Ok(LevelDelta {
        action,
        side,
        price_e9: price,
        no_ord: Some(value(&fields, 3430)?.parse::<u32>()?),
        acc_size: Some(value(&fields, 4356)?.parse::<u32>()?),
        level_time_ms: Some(value(&fields, 6527)?.parse::<u32>()?),
        level_time_msp: Some(value(&fields, 6528)?.parse::<u32>()?),
        level_date: Some(parse_date_yyyymmdd(value(&fields, 6529)?)?),
        level_time_ns: fields
            .get(&ENTRY_TIME_NS_FID.0)
            .map(|field| parse_hms_ns(&field.value))
            .transpose()?,
    })
}

fn value(fields: &HashMap<u16, RawFid>, fid: u16) -> Result<&str> {
    fields
        .get(&fid)
        .map(|field| field.value.as_str())
        .ok_or_else(|| anyhow!("missing MapEntry FID {fid}"))
}

fn parse_entry_key(value: &str) -> Result<(i64, Side)> {
    let (price, suffix) = value.split_at(value.len().checked_sub(1).context("empty MapEntry key")?);
    let side = match suffix {
        "B" => Side::Bid,
        "A" => Side::Ask,
        _ => bail!("unsupported MapEntry key {value:?}"),
    };
    Ok((parse_decimal_e9(price)?, side))
}

#[derive(Default)]
struct BookAudit {
    levels: HashMap<String, HashSet<(Side, i64)>>,
}

impl BookAudit {
    fn apply(&mut self, message: &LogicalMessage, census: &mut Census) {
        let book = self.levels.entry(message.ric.clone()).or_default();
        if message.message_class == MessageClass::Refresh {
            if message.entries.is_empty() {
                census.refresh_without_entries += 1;
            } else {
                book.clear();
                census.book_images += 1;
            }
        }
        for entry in &message.entries {
            census.map_entries += 1;
            *census
                .map_actions
                .entry(entry.action.as_str().to_string())
                .or_default() += 1;
            let key = (entry.side, entry.price_e9);
            match entry.action {
                LevelAction::Add => {
                    if !book.insert(key) {
                        census.add_existing += 1;
                    }
                }
                LevelAction::Update => {
                    if !book.contains(&key) {
                        census.update_missing += 1;
                    }
                    book.insert(key);
                }
                LevelAction::Delete => {
                    if !book.remove(&key) {
                        census.delete_missing += 1;
                    }
                }
            }
        }
    }

    fn depths(&self) -> BTreeMap<String, u64> {
        self.levels
            .iter()
            .map(|(ric, levels)| (ric.clone(), levels.len() as u64))
            .collect()
    }
}

fn observe_message(message: &LogicalMessage, census: &mut Census, books: &mut BookAudit) {
    census.messages += 1;
    *census
        .messages_by_ric
        .entry(message.ric.clone())
        .or_default() += 1;
    *census
        .msg_classes
        .entry(message.message_class.as_str().to_string())
        .or_default() += 1;
    *census
        .update_types
        .entry(message.update_type.clone())
        .or_default() += 1;
    let summary_mask = message.summary.as_ref().map_or(0_u32, |summary| {
        summary.fields.iter().fold(0_u32, |mask, field| {
            mask | (1_u32 << summary_index(field.fid).expect("parsed Summary FID"))
        })
    });
    if message.summary.is_none() {
        census.messages_without_summary += 1;
    }
    *census
        .summary_masks
        .entry(format!("{summary_mask:08x}"))
        .or_default() += 1;
    books.apply(message, census);
}

pub fn scan_csv<F>(
    path: &Path,
    period: &str,
    max_messages: Option<u64>,
    mut sink: F,
) -> Result<Census>
where
    F: FnMut(&LogicalMessage) -> Result<()>,
{
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::with_capacity(16 * 1024 * 1024, file));
    let header = reader
        .records()
        .next()
        .ok_or_else(|| anyhow!("{} is empty", path.display()))??;
    validate_header(&header)?;
    let mut census = Census {
        period: period.to_string(),
        ..Census::default()
    };
    let mut books = BookAudit::default();
    let mut current: Option<MessageBuilder> = None;
    let mut source_row = 0_u64;

    for result in reader.records() {
        let record = result.with_context(|| format!("read {}", path.display()))?;
        source_row += 1;
        census.physical_rows += 1;
        if !field(&record, 0).is_empty() {
            if let Some(builder) = current.take() {
                let message = builder.finish()?;
                observe_message(&message, &mut census, &mut books);
                sink(&message)?;
                if max_messages.is_some_and(|limit| census.messages >= limit) {
                    census.capped = true;
                    break;
                }
            }
            current = Some(MessageBuilder::from_outer(&record, source_row)?);
        } else {
            current
                .as_mut()
                .ok_or_else(|| anyhow!("orphan child at source row {source_row}"))?
                .push_child(&record, source_row)?;
        }
    }
    if !census.capped {
        if let Some(builder) = current.take() {
            let message = builder.finish()?;
            observe_message(&message, &mut census, &mut books);
            sink(&message)?;
        }
    }
    census.final_depth_by_ric = books.depths();
    Ok(census)
}

pub fn validate_header(record: &StringRecord) -> Result<()> {
    if record.len() != HEADER.len()
        || record
            .iter()
            .zip(HEADER.iter())
            .any(|(actual, expected)| actual != *expected)
    {
        bail!("unexpected MBP CSV header: {record:?}");
    }
    Ok(())
}

fn field(record: &StringRecord, index: usize) -> &str {
    record.get(index).unwrap_or("")
}

fn parse_u64(value: &str, label: &str, source_row: u64) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("source row {source_row} parse {label}={value:?}"))
}

fn parse_optional_u64(value: &str, label: &str, source_row: u64) -> Result<Option<u64>> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_u64(value, label, source_row).map(Some)
    }
}

fn parse_u16(value: &str, label: &str, source_row: u64) -> Result<u16> {
    value
        .parse::<u16>()
        .with_context(|| format!("source row {source_row} parse {label}={value:?}"))
}

fn parse_optional_u16(value: &str, label: &str, source_row: u64) -> Result<Option<u16>> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_u16(value, label, source_row).map(Some)
    }
}

fn parse_usize(value: &str, label: &str, source_row: u64) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("source row {source_row} parse {label}={value:?}"))
}

fn parse_gmt_offset_minutes(value: &str) -> Result<i16> {
    let (negative, body) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let (hours, fraction) = body.split_once('.').unwrap_or((body, ""));
    if hours.is_empty()
        || !hours.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("invalid GMT Offset {value:?}");
    }
    let hours = hours.parse::<i32>()?;
    let fraction_minutes = match fraction {
        "" => 0,
        "5" | "50" | "500" => 30,
        "25" | "250" => 15,
        "75" | "750" => 45,
        _ => bail!("GMT Offset is not an exact quarter hour: {value:?}"),
    };
    let mut minutes = hours * 60 + fraction_minutes;
    if negative {
        minutes = -minutes;
    }
    i16::try_from(minutes).context("GMT Offset does not fit i16")
}

fn parse_date_yyyymmdd(value: &str) -> Result<i32> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        bail!("invalid date {value:?}");
    }
    let year = decimal_digits(&bytes[0..4])?;
    let month = decimal_digits(&bytes[5..7])?;
    let day = decimal_digits(&bytes[8..10])?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        bail!("date field out of range {value:?}");
    }
    Ok(year * 10_000 + month * 100 + day)
}

fn decimal_digits(bytes: &[u8]) -> Result<i32> {
    if !bytes.iter().all(u8::is_ascii_digit) {
        bail!("non-digit date/timestamp field");
    }
    Ok(bytes
        .iter()
        .fold(0_i32, |value, byte| value * 10 + i32::from(byte - b'0')))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * shifted_month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn parse_utc_ns(value: &str) -> Result<u64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || *bytes.last().unwrap_or(&0) != b'Z'
    {
        bail!("expected UTC RFC3339 timestamp, got {value:?}");
    }
    let year = i64::from(decimal_digits(&bytes[0..4])?);
    let month = i64::from(decimal_digits(&bytes[5..7])?);
    let day = i64::from(decimal_digits(&bytes[8..10])?);
    let hour = i64::from(decimal_digits(&bytes[11..13])?);
    let minute = i64::from(decimal_digits(&bytes[14..16])?);
    let second = i64::from(decimal_digits(&bytes[17..19])?);
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        bail!("timestamp field out of range: {value:?}");
    }
    let fraction = if bytes.len() == 20 {
        &[][..]
    } else {
        if bytes[19] != b'.' {
            bail!("expected fractional separator in {value:?}");
        }
        &bytes[20..bytes.len() - 1]
    };
    if fraction.len() > 9 || !fraction.iter().all(u8::is_ascii_digit) {
        bail!("invalid timestamp fraction in {value:?}");
    }
    let mut nanos = i64::from(decimal_digits(fraction)?);
    for _ in fraction.len()..9 {
        nanos *= 10;
    }
    let seconds = days_from_civil(year, month, day)
        .checked_mul(86_400)
        .and_then(|base| base.checked_add(hour * 3_600 + minute * 60 + second))
        .context("timestamp seconds overflow")?;
    let total = seconds
        .checked_mul(1_000_000_000)
        .and_then(|base| base.checked_add(nanos))
        .context("timestamp nanoseconds overflow")?;
    u64::try_from(total).context("timestamp predates Unix epoch")
}
