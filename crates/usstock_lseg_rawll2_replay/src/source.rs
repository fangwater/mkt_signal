use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use csv::StringRecord;
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub const HEADER: [&str; 14] = [
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub fid: u32,
    pub name: String,
    pub value: String,
    pub enum_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub source_row: u64,
    pub ric: String,
    pub ts_utc_ns: u64,
    pub gmt_offset_minutes: i16,
    pub message_class: String,
    pub update_type: String,
    pub source_sequence: String,
    pub fields: Vec<Field>,
    pub(crate) declared_fields: usize,
}

impl Message {
    fn outer(source_row: u64, row: &StringRecord) -> Result<Self> {
        if row.len() < HEADER.len()
            || row.get(1) != Some("Market Price")
            || row.get(4) != Some("Legacy Level 2")
        {
            bail!("invalid rawLL2 outer row {source_row}: {row:?}");
        }
        let ric = row.get(0).unwrap_or_default();
        if ric.is_empty() || !ric.is_ascii() || ric.len() > 255 {
            bail!("invalid rawLL2 RIC at source row {source_row}: {ric:?}");
        }
        let declared_fields = row
            .get(13)
            .unwrap_or_default()
            .parse::<usize>()
            .with_context(|| format!("parse Number of FIDs at source row {source_row}"))?;
        let parsed = DateTime::parse_from_rfc3339(row.get(2).unwrap_or_default())
            .with_context(|| format!("parse UTC Date-Time at source row {source_row}"))?;
        let seconds = u64::try_from(parsed.timestamp())
            .with_context(|| format!("negative Date-Time at source row {source_row}"))?;
        let ts_utc_ns = seconds
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(u64::from(parsed.timestamp_subsec_nanos())))
            .context("UTC nanosecond timestamp overflow")?;
        let gmt_offset_minutes = row
            .get(3)
            .unwrap_or_default()
            .parse::<i16>()
            .with_context(|| format!("parse GMT Offset at source row {source_row}"))?
            .checked_mul(60)
            .context("GMT Offset minute overflow")?;
        Ok(Self {
            source_row,
            ric: ric.to_owned(),
            ts_utc_ns,
            gmt_offset_minutes,
            message_class: row.get(5).unwrap_or_default().to_owned(),
            update_type: row.get(6).unwrap_or_default().to_owned(),
            source_sequence: row.get(12).unwrap_or_default().to_owned(),
            fields: Vec::with_capacity(declared_fields),
            declared_fields,
        })
    }

    fn child(&mut self, source_row: u64, row: &StringRecord) -> Result<()> {
        if row.len() < 10 || row.get(0) != Some("") || row.get(4) != Some("FID") {
            bail!("invalid rawLL2 FID row {source_row}: {row:?}");
        }
        self.fields.push(Field {
            fid: row
                .get(5)
                .unwrap_or_default()
                .parse()
                .with_context(|| format!("parse FID at source row {source_row}"))?,
            name: row.get(7).unwrap_or_default().to_owned(),
            value: row.get(8).unwrap_or_default().to_owned(),
            enum_value: row.get(9).unwrap_or_default().to_owned(),
        });
        if self.fields.len() > self.declared_fields {
            bail!(
                "rawLL2 {} row {} declares {} FIDs but has more",
                self.ric,
                self.source_row,
                self.declared_fields
            );
        }
        Ok(())
    }

    fn finish(self) -> Result<Self> {
        if self.fields.len() != self.declared_fields {
            bail!(
                "rawLL2 {} row {} declares {} FIDs but has {}",
                self.ric,
                self.source_row,
                self.declared_fields,
                self.fields.len()
            );
        }
        Ok(self)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Census {
    pub physical_rows: u64,
    pub messages: u64,
    pub messages_by_ric: BTreeMap<String, u64>,
    pub message_classes: BTreeMap<String, u64>,
    pub update_types: BTreeMap<String, u64>,
    pub fids: BTreeMap<String, u64>,
}

impl Census {
    pub(crate) fn observe(&mut self, message: &Message) {
        self.messages += 1;
        *self.messages_by_ric.entry(message.ric.clone()).or_default() += 1;
        *self
            .message_classes
            .entry(message.message_class.clone())
            .or_default() += 1;
        *self
            .update_types
            .entry(message.update_type.clone())
            .or_default() += 1;
        for field in &message.fields {
            *self
                .fids
                .entry(format!("{}:{}", field.fid, field.name))
                .or_default() += 1;
        }
    }
}

pub fn scan_gzip<F>(path: &Path, mut on_message: F) -> Result<Census>
where
    F: FnMut(&Message) -> Result<()>,
{
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let gzip = GzDecoder::new(BufReader::new(file));
    scan_reader_limited(gzip, None, &mut on_message)
}

pub fn scan_reader<R, F>(reader: R, on_message: &mut F) -> Result<Census>
where
    R: Read,
    F: FnMut(&Message) -> Result<()>,
{
    scan_reader_limited(reader, None, on_message)
}

pub fn scan_gzip_limited<F>(path: &Path, limit: u64, mut on_message: F) -> Result<Census>
where
    F: FnMut(&Message) -> Result<()>,
{
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    scan_reader_limited(
        GzDecoder::new(BufReader::new(file)),
        Some(limit),
        &mut on_message,
    )
}

fn scan_reader_limited<R, F>(reader: R, limit: Option<u64>, on_message: &mut F) -> Result<Census>
where
    R: Read,
    F: FnMut(&Message) -> Result<()>,
{
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(reader);
    let mut rows = csv.records();
    let header = rows.next().ok_or_else(|| anyhow!("empty rawLL2 input"))??;
    if header.iter().collect::<Vec<_>>() != HEADER {
        bail!("unexpected rawLL2 CSV header: {header:?}");
    }
    let mut census = Census {
        physical_rows: 1,
        ..Default::default()
    };
    let mut current: Option<Message> = None;
    for (index, result) in rows.enumerate() {
        let source_row = u64::try_from(index)? + 2;
        let row = result.with_context(|| format!("parse rawLL2 CSV row {source_row}"))?;
        census.physical_rows += 1;
        if !row.get(0).unwrap_or_default().is_empty() {
            if let Some(message) = current.take() {
                let message = message.finish()?;
                census.observe(&message);
                on_message(&message)?;
                if limit.is_some_and(|limit| census.messages >= limit) {
                    return Ok(census);
                }
            }
            current = Some(Message::outer(source_row, &row)?);
        } else {
            current
                .as_mut()
                .ok_or_else(|| anyhow!("rawLL2 FID before outer message at row {source_row}"))?
                .child(source_row, &row)?;
        }
    }
    if let Some(message) = current {
        let message = message.finish()?;
        census.observe(&message);
        on_message(&message)?;
    }
    Ok(census)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_complete_rawll2_message() {
        let input = concat!(
            "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n",
            "ABBV.N,Market Price,2021-07-01T03:46:06.594175513Z,-4,Legacy Level 2,UPDATE,CORRECTION,,,,2508,,45744,1\n",
            ",,,,FID,15,,CURRENCY,840,USD\n",
        );
        let mut messages = Vec::new();
        let census = scan_reader(input.as_bytes(), &mut |message| {
            messages.push(message.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(census.messages, 1);
        assert_eq!(messages[0].fields[0].name, "CURRENCY");
        assert_eq!(messages[0].ts_utc_ns, 1_625_112_766_594_175_513);
    }
}
