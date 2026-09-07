use anyhow::{anyhow, bail, Context, Result};
use csv::StringRecord;
use std::io::Read;

pub const RAW_HEADER: [&str; 14] = [
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
pub struct RawField {
    pub fid: u32,
    pub name: String,
    pub value: String,
    pub enum_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    pub source_row: u64,
    pub ric: String,
    pub date_time: String,
    pub message_class: String,
    pub update_type: String,
    pub source_sequence: String,
    pub fields: Vec<RawField>,
    declared_fields: usize,
}

impl RawMessage {
    fn from_outer(source_row: u64, row: &StringRecord) -> Result<Self> {
        if row.len() < RAW_HEADER.len()
            || row.get(0).unwrap_or_default().is_empty()
            || row.get(1) != Some("Market Price")
            || row.get(4) != Some("Raw")
        {
            bail!("invalid RAW outer row {source_row}: {row:?}");
        }
        let declared_fields = row
            .get(13)
            .unwrap_or_default()
            .parse::<usize>()
            .with_context(|| format!("parse Number of FIDs at source row {source_row}"))?;
        Ok(Self {
            source_row,
            ric: row[0].to_string(),
            date_time: row[2].to_string(),
            message_class: row[5].to_string(),
            update_type: row[6].to_string(),
            source_sequence: row.get(12).unwrap_or_default().to_string(),
            fields: Vec::with_capacity(declared_fields),
            declared_fields,
        })
    }

    fn push_child(&mut self, source_row: u64, row: &StringRecord) -> Result<()> {
        if row.len() < 10 || row.get(0) != Some("") || row.get(4) != Some("FID") {
            bail!("invalid RAW FID row {source_row}: {row:?}");
        }
        self.fields.push(RawField {
            fid: row
                .get(5)
                .unwrap_or_default()
                .parse::<u32>()
                .with_context(|| format!("parse FID at source row {source_row}"))?,
            name: row.get(7).unwrap_or_default().to_string(),
            value: row.get(8).unwrap_or_default().to_string(),
            enum_value: row.get(9).unwrap_or_default().to_string(),
        });
        if self.fields.len() > self.declared_fields {
            bail!(
                "RAW message {} {} declares {} FIDs but has more",
                self.ric,
                self.date_time,
                self.declared_fields
            );
        }
        Ok(())
    }

    fn validate(self) -> Result<Self> {
        if self.fields.len() != self.declared_fields {
            bail!(
                "RAW message {} {} declares {} FIDs but has {}",
                self.ric,
                self.date_time,
                self.declared_fields,
                self.fields.len()
            );
        }
        Ok(self)
    }
}

pub fn read_messages<R, F>(reader: R, mut on_message: F) -> Result<u64>
where
    R: Read,
    F: FnMut(RawMessage) -> Result<()>,
{
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(reader);
    let mut rows = csv.records();
    let header = rows.next().ok_or_else(|| anyhow!("empty RAW input"))??;
    if header.iter().collect::<Vec<_>>() != RAW_HEADER {
        bail!("unexpected RAW header: {header:?}");
    }

    let mut current: Option<RawMessage> = None;
    let mut messages = 0_u64;
    for (index, result) in rows.enumerate() {
        let source_row = u64::try_from(index)? + 2;
        let row = result.with_context(|| format!("parse RAW CSV source row {source_row}"))?;
        if !row.get(0).unwrap_or_default().is_empty() {
            if let Some(message) = current.take() {
                on_message(message.validate()?)?;
                messages += 1;
            }
            current = Some(RawMessage::from_outer(source_row, &row)?);
        } else {
            current
                .as_mut()
                .ok_or_else(|| anyhow!("RAW FID before outer row at {source_row}"))?
                .push_child(source_row, &row)?;
        }
    }
    if let Some(message) = current {
        on_message(message.validate()?)?;
        messages += 1;
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_complete_messages() {
        let source = concat!(
            "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n",
            "AAPL.O,Market Price,2021-07-01T00:00:00.1Z,-4,Raw,UPDATE,QUOTE,,,,74,,3,2\n",
            ",,,,FID,22,,BID,10.1,\n",
            ",,,,FID,25,,ASK,10.2,\n",
        );
        let mut out = Vec::new();
        let count = read_messages(source.as_bytes(), |message| {
            out.push(message);
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(out[0].fields.len(), 2);
        assert_eq!(out[0].fields[0].fid, 22);
    }
}
