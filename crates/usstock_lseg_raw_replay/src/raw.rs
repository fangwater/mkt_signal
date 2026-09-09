use anyhow::{anyhow, bail, Context, Result};
use csv::StringRecord;
use std::io::{Cursor, ErrorKind, Read, Write};

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

const PARSED_MESSAGE_MAX: usize = 16 * 1024 * 1024;

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).context("parsed RAW string exceeds u32")?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0; 4];
    input.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn take_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn take_string(input: &mut Cursor<&[u8]>) -> Result<String> {
    let len = usize::try_from(take_u32(input)?)?;
    if len > PARSED_MESSAGE_MAX
        || len
            > input
                .get_ref()
                .len()
                .saturating_sub(input.position() as usize)
    {
        bail!("invalid parsed RAW string length {len}");
    }
    let start = usize::try_from(input.position())?;
    let end = start + len;
    input.set_position(u64::try_from(end)?);
    Ok(std::str::from_utf8(&input.get_ref()[start..end])?.to_owned())
}

/// Compact lossless representation after CSV boundary/FID validation.
pub fn write_parsed_message<W: Write>(writer: &mut W, message: &RawMessage) -> Result<u64> {
    let mut payload = Vec::with_capacity(256 + message.fields.len() * 40);
    payload.extend_from_slice(&message.source_row.to_le_bytes());
    for value in [
        &message.ric,
        &message.date_time,
        &message.message_class,
        &message.update_type,
        &message.source_sequence,
    ] {
        put_string(&mut payload, value)?;
    }
    payload.extend_from_slice(&u32::try_from(message.fields.len())?.to_le_bytes());
    for field in &message.fields {
        payload.extend_from_slice(&field.fid.to_le_bytes());
        put_string(&mut payload, &field.name)?;
        put_string(&mut payload, &field.value)?;
        put_string(&mut payload, &field.enum_value)?;
    }
    if payload.len() > PARSED_MESSAGE_MAX {
        bail!("parsed RAW message exceeds {PARSED_MESSAGE_MAX} bytes");
    }
    writer.write_all(&u32::try_from(payload.len())?.to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(u64::try_from(payload.len() + 4)?)
}

pub fn read_parsed_messages<R, F>(
    mut reader: R,
    expected_ric: &str,
    mut on_message: F,
) -> Result<u64>
where
    R: Read,
    F: FnMut(RawMessage) -> Result<()>,
{
    let mut count = 0;
    loop {
        let mut length = [0; 4];
        let first = match reader.read(&mut length[..1]) {
            Ok(0) => return Ok(count),
            Ok(1) => 1,
            Ok(_) => unreachable!(),
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        };
        if let Err(error) = reader.read_exact(&mut length[first..]) {
            if error.kind() == ErrorKind::UnexpectedEof {
                bail!("truncated parsed RAW length after {count} messages");
            }
            return Err(error.into());
        }
        let len = usize::try_from(u32::from_le_bytes(length))?;
        if len == 0 || len > PARSED_MESSAGE_MAX {
            bail!("invalid parsed RAW message length {len}");
        }
        let mut payload = vec![0; len];
        reader
            .read_exact(&mut payload)
            .context("truncated parsed RAW payload")?;
        let mut input = Cursor::new(payload.as_slice());
        let source_row = take_u64(&mut input)?;
        let ric = take_string(&mut input)?;
        let date_time = take_string(&mut input)?;
        let message_class = take_string(&mut input)?;
        let update_type = take_string(&mut input)?;
        let source_sequence = take_string(&mut input)?;
        if ric != expected_ric {
            bail!("parsed segment expected {expected_ric}, found {ric}");
        }
        let fields_len = usize::try_from(take_u32(&mut input)?)?;
        let mut fields = Vec::with_capacity(fields_len);
        for _ in 0..fields_len {
            fields.push(RawField {
                fid: take_u32(&mut input)?,
                name: take_string(&mut input)?,
                value: take_string(&mut input)?,
                enum_value: take_string(&mut input)?,
            });
        }
        if input.position() != u64::try_from(payload.len())? {
            bail!("parsed RAW payload has trailing bytes");
        }
        on_message(RawMessage {
            source_row,
            ric,
            date_time,
            message_class,
            update_type,
            source_sequence,
            fields,
            declared_fields: fields_len,
        })?;
        count += 1;
    }
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
