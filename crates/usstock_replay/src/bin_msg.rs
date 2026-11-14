//! Lossless binary messages stored by the US-stock TAS replay.
//!
//! `UsStockSourceRowMsg` preserves every CSV cell in source-header order.
//! `UsStockSchemaMsg` preserves that header.  RocksDB stores their encoded
//! bytes directly; it does not hold a second, lossy event representation.

use anyhow::{anyhow, bail, Result};
use csv::StringRecord;

pub const SOURCE_ROW_MAGIC: [u8; 2] = *b"UR";
pub const SCHEMA_MAGIC: [u8; 2] = *b"UH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsStockSourceRowMsg {
    encoded: Vec<u8>,
}

impl UsStockSourceRowMsg {
    /// Build a message from a CSV row. `expected_columns` is the period schema
    /// width; extra source cells are retained instead of discarded.
    pub fn from_csv(record: &StringRecord, expected_columns: usize) -> Result<Self> {
        let column_count = record.len().max(expected_columns);
        let column_count_u32 = u32::try_from(column_count)
            .map_err(|_| anyhow!("source row has too many columns: {column_count}"))?;
        let bitmap_len = (column_count + 7) / 8;
        let bitmap_len_u32 = u32::try_from(bitmap_len)
            .map_err(|_| anyhow!("source row bitmap is too large: {bitmap_len}"))?;
        let mut bitmap = vec![0_u8; bitmap_len];
        let mut payloads = Vec::new();
        for index in 0..column_count {
            let value = record.get(index).unwrap_or("");
            if value.is_empty() {
                continue;
            }
            bitmap[index / 8] |= 1 << (index % 8);
            let len = u32::try_from(value.len())
                .map_err(|_| anyhow!("source cell {index} exceeds u32 length"))?;
            payloads.push((len, value));
        }
        let mut encoded = Vec::with_capacity(
            10 + bitmap_len
                + payloads
                    .iter()
                    .map(|(_, value)| value.len() + 4)
                    .sum::<usize>(),
        );
        encoded.extend_from_slice(&SOURCE_ROW_MAGIC);
        encoded.extend_from_slice(&column_count_u32.to_le_bytes());
        encoded.extend_from_slice(&bitmap_len_u32.to_le_bytes());
        encoded.extend_from_slice(&bitmap);
        for (len, value) in payloads {
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(value.as_bytes());
        }
        Ok(Self { encoded })
    }

    pub fn from_bytes(encoded: Vec<u8>) -> Result<Self> {
        decode_cells(&encoded)?;
        Ok(Self { encoded })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }

    pub fn cells(&self) -> Result<Vec<String>> {
        decode_cells(&self.encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsStockSchemaMsg {
    encoded: Vec<u8>,
}

impl UsStockSchemaMsg {
    pub fn from_headers(headers: &StringRecord) -> Result<Self> {
        let count = u32::try_from(headers.len())
            .map_err(|_| anyhow!("source schema has too many columns"))?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&SCHEMA_MAGIC);
        encoded.extend_from_slice(&count.to_le_bytes());
        for name in headers.iter() {
            let len = u32::try_from(name.len())
                .map_err(|_| anyhow!("source schema column exceeds u32 length"))?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(name.as_bytes());
        }
        Ok(Self { encoded })
    }

    pub fn from_bytes(encoded: Vec<u8>) -> Result<Self> {
        decode_headers(&encoded)?;
        Ok(Self { encoded })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }

    pub fn headers(&self) -> Result<Vec<String>> {
        decode_headers(&self.encoded)
    }
}

fn take<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow!("bin msg offset overflow"))?;
    let value = buf
        .get(*offset..end)
        .ok_or_else(|| anyhow!("truncated bin msg"))?;
    *offset = end;
    Ok(value)
}

fn ensure_consumed(buf: &[u8], offset: usize) -> Result<()> {
    if offset != buf.len() {
        bail!("bin msg has {} trailing bytes", buf.len() - offset);
    }
    Ok(())
}

fn decode_cells(encoded: &[u8]) -> Result<Vec<String>> {
    if encoded.len() < 10 || encoded[..2] != SOURCE_ROW_MAGIC {
        bail!("invalid US-stock source-row bin msg");
    }
    let column_count = u32::from_le_bytes(encoded[2..6].try_into()?) as usize;
    let bitmap_len = u32::from_le_bytes(encoded[6..10].try_into()?) as usize;
    if bitmap_len != (column_count + 7) / 8 {
        bail!("source-row bitmap length does not match column count");
    }
    let mut offset = 10;
    let bitmap = take(encoded, &mut offset, bitmap_len)?;
    let mut cells = Vec::with_capacity(column_count);
    for index in 0..column_count {
        if bitmap[index / 8] & (1 << (index % 8)) == 0 {
            cells.push(String::new());
            continue;
        }
        let len = u32::from_le_bytes(take(encoded, &mut offset, 4)?.try_into()?) as usize;
        cells.push(std::str::from_utf8(take(encoded, &mut offset, len)?)?.to_string());
    }
    ensure_consumed(encoded, offset)?;
    Ok(cells)
}

fn decode_headers(encoded: &[u8]) -> Result<Vec<String>> {
    if encoded.len() < 6 || encoded[..2] != SCHEMA_MAGIC {
        bail!("invalid US-stock schema bin msg");
    }
    let count = u32::from_le_bytes(encoded[2..6].try_into()?) as usize;
    let mut offset = 6;
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u32::from_le_bytes(take(encoded, &mut offset, 4)?.try_into()?) as usize;
        names.push(std::str::from_utf8(take(encoded, &mut offset, len)?)?.to_string());
    }
    ensure_consumed(encoded, offset)?;
    Ok(names)
}
