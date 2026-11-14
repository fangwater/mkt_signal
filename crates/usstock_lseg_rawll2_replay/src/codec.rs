use crate::source::{Field, Message};
use anyhow::{bail, Context, Result};

const MAGIC: &[u8; 4] = b"RL2\x01";

pub fn encode_key(message: &Message) -> [u8; 16] {
    let mut key = [0_u8; 16];
    key[..8].copy_from_slice(&message.ts_utc_ns.to_be_bytes());
    key[8..].copy_from_slice(&message.source_row.to_be_bytes());
    key
}

pub fn decode_key(key: &[u8]) -> Result<(u64, u64)> {
    if key.len() != 16 {
        bail!("rawLL2 key must be 16 bytes, got {}", key.len());
    }
    Ok((
        u64::from_be_bytes(key[..8].try_into()?),
        u64::from_be_bytes(key[8..].try_into()?),
    ))
}

fn put_u16(out: &mut Vec<u8>, value: usize, label: &str) -> Result<()> {
    out.extend_from_slice(
        &u16::try_from(value)
            .with_context(|| format!("{label} length exceeds u16"))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_u32(out: &mut Vec<u8>, value: usize, label: &str) -> Result<()> {
    out.extend_from_slice(
        &u32::try_from(value)
            .with_context(|| format!("{label} length exceeds u32"))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_text(out: &mut Vec<u8>, text: &str, width: usize, label: &str) -> Result<()> {
    if !text.is_ascii() {
        bail!("{label} is not ASCII: {text:?}");
    }
    match width {
        1 => out.push(u8::try_from(text.len()).with_context(|| format!("{label} too long"))?),
        2 => put_u16(out, text.len(), label)?,
        4 => put_u32(out, text.len(), label)?,
        _ => unreachable!(),
    }
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

pub fn encode_message(message: &Message) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64 + message.fields.len() * 32);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&message.source_row.to_le_bytes());
    out.extend_from_slice(&message.gmt_offset_minutes.to_le_bytes());
    put_text(&mut out, &message.message_class, 1, "message class")?;
    put_text(&mut out, &message.update_type, 1, "update type")?;
    put_text(&mut out, &message.source_sequence, 2, "source sequence")?;
    put_u32(&mut out, message.fields.len(), "field count")?;
    for field in &message.fields {
        out.extend_from_slice(&field.fid.to_le_bytes());
        put_text(&mut out, &field.name, 2, "FID name")?;
        put_text(&mut out, &field.value, 4, "FID value")?;
        put_text(&mut out, &field.enum_value, 2, "FID enum")?;
    }
    Ok(out)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .context("rawLL2 value offset overflow")?;
        let value = self
            .bytes
            .get(self.offset..end)
            .context("truncated rawLL2 value")?;
        self.offset = end;
        Ok(value)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn text(&mut self, width: usize) -> Result<String> {
        let len = match width {
            1 => usize::from(self.take(1)?[0]),
            2 => usize::from(self.u16()?),
            4 => usize::try_from(self.u32()?)?,
            _ => unreachable!(),
        };
        let bytes = self.take(len)?;
        if !bytes.is_ascii() {
            bail!("rawLL2 text is not ASCII");
        }
        Ok(std::str::from_utf8(bytes)?.to_owned())
    }
}

pub fn decode_message(value: &[u8], ric: &str, ts_utc_ns: u64) -> Result<Message> {
    let mut cursor = Cursor {
        bytes: value,
        offset: 0,
    };
    if cursor.take(4)? != MAGIC {
        bail!("invalid rawLL2 value magic");
    }
    let source_row = cursor.u64()?;
    let gmt_offset_minutes = i16::from_le_bytes(cursor.take(2)?.try_into()?);
    let message_class = cursor.text(1)?;
    let update_type = cursor.text(1)?;
    let source_sequence = cursor.text(2)?;
    let field_count = usize::try_from(cursor.u32()?)?;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(Field {
            fid: cursor.u32()?,
            name: cursor.text(2)?,
            value: cursor.text(4)?,
            enum_value: cursor.text(2)?,
        });
    }
    if cursor.offset != value.len() {
        bail!("trailing bytes in rawLL2 value");
    }
    Ok(Message {
        source_row,
        ric: ric.to_owned(),
        ts_utc_ns,
        gmt_offset_minutes,
        message_class,
        update_type,
        source_sequence,
        declared_fields: fields.len(),
        fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trips() {
        let message = Message {
            source_row: 2,
            ric: "ABBV.N".to_owned(),
            ts_utc_ns: 3,
            gmt_offset_minutes: -240,
            message_class: "UPDATE".to_owned(),
            update_type: "CORRECTION".to_owned(),
            source_sequence: "45744".to_owned(),
            declared_fields: 1,
            fields: vec![Field {
                fid: 15,
                name: "CURRENCY".to_owned(),
                value: "840".to_owned(),
                enum_value: "USD".to_owned(),
            }],
        };
        let key = encode_key(&message);
        assert_eq!(decode_key(&key).unwrap(), (3, 2));
        assert_eq!(
            decode_message(&encode_message(&message).unwrap(), "ABBV.N", 3).unwrap(),
            message
        );
    }
}
