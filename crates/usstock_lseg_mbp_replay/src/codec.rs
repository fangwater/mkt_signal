use crate::model::{
    summary_index, LevelAction, LevelDelta, LogicalMessage, MessageClass, Side, SummaryDelta,
    SummaryField, SUMMARY_FIDS,
};
use anyhow::{bail, Context, Result};

pub const MAGIC: [u8; 3] = [b'M', b'B', 2];
pub const KIND_LOGICAL_MESSAGE: u8 = 1;
pub const KEY_LEN: usize = 16;
pub const VALUE_HEADER_LEN: usize = 52;
pub const PRICE_SCALE: i128 = 1_000_000_000;
const FLAG_HAS_SUMMARY: u8 = 1;
const FLAG_BOOK_IMAGE: u8 = 2;
const TEMPLATE_MISSING: u16 = u16::MAX;
const SEQUENCE_MISSING: u64 = u64::MAX;
const ENTRY_HAS_FIELDS: u16 = 1;
const ENTRY_HAS_NS: u16 = 2;

pub fn encode_key(message: &LogicalMessage) -> [u8; KEY_LEN] {
    let mut out = [0_u8; KEY_LEN];
    out[..8].copy_from_slice(&message.ts_utc_ns.to_be_bytes());
    out[8..].copy_from_slice(&message.source_row.to_be_bytes());
    out
}

pub fn decode_key(input: &[u8]) -> Result<(u64, u64)> {
    if input.len() != KEY_LEN {
        bail!("MBP key must be {KEY_LEN} bytes, got {}", input.len());
    }
    Ok((
        u64::from_be_bytes(input[..8].try_into()?),
        u64::from_be_bytes(input[8..].try_into()?),
    ))
}

pub fn encode_message(message: &LogicalMessage) -> Result<Vec<u8>> {
    if message.message_class == MessageClass::Status {
        if message.source_sequence.is_some()
            || message.summary.is_some()
            || !message.entries.is_empty()
        {
            bail!("STATUS must be standalone and have no source sequence");
        }
    } else if message.source_sequence.is_none() {
        bail!("REFRESH/UPDATE message has no source sequence");
    }
    let entry_count =
        u16::try_from(message.entries.len()).context("too many entries in message")?;
    let update_type = message.update_type.as_bytes();
    let update_len = u8::try_from(update_type.len()).context("outer update type is too long")?;
    if !message.update_type.is_ascii() {
        bail!("outer update type is not ASCII");
    }

    let (present_mask, empty_mask, enum_mask) = summary_masks(message.summary.as_ref())?;
    let mut flags = 0_u8;
    if message.summary.is_some() {
        flags |= FLAG_HAS_SUMMARY;
    }
    if message.is_book_image() {
        flags |= FLAG_BOOK_IMAGE;
    }

    let mut out =
        Vec::with_capacity(VALUE_HEADER_LEN + update_type.len() + message.entries.len() * 32);
    out.extend_from_slice(&MAGIC);
    out.push(KIND_LOGICAL_MESSAGE);
    out.push(message.message_class as u8);
    out.push(flags);
    out.extend_from_slice(&message.gmt_offset_minutes.to_le_bytes());
    out.extend_from_slice(&message.ts_utc_ns.to_le_bytes());
    out.extend_from_slice(
        &message
            .source_sequence
            .unwrap_or(SEQUENCE_MISSING)
            .to_le_bytes(),
    );
    out.extend_from_slice(&message.source_row.to_le_bytes());
    out.extend_from_slice(&message.pe_code.to_le_bytes());
    out.extend_from_slice(
        &message
            .template_number
            .unwrap_or(TEMPLATE_MISSING)
            .to_le_bytes(),
    );
    out.extend_from_slice(&entry_count.to_le_bytes());
    out.push(update_len);
    out.push(0);
    out.extend_from_slice(&present_mask.to_le_bytes());
    out.extend_from_slice(&empty_mask.to_le_bytes());
    out.extend_from_slice(&enum_mask.to_le_bytes());
    debug_assert_eq!(out.len(), VALUE_HEADER_LEN);
    out.extend_from_slice(update_type);

    if let Some(summary) = &message.summary {
        for field in &summary.fields {
            if let Some(value) = &field.value {
                encode_summary_value(&mut out, field.fid, value)?;
            }
        }
        for field in &summary.fields {
            if let Some(value) = &field.enum_value {
                encode_short_ascii(&mut out, value, "Summary enum")?;
            }
        }
    }
    for entry in &message.entries {
        encode_entry(&mut out, entry)?;
    }
    Ok(out)
}

fn summary_masks(summary: Option<&SummaryDelta>) -> Result<(u32, u32, u32)> {
    let mut present = 0_u32;
    let mut empty = 0_u32;
    let mut enums = 0_u32;
    let Some(summary) = summary else {
        return Ok((0, 0, 0));
    };
    let mut previous_index = None;
    for field in &summary.fields {
        let index = summary_index(field.fid)
            .ok_or_else(|| anyhow::anyhow!("unknown Summary FID {}", field.fid))?;
        if previous_index.is_some_and(|previous| previous >= index) {
            bail!("Summary fields are not in canonical FID bit order");
        }
        previous_index = Some(index);
        let bit = 1_u32 << index;
        if present & bit != 0 {
            bail!("duplicate Summary FID {}", field.fid);
        }
        present |= bit;
        if field.value.is_none() {
            empty |= bit;
        }
        if field.enum_value.is_some() {
            enums |= bit;
        }
    }
    Ok((present, empty, enums))
}

fn encode_summary_value(out: &mut Vec<u8>, fid: u16, value: &str) -> Result<()> {
    match fid {
        4148 => out.extend_from_slice(&parse_u32(value, "TIMACT_MS")?.to_le_bytes()),
        14269 => out.extend_from_slice(&parse_hms_ns(value)?.to_le_bytes()),
        _ => encode_short_ascii(out, value, "Summary value")?,
    }
    Ok(())
}

fn encode_entry(out: &mut Vec<u8>, entry: &LevelDelta) -> Result<()> {
    out.push(entry.action as u8);
    out.push(entry.side as u8);
    let has_fields = entry.action != LevelAction::Delete;
    let mut flags = 0_u16;
    if has_fields {
        flags |= ENTRY_HAS_FIELDS;
    }
    if entry.level_time_ns.is_some() {
        flags |= ENTRY_HAS_NS;
    }
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&entry.price_e9.to_le_bytes());
    if !has_fields {
        if entry.no_ord.is_some()
            || entry.acc_size.is_some()
            || entry.level_time_ms.is_some()
            || entry.level_time_msp.is_some()
            || entry.level_date.is_some()
            || entry.level_time_ns.is_some()
        {
            bail!("DELETE entry unexpectedly has child fields");
        }
        return Ok(());
    }
    out.extend_from_slice(&entry.no_ord.context("entry missing NO_ORD")?.to_le_bytes());
    out.extend_from_slice(
        &entry
            .acc_size
            .context("entry missing ACC_SIZE")?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &entry
            .level_time_ms
            .context("entry missing LV_TIM_MS")?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &entry
            .level_time_msp
            .context("entry missing LV_TIM_MSP")?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &entry
            .level_date
            .context("entry missing LV_DATE")?
            .to_le_bytes(),
    );
    if let Some(value) = entry.level_time_ns {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

pub fn decode_message(ric: &str, input: &[u8]) -> Result<LogicalMessage> {
    if input.len() < VALUE_HEADER_LEN || input[..3] != MAGIC {
        bail!("invalid MBP value magic or truncated header");
    }
    if input[3] != KIND_LOGICAL_MESSAGE {
        bail!("unsupported MBP value kind {}", input[3]);
    }
    let message_class = MessageClass::try_from(input[4])?;
    let flags = input[5];
    if flags & !(FLAG_HAS_SUMMARY | FLAG_BOOK_IMAGE) != 0 || input[39] != 0 {
        bail!("MBP value has non-zero reserved flags/byte");
    }
    let gmt_offset_minutes = i16::from_le_bytes(input[6..8].try_into()?);
    let ts_utc_ns = u64::from_le_bytes(input[8..16].try_into()?);
    let raw_sequence = u64::from_le_bytes(input[16..24].try_into()?);
    let source_row = u64::from_le_bytes(input[24..32].try_into()?);
    let pe_code = u16::from_le_bytes(input[32..34].try_into()?);
    let raw_template = u16::from_le_bytes(input[34..36].try_into()?);
    let entry_count = u16::from_le_bytes(input[36..38].try_into()?) as usize;
    let update_len = input[38] as usize;
    let present_mask = u32::from_le_bytes(input[40..44].try_into()?);
    let empty_mask = u32::from_le_bytes(input[44..48].try_into()?);
    let enum_mask = u32::from_le_bytes(input[48..52].try_into()?);
    let valid_mask = (1_u32 << SUMMARY_FIDS.len()) - 1;
    if (present_mask | empty_mask | enum_mask) & !valid_mask != 0
        || empty_mask & !present_mask != 0
        || enum_mask & !present_mask != 0
    {
        bail!("invalid Summary masks");
    }
    let has_summary = flags & FLAG_HAS_SUMMARY != 0;
    if !has_summary && (present_mask != 0 || empty_mask != 0 || enum_mask != 0) {
        bail!("Summary masks set without Summary row");
    }

    let mut offset = VALUE_HEADER_LEN;
    let update_type = take_ascii(input, &mut offset, update_len, "outer update type")?;
    let mut summary = has_summary.then(SummaryDelta::default);
    if let Some(summary) = &mut summary {
        for (index, (fid, _)) in SUMMARY_FIDS.iter().enumerate() {
            let bit = 1_u32 << index;
            if present_mask & bit == 0 {
                continue;
            }
            let value = if empty_mask & bit != 0 {
                None
            } else {
                Some(decode_summary_value(input, &mut offset, *fid)?)
            };
            summary.fields.push(SummaryField {
                fid: *fid,
                value,
                enum_value: None,
            });
        }
        for (index, field) in summary.fields.iter_mut().enumerate() {
            let fid_index = summary_index(field.fid).expect("known Summary FID");
            if enum_mask & (1_u32 << fid_index) != 0 {
                field.enum_value = Some(decode_short_ascii(input, &mut offset, "Summary enum")?);
            }
            let _ = index;
        }
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(decode_entry(input, &mut offset)?);
    }
    if offset != input.len() {
        bail!("MBP value has {} trailing bytes", input.len() - offset);
    }
    let message = LogicalMessage {
        ric: ric.to_string(),
        ts_utc_ns,
        source_row,
        source_sequence: (raw_sequence != SEQUENCE_MISSING).then_some(raw_sequence),
        gmt_offset_minutes,
        message_class,
        update_type,
        pe_code,
        template_number: (raw_template != TEMPLATE_MISSING).then_some(raw_template),
        summary,
        entries,
    };
    if message.message_class == MessageClass::Status {
        if message.source_sequence.is_some()
            || message.summary.is_some()
            || !message.entries.is_empty()
        {
            bail!("decoded STATUS is not a standalone message");
        }
    } else if message.source_sequence.is_none() {
        bail!("decoded REFRESH/UPDATE has no source sequence");
    }
    if (flags & FLAG_BOOK_IMAGE != 0) != message.is_book_image() {
        bail!("book-image flag does not match message contents");
    }
    Ok(message)
}

fn decode_summary_value(input: &[u8], offset: &mut usize, fid: u16) -> Result<String> {
    match fid {
        4148 => Ok(u32::from_le_bytes(take(input, offset, 4)?.try_into()?).to_string()),
        14269 => Ok(format_hms_ns(u64::from_le_bytes(
            take(input, offset, 8)?.try_into()?,
        ))?),
        _ => decode_short_ascii(input, offset, "Summary value"),
    }
}

fn decode_entry(input: &[u8], offset: &mut usize) -> Result<LevelDelta> {
    let action = LevelAction::try_from(take(input, offset, 1)?[0])?;
    let side = Side::try_from(take(input, offset, 1)?[0])?;
    let flags = u16::from_le_bytes(take(input, offset, 2)?.try_into()?);
    if flags & !(ENTRY_HAS_FIELDS | ENTRY_HAS_NS) != 0 {
        bail!("entry has unknown flags {flags:#x}");
    }
    let price_e9 = i64::from_le_bytes(take(input, offset, 8)?.try_into()?);
    let has_fields = flags & ENTRY_HAS_FIELDS != 0;
    if action == LevelAction::Delete {
        if has_fields || flags & ENTRY_HAS_NS != 0 {
            bail!("DELETE entry has field flags");
        }
        return Ok(LevelDelta {
            action,
            side,
            price_e9,
            no_ord: None,
            acc_size: None,
            level_time_ms: None,
            level_time_msp: None,
            level_date: None,
            level_time_ns: None,
        });
    }
    if !has_fields {
        bail!("ADD/UPDATE entry is missing child-field flag");
    }
    Ok(LevelDelta {
        action,
        side,
        price_e9,
        no_ord: Some(u32::from_le_bytes(take(input, offset, 4)?.try_into()?)),
        acc_size: Some(u32::from_le_bytes(take(input, offset, 4)?.try_into()?)),
        level_time_ms: Some(u32::from_le_bytes(take(input, offset, 4)?.try_into()?)),
        level_time_msp: Some(u32::from_le_bytes(take(input, offset, 4)?.try_into()?)),
        level_date: Some(i32::from_le_bytes(take(input, offset, 4)?.try_into()?)),
        level_time_ns: if flags & ENTRY_HAS_NS != 0 {
            Some(u64::from_le_bytes(take(input, offset, 8)?.try_into()?))
        } else {
            None
        },
    })
}

fn take<'a>(input: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(length)
        .context("MBP value offset overflow")?;
    let value = input.get(*offset..end).context("truncated MBP value")?;
    *offset = end;
    Ok(value)
}

fn encode_short_ascii(out: &mut Vec<u8>, value: &str, label: &str) -> Result<()> {
    if !value.is_ascii() {
        bail!("{label} is not ASCII: {value:?}");
    }
    let len = u8::try_from(value.len()).with_context(|| format!("{label} is too long"))?;
    out.push(len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_short_ascii(input: &[u8], offset: &mut usize, label: &str) -> Result<String> {
    let length = take(input, offset, 1)?[0] as usize;
    take_ascii(input, offset, length, label)
}

fn take_ascii(input: &[u8], offset: &mut usize, length: usize, label: &str) -> Result<String> {
    let value = take(input, offset, length)?;
    if !value.is_ascii() {
        bail!("{label} is not ASCII");
    }
    Ok(std::str::from_utf8(value)?.to_string())
}

pub fn parse_decimal_e9(value: &str) -> Result<i64> {
    let (negative, body) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 9
    {
        bail!("invalid decimal price {value:?}");
    }
    let whole = whole.parse::<i128>()?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i128>()?
    };
    let scaled_fraction = fraction_value * 10_i128.pow((9 - fraction.len()) as u32);
    let mut scaled = whole
        .checked_mul(PRICE_SCALE)
        .and_then(|base| base.checked_add(scaled_fraction))
        .context("scaled price overflow")?;
    if negative {
        scaled = -scaled;
    }
    i64::try_from(scaled).context("scaled price does not fit i64")
}

pub fn format_decimal_e9(value: i64) -> String {
    let negative = value < 0;
    let magnitude = i128::from(value).abs();
    let whole = magnitude / PRICE_SCALE;
    let fraction = magnitude % PRICE_SCALE;
    let sign = if negative { "-" } else { "" };
    if fraction == 0 {
        format!("{sign}{whole}")
    } else {
        let mut digits = format!("{fraction:09}");
        while digits.ends_with('0') {
            digits.pop();
        }
        format!("{sign}{whole}.{digits}")
    }
}

pub fn parse_hms_ns(value: &str) -> Result<u64> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let mut parts = whole.split(':');
    let hour = parts.next().context("time missing hour")?.parse::<u64>()?;
    let minute = parts
        .next()
        .context("time missing minute")?
        .parse::<u64>()?;
    let second = parts
        .next()
        .context("time missing second")?
        .parse::<u64>()?;
    if parts.next().is_some()
        || minute >= 60
        || second >= 60
        || fraction.len() > 9
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("invalid nanosecond clock {value:?}");
    }
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u64>()?
    };
    let nanos = fraction_value * 10_u64.pow((9 - fraction.len()) as u32);
    (hour * 3600 + minute * 60 + second)
        .checked_mul(1_000_000_000)
        .and_then(|base| base.checked_add(nanos))
        .context("nanosecond clock overflow")
}

pub fn format_hms_ns(value: u64) -> Result<String> {
    let seconds = value / 1_000_000_000;
    let nanos = value % 1_000_000_000;
    let hour = seconds / 3600;
    let minute = seconds % 3600 / 60;
    let second = seconds % 60;
    Ok(format!("{hour:02}:{minute:02}:{second:02}.{nanos:09}"))
}

fn parse_u32(value: &str, label: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .with_context(|| format!("parse {label}={value:?}"))
}
