//! Strict parser for LSEG 66-column Normalized LL2 snapshots.

use anyhow::{anyhow, bail, Context, Result};

use crate::{MISSING_PRICE, PRICE_SCALE};

pub const LL2_DEPTH_LEVELS: usize = 10;
pub const LL2_SOURCE_COLUMNS: usize = 66;
pub const MISSING_COUNT: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ll2SourceGroup {
    pub ric: String,
    pub second_utc_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedLl2Snapshot {
    pub ric: String,
    pub source_ts_utc_ns: u64,
    pub gmt_offset_minutes: i16,
    pub exch_time_ns: u64,
    pub bid_prices: [i64; LL2_DEPTH_LEVELS],
    pub bid_sizes: [i64; LL2_DEPTH_LEVELS],
    pub bid_counts: [u32; LL2_DEPTH_LEVELS],
    pub ask_prices: [i64; LL2_DEPTH_LEVELS],
    pub ask_sizes: [i64; LL2_DEPTH_LEVELS],
    pub ask_counts: [u32; LL2_DEPTH_LEVELS],
}

impl NormalizedLl2Snapshot {
    fn empty(
        ric: String,
        source_ts_utc_ns: u64,
        gmt_offset_minutes: i16,
        exch_time_ns: u64,
    ) -> Self {
        Self {
            ric,
            source_ts_utc_ns,
            gmt_offset_minutes,
            exch_time_ns,
            bid_prices: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            bid_sizes: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            bid_counts: [MISSING_COUNT; LL2_DEPTH_LEVELS],
            ask_prices: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            ask_sizes: [MISSING_PRICE; LL2_DEPTH_LEVELS],
            ask_counts: [MISSING_COUNT; LL2_DEPTH_LEVELS],
        }
    }
}

fn next_csv_field<'a>(line: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    if *offset >= line.len() {
        bail!("LL2 row ends before expected column");
    }
    if line[*offset] == b'"' {
        *offset += 1;
        let start = *offset;
        while *offset < line.len() && line[*offset] != b'"' {
            *offset += 1;
        }
        if *offset == line.len() || line.get(*offset + 1) == Some(&b'"') {
            bail!("unsupported quoted LL2 CSV field");
        }
        let end = *offset;
        *offset += 1;
        if line.get(*offset) == Some(&b',') {
            *offset += 1;
        } else if *offset != line.len() {
            bail!("unexpected byte after quoted LL2 CSV field");
        }
        return Ok(&line[start..end]);
    }
    let start = *offset;
    while *offset < line.len() && line[*offset] != b',' {
        if line[*offset] == b'"' {
            bail!("quote in unquoted LL2 CSV field");
        }
        *offset += 1;
    }
    let end = *offset;
    if line.get(*offset) == Some(&b',') {
        *offset += 1;
    }
    Ok(&line[start..end])
}

fn next_book_field<'a>(line: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    if *offset >= line.len() {
        return Ok(&[]);
    }
    next_csv_field(line, offset)
}

pub fn strip_line_ending(mut line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\n') {
        line = &line[..line.len() - 1];
    }
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    line
}

pub fn validate_normalized_ll2_header(line: &[u8]) -> Result<()> {
    let line = strip_line_ending(line);
    let mut offset = 0;
    let mut fields = Vec::with_capacity(LL2_SOURCE_COLUMNS);
    for _ in 0..LL2_SOURCE_COLUMNS {
        fields.push(next_csv_field(line, &mut offset)?);
    }
    if offset != line.len() {
        bail!("LL2 header has more than {LL2_SOURCE_COLUMNS} columns");
    }
    if fields[0] != b"#RIC"
        || fields[1] != b"Domain"
        || fields[2] != b"Date-Time"
        || fields[3] != b"GMT Offset"
        || fields[4] != b"Type"
        || fields[65] != b"Exch Time"
    {
        bail!("unexpected LL2 header identity columns");
    }
    for level in 1..=LL2_DEPTH_LEVELS {
        let base = 5 + (level - 1) * 6;
        let expected = [
            format!("L{level}-BidPrice"),
            format!("L{level}-BidSize"),
            format!("L{level}-BuyNo"),
            format!("L{level}-AskPrice"),
            format!("L{level}-AskSize"),
            format!("L{level}-SellNo"),
        ];
        for (index, name) in expected.iter().enumerate() {
            if fields[base + index] != name.as_bytes() {
                bail!("unexpected LL2 header at column {}", base + index);
            }
        }
    }
    Ok(())
}

fn parse_count(raw: &[u8]) -> Result<u32> {
    if raw.is_empty() {
        return Ok(MISSING_COUNT);
    }
    let value = raw.iter().try_fold(0u32, |value, byte| {
        if !byte.is_ascii_digit() {
            bail!("invalid LL2 book count {:?}", String::from_utf8_lossy(raw));
        }
        value
            .checked_mul(10)
            .and_then(|current| current.checked_add(u32::from(*byte - b'0')))
            .ok_or_else(|| anyhow!("LL2 book count overflow"))
    })?;
    if value == MISSING_COUNT {
        bail!("LL2 book count reserves u32::MAX for source missing");
    }
    Ok(value)
}

fn parse_scaled(raw: &[u8], field: &str) -> Result<i64> {
    if raw.is_empty() {
        return Ok(MISSING_PRICE);
    }
    let negative = raw.first() == Some(&b'-');
    let mut index = usize::from(matches!(raw.first(), Some(b'-' | b'+')));
    let integer_start = index;
    let mut integer = 0i128;
    while index < raw.len() && raw[index] != b'.' {
        if !raw[index].is_ascii_digit() {
            bail!("invalid LL2 {field} {:?}", String::from_utf8_lossy(raw));
        }
        integer = integer
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(raw[index] - b'0')))
            .ok_or_else(|| anyhow!("LL2 {field} overflow"))?;
        index += 1;
    }
    if index == integer_start {
        bail!("invalid LL2 {field} {:?}", String::from_utf8_lossy(raw));
    }
    let mut fraction = 0i128;
    let mut digits = 0usize;
    if index < raw.len() {
        index += 1;
        while index < raw.len() {
            if !raw[index].is_ascii_digit() || digits == 9 {
                bail!("invalid LL2 {field} {:?}", String::from_utf8_lossy(raw));
            }
            fraction = fraction
                .checked_mul(10)
                .and_then(|value| value.checked_add(i128::from(raw[index] - b'0')))
                .ok_or_else(|| anyhow!("LL2 {field} overflow"))?;
            digits += 1;
            index += 1;
        }
    }
    for _ in digits..9 {
        fraction *= 10;
    }
    let mut scaled = integer
        .checked_mul(PRICE_SCALE)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| anyhow!("LL2 {field} overflow"))?;
    if negative {
        scaled = -scaled;
    }
    i64::try_from(scaled).map_err(|_| anyhow!("LL2 {field} does not fit i64"))
}

fn parse_gmt_offset_minutes(raw: &[u8]) -> Result<i16> {
    if raw.is_empty() {
        bail!("LL2 GMT Offset is empty");
    }
    let scaled_hours = i128::from(parse_scaled(raw, "GMT Offset")?);
    let scaled_minutes = scaled_hours
        .checked_mul(60)
        .ok_or_else(|| anyhow!("LL2 GMT Offset overflow"))?;
    if scaled_minutes % PRICE_SCALE != 0 {
        bail!(
            "LL2 GMT Offset {:?} is not an exact minute offset",
            String::from_utf8_lossy(raw)
        );
    }
    i16::try_from(scaled_minutes / PRICE_SCALE)
        .map_err(|_| anyhow!("LL2 GMT Offset does not fit i16 minutes"))
}

fn parse_extended_exch_time_ns(raw: &str) -> Result<u64> {
    if raw.is_empty() {
        return Ok(u64::MAX);
    }
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid LL2 Exch Time fraction {raw:?}");
    }
    let mut parts = whole.split(':');
    let hour = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("invalid LL2 Exch Time {raw:?}"))?
        .parse::<u64>()
        .map_err(|error| anyhow!("invalid LL2 Exch Time hour {raw:?}: {error}"))?;
    let minute = parts
        .next()
        .ok_or_else(|| anyhow!("invalid LL2 Exch Time {raw:?}"))?
        .parse::<u64>()
        .map_err(|error| anyhow!("invalid LL2 Exch Time minute {raw:?}: {error}"))?;
    let second = parts
        .next()
        .ok_or_else(|| anyhow!("invalid LL2 Exch Time {raw:?}"))?
        .parse::<u64>()
        .map_err(|error| anyhow!("invalid LL2 Exch Time second {raw:?}: {error}"))?;
    if parts.next().is_some() || minute >= 60 || second >= 60 {
        bail!("invalid LL2 Exch Time {raw:?}");
    }
    let fraction_ns = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<u64>()
            .map_err(|error| anyhow!("invalid LL2 Exch Time fraction {raw:?}: {error}"))?
            .checked_mul(10u64.pow((9 - fraction.len()) as u32))
            .ok_or_else(|| anyhow!("LL2 Exch Time fraction overflow"))?
    };
    hour.checked_mul(3_600)
        .and_then(|value| value.checked_add(minute * 60 + second))
        .and_then(|value| value.checked_mul(1_000_000_000))
        .and_then(|value| value.checked_add(fraction_ns))
        .ok_or_else(|| anyhow!("LL2 Exch Time overflow {raw:?}"))
}

fn parse_digits(raw: &[u8]) -> Result<i32> {
    if raw.is_empty() || !raw.iter().all(u8::is_ascii_digit) {
        bail!(
            "invalid LL2 Date-Time component {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    raw.iter().try_fold(0i32, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|current| current.checked_add(i32::from(*byte - b'0')))
            .ok_or_else(|| anyhow!("LL2 Date-Time component overflow"))
    })
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era * 146_097 + doe - 719_468)
}

pub fn parse_ll2_datetime_ns(raw: &[u8]) -> Result<u64> {
    if raw.len() < 20
        || raw[4] != b'-'
        || raw[7] != b'-'
        || raw[10] != b'T'
        || raw[13] != b':'
        || raw[16] != b':'
        || raw.last() != Some(&b'Z')
    {
        bail!(
            "unsupported LL2 Date-Time {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    let year = parse_digits(&raw[0..4])?;
    let month = parse_digits(&raw[5..7])? as u32;
    let day = parse_digits(&raw[8..10])? as u32;
    let hour = parse_digits(&raw[11..13])? as u32;
    let minute = parse_digits(&raw[14..16])? as u32;
    let second = parse_digits(&raw[17..19])? as u32;
    if !(1..=12).contains(&month) || day == 0 || hour >= 24 || minute >= 60 || second >= 60 {
        bail!("invalid LL2 Date-Time {:?}", String::from_utf8_lossy(raw));
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        28 + u32::from(leap),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day > month_days[(month - 1) as usize] {
        bail!("invalid LL2 Date-Time {:?}", String::from_utf8_lossy(raw));
    }
    let fraction_raw = match raw.get(19) {
        Some(b'.') => &raw[20..raw.len() - 1],
        Some(b'Z') => &[][..],
        _ => bail!(
            "unsupported LL2 Date-Time {:?}",
            String::from_utf8_lossy(raw)
        ),
    };
    if fraction_raw.len() > 9 || !fraction_raw.iter().all(u8::is_ascii_digit) {
        bail!(
            "invalid LL2 Date-Time fraction {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    let mut fraction = if fraction_raw.is_empty() {
        0
    } else {
        u64::try_from(parse_digits(fraction_raw)?)?
    };
    for _ in fraction_raw.len()..9 {
        fraction *= 10;
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        bail!(
            "LL2 Date-Time before Unix epoch {:?}",
            String::from_utf8_lossy(raw)
        );
    }
    let seconds = u64::try_from(days)?
        .checked_mul(86_400)
        .and_then(|value| {
            value.checked_add(u64::from(hour) * 3_600 + u64::from(minute) * 60 + u64::from(second))
        })
        .ok_or_else(|| anyhow!("LL2 Date-Time overflow"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| anyhow!("LL2 Date-Time overflow"))
}

pub fn parse_normalized_ll2_line(line: &[u8]) -> Result<NormalizedLl2Snapshot> {
    let line = strip_line_ending(line);
    let mut offset = 0;
    let ric = std::str::from_utf8(next_csv_field(line, &mut offset)?)
        .context("LL2 #RIC is not UTF-8")?
        .to_string();
    let domain = next_csv_field(line, &mut offset)?;
    let date_time = next_csv_field(line, &mut offset)?;
    let gmt_offset = next_csv_field(line, &mut offset)?;
    let event_type = next_csv_field(line, &mut offset)?;
    if domain != b"Market Price" || event_type != b"Normalized LL2" {
        bail!(
            "unexpected LL2 Domain/Type for {ric}: {:?}/{:?}",
            String::from_utf8_lossy(domain),
            String::from_utf8_lossy(event_type)
        );
    }
    let source_ts_utc_ns = parse_ll2_datetime_ns(date_time)?;
    let gmt_offset_minutes = parse_gmt_offset_minutes(gmt_offset)?;
    let mut snapshot =
        NormalizedLl2Snapshot::empty(ric.clone(), source_ts_utc_ns, gmt_offset_minutes, 0);
    for level in 0..LL2_DEPTH_LEVELS {
        snapshot.bid_prices[level] =
            parse_scaled(next_book_field(line, &mut offset)?, "bid price")?;
        snapshot.bid_sizes[level] = parse_scaled(next_book_field(line, &mut offset)?, "bid size")?;
        snapshot.bid_counts[level] = parse_count(next_book_field(line, &mut offset)?)?;
        snapshot.ask_prices[level] =
            parse_scaled(next_book_field(line, &mut offset)?, "ask price")?;
        snapshot.ask_sizes[level] = parse_scaled(next_book_field(line, &mut offset)?, "ask size")?;
        snapshot.ask_counts[level] = parse_count(next_book_field(line, &mut offset)?)?;
    }
    let exch_time = std::str::from_utf8(next_book_field(line, &mut offset)?)
        .context("LL2 Exch Time is not UTF-8")?;
    snapshot.exch_time_ns = parse_extended_exch_time_ns(exch_time)?;
    if offset != line.len() {
        bail!("LL2 row for {ric} has extra columns");
    }
    Ok(snapshot)
}

pub fn parse_normalized_ll2_group(line: &[u8]) -> Result<Ll2SourceGroup> {
    let line = strip_line_ending(line);
    let mut offset = 0;
    let ric = std::str::from_utf8(next_csv_field(line, &mut offset)?)
        .context("LL2 #RIC is not UTF-8")?
        .to_string();
    let domain = next_csv_field(line, &mut offset)?;
    let date_time = next_csv_field(line, &mut offset)?;
    let _gmt_offset = next_csv_field(line, &mut offset)?;
    let event_type = next_csv_field(line, &mut offset)?;
    if domain != b"Market Price" || event_type != b"Normalized LL2" {
        bail!(
            "unexpected LL2 Domain/Type for {ric}: {:?}/{:?}",
            String::from_utf8_lossy(domain),
            String::from_utf8_lossy(event_type)
        );
    }
    let source_ts_utc_ns = parse_ll2_datetime_ns(date_time)?;
    Ok(Ll2SourceGroup {
        ric,
        second_utc_ns: source_ts_utc_ns / 1_000_000_000 * 1_000_000_000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_line() -> Vec<u8> {
        let mut fields = vec![
            "ADF26".to_string(),
            "Market Price".to_string(),
            "2026-01-01T23:00:00.019074617Z".to_string(),
            "-6".to_string(),
            "Normalized LL2".to_string(),
        ];
        for level in 0..LL2_DEPTH_LEVELS {
            fields.extend([
                format!("{:.5}", 0.6671 - level as f64 * 0.0001),
                (level + 1).to_string(),
                level.to_string(),
                format!("{:.5}", 0.66785 + level as f64 * 0.0001),
                (level + 2).to_string(),
                (level + 1).to_string(),
            ]);
        }
        fields.push("23:00:00.000000000".to_string());
        fields.join(",").into_bytes()
    }

    #[test]
    fn parses_fixed_depth_snapshot() {
        let row = parse_normalized_ll2_line(&source_line()).unwrap();
        assert_eq!(row.ric, "ADF26");
        assert_eq!(row.source_ts_utc_ns, 1_767_308_400_019_074_617);
        assert_eq!(row.bid_prices[0], 667_100_000);
        assert_eq!(row.ask_prices[0], 667_850_000);
        assert_eq!(row.bid_sizes[9], 10_000_000_000);
        assert_eq!(row.ask_counts[9], 10);
        assert_eq!(row.gmt_offset_minutes, -360);
        assert_eq!(row.exch_time_ns, 82_800_000_000_000);
    }

    #[test]
    fn accepts_second_precision_timestamp_and_trailing_empty_depth() {
        assert_eq!(
            parse_ll2_datetime_ns(b"1970-01-01T00:00:01Z").unwrap(),
            1_000_000_000
        );
        let line = b"ADF26,Market Price,2026-01-01T23:00:00Z,-6,Normalized LL2";
        let row = parse_normalized_ll2_line(line).unwrap();
        assert_eq!(row.bid_prices, [MISSING_PRICE; LL2_DEPTH_LEVELS]);
        assert_eq!(row.ask_counts, [MISSING_COUNT; LL2_DEPTH_LEVELS]);
    }

    #[test]
    fn group_uses_ric_and_utc_second() {
        let group = parse_normalized_ll2_group(&source_line()).unwrap();
        assert_eq!(group.ric, "ADF26");
        assert_eq!(group.second_utc_ns, 1_767_308_400_000_000_000);
    }

    #[test]
    fn accepts_explicit_positive_gmt_offset() {
        let line = String::from_utf8(source_line())
            .unwrap()
            .replace(",-6,", ",+0,");
        let row = parse_normalized_ll2_line(line.as_bytes()).unwrap();
        assert_eq!(row.gmt_offset_minutes, 0);
    }

    #[test]
    fn accepts_exchange_time_beyond_24_hours() {
        let line = String::from_utf8(source_line())
            .unwrap()
            .replace("23:00:00.000000000", "31:14:00.000000000");
        let row = parse_normalized_ll2_line(line.as_bytes()).unwrap();
        assert_eq!(row.exch_time_ns, (31 * 3_600 + 14 * 60) * 1_000_000_000);
    }
}
