//! RIC routing for `cme_tas_replay_all`.
//!
//! A product is the RIC prefix before a month code + trailing digits, an index
//! RIC with the leading `.` stripped, or the live stem when there is no month
//! code (`HSIEAS`, `LCOTOT`). Quotes are a
//! 1s BBO snapshot on Exch Time; missing Exch Time falls back to `Date-Time`.
//! Half-side quotes overlay only within one second bucket. Cross-part merge
//! applies the later `Date-Time` for the same bucket; seconds never relay sides.

use anyhow::{anyhow, bail, Result};
use rocksdb::MergeOperands;

use crate::{
    decode_cme_quote, encode_cme_quote, encode_ric, overlay_quote_bbo, ric_live_stem,
    validate_period, MISSING_EXCH_HMS_NS, MONTH_CODES, RIC_LEN,
};

pub const ALL_KEY_KIND_LEN: usize = 1;
pub const ALL_KEY_LEN: usize = ALL_KEY_KIND_LEN + RIC_LEN + 8 + 2 + 4;
pub const NS_PER_DAY: u64 = 86_400_000_000_000;
pub const NS_PER_SEC: u64 = 1_000_000_000;
pub const HALF_DAY_NS: u64 = NS_PER_DAY / 2;
pub const PRODUCT_CF_PREFIX: &str = "p:";
pub const FORBIDDEN_LEGACY_ROCKSDB: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb";

fn is_source_product(product: &str) -> bool {
    !product.is_empty() && product.bytes().all(|byte| byte.is_ascii_graphic())
}

/// Product name for a source `#RIC`, or `None` if it cannot be parsed.
pub fn parse_product(ric: &str) -> Option<String> {
    if ric.is_empty() || !ric.is_ascii() {
        return None;
    }
    if let Some(rest) = ric.strip_prefix('.') {
        if !is_source_product(rest) {
            return None;
        }
        return Some(rest.to_string());
    }
    let stem = ric_live_stem(ric);
    if stem.is_empty() || !stem.is_ascii() {
        return None;
    }
    let bytes = stem.as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1].is_ascii_digit() {
        end -= 1;
    }
    if end != bytes.len() && end > 0 && MONTH_CODES.contains(&bytes[end - 1]) {
        let product = &stem[..end - 1];
        if is_source_product(product) {
            return Some(product.to_string());
        }
    }
    // Non-dated instrument (HSIEAS, LCOTOT): the live stem is the product.
    if is_source_product(stem) {
        return Some(stem.to_string());
    }
    None
}

/// Left-hand calendar year of a TAS period directory suffix.
pub fn period_year(period: &str) -> Result<u16> {
    validate_period(period)?;
    let year = period
        .get(..4)
        .ok_or_else(|| anyhow!("TAS period {period:?} has no year prefix"))?;
    if !year.bytes().all(|b| b.is_ascii_digit()) {
        bail!("TAS period {period:?} does not start with a 4-digit year");
    }
    year.parse::<u16>()
        .map_err(|err| anyhow!("TAS period {period:?} year: {err}"))
}

pub fn product_cf_name(year: u16, product: &str) -> Result<String> {
    if !is_source_product(product) {
        bail!("product {product:?} is not printable ASCII");
    }
    let mut encoded = String::with_capacity(product.len());
    for byte in product.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    Ok(format!("{PRODUCT_CF_PREFIX}{year}:{encoded}"))
}

fn is_encoded_product_cf_segment(segment: &str) -> bool {
    fn is_upper_hex(byte: u8) -> bool {
        byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte)
    }

    let bytes = segment.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            index += 1;
            continue;
        }
        if byte != b'%'
            || index + 2 >= bytes.len()
            || !is_upper_hex(bytes[index + 1])
            || !is_upper_hex(bytes[index + 2])
        {
            return false;
        }
        index += 3;
    }
    true
}

pub fn is_product_cf_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(PRODUCT_CF_PREFIX) else {
        return false;
    };
    let Some((year, product)) = rest.split_once(':') else {
        return false;
    };
    year.len() == 4
        && year.bytes().all(|b| b.is_ascii_digit())
        && is_encoded_product_cf_segment(product)
}

pub fn encode_all_key(
    kind: u8,
    ric: &str,
    ts_utc_ns: u64,
    part: u16,
    seq: u32,
) -> Result<[u8; ALL_KEY_LEN]> {
    let mut key = [0u8; ALL_KEY_LEN];
    key[0] = kind;
    key[1..1 + RIC_LEN].copy_from_slice(&encode_ric(ric)?);
    key[1 + RIC_LEN..1 + RIC_LEN + 8].copy_from_slice(&ts_utc_ns.to_be_bytes());
    key[1 + RIC_LEN + 8..1 + RIC_LEN + 10].copy_from_slice(&part.to_be_bytes());
    key[1 + RIC_LEN + 10..].copy_from_slice(&seq.to_be_bytes());
    Ok(key)
}

/// Attach a calendar day from `Date-Time` onto `Exch Time`.
///
/// Both clocks are the same UTC face in this pack. A >12h gap means the
/// Date-Time already rolled past midnight while Exch Time is still the
/// previous day, or the reverse.
pub fn exch_event_time_ns(ts_utc_ns: u64, exch_hms_ns: u64) -> Result<u64> {
    if exch_hms_ns == MISSING_EXCH_HMS_NS {
        bail!("Exch Time is missing");
    }
    if exch_hms_ns >= NS_PER_DAY {
        bail!("Exch Time ns {exch_hms_ns} is not a time of day");
    }
    let day0 = ts_utc_ns / NS_PER_DAY;
    let mut exch_utc = day0
        .checked_mul(NS_PER_DAY)
        .and_then(|v| v.checked_add(exch_hms_ns))
        .ok_or_else(|| anyhow!("Exch Time overflowed against Date-Time"))?;
    if exch_utc > ts_utc_ns && exch_utc - ts_utc_ns > HALF_DAY_NS {
        exch_utc = exch_utc
            .checked_sub(NS_PER_DAY)
            .ok_or_else(|| anyhow!("Exch Time underflowed against Date-Time"))?;
    } else if ts_utc_ns > exch_utc && ts_utc_ns - exch_utc > HALF_DAY_NS {
        exch_utc = exch_utc
            .checked_add(NS_PER_DAY)
            .ok_or_else(|| anyhow!("Exch Time overflowed next day"))?;
    }
    Ok(exch_utc)
}

/// Attach the UTC date and floor the exchange event time to one second.
pub fn exch_second_bucket_ns(ts_utc_ns: u64, exch_hms_ns: u64) -> Result<u64> {
    Ok((exch_event_time_ns(ts_utc_ns, exch_hms_ns)? / NS_PER_SEC) * NS_PER_SEC)
}

/// 1s BBO bucket. Missing Exch Time falls back to `Date-Time`, floored to 1s.
/// The bool is true when the fallback was used.
pub fn quote_second_bucket_ns(ts_utc_ns: u64, exch_hms_ns: u64) -> Result<(u64, bool)> {
    if exch_hms_ns == MISSING_EXCH_HMS_NS {
        return Ok(((ts_utc_ns / NS_PER_SEC) * NS_PER_SEC, true));
    }
    Ok((exch_second_bucket_ns(ts_utc_ns, exch_hms_ns)?, false))
}

pub fn later_quote_value(left: &[u8], right: &[u8]) -> Vec<u8> {
    match (decode_cme_quote(left), decode_cme_quote(right)) {
        (Ok(a), Ok(b)) => {
            let merged = if b.ts_utc_ns >= a.ts_utc_ns {
                overlay_quote_bbo(&a, &b)
            } else {
                overlay_quote_bbo(&b, &a)
            };
            encode_cme_quote(&merged)
                .map(|bytes| bytes.to_vec())
                .unwrap_or_else(|_| {
                    if b.ts_utc_ns >= a.ts_utc_ns {
                        right.to_vec()
                    } else {
                        left.to_vec()
                    }
                })
        }
        (Ok(_), Err(_)) => left.to_vec(),
        (Err(_), Ok(_)) => right.to_vec(),
        (Err(_), Err(_)) => {
            if right.len() >= left.len() {
                right.to_vec()
            } else {
                left.to_vec()
            }
        }
    }
}

pub fn quote_last_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut best = existing.map(|value| value.to_vec());
    for operand in operands {
        best = Some(match best {
            Some(prev) => later_quote_value(&prev, operand),
            None => operand.to_vec(),
        });
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode_cme_quote, SlimQuote, KIND_CME_QUOTE};

    #[test]
    fn parse_product_table() {
        let cases = [
            ("CLG24", Some("CL")),
            ("ADH0", Some("AD")),
            ("SILH24", Some("SIL")),
            ("ZBH24", Some("ZB")),
            ("LCOG24", Some("LCO")),
            ("NGLNDG1324", Some("NGLND")),
            ("S1RK8", Some("S1R")),
            ("CFI2H4", Some("CFI2")),
            ("ADF26^2", Some("AD")),
            (".FTXIN9", Some("FTXIN9")),
            (".NSEI", Some("NSEI")),
            (".FTFTCRTWNT", Some("FTFTCRTWNT")),
            ("SF0", Some("S")),
            ("SFH0", Some("SF")),
            ("USH0", Some("US")),
            ("CH24", Some("C")),
            ("CDH24", Some("CD")),
            ("WTCLZ6", Some("WTCL")),
            ("UM4", Some("U")),
            ("LCOTOT", Some("LCOTOT")),
            ("HSIEAS", Some("HSIEAS")),
            ("HSIEAS^1", Some("HSIEAS")),
            ("FCPO-TOT", Some("FCPO-TOT")),
            ("VND5YRD=SCBV", Some("VND5YRD=SCBV")),
            ("", None),
            (".", None),
        ];
        for (ric, expected) in cases {
            assert_eq!(
                parse_product(ric).as_deref(),
                expected,
                "parse_product({ric:?})"
            );
        }
    }

    #[test]
    fn period_year_uses_left_hand_year() {
        assert_eq!(period_year("2010-01-01_2011-01-01").unwrap(), 2010);
        assert_eq!(period_year("2026-01-01_2026-06-01").unwrap(), 2026);
        assert!(period_year("").is_err());
    }

    #[test]
    fn product_cf_name_format() {
        assert_eq!(product_cf_name(2010, "CL").unwrap(), "p:2010:CL");
        assert_eq!(product_cf_name(2026, "NGLND").unwrap(), "p:2026:NGLND");
        assert_eq!(product_cf_name(2010, "FTXIN9").unwrap(), "p:2010:FTXIN9");
        assert_eq!(
            product_cf_name(2011, "FCPO-TOT").unwrap(),
            "p:2011:FCPO%2DTOT"
        );
        assert_eq!(
            product_cf_name(2010, "VND5YRD=SCBV").unwrap(),
            "p:2010:VND5YRD%3DSCBV"
        );
        assert_ne!(
            product_cf_name(2010, "A-B").unwrap(),
            product_cf_name(2010, "A_B").unwrap()
        );
        assert!(is_product_cf_name("p:2010:CL"));
        assert!(is_product_cf_name("p:2011:FCPO%2DTOT"));
        assert!(is_product_cf_name("p:2010:VND5YRD%3DSCBV"));
        assert!(!is_product_cf_name("p:2011:FCPO%2dTOT"));
        assert!(!is_product_cf_name("cme_quote"));
        assert!(!is_product_cf_name("p:2010"));
    }

    #[test]
    fn exch_second_bucket_same_day() {
        let day = 20_454u64;
        let exch = 82_800_000_000_000; // 23:00:00
        let ts = day * NS_PER_DAY + exch + 24_033_858;
        let bucket = exch_second_bucket_ns(ts, exch).unwrap();
        assert_eq!(bucket, day * NS_PER_DAY + exch);
    }

    #[test]
    fn exch_second_bucket_rolls_back_across_utc_midnight() {
        let day = 20_000u64;
        let ts = day * NS_PER_DAY + 10_000_000;
        let exch = NS_PER_DAY - 50_000_000;
        let bucket = exch_second_bucket_ns(ts, exch).unwrap();
        assert_eq!(
            bucket,
            (day - 1) * NS_PER_DAY + ((NS_PER_DAY - 50_000_000) / NS_PER_SEC) * NS_PER_SEC
        );
    }

    #[test]
    fn exch_event_time_keeps_subsecond_precision() {
        let day = 20_000u64;
        let exch = 12_345_678_901u64;
        let ts = day * NS_PER_DAY + exch + 20_000_000;
        assert_eq!(
            exch_event_time_ns(ts, exch).unwrap(),
            day * NS_PER_DAY + exch
        );
    }

    #[test]
    fn exch_second_bucket_rejects_missing_exch_time() {
        assert!(exch_second_bucket_ns(1, crate::MISSING_EXCH_HMS_NS).is_err());
    }

    #[test]
    fn quote_second_bucket_falls_back_to_date_time() {
        let ts = 1_700_000_123_456_789;
        let (bucket, fallback) = quote_second_bucket_ns(ts, crate::MISSING_EXCH_HMS_NS).unwrap();
        assert!(fallback);
        assert_eq!(bucket, (ts / NS_PER_SEC) * NS_PER_SEC);
        let exch = 82_800_000_000_000;
        let day = 20_454u64;
        let ts = day * NS_PER_DAY + exch + 24_033_858;
        let (bucket, fallback) = quote_second_bucket_ns(ts, exch).unwrap();
        assert!(!fallback);
        assert_eq!(bucket, day * NS_PER_DAY + exch);
    }

    #[test]
    fn later_quote_keeps_newer_date_time() {
        let older = encode_cme_quote(&SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 100,
            exch_hms_ns: 1,
            bid: 1,
            bid_size: 1,
            ask: 2,
            ask_size: 1,
        })
        .unwrap();
        let newer = encode_cme_quote(&SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 200,
            exch_hms_ns: 1,
            bid: 3,
            bid_size: 1,
            ask: 4,
            ask_size: 1,
        })
        .unwrap();
        let picked = later_quote_value(&older, &newer);
        assert_eq!(picked, newer.as_slice());
        assert_eq!(
            encode_all_key(KIND_CME_QUOTE, "ADF26", 1, 0, 0).unwrap()[0],
            KIND_CME_QUOTE
        );
        let later_bid_only = encode_cme_quote(&SlimQuote {
            ric: "ADF26".to_string(),
            ts_utc_ns: 300,
            exch_hms_ns: 1,
            bid: 5,
            bid_size: 4,
            ask: crate::MISSING_PRICE,
            ask_size: crate::MISSING_VOLUME,
        })
        .unwrap();
        let overlaid = decode_cme_quote(&later_quote_value(&newer, &later_bid_only)).unwrap();
        assert_eq!(overlaid.bid, 5);
        assert_eq!(overlaid.bid_size, 4);
        assert_eq!(overlaid.ask, 4);
        assert_eq!(overlaid.ask_size, 1);
        assert_eq!(overlaid.ts_utc_ns, 300);
    }
}
