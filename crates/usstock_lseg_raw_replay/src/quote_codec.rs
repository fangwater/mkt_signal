use anyhow::{anyhow, bail, Result};

pub const KEY_LEN: usize = 17;
pub const QUOTE_VALUE_LEN: usize = 40;
pub const QUOTE_STATE_VALUE_LEN: usize = 24;
pub const CANDIDATE_VALUE_LEN: usize = 64;
pub const VENUE_LEN: usize = 8;

pub const MSG_QUOTE: u8 = 0x01;
pub const MSG_QUOTE_STATE: u8 = 0x20;
pub const MISSING_PRICE: i64 = i64::MIN;
pub const MISSING_SIZE: u32 = u32::MAX;
pub const MISSING_CODE: u16 = u16::MAX;
pub const SIDE_UNCHANGED: u8 = 0;
pub const SIDE_CLEAR: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteCandidate {
    pub source_ts_utc_ns: u64,
    pub source_order: u64,
    pub bid: i64,
    pub bid_size: u32,
    pub ask: i64,
    pub ask_size: u32,
    pub bid_venue: String,
    pub ask_venue: String,
    pub quality_code: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteValue {
    pub source_ts_utc_ns: u64,
    pub source_order: u64,
    pub bid: i64,
    pub bid_size: u32,
    pub ask: i64,
    pub ask_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteStateValue {
    pub source_ts_utc_ns: u64,
    pub source_order: u64,
    pub bid_action: u8,
    pub ask_action: u8,
    pub quality_code: u16,
}

pub fn encode_key(msg_type: u8, ts_utc_ns: u64, source_row: u64) -> [u8; KEY_LEN] {
    let mut out = [0_u8; KEY_LEN];
    out[0] = msg_type;
    out[1..9].copy_from_slice(&ts_utc_ns.to_be_bytes());
    out[9..17].copy_from_slice(&source_row.to_be_bytes());
    out
}

pub fn decode_key(bytes: &[u8]) -> Result<(u8, u64, u64)> {
    if bytes.len() != KEY_LEN {
        bail!(
            "RAW replay key must be {KEY_LEN} bytes, got {}",
            bytes.len()
        );
    }
    Ok((
        bytes[0],
        u64::from_be_bytes(bytes[1..9].try_into()?),
        u64::from_be_bytes(bytes[9..17].try_into()?),
    ))
}

fn encode_venue(value: &str) -> Result<[u8; VENUE_LEN]> {
    if !value.is_ascii() || value.len() > VENUE_LEN || value.as_bytes().contains(&0) {
        bail!("venue {value:?} must be ASCII and at most {VENUE_LEN} bytes");
    }
    let mut out = [0_u8; VENUE_LEN];
    out[..value.len()].copy_from_slice(value.as_bytes());
    Ok(out)
}

fn decode_venue(value: &[u8]) -> Result<String> {
    let end = value
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(value.len());
    if value[end..].iter().any(|byte| *byte != 0) {
        bail!("venue slot has nonzero bytes after terminator");
    }
    Ok(std::str::from_utf8(&value[..end])?.to_string())
}

pub fn encode_candidate(row: &QuoteCandidate) -> Result<[u8; CANDIDATE_VALUE_LEN]> {
    let mut out = [0_u8; CANDIDATE_VALUE_LEN];
    out[0..8].copy_from_slice(&row.source_ts_utc_ns.to_le_bytes());
    out[8..16].copy_from_slice(&row.source_order.to_le_bytes());
    out[16..24].copy_from_slice(&row.bid.to_le_bytes());
    out[24..32].copy_from_slice(&row.ask.to_le_bytes());
    out[32..36].copy_from_slice(&row.bid_size.to_le_bytes());
    out[36..40].copy_from_slice(&row.ask_size.to_le_bytes());
    out[40..48].copy_from_slice(&encode_venue(&row.bid_venue)?);
    out[48..56].copy_from_slice(&encode_venue(&row.ask_venue)?);
    out[56..58].copy_from_slice(&row.quality_code.to_le_bytes());
    Ok(out)
}

pub fn decode_candidate(bytes: &[u8]) -> Result<QuoteCandidate> {
    if bytes.len() != CANDIDATE_VALUE_LEN {
        bail!(
            "quote candidate must be {CANDIDATE_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    if bytes[58..].iter().any(|byte| *byte != 0) {
        bail!("quote candidate reserved bytes are nonzero");
    }
    Ok(QuoteCandidate {
        source_ts_utc_ns: u64::from_le_bytes(bytes[0..8].try_into()?),
        source_order: u64::from_le_bytes(bytes[8..16].try_into()?),
        bid: i64::from_le_bytes(bytes[16..24].try_into()?),
        ask: i64::from_le_bytes(bytes[24..32].try_into()?),
        bid_size: u32::from_le_bytes(bytes[32..36].try_into()?),
        ask_size: u32::from_le_bytes(bytes[36..40].try_into()?),
        bid_venue: decode_venue(&bytes[40..48])?,
        ask_venue: decode_venue(&bytes[48..56])?,
        quality_code: u16::from_le_bytes(bytes[56..58].try_into()?),
    })
}

pub fn encode_quote(row: &QuoteValue) -> [u8; QUOTE_VALUE_LEN] {
    let mut out = [0_u8; QUOTE_VALUE_LEN];
    out[0..8].copy_from_slice(&row.source_ts_utc_ns.to_le_bytes());
    out[8..16].copy_from_slice(&row.source_order.to_le_bytes());
    out[16..24].copy_from_slice(&row.bid.to_le_bytes());
    out[24..28].copy_from_slice(&row.bid_size.to_le_bytes());
    out[28..36].copy_from_slice(&row.ask.to_le_bytes());
    out[36..40].copy_from_slice(&row.ask_size.to_le_bytes());
    out
}

pub fn decode_quote(bytes: &[u8]) -> Result<QuoteValue> {
    if bytes.len() != QUOTE_VALUE_LEN {
        bail!(
            "QuoteMsg must be {QUOTE_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    Ok(QuoteValue {
        source_ts_utc_ns: u64::from_le_bytes(bytes[0..8].try_into()?),
        source_order: u64::from_le_bytes(bytes[8..16].try_into()?),
        bid: i64::from_le_bytes(bytes[16..24].try_into()?),
        bid_size: u32::from_le_bytes(bytes[24..28].try_into()?),
        ask: i64::from_le_bytes(bytes[28..36].try_into()?),
        ask_size: u32::from_le_bytes(bytes[36..40].try_into()?),
    })
}

pub fn encode_quote_state(row: &QuoteStateValue) -> Result<[u8; QUOTE_STATE_VALUE_LEN]> {
    if row.bid_action > SIDE_CLEAR || row.ask_action > SIDE_CLEAR {
        bail!("invalid quote state side action");
    }
    let mut out = [0_u8; QUOTE_STATE_VALUE_LEN];
    out[0..8].copy_from_slice(&row.source_ts_utc_ns.to_le_bytes());
    out[8..16].copy_from_slice(&row.source_order.to_le_bytes());
    out[16] = row.bid_action;
    out[17] = row.ask_action;
    out[18..20].copy_from_slice(&row.quality_code.to_le_bytes());
    Ok(out)
}

pub fn decode_quote_state(bytes: &[u8]) -> Result<QuoteStateValue> {
    if bytes.len() != QUOTE_STATE_VALUE_LEN {
        bail!(
            "QuoteStateMsg must be {QUOTE_STATE_VALUE_LEN} bytes, got {}",
            bytes.len()
        );
    }
    if bytes[20..].iter().any(|byte| *byte != 0) {
        bail!("QuoteStateMsg reserved bytes are nonzero");
    }
    let row = QuoteStateValue {
        source_ts_utc_ns: u64::from_le_bytes(bytes[0..8].try_into()?),
        source_order: u64::from_le_bytes(bytes[8..16].try_into()?),
        bid_action: bytes[16],
        ask_action: bytes[17],
        quality_code: u16::from_le_bytes(bytes[18..20].try_into()?),
    };
    if row.bid_action > SIDE_CLEAR || row.ask_action > SIDE_CLEAR {
        return Err(anyhow!("invalid decoded quote state side action"));
    }
    Ok(row)
}

pub fn later_candidate(left: &[u8], right: &[u8]) -> Vec<u8> {
    match (decode_candidate(left), decode_candidate(right)) {
        (Ok(a), Ok(b)) => {
            if b.source_order >= a.source_order {
                right.to_vec()
            } else {
                left.to_vec()
            }
        }
        (Ok(_), Err(_)) => left.to_vec(),
        (Err(_), Ok(_)) => right.to_vec(),
        (Err(_), Err(_)) => right.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(ts: u64, order: u64) -> QuoteCandidate {
        QuoteCandidate {
            source_ts_utc_ns: ts,
            source_order: order,
            bid: 10_000_000_000,
            bid_size: 1,
            ask: 10_100_000_000,
            ask_size: 2,
            bid_venue: "NAS".to_string(),
            ask_venue: "BAT".to_string(),
            quality_code: MISSING_CODE,
        }
    }

    #[test]
    fn fixed_codecs_round_trip() {
        let row = candidate(10, 20);
        assert_eq!(
            decode_candidate(&encode_candidate(&row).unwrap()).unwrap(),
            row
        );
        let quote = QuoteValue {
            source_ts_utc_ns: 10,
            source_order: 20,
            bid: row.bid,
            bid_size: row.bid_size,
            ask: MISSING_PRICE,
            ask_size: MISSING_SIZE,
        };
        assert_eq!(decode_quote(&encode_quote(&quote)).unwrap(), quote);
        let state = QuoteStateValue {
            source_ts_utc_ns: 10,
            source_order: 20,
            bid_action: SIDE_UNCHANGED,
            ask_action: SIDE_CLEAR,
            quality_code: 94,
        };
        assert_eq!(
            decode_quote_state(&encode_quote_state(&state).unwrap()).unwrap(),
            state
        );
        assert_eq!(
            decode_key(&encode_key(MSG_QUOTE, 30, 0)).unwrap(),
            (MSG_QUOTE, 30, 0)
        );
    }

    #[test]
    fn merge_picks_latest_source_order() {
        let a = encode_candidate(&candidate(10, 1)).unwrap();
        let b = encode_candidate(&candidate(10, 2)).unwrap();
        assert_eq!(later_candidate(&a, &b), b);
        assert_eq!(later_candidate(&b, &a), b);
    }
}
