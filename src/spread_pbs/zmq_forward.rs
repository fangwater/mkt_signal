use anyhow::{ensure, Result};
use mkt_parsers::msg::mkt_msg::MktMsgType;

use super::publisher::SPREAD_PAYLOAD_BYTES;

pub const DEFAULT_ZMQ_PORT: u16 = 6320;
pub const DEFAULT_COLO_HOST: &str = "13.115.227.29";
pub const DEFAULT_ZMQ_HWM: i32 = 128;
pub const DEFAULT_ZMQ_SOCKET_BUFFER_BYTES: i32 = 65_536;

const WIRE_MAGIC: [u8; 4] = *b"BBO1";
pub const WIRE_HEADER_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireHeader {
    pub session_id: u64,
    pub sequence: u64,
    pub sent_ts_us: i64,
}

impl WireHeader {
    pub fn encode(self) -> [u8; WIRE_HEADER_BYTES] {
        let mut out = [0_u8; WIRE_HEADER_BYTES];
        out[..4].copy_from_slice(&WIRE_MAGIC);
        out[4..8].copy_from_slice(&(SPREAD_PAYLOAD_BYTES as u32).to_le_bytes());
        out[8..16].copy_from_slice(&self.session_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.sequence.to_le_bytes());
        out[24..32].copy_from_slice(&self.sent_ts_us.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == WIRE_HEADER_BYTES,
            "invalid BBO ZMQ header length: got={} expected={}",
            bytes.len(),
            WIRE_HEADER_BYTES
        );
        ensure!(bytes[..4] == WIRE_MAGIC, "invalid BBO ZMQ header magic");

        let payload_len = u32::from_le_bytes(bytes[4..8].try_into().expect("checked header size"));
        ensure!(
            payload_len as usize == SPREAD_PAYLOAD_BYTES,
            "invalid BBO ZMQ payload length in header: got={} expected={}",
            payload_len,
            SPREAD_PAYLOAD_BYTES
        );

        Ok(Self {
            session_id: u64::from_le_bytes(bytes[8..16].try_into().expect("checked header size")),
            sequence: u64::from_le_bytes(bytes[16..24].try_into().expect("checked header size")),
            sent_ts_us: i64::from_le_bytes(bytes[24..32].try_into().expect("checked header size")),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BboMeta<'a> {
    pub symbol: &'a str,
    pub event_ts_us: i64,
}

pub fn decode_bbo_meta(payload: &[u8]) -> Result<BboMeta<'_>> {
    ensure!(
        payload.len() == SPREAD_PAYLOAD_BYTES,
        "invalid BBO payload length: got={} expected={}",
        payload.len(),
        SPREAD_PAYLOAD_BYTES
    );

    let msg_type = u32::from_le_bytes(payload[..4].try_into().expect("checked payload size"));
    ensure!(
        msg_type == MktMsgType::AskBidSpread as u32,
        "invalid BBO message type: {}",
        msg_type
    );

    let symbol_len =
        u32::from_le_bytes(payload[4..8].try_into().expect("checked payload size")) as usize;
    let timestamp_offset = 8_usize.saturating_add(symbol_len);
    ensure!(
        symbol_len > 0 && timestamp_offset.saturating_add(8) <= payload.len(),
        "invalid BBO symbol length: {}",
        symbol_len
    );
    let symbol = std::str::from_utf8(&payload[8..timestamp_offset])?;
    let event_ts_us = i64::from_le_bytes(
        payload[timestamp_offset..timestamp_offset + 8]
            .try_into()
            .expect("checked timestamp bounds"),
    );

    Ok(BboMeta {
        symbol,
        event_ts_us,
    })
}

pub fn bbo_service_name(service_root: &str, venue: &str) -> Result<String> {
    let root = service_root.trim().trim_matches('/');
    let venue = venue.trim().trim_matches('/');
    ensure!(!root.is_empty(), "service root cannot be empty");
    ensure!(!venue.is_empty(), "venue cannot be empty");
    ensure!(
        !root.contains('/'),
        "service root must be one path component: {}",
        root
    );
    ensure!(
        !venue.contains('/'),
        "venue must be one path component: {}",
        venue
    );
    Ok(format!("{root}/{venue}/ask_bid_spread"))
}

pub fn bbo_topic(venue: &str) -> Result<String> {
    let venue = venue.trim().trim_matches('/');
    ensure!(!venue.is_empty(), "venue cannot be empty");
    ensure!(
        !venue.contains('/'),
        "venue must be one path component: {}",
        venue
    );
    Ok(format!("spread_bbo/{venue}"))
}

pub fn tcp_endpoint(host: &str, port: u16) -> Result<String> {
    let host = host.trim();
    ensure!(!host.is_empty(), "ZMQ host cannot be empty");
    ensure!(port > 0, "ZMQ port cannot be zero");
    Ok(format!("tcp://{host}:{port}"))
}

pub fn is_latency_symbol(symbol: &str) -> bool {
    matches!(symbol, "BTCUSDT" | "ETHUSDT" | "SOLUSDT")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::AskBidSpreadMsg;

    #[test]
    fn wire_header_round_trip() {
        let expected = WireHeader {
            session_id: 123,
            sequence: 456,
            sent_ts_us: 1_753_000_000_123_456,
        };
        assert_eq!(WireHeader::decode(&expected.encode()).unwrap(), expected);
    }

    #[test]
    fn wire_header_rejects_bad_magic_and_payload_size() {
        let mut bytes = WireHeader {
            session_id: 1,
            sequence: 2,
            sent_ts_us: 3,
        }
        .encode();
        bytes[0] = b'X';
        assert!(WireHeader::decode(&bytes).is_err());

        let mut bytes = WireHeader {
            session_id: 1,
            sequence: 2,
            sent_ts_us: 3,
        }
        .encode();
        bytes[4..8].copy_from_slice(&64_u32.to_le_bytes());
        assert!(WireHeader::decode(&bytes).is_err());
    }

    #[test]
    fn decodes_padded_bbo_metadata() {
        let msg = AskBidSpreadMsg::create(
            "BTCUSDT".to_string(),
            1_753_000_000_123_000,
            100.0,
            2.0,
            100.1,
            3.0,
        );
        let encoded = msg.to_bytes();
        let mut payload = [0_u8; SPREAD_PAYLOAD_BYTES];
        payload[..encoded.len()].copy_from_slice(&encoded);

        assert_eq!(
            decode_bbo_meta(&payload).unwrap(),
            BboMeta {
                symbol: "BTCUSDT",
                event_ts_us: 1_753_000_000_123_000,
            }
        );
    }

    #[test]
    fn builds_default_route_names() {
        assert_eq!(
            bbo_service_name("spread_pbs", "binance-futures").unwrap(),
            "spread_pbs/binance-futures/ask_bid_spread"
        );
        assert_eq!(
            bbo_topic("binance-futures").unwrap(),
            "spread_bbo/binance-futures"
        );
        assert_eq!(
            tcp_endpoint(DEFAULT_COLO_HOST, DEFAULT_ZMQ_PORT).unwrap(),
            "tcp://13.115.227.29:6320"
        );
    }
}
