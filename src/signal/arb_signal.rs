use crate::common::tick_math::QuantizedValue;
use crate::signal::hedge_signal::ArbHedgeSignalQueryMsg;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::common::bytes_helper;

const ARB_BACKWARD_QUERY_CANCEL_CANDIDATES: u8 = 2;
const ARB_BACKWARD_QUERY_HEDGE: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArbCancelTriggerCtx {
    pub trigger_ts: i64,
    pub freq_ms: u64,
}

impl ArbCancelTriggerCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_i64_le(self.trigger_ts);
        buf.put_u64_le(self.freq_ms);
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 16 {
            return Err("Not enough bytes for ArbCancelTriggerCtx".to_string());
        }
        let trigger_ts = bytes.get_i64_le();
        let freq_ms = bytes.get_u64_le();
        Ok(Self {
            trigger_ts,
            freq_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArbCancelCandidateEntry {
    pub strategy_id: i32,
    pub price_qv: QuantizedValue,
}

impl ArbCancelCandidateEntry {
    pub fn new(strategy_id: i32, price_qv: QuantizedValue) -> Self {
        Self {
            strategy_id,
            price_qv,
        }
    }

    pub fn encoded_len(&self) -> usize {
        4 + 8 + 4 + 8
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArbCancelCandidateSymbolGroup {
    pub symbol: [u8; 32],
    pub items: Vec<ArbCancelCandidateEntry>,
}

impl ArbCancelCandidateSymbolGroup {
    pub fn new(symbol: &str) -> Self {
        let mut symbol_buf = [0u8; 32];
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        symbol_buf[..len].copy_from_slice(&bytes[..len]);
        Self {
            symbol: symbol_buf,
            items: Vec::new(),
        }
    }

    pub fn get_symbol(&self) -> String {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.symbol[..end]).to_string()
    }

    pub fn encoded_len(&self) -> usize {
        32 + 4
            + self
                .items
                .iter()
                .map(ArbCancelCandidateEntry::encoded_len)
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArbCancelCandidateQueryMsg {
    pub trigger_ts: i64,
    pub groups: Vec<ArbCancelCandidateSymbolGroup>,
}

impl ArbCancelCandidateQueryMsg {
    pub fn new(trigger_ts: i64) -> Self {
        Self {
            trigger_ts,
            groups: Vec::new(),
        }
    }

    pub fn push_grouped(&mut self, symbol: &str, entry: ArbCancelCandidateEntry) {
        if let Some(last_group) = self.groups.last_mut() {
            if last_group.get_symbol().eq_ignore_ascii_case(symbol) {
                last_group.items.push(entry);
                return;
            }
        }
        let mut group = ArbCancelCandidateSymbolGroup::new(symbol);
        group.items.push(entry);
        self.groups.push(group);
    }

    pub fn encoded_len(&self) -> usize {
        8 + 4
            + self
                .groups
                .iter()
                .map(ArbCancelCandidateSymbolGroup::encoded_len)
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn next_encoded_len_with(&self, symbol: &str, entry: &ArbCancelCandidateEntry) -> usize {
        let add_group_overhead = match self.groups.last() {
            Some(last_group) if last_group.get_symbol().eq_ignore_ascii_case(symbol) => 0,
            _ => 32 + 4,
        };
        self.encoded_len() + add_group_overhead + entry.encoded_len()
    }
}

#[derive(Debug, Clone)]
pub enum ArbBackwardQueryMsg {
    CancelCandidates(ArbCancelCandidateQueryMsg),
    Hedge(ArbHedgeSignalQueryMsg),
}

impl ArbBackwardQueryMsg {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Self::CancelCandidates(msg) => {
                buf.put_u8(ARB_BACKWARD_QUERY_CANCEL_CANDIDATES);
                buf.put_i64_le(msg.trigger_ts);
                buf.put_u32_le(msg.groups.len() as u32);
                for group in &msg.groups {
                    bytes_helper::write_fixed_bytes(&mut buf, &group.symbol);
                    buf.put_u32_le(group.items.len() as u32);
                    for item in &group.items {
                        let (tick_i64, tick_exp) = item.price_qv.get_tick_parts();
                        buf.put_i32_le(item.strategy_id);
                        buf.put_i64_le(tick_i64);
                        buf.put_i32_le(tick_exp);
                        buf.put_i64_le(item.price_qv.get_count());
                    }
                }
            }
            Self::Hedge(msg) => {
                buf.put_u8(ARB_BACKWARD_QUERY_HEDGE);
                buf.put(msg.to_bytes());
            }
        }
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 1 {
            return Err("Not enough bytes for ArbBackwardQueryMsg type".to_string());
        }
        let kind = bytes.get_u8();
        match kind {
            ARB_BACKWARD_QUERY_CANCEL_CANDIDATES => {
                if bytes.remaining() < 8 + 4 {
                    return Err(
                        "Not enough bytes for ArbCancelCandidateQueryMsg header".to_string()
                    );
                }
                let trigger_ts = bytes.get_i64_le();
                let group_len = bytes.get_u32_le() as usize;
                let mut groups = Vec::with_capacity(group_len);
                for _ in 0..group_len {
                    if bytes.remaining() < 32 + 4 {
                        return Err(
                            "Not enough bytes for ArbCancelCandidateQueryMsg group header"
                                .to_string(),
                        );
                    }
                    let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
                    let item_len = bytes.get_u32_le() as usize;
                    let mut items = Vec::with_capacity(item_len);
                    for _ in 0..item_len {
                        if bytes.remaining() < 4 + 8 + 4 + 8 {
                            return Err(
                                "Not enough bytes for ArbCancelCandidateQueryMsg item".to_string()
                            );
                        }
                        let strategy_id = bytes.get_i32_le();
                        let tick_i64 = bytes.get_i64_le();
                        let tick_exp = bytes.get_i32_le();
                        let count = bytes.get_i64_le();
                        items.push(ArbCancelCandidateEntry {
                            strategy_id,
                            price_qv: QuantizedValue::from_parts(tick_i64, tick_exp, count),
                        });
                    }
                    groups.push(ArbCancelCandidateSymbolGroup { symbol, items });
                }
                Ok(Self::CancelCandidates(ArbCancelCandidateQueryMsg {
                    trigger_ts,
                    groups,
                }))
            }
            ARB_BACKWARD_QUERY_HEDGE => Ok(Self::Hedge(ArbHedgeSignalQueryMsg::from_bytes(bytes)?)),
            _ => Err(format!("Unknown ArbBackwardQueryMsg kind: {}", kind)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ArbBackwardQueryMsg;
    use crate::signal::hedge_signal::ArbHedgeSignalQueryMsg;

    #[test]
    fn arb_backward_query_wraps_hedge_query() {
        let msg = ArbBackwardQueryMsg::Hedge(ArbHedgeSignalQueryMsg::new(
            42, "BTCUSDT", 1.5, 0.75, 2.25, 1000.0, 101.25, 7,
        ));
        let parsed = ArbBackwardQueryMsg::from_bytes(msg.to_bytes()).expect("roundtrip");
        match parsed {
            ArbBackwardQueryMsg::Hedge(inner) => {
                assert_eq!(inner.strategy_id, 42);
                assert_eq!(inner.get_symbol(), "BTCUSDT");
                assert_eq!(inner.request_seq, 7);
                assert!((inner.net_qty - 1.5).abs() < 1e-12);
                assert!((inner.due_hedge_qty - 0.75).abs() < 1e-12);
                assert!((inner.pending_hedge_qty - 2.25).abs() < 1e-12);
                assert!((inner.symbol_exposure_u - 1000.0).abs() < 1e-12);
                assert!((inner.weighted_inventory_price - 101.25).abs() < 1e-12);
            }
            _ => panic!("expected hedge state query"),
        }
    }
}
