use crate::common::tick_math::QuantizedValue;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::{bytes_helper, SignalBytes};
use crate::signal::hedge_signal::MmHedgeSignalQueryMsg;
use bytes::{Buf, BufMut, Bytes, BytesMut};

const MM_BACKWARD_QUERY_HEDGE: u8 = 1;
const MM_BACKWARD_QUERY_CANCEL_CANDIDATES: u8 = 2;
#[derive(Debug, Clone, Default)]
pub struct MmCancelTriggerCtx {
    pub trigger_ts: i64,
    pub freq_ms: u64,
}

impl SignalBytes for MmCancelTriggerCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_i64_le(self.trigger_ts);
        buf.put_u64_le(self.freq_ms);
        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 16 {
            return Err("Not enough bytes for MmCancelTriggerCtx".to_string());
        }
        let trigger_ts = bytes.get_i64_le();
        let freq_ms = bytes.get_u64_le();
        if bytes.remaining() != 0 {
            return Err("Unexpected trailing bytes for MmCancelTriggerCtx".to_string());
        }
        Ok(Self {
            trigger_ts,
            freq_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MmCancelCandidateEntry {
    pub strategy_id: i32,
    pub client_order_id: i64,
    pub symbol: [u8; 32],
    pub side: u8,
    pub price_qv: QuantizedValue,
}

impl MmCancelCandidateEntry {
    pub fn new(
        strategy_id: i32,
        client_order_id: i64,
        symbol: &str,
        side: Side,
        price_qv: QuantizedValue,
    ) -> Self {
        let mut symbol_bytes = [0u8; 32];
        let raw = symbol.as_bytes();
        let len = raw.len().min(32);
        symbol_bytes[..len].copy_from_slice(&raw[..len]);
        Self {
            strategy_id,
            client_order_id,
            symbol: symbol_bytes,
            side: side.to_u8(),
            price_qv,
        }
    }

    pub fn get_symbol(&self) -> String {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.symbol[..end]).to_string()
    }

    pub fn get_side(&self) -> Result<Side, String> {
        Side::from_u8(self.side).ok_or_else(|| format!("Invalid side: {}", self.side))
    }

    pub fn encoded_len(&self) -> usize {
        let symbol_len = self.symbol.iter().position(|&b| b == 0).unwrap_or(32);
        4 + 8 + 1 + symbol_len + 1 + 8 + 4 + 8
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MmCancelCandidateQueryMsg {
    pub trigger_ts: i64,
    pub items: Vec<MmCancelCandidateEntry>,
}

impl MmCancelCandidateQueryMsg {
    pub fn new(trigger_ts: i64) -> Self {
        Self {
            trigger_ts,
            items: Vec::new(),
        }
    }

    pub fn push(&mut self, entry: MmCancelCandidateEntry) {
        self.items.push(entry);
    }

    pub fn encoded_len(&self) -> usize {
        8 + 4 + self.items.iter().map(|item| item.encoded_len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum MmBackwardQueryMsg {
    Hedge(MmHedgeSignalQueryMsg),
    CancelCandidates(MmCancelCandidateQueryMsg),
}

impl MmBackwardQueryMsg {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Self::Hedge(msg) => {
                buf.put_u8(MM_BACKWARD_QUERY_HEDGE);
                buf.put(msg.to_bytes());
            }
            Self::CancelCandidates(msg) => {
                buf.put_u8(MM_BACKWARD_QUERY_CANCEL_CANDIDATES);
                buf.put_i64_le(msg.trigger_ts);
                buf.put_u32_le(msg.items.len() as u32);
                for item in &msg.items {
                    let (tick_i64, tick_exp) = item.price_qv.get_tick_parts();
                    buf.put_i32_le(item.strategy_id);
                    buf.put_i64_le(item.client_order_id);
                    bytes_helper::write_fixed_bytes(&mut buf, &item.symbol);
                    buf.put_u8(item.side);
                    buf.put_i64_le(tick_i64);
                    buf.put_i32_le(tick_exp);
                    buf.put_i64_le(item.price_qv.get_count());
                }
            }
        }
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 1 {
            return Err("Not enough bytes for MmBackwardQueryMsg type".to_string());
        }
        let kind = bytes.get_u8();
        match kind {
            MM_BACKWARD_QUERY_HEDGE => Ok(Self::Hedge(MmHedgeSignalQueryMsg::from_bytes(bytes)?)),
            MM_BACKWARD_QUERY_CANCEL_CANDIDATES => {
                if bytes.remaining() < 8 + 4 {
                    return Err(
                        "Not enough bytes for MmCancelCandidateQueryMsg header".to_string(),
                    );
                }
                let trigger_ts = bytes.get_i64_le();
                let item_len = bytes.get_u32_le() as usize;
                let mut items = Vec::with_capacity(item_len);
                for _ in 0..item_len {
                    if bytes.remaining() < 4 + 8 + 1 + 1 + 8 + 4 + 8 {
                        return Err(
                            "Not enough bytes for MmCancelCandidateQueryMsg item".to_string(),
                        );
                    }
                    let strategy_id = bytes.get_i32_le();
                    let client_order_id = bytes.get_i64_le();
                    let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
                    let side = bytes.get_u8();
                    if Side::from_u8(side).is_none() {
                        return Err(format!("Invalid MmCancelCandidateEntry side: {}", side));
                    }
                    let tick_i64 = bytes.get_i64_le();
                    let tick_exp = bytes.get_i32_le();
                    let count = bytes.get_i64_le();
                    items.push(MmCancelCandidateEntry {
                        strategy_id,
                        client_order_id,
                        symbol,
                        side,
                        price_qv: QuantizedValue::from_parts(tick_i64, tick_exp, count),
                    });
                }
                if bytes.remaining() != 0 && bytes.iter().any(|&b| b != 0) {
                    return Err(
                        "Unexpected non-zero trailing bytes for MmCancelCandidateQueryMsg"
                            .to_string(),
                    );
                }
                Ok(Self::CancelCandidates(MmCancelCandidateQueryMsg {
                    trigger_ts,
                    items,
                }))
            }
            _ => Err(format!("Unknown MmBackwardQueryMsg kind: {}", kind)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MmBackwardQueryMsg, MmCancelCandidateEntry, MmCancelCandidateQueryMsg, MmCancelTriggerCtx,
    };
    use crate::common::tick_math::QuantizedValue;
    use crate::pre_trade::order_manager::Side;
    use crate::signal::hedge_signal::MmHedgeSignalQueryMsg;
    use crate::signal::common::SignalBytes;

    #[test]
    fn mm_cancel_trigger_ctx_roundtrip() {
        let ctx = MmCancelTriggerCtx {
            trigger_ts: 123,
            freq_ms: 30_000,
        };
        let parsed = MmCancelTriggerCtx::from_bytes(ctx.to_bytes()).expect("roundtrip");
        assert_eq!(parsed.trigger_ts, 123);
        assert_eq!(parsed.freq_ms, 30_000);
    }

    #[test]
    fn mm_backward_query_wraps_hedge_query() {
        let msg = MmBackwardQueryMsg::Hedge(MmHedgeSignalQueryMsg::new(
            "BTCUSDT", 1.0, 2.0, 3.0, 100.0,
        ));
        let parsed = MmBackwardQueryMsg::from_bytes(msg.to_bytes()).expect("roundtrip");
        match parsed {
            MmBackwardQueryMsg::Hedge(inner) => {
                assert_eq!(inner.get_symbol(), "BTCUSDT");
            }
            _ => panic!("expected hedge query"),
        }
    }

    #[test]
    fn mm_backward_query_wraps_cancel_candidates() {
        let mut msg = MmCancelCandidateQueryMsg::new(456);
        msg.push(MmCancelCandidateEntry::new(
            11,
            22,
            "ETHUSDT",
            Side::Sell,
            QuantizedValue::from_parts(1, -2, 333),
        ));
        let parsed =
            MmBackwardQueryMsg::from_bytes(MmBackwardQueryMsg::CancelCandidates(msg).to_bytes())
                .expect("roundtrip");
        match parsed {
            MmBackwardQueryMsg::CancelCandidates(inner) => {
                assert_eq!(inner.trigger_ts, 456);
                assert_eq!(inner.items.len(), 1);
                assert_eq!(inner.items[0].strategy_id, 11);
                assert_eq!(inner.items[0].client_order_id, 22);
                assert_eq!(inner.items[0].get_symbol(), "ETHUSDT");
                assert_eq!(inner.items[0].get_side().expect("valid side"), Side::Sell);
                assert_eq!(inner.items[0].price_qv.get_count(), 333);
            }
            _ => panic!("expected cancel candidates"),
        }
    }
}
