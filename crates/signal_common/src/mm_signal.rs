use crate::common::{bytes_helper, SignalBytes, SignalSliceReader};
use crate::hedge_signal::MmHedgeSignalQueryMsg;
use crate::tick_math::QuantizedValue;
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

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        Self::from_slice(bytes.as_ref())
    }
}

impl MmCancelTriggerCtx {
    pub fn from_slice(bytes: &[u8]) -> Result<Self, String> {
        let mut reader = SignalSliceReader::new(bytes);
        let trigger_ts = reader.read_i64_le("MmCancelTriggerCtx trigger_ts")?;
        let freq_ms = reader.read_u64_le("MmCancelTriggerCtx freq_ms")?;
        reader.finish_exact("MmCancelTriggerCtx")?;
        Ok(Self {
            trigger_ts,
            freq_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MmCancelCandidateEntry {
    pub strategy_id: i32,
    pub price_qv: QuantizedValue,
}

impl MmCancelCandidateEntry {
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

#[derive(Debug, Clone, PartialEq)]
pub struct MmCancelCandidateSymbolGroup {
    pub symbol: [u8; 32],
    pub items: Vec<MmCancelCandidateEntry>,
}

impl MmCancelCandidateSymbolGroup {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: bytes_helper::fixed_bytes_from_str(symbol),
            items: Vec::new(),
        }
    }

    pub fn symbol_matches(&self, symbol: &str) -> bool {
        bytes_helper::fixed_bytes_eq_ignore_ascii_case(&self.symbol, symbol)
    }

    pub fn get_symbol(&self) -> String {
        let end = bytes_helper::fixed_bytes_len(&self.symbol);
        String::from_utf8_lossy(&self.symbol[..end]).to_string()
    }

    pub fn encoded_len(&self) -> usize {
        32 + 4
            + self
                .items
                .iter()
                .map(MmCancelCandidateEntry::encoded_len)
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MmCancelCandidateQueryMsg {
    pub trigger_ts: i64,
    pub groups: Vec<MmCancelCandidateSymbolGroup>,
}

impl MmCancelCandidateQueryMsg {
    pub fn new(trigger_ts: i64) -> Self {
        Self {
            trigger_ts,
            groups: Vec::new(),
        }
    }

    pub fn push_grouped(&mut self, symbol: &str, entry: MmCancelCandidateEntry) {
        if let Some(last_group) = self.groups.last_mut() {
            if last_group.symbol_matches(symbol) {
                last_group.items.push(entry);
                return;
            }
        }
        let mut group = MmCancelCandidateSymbolGroup::new(symbol);
        group.items.push(entry);
        self.groups.push(group);
    }

    pub fn encoded_len(&self) -> usize {
        8 + 4
            + self
                .groups
                .iter()
                .map(MmCancelCandidateSymbolGroup::encoded_len)
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn next_encoded_len_with(&self, symbol: &str, entry: &MmCancelCandidateEntry) -> usize {
        let add_group_overhead = match self.groups.last() {
            Some(last_group) if last_group.symbol_matches(symbol) => 0,
            _ => 32 + 4,
        };
        8 + 4
            + self
                .groups
                .iter()
                .map(MmCancelCandidateSymbolGroup::encoded_len)
                .sum::<usize>()
            + add_group_overhead
            + entry.encoded_len()
    }

    pub fn write_backward_to(&self, buf: &mut BytesMut) {
        buf.put_u8(MM_BACKWARD_QUERY_CANCEL_CANDIDATES);
        buf.put_i64_le(self.trigger_ts);
        buf.put_u32_le(self.groups.len() as u32);
        for group in &self.groups {
            bytes_helper::write_fixed_bytes(buf, &group.symbol);
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
}

#[derive(Debug, Clone)]
pub enum MmBackwardQueryMsg {
    Hedge(MmHedgeSignalQueryMsg),
    CancelCandidates(MmCancelCandidateQueryMsg),
}

impl MmBackwardQueryMsg {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
        buf.freeze()
    }

    pub fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Hedge(msg) => msg.encoded_len(),
            Self::CancelCandidates(msg) => msg.encoded_len(),
        }
    }

    pub fn write_to(&self, buf: &mut BytesMut) {
        match self {
            Self::Hedge(msg) => {
                buf.put_u8(MM_BACKWARD_QUERY_HEDGE);
                buf.put(msg.to_bytes());
            }
            Self::CancelCandidates(msg) => {
                msg.write_backward_to(buf);
            }
        }
    }

    pub fn write_to_slice(&self, out: &mut [u8]) -> Option<()> {
        if out.len() < self.encoded_len() {
            return None;
        }
        match self {
            Self::Hedge(msg) => {
                *out.get_mut(0)? = MM_BACKWARD_QUERY_HEDGE;
                msg.write_to_slice(out.get_mut(1..)?)?;
                Some(())
            }
            Self::CancelCandidates(_) => None,
        }
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
                    return Err("Not enough bytes for MmCancelCandidateQueryMsg header".to_string());
                }
                let trigger_ts = bytes.get_i64_le();
                let group_len = bytes.get_u32_le() as usize;
                let mut groups = Vec::with_capacity(group_len);
                for _ in 0..group_len {
                    if bytes.remaining() < 32 + 4 {
                        return Err(
                            "Not enough bytes for MmCancelCandidateQueryMsg group header"
                                .to_string(),
                        );
                    }
                    let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
                    let item_len = bytes.get_u32_le() as usize;
                    let mut items = Vec::with_capacity(item_len);
                    for _ in 0..item_len {
                        if bytes.remaining() < 4 + 8 + 4 + 8 {
                            return Err(
                                "Not enough bytes for MmCancelCandidateQueryMsg item".to_string()
                            );
                        }
                        let strategy_id = bytes.get_i32_le();
                        let tick_i64 = bytes.get_i64_le();
                        let tick_exp = bytes.get_i32_le();
                        let count = bytes.get_i64_le();
                        items.push(MmCancelCandidateEntry {
                            strategy_id,
                            price_qv: QuantizedValue::from_parts(tick_i64, tick_exp, count),
                        });
                    }
                    groups.push(MmCancelCandidateSymbolGroup { symbol, items });
                }
                if bytes.remaining() != 0 && bytes.iter().any(|&b| b != 0) {
                    return Err(
                        "Unexpected non-zero trailing bytes for MmCancelCandidateQueryMsg"
                            .to_string(),
                    );
                }
                Ok(Self::CancelCandidates(MmCancelCandidateQueryMsg {
                    trigger_ts,
                    groups,
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
    use crate::common::SignalBytes;
    use crate::hedge_signal::MmHedgeSignalQueryMsg;
    use crate::tick_math::QuantizedValue;

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
            "BTCUSDT", 1.0, 2.0, 3.0, 100.0, 101.25, 7,
        ));
        let expected = msg.to_bytes();
        let mut written = vec![0u8; msg.encoded_len()];
        msg.write_to_slice(&mut written)
            .expect("slice writer should fit");
        assert_eq!(written.as_slice(), expected.as_ref());
        let parsed = MmBackwardQueryMsg::from_bytes(msg.to_bytes()).expect("roundtrip");
        match parsed {
            MmBackwardQueryMsg::Hedge(inner) => {
                assert_eq!(inner.get_symbol(), "BTCUSDT");
                assert_eq!(inner.request_seq, 7);
                assert!((inner.weighted_inventory_price - 101.25).abs() < 1e-12);
            }
            _ => panic!("expected hedge query"),
        }
    }

    #[test]
    fn mm_backward_query_wraps_cancel_candidates() {
        let mut msg = MmCancelCandidateQueryMsg::new(456);
        msg.push_grouped(
            "ETHUSDT",
            MmCancelCandidateEntry::new(11, QuantizedValue::from_parts(1, -2, 333)),
        );
        let parsed =
            MmBackwardQueryMsg::from_bytes(MmBackwardQueryMsg::CancelCandidates(msg).to_bytes())
                .expect("roundtrip");
        match parsed {
            MmBackwardQueryMsg::CancelCandidates(inner) => {
                assert_eq!(inner.trigger_ts, 456);
                assert_eq!(inner.groups.len(), 1);
                assert_eq!(inner.groups[0].get_symbol(), "ETHUSDT");
                assert_eq!(inner.groups[0].items.len(), 1);
                assert_eq!(inner.groups[0].items[0].strategy_id, 11);
                assert_eq!(inner.groups[0].items[0].price_qv.get_count(), 333);
            }
            _ => panic!("expected cancel candidates"),
        }
    }
}
