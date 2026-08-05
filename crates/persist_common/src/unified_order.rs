pub const SIGNAL_BBO_LEG_BINARY_LEN: usize = 1 + 8 + 8 * 4;
pub const SIGNAL_BBO_BINARY_LEN: usize = 1 + SIGNAL_BBO_LEG_BINARY_LEN * 2;

const SIGNAL_BBO_OPEN_PRESENT: u8 = 1 << 0;
const SIGNAL_BBO_HEDGE_PRESENT: u8 = 1 << 1;
const SIGNAL_BBO_VALID_MASK: u8 = SIGNAL_BBO_OPEN_PRESENT | SIGNAL_BBO_HEDGE_PRESENT;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SignalBboLeg {
    pub venue: u8,
    pub ts: i64,
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
}

impl SignalBboLeg {
    pub fn new(
        venue: u8,
        ts: i64,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    ) -> Self {
        Self {
            venue,
            ts,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        }
    }

    pub fn checked(
        venue: u8,
        ts: i64,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    ) -> Option<Self> {
        (bid_price.is_finite()
            && bid_price > 0.0
            && bid_qty.is_finite()
            && bid_qty > 0.0
            && ask_price.is_finite()
            && ask_price > 0.0
            && ask_qty.is_finite()
            && ask_qty > 0.0)
            .then(|| Self::new(venue, ts, bid_price, bid_qty, ask_price, ask_qty))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SignalBbo {
    pub open: Option<SignalBboLeg>,
    pub hedge: Option<SignalBboLeg>,
}

impl SignalBbo {
    pub fn new(open: Option<SignalBboLeg>, hedge: Option<SignalBboLeg>) -> Option<Self> {
        (open.is_some() || hedge.is_some()).then_some(Self { open, hedge })
    }

    pub fn encode_optional(value: Option<Self>) -> [u8; SIGNAL_BBO_BINARY_LEN] {
        let value = value.unwrap_or_default();
        let mut out = [0u8; SIGNAL_BBO_BINARY_LEN];
        let mut mask = 0u8;
        if value.open.is_some() {
            mask |= SIGNAL_BBO_OPEN_PRESENT;
        }
        if value.hedge.is_some() {
            mask |= SIGNAL_BBO_HEDGE_PRESENT;
        }
        out[0] = mask;
        encode_leg(&mut out[1..1 + SIGNAL_BBO_LEG_BINARY_LEN], value.open);
        encode_leg(&mut out[1 + SIGNAL_BBO_LEG_BINARY_LEN..], value.hedge);
        out
    }

    pub fn decode_optional(raw: &[u8]) -> Result<Option<Self>, String> {
        if raw.len() != SIGNAL_BBO_BINARY_LEN {
            return Err(format!(
                "invalid signal_bbo length: expected {}, got {}",
                SIGNAL_BBO_BINARY_LEN,
                raw.len()
            ));
        }
        let mask = raw[0];
        if mask & !SIGNAL_BBO_VALID_MASK != 0 {
            return Err(format!("invalid signal_bbo presence mask: {mask}"));
        }
        let open = decode_leg(
            &raw[1..1 + SIGNAL_BBO_LEG_BINARY_LEN],
            mask & SIGNAL_BBO_OPEN_PRESENT != 0,
        );
        let hedge = decode_leg(
            &raw[1 + SIGNAL_BBO_LEG_BINARY_LEN..],
            mask & SIGNAL_BBO_HEDGE_PRESENT != 0,
        );
        Ok(Self::new(open, hedge))
    }
}

fn encode_leg(out: &mut [u8], leg: Option<SignalBboLeg>) {
    let Some(leg) = leg else {
        return;
    };
    out[0] = leg.venue;
    out[1..9].copy_from_slice(&leg.ts.to_le_bytes());
    out[9..17].copy_from_slice(&leg.bid_price.to_le_bytes());
    out[17..25].copy_from_slice(&leg.bid_qty.to_le_bytes());
    out[25..33].copy_from_slice(&leg.ask_price.to_le_bytes());
    out[33..41].copy_from_slice(&leg.ask_qty.to_le_bytes());
}

fn decode_leg(raw: &[u8], present: bool) -> Option<SignalBboLeg> {
    if !present {
        return None;
    }
    let read_i64 = |start: usize| {
        i64::from_le_bytes(
            raw[start..start + 8]
                .try_into()
                .expect("fixed signal_bbo leg"),
        )
    };
    let read_f64 = |start: usize| {
        f64::from_le_bytes(
            raw[start..start + 8]
                .try_into()
                .expect("fixed signal_bbo leg"),
        )
    };
    Some(SignalBboLeg::new(
        raw[0],
        read_i64(1),
        read_f64(9),
        read_f64(17),
        read_f64(25),
        read_f64(33),
    ))
}

/// 统一订单格式（仅结构定义，不含持久化接线逻辑）。
///
/// 设计约束：
/// - 不使用 `String`，所有可变文本统一使用二进制字节表示。
/// - 枚举类字段统一使用 `u8` 紧凑编码，编码值与现有模块保持一致：
///   - `venue` 对齐 `TradingVenue::to_u8()`
///   - `ttype` 对齐 `OrderType` 的二进制编码
///   - `side` 对齐 `Side::to_u8()`
///   - `status` 对齐 `OrderStatus::to_u8()`
/// - `from_key` 采用 `u32 + bytes` 形式，放在结构尾部，满足不定长扩展需求。
#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedOrderRecord {
    /// 交易标的字节长度（`symbol` 的长度，单位：byte）。
    pub symbol_len: u16,
    /// 交易标的字节内容（例如 `BTCUSDT` 的 UTF-8 bytes）。
    pub symbol: Vec<u8>,

    /// 订单创建时间戳（订单首次生成时间）。
    pub create_ts: i64,
    /// 订单最近一次状态变更时间戳。
    pub update_ts: i64,
    /// 触发该订单的信号时间戳。
    pub signal_ts: i64,
    /// 最近一次给 trade engine / query engine 发送请求的本地时间戳（µs）。
    pub submit_ts: i64,
    /// OrderUpdate / TradeUpdate / 查询回报最近一次被本地实质性接受的时间戳（µs）。
    pub local_ts: i64,
    /// 触发订单决策的盘口事件时间（µs）；双腿信号取两腿最大值，缺失时为 0。
    pub mkt_ts: i64,

    /// 客户端自定义订单 ID（幂等与追踪）。
    pub client_order_id: i64,

    /// 交易所/交易场所（`u8`，对齐 `TradingVenue` 编码）。
    pub venue: u8,
    /// 订单类型（`u8`，对齐 `OrderType` 编码）。
    pub ttype: u8,
    /// 买卖方向（`u8`，对齐 `Side` 编码）。
    pub side: u8,

    /// 下单价格。
    pub price: f64,
    /// 相对参考价的偏移量。
    pub price_offset: f64,
    /// 初始下单数量。
    pub amount_init: f64,
    /// 本次更新对应的数量（如增减仓/成交增量）。
    pub amount_update: f64,

    /// 订单状态（`u8`，对齐 `OrderStatus` 编码）。
    pub status: u8,

    /// `from_key` 字节长度（单位：byte）。
    pub from_key_len: u32,
    /// 来源规则标识（不限制长度，原始二进制 bytes）。
    pub from_key: Vec<u8>,

    /// Decision-time BBO. Current producers populate it; historical records may
    /// omit the fixed binary tail and decode to None.
    pub signal_bbo: Option<SignalBbo>,
}

impl UnifiedOrderRecord {
    /// 根据 `symbol` 与 `from_key` 自动回填长度字段。
    pub fn refresh_lengths(&mut self) {
        self.symbol_len = self.symbol.len() as u16;
        self.from_key_len = self.from_key.len() as u32;
    }

    /// 检查长度字段是否与实际 bytes 一致。
    pub fn length_fields_consistent(&self) -> bool {
        self.symbol_len as usize == self.symbol.len()
            && self.from_key_len as usize == self.from_key.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_bbo_fixed_binary_roundtrip() {
        let open = SignalBboLeg::new(1, 10, 100.0, 2.0, 100.1, 3.0);
        let hedge = SignalBboLeg::new(2, 11, 99.9, 4.0, 100.0, 5.0);
        let value = SignalBbo::new(Some(open), Some(hedge));
        let encoded = SignalBbo::encode_optional(value);
        assert_eq!(encoded.len(), SIGNAL_BBO_BINARY_LEN);
        assert_eq!(SignalBbo::decode_optional(&encoded).unwrap(), value);
    }

    #[test]
    fn signal_bbo_empty_binary_decodes_to_none() {
        let encoded = SignalBbo::encode_optional(None);
        assert_eq!(SignalBbo::decode_optional(&encoded).unwrap(), None);
    }
}
