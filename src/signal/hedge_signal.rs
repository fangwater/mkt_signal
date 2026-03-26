use crate::common::tick_math::QuantizedValue;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::{bytes_helper, SignalBytes, TradingLeg};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Unified arbitrage hedge signal context (supports both maker and taker strategies)
/// When exp_time > 0, it's treated as a maker order (limit order)
/// When exp_time == 0, it's treated as a taker order (market order)
#[derive(Debug, Clone)]
pub struct ArbHedgeCtx {
    /// Strategy ID that needs hedging
    pub strategy_id: i32,

    /// Client order ID for tracking
    pub client_order_id: i64,

    /// Hedge quantity tick/count encoding (qty = tick * count)
    pub hedge_qty_qv: QuantizedValue,

    /// Hedge side (Buy/Sell) - stored as u8
    pub hedge_side: u8,

    /// Hedge price tick/count encoding (price = tick * count)
    pub hedge_price_qv: QuantizedValue,

    /// Whether to use post-only order (ignored when exp_time == 0)
    pub maker_only: bool,

    /// Order expiration time (microseconds)
    /// When > 0: Maker order with limit price
    /// When == 0: Taker order (market order)
    pub exp_time: i64,

    /// Opening leg market data (the leg that triggered this hedge)
    /// For as-market hedge: all values are 0
    /// For query-derived hedge: contains actual market data
    pub opening_leg: TradingLeg,

    /// Opening leg symbol
    pub opening_symbol: [u8; 32],

    /// Hedging leg market data
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Market data timestamp
    pub market_ts: i64,

    /// Price offset from best bid/ask for limit order placement
    /// 0.0 for MT taker orders (market orders)
    /// Actual offset value for query-derived maker orders
    pub price_offset: f64,

    /// Spread rate between opening and hedging legs
    pub spread_rate: f64,

    /// From key length
    pub from_key_len: u32,

    /// From key bytes
    pub from_key: Vec<u8>,
}

/// Market maker hedge signal context (query response)
#[derive(Debug, Clone)]
pub struct MmHedgeCtx {
    /// Market leg snapshot
    pub opening_leg: TradingLeg,

    /// Leg symbol
    pub opening_symbol: [u8; 32],

    /// Price levels encoded by QuantizedValue (same encoding style as MmOpenCtx::price_qv)
    pub price_qv_list: Vec<QuantizedValue>,

    /// Amount levels encoded by QuantizedValue (same encoding style as MmOpenCtx::amount_qv)
    pub amount_qv_list: Vec<QuantizedValue>,

    /// Explicit price offsets aligned with `price_qv_list`
    pub price_offsets: Vec<f64>,

    /// Queried tlen values aligned with `price_qv_list`
    pub tlen_values: Vec<f64>,

    /// Signal timestamp (microseconds)
    pub signal_ts: i64,

    /// Next query timestamp (microseconds)
    pub next_query_ts: i64,

    /// From key length
    pub from_key_len: u32,

    /// From key bytes
    pub from_key: Vec<u8>,
}

impl ArbHedgeCtx {
    /// Create new hedge context
    pub fn new() -> Self {
        Self {
            strategy_id: 0,
            client_order_id: 0,
            hedge_qty_qv: QuantizedValue::zero(),
            hedge_side: 0,
            hedge_price_qv: QuantizedValue::zero(),
            maker_only: false,
            exp_time: 0,
            opening_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            hedging_symbol: [0u8; 32],
            market_ts: 0,
            price_offset: 0.0,
            spread_rate: 0.0,
            from_key_len: 0,
            from_key: Vec::new(),
        }
    }

    /// Create a maker hedge context (limit order)
    pub fn new_maker(
        strategy_id: i32,
        client_order_id: i64,
        hedge_side: u8,
        hedge_qty: f64,
        qty_tick: f64,
        limit_price: f64,
        price_tick: f64,
        maker_only: bool,
        exp_time: i64,
    ) -> Self {
        let mut ctx = Self::new();
        ctx.strategy_id = strategy_id;
        ctx.client_order_id = client_order_id;
        ctx.hedge_side = hedge_side;
        ctx.set_hedge_qty_with_tick_floor(hedge_qty, qty_tick);
        ctx.set_hedge_price_with_tick_floor(limit_price, price_tick);
        ctx.maker_only = maker_only;
        ctx.exp_time = exp_time;
        ctx
    }

    /// Create a taker hedge context (market order)
    pub fn new_taker(
        strategy_id: i32,
        client_order_id: i64,
        hedge_side: u8,
        hedge_qty: f64,
        qty_tick: f64,
    ) -> Self {
        let mut ctx = Self::new();
        ctx.strategy_id = strategy_id;
        ctx.client_order_id = client_order_id;
        ctx.hedge_side = hedge_side;
        ctx.set_hedge_qty_with_tick_floor(hedge_qty, qty_tick);
        ctx.exp_time = 0; // 0 indicates taker/market order
        ctx
    }

    /// Check if this is a maker order
    pub fn is_maker(&self) -> bool {
        self.exp_time > 0
    }

    /// Check if this is a taker order
    pub fn is_taker(&self) -> bool {
        self.exp_time == 0
    }

    /// Set opening symbol
    pub fn set_opening_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.opening_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get opening symbol
    pub fn get_opening_symbol(&self) -> String {
        let end = self
            .opening_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.opening_symbol[..end]).to_string()
    }

    /// Set hedging symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.hedging_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get hedging symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self
            .hedging_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.hedge_side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.hedge_side = side.to_u8();
    }

    /// Set from key bytes (updates length)
    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }

    pub fn set_hedge_price_with_tick_floor(&mut self, price: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.hedge_price_qv = QuantizedValue::encode_floor(price, preferred_tick)
            .unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn set_hedge_qty_with_tick_floor(&mut self, qty: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.hedge_qty_qv =
            QuantizedValue::encode_floor(qty, preferred_tick).unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn hedge_price_value(&self) -> f64 {
        self.hedge_price_qv.get_val()
    }

    pub fn hedge_qty_value(&self) -> f64 {
        self.hedge_qty_qv.get_val()
    }

    pub fn hedge_price_count(&self) -> i64 {
        self.hedge_price_qv.get_count()
    }

    pub fn hedge_qty_count(&self) -> i64 {
        self.hedge_qty_qv.get_count()
    }

    /// Get appropriate hedge price based on the hedge side
    /// 根据对冲方向获取合适的价格（选择不会立即成交的盘口最优价格）
    ///
    /// # 价格选择逻辑:
    /// - 如果是 Maker 订单且已设置限价：使用指定的 limit_price
    /// - 否则使用盘口最优价格（不会立即成交）：
    ///   - Buy 方向（做多）：使用 bid0（挂在买盘最优价，等待成交）
    ///   - Sell 方向（做空）：使用 ask0（挂在卖盘最优价，等待成交）
    ///
    /// # 原理说明:
    /// - 当我们要买入（做多）时，挂在 bid0 不会立即成交，而是等待卖方主动成交
    /// - 当我们要卖出（做空）时，挂在 ask0 不会立即成交，而是等待买方主动成交
    /// - 这样可以获得更好的成交价格（赚取买卖价差）
    ///
    /// # Returns
    /// 返回适合对冲方向的盘口最优价格
    pub fn get_hedge_price(&self) -> f64 {
        // 如果是 Maker 订单且设置了限价，使用限价
        let hedge_price = self.hedge_price_value();
        if self.is_maker() && hedge_price > 0.0 {
            return hedge_price;
        }

        // 使用盘口最优价格（不会立即成交的价格）
        match self.get_side() {
            Some(Side::Buy) => {
                // 买入（做多）时使用 bid0，挂在买盘最优价
                // 这样不会立即成交，而是等待卖方来成交我们的订单
                self.hedging_leg.bid0
            }
            Some(Side::Sell) => {
                // 卖出（做空）时使用 ask0，挂在卖盘最优价
                // 这样不会立即成交，而是等待买方来成交我们的订单
                self.hedging_leg.ask0
            }
            None => {
                // 如果无法确定方向，默认返回 bid0
                // 这种情况通常不应该发生
                self.hedging_leg.bid0
            }
        }
    }
}

impl MmHedgeCtx {
    /// Create new market maker hedge context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            price_qv_list: Vec::new(),
            amount_qv_list: Vec::new(),
            price_offsets: Vec::new(),
            tlen_values: Vec::new(),
            signal_ts: 0,
            next_query_ts: 0,
            from_key_len: 0,
            from_key: Vec::new(),
        }
    }

    /// Set opening leg symbol
    pub fn set_opening_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.opening_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get opening leg symbol
    pub fn get_opening_symbol(&self) -> String {
        let end = self
            .opening_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.opening_symbol[..end]).to_string()
    }

    /// Set from key bytes (updates length)
    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }
}

impl SignalBytes for ArbHedgeCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32_le(self.strategy_id);
        buf.put_i64_le(self.client_order_id);
        buf.put_u8(self.hedge_side);
        let (hedge_price_tick_i64, hedge_price_tick_exp) = self.hedge_price_qv.get_tick_parts();
        let (hedge_qty_tick_i64, hedge_qty_tick_exp) = self.hedge_qty_qv.get_tick_parts();
        buf.put_i64_le(hedge_price_tick_i64);
        buf.put_i32_le(hedge_price_tick_exp);
        buf.put_i64_le(self.hedge_price_qv.get_count());
        buf.put_i64_le(hedge_qty_tick_i64);
        buf.put_i32_le(hedge_qty_tick_exp);
        buf.put_i64_le(self.hedge_qty_qv.get_count());
        buf.put_u8(if self.maker_only { 1 } else { 0 });
        buf.put_i64_le(self.exp_time);

        // Opening leg market data (0 for as-market hedge)
        buf.put_u8(self.opening_leg.venue);
        buf.put_f64_le(self.opening_leg.bid0);
        buf.put_f64_le(self.opening_leg.ask0);
        buf.put_i64_le(self.opening_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg market data
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        buf.put_i64_le(self.hedging_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        buf.put_i64_le(self.market_ts);
        buf.put_f64_le(self.price_offset);
        buf.put_f64_le(self.spread_rate);

        let from_key_len = self.from_key.len() as u32;
        buf.put_u32_le(from_key_len);
        buf.put_slice(&self.from_key);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        // 基本字段长度: 4 + 8 + 1 + 8 + 4 + 8 + 8 + 4 + 8 + 1 + 8 = 62
        const BASIC_FIELDS_LEN: usize = 4 + 8 + 1 + 8 + 4 + 8 + 8 + 4 + 8 + 1 + 8;
        if bytes.remaining() < BASIC_FIELDS_LEN {
            return Err("Not enough bytes for ArbHedgeCtx basic fields".to_string());
        }

        let strategy_id = bytes.get_i32_le();
        let client_order_id = bytes.get_i64_le();
        let hedge_side = bytes.get_u8();
        let hedge_price_tick_i64 = bytes.get_i64_le();
        let hedge_price_tick_exp = bytes.get_i32_le();
        let hedge_price_count = bytes.get_i64_le();
        let hedge_qty_tick_i64 = bytes.get_i64_le();
        let hedge_qty_tick_exp = bytes.get_i32_le();
        let hedge_qty_count = bytes.get_i64_le();
        let maker_only = bytes.get_u8() != 0;
        let exp_time = bytes.get_i64_le();

        // Opening leg market data
        if bytes.remaining() < 1 + 8 + 8 + 8 {
            return Err("Not enough bytes for opening leg".to_string());
        }
        let opening_venue = bytes.get_u8();
        let opening_bid0 = bytes.get_f64_le();
        let opening_ask0 = bytes.get_f64_le();
        let opening_ts = bytes.get_i64_le();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Hedging leg market data
        if bytes.remaining() < 1 + 8 + 8 + 8 {
            return Err("Not enough bytes for hedging leg".to_string());
        }
        let hedging_venue = bytes.get_u8();
        let hedging_bid0 = bytes.get_f64_le();
        let hedging_ask0 = bytes.get_f64_le();
        let hedging_ts = bytes.get_i64_le();
        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        if bytes.remaining() < 8 + 8 + 8 + 4 {
            return Err("Not enough bytes for ArbHedgeCtx tail".to_string());
        }
        let market_ts = bytes.get_i64_le();
        let price_offset = bytes.get_f64_le();
        let spread_rate = bytes.get_f64_le();
        let from_key_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < from_key_len {
            return Err(format!(
                "Not enough bytes for from_key: need {}, have {}",
                from_key_len,
                bytes.remaining()
            ));
        }
        let from_key = bytes.copy_to_bytes(from_key_len).to_vec();

        if bytes.remaining() != 0 {
            return Err("Unexpected trailing bytes for ArbHedgeCtx".to_string());
        }

        Ok(ArbHedgeCtx {
            strategy_id,
            client_order_id,
            hedge_qty_qv: QuantizedValue::from_parts(
                hedge_qty_tick_i64,
                hedge_qty_tick_exp,
                hedge_qty_count,
            ),
            hedge_side,
            hedge_price_qv: QuantizedValue::from_parts(
                hedge_price_tick_i64,
                hedge_price_tick_exp,
                hedge_price_count,
            ),
            maker_only,
            exp_time,
            opening_leg: TradingLeg {
                venue: opening_venue,
                bid0: opening_bid0,
                ask0: opening_ask0,
                ts: opening_ts,
            },
            opening_symbol,
            hedging_leg: TradingLeg {
                venue: hedging_venue,
                bid0: hedging_bid0,
                ask0: hedging_ask0,
                ts: hedging_ts,
            },
            hedging_symbol,
            market_ts,
            price_offset,
            spread_rate,
            from_key_len: from_key.len() as u32,
            from_key,
        })
    }
}

impl SignalBytes for MmHedgeCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        buf.put_u8(self.opening_leg.venue);
        buf.put_f64_le(self.opening_leg.bid0);
        buf.put_f64_le(self.opening_leg.ask0);
        buf.put_i64_le(self.opening_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Price qv list
        buf.put_u32_le(self.price_qv_list.len() as u32);
        for price_qv in &self.price_qv_list {
            let (tick_i64, tick_exp) = price_qv.get_tick_parts();
            buf.put_i64_le(tick_i64);
            buf.put_i32_le(tick_exp);
            buf.put_i64_le(price_qv.get_count());
        }

        // Amount qv list
        buf.put_u32_le(self.amount_qv_list.len() as u32);
        for amount_qv in &self.amount_qv_list {
            let (tick_i64, tick_exp) = amount_qv.get_tick_parts();
            buf.put_i64_le(tick_i64);
            buf.put_i32_le(tick_exp);
            buf.put_i64_le(amount_qv.get_count());
        }

        // offset list
        buf.put_u32_le(self.price_offsets.len() as u32);
        for offset in &self.price_offsets {
            buf.put_f64_le(*offset);
        }

        // tlen list
        buf.put_u32_le(self.tlen_values.len() as u32);
        for tlen in &self.tlen_values {
            buf.put_f64_le(*tlen);
        }

        // Signal timestamp
        buf.put_i64_le(self.signal_ts);

        // Next query timestamp
        buf.put_i64_le(self.next_query_ts);

        // from_key
        let from_key_len = self.from_key.len() as u32;
        buf.put_u32_le(from_key_len);
        buf.put_slice(&self.from_key);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        const QV_BYTES_LEN: usize = 8 + 4 + 8;

        // Opening leg
        if bytes.remaining() < 1 + 8 + 8 + 8 {
            return Err("Not enough bytes for opening leg".to_string());
        }
        let opening_venue = bytes.get_u8();
        let opening_bid0 = bytes.get_f64_le();
        let opening_ask0 = bytes.get_f64_le();
        let opening_ts = bytes.get_i64_le();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for price_qv_list length".to_string());
        }
        let price_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < price_len.saturating_mul(QV_BYTES_LEN) {
            return Err(format!(
                "Not enough bytes for price_qv_list: need {}, have {}",
                price_len.saturating_mul(QV_BYTES_LEN),
                bytes.remaining()
            ));
        }
        let mut price_qv_list = Vec::with_capacity(price_len);
        for _ in 0..price_len {
            let tick_i64 = bytes.get_i64_le();
            let tick_exp = bytes.get_i32_le();
            let count = bytes.get_i64_le();
            price_qv_list.push(QuantizedValue::from_parts(tick_i64, tick_exp, count));
        }

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for amount_qv_list length".to_string());
        }
        let amount_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < amount_len.saturating_mul(QV_BYTES_LEN) {
            return Err(format!(
                "Not enough bytes for amount_qv_list: need {}, have {}",
                amount_len.saturating_mul(QV_BYTES_LEN),
                bytes.remaining()
            ));
        }
        let mut amount_qv_list = Vec::with_capacity(amount_len);
        for _ in 0..amount_len {
            let tick_i64 = bytes.get_i64_le();
            let tick_exp = bytes.get_i32_le();
            let count = bytes.get_i64_le();
            amount_qv_list.push(QuantizedValue::from_parts(tick_i64, tick_exp, count));
        }

        if price_qv_list.len() != amount_qv_list.len() {
            return Err(format!(
                "price_qv_list/amount_qv_list length mismatch: price_len={} amount_len={}",
                price_qv_list.len(),
                amount_qv_list.len()
            ));
        }

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for price_offsets length".to_string());
        }
        let price_offset_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < price_offset_len.saturating_mul(8) {
            return Err(format!(
                "Not enough bytes for price_offsets: need {}, have {}",
                price_offset_len.saturating_mul(8),
                bytes.remaining()
            ));
        }
        let mut price_offsets = Vec::with_capacity(price_offset_len);
        for _ in 0..price_offset_len {
            price_offsets.push(bytes.get_f64_le());
        }

        if price_qv_list.len() != price_offsets.len() {
            return Err(format!(
                "price_qv_list/price_offsets length mismatch: price_len={} offset_len={}",
                price_qv_list.len(),
                price_offsets.len()
            ));
        }

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for tlen_values length".to_string());
        }
        let tlen_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < tlen_len.saturating_mul(8) {
            return Err(format!(
                "Not enough bytes for tlen_values: need {}, have {}",
                tlen_len.saturating_mul(8),
                bytes.remaining()
            ));
        }
        let mut tlen_values = Vec::with_capacity(tlen_len);
        for _ in 0..tlen_len {
            tlen_values.push(bytes.get_f64_le());
        }

        if !tlen_values.is_empty() && tlen_values.len() != price_qv_list.len() {
            return Err(format!(
                "price_qv_list/tlen_values length mismatch: price_len={} tlen_len={}",
                price_qv_list.len(),
                tlen_values.len()
            ));
        }

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for signal_ts".to_string());
        }
        let signal_ts = bytes.get_i64_le();

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for next_query_ts".to_string());
        }
        let next_query_ts = bytes.get_i64_le();

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for from_key length".to_string());
        }
        let from_key_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < from_key_len {
            return Err(format!(
                "Not enough bytes for from_key: need {}, have {}",
                from_key_len,
                bytes.remaining()
            ));
        }
        let from_key = bytes.copy_to_bytes(from_key_len).to_vec();

        if bytes.remaining() != 0 {
            return Err("Unexpected trailing bytes for MmHedgeCtx".to_string());
        }

        Ok(MmHedgeCtx {
            opening_leg: TradingLeg {
                venue: opening_venue,
                bid0: opening_bid0,
                ask0: opening_ask0,
                ts: opening_ts,
            },
            opening_symbol,
            price_qv_list,
            amount_qv_list,
            price_offsets,
            tlen_values,
            signal_ts,
            next_query_ts,
            from_key_len: from_key_len as u32,
            from_key,
        })
    }
}

/// MM 对冲查询消息（携带 symbol + 当期买/卖成交 + 累计净头寸）
#[derive(Debug, Clone)]
pub struct MmHedgeSignalQueryMsg {
    /// 交易对
    pub symbol: [u8; 32],
    /// 当期买成交量（base qty）
    pub period_buy_qty: f64,
    /// 当期卖成交量（base qty）
    pub period_sell_qty: f64,
    /// 累计净头寸（base qty，正=多，负=空）
    pub net_qty: f64,
    /// 单币敞口预算（USDT），来源 pre_trade_risk_params: max_pos_u * max_symbol_exposure_ratio
    pub symbol_exposure_u: f64,
}

impl MmHedgeSignalQueryMsg {
    pub fn new(
        symbol: &str,
        period_buy_qty: f64,
        period_sell_qty: f64,
        net_qty: f64,
        symbol_exposure_u: f64,
    ) -> Self {
        let mut symbol_bytes = [0u8; 32];
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        symbol_bytes[..len].copy_from_slice(&bytes[..len]);
        Self {
            symbol: symbol_bytes,
            period_buy_qty,
            period_sell_qty,
            net_qty,
            symbol_exposure_u,
        }
    }

    pub fn get_symbol(&self) -> String {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.symbol[..end]).to_string()
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        bytes_helper::write_fixed_bytes(&mut buf, &self.symbol);
        buf.put_f64_le(self.period_buy_qty);
        buf.put_f64_le(self.period_sell_qty);
        buf.put_f64_le(self.net_qty);
        buf.put_f64_le(self.symbol_exposure_u);
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 32 {
            return Err("insufficient bytes for buy/sell/net qty/symbol_exposure_u".to_string());
        }
        let period_buy_qty = bytes.get_f64_le();
        let period_sell_qty = bytes.get_f64_le();
        let net_qty = bytes.get_f64_le();
        let symbol_exposure_u = bytes.get_f64_le();
        if bytes.remaining() != 0 {
            if bytes.iter().any(|&b| b != 0) {
                return Err(
                    "Unexpected non-zero trailing bytes for MmHedgeSignalQueryMsg".to_string(),
                );
            }
        }
        Ok(Self {
            symbol,
            period_buy_qty,
            period_sell_qty,
            net_qty,
            symbol_exposure_u,
        })
    }
}

/// Query message for requesting hedge order pricing from upstream model
/// Used for limit order strategies where upstream decides the limit price
#[derive(Debug, Clone)]
pub struct ArbHedgeSignalQueryMsg {
    /// Strategy ID that needs hedging
    pub strategy_id: i32,

    /// Client order ID for tracking
    pub client_order_id: i64,

    /// Query timestamp (microseconds)
    pub query_time: i64,

    /// Hedge quantity in base units
    pub hedge_base_qty: f64,

    /// Hedge side (Buy/Sell) - stored as u8
    pub hedge_side: u8,

    /// Opening leg venue (e.g., BinanceMargin)
    pub opening_venue: u8,

    /// Opening leg symbol
    pub opening_symbol: [u8; 32],

    /// Hedging venue (futures leg)
    pub hedging_venue: u8,

    /// Hedging symbol (e.g., BTCUSDT)
    pub hedging_symbol: [u8; 32],

    /// Number of hedge requests issued for this strategy (monotonic)
    pub request_seq: u32,

    /// Open signal timestamp (microseconds)
    pub open_signal_ts: i64,

    /// Opening leg bid/ask at open signal time
    pub open_bid0: f64,
    pub open_ask0: f64,
    pub open_leg_ts: i64,

    /// Hedging leg bid/ask at open signal time
    pub hedge_bid0: f64,
    pub hedge_ask0: f64,
    pub hedge_leg_ts: i64,
}

impl ArbHedgeSignalQueryMsg {
    /// Create new query message
    pub fn new(
        strategy_id: i32,
        client_order_id: i64,
        query_time: i64,
        hedge_base_qty: f64,
        hedge_side: u8,
        opening_venue: u8,
        opening_symbol: &str,
        hedging_venue: u8,
        hedging_symbol: &str,
        request_seq: u32,
        open_signal_ts: i64,
        open_bid0: f64,
        open_ask0: f64,
        open_leg_ts: i64,
        hedge_bid0: f64,
        hedge_ask0: f64,
        hedge_leg_ts: i64,
    ) -> Self {
        let mut opening_symbol_bytes = [0u8; 32];
        let bytes = opening_symbol.as_bytes();
        let len = bytes.len().min(32);
        opening_symbol_bytes[..len].copy_from_slice(&bytes[..len]);

        let mut hedging_symbol_bytes = [0u8; 32];
        let bytes = hedging_symbol.as_bytes();
        let len = bytes.len().min(32);
        hedging_symbol_bytes[..len].copy_from_slice(&bytes[..len]);

        Self {
            strategy_id,
            client_order_id,
            query_time,
            hedge_base_qty,
            hedge_side,
            opening_venue,
            opening_symbol: opening_symbol_bytes,
            hedging_venue,
            hedging_symbol: hedging_symbol_bytes,
            request_seq,
            open_signal_ts,
            open_bid0,
            open_ask0,
            open_leg_ts,
            hedge_bid0,
            hedge_ask0,
            hedge_leg_ts,
        }
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.hedge_side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.hedge_side = side.to_u8();
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32_le(self.strategy_id);
        buf.put_i64_le(self.client_order_id);
        buf.put_i64_le(self.query_time);
        buf.put_f64_le(self.hedge_base_qty);
        buf.put_u8(self.hedge_side);

        // Opening leg info
        buf.put_u8(self.opening_venue);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg info
        buf.put_u8(self.hedging_venue);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);
        buf.put_u32_le(self.request_seq);

        // Open signal snapshot (optional for backward compatibility)
        buf.put_i64_le(self.open_signal_ts);
        buf.put_f64_le(self.open_bid0);
        buf.put_f64_le(self.open_ask0);
        buf.put_i64_le(self.open_leg_ts);
        buf.put_f64_le(self.hedge_bid0);
        buf.put_f64_le(self.hedge_ask0);
        buf.put_i64_le(self.hedge_leg_ts);
        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 {
            return Err("insufficient bytes for strategy_id".into());
        }
        let strategy_id = bytes.get_i32_le();

        if bytes.remaining() < 8 {
            return Err("insufficient bytes for client_order_id".into());
        }
        let client_order_id = bytes.get_i64_le();

        if bytes.remaining() < 8 {
            return Err("insufficient bytes for query_time".into());
        }
        let query_time = bytes.get_i64_le();

        if bytes.remaining() < 8 {
            return Err("insufficient bytes for hedge_base_qty".into());
        }
        let hedge_base_qty = bytes.get_f64_le();

        if bytes.remaining() < 1 {
            return Err("insufficient bytes for hedge_side".into());
        }
        let hedge_side = bytes.get_u8();

        // Opening leg info
        if bytes.remaining() < 1 {
            return Err("insufficient bytes for opening venue".into());
        }
        let opening_venue = bytes.get_u8();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Hedging leg info
        if bytes.remaining() < 1 {
            return Err("insufficient bytes for hedging venue".into());
        }
        let hedging_venue = bytes.get_u8();

        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 4 {
            return Err("insufficient bytes for request_seq".into());
        }
        let request_seq = bytes.get_u32_le();

        if bytes.remaining() < 56 {
            return Err("insufficient bytes for open signal snapshot".into());
        }
        let open_signal_ts = bytes.get_i64_le();
        let open_bid0 = bytes.get_f64_le();
        let open_ask0 = bytes.get_f64_le();
        let open_leg_ts = bytes.get_i64_le();
        let hedge_bid0 = bytes.get_f64_le();
        let hedge_ask0 = bytes.get_f64_le();
        let hedge_leg_ts = bytes.get_i64_le();

        Ok(Self {
            strategy_id,
            client_order_id,
            query_time,
            hedge_base_qty,
            hedge_side,
            opening_venue,
            opening_symbol,
            hedging_venue,
            hedging_symbol,
            request_seq,
            open_signal_ts,
            open_bid0,
            open_ask0,
            open_leg_ts,
            hedge_bid0,
            hedge_ask0,
            hedge_leg_ts,
        })
    }

    /// 获取开仓symbol
    pub fn get_opening_symbol(&self) -> String {
        let end = self
            .opening_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.opening_symbol[..end]).to_string()
    }

    /// 获取对冲symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self
            .hedging_symbol
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{MmHedgeCtx, MmHedgeSignalQueryMsg};
    use crate::common::tick_math::QuantizedValue;
    use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};

    #[test]
    fn mm_hedge_query_from_bytes_accepts_zero_padding() {
        let msg = MmHedgeSignalQueryMsg::new("SOLUSDT", 1.0, 0.5, 0.5, 1000.0);
        let mut raw = msg.to_bytes().to_vec();
        raw.extend_from_slice(&[0u8; 16]);

        let parsed = MmHedgeSignalQueryMsg::from_bytes(bytes::Bytes::from(raw));
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.get_symbol(), "SOLUSDT");
        assert!((parsed.period_buy_qty - 1.0).abs() < 1e-12);
    }

    #[test]
    fn mm_hedge_query_from_bytes_rejects_non_zero_trailing_bytes() {
        let msg = MmHedgeSignalQueryMsg::new("SOLUSDT", 1.0, 0.5, 0.5, 1000.0);
        let mut raw = msg.to_bytes().to_vec();
        raw.extend_from_slice(&[0u8, 0u8, 1u8]);

        let parsed = MmHedgeSignalQueryMsg::from_bytes(bytes::Bytes::from(raw));
        assert!(parsed.is_err());
        assert!(parsed
            .err()
            .unwrap()
            .contains("Unexpected non-zero trailing bytes"));
    }

    #[test]
    fn mm_hedge_ctx_roundtrip_with_tlen_values() {
        let mut ctx = MmHedgeCtx::new();
        ctx.opening_leg = TradingLeg::new(TradingVenue::BinanceFutures, 100.0, 100.1, 123456);
        ctx.set_opening_symbol("BTCUSDT");
        ctx.price_qv_list
            .push(QuantizedValue::from_parts(1, -1, 724310));
        ctx.amount_qv_list
            .push(QuantizedValue::from_parts(1, -3, 5000));
        ctx.price_offsets.push(0.0015);
        ctx.tlen_values.push(0.1234);
        ctx.signal_ts = 111;
        ctx.next_query_ts = 222;
        ctx.set_from_key(b"fk".to_vec());

        let parsed = MmHedgeCtx::from_bytes(ctx.to_bytes()).unwrap();
        assert_eq!(parsed.get_opening_symbol(), "BTCUSDT");
        assert_eq!(parsed.price_qv_list.len(), 1);
        assert_eq!(parsed.amount_qv_list.len(), 1);
        assert_eq!(parsed.price_offsets, vec![0.0015]);
        assert_eq!(parsed.tlen_values, vec![0.1234]);
        assert_eq!(parsed.signal_ts, 111);
        assert_eq!(parsed.next_query_ts, 222);
        assert_eq!(parsed.from_key, b"fk");
    }
}
