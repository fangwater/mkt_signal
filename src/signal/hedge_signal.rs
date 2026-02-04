use crate::pre_trade::order_manager::Side;
use crate::signal::common::{bytes_helper, SignalBytes, TradingLeg};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Unified arbitrage hedge signal context (supports both maker and taker strategies)
/// When exp_time > 0, it's treated as a maker order (limit order)
/// When exp_time == 0, it's treated as a taker order (market order)
#[derive(Debug, Clone, Copy)]
pub struct ArbHedgeCtx {
    /// Strategy ID that needs hedging
    pub strategy_id: i32,

    /// Client order ID for tracking
    pub client_order_id: i64,

    /// Hedge quantity
    pub hedge_qty: f64,

    /// Hedge side (Buy/Sell) - stored as u8
    pub hedge_side: u8,

    /// Limit price for maker order (ignored when exp_time == 0)
    pub limit_price: f64,

    /// Price tick size (ignored when exp_time == 0)
    pub price_tick: f64,

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
}

/// Market maker hedge signal context (query response)
#[derive(Debug, Clone)]
pub struct MmHedgeCtx {
    /// Market leg snapshot
    pub opening_leg: TradingLeg,

    /// Leg symbol
    pub opening_symbol: [u8; 32],

    /// Price tick as power of 10 (price_tick = 10^price_tick_exp)
    pub price_tick_exp: i32,

    /// Qty tick as power of 10 (qty_tick = 10^qty_tick_exp)
    pub qty_tick_exp: i32,

    /// Price levels in ticks (price = price_list[i] * 10^price_tick_exp)
    pub price_list: Vec<i32>,

    /// Amount per level in qty ticks (qty = amount_list[i] * 10^qty_tick_exp)
    pub amount_list: Vec<i64>,

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
            hedge_qty: 0.0,
            hedge_side: 0,
            limit_price: 0.0,
            price_tick: 0.0,
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
        }
    }

    /// Create a maker hedge context (limit order)
    pub fn new_maker(
        strategy_id: i32,
        client_order_id: i64,
        hedge_qty: f64,
        hedge_side: u8,
        limit_price: f64,
        price_tick: f64,
        maker_only: bool,
        exp_time: i64,
    ) -> Self {
        let mut ctx = Self::new();
        ctx.strategy_id = strategy_id;
        ctx.client_order_id = client_order_id;
        ctx.hedge_qty = hedge_qty;
        ctx.hedge_side = hedge_side;
        ctx.limit_price = limit_price;
        ctx.price_tick = price_tick;
        ctx.maker_only = maker_only;
        ctx.exp_time = exp_time;
        ctx
    }

    /// Create a taker hedge context (market order)
    pub fn new_taker(
        strategy_id: i32,
        client_order_id: i64,
        hedge_qty: f64,
        hedge_side: u8,
    ) -> Self {
        let mut ctx = Self::new();
        ctx.strategy_id = strategy_id;
        ctx.client_order_id = client_order_id;
        ctx.hedge_qty = hedge_qty;
        ctx.hedge_side = hedge_side;
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
        if self.is_maker() && self.limit_price > 0.0 {
            return self.limit_price;
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
            price_tick_exp: 0,
            qty_tick_exp: 0,
            price_list: Vec::new(),
            amount_list: Vec::new(),
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
        buf.put_f64_le(self.hedge_qty);
        buf.put_u8(self.hedge_side);
        buf.put_f64_le(self.limit_price);
        buf.put_f64_le(self.price_tick);
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

        buf.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        // 基本字段长度: 4 + 8 + 8 + 1 + 8 + 8 + 1 + 8 = 46
        const BASIC_FIELDS_LEN: usize = 4 + 8 + 8 + 1 + 8 + 8 + 1 + 8;
        const EXTRA_FIELDS_MAX: usize = 2;

        fn detect_format(bytes: &Bytes, base_offset: usize) -> Option<(bool, usize)> {
            let total = bytes.len();
            let mut with_ts_match = None;
            let mut without_ts_match = None;

            for with_ts in [true, false] {
                let (open_len_idx, hedge_len_base, total_base) = if with_ts {
                    (base_offset + 25, base_offset + 51, base_offset + 52)
                } else {
                    (base_offset + 17, base_offset + 35, base_offset + 36)
                };

                if total <= open_len_idx {
                    continue;
                }
                let open_len = bytes[open_len_idx] as usize;
                if open_len > 32 {
                    continue;
                }
                let hedge_len_idx = hedge_len_base + open_len;
                if total <= hedge_len_idx {
                    continue;
                }
                let hedge_len = bytes[hedge_len_idx] as usize;
                if hedge_len > 32 {
                    continue;
                }

                for extra_fields in 0..=EXTRA_FIELDS_MAX {
                    let tail_len = extra_fields * 8;
                    let expected = total_base + open_len + hedge_len + tail_len;
                    if total == expected {
                        if with_ts {
                            with_ts_match = Some(extra_fields);
                        } else {
                            without_ts_match = Some(extra_fields);
                        }
                        break;
                    }
                }
            }

            match (with_ts_match, without_ts_match) {
                (Some(extra), None) => Some((true, extra)),
                (None, Some(extra)) => Some((false, extra)),
                (Some(extra), Some(_)) => Some((true, extra)),
                (None, None) => None,
            }
        }

        fn parse(
            mut bytes: Bytes,
            with_ts: bool,
            extra_fields: Option<usize>,
        ) -> Result<ArbHedgeCtx, String> {
            if bytes.remaining() < BASIC_FIELDS_LEN {
                return Err("Not enough bytes for ArbHedgeCtx basic fields".to_string());
            }

            let strategy_id = bytes.get_i32_le();
            let client_order_id = bytes.get_i64_le();
            let hedge_qty = bytes.get_f64_le();
            let hedge_side = bytes.get_u8();
            let limit_price = bytes.get_f64_le();
            let price_tick = bytes.get_f64_le();
            let maker_only = bytes.get_u8() != 0;
            let exp_time = bytes.get_i64_le();

            // Opening leg market data
            if bytes.remaining() < 1 + 8 + 8 {
                return Err("Not enough bytes for opening leg".to_string());
            }
            let opening_venue = bytes.get_u8();
            let opening_bid0 = bytes.get_f64_le();
            let opening_ask0 = bytes.get_f64_le();
            let opening_ts = if with_ts {
                if bytes.remaining() < 8 {
                    return Err("Not enough bytes for opening leg ts".to_string());
                }
                bytes.get_i64_le()
            } else {
                0
            };
            let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

            // Hedging leg market data
            if bytes.remaining() < 1 + 8 + 8 {
                return Err("Not enough bytes for hedging leg".to_string());
            }
            let hedging_venue = bytes.get_u8();
            let hedging_bid0 = bytes.get_f64_le();
            let hedging_ask0 = bytes.get_f64_le();
            let hedging_ts = if with_ts {
                if bytes.remaining() < 8 {
                    return Err("Not enough bytes for hedging leg ts".to_string());
                }
                bytes.get_i64_le()
            } else {
                0
            };
            let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

            let remaining = bytes.remaining();
            let extra_fields = match extra_fields {
                Some(fields) => {
                    if remaining != fields * 8 {
                        return Err("Unexpected tail length for ArbHedgeCtx".to_string());
                    }
                    fields
                }
                None => {
                    if remaining % 8 != 0 {
                        return Err("Invalid tail length for ArbHedgeCtx".to_string());
                    }
                    let fields = remaining / 8;
                    if fields > EXTRA_FIELDS_MAX {
                        return Err("Too many tail fields for ArbHedgeCtx".to_string());
                    }
                    fields
                }
            };

            let mut market_ts = 0;
            let mut price_offset = 0.0;
            if extra_fields >= 1 {
                market_ts = bytes.get_i64_le();
            }
            if extra_fields >= 2 {
                price_offset = bytes.get_f64_le();
            }
            if bytes.remaining() != 0 {
                return Err("Unexpected trailing bytes for ArbHedgeCtx".to_string());
            }

            Ok(ArbHedgeCtx {
                strategy_id,
                client_order_id,
                hedge_qty,
                hedge_side,
                limit_price,
                price_tick,
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
            })
        }

        let format = detect_format(&bytes, BASIC_FIELDS_LEN);
        match format {
            Some((with_ts, extra_fields)) => parse(bytes, with_ts, Some(extra_fields)),
            None => parse(bytes.clone(), true, None).or_else(|_| parse(bytes, false, None)),
        }
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

        // Price/qty tick exponents
        buf.put_i32_le(self.price_tick_exp);
        buf.put_i32_le(self.qty_tick_exp);

        // Price list (ticks)
        buf.put_u32_le(self.price_list.len() as u32);
        for price in &self.price_list {
            buf.put_i32_le(*price);
        }

        // Amount list (qty ticks)
        buf.put_u32_le(self.amount_list.len() as u32);
        for amount in &self.amount_list {
            buf.put_i64_le(*amount);
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
        // Opening leg
        if bytes.remaining() < 1 + 8 + 8 + 8 {
            return Err("Not enough bytes for opening leg".to_string());
        }
        let opening_venue = bytes.get_u8();
        let opening_bid0 = bytes.get_f64_le();
        let opening_ask0 = bytes.get_f64_le();
        let opening_ts = bytes.get_i64_le();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for price/qty tick exponents".to_string());
        }
        let price_tick_exp = bytes.get_i32_le();
        let qty_tick_exp = bytes.get_i32_le();

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for price_list length".to_string());
        }
        let price_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < price_len.saturating_mul(4) {
            return Err(format!(
                "Not enough bytes for price_list: need {}, have {}",
                price_len.saturating_mul(4),
                bytes.remaining()
            ));
        }
        let mut price_list = Vec::with_capacity(price_len);
        for _ in 0..price_len {
            price_list.push(bytes.get_i32_le());
        }

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for amount_list length".to_string());
        }
        let amount_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < amount_len.saturating_mul(8) {
            return Err(format!(
                "Not enough bytes for amount_list: need {}, have {}",
                amount_len.saturating_mul(8),
                bytes.remaining()
            ));
        }
        let mut amount_list = Vec::with_capacity(amount_len);
        for _ in 0..amount_len {
            let amount = bytes.get_i64_le();
            amount_list.push(amount);
        }

        if price_list.len() != amount_list.len() {
            return Err(format!(
                "price_list/amount_list length mismatch: price_len={} amount_len={}",
                price_list.len(),
                amount_list.len()
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
            price_tick_exp,
            qty_tick_exp,
            price_list,
            amount_list,
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
}

impl MmHedgeSignalQueryMsg {
    pub fn new(symbol: &str, period_buy_qty: f64, period_sell_qty: f64, net_qty: f64) -> Self {
        let mut symbol_bytes = [0u8; 32];
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        symbol_bytes[..len].copy_from_slice(&bytes[..len]);
        Self {
            symbol: symbol_bytes,
            period_buy_qty,
            period_sell_qty,
            net_qty,
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
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 24 {
            return Err("insufficient bytes for buy/sell/net qty".to_string());
        }
        let period_buy_qty = bytes.get_f64_le();
        let period_sell_qty = bytes.get_f64_le();
        let net_qty = bytes.get_f64_le();
        if bytes.remaining() != 0 && bytes.iter().any(|&b| b != 0) {
            return Err("Unexpected trailing bytes for MmHedgeSignalQueryMsg".to_string());
        }
        Ok(Self {
            symbol,
            period_buy_qty,
            period_sell_qty,
            net_qty,
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

    /// Hedge quantity
    pub hedge_qty: f64,

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
        hedge_qty: f64,
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
            hedge_qty,
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
        buf.put_f64_le(self.hedge_qty);
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
            return Err("insufficient bytes for hedge_qty".into());
        }
        let hedge_qty = bytes.get_f64_le();

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
            hedge_qty,
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
