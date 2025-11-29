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
            },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
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
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg market data
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        buf.put_i64_le(self.market_ts);
        buf.put_f64_le(self.price_offset);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        // 基本字段长度: 4 + 8 + 8 + 1 + 8 + 8 + 1 + 8 = 46
        const BASIC_FIELDS_LEN: usize = 4 + 8 + 8 + 1 + 8 + 8 + 1 + 8;
        // 旧格式总长度: 46 + hedging_leg(17) + hedging_symbol(32) + market_ts(8) + price_offset(8) = 111
        const OLD_FORMAT_LEN: usize = 111;
        // 新格式总长度: 111 + opening_leg(17) + opening_symbol(32) = 160
        const NEW_FORMAT_LEN: usize = 160;

        let total_len = bytes.remaining();
        if total_len < OLD_FORMAT_LEN {
            return Err(format!(
                "Not enough bytes for ArbHedgeCtx: got {}, need at least {}",
                total_len, OLD_FORMAT_LEN
            ));
        }

        // 判断是否为新格式（包含 opening_leg）
        let has_opening_leg = total_len >= NEW_FORMAT_LEN;

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

        // Opening leg market data (新格式才有，旧格式默认为空)
        let (opening_venue, opening_bid0, opening_ask0, opening_symbol) = if has_opening_leg {
            if bytes.remaining() < 1 + 8 + 8 {
                return Err("Not enough bytes for opening leg".to_string());
            }
            let venue = bytes.get_u8();
            let bid0 = bytes.get_f64_le();
            let ask0 = bytes.get_f64_le();
            let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
            (venue, bid0, ask0, symbol)
        } else {
            // 旧格式：opening_leg 默认为空
            (0u8, 0.0, 0.0, [0u8; 32])
        };

        // Hedging leg market data
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for hedging leg".to_string());
        }
        let hedging_venue = bytes.get_u8();
        let hedging_bid0 = bytes.get_f64_le();
        let hedging_ask0 = bytes.get_f64_le();
        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for market_ts".to_string());
        }
        let market_ts = bytes.get_i64_le();

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for price_offset".to_string());
        }
        let price_offset = bytes.get_f64_le();

        Ok(Self {
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
            },
            opening_symbol,
            hedging_leg: TradingLeg {
                venue: hedging_venue,
                bid0: hedging_bid0,
                ask0: hedging_ask0,
            },
            hedging_symbol,
            market_ts,
            price_offset,
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

    /// Opening leg market data (the leg that triggered this hedge)
    pub opening_leg: TradingLeg,

    /// Opening leg symbol
    pub opening_symbol: [u8; 32],

    /// Trading venue (exchange and type) - stored as u8 (hedging venue)
    pub venue: u8,

    /// Hedging symbol (e.g., BTCUSDT)
    pub hedging_symbol: [u8; 32],

    /// Number of hedge requests issued for this strategy (monotonic)
    pub request_seq: u32,
}

impl ArbHedgeSignalQueryMsg {
    /// Create new query message
    pub fn new(
        strategy_id: i32,
        client_order_id: i64,
        query_time: i64,
        hedge_qty: f64,
        hedge_side: u8,
        opening_leg: TradingLeg,
        opening_symbol: &str,
        venue: u8,
        hedging_symbol: &str,
        request_seq: u32,
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
            opening_leg,
            opening_symbol: opening_symbol_bytes,
            venue,
            hedging_symbol: hedging_symbol_bytes,
            request_seq,
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

        // Opening leg market data
        buf.put_u8(self.opening_leg.venue);
        buf.put_f64_le(self.opening_leg.bid0);
        buf.put_f64_le(self.opening_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg info
        buf.put_u8(self.venue);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);
        buf.put_u32_le(self.request_seq);
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

        // Opening leg market data
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("insufficient bytes for opening leg".into());
        }
        let opening_venue = bytes.get_u8();
        let opening_bid0 = bytes.get_f64_le();
        let opening_ask0 = bytes.get_f64_le();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Hedging leg info
        if bytes.remaining() < 1 {
            return Err("insufficient bytes for venue".into());
        }
        let venue = bytes.get_u8();

        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 4 {
            return Err("insufficient bytes for request_seq".into());
        }
        let request_seq = bytes.get_u32_le();

        Ok(Self {
            strategy_id,
            client_order_id,
            query_time,
            hedge_qty,
            hedge_side,
            opening_leg: TradingLeg {
                venue: opening_venue,
                bid0: opening_bid0,
                ask0: opening_ask0,
            },
            opening_symbol,
            venue,
            hedging_symbol,
            request_seq,
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
