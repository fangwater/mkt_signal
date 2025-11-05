use crate::signal::common::{TradingLeg, Side, OrderType, SignalBytes, bytes_helper};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Generic arbitrage open signal context
#[derive(Debug, Clone, Copy)]
pub struct ArbOpenCtx {
    /// Opening leg (active leg)
    pub opening_leg: TradingLeg,

    /// Opening leg symbol - using fixed size array to avoid heap allocation
    pub opening_symbol: [u8; 32],  // 32 bytes should be enough for symbol

    /// Hedging leg (passive leg)
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Trade amount
    pub amount: f32,

    /// Trade side (for opening leg) - stored as u8
    pub side: u8,

    /// Order type - stored as u8
    pub order_type: u8,

    /// Opening price
    pub price: f64,

    /// Price tick size / minimum price movement
    pub price_tick: f64,

    /// Order expiration time (microseconds)
    pub exp_time: i64,

    /// Creation timestamp (microseconds)
    pub create_ts: i64,

    /// Opening threshold (trigger condition)
    pub open_threshold: f64,

    /// Hedge timeout (microseconds)
    pub hedge_timeout_us: i64,

    /// Funding rate moving average (futures specific, 0 means none)
    pub funding_ma: f64,

    /// Predicted funding rate (futures specific, 0 means none)
    pub predicted_funding_rate: f64,

    /// Loan rate (margin/leverage specific, 0 means none)
    pub loan_rate: f64,
}

impl ArbOpenCtx {
    /// Create new arbitrage open context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg { venue: 0, bid0: 0.0, ask0: 0.0 },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg { venue: 0, bid0: 0.0, ask0: 0.0 },
            hedging_symbol: [0u8; 32],
            amount: 0.0,
            side: 0,
            order_type: 0,
            price: 0.0,
            price_tick: 0.0,
            exp_time: 0,
            create_ts: 0,
            open_threshold: 0.0,
            hedge_timeout_us: 0,
            funding_ma: 0.0,
            predicted_funding_rate: 0.0,
            loan_rate: 0.0,
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
        let end = self.opening_symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.opening_symbol[..end]).to_string()
    }

    /// Set hedging leg symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.hedging_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get hedging leg symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self.hedging_symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    /// Get OrderType enum
    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
    }

    /// Set OrderType
    pub fn set_order_type(&mut self, order_type: OrderType) {
        self.order_type = order_type.to_u8();
    }


    /// 构造 margin 正向买入限价单，全部风控检查通过后写入 order manager。
    pub fn create_margin_order(&mut self) -> Result<(), String> {
        // todo: 这个检查放到外部去更合适
        // if self.margin_order_id != 0 {
        //     return Err(format!(
        //         "{}: strategy_id={} 已存在 margin 订单 {}",
        //         Self::strategy_name(),
        //         self.strategy_id,
        //         self.margin_order_id
        //     ));
        // }


        let mut params_parts = vec![
            format!("symbol={}", open_ctx.spot_symbol),
            format!("side={}", side_str),
            format!("type={}", order_type_str),
            format!("quantity={}", qty_str),
            format!("newClientOrderId={}", order_id),
        ];
        if open_ctx.order_type.is_limit() {
            params_parts.push("timeInForce=GTC".to_string());
            params_parts.push(format!("price={}", format_price(effective_price)));
        }

        debug!(
            "{}: strategy_id={} margin 开仓价格对齐 raw={:.8} tick={:.8} aligned={:.8}",
            Self::strategy_name(),
            self.strategy_id,
            raw_price,
            price_tick,
            effective_price
        );

        debug!(
            "{}: strategy_id={} 构造 margin 开仓参数 {:?}",
            Self::strategy_name(),
            self.strategy_id,
            params_parts
        );

        let params = Bytes::from(params_parts.join("&"));
        let request = BinanceNewMarginOrderRequest::create(now, order_id, params);

        self.order_tx
            .send(request.to_bytes())
            .map_err(|e| format!("{}: 推送 margin 开仓失败: {}", Self::strategy_name(), e))?;

        let mut order = Order::new(
            order_id,
            open_ctx.order_type,
            open_ctx.spot_symbol.clone(),
            open_ctx.side,
            eff_qty,
            effective_price,
        );
        order.set_submit_time(now);

        order_manager.insert(order);
        self.margin_order_id = order_id;
        self.initial_margin_order_id = order_id;
        self.open_timeout_us = (open_ctx.exp_time > 0).then_some(open_ctx.exp_time);

        info!(
            "{}: strategy_id={} 提交 margin 开仓请求 symbol={} qty={:.6} type={} price={:.8} order_id={}",
            Self::strategy_name(),
            self.strategy_id,
            open_ctx.spot_symbol,
            open_ctx.amount,
            order_type_str,
            effective_price,
            order_id
        );

        Ok(())
    }

}

impl SignalBytes for ArbOpenCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        buf.put_u8(self.opening_leg.venue);
        buf.put_f64_le(self.opening_leg.bid0);
        buf.put_f64_le(self.opening_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        // Trade parameters
        buf.put_f32_le(self.amount);
        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.price_tick);
        buf.put_i64_le(self.exp_time);
        buf.put_i64_le(self.create_ts);
        buf.put_f64_le(self.open_threshold);
        buf.put_i64_le(self.hedge_timeout_us);

        // Optional fields (using 0.0 for None)
        buf.put_f64_le(self.funding_ma);
        buf.put_f64_le(self.predicted_funding_rate);
        buf.put_f64_le(self.loan_rate);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        // Opening leg
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for opening leg".to_string());
        }
        let opening_venue = bytes.get_u8();
        let opening_bid0 = bytes.get_f64_le();
        let opening_ask0 = bytes.get_f64_le();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Hedging leg
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for hedging leg".to_string());
        }
        let hedging_venue = bytes.get_u8();
        let hedging_bid0 = bytes.get_f64_le();
        let hedging_ask0 = bytes.get_f64_le();
        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Trade parameters
        if bytes.remaining() < 4 + 1 + 1 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 {
            return Err("Not enough bytes for trade parameters".to_string());
        }
        let amount = bytes.get_f32_le();
        let side = bytes.get_u8();
        let order_type = bytes.get_u8();
        let price = bytes.get_f64_le();
        let price_tick = bytes.get_f64_le();
        let exp_time = bytes.get_i64_le();
        let create_ts = bytes.get_i64_le();
        let open_threshold = bytes.get_f64_le();
        let hedge_timeout_us = bytes.get_i64_le();

        // Optional fields
        let funding_ma = bytes.get_f64_le();
        let predicted_funding_rate = bytes.get_f64_le();
        let loan_rate = bytes.get_f64_le();

        Ok(Self {
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
            amount,
            side,
            order_type,
            price,
            price_tick,
            exp_time,
            create_ts,
            open_threshold,
            hedge_timeout_us,
            funding_ma,
            predicted_funding_rate,
            loan_rate,
        })
    }
}