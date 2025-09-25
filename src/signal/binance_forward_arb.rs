use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::common::account_msg::{ExecutionReportMsg,OrderTradeUpdateMsg};
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::exposure_manager::ExposureManager;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::signal::strategy::Strategy;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_request::{
    BinanceNewMarginOrderRequest, BinanceNewUMOrderRequest, TradeRequestType,
};
use crate::trade_engine::trade_response_handle::TradeExecOutcome;

/// 币安单所正向套利开仓信号上下文
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbOpenCtx {
    pub spot_symbol: String,
    pub amount: f32,
    pub side: Side,
    pub order_type: OrderType,
    pub price: f32,
    pub exp_time: i64,
}

/// 币安单所正向套利平仓信号上下文
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbCloseCtx {
    pub exp_time: i64,
}

/// 币安单所正向套利对冲信号上下文
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbHedgeCtx {
    pub spot_order_id: i64,
}

impl BinSingleForwardArbOpenCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32_le(self.spot_symbol.len() as u32);
        buf.put_slice(self.spot_symbol.as_bytes());
        buf.put_f32_le(self.amount);
        buf.put_u8(self.side as u8);
        buf.put_u8(self.order_type as u8);
        buf.put_f32_le(self.price);
        buf.put_i64_le(self.exp_time);

        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 {
            return Err("Not enough bytes for spot_symbol length".to_string());
        }
        let spot_symbol_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < spot_symbol_len {
            return Err("Not enough bytes for spot_symbol".to_string());
        }
        let spot_symbol = String::from_utf8(bytes.copy_to_bytes(spot_symbol_len).to_vec())
            .map_err(|e| format!("Invalid UTF-8 for spot_symbol: {e}"))?;

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for amount".to_string());
        }
        let amount = bytes.get_f32_le();

        if bytes.remaining() < 1 {
            return Err("Not enough bytes for side".to_string());
        }
        let side = Side::from_u8(bytes.get_u8()).ok_or_else(|| "Invalid side value".to_string())?;

        if bytes.remaining() < 1 {
            return Err("Not enough bytes for order_type".to_string());
        }
        let order_type = OrderType::from_u8(bytes.get_u8())
            .ok_or_else(|| "Invalid order_type value".to_string())?;

        if bytes.remaining() < 4 {
            return Err("Not enough bytes for price".to_string());
        }
        let price = bytes.get_f32_le();

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for exp_time".to_string());
        }
        let exp_time = bytes.get_i64_le();

        Ok(Self {
            spot_symbol,
            amount,
            side,
            order_type,
            price,
            exp_time,
        })
    }
}

impl BinSingleForwardArbCloseCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i64_le(self.exp_time);
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        let exp_time = bytes.get_i64_le();
        Ok(Self {
            exp_time
        })
    }
}

impl BinSingleForwardArbHedgeCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i64_le(self.spot_order_id);

        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 8 {
            return Err("Not enough bytes for spot_order_id".to_string());
        }
        let spot_order_id = bytes.get_i64_le();

        Ok(Self { spot_order_id })
    }
}

/// 策略运行参数
#[derive(Debug, Clone)]
pub struct BinSingleForwardArbStrategyCfg {
    pub open_range: f64,
    pub order_timeout_ms: i64,
    pub max_position: f64,
}

impl Default for BinSingleForwardArbStrategyCfg {
    fn default() -> Self {
        Self {
            open_range: 0.001,
            order_timeout_ms: 3_000,
            max_position: 1_000.0,
        }
    }
}

/// 币安单所正向套利策略
pub struct BinSingleForwardArbStrategy {
    pub strategy_id: i32, //策略id
    pub symbol: String,
    pub create_time: i64, //策略构建时间
    pub margin_order_id: i64,
    pub um_hedge_order_id: i64,
    pub close_margin_order_id: i64,
    pub close_um_hedge_order_id: i64,
    order_seq: u32,
    order_manager: Rc<RefCell<OrderManager>>,
    exposure_manager: Rc<RefCell<ExposureManager>>,
    order_tx: UnboundedSender<Bytes>,
}

impl BinSingleForwardArbStrategy {
    pub fn new(
        id: i32,
        now: i64,
        symbol: String,
        order_manager: Rc<RefCell<OrderManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        order_tx: UnboundedSender<Bytes>,
    ) -> Self {
        Self {
            strategy_id: id,
            symbol,
            create_time: now,
            margin_order_id: 0,
            um_hedge_order_id: 0,
            close_margin_order_id: 0,
            close_um_hedge_order_id: 0,
            order_seq: 0,
            order_manager,
            exposure_manager,
            order_tx,
        }
    }
    //1、传入order_manager的&ref，检查当前挂单是否小于3
    fn check_current_pending_limit_order(symbol: &String, order_manager: &OrderManager) -> bool {
        const MAX_PENDING_LIMIT: i32 = 3;
        let count = order_manager.get_symbol_pending_limit_order_count(symbol);
        if count >= MAX_PENDING_LIMIT {
            warn!(
                "{}: symbol={} 当前限价挂单数={}，超过上限 {}",
                Self::strategy_name(),
                symbol,
                count,
                MAX_PENDING_LIMIT
            );
            false
        } else {
            true
        }
    }

    //2、传入binance_pm_spot_manager的ref，检测当前symbol的敞口
    //symbol是xxusdt，查看当前symbol的敞口是否大于总资产比例的3%
    fn check_for_symbol_exposure(symbol: &String, exposure_manager: &ExposureManager) -> bool {
        const EXPOSURE_RATIO_LIMIT: f64 = 0.03;
        let symbol_upper = symbol.to_uppercase();
        let Some(base_asset) = Self::extract_base_asset(&symbol_upper) else {
            warn!(
                "{}: 无法识别 symbol={} 的基础资产，跳过敞口检查",
                Self::strategy_name(),
                symbol
            );
            return true;
        };

        let Some(entry) = exposure_manager.exposure_for_asset(&base_asset) else {
            warn!(
                "{}: 未找到资产 {} 的敞口记录，跳过敞口检查",
                Self::strategy_name(),
                base_asset
            );
            return true;
        };

        let total_equity = exposure_manager.total_equity();
        if total_equity <= f64::EPSILON {
            warn!(
                "{}: 账户总权益近似为 0，无法计算敞口比例",
                Self::strategy_name()
            );
            return false;
        }

        let ratio = entry.exposure.abs() / total_equity;
        if ratio > EXPOSURE_RATIO_LIMIT {
            warn!(
                "{}: 资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口={:.6}, 权益={:.6})",
                Self::strategy_name(),
                base_asset,
                ratio * 100.0,
                EXPOSURE_RATIO_LIMIT * 100.0,
                entry.exposure,
                total_equity
            );
            false
        } else {
            debug!(
                "{}: 资产 {} 敞口占比 {:.4}% 合规 (敞口={:.6}, 权益={:.6})",
                Self::strategy_name(),
                base_asset,
                ratio * 100.0,
                entry.exposure,
                total_equity
            );
            true
        }
    }

    //3、检查总敞口是否大于总资产的3%
    fn check_for_total_exposure(exposure_manager: &ExposureManager) -> bool {
        const EXPOSURE_RATIO_LIMIT: f64 = 0.03;
        let total_equity = exposure_manager.total_equity();
        if total_equity <= f64::EPSILON {
            warn!(
                "{}: 账户总权益近似为 0，无法计算总敞口占比",
                Self::strategy_name()
            );
            return false;
        }

        let abs_total = exposure_manager.total_abs_exposure();
        let ratio = abs_total / total_equity;
        if ratio > EXPOSURE_RATIO_LIMIT {
            warn!(
                "{}: 总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口={:.6}, 权益={:.6})",
                Self::strategy_name(),
                ratio * 100.0,
                EXPOSURE_RATIO_LIMIT * 100.0,
                abs_total,
                total_equity
            );
            false
        } else {
            debug!(
                "{}: 总敞口占比 {:.4}% 合规 (总敞口={:.6}, 权益={:.6})",
                Self::strategy_name(),
                ratio * 100.0,
                abs_total,
                total_equity
            );
            true
        }
    }

    fn extract_base_asset(symbol_upper: &str) -> Option<String> {
        const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
        for quote in QUOTES {
            if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
                return Some(symbol_upper[..symbol_upper.len() - quote.len()].to_string());
            }
        }
        None
    }

    fn strategy_name() -> &'static str {
        "BinSingleForwardArbStrategy"
    }

    /// 构造 margin 正向买入限价单，全部风控检查通过后写入 order manager。
    pub fn create_margin_order(
        &mut self,
        open_ctx: &BinSingleForwardArbOpenCtx,
    ) -> Result<(), String> {
        if self.margin_order_id != 0 {
            return Err(format!(
                "{}: strategy_id={} 已存在 margin 订单 {}",
                Self::strategy_name(),
                self.strategy_id,
                self.margin_order_id
            ));
        }

        {
            let order_manager = self.order_manager.borrow();
            if !Self::check_current_pending_limit_order(&open_ctx.spot_symbol, &order_manager) {
                return Err(format!(
                    "{}: symbol={} 挂单数量超限",
                    Self::strategy_name(),
                    open_ctx.spot_symbol
                ));
            }
        }

        {
            let exposure_manager = self.exposure_manager.borrow();
            if !Self::check_for_symbol_exposure(&open_ctx.spot_symbol, &exposure_manager) {
                return Err(format!(
                    "{}: symbol={} 敞口校验未通过",
                    Self::strategy_name(),
                    open_ctx.spot_symbol
                ));
            }

            if !Self::check_for_total_exposure(&exposure_manager) {
                return Err(format!("{}: 总敞口校验未通过", Self::strategy_name()));
            }
        }

        if !open_ctx.side.is_buy() {
            return Err(format!(
                "{}: 仅支持买入方向，下单方向为 {:?}",
                Self::strategy_name(),
                open_ctx.side
            ));
        }

        if !open_ctx.spot_symbol.eq_ignore_ascii_case(&self.symbol) {
            return Err(format!(
                "{}: 策略 symbol={} 与下单 symbol={} 不一致",
                Self::strategy_name(),
                self.symbol,
                open_ctx.spot_symbol
            ));
        }

        if open_ctx.amount <= 0.0 {
            return Err(format!(
                "{}: 无效的下单数量 amount={}",
                Self::strategy_name(),
                open_ctx.amount
            ));
        }

        if open_ctx.order_type.is_limit() && open_ctx.price <= 0.0 {
            return Err(format!(
                "{}: 限价单价格必须大于 0，实际为 {}",
                Self::strategy_name(),
                open_ctx.price
            ));
        }

        let order_id = self.next_order_id();
        let mut order_manager = self.order_manager.borrow_mut();
        if order_manager.get(order_id).is_some() {
            return Err(format!(
                "{}: order_id={} 已存在",
                Self::strategy_name(),
                order_id
            ));
        }

        let price = if open_ctx.order_type.is_limit() {
            Some(open_ctx.price as f64)
        } else {
            None
        };

        let mut order = Order::new(
            order_id,
            open_ctx.order_type,
            open_ctx.spot_symbol.clone(),
            open_ctx.side,
            f64::from(open_ctx.amount),
            price,
        );
        order.set_submit_time(get_timestamp_us());

        order_manager.insert(order);
        self.margin_order_id = order_id;

        debug!(
            "{}: strategy_id={} 创建 margin 订单成功 order_id={} symbol={} qty={:.6} order_type={:?}",
            Self::strategy_name(),
            self.strategy_id,
            order_id,
            open_ctx.spot_symbol,
            open_ctx.amount,
            open_ctx.order_type
        );

        Ok(())
    }

    fn create_hedge_um_order_from_margin_order(
        &mut self,
        hedge_ctx: &BinSingleForwardArbHedgeCtx,
    ) -> Result<(), String> {
        let margin_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(hedge_ctx.spot_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: 未找到 margin 订单 id={}",
                        Self::strategy_name(),
                        hedge_ctx.spot_order_id
                    )
                })?
                .clone()
        };

        if margin_order.quantity <= 0.0 {
            return Err(format!(
                "{}: margin 订单数量非法 amount={}",
                Self::strategy_name(),
                margin_order.quantity
            ));
        }

        if self.um_hedge_order_id != 0 {
            return Err(format!(
                "{}: strategy_id={} 已存在 UM 对冲订单 {}",
                Self::strategy_name(),
                self.strategy_id,
                self.um_hedge_order_id
            ));
        }

        let (hedge_side_enum, hedge_side_str, position_side_str) = match margin_order.side {
            Side::Buy => (Side::Sell, "SELL", "SHORT"),
            Side::Sell => (Side::Buy, "BUY", "LONG"),
        };

        let hedge_quantity_str = format_quantity(margin_order.quantity);
        let order_id = self.next_order_id();
        let create_time = get_timestamp_us();
        let hedge_symbol = margin_order.symbol.clone();

        let params = Bytes::from(format!(
            "symbol={}&side={}&type=MARKET&quantity={}&positionSide={}&newClientOrderId={}",
            hedge_symbol, hedge_side_str, hedge_quantity_str, position_side_str, order_id
        ));

        let request = BinanceNewUMOrderRequest::create(create_time, order_id, params);
        let payload = request.to_bytes();

        self.order_tx
            .send(payload)
            .map_err(|e| format!("{}: 推送对冲订单失败: {}", Self::strategy_name(), e))?;

        let mut order_manager = self.order_manager.borrow_mut();
        let mut hedge_order = Order::new(
            order_id,
            OrderType::Market,
            hedge_symbol,
            hedge_side_enum,
            margin_order.quantity,
            None,
        );
        hedge_order.set_submit_time(create_time);
        order_manager.insert(hedge_order);

        self.um_hedge_order_id = order_id;

        debug!(
            "{}: strategy_id={} 创建 UM 对冲订单成功 order_id={} qty={:.6} side={}",
            Self::strategy_name(),
            self.strategy_id,
            order_id,
            margin_order.quantity,
            hedge_side_str
        );

        Ok(())
    }

    fn close_positions(&mut self) -> Result<(), String> {
        let margin_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(self.margin_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: 未找到 margin 订单 id={}",
                        Self::strategy_name(),
                        self.margin_order_id
                    )
                })?
                .clone()
        };

        let um_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(self.um_hedge_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: 未找到 UM 订单 id={}",
                        Self::strategy_name(),
                        self.um_hedge_order_id
                    )
                })?
                .clone()
        };

        if margin_order.quantity <= 0.0 || um_order.quantity <= 0.0 {
            return Err(format!(
                "{}: 平仓订单数量异常 margin_qty={} um_qty={}",
                Self::strategy_name(),
                margin_order.quantity,
                um_order.quantity
            ));
        }

        let (margin_side_enum, margin_side_str) = match margin_order.side {
            Side::Buy => (Side::Sell, "SELL"),
            Side::Sell => (Side::Buy, "BUY"),
        };

        let (um_side_enum, um_side_str, position_side_str) = match um_order.side {
            Side::Buy => (Side::Sell, "SELL", "LONG"),
            Side::Sell => (Side::Buy, "BUY", "SHORT"),
        };

        let margin_close_id = self.next_order_id();
        let um_close_id = self.next_order_id();
        let now = get_timestamp_us();

        let margin_params = Bytes::from(format!(
            "symbol={}&side={}&type=MARKET&quantity={}&newClientOrderId={}",
            margin_order.symbol,
            margin_side_str,
            format_quantity(margin_order.quantity),
            margin_close_id
        ));

        let margin_request =
            BinanceNewMarginOrderRequest::create(now, margin_close_id, margin_params);
        self.order_tx
            .send(margin_request.to_bytes())
            .map_err(|e| format!("{}: 推送 margin 平仓失败: {}", Self::strategy_name(), e))?;

        let um_params = Bytes::from(format!(
            "symbol={}&side={}&type=MARKET&quantity={}&positionSide={}&newClientOrderId={}",
            um_order.symbol,
            um_side_str,
            format_quantity(um_order.quantity),
            position_side_str,
            um_close_id
        ));

        let um_request = BinanceNewUMOrderRequest::create(now, um_close_id, um_params);
        self.order_tx
            .send(um_request.to_bytes())
            .map_err(|e| format!("{}: 推送 UM 平仓失败: {}", Self::strategy_name(), e))?;

        {
            let mut manager = self.order_manager.borrow_mut();

            let mut margin_close_order = Order::new(
                margin_close_id,
                OrderType::Market,
                margin_order.symbol,
                margin_side_enum,
                margin_order.quantity,
                None,
            );
            margin_close_order.set_submit_time(now);
            manager.insert(margin_close_order);

            let mut um_close_order = Order::new(
                um_close_id,
                OrderType::Market,
                um_order.symbol,
                um_side_enum,
                um_order.quantity,
                None,
            );
            um_close_order.set_submit_time(now);
            manager.insert(um_close_order);
        }

        self.close_margin_order_id = margin_close_id;
        self.close_um_hedge_order_id = um_close_id;

        debug!(
            "{}: strategy_id={} 触发平仓成功 margin_order={} um_order={}",
            Self::strategy_name(),
            self.strategy_id,
            margin_close_id,
            um_close_id
        );

        Ok(())
    }

    fn next_order_id(&mut self) -> i64 {
        let seq = self.order_seq;
        self.order_seq = self.order_seq.wrapping_add(1);
        Self::compose_order_id(self.strategy_id, seq)
    }

    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        ((order_id >> 32) as i32) == self.strategy_id
    }
}

fn format_quantity(quantity: f64) -> String {
    let mut s = format!("{:.8}", quantity);
    if let Some(dot_pos) = s.find('.') {
        while s.len() > dot_pos + 1 && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

impl Strategy for BinSingleForwardArbStrategy {
    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn handle_trade_signal(&mut self, signal_raws: &Bytes) {
        match TradeSignal::from_bytes(signal_raws) {
            Ok(signal) => match signal.signal_type {
                SignalType::BinSingleForwardArbOpen => {
                    match BinSingleForwardArbOpenCtx::from_bytes(signal.context.clone()) {
                        Ok(ctx) => match self.create_margin_order(&ctx) {
                            Ok(()) => debug!(
                                "{}: strategy_id={} 成功创建开仓订单",
                                Self::strategy_name(),
                                self.strategy_id
                            ),
                            Err(err) => warn!(
                                "{}: strategy_id={} 开仓下单失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            ),
                        },
                        Err(err) => {
                            warn!(
                                "{}: strategy_id={} 解析开仓上下文失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        }
                    }
                }
                SignalType::BinSingleForwardArbHedge => {
                    match BinSingleForwardArbHedgeCtx::from_bytes(signal.context.clone()) {
                        Ok(ctx) => match self.create_hedge_um_order_from_margin_order(&ctx) {
                            Ok(()) => debug!(
                                "{}: strategy_id={} 成功触发对冲订单",
                                Self::strategy_name(),
                                self.strategy_id
                            ),
                            Err(err) => warn!(
                                "{}: strategy_id={} 创建对冲订单失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            ),
                        },
                        Err(err) => {
                            warn!(
                                "{}: strategy_id={} 解析对冲上下文失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        }
                    }
                }
                SignalType::BinSingleForwardArbClose => {
                    match BinSingleForwardArbCloseCtx::from_bytes(signal.context.clone()) {
                        Ok(_) => match self.close_positions() {
                            Ok(()) => debug!(
                                "{}: strategy_id={} 成功触发平仓",
                                Self::strategy_name(),
                                self.strategy_id
                            ),
                            Err(err) => warn!(
                                "{}: strategy_id={} 平仓失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            ),
                        },
                        Err(err) => {
                            warn!(
                                "{}: strategy_id={} 解析平仓上下文失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        }
                    }
                }
            },
            Err(err) => warn!(
                "failed to parse trade signal for strategy_id={}: {}",
                self.strategy_id, err
            ),
        }
    }
    fn handle_trade_response(&mut self, outcome: &TradeExecOutcome) {
        if self.is_strategy_order(outcome.client_order_id) {
            //判断是以下哪个成交回报
            match outcome.req_type {
                TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceNewUMOrder => {
                    debug!(
                        "{}: strategy_id={} trade_response req_type={:?} client_order_id={} status={} body_len={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        outcome.req_type,
                        outcome.client_order_id,
                        outcome.status,
                        outcome.body.len()
                    );

                    let success = (200..300).contains(&outcome.status);
                    let now = get_timestamp_us();

                    let mut manager = self.order_manager.borrow_mut();
                    if !manager.update(outcome.client_order_id, |order| {
                        if success {
                            order.update_status(OrderExecutionStatus::Acked);
                            order.set_ack_time(now);
                        } else {
                            order.update_status(OrderExecutionStatus::Rejected);
                            order.set_end_time(now);
                        }
                    }) {

                        warn!(
                            "{}: strategy_id={} 未找到 client_order_id={} 对应订单",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.client_order_id
                        );
                    }

                    if !success {
                        warn!(
                            "{}: strategy_id={} 下单失败 req_type={:?} status={} body={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.req_type,
                            outcome.status,
                            outcome.body
                        );
                    }
                }
                _ => {
                    debug!(
                        "{}: strategy_id={} 忽略响应 req_type={:?}",
                        Self::strategy_name(),
                        self.strategy_id,
                        outcome.req_type
                    );
                }
            }
        }
    }

    fn handle_binance_execution_report(&mut self, report: &ExecutionReportMsg) {
        let order_id = report.client_order_id;
        if !self.is_strategy_order(order_id) {
            return;
        }
        let _ = match report.order_status.as_str() {
            "NEW" => {
                //NEW 更新order的create time，状态修改为ACK
                let mut manager = self.order_manager.borrow_mut();
                if !manager.update(order_id, |order| {
                    //订单从提交转移到确认状态
                    order.update_status(OrderExecutionStatus::Create);
                    //确定订单创建的交易所时间
                    order.set_create_time(report.event_time);
                    //更新订单成交量
                    order.update_rem_qty(report.cumulative_filled_quantity);
                }) {
                    warn!(
                        "{}: strategy_id={} execution_report 未找到订单 id={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        order_id
                    );
                }
            }
            "FILLED" => {
                //部分成交，更新订单状态
                let mut manager = self.order_manager.borrow_mut();
                let mut qty:f64 = 0.0;
                if !manager.update(order_id, |order| {
                    //订单从提交转移到确认状态
                    order.update_status(OrderExecutionStatus::Filled);
                    //确定订单创建的交易所时间
                    order.set_filled_time(report.event_time);
                    //更新订单成交量
                    qty = order.quantity;
                    order.update_rem_qty(report.cumulative_filled_quantity);
                }) {
                    warn!(
                        "{}: strategy_id={} execution_report 未找到订单 id={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        order_id
                    );
                }
                //裁决判断当前未对冲的头寸大小，计算usdt价值，是否足够下一单
                //获取markprice
                
                if report.cumulative_filled_quantity {
                    

                }
            },
            "CANCELED" | "EXPIRED" => {

            },
            "REJECTED" | "TRADE_PREVENTION" => {

            },
            _ => None,
        };

        let Some(status) = status else {
            debug!(
                "{}: strategy_id={} execution_report 未处理的状态 order_status={}",
                Self::strategy_name(),
                self.strategy_id,
                report.order_status
            );
            return;
        };

        if order_id == self.margin_order_id {
            debug!(
                "{}: strategy_id={} margin execution report status={}",
                Self::strategy_name(),
                self.strategy_id,
                report.order_status
            );
            // TODO: 根据 executionReport 更新杠杆开仓订单状态
        } else if order_id == self.close_margin_order_id {
            debug!(
                "{}: strategy_id={} margin close execution report status={}",
                Self::strategy_name(),
                self.strategy_id,
                report.order_status
            );
            // TODO: 根据 executionReport 更新杠杆平仓订单状态
        } else {
            debug!(
                "{}: strategy_id={} UM execution report status={}",
                Self::strategy_name(),
                self.strategy_id,
                report.order_status
            );
            // TODO: 处理合约对冲 / 平仓订单执行回报
        }
    }
    fn handle_binance_order_trade_update(&mut self, update: &OrderTradeUpdateMsg){
        
    }

    // fn handle_account_event(&mut self, account_event_msg_raws: &Bytes) {
    //     if account_event_msg_raws.len() < 8 {
    //         return;
    //     }

    //     let header = account_event_msg_raws.as_ref();
    //     let event_type = account_event_type_from_bytes(header);
    //     let payload_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

    //     if header.len() < 8 + payload_len {
    //         warn!(
    //             "{}: strategy_id={} account event truncated len={} expect {}",
    //             Self::strategy_name(),
    //             self.strategy_id,
    //             header.len(),
    //             8 + payload_len
    //         );
    //         return;
    //     }

    //     let payload = &header[8..8 + payload_len];

    //     match event_type {
    //         AccountEventType::ExecutionReport => {
    //             match ExecutionReportMsg::from_bytes(payload) {
    //                 Ok(report) => {
    //                     debug!(
    //                         "{}: strategy_id={} execution_report order_id={} status={}",
    //                         Self::strategy_name(),
    //                         self.strategy_id,
    //                         report.order_id,
    //                         report.order_status
    //                     );
    //                     // TODO: 进一步处理 executionReport 事件
    //                 }
    //                 Err(err) => warn!(
    //                     "{}: strategy_id={} 解析 executionReport 失败: {}",
    //                     Self::strategy_name(),
    //                     self.strategy_id,
    //                     err
    //                 ),
    //             }
    //         }
    //         AccountEventType::OrderTradeUpdate => {
    //             match OrderTradeUpdateMsg::from_bytes(payload) {
    //                 Ok(update) => {
    //                     debug!(
    //                         "{}: strategy_id={} order_trade_update order_id={} trade_id={}",
    //                         Self::strategy_name(),
    //                         self.strategy_id,
    //                         update.order_id,
    //                         update.trade_id
    //                     );
    //                     // TODO: 进一步处理 ORDER_TRADE_UPDATE 事件
    //                 }
    //                 Err(err) => warn!(
    //                     "{}: strategy_id={} 解析 order_trade_update 失败: {}",
    //                     Self::strategy_name(),
    //                     self.strategy_id,
    //                     err
    //                 ),
    //             }
    //         }
    //         _ => {}
    //     }
    // }

    fn hanle_period_clock(&mut self, current_tp: i64) {
        todo!()
    }

    fn is_active(&self) -> bool {
        todo!()
    }
}
