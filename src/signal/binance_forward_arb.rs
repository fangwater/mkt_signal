use std::cell::RefCell;
use std::rc::Rc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, error, info, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::common::account_msg::{ExecutionReportMsg, OrderTradeUpdateMsg};
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::exposure_manager::ExposureManager;
use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::signal::strategy::{Strategy, StrategySnapshot};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_request::{
    BinanceCancelMarginOrderRequest, BinanceNewMarginOrderRequest, BinanceNewUMOrderRequest,
    TradeRequestType,
};
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use std::cmp::Ordering;
use serde::{Serialize, Deserialize};

/// 币安单所正向套利开仓信号上下文
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbOpenCtx {
    pub spot_symbol: String,
    pub amount: f32,
    pub side: Side,
    pub order_type: OrderType,
    pub price: f64,
    pub price_tick: f64,
    pub exp_time: i64,
}

/// 币安单所正向套利对冲信号上下文
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbHedgeCtx {
    pub spot_order_id: i64,
}

/// 杠杆平仓信号上下文（限价单）
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbCloseMarginCtx {
    pub spot_symbol: String,
    pub limit_price: f64,
    pub price_tick: f64,
    pub exp_time: i64,
}

/// UM 平仓信号上下文（市价单）
#[derive(Clone, Debug)]
pub struct BinSingleForwardArbCloseUmCtx {
    pub um_symbol: String,
    pub exp_time: i64,
}

/// 内部统一的 UM 订单更新结果，用于将 REST 推送的状态转换为易于判断的枚举。
/// 这样可以避免在核心逻辑中反复解析字符串状态，也便于统一打印日志。
#[derive(Debug, Clone, Copy)]
enum UmOrderUpdateOutcome {
    Created,
    PartiallyFilled(f64),
    Filled,
    Expired,
    Ignored,
}

impl BinSingleForwardArbOpenCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32_le(self.spot_symbol.len() as u32);
        buf.put_slice(self.spot_symbol.as_bytes());
        buf.put_f32_le(self.amount);
        buf.put_u8(self.side as u8);
        buf.put_u8(self.order_type as u8);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.price_tick);
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

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for price".to_string());
        }
        let price = bytes.get_f64_le();

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for price_tick".to_string());
        }
        let price_tick = bytes.get_f64_le();

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
            price_tick,
            exp_time,
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

impl BinSingleForwardArbCloseMarginCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32_le(self.spot_symbol.len() as u32);
        buf.put_slice(self.spot_symbol.as_bytes());
        buf.put_f64_le(self.limit_price);
        buf.put_f64_le(self.price_tick);
        buf.put_i64_le(self.exp_time);

        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 {
            return Err("Not enough bytes for spot_symbol length".to_string());
        }
        let symbol_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < symbol_len {
            return Err("Not enough bytes for spot_symbol".to_string());
        }
        let spot_symbol = String::from_utf8(bytes.copy_to_bytes(symbol_len).to_vec())
            .map_err(|e| format!("Invalid UTF-8 for spot_symbol: {e}"))?;

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for limit_price".to_string());
        }
        let limit_price = bytes.get_f64_le();

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for price_tick".to_string());
        }
        let price_tick = bytes.get_f64_le();

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for exp_time".to_string());
        }
        let exp_time = bytes.get_i64_le();

        Ok(Self {
            spot_symbol,
            limit_price,
            price_tick,
            exp_time,
        })
    }
}

impl BinSingleForwardArbCloseUmCtx {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u32_le(self.um_symbol.len() as u32);
        buf.put_slice(self.um_symbol.as_bytes());
        buf.put_i64_le(self.exp_time);

        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 {
            return Err("Not enough bytes for um_symbol length".to_string());
        }
        let symbol_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < symbol_len {
            return Err("Not enough bytes for um_symbol".to_string());
        }
        let um_symbol = String::from_utf8(bytes.copy_to_bytes(symbol_len).to_vec())
            .map_err(|e| format!("Invalid UTF-8 for um_symbol: {e}"))?;

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for exp_time".to_string());
        }
        let exp_time = bytes.get_i64_le();

        Ok(Self {
            um_symbol,
            exp_time,
        })
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
    pub open_timeout_us: Option<i64>,
    pub close_margin_timeout_us: Option<i64>,
    signal_tx: Option<UnboundedSender<Bytes>>,
    um_close_signal_sent: bool,
    order_seq: u32,
    order_manager: Rc<RefCell<OrderManager>>,
    exposure_manager: Rc<RefCell<ExposureManager>>,
    order_tx: UnboundedSender<Bytes>,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    min_qty_table: std::rc::Rc<MinQtyTable>,
}

impl BinSingleForwardArbStrategy {
    pub fn new(
        id: i32,
        now: i64,
        symbol: String,
        order_manager: Rc<RefCell<OrderManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        order_tx: UnboundedSender<Bytes>,
        max_symbol_exposure_ratio: f64,
        max_total_exposure_ratio: f64,
        min_qty_table: Rc<MinQtyTable>,
    ) -> Self {
        let strategy = Self {
            strategy_id: id,
            symbol,
            create_time: now,
            margin_order_id: 0,
            um_hedge_order_id: 0,
            close_margin_order_id: 0,
            close_um_hedge_order_id: 0,
            open_timeout_us: None,
            close_margin_timeout_us: None,
            signal_tx: None,
            um_close_signal_sent: false,
            order_seq: 0,
            order_manager,
            exposure_manager,
            order_tx,
            max_symbol_exposure_ratio,
            max_total_exposure_ratio,
            min_qty_table,
        };

        info!(
            "{}: strategy_id={} 初始化 symbol={} max_symbol_ratio={:.2}% max_total_ratio={:.2}%",
            Self::strategy_name(),
            strategy.strategy_id,
            strategy.symbol,
            strategy.max_symbol_exposure_ratio * 100.0,
            strategy.max_total_exposure_ratio * 100.0
        );

        strategy
    }

    pub fn set_signal_sender(&mut self, signal_tx: UnboundedSender<Bytes>) {
        self.signal_tx = Some(signal_tx);
    }
    //1、传入order_manager的&ref，检查当前挂单是否小于3
    fn check_current_pending_limit_order(symbol: &String, order_manager: &OrderManager) -> bool {
        const MAX_PENDING_LIMIT: i32 = 3;
        let count = order_manager.get_symbol_pending_limit_order_count(symbol);
        if count >= MAX_PENDING_LIMIT {
            warn!(
                "{}: symbol={} 当前限价挂单数={}，达到上限 {}",
                Self::strategy_name(),
                symbol,
                count,
                MAX_PENDING_LIMIT
            );
            // 打印该 symbol 下当前挂着的限价单（非终态）
            Self::log_pending_limit_orders(symbol, order_manager);
            false
        } else {
            true
        }
    }

    //2、传入binance_pm_spot_manager的ref，检测当前symbol的敞口
    //symbol是xxusdt，查看当前symbol的敞口是否大于总资产比例的3%
    fn check_for_symbol_exposure(&self, symbol: &str, exposure_manager: &ExposureManager) -> bool {
        let limit = self.max_symbol_exposure_ratio;
        if limit <= 0.0 {
            debug!(
                "{}: symbol={} 敞口阈值 <= 0，跳过敞口检查",
                Self::strategy_name(),
                symbol
            );
            return true;
        }

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
        if ratio > limit {
            warn!(
                "{}: 资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口={:.6}, 权益={:.6})",
                Self::strategy_name(),
                base_asset,
                ratio * 100.0,
                limit * 100.0,
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

    //3、检查总敞口是否超过配置阈值
    fn check_for_total_exposure(&self, exposure_manager: &ExposureManager) -> bool {
        let limit = self.max_total_exposure_ratio;
        if limit <= 0.0 {
            debug!("{}: 总敞口阈值 <= 0，跳过检查", Self::strategy_name());
            return true;
        }

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
        if ratio > limit {
            warn!(
                "{}: 总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口={:.6}, 权益={:.6})",
                Self::strategy_name(),
                ratio * 100.0,
                limit * 100.0,
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
            if !self.check_for_symbol_exposure(&open_ctx.spot_symbol, &exposure_manager) {
                return Err(format!(
                    "{}: symbol={} 敞口校验未通过",
                    Self::strategy_name(),
                    open_ctx.spot_symbol
                ));
            }

            if !self.check_for_total_exposure(&exposure_manager) {
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

        let now = get_timestamp_us();

        let side_str = open_ctx.side.as_str();
        let order_type_str = open_ctx.order_type.as_str();
        // 数量 LOT_SIZE 对齐（优先 margin，再退化 spot）并确保不低于 minQty
        let raw_qty = f64::from(open_ctx.amount);
        let step = self
            .min_qty_table
            .margin_step_by_symbol(&self.symbol)
            .or_else(|| self.min_qty_table.spot_step_by_symbol(&self.symbol))
            .unwrap_or(0.0);
        let min_qty = self
            .min_qty_table
            .margin_min_qty_by_symbol(&self.symbol)
            .or_else(|| self.min_qty_table.spot_min_qty_by_symbol(&self.symbol))
            .unwrap_or(0.0);
        let mut eff_qty = if step > 0.0 {
            align_price_floor(raw_qty, step)
        } else {
            raw_qty
        };
        if min_qty > 0.0 && eff_qty < min_qty {
            eff_qty = min_qty;
        }
        if eff_qty <= 0.0 {
            return Err(format!(
                "{}: symbol={} 数量对齐后为 0，raw_qty={} step={} min_qty={}",
                Self::strategy_name(),
                self.symbol,
                raw_qty,
                step,
                min_qty
            ));
        }
        // 价格对齐（限价单）
        let price_tick = open_ctx.price_tick.max(0.0);
        let raw_price = open_ctx.price;
        let mut effective_price = raw_price;

        if open_ctx.order_type.is_limit() {
            if price_tick > 0.0 {
                effective_price = align_price_floor(effective_price, price_tick);
            }
        }

        // 名义金额下限（minNotional），若有价格可用则按名义金额抬升数量
        let min_notional = self
            .min_qty_table
            .margin_min_notional_by_symbol(&self.symbol)
            .or_else(|| self.min_qty_table.spot_min_notional_by_symbol(&self.symbol))
            .unwrap_or(0.0);
        if min_notional > 0.0 && effective_price > 0.0 {
            let required_qty = min_notional / effective_price;
            if eff_qty < required_qty {
                let before = eff_qty;
                eff_qty = if step > 0.0 {
                    align_price_ceil(required_qty, step)
                } else {
                    required_qty
                };
                debug!(
                    "{}: strategy_id={} 名义金额对齐 min_notional={:.8} price={:.8} qty_up: {} -> {}",
                    Self::strategy_name(),
                    self.strategy_id,
                    min_notional,
                    effective_price,
                    before,
                    eff_qty
                );
            }
        }

        debug!(
            "{}: strategy_id={} 数量/名义金额对齐 raw_qty={:.8} step={:.8} min_qty={:.8} min_notional={:.8} price={:.8} aligned_qty={:.8}",
            Self::strategy_name(),
            self.strategy_id,
            raw_qty,
            step,
            min_qty,
            min_notional,
            effective_price,
            eff_qty
        );
        let qty_str = format_quantity(eff_qty);
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

        debug!(
            "{}: strategy_id={} margin 开仓请求已推送 order_id={} payload_len={}",
            Self::strategy_name(),
            self.strategy_id,
            order_id,
            request.to_bytes().len()
        );

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
        self.open_timeout_us = (open_ctx.exp_time > 0).then_some(open_ctx.exp_time);
        self.um_close_signal_sent = false;
        self.close_margin_timeout_us = None;

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
        debug!(
            "{}: strategy_id={} UM 对冲下单参数 symbol={} side={} pos_side={} qty={} clientOrderId={} payload_len={}",
            Self::strategy_name(),
            self.strategy_id,
            hedge_symbol,
            hedge_side_str,
            position_side_str,
            hedge_quantity_str,
            order_id,
            payload.len()
        );

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
            0.0,
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

    fn close_margin_with_limit(
        &mut self,
        ctx: &BinSingleForwardArbCloseMarginCtx,
    ) -> Result<(), String> {
        if self.close_margin_order_id != 0 {
            return Err(format!(
                "{}: strategy_id={} 已存在待执行的 margin 平仓单 id={}",
                Self::strategy_name(),
                self.strategy_id,
                self.close_margin_order_id
            ));
        }

        if ctx.limit_price <= 0.0 {
            return Err(format!(
                "{}: strategy_id={} 平仓限价无效 limit_price={}",
                Self::strategy_name(),
                self.strategy_id,
                ctx.limit_price
            ));
        }

        let margin_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(self.margin_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: strategy_id={} 未找到原始 margin 订单 id={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        self.margin_order_id
                    )
                })?
                .clone()
        };

        if !ctx.spot_symbol.eq_ignore_ascii_case(&margin_order.symbol) {
            return Err(format!(
                "{}: strategy_id={} margin 平仓上下文 symbol={} 与持仓 symbol={} 不匹配",
                Self::strategy_name(),
                self.strategy_id,
                ctx.spot_symbol,
                margin_order.symbol
            ));
        }

        let expected_qty = margin_order.quantity;
        // 按 LOT_SIZE 对齐平仓数量，并不低于 minQty，最终不超过 expected_qty
        let step = self
            .min_qty_table
            .margin_step_by_symbol(&margin_order.symbol)
            .or_else(|| self.min_qty_table.spot_step_by_symbol(&margin_order.symbol))
            .unwrap_or(0.0);
        let min_qty = self
            .min_qty_table
            .margin_min_qty_by_symbol(&margin_order.symbol)
            .or_else(|| self.min_qty_table.spot_min_qty_by_symbol(&margin_order.symbol))
            .unwrap_or(0.0);
        let mut close_qty = if step > 0.0 {
            align_price_floor(expected_qty, step)
        } else {
            expected_qty
        };
        if min_qty > 0.0 && close_qty < min_qty {
            close_qty = min_qty;
        }
        // 名义金额校验（若配置可用）
        let min_notional = self
            .min_qty_table
            .margin_min_notional_by_symbol(&margin_order.symbol)
            .or_else(|| self.min_qty_table.spot_min_notional_by_symbol(&margin_order.symbol))
            .unwrap_or(0.0);
        let price_tick = ctx.price_tick.max(0.0);
        let mut limit_price = ctx.limit_price;
        if price_tick > 0.0 {
            limit_price = align_price_ceil(limit_price, price_tick);
        }
        if min_notional > 0.0 && limit_price > 0.0 {
            let required_qty = min_notional / limit_price;
            if close_qty < required_qty {
                let before = close_qty;
                close_qty = if step > 0.0 { align_price_ceil(required_qty, step) } else { required_qty };
                debug!(
                    "{}: strategy_id={} 平仓名义金额对齐 min_notional={:.8} price={:.8} qty_up: {} -> {}",
                    Self::strategy_name(),
                    self.strategy_id,
                    min_notional,
                    limit_price,
                    before,
                    close_qty
                );
            }
        }
        if close_qty > expected_qty {
            warn!(
                "{}: strategy_id={} 平仓可用不足以满足名义下限，将按可用数量下单 required={} expected={}",
                Self::strategy_name(),
                self.strategy_id,
                close_qty,
                expected_qty
            );
            close_qty = expected_qty; // 不超过可平仓数量
        }
        debug!(
            "{}: strategy_id={} 平仓数量对齐 raw_qty={:.8} step={:.8} min_qty={:.8} min_notional={:.8} price={:.8} aligned_qty={:.8}",
            Self::strategy_name(),
            self.strategy_id,
            expected_qty,
            step,
            min_qty,
            min_notional,
            limit_price,
            close_qty
        );

        if close_qty <= 0.0 {
            return Err(format!(
                "{}: strategy_id={} margin 平仓数量无效 close_qty={}",
                Self::strategy_name(),
                self.strategy_id,
                close_qty
            ));
        }

        let (close_side_enum, close_side_str) = match margin_order.side {
            Side::Buy => (Side::Sell, "SELL"),
            Side::Sell => (Side::Buy, "BUY"),
        };

        let raw_price = ctx.limit_price;
        let margin_close_id = self.next_order_id();
        let now = get_timestamp_us();

        let params = Bytes::from(format!(
            "symbol={}&side={}&type=LIMIT&timeInForce=GTC&quantity={}&price={}&newClientOrderId={}",
            margin_order.symbol,
            close_side_str,
            format_quantity(close_qty),
            format_price(limit_price),
            margin_close_id
        ));

        debug!(
            "{}: strategy_id={} margin 平仓价格对齐 raw={:.8} tick={:.8} aligned={:.8}",
            Self::strategy_name(),
            self.strategy_id,
            raw_price,
            price_tick,
            limit_price
        );

        let request = BinanceNewMarginOrderRequest::create(now, margin_close_id, params);
        self.order_tx
            .send(request.to_bytes())
            .map_err(|e| format!("{}: 推送 margin 平仓失败: {}", Self::strategy_name(), e))?;

        {
            let mut manager = self.order_manager.borrow_mut();
            let mut close_order = Order::new(
                margin_close_id,
                OrderType::Limit,
                margin_order.symbol.clone(),
                close_side_enum,
                close_qty,
                limit_price,
            );
            close_order.set_submit_time(now);
            manager.insert(close_order);
        }

        self.close_margin_order_id = margin_close_id;
        self.um_close_signal_sent = false;
        self.close_margin_timeout_us = (ctx.exp_time > 0).then_some(ctx.exp_time);

        info!(
            "{}: strategy_id={} 提交 margin 限价平仓 symbol={} qty={:.6} price={:.8} order_id={}",
            Self::strategy_name(),
            self.strategy_id,
            margin_order.symbol,
            close_qty,
            limit_price,
            margin_close_id
        );

        Ok(())
    }

    fn close_um_with_market(&mut self, ctx: &BinSingleForwardArbCloseUmCtx) -> Result<(), String> {
        if self.close_um_hedge_order_id != 0 {
            return Err(format!(
                "{}: strategy_id={} 已存在待执行的 UM 平仓单 id={}",
                Self::strategy_name(),
                self.strategy_id,
                self.close_um_hedge_order_id
            ));
        }

        let um_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(self.um_hedge_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: strategy_id={} 未找到原始 UM 订单 id={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        self.um_hedge_order_id
                    )
                })?
                .clone()
        };

        if !ctx.um_symbol.eq_ignore_ascii_case(&um_order.symbol) {
            return Err(format!(
                "{}: strategy_id={} UM 平仓上下文 symbol={} 与持仓 symbol={} 不匹配",
                Self::strategy_name(),
                self.strategy_id,
                ctx.um_symbol,
                um_order.symbol
            ));
        }

        let expected_qty = um_order.quantity;
        let close_qty = expected_qty;

        if close_qty <= 0.0 {
            return Err(format!(
                "{}: strategy_id={} UM 平仓数量无效 close_qty={}",
                Self::strategy_name(),
                self.strategy_id,
                close_qty
            ));
        }

        let (close_side_enum, close_side_str, position_side_str) = match um_order.side {
            Side::Buy => (Side::Sell, "SELL", "LONG"),
            Side::Sell => (Side::Buy, "BUY", "SHORT"),
        };

        let um_close_id = self.next_order_id();
        let now = get_timestamp_us();

        let params = Bytes::from(format!(
            "symbol={}&side={}&type=MARKET&quantity={}&positionSide={}&newClientOrderId={}",
            um_order.symbol,
            close_side_str,
            format_quantity(close_qty),
            position_side_str,
            um_close_id
        ));

        let request = BinanceNewUMOrderRequest::create(now, um_close_id, params);
        self.order_tx
            .send(request.to_bytes())
            .map_err(|e| format!("{}: 推送 UM 平仓失败: {}", Self::strategy_name(), e))?;

        {
            let mut manager = self.order_manager.borrow_mut();
            let mut close_order = Order::new(
                um_close_id,
                OrderType::Market,
                um_order.symbol.clone(),
                close_side_enum,
                close_qty,
                0.0,
            );
            close_order.set_submit_time(now);
            manager.insert(close_order);
        }

        self.close_um_hedge_order_id = um_close_id;
        self.um_close_signal_sent = true;
        self.close_margin_timeout_us = None;

        info!(
            "{}: strategy_id={} 提交 UM 市价平仓 symbol={} qty={:.6} order_id={}",
            Self::strategy_name(),
            self.strategy_id,
            um_order.symbol,
            close_qty,
            um_close_id
        );

        Ok(())
    }

    fn um_order_label(&self, client_order_id: i64) -> &'static str {
        if client_order_id == self.um_hedge_order_id {
            "UM 对冲单"
        } else if client_order_id == self.close_um_hedge_order_id {
            "UM 平仓单"
        } else {
            "UM 未识别订单"
        }
    }

    fn apply_um_order_update(
        order: &mut Order,
        event: &OrderTradeUpdateMsg,
    ) -> UmOrderUpdateOutcome {
        match event.execution_type.as_str() {
            "NEW" | "TRADE" => Self::apply_um_fill_state(order, event),
            "EXPIRED" => {
                warn!(
                    "UM Order Execution Type=EXPIRED，保持在 Expired 状态，raw_event={:?}",
                    event
                );
                UmOrderUpdateOutcome::Expired
            }
            "CANCELED" => {
                panic!(
                    "UM Order Execution Type=CANCELED，不符合预期，raw_event={:?}",
                    event
                );
            }
            "CALCULATED" => {
                panic!(
                    "UM Order Execution Type=CALCULATED(强平)，raw_event={:?}",
                    event
                );
            }
            other => {
                warn!(
                    "UM Order Execution Type={} 未识别，raw_event={:?}，忽略",
                    other, event
                );
                UmOrderUpdateOutcome::Ignored
            }
        }
    }

    /// 打印该 symbol 下当前挂着的限价单（非终态）为三线表
    fn log_pending_limit_orders(symbol: &str, order_manager: &OrderManager) {
        let now_us = get_timestamp_us();
        let mut rows: Vec<Vec<String>> = Vec::new();
        for id in order_manager.get_all_ids() {
            if let Some(ord) = order_manager.get(id) {
                if !ord.symbol.eq_ignore_ascii_case(symbol) {
                    continue;
                }
                if !ord.order_type.is_limit() {
                    continue;
                }
                if ord.status.is_terminal() {
                    continue;
                }
                let age_ms = if ord.submit_time > 0 {
                    (now_us.saturating_sub(ord.submit_time) / 1000) as i64
                } else {
                    0
                };
                rows.push(vec![
                    ord.order_id.to_string(),
                    ord.side.as_str().to_string(),
                    Self::format_decimal(ord.quantity),
                    Self::format_decimal(ord.price),
                    ord.status.as_str().to_string(),
                    age_ms.to_string(),
                ]);
            }
        }
        // 稳定排序：按 age_ms 降序，其次按 order_id 升序
        rows.sort_by(|a, b| {
            let age_a: i64 = a[5].parse().unwrap_or(0);
            let age_b: i64 = b[5].parse().unwrap_or(0);
            match age_b.cmp(&age_a) {
                Ordering::Equal => a[0].cmp(&b[0]),
                other => other,
            }
        });
        let table = Self::render_three_line_table(
            &["OrderId", "Side", "Qty", "Price", "Status", "Age(ms)"],
            &rows,
        );
        warn!(
            "{}: symbol={} 当前待成交限价单列表\n{}",
            BinSingleForwardArbStrategy::strategy_name(),
            symbol,
            table
        );
    }

    pub fn snapshot_struct(&self) -> BinSingleForwardArbSnapshot {
        BinSingleForwardArbSnapshot {
            strategy_id: self.strategy_id,
            symbol: self.symbol.clone(),
            create_time: self.create_time,
            margin_order_id: self.margin_order_id,
            um_hedge_order_id: self.um_hedge_order_id,
            close_margin_order_id: self.close_margin_order_id,
            close_um_hedge_order_id: self.close_um_hedge_order_id,
            open_timeout_us: self.open_timeout_us,
            close_margin_timeout_us: self.close_margin_timeout_us,
            um_close_signal_sent: self.um_close_signal_sent,
            order_seq: self.order_seq,
        }
    }

    pub fn from_snapshot(
        snap: &BinSingleForwardArbSnapshot,
        order_manager: Rc<RefCell<OrderManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        order_tx: UnboundedSender<Bytes>,
        max_symbol_exposure_ratio: f64,
        max_total_exposure_ratio: f64,
        min_qty_table: std::rc::Rc<MinQtyTable>,
    ) -> Self {
        let mut s = Self::new(
            snap.strategy_id,
            snap.create_time,
            snap.symbol.clone(),
            order_manager,
            exposure_manager,
            order_tx,
            max_symbol_exposure_ratio,
            max_total_exposure_ratio,
            min_qty_table,
        );
        s.margin_order_id = snap.margin_order_id;
        s.um_hedge_order_id = snap.um_hedge_order_id;
        s.close_margin_order_id = snap.close_margin_order_id;
        s.close_um_hedge_order_id = snap.close_um_hedge_order_id;
        s.open_timeout_us = snap.open_timeout_us;
        s.close_margin_timeout_us = snap.close_margin_timeout_us;
        s.um_close_signal_sent = snap.um_close_signal_sent;
        s.order_seq = snap.order_seq;
        s
    }
}
impl Drop for BinSingleForwardArbStrategy {
    fn drop(&mut self) {
        // 在回收前输出订单生命周期汇总（开仓/对冲/平仓）
        let now = get_timestamp_us();
        let mgr_ro = self.order_manager.borrow();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let pairs = [
            ("MarginOpen", self.margin_order_id),
            ("UMHedge", self.um_hedge_order_id),
            ("MarginClose", self.close_margin_order_id),
            ("UMClose", self.close_um_hedge_order_id),
        ];
        for (kind, id) in pairs {
            if id == 0 { continue; }
            if let Some(o) = mgr_ro.get(id) {
                let age_ms = if o.submit_time > 0 { ((now - o.submit_time)/1000) as i64 } else { 0 };
                rows.push(vec![
                    id.to_string(),
                    kind.to_string(),
                    o.side.as_str().to_string(),
                    Self::format_decimal(o.quantity),
                    Self::format_decimal(o.price),
                    o.status.as_str().to_string(),
                    age_ms.to_string(),
                ]);
            } else {
                rows.push(vec![
                    id.to_string(),
                    kind.to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "Removed".to_string(),
                    "0".to_string(),
                ]);
            }
        }
        drop(mgr_ro);

        if !rows.is_empty() {
            let table = Self::render_three_line_table(
                &["OrderId", "Kind", "Side", "Qty", "Price", "Status", "Age(ms)"],
                &rows,
            );
            info!(
                "{}: strategy_id={} 订单生命周期汇总\n{}",
                Self::strategy_name(),
                self.strategy_id,
                table
            );
        }

        // 回收本策略相关订单
        let mut mgr = self.order_manager.borrow_mut();
        for id in [
            self.margin_order_id,
            self.um_hedge_order_id,
            self.close_margin_order_id,
            self.close_um_hedge_order_id,
        ] {
            if id != 0 {
                let _ = mgr.remove(id);
            }
        }
        debug!(
            "{}: strategy_id={} 生命周期结束，相关订单已回收",
            Self::strategy_name(),
            self.strategy_id
        );
    }
}

impl BinSingleForwardArbStrategy {
    fn format_decimal(value: f64) -> String {
        if value == 0.0 {
            return "0".to_string();
        }
        let mut s = format!("{:.6}", value);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        if s.is_empty() { "0".to_string() } else { s }
    }

    fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
        let widths = Self::compute_widths(headers, rows);
        let mut out = String::new();
        out.push_str(&Self::build_separator(&widths, '-'));
        out.push('\n');
        out.push_str(&Self::build_row(
            headers
                .iter()
                .map(|h| h.to_string())
                .collect::<Vec<String>>(),
            &widths,
        ));
        out.push('\n');
        out.push_str(&Self::build_separator(&widths, '='));
        if rows.is_empty() {
            out.push('\n');
            out.push_str(&Self::build_separator(&widths, '-'));
            return out;
        }
        for row in rows {
            out.push('\n');
            out.push_str(&Self::build_row(row.clone(), &widths));
        }
        out.push('\n');
        out.push_str(&Self::build_separator(&widths, '-'));
        out
    }

    fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
        let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
        for row in rows {
            for (idx, cell) in row.iter().enumerate() {
                if idx >= widths.len() {
                    continue;
                }
                widths[idx] = widths[idx].max(cell.len());
            }
        }
        widths
    }

    fn build_separator(widths: &[usize], fill: char) -> String {
        let mut line = String::new();
        line.push('+');
        for width in widths {
            line.push_str(&fill.to_string().repeat(width + 2));
            line.push('+');
        }
        line
    }

    fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
        let mut row = String::new();
        row.push('|');
        for (cell, width) in cells.iter().zip(widths.iter()) {
            row.push(' ');
            row.push_str(&format!("{:<width$}", cell, width = *width));
            row.push(' ');
            row.push('|');
        }
        row
    }
}

impl BinSingleForwardArbStrategy {
    fn apply_um_fill_state(order: &mut Order, event: &OrderTradeUpdateMsg) -> UmOrderUpdateOutcome {
        match event.order_status.as_str() {
            "NEW" => {
                order.update_status(OrderExecutionStatus::Create);
                order.set_create_time(event.event_time);
                UmOrderUpdateOutcome::Created
            }
            "PARTIALLY_FILLED" => {
                order.set_create_time(event.event_time);
                order.update_cumulative_filled_quantity(event.cumulative_filled_quantity);
                UmOrderUpdateOutcome::PartiallyFilled(event.cumulative_filled_quantity)
            }
            "FILLED" => {
                order.update_status(OrderExecutionStatus::Filled);
                order.set_filled_time(event.event_time);
                order.update_cumulative_filled_quantity(event.cumulative_filled_quantity);

                if order.hedged_quantily.abs() > 1e-8 {
                    panic!(
                        "UM Order Status=FILLED 但仍有剩余数量，order={:?} remaining={}",
                        order, order.hedged_quantily
                    );
                }

                UmOrderUpdateOutcome::Filled
            }
            "EXPIRED_IN_MATCH" => UmOrderUpdateOutcome::Ignored,
            "CANCELED" => {
                panic!("UM Order Status=CANCELED，不符合预期，order={:?}", order);
            }
            "EXPIRED" => {
                warn!("UM Order Status=EXPIRED，order={:?}", order);
                UmOrderUpdateOutcome::Expired
            }
            other => {
                panic!("UM Order Status={} 未识别，order={:?}", other, order);
            }
        }
    }

    fn trigger_um_close_signal(&mut self) -> Result<(), String> {
        if self.um_close_signal_sent {
            debug!(
                "{}: strategy_id={} UM 平仓信号已发出，忽略重复触发",
                Self::strategy_name(),
                self.strategy_id
            );
            return Ok(());
        }

        let um_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(self.um_hedge_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: strategy_id={} 未找到待平仓的 UM 订单 id={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        self.um_hedge_order_id
                    )
                })?
                .clone()
        };

        let qty = um_order.quantity;
        self.close_margin_timeout_us = None;

        let ctx = BinSingleForwardArbCloseUmCtx {
            um_symbol: um_order.symbol.clone(),
            exp_time: get_timestamp_us(),
        };

        if let Some(tx) = &self.signal_tx {
            let signal = TradeSignal::create(
                SignalType::BinSingleForwardArbCloseUm,
                ctx.exp_time,
                0.0,
                ctx.to_bytes(),
            );

            if let Err(err) = tx.send(signal.to_bytes()) {
                warn!(
                    "{}: strategy_id={} 派发 UM 平仓信号失败: {}，改为直接市价平仓",
                    Self::strategy_name(),
                    self.strategy_id,
                    err
                );
                self.close_um_with_market(&ctx)?;
                self.um_close_signal_sent = true;
            } else {
                self.um_close_signal_sent = true;
                info!(
                    "{}: strategy_id={} 已派发 UM 平仓信号 symbol={} qty={:.6}",
                    Self::strategy_name(),
                    self.strategy_id,
                    ctx.um_symbol,
                    qty
                );
            }
        } else {
            warn!(
                "{}: strategy_id={} 未配置 signal_tx，直接执行 UM 市价平仓",
                Self::strategy_name(),
                self.strategy_id
            );
            self.close_um_with_market(&ctx)?;
        }

        Ok(())
    }

    fn submit_margin_cancel(&self, symbol: &str, order_id: i64) -> Result<(), String> {
        let now = get_timestamp_us();
        // 使用 origClientOrderId 以客户端订单ID撤单；当前未保存交易所 orderId
        let params = Bytes::from(format!(
            "symbol={}&origClientOrderId={}",
            symbol, order_id
        ));
        let request = BinanceCancelMarginOrderRequest::create(now, order_id, params);

        self.order_tx
            .send(request.to_bytes())
            .map_err(|e| format!("{}: 发送 margin 撤单失败: {}", Self::strategy_name(), e))
    }

    fn next_order_id(&mut self) -> i64 {
        let seq = self.order_seq;
        self.order_seq = self.order_seq.wrapping_add(1);
        Self::compose_order_id(self.strategy_id, seq)
    }

    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }
}

fn format_decimal(value: f64) -> String {
    let mut s = format!("{:.8}", value);
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

fn format_quantity(quantity: f64) -> String {
    format_decimal(quantity)
}

fn format_price(price: f64) -> String {
    format_decimal(price)
}

fn align_price_floor(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return value;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((value * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((value / tick) + 1e-9).floor();
    scaled * tick
}

fn align_price_ceil(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return value;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((value * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((value / tick) - 1e-9).ceil();
    scaled * tick
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}

impl Strategy for BinSingleForwardArbStrategy {
    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        ((order_id >> 32) as i32) == self.strategy_id
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
                SignalType::BinSingleForwardArbCloseMargin => {
                    match BinSingleForwardArbCloseMarginCtx::from_bytes(signal.context.clone()) {
                        Ok(ctx) => {
                            self.um_close_signal_sent = false;
                            match self.close_margin_with_limit(&ctx) {
                                Ok(()) => debug!(
                                    "{}: strategy_id={} 成功触发 margin 限价平仓",
                                    Self::strategy_name(),
                                    self.strategy_id
                                ),
                                Err(err) => warn!(
                                    "{}: strategy_id={} margin 平仓失败: {}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    err
                                ),
                            }
                        }
                        Err(err) => {
                            warn!(
                                "{}: strategy_id={} 解析 margin 平仓上下文失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        }
                    }
                }
                SignalType::BinSingleForwardArbCloseUm => {
                    match BinSingleForwardArbCloseUmCtx::from_bytes(signal.context.clone()) {
                        Ok(ctx) => match self.close_um_with_market(&ctx) {
                            Ok(()) => debug!(
                                "{}: strategy_id={} 成功触发 UM 市价平仓",
                                Self::strategy_name(),
                                self.strategy_id
                            ),
                            Err(err) => warn!(
                                "{}: strategy_id={} UM 平仓失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            ),
                        },
                        Err(err) => {
                            warn!(
                                "{}: strategy_id={} 解析 UM 平仓上下文失败: {}",
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
                    let mut final_status: Option<OrderExecutionStatus> = None;

                    let mut manager = self.order_manager.borrow_mut();
                    if !manager.update(outcome.client_order_id, |order| {
                        if success {
                            order.update_status(OrderExecutionStatus::Create);
                            order.set_ack_time(now);
                        } else {
                            order.update_status(OrderExecutionStatus::Rejected);
                            order.set_end_time(now);
                        }
                        final_status = Some(order.status);
                    }) {
                        warn!(
                            "{}: strategy_id={} 未找到 client_order_id={} 对应订单",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.client_order_id
                        );
                    } else if outcome.req_type == TradeRequestType::BinanceNewMarginOrder {
                        if let Some(status) = final_status {
                            if status.is_terminal() && status != OrderExecutionStatus::Filled {
                                self.margin_order_id = 0;
                                self.open_timeout_us = None;
                                warn!(
                                    "{}: strategy_id={} margin 开仓回执终止 status={} body={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    status.as_str(),
                                    outcome.body
                                );
                            }
                        }
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
                TradeRequestType::BinanceCancelMarginOrder => {
                    let success = (200..300).contains(&outcome.status);
                    if success {
                        let now = get_timestamp_us();
                        let mut manager = self.order_manager.borrow_mut();
                        let mut existed = false;
                        if manager.update(outcome.client_order_id, |order| {
                            order.update_status(OrderExecutionStatus::Cancelled);
                            order.set_end_time(now);
                        }) {
                            existed = true;
                            info!(
                                "{}: strategy_id={} margin 撤单成功 client_order_id={} status={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                outcome.client_order_id,
                                outcome.status
                            );
                        } else {
                            debug!(
                                "{}: strategy_id={} margin 撤单响应但未找到订单 id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                outcome.client_order_id
                            );
                        }
                        // 删除订单，释放挂单计数
                        if existed {
                            let _ = manager.remove(outcome.client_order_id);
                        }

                        if self.margin_order_id == outcome.client_order_id {
                            self.margin_order_id = 0;
                            self.open_timeout_us = None;
                        }
                    } else {
                        warn!(
                            "{}: strategy_id={} margin 撤单失败 status={} body={}",
                            Self::strategy_name(),
                            self.strategy_id,
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

    fn handle_binance_margin_order_update(&mut self, report: &ExecutionReportMsg) {
        let order_id = report.client_order_id;
        if !self.is_strategy_order(order_id) {
            return;
        }

        let status_str = report.order_status.as_str();
        let execution_type = report.execution_type.as_str();

        let mut remove_after_update = false;
        {
            let mut manager = self.order_manager.borrow_mut();
            if !manager.update(order_id, |order| match status_str {
                "NEW" => {
                    order.update_status(OrderExecutionStatus::Create);
                    order.set_create_time(report.event_time);
                }
                "PARTIALLY_FILLED" => {
                    order.update_cumulative_filled_quantity(report.cumulative_filled_quantity);
                    order.set_create_time(report.event_time);
                }
                "FILLED" => {
                    order.update_status(OrderExecutionStatus::Filled);
                    order.set_filled_time(report.event_time);
                    order.update_cumulative_filled_quantity(report.cumulative_filled_quantity);
                    // 成功路径不立即移除，等待配对订单也 FILLED 后成对移除
                }
                "CANCELED" | "EXPIRED" => {
                    order.update_status(OrderExecutionStatus::Cancelled);
                    order.set_end_time(report.event_time);
                    remove_after_update = true;
                }
                "REJECTED" | "TRADE_PREVENTION" => {
                    order.update_status(OrderExecutionStatus::Rejected);
                    order.set_end_time(report.event_time);
                    remove_after_update = true;
                }
                _ => {}
            }) {
                warn!(
                    "{}: strategy_id={} execution_report 未找到订单 id={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_id
                );
            }
        }

        if remove_after_update {
            self.order_manager.borrow_mut().remove(order_id);
        }

        if order_id == self.margin_order_id {
            debug!(
                "{}: strategy_id={} margin execution report status={} execution_type={}",
                Self::strategy_name(),
                self.strategy_id,
                status_str,
                execution_type
            );

            match status_str {
                "FILLED" => {
                    self.open_timeout_us = None;
                    debug!(
                        "{}: strategy_id={} margin 开仓单 FILLED，保留订单供生命周期管理",
                        Self::strategy_name(),
                        self.strategy_id
                    );
                    // 触发对冲信号，由策略消费并创建 UM 对冲市价单
                    let ctx = BinSingleForwardArbHedgeCtx { spot_order_id: order_id };
                    if let Some(tx) = &self.signal_tx {
                        let sig = TradeSignal::create(
                            SignalType::BinSingleForwardArbHedge,
                            get_timestamp_us(),
                            0.0,
                            ctx.to_bytes(),
                        );
                        if let Err(err) = tx.send(sig.to_bytes()) {
                            warn!(
                                "{}: strategy_id={} 发送 Hedge 信号失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        } else {
                            debug!(
                                "{}: strategy_id={} 已派发 Hedge 信号 spot_order_id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                order_id
                            );
                        }
                    } else {
                        warn!(
                            "{}: strategy_id={} signal_tx 未配置，无法派发 Hedge 信号",
                            Self::strategy_name(),
                            self.strategy_id
                        );
                    }
                }
                "CANCELED" | "EXPIRED" | "REJECTED" | "TRADE_PREVENTION" => {
                    self.open_timeout_us = None;
                    self.margin_order_id = 0;
                }
                _ => {}
            }
            return;
        }

        if order_id == self.close_margin_order_id {
            debug!(
                "{}: strategy_id={} margin close execution report status={} execution_type={}",
                Self::strategy_name(),
                self.strategy_id,
                status_str,
                execution_type
            );

            match status_str {
                "FILLED" => {
                    self.close_margin_timeout_us = None;
                    // 发出 UM 平仓信号，待整个生命周期结束后统一回收
                    if let Err(err) = self.trigger_um_close_signal() {
                        warn!(
                            "{}: strategy_id={} 派发 UM 平仓流程失败: {}",
                            Self::strategy_name(),
                            self.strategy_id,
                            err
                        );
                    }
                    debug!(
                        "{}: strategy_id={} margin 平仓单 FILLED，保留订单供生命周期管理",
                        Self::strategy_name(),
                        self.strategy_id
                    );
                }
                "CANCELED" | "EXPIRED" | "REJECTED" | "TRADE_PREVENTION" => {
                    warn!(
                        "{}: strategy_id={} margin 平仓未完成，状态={} execution_type={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        status_str,
                        execution_type
                    );
                    self.close_margin_timeout_us = None;
                    self.um_close_signal_sent = false;
                }
                _ => {}
            }
            return;
        }

        debug!(
            "{}: strategy_id={} margin execution report (其他订单) status={} execution_type={}",
            Self::strategy_name(),
            self.strategy_id,
            status_str,
            execution_type
        );
    }
    // handle_binance_futures_order_update
    // 1、处理 UM 对冲单的挂单 / 成交状态
    // 2、处理 UM 平仓单的挂单 / 成交状态
    fn handle_binance_futures_order_update(&mut self, event: &OrderTradeUpdateMsg) {
        let client_order_id = event.client_order_id;
        if !self.is_strategy_order(client_order_id) {
            return;
        }

        let order_label = self.um_order_label(client_order_id);
        debug!(
            "{}: strategy_id={} 收到 {} UM Order Execution Type={} UM Order Status={}",
            Self::strategy_name(),
            self.strategy_id,
            order_label,
            event.execution_type,
            event.order_status
        );

        if event.business_unit != "UM" {
            error!(
                "{}: strategy_id={} {} business_unit 异常={}",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                event.business_unit
            );
        }

        if event.order_type != "MARKET" {
            error!(
                "{}: strategy_id={} {} order_type 异常={}",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                event.order_type
            );
        }

        let mut manager = self.order_manager.borrow_mut();
        let mut outcome = UmOrderUpdateOutcome::Ignored;

        if !manager.update(client_order_id, |order| {
            outcome = Self::apply_um_order_update(order, event);
        }) {
            warn!(
                "{}: strategy_id={} 未找到 {} client_order_id={}，保留激活状态",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                client_order_id
            );
            return;
        }

        drop(manager);

        match outcome {
            UmOrderUpdateOutcome::Created => {
                debug!(
                    "{}: strategy_id={} {} 已确认，等待成交，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::PartiallyFilled(cumulative) => {
                info!(
                    "{}: strategy_id={} {} 部分成交累计数量={:.6}，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    cumulative,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Filled => {
                info!(
                    "{}: strategy_id={} {} 已全部成交，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Expired => {
                warn!(
                    "{}: strategy_id={} {} 收到 EXPIRED 状态，请核实，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Ignored => {
                debug!(
                    "{}: strategy_id={} {} 状态无需处理，UM Order Execution Type={} UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.execution_type,
                    event.order_status
                );
            }
        }

        // 成功路径不立即移除，保留订单直到策略生命周期结束
    }

    fn hanle_period_clock(&mut self, current_tp: i64) {
        // 周期性检查：开仓限价/市价单是否长时间未成交，需要撤单
        if self.margin_order_id == 0 {
            warn!(
                "{}: strategy_id={} 当前无 margin 开仓单，策略将等待回收",
                Self::strategy_name(),
                self.strategy_id
            );
            self.open_timeout_us = None;
        } else {
            let open_order = {
                let manager = self.order_manager.borrow();
                manager.get(self.margin_order_id)
            };

            match open_order {
                Some(order) => {
                    if order.status.is_terminal() {
                        self.open_timeout_us = None;
                    } else if let (Some(timeout_us), submit_time) =
                        (self.open_timeout_us, order.submit_time)
                    {
                        if submit_time > 0 && current_tp.saturating_sub(submit_time) >= timeout_us {
                            info!(
                                "{}: strategy_id={} margin 开仓单超时，尝试撤单 order_id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                order.order_id
                            );

                            if let Err(err) =
                                self.submit_margin_cancel(&order.symbol, order.order_id)
                            {
                                warn!(
                                    "{}: strategy_id={} margin 开仓撤单失败: {}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    err
                                );
                            } else {
                                let mut manager = self.order_manager.borrow_mut();
                                if !manager.update(order.order_id, |o| {
                                    o.update_status(OrderExecutionStatus::Commit);
                                    o.set_end_time(current_tp);
                                }) {
                                    warn!(
                                        "{}: strategy_id={} 撤单后更新状态失败 id={}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        order.order_id
                                    );
                                }
                                self.open_timeout_us = None;
                            }
                        }
                    }
                }
                None => {
                    warn!(
                        "{}: strategy_id={} 未找到 margin 开仓单 id={}，清除本地状态",
                        Self::strategy_name(),
                        self.strategy_id,
                        self.margin_order_id
                    );
                    self.margin_order_id = 0;
                    self.open_timeout_us = None;
                }
            }
        }

        // 周期性检查：限价平仓单是否超时
        if self.close_margin_order_id == 0 {
            self.close_margin_timeout_us = None;
        } else {
            let close_order = {
                let manager = self.order_manager.borrow();
                manager.get(self.close_margin_order_id)
            };

            match close_order {
                Some(order) => {
                    if order.status.is_terminal() {
                        self.close_margin_timeout_us = None;
                    } else if let (Some(timeout_us), submit_time) =
                        (self.close_margin_timeout_us, order.submit_time)
                    {
                        if submit_time > 0 && current_tp.saturating_sub(submit_time) >= timeout_us {
                            info!(
                                "{}: strategy_id={} margin 平仓单超时，尝试撤单 order_id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                order.order_id
                            );

                            if let Err(err) =
                                self.submit_margin_cancel(&order.symbol, order.order_id)
                            {
                                warn!(
                                    "{}: strategy_id={} margin 平仓撤单失败: {}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    err
                                );
                            } else {
                                let mut manager = self.order_manager.borrow_mut();
                                if !manager.update(order.order_id, |o| {
                                    o.update_status(OrderExecutionStatus::Commit);
                                    o.set_end_time(current_tp);
                                }) {
                                    warn!(
                                        "{}: strategy_id={} 更新平仓订单状态失败 id={}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        order.order_id
                                    );
                                }
                                self.close_margin_timeout_us = None;
                            }
                        }
                    }
                }
                None => {
                    warn!(
                        "{}: strategy_id={} 未找到 margin 平仓单 id={}，清除本地状态",
                        Self::strategy_name(),
                        self.strategy_id,
                        self.close_margin_order_id
                    );
                    self.close_margin_order_id = 0;
                    self.close_margin_timeout_us = None;
                }
            }
        }
    }

    fn is_active(&self) -> bool {
        //判断订单是否完全成交，按照执行顺序检查，避免冗余
        let manager = self.order_manager.borrow();
        if self.margin_order_id == 0 {
            warn!(
                "{}: strategy_id={} 缺少 margin 开仓单，返回非活跃",
                Self::strategy_name(),
                self.strategy_id
            );
            return false;
        }

        let Some(margin_order) = manager.get(self.margin_order_id) else {
            warn!(
                "{}: strategy_id={} 未找到 margin 开仓单 id={}，返回非活跃",
                Self::strategy_name(),
                self.strategy_id,
                self.margin_order_id
            );
            return false;
        };

        if margin_order.status == OrderExecutionStatus::Filled {
            // proceed
        } else if margin_order.status.is_terminal() {
            warn!(
                "{}: strategy_id={} margin 开仓单终结 status={}，策略退出",
                Self::strategy_name(),
                self.strategy_id,
                margin_order.status.as_str()
            );
            return false;
        } else {
            debug!(
                "{}: strategy_id={} margin 开仓单状态={}，等待成交",
                Self::strategy_name(),
                self.strategy_id,
                margin_order.status.as_str()
            );
            return true;
        }

        if self.um_hedge_order_id == 0 {
            debug!(
                "{}: strategy_id={} UM 对冲单未创建，策略仍在执行",
                Self::strategy_name(),
                self.strategy_id
            );
            return true;
        }

        let Some(um_order) = manager.get(self.um_hedge_order_id) else {
            info!(
                "{}: strategy_id={} 未找到 UM 对冲单 id={}，待平仓，保持激活状态",
                Self::strategy_name(),
                self.strategy_id,
                self.um_hedge_order_id
            );
            return true;
        };

        if um_order.status != OrderExecutionStatus::Filled {
            debug!(
                "{}: strategy_id={} UM 对冲单状态={}，等待成交",
                Self::strategy_name(),
                self.strategy_id,
                um_order.status.as_str()
            );
            return true;
        }

        if self.close_margin_order_id == 0 {
            debug!(
                "{}: strategy_id={} margin 平仓单未触发，策略仍在执行",
                Self::strategy_name(),
                self.strategy_id
            );
            return true;
        }

        let Some(close_margin_order) = manager.get(self.close_margin_order_id) else {
            info!(
                "{}: strategy_id={} 未找到 margin 平仓单 id={}，待平仓，保持激活状态",
                Self::strategy_name(),
                self.strategy_id,
                self.close_margin_order_id
            );
            return true;
        };

        if close_margin_order.status != OrderExecutionStatus::Filled {
            debug!(
                "{}: strategy_id={} margin 平仓单状态={}，等待成交",
                Self::strategy_name(),
                self.strategy_id,
                close_margin_order.status.as_str()
            );
            return true;
        }

        if self.close_um_hedge_order_id == 0 {
            debug!(
                "{}: strategy_id={} UM 平仓单未触发，策略仍在执行",
                Self::strategy_name(),
                self.strategy_id
            );
            return true;
        }

        let Some(close_um_order) = manager.get(self.close_um_hedge_order_id) else {
            info!(
                "{}: strategy_id={} 未找到 UM 平仓单 id={}，待平仓，保持激活状态",
                Self::strategy_name(),
                self.strategy_id,
                self.close_um_hedge_order_id
            );
            return true;
        };

        if close_um_order.status != OrderExecutionStatus::Filled {
            debug!(
                "{}: strategy_id={} UM 平仓单状态={}，等待成交",
                Self::strategy_name(),
                self.strategy_id,
                close_um_order.status.as_str()
            );
            return true;
        }

        info!(
            "{}: strategy_id={} 所有订单已全部成交，策略进入完成状态",
            Self::strategy_name(),
            self.strategy_id
        );

        false
    }

    fn snapshot(&self) -> Option<StrategySnapshot<'_>> {
        let snap = self.snapshot_struct();
        let bytes = match snap.to_bytes() {
            Ok(b) => b,
            Err(_) => return None,
        };
        Some(StrategySnapshot { type_name: "BinSingleForwardArbStrategy", payload: bytes })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinSingleForwardArbSnapshot {
    pub strategy_id: i32,
    pub symbol: String,
    pub create_time: i64,
    pub margin_order_id: i64,
    pub um_hedge_order_id: i64,
    pub close_margin_order_id: i64,
    pub close_um_hedge_order_id: i64,
    pub open_timeout_us: Option<i64>,
    pub close_margin_timeout_us: Option<i64>,
    pub um_close_signal_sent: bool,
    pub order_seq: u32,
}

impl BinSingleForwardArbSnapshot {
    pub fn to_bytes(&self) -> Result<Bytes, bincode::Error> {
        let v = bincode::serialize(self)?;
        Ok(Bytes::from(v))
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}
