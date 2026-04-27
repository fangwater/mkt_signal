use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderExecutionStatus, Side};
use crate::pre_trade::{QueryEngHub, TradeEngHub};
use crate::signal::common::{OrderStatus, TradingVenue};
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{
    ArbOrphanHandoff, ArbOrphanResidualHandoff, ArbOrphanUniformCtx, ForceCloseControl, Strategy,
};
use crate::strategy::net_qty_queue::NetQtyQueue;
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    UniformAmountSource,
};
use log::{debug, info, warn};
use std::any::Any;
use std::collections::HashMap;

const ARB_ORPHAN_EPS: f64 = 1e-12;
const ARB_ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const ARB_ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;
const ARB_ORPHAN_DEFAULT_HEDGE_RESIDUAL_LOWER_USDT: f64 = 0.0;
const ARB_ORPHAN_DEFAULT_HEDGE_RESIDUAL_UPPER_USDT: f64 = f64::INFINITY;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbOrphanLeg {
    Open,
    Hedge,
}

#[derive(Debug, Clone)]
pub struct ArbOrphanSnapshot {
    pub symbol: String,
    pub net_qty: f64,
    pub hedge_residual_qty: QuantizedValue,
    pub hedge_residual_lower_usdt: f64,
    pub hedge_residual_upper_usdt: f64,
    pub weighted_inventory_price: f64,
    pub tracked_orders: usize,
}

pub struct ArbOrphanStrategy {
    strategy_id: i32,
    symbol: String,
    active: bool,
    order_legs: HashMap<i64, ArbOrphanLeg>,
    query_states: HashMap<i64, ArbOrphanQueryState>,
    uniform_contexts: HashMap<i64, ArbOrphanUniformCtx>,
    net_qty_queue: NetQtyQueue,
    net_qty: f64,
    /// hedge 残差累计量，base 口径，带方向。
    ///
    /// 语义：已经确认产生 open exposure，但当前尚未成功形成 hedge 订单处理的待对冲数量。
    /// 正数表示需要 Sell hedge，负数表示需要 Buy hedge。
    /// 新增残差直接按符号累加，天然支持正负轧差。
    ///
    /// 注意：这不是已成交 net，不进入 net_qty_queue。
    /// net_qty_queue 只记录真实成交。
    hedge_residual_qty: QuantizedValue,
    /// residual 触发 hedge 的下限，USDT 口径。
    /// abs(residual) * mark_price 低于该值时不发 hedge，继续累积。
    hedge_residual_lower_usdt: f64,
    /// residual 单次 hedge 的上限，USDT 口径。
    /// 超过该值时只取一部分，剩余 residual 留在 orphan 内继续等待下一笔。
    hedge_residual_upper_usdt: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArbOrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
}

impl ArbOrphanStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            active: true,
            order_legs: HashMap::new(),
            query_states: HashMap::new(),
            uniform_contexts: HashMap::new(),
            net_qty_queue: NetQtyQueue::new(),
            net_qty: 0.0,
            hedge_residual_qty: QuantizedValue::zero(),
            hedge_residual_lower_usdt: ARB_ORPHAN_DEFAULT_HEDGE_RESIDUAL_LOWER_USDT,
            hedge_residual_upper_usdt: ARB_ORPHAN_DEFAULT_HEDGE_RESIDUAL_UPPER_USDT,
        }
    }

    fn initial_query_state() -> ArbOrphanQueryState {
        ArbOrphanQueryState {
            query_count: 0,
            ticks_until_next_query: 0,
        }
    }

    fn next_query_ticks(query_count: u8) -> u32 {
        let multiplier = 1_u32
            .checked_shl(query_count.min(31) as u32)
            .unwrap_or(u32::MAX);
        ARB_ORPHAN_QUERY_BASE_TICKS
            .saturating_mul(multiplier)
            .min(ARB_ORPHAN_QUERY_MAX_TICKS)
    }

    fn should_cancel_order(status: OrderExecutionStatus) -> bool {
        !status.is_terminal() && status != OrderExecutionStatus::Commit
    }

    fn ensure_query_state(&mut self, client_order_id: i64) {
        self.query_states
            .entry(client_order_id)
            .or_insert_with(Self::initial_query_state);
    }

    pub fn adopt_order_id(&mut self, handoff: &ArbOrphanHandoff) -> bool {
        if handoff.client_order_id <= 0 {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(handoff.client_order_id) else {
            return false;
        };
        let symbol = normalize_symbol_for_internal(&order.symbol);
        let venue = order.venue;
        let status = order.status;
        if symbol != self.symbol {
            return false;
        }
        drop(order);

        self.order_legs.insert(handoff.client_order_id, handoff.leg);
        self.ensure_query_state(handoff.client_order_id);
        if let Some(ctx) = handoff.uniform_ctx.clone() {
            self.uniform_contexts.insert(handoff.client_order_id, ctx);
        }
        info!(
            "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} adopted order symbol={} client_order_id={} venue={:?} status={:?} leg={:?} source_strategy_id={}",
            self.strategy_id,
            symbol,
            handoff.client_order_id,
            venue,
            status,
            handoff.leg,
            handoff.source_strategy_id
        );
        true
    }

    pub fn track_open_order_id(&mut self, client_order_id: i64) {
        self.track_order_id(client_order_id, ArbOrphanLeg::Open);
    }

    pub fn track_hedge_order_id(&mut self, client_order_id: i64) {
        self.track_order_id(client_order_id, ArbOrphanLeg::Hedge);
    }

    pub fn track_order_id(&mut self, client_order_id: i64, leg: ArbOrphanLeg) {
        if client_order_id <= 0 {
            return;
        }
        self.order_legs.insert(client_order_id, leg);
        self.ensure_query_state(client_order_id);
        debug!(
            "ArbOrphanStrategy: strategy_id={} track order client_order_id={} leg={:?}",
            self.strategy_id, client_order_id, leg
        );
    }

    pub fn snapshot(&self) -> ArbOrphanSnapshot {
        ArbOrphanSnapshot {
            symbol: self.symbol.clone(),
            net_qty: self.net_qty,
            hedge_residual_qty: self.hedge_residual_qty,
            hedge_residual_lower_usdt: self.hedge_residual_lower_usdt,
            hedge_residual_upper_usdt: self.hedge_residual_upper_usdt,
            weighted_inventory_price: self.weighted_inventory_price(),
            tracked_orders: self.order_legs.len(),
        }
    }

    pub fn weighted_inventory_price(&self) -> f64 {
        self.net_qty_queue.weighted_avg_price().unwrap_or(0.0)
    }

    pub fn net_qty(&self) -> f64 {
        self.net_qty
    }

    pub fn hedge_residual_qty(&self) -> QuantizedValue {
        self.hedge_residual_qty
    }

    pub fn set_hedge_residual_usdt_bounds(&mut self, lower_usdt: f64, upper_usdt: f64) {
        self.hedge_residual_lower_usdt = lower_usdt.max(0.0);
        self.hedge_residual_upper_usdt = if upper_usdt.is_finite() && upper_usdt > 0.0 {
            upper_usdt
        } else {
            f64::INFINITY
        };
    }

    fn signed_base_qty(side: Side, base_qty: f64) -> f64 {
        match side {
            Side::Buy => base_qty,
            Side::Sell => -base_qty,
        }
    }

    fn qv_sign(qv: QuantizedValue) -> i8 {
        let (tick_i64, _) = qv.get_tick_parts();
        let count = qv.get_count();
        if tick_i64 == 0 || count == 0 {
            0
        } else if (tick_i64 > 0) == (count > 0) {
            1
        } else {
            -1
        }
    }

    fn qv_is_zero(qv: QuantizedValue) -> bool {
        Self::qv_sign(qv) == 0 || qv.get_val().abs() <= ARB_ORPHAN_EPS
    }

    fn pow10_i128(exp: u32) -> Option<i128> {
        let mut value = 1_i128;
        for _ in 0..exp {
            value = value.checked_mul(10)?;
        }
        Some(value)
    }

    fn qv_units_at_exp(qv: QuantizedValue, target_exp: i32) -> Option<i128> {
        let (tick_i64, tick_exp) = qv.get_tick_parts();
        if tick_i64 == 0 || qv.get_count() == 0 {
            return Some(0);
        }
        if target_exp > tick_exp {
            return None;
        }
        let scale = u32::try_from(tick_exp - target_exp).ok()?;
        let factor = Self::pow10_i128(scale)?;
        (tick_i64 as i128)
            .checked_mul(factor)?
            .checked_mul(qv.get_count() as i128)
    }

    fn qv_from_units_at_exp(units: i128, exp: i32) -> Option<QuantizedValue> {
        if units == 0 {
            return Some(QuantizedValue::zero());
        }
        let count = i64::try_from(units).ok()?;
        Some(QuantizedValue::from_parts(1, exp, count))
    }

    fn qv_add_exact(lhs: QuantizedValue, rhs: QuantizedValue) -> Option<QuantizedValue> {
        if Self::qv_is_zero(lhs) {
            return Some(rhs);
        }
        if Self::qv_is_zero(rhs) {
            return Some(lhs);
        }
        let (_, lhs_exp) = lhs.get_tick_parts();
        let (_, rhs_exp) = rhs.get_tick_parts();
        let exp = lhs_exp.min(rhs_exp);
        let lhs_units = Self::qv_units_at_exp(lhs, exp)?;
        let rhs_units = Self::qv_units_at_exp(rhs, exp)?;
        Self::qv_from_units_at_exp(lhs_units.checked_add(rhs_units)?, exp)
    }

    fn qv_neg(qv: QuantizedValue) -> Option<QuantizedValue> {
        let (tick_i64, tick_exp) = qv.get_tick_parts();
        Some(QuantizedValue::from_parts(
            tick_i64,
            tick_exp,
            qv.get_count().checked_neg()?,
        ))
    }

    fn qv_sub_exact(lhs: QuantizedValue, rhs: QuantizedValue) -> Option<QuantizedValue> {
        Self::qv_add_exact(lhs, Self::qv_neg(rhs)?)
    }

    fn qv_with_sign(qv: QuantizedValue, sign: i8) -> Option<QuantizedValue> {
        if sign == 0 || Self::qv_is_zero(qv) {
            return Some(QuantizedValue::zero());
        }
        let (tick_i64, tick_exp) = qv.get_tick_parts();
        let tick_abs = i64::try_from((tick_i64 as i128).abs()).ok()?;
        let count_abs = (qv.get_count() as i128).abs();
        let signed_count = count_abs.checked_mul(sign as i128)?;
        Some(QuantizedValue::from_parts(
            tick_abs,
            tick_exp,
            i64::try_from(signed_count).ok()?,
        ))
    }

    fn signed_qv_for_side(side: Side, base_qty_qv: QuantizedValue) -> Option<QuantizedValue> {
        match side {
            Side::Buy => Self::qv_with_sign(base_qty_qv, 1),
            Side::Sell => Self::qv_with_sign(base_qty_qv, -1),
        }
    }

    fn hedge_unfilled_residual_qv(
        side: Side,
        unfilled_base_qty_qv: QuantizedValue,
    ) -> Option<QuantizedValue> {
        match side {
            // Sell hedge 未成交，说明还有正 exposure 等待对冲。
            Side::Sell => Self::qv_with_sign(unfilled_base_qty_qv, 1),
            // Buy hedge 未成交，说明还有负 exposure 等待对冲。
            Side::Buy => Self::qv_with_sign(unfilled_base_qty_qv, -1),
        }
    }

    fn min_qty_symbol_key(venue: TradingVenue, symbol: &str) -> String {
        match venue {
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
            }
            TradingVenue::GateMargin | TradingVenue::GateFutures => {
                symbol.to_uppercase().replace('_', "").replace('-', "")
            }
            _ => symbol.to_uppercase(),
        }
    }

    fn base_qty_step(venue: TradingVenue, symbol: &str) -> f64 {
        let Some(table) = MonitorChannel::instance().venue_min_qty_table(venue) else {
            return 0.0;
        };
        let symbol_key = Self::min_qty_symbol_key(venue, symbol);
        let Some(step) = table.step_size(&symbol_key).filter(|step| *step > 0.0) else {
            return 0.0;
        };
        let multiplier = match venue {
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                table.contract_multiplier(&symbol_key)
            }
            _ => 1.0,
        };
        if multiplier > 0.0 {
            step * multiplier
        } else {
            0.0
        }
    }

    fn base_qty_qv_from_venue_qty(
        venue: TradingVenue,
        symbol: &str,
        venue_qty: f64,
    ) -> Option<QuantizedValue> {
        let base_qty = MonitorChannel::instance().qty_to_base(venue, symbol, venue_qty);
        if base_qty <= ARB_ORPHAN_EPS {
            return None;
        }
        let base_step = Self::base_qty_step(venue, symbol);
        if base_step > 0.0 {
            if let Some(qv) = QuantizedValue::encode_floor(base_qty, base_step) {
                if qv.get_val() <= base_qty + ARB_ORPHAN_EPS {
                    return Some(qv);
                }
            }
        }
        QuantizedValue::from_decimal(base_qty)
    }

    /// 增加 hedge residual，base 口径，带方向。
    /// 和已有 residual 自动轧差；轧平后归零。
    ///
    /// residual 只记录待 hedge exposure，不记录价格，也不触碰 net_qty_queue。
    pub fn add_hedge_residual_qty(&mut self, signed_qty: QuantizedValue, reason: &str) {
        if Self::qv_is_zero(signed_qty) {
            return;
        }
        let before = self.hedge_residual_qty;
        let Some(after) = Self::qv_add_exact(self.hedge_residual_qty, signed_qty) else {
            warn!(
                "ArbOrphanHedgeResidual: strategy_id={} symbol={} add failed due qv conversion signed_qty={} before={} reason={}",
                self.strategy_id,
                self.symbol,
                signed_qty.decimal_string(),
                before.decimal_string(),
                reason
            );
            return;
        };
        self.hedge_residual_qty = if Self::qv_is_zero(after) {
            QuantizedValue::zero()
        } else {
            after
        };
        info!(
            "ArbOrphanHedgeResidual: strategy_id={} symbol={} add signed_qty={} before={} after={} reason={}",
            self.strategy_id,
            self.symbol,
            signed_qty.decimal_string(),
            before.decimal_string(),
            self.hedge_residual_qty.decimal_string(),
            reason
        );
    }

    pub fn add_hedge_residual_base_qty(
        &mut self,
        venue: TradingVenue,
        venue_symbol: &str,
        signed_base_qty: f64,
        reason: &str,
    ) {
        if signed_base_qty.abs() <= ARB_ORPHAN_EPS {
            return;
        }
        let base_step = Self::base_qty_step(venue, venue_symbol);
        let unsigned_qv = QuantizedValue::encode_floor(signed_base_qty.abs(), base_step)
            .or_else(|| QuantizedValue::from_decimal(signed_base_qty.abs()));
        let Some(unsigned_qv) = unsigned_qv else {
            warn!(
                "ArbOrphanHedgeResidual: strategy_id={} symbol={} add_base_qty failed signed_base_qty={:.8} venue={:?} venue_symbol={} reason={}",
                self.strategy_id, self.symbol, signed_base_qty, venue, venue_symbol, reason
            );
            return;
        };
        let sign = if signed_base_qty >= 0.0 { 1 } else { -1 };
        if let Some(signed_qv) = Self::qv_with_sign(unsigned_qv, sign) {
            self.add_hedge_residual_qty(signed_qv, reason);
        }
    }

    /// residual 够最小 hedge 量时，一次性取走全部 signed qty 并清零。
    /// 不够时返回 None，residual 保持不变。
    pub fn try_take_hedge_residual_qty(
        &mut self,
        min_base_qty: f64,
        reason: &str,
    ) -> Option<QuantizedValue> {
        let min_base_qty = min_base_qty.max(0.0);
        if Self::qv_is_zero(self.hedge_residual_qty)
            || self.hedge_residual_qty.get_val().abs() + ARB_ORPHAN_EPS < min_base_qty
        {
            debug!(
                "ArbOrphanHedgeResidual: strategy_id={} symbol={} take skipped residual={} min_base_qty={:.8} reason={}",
                self.strategy_id,
                self.symbol,
                self.hedge_residual_qty.decimal_string(),
                min_base_qty,
                reason
            );
            return None;
        }
        let taken = self.hedge_residual_qty;
        self.hedge_residual_qty = QuantizedValue::zero();
        info!(
            "ArbOrphanHedgeResidual: strategy_id={} symbol={} take signed_qty={} min_base_qty={:.8} reason={}",
            self.strategy_id,
            self.symbol,
            taken.decimal_string(),
            min_base_qty,
            reason
        );
        Some(taken)
    }

    fn forget_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        let removed = self.order_legs.remove(&client_order_id).is_some();
        if removed {
            self.query_states.remove(&client_order_id);
            self.uniform_contexts.remove(&client_order_id);
            info!(
                "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} forgot order client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
        }
        removed
    }

    fn apply_net_qty_fill(&mut self, fill_ts: i64, signed_qty: f64, price: f64, leg: ArbOrphanLeg) {
        if signed_qty.abs() <= ARB_ORPHAN_EPS {
            return;
        }
        let before = self.net_qty;
        let result = self.net_qty_queue.apply_fill(fill_ts, signed_qty, price);
        self.net_qty = result.net_qty;
        debug!(
            "ArbOrphanNetQueue: strategy_id={} symbol={} leg={:?} fill_ts={} signed_qty={:.8} price={:.8} matched_qty={:.8} appended_qty={:.8} net_before={:.8} net_after={:.8} lots={}",
            self.strategy_id,
            self.symbol,
            leg,
            fill_ts,
            signed_qty,
            price,
            result.matched_qty,
            result.appended_qty,
            before,
            self.net_qty,
            self.net_qty_queue.len()
        );
    }

    fn record_terminal_fill(
        &mut self,
        leg: ArbOrphanLeg,
        fill_ts: i64,
        side: Side,
        base_qty: f64,
        base_qty_qv: Option<QuantizedValue>,
        price: f64,
    ) {
        if base_qty <= ARB_ORPHAN_EPS {
            return;
        }
        let signed_qty = Self::signed_base_qty(side, base_qty);
        self.apply_net_qty_fill(fill_ts, signed_qty, price, leg);
        if leg == ArbOrphanLeg::Open {
            if let Some(base_qty_qv) = base_qty_qv.and_then(|qv| Self::signed_qv_for_side(side, qv))
            {
                self.add_hedge_residual_qty(base_qty_qv, "open terminal fill");
            }
        }
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
        let Some(leg) = self.order_legs.get(&client_order_id).copied() else {
            return;
        };
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            self.forget_order_id(client_order_id, reason);
            return;
        };
        let snapshot = {
            let mgr = order_mgr.borrow();
            mgr.get(client_order_id).map(|order| {
                (
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.quantity,
                    order.cumulative_filled_quantity,
                    order.price,
                )
            })
        };
        let Some((venue, symbol, side, order_qty, cumulative_qty, price)) = snapshot else {
            self.forget_order_id(client_order_id, reason);
            return;
        };
        let base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, cumulative_qty);
        let base_qty_qv = Self::base_qty_qv_from_venue_qty(venue, &symbol, cumulative_qty);
        self.record_terminal_fill(leg, event_time, side, base_qty, base_qty_qv, price);
        if leg == ArbOrphanLeg::Hedge {
            let total_base_qty_qv = Self::base_qty_qv_from_venue_qty(venue, &symbol, order_qty);
            if let (Some(total_base_qty_qv), Some(base_qty_qv)) = (total_base_qty_qv, base_qty_qv) {
                if let Some(unfilled_qv) = Self::qv_sub_exact(total_base_qty_qv, base_qty_qv) {
                    if Self::qv_sign(unfilled_qv) > 0 && !Self::qv_is_zero(unfilled_qv) {
                        if let Some(residual_qty) =
                            Self::hedge_unfilled_residual_qv(side, unfilled_qv)
                        {
                            self.add_hedge_residual_qty(residual_qty, "hedge terminal unfilled");
                        }
                    }
                }
            }
        }
        info!(
            "ArbOrphanStrategy: strategy_id={} finalized order client_order_id={} leg={:?} symbol={} venue={:?} side={:?} order_qty={:.8} cumulative_qty={:.8} base_qty={:.8} residual={} reason={}",
            self.strategy_id,
            client_order_id,
            leg,
            symbol,
            venue,
            side,
            order_qty,
            cumulative_qty,
            base_qty,
            self.hedge_residual_qty.decimal_string(),
            reason
        );
        let _ = order_mgr.borrow_mut().remove(client_order_id);
        self.forget_order_id(client_order_id, reason);
    }

    fn update_order_from_order_update(&mut self, update: &dyn OrderUpdate) {
        let client_order_id = update.client_order_id();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
            if update.cumulative_filled_quantity() > order.cumulative_filled_quantity {
                order.cumulative_filled_quantity = update.cumulative_filled_quantity();
            }
            if update.order_id() > 0 {
                order.set_exchange_order_id(update.order_id());
            }
            if update.price() > 0.0 {
                order.price = update.price();
            }
            match update.status() {
                OrderStatus::New | OrderStatus::PartiallyFilled => {
                    if !order.status.is_terminal() {
                        order.status = OrderExecutionStatus::Create;
                    }
                }
                OrderStatus::Filled => {
                    order.status = OrderExecutionStatus::Filled;
                    order.set_end_time(update.event_time());
                }
                OrderStatus::Canceled => {
                    order.status = OrderExecutionStatus::Cancelled;
                    order.set_end_time(update.event_time());
                }
                OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                    order.status = OrderExecutionStatus::Rejected;
                    order.set_end_time(update.event_time());
                }
            }
        });
    }

    fn update_order_from_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let client_order_id = trade.client_order_id();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
            if trade.cumulative_filled_quantity() > order.cumulative_filled_quantity {
                order.cumulative_filled_quantity = trade.cumulative_filled_quantity();
            }
            order.set_filled_time(trade.trade_time());
            if trade.order_id() > 0 {
                order.set_exchange_order_id(trade.order_id());
            }
            if trade.price() > 0.0 {
                order.price = trade.price();
            }
            match trade.order_status() {
                Some(OrderStatus::Filled) => {
                    order.status = OrderExecutionStatus::Filled;
                    order.set_end_time(trade.event_time());
                }
                Some(OrderStatus::PartiallyFilled) => {
                    if !order.status.is_terminal() {
                        order.status = OrderExecutionStatus::Create;
                    }
                }
                Some(OrderStatus::Canceled) => {
                    order.status = OrderExecutionStatus::Cancelled;
                    order.set_end_time(trade.event_time());
                }
                Some(OrderStatus::Expired | OrderStatus::ExpiredInMatch) => {
                    order.status = OrderExecutionStatus::Rejected;
                    order.set_end_time(trade.event_time());
                }
                Some(OrderStatus::New) | None => {}
            }
        });
    }

    fn request_cancel_for_order(&mut self, client_order_id: i64, reason: &str) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            return false;
        };
        if !Self::should_cancel_order(order.status) {
            return false;
        }
        let exchange = order.venue.trade_engine_exchange();
        let cancel_bytes = match order.get_order_cancel_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} build cancel failed client_order_id={} reason={} err={}",
                    self.strategy_id, client_order_id, reason, err
                );
                return false;
            }
        };
        match TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
            Ok(()) => {
                info!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} sent cancel client_order_id={} exchange={} reason={}",
                    self.strategy_id, client_order_id, exchange, reason
                );
                true
            }
            Err(err) => {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} send cancel failed client_order_id={} exchange={} reason={} err={:#}",
                    self.strategy_id, client_order_id, exchange, reason, err
                );
                false
            }
        }
    }

    fn send_order_query(&mut self, client_order_id: i64) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            self.forget_order_id(client_order_id, "query missing local order");
            return false;
        };
        let request_query_id = client_order_id;
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        self.strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                info!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} query sent client_order_id={} request_query_id={}",
                    self.strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} build query failed client_order_id={} err={}",
                    self.strategy_id, client_order_id, err
                );
                false
            }
        }
    }
}

impl ForceCloseControl for ArbOrphanStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for ArbOrphanStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        self.order_legs.contains_key(&order_id)
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn adopt_arb_orphan_order_id(&mut self, handoff: &ArbOrphanHandoff) -> bool {
        self.adopt_order_id(handoff)
    }

    fn adopt_arb_orphan_residual(&mut self, residual: &ArbOrphanResidualHandoff) -> bool {
        if residual.signed_base_qty.abs() <= ARB_ORPHAN_EPS {
            return false;
        }
        self.add_hedge_residual_base_qty(
            residual.venue,
            &residual.symbol,
            residual.signed_base_qty,
            "handoff",
        );
        info!(
            "ARB orphan residual handoff: symbol={} venue={:?} signed_base_qty={:.8} source_strategy_id={} adopted_strategy_id={}",
            residual.symbol,
            residual.venue,
            residual.signed_base_qty,
            residual.source_strategy_id,
            self.strategy_id
        );
        true
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if normalize_symbol_for_internal(update.symbol()) != self.symbol {
            return;
        }
        let client_order_id = update.client_order_id();
        if !self.is_strategy_order(client_order_id) {
            return;
        }
        let uniform_ctx = self.uniform_contexts.get(&client_order_id).cloned();
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);
        self.update_order_from_order_update(update);
        if let Some(ctx) = uniform_ctx.as_ref() {
            let updated_order = MonitorChannel::try_order_manager()
                .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
            if let Some(order) = updated_order {
                if update.status() == OrderStatus::New {
                    publish_uniform_new_order(
                        update,
                        &order,
                        prev_cumulative_filled_qty,
                        ctx,
                        "ArbOrphanStrategy: strategy_role=arb_orphan",
                        self.strategy_id,
                        UniformAmountSource::OrderUpdate,
                    );
                }
                if matches!(
                    update.status(),
                    OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
                ) {
                    publish_uniform_terminal_order(
                        update,
                        &order,
                        prev_cumulative_filled_qty,
                        ctx,
                        "ArbOrphanStrategy: strategy_role=arb_orphan",
                        self.strategy_id,
                        UniformAmountSource::OrderUpdate,
                    );
                }
            }
        }
        if matches!(
            update.status(),
            OrderStatus::Canceled
                | OrderStatus::Filled
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        ) {
            self.finalize_terminal_order(
                client_order_id,
                update.event_time(),
                "terminal order update",
            );
        } else {
            self.request_cancel_for_order(client_order_id, "non-terminal order update");
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if normalize_symbol_for_internal(trade.symbol()) != self.symbol {
            return;
        }
        let client_order_id = trade.client_order_id();
        if !self.is_strategy_order(client_order_id) {
            return;
        }
        let uniform_ctx = self.uniform_contexts.get(&client_order_id).cloned();
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);
        self.update_order_from_trade_update(trade);
        if let (Some(ctx), Some(status)) = (uniform_ctx.as_ref(), trade.order_status()) {
            let updated_order = MonitorChannel::try_order_manager()
                .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
            if let Some(order) = updated_order {
                publish_uniform_trade_order(
                    trade,
                    &order,
                    prev_cumulative_filled_qty,
                    status,
                    ctx,
                    "ArbOrphanStrategy: strategy_role=arb_orphan",
                    self.strategy_id,
                );
            }
        }
        if trade.order_status().is_some_and(|status| {
            matches!(
                status,
                OrderStatus::Canceled
                    | OrderStatus::Filled
                    | OrderStatus::Expired
                    | OrderStatus::ExpiredInMatch
            )
        }) {
            self.finalize_terminal_order(
                client_order_id,
                trade.event_time(),
                "terminal trade update",
            );
        } else {
            self.request_cancel_for_order(client_order_id, "non-terminal trade update");
        }
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        let tracked: Vec<i64> = self.order_legs.keys().copied().collect();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        for client_order_id in tracked {
            let order_opt = order_mgr.borrow().get(client_order_id);
            let Some(order) = order_opt else {
                self.forget_order_id(client_order_id, "missing local order on period clock");
                continue;
            };
            if order.status.is_terminal() {
                drop(order);
                self.finalize_terminal_order(
                    client_order_id,
                    get_timestamp_us(),
                    "terminal local order on period clock",
                );
                continue;
            }
            let should_cancel = Self::should_cancel_order(order.status);
            drop(order);
            if should_cancel {
                self.request_cancel_for_order(client_order_id, "period clock");
            }

            let Some(query_state) = self.query_states.get_mut(&client_order_id) else {
                continue;
            };
            if query_state.ticks_until_next_query > 0 {
                query_state.ticks_until_next_query -= 1;
                continue;
            }
            let next_query_count = query_state.query_count.saturating_add(1);
            query_state.query_count = next_query_count;
            query_state.ticks_until_next_query = Self::next_query_ticks(next_query_count);
            let _ = self.send_order_query(client_order_id);
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::ArbOrphanStrategy;
    use crate::pre_trade::order_manager::OrderExecutionStatus;

    #[test]
    fn cancel_policy_is_owned_by_orphan_from_local_status() {
        assert!(!ArbOrphanStrategy::should_cancel_order(
            OrderExecutionStatus::Commit
        ));
        assert!(ArbOrphanStrategy::should_cancel_order(
            OrderExecutionStatus::Create
        ));
        assert!(!ArbOrphanStrategy::should_cancel_order(
            OrderExecutionStatus::Cancelled
        ));
    }
}
