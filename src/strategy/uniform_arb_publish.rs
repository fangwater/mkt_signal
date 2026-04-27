use crate::pre_trade::order_manager::{Order, OrderManager};
use crate::signal::common::OrderStatus;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{publish_uniform_order_event, UniformOrderEventKind};
use log::warn;

#[derive(Debug, Clone, PartialEq)]
pub struct ArbUniformPublishCtx {
    pub signal_ts: i64,
    pub from_key: Vec<u8>,
    pub price_offset: f64,
}

fn compute_uniform_amount_update(
    order: &Order,
    incoming_cum: f64,
    prev_cumulative_filled_qty: f64,
    status: OrderStatus,
    strategy_label: &str,
    strategy_id: i32,
) -> f64 {
    match OrderManager::compute_uniform_amount_update_from_cumulative(
        prev_cumulative_filled_qty,
        incoming_cum,
    ) {
        Some(delta) => delta,
        None => {
            warn!(
                "{}: strategy_id={} uniform {:?} amount_update rollback detected: client_order_id={} prev={:.8} incoming={:.8}",
                strategy_label,
                strategy_id,
                status,
                order.client_order_id,
                prev_cumulative_filled_qty,
                incoming_cum
            );
            0.0
        }
    }
}

pub fn publish_arb_uniform_new_order(
    order_update: &dyn OrderUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    ctx: &ArbUniformPublishCtx,
    strategy_label: &str,
    strategy_id: i32,
) {
    let amount_update = compute_uniform_amount_update(
        order,
        order_update.cumulative_filled_quantity(),
        prev_cumulative_filled_qty,
        order_update.status(),
        strategy_label,
        strategy_id,
    );

    publish_uniform_order_event(
        order,
        UniformOrderEventKind::New,
        order_update.event_time(),
        order_update.status(),
        ctx.signal_ts,
        ctx.from_key.clone(),
        None,
        ctx.price_offset,
        amount_update,
    );
}

pub fn publish_arb_uniform_terminal_order(
    order_update: &dyn OrderUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    ctx: &ArbUniformPublishCtx,
    strategy_label: &str,
    strategy_id: i32,
) {
    let amount_update = compute_uniform_amount_update(
        order,
        order_update.cumulative_filled_quantity(),
        prev_cumulative_filled_qty,
        order_update.status(),
        strategy_label,
        strategy_id,
    );

    publish_uniform_order_event(
        order,
        UniformOrderEventKind::Terminal,
        order_update.event_time(),
        order_update.status(),
        ctx.signal_ts,
        ctx.from_key.clone(),
        None,
        ctx.price_offset,
        amount_update,
    );
}

pub fn publish_arb_uniform_trade_order(
    trade: &dyn TradeUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    status: OrderStatus,
    ctx: &ArbUniformPublishCtx,
    strategy_label: &str,
    strategy_id: i32,
) {
    if !matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
        return;
    }

    let amount_update = compute_uniform_amount_update(
        order,
        trade.cumulative_filled_quantity(),
        prev_cumulative_filled_qty,
        status,
        strategy_label,
        strategy_id,
    );

    publish_uniform_order_event(
        order,
        UniformOrderEventKind::Trade,
        trade.event_time(),
        status,
        ctx.signal_ts,
        ctx.from_key.clone(),
        Some(trade.price()),
        ctx.price_offset,
        amount_update,
    );
}

pub fn publish_arb_uniform_trade_order_from_order_update(
    order_update: &dyn OrderUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    ctx: &ArbUniformPublishCtx,
    strategy_label: &str,
    strategy_id: i32,
) {
    let status = order_update.status();
    if !matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
        return;
    }

    let amount_update = compute_uniform_amount_update(
        order,
        order.cumulative_filled_quantity,
        prev_cumulative_filled_qty,
        status,
        strategy_label,
        strategy_id,
    );

    publish_uniform_order_event(
        order,
        UniformOrderEventKind::Trade,
        order_update.event_time(),
        status,
        ctx.signal_ts,
        ctx.from_key.clone(),
        None,
        ctx.price_offset,
        amount_update,
    );
}
