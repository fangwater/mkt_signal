use crate::persist_manager::unified_order::UnifiedOrderRecord;
use crate::pre_trade::order_manager::{Order, OrderManager};
use crate::pre_trade::PersistChannel;
use crate::signal::common::OrderStatus;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use log::warn;

#[derive(Debug, Clone, PartialEq)]
pub struct UniformPublishCtx {
    pub signal_ts: i64,
    pub from_key: Vec<u8>,
    pub price_offset: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniformOrderEventKind {
    New,
    Terminal,
    Trade,
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

pub fn build_uniform_order_record(
    order: &Order,
    create_ts: i64,
    update_ts: i64,
    status: OrderStatus,
    signal_ts: i64,
    from_key: Vec<u8>,
    price: f64,
    price_offset: f64,
    amount_update: f64,
) -> UnifiedOrderRecord {
    let mut record = UnifiedOrderRecord {
        symbol_len: 0,
        symbol: order.symbol.as_bytes().to_vec(),
        create_ts,
        update_ts,
        signal_ts,
        client_order_id: order.client_order_id,
        venue: order.venue as u8,
        ttype: order.order_type.to_u8(),
        side: order.side.to_u8(),
        price,
        price_offset,
        amount_init: order.quantity,
        amount_update,
        status: status.to_u8(),
        from_key_len: 0,
        from_key,
    };
    record.refresh_lengths();
    record
}

pub fn publish_uniform_order_event(
    order: &Order,
    event_kind: UniformOrderEventKind,
    event_ts: i64,
    status: OrderStatus,
    signal_ts: i64,
    from_key: Vec<u8>,
    price_override: Option<f64>,
    price_offset: f64,
    amount_update: f64,
) {
    let create_ts = match event_kind {
        UniformOrderEventKind::New => event_ts,
        UniformOrderEventKind::Terminal | UniformOrderEventKind::Trade => order.timestamp.create_t,
    };
    let price = price_override
        .filter(|p| p.is_finite() && *p > 0.0)
        .unwrap_or(order.price);

    let record = build_uniform_order_record(
        order,
        create_ts,
        event_ts,
        status,
        signal_ts,
        from_key,
        price,
        price_offset,
        amount_update,
    );

    PersistChannel::with(|ch| ch.publish_uniform_order(&record));
}

pub fn publish_uniform_new_order(
    order_update: &dyn OrderUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    ctx: &UniformPublishCtx,
    strategy_label: &str,
    strategy_id: i32,
) {
    let amount_update = compute_uniform_amount_update(
        order,
        order.cumulative_filled_quantity,
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

pub fn publish_uniform_terminal_order(
    order_update: &dyn OrderUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    ctx: &UniformPublishCtx,
    strategy_label: &str,
    strategy_id: i32,
) {
    let amount_update = compute_uniform_amount_update(
        order,
        order.cumulative_filled_quantity,
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

pub fn publish_uniform_trade_order(
    trade: &dyn TradeUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    status: OrderStatus,
    ctx: &UniformPublishCtx,
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

pub fn publish_uniform_trade_order_from_order_update(
    order_update: &dyn OrderUpdate,
    order: &Order,
    prev_cumulative_filled_qty: f64,
    ctx: &UniformPublishCtx,
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
