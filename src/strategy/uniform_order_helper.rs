use crate::persist_manager::unified_order::UnifiedOrderRecord;
use crate::pre_trade::order_manager::Order;
use crate::pre_trade::PersistChannel;
use crate::signal::common::OrderStatus;

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
