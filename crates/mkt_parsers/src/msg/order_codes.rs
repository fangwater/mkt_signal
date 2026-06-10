pub fn side_to_u8(side: &str) -> u8 {
    match side.to_ascii_lowercase().as_str() {
        "buy" => 1,
        "sell" => 2,
        _ => 0,
    }
}

pub fn side_to_u8_default_buy(side: &str) -> u8 {
    match side_to_u8(side) {
        0 => 1,
        code => code,
    }
}

pub fn order_type_to_u8(order_type: &str) -> u8 {
    match order_type.to_ascii_uppercase().as_str() {
        "LIMIT" | "POSTONLY" | "POST_ONLY" | "POST-ONLY" => 1,
        "MARKET" => 3,
        "STOP_LOSS" => 4,
        "STOP_LOSS_LIMIT" => 5,
        "TAKE_PROFIT" => 6,
        "TAKE_PROFIT_LIMIT" => 7,
        "STOP_MARKET" => 8,
        "TAKE_PROFIT_MARKET" => 9,
        _ => 0,
    }
}

pub fn order_type_to_u8_default_limit(order_type: &str) -> u8 {
    match order_type_to_u8(order_type) {
        0 => 1,
        code => code,
    }
}

pub fn time_in_force_to_u8(tif: &str) -> u8 {
    match tif.to_ascii_uppercase().as_str() {
        "GTC" => 0,
        "IOC" => 1,
        "FOK" => 2,
        "GTX" | "POSTONLY" | "POST_ONLY" | "POST-ONLY" | "POC" => 3,
        _ => 0,
    }
}

pub fn execution_type_to_u8(execution_type: &str) -> u8 {
    match execution_type.to_ascii_uppercase().as_str() {
        "NEW" => 1,
        "CANCELED" | "CANCELLED" => 2,
        "REPLACED" => 3,
        "REJECTED" => 4,
        "TRADE" => 5,
        "EXPIRED" => 6,
        "TRADE_PREVENTION" => 7,
        _ => 1,
    }
}

pub fn order_status_to_u8(status: &str) -> u8 {
    match status.to_ascii_uppercase().as_str() {
        "NEW" => 1,
        "PARTIALLY_FILLED" => 2,
        "FILLED" => 3,
        "CANCELED" | "CANCELLED" => 4,
        "EXPIRED" => 5,
        "EXPIRED_IN_MATCH" => 6,
        _ => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BybitStatus {
    New,
    Created,
    Untriggered,
    Triggered,
    Active,
    PartiallyFilled,
    Filled,
    Cancelled,
    Deactivated,
    PartiallyFilledCanceled,
    Rejected,
    Expired,
    Unknown,
}

fn bybit_status_kind(status: &str) -> BybitStatus {
    match status.to_ascii_lowercase().as_str() {
        "new" => BybitStatus::New,
        "created" => BybitStatus::Created,
        "untriggered" => BybitStatus::Untriggered,
        "triggered" => BybitStatus::Triggered,
        "active" => BybitStatus::Active,
        "partiallyfilled" | "partially_filled" | "partialfill" | "partial-fill" => {
            BybitStatus::PartiallyFilled
        }
        "filled" => BybitStatus::Filled,
        "cancelled" | "canceled" => BybitStatus::Cancelled,
        "deactivated" => BybitStatus::Deactivated,
        "partiallyfilledcanceled" => BybitStatus::PartiallyFilledCanceled,
        "rejected" => BybitStatus::Rejected,
        "expired" => BybitStatus::Expired,
        _ => BybitStatus::Unknown,
    }
}

pub fn bybit_status_to_order_status(status: &str) -> u8 {
    match bybit_status_kind(status) {
        BybitStatus::New
        | BybitStatus::Created
        | BybitStatus::Untriggered
        | BybitStatus::Triggered
        | BybitStatus::Active => 1,
        BybitStatus::PartiallyFilled => 2,
        BybitStatus::Filled => 3,
        BybitStatus::Cancelled | BybitStatus::Deactivated => 4,
        BybitStatus::PartiallyFilledCanceled | BybitStatus::Rejected | BybitStatus::Expired => 5,
        BybitStatus::Unknown => 1,
    }
}

pub fn bybit_status_to_execution_type(status: &str) -> u8 {
    match bybit_status_kind(status) {
        BybitStatus::New
        | BybitStatus::Created
        | BybitStatus::Untriggered
        | BybitStatus::Triggered
        | BybitStatus::Active => 1,
        BybitStatus::PartiallyFilled | BybitStatus::Filled => 5,
        BybitStatus::Cancelled | BybitStatus::Deactivated => 2,
        BybitStatus::Expired => 6,
        BybitStatus::PartiallyFilledCanceled | BybitStatus::Rejected => 8,
        BybitStatus::Unknown => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitOrderTopicKind {
    OrderUpdate,
    TradeUpdate,
    Ignore,
}

pub fn bybit_order_topic_kind(status: &str) -> BybitOrderTopicKind {
    match bybit_status_kind(status) {
        BybitStatus::New
        | BybitStatus::Rejected
        | BybitStatus::Cancelled
        | BybitStatus::PartiallyFilledCanceled => BybitOrderTopicKind::OrderUpdate,
        BybitStatus::PartiallyFilled | BybitStatus::Filled => BybitOrderTopicKind::TradeUpdate,
        _ => BybitOrderTopicKind::Ignore,
    }
}

pub fn bybit_order_topic_execution_type(status: &str) -> u8 {
    match bybit_status_kind(status) {
        BybitStatus::Cancelled | BybitStatus::PartiallyFilledCanceled => 2,
        BybitStatus::Rejected => 8,
        _ => 1,
    }
}

pub fn bybit_order_topic_order_status(status: &str) -> u8 {
    match bybit_status_kind(status) {
        BybitStatus::Cancelled | BybitStatus::PartiallyFilledCanceled => 4,
        BybitStatus::Rejected => 5,
        _ => 1,
    }
}

pub fn execution_type_label(execution_type: u8) -> &'static str {
    match execution_type {
        5 => "Trade",
        2 => "Canceled",
        6 => "Expired",
        8 => "Rejected",
        _ => "New",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitgetStatus {
    New,
    Live,
    Init,
    PartiallyFilled,
    Filled,
    Cancelled,
    Expired,
    Rejected,
    RejectedMaker,
    Unknown,
}

fn bitget_status_kind(status: &str) -> BitgetStatus {
    match status.to_ascii_lowercase().as_str() {
        "new" => BitgetStatus::New,
        "live" => BitgetStatus::Live,
        "init" => BitgetStatus::Init,
        "partially_filled" | "partially-filled" | "partial-fill" => BitgetStatus::PartiallyFilled,
        "filled" | "full-fill" => BitgetStatus::Filled,
        "cancelled" | "canceled" => BitgetStatus::Cancelled,
        "expired" => BitgetStatus::Expired,
        "rejected" => BitgetStatus::Rejected,
        "rejected_maker" => BitgetStatus::RejectedMaker,
        _ => BitgetStatus::Unknown,
    }
}

pub fn bitget_status_to_order_status(status: &str) -> u8 {
    match bitget_status_kind(status) {
        BitgetStatus::New | BitgetStatus::Live | BitgetStatus::Init => 1,
        BitgetStatus::PartiallyFilled => 2,
        BitgetStatus::Filled => 3,
        BitgetStatus::Cancelled => 4,
        BitgetStatus::Expired | BitgetStatus::Rejected | BitgetStatus::RejectedMaker => 5,
        BitgetStatus::Unknown => 1,
    }
}

pub fn bitget_status_to_execution_type(status: &str) -> u8 {
    match bitget_status_kind(status) {
        BitgetStatus::New | BitgetStatus::Live | BitgetStatus::Init => 1,
        BitgetStatus::PartiallyFilled | BitgetStatus::Filled => 5,
        BitgetStatus::Cancelled => 2,
        BitgetStatus::Expired => 6,
        BitgetStatus::Rejected | BitgetStatus::RejectedMaker => 8,
        BitgetStatus::Unknown => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateEventKind {
    Put,
    Update,
    Open,
    Finish,
    Finished,
    Unknown,
}

fn gate_event_kind(event: &str) -> GateEventKind {
    match event.trim().to_ascii_lowercase().as_str() {
        "put" => GateEventKind::Put,
        "update" => GateEventKind::Update,
        "open" => GateEventKind::Open,
        "finish" => GateEventKind::Finish,
        "finished" => GateEventKind::Finished,
        _ => GateEventKind::Unknown,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateFinishKind {
    New,
    Update,
    Filled,
    Cancelled,
    Liquidated,
    ReduceOnly,
    PositionClose,
    ReduceOut,
    Poc,
    Stp,
    Ioc,
    AutoDeleveraging,
    Unknown,
}

fn gate_finish_kind(finish_as: &str) -> GateFinishKind {
    match finish_as.trim().to_ascii_lowercase().as_str() {
        "_new" => GateFinishKind::New,
        "_update" => GateFinishKind::Update,
        "filled" => GateFinishKind::Filled,
        "cancelled" | "canceled" => GateFinishKind::Cancelled,
        "liquidated" => GateFinishKind::Liquidated,
        "reduce_only" => GateFinishKind::ReduceOnly,
        "position_close" => GateFinishKind::PositionClose,
        "reduce_out" => GateFinishKind::ReduceOut,
        "poc" => GateFinishKind::Poc,
        "stp" => GateFinishKind::Stp,
        "ioc" => GateFinishKind::Ioc,
        "auto_deleveraging" => GateFinishKind::AutoDeleveraging,
        _ => GateFinishKind::Unknown,
    }
}

pub fn gate_event_to_execution_and_status(event: &str, finish_as: &str) -> (u8, u8) {
    match gate_event_kind(event) {
        GateEventKind::Put => (1, 1),
        GateEventKind::Update => (5, 2),
        GateEventKind::Open => match gate_finish_kind(finish_as) {
            GateFinishKind::New => (1, 1),
            GateFinishKind::Update => (5, 2),
            _ => (1, 1),
        },
        GateEventKind::Finish | GateEventKind::Finished => match gate_finish_kind(finish_as) {
            GateFinishKind::Filled => (5, 3),
            GateFinishKind::Cancelled
            | GateFinishKind::Liquidated
            | GateFinishKind::ReduceOnly
            | GateFinishKind::PositionClose
            | GateFinishKind::ReduceOut
            | GateFinishKind::Poc => (2, 4),
            GateFinishKind::Stp => (7, 4),
            GateFinishKind::Ioc | GateFinishKind::AutoDeleveraging => (5, 3),
            _ => (6, 5),
        },
        GateEventKind::Unknown => (1, 1),
    }
}

pub fn gate_spot_event_to_execution_and_status(event: &str, finish_as: &str) -> Option<(u8, u8)> {
    if finish_as.trim().is_empty() {
        return None;
    }
    match gate_event_kind(event) {
        GateEventKind::Put | GateEventKind::Update | GateEventKind::Finish => {
            Some(gate_event_to_execution_and_status(event, finish_as))
        }
        _ => None,
    }
}

pub fn gate_futures_event_to_execution_and_status(
    status: &str,
    finish_as: &str,
) -> Option<(u8, u8)> {
    let event = gate_event_kind(status);
    let finish = gate_finish_kind(finish_as);
    let supported = matches!(
        (event, finish),
        (GateEventKind::Open, GateFinishKind::New)
            | (GateEventKind::Open, GateFinishKind::Update)
            | (GateEventKind::Finished, GateFinishKind::Filled)
            | (GateEventKind::Finished, GateFinishKind::Ioc)
            | (GateEventKind::Finished, GateFinishKind::Cancelled)
            | (GateEventKind::Finished, GateFinishKind::Liquidated)
            | (GateEventKind::Finished, GateFinishKind::AutoDeleveraging)
            | (GateEventKind::Finished, GateFinishKind::ReduceOnly)
            | (GateEventKind::Finished, GateFinishKind::PositionClose)
            | (GateEventKind::Finished, GateFinishKind::Stp)
            | (GateEventKind::Finished, GateFinishKind::ReduceOut)
            | (GateEventKind::Finished, GateFinishKind::Poc)
    );
    supported.then(|| gate_event_to_execution_and_status(status, finish_as))
}

pub fn gate_order_uses_fill_price(event_or_status: &str, finish_as: &str) -> bool {
    gate_event_kind(event_or_status) == GateEventKind::Update
        || matches!(
            gate_finish_kind(finish_as),
            GateFinishKind::Update | GateFinishKind::Filled | GateFinishKind::Ioc
        )
}
