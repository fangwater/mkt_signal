//! Conversion of the single-loop OMS into the shared native persistence contract.
use crate::{core::TradingCore, model::*};
use persist_common::cn_futures::{
    NativeMutation, NativeOrderRecord, PersistedOrderKey, SendState, StoreIdentity,
};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
/// Length-delimited scope prevents accidental cross-account/front recovery without storing credentials.
pub fn store_identity(adapter: &str, scope: &[&[u8]]) -> StoreIdentity {
    let mut hash = Sha256::new();
    for part in scope {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    StoreIdentity {
        adapter: adapter.into(),
        account_hash: format!("{:x}", hash.finalize()),
    }
}
pub fn record(
    day: &str,
    order: &Order,
    instrument: Option<Instrument>,
    send_state: SendState,
) -> NativeOrderRecord {
    record_at(day, order, instrument, send_state, now_us())
}
pub(crate) fn record_at(
    day: &str,
    order: &Order,
    instrument: Option<Instrument>,
    send_state: SendState,
    now: u64,
) -> NativeOrderRecord {
    record_owned_at(day, order.clone(), instrument, send_state, now)
}
pub(crate) fn record_owned_at(
    day: &str,
    order: Order,
    instrument: Option<Instrument>,
    send_state: SendState,
    now: u64,
) -> NativeOrderRecord {
    let submitted_intent = order.owned.then(|| order.request.clone());
    let locally_submitted = order.owned;
    NativeOrderRecord {
        identity: PersistedOrderKey {
            trading_day: day.into(),
            venue: order.request.venue,
            native: order.key.clone(),
        },
        order,
        submitted_intent,
        locally_submitted,
        send_state,
        instrument,
        first_received_us: now,
        updated_us: now,
        evidence_seq: 0,
    }
}
pub fn projection(
    core: &TradingCore,
    observed: &[OrderKey],
    reconciliation: bool,
) -> NativeMutation {
    let view = core.view();
    let mut mutation = NativeMutation::default();
    if reconciliation {
        mutation.reconciliation_day = Some(view.trading_day.into());
    }
    for order in view.orders {
        let send_state = if !order.owned {
            SendState::Observed
        } else if order.status == OrderStatus::Rejected {
            SendState::RejectedLocally
        } else if view.ready {
            SendState::SubmittedLocally
        } else {
            SendState::Uncertain
        };
        let instrument = view
            .instruments
            .iter()
            .find(|r| r.venue == order.request.venue && r.symbol == order.request.symbol)
            .copied()
            .cloned();
        let record = record(view.trading_day, order, instrument, send_state);
        if observed.contains(&order.key) || reconciliation {
            mutation.observed.push(record.identity.clone());
        }
        mutation.orders.push(record);
    }
    mutation
}
pub fn prepared(day: &str, p: &PreparedOrder) -> NativeMutation {
    prepared_at(day, p, now_us())
}
pub(crate) fn prepared_at(day: &str, p: &PreparedOrder, received_us: u64) -> NativeMutation {
    let order = Order {
        request: p.intent.clone(),
        key: p.key.clone(),
        system_id: String::new(),
        limit_price: p.limit_price,
        status: OrderStatus::Pending,
        filled: 0,
        reported_filled: 0,
        cancelled: 0,
        reserved_today: 0,
        reserved_yesterday: 0,
        local_cash_held: 0.0,
        cash_reserved_per_lot: 0.0,
        cancel_pending: false,
        owned: true,
    };
    NativeMutation {
        orders: vec![record_owned_at(
            day,
            order,
            None,
            SendState::Prepared,
            received_us,
        )],
        ..Default::default()
    }
}

/// Live callbacks project only the changed order, independent of account-history size.
pub fn projection_for(
    core: &TradingCore,
    keys: &[OrderKey],
    observed: &[OrderKey],
) -> NativeMutation {
    let mut mutation = NativeMutation::default();
    for key in keys {
        let Some(order) = core.order(key) else {
            continue;
        };
        let send_state = if !order.owned {
            SendState::Observed
        } else if order.status == OrderStatus::Rejected {
            SendState::RejectedLocally
        } else if core.is_ready() {
            SendState::SubmittedLocally
        } else {
            SendState::Uncertain
        };
        let record = record(
            core.trading_day(),
            order,
            core.instrument(order.request.venue, &order.request.symbol)
                .cloned(),
            send_state,
        );
        if observed.contains(key) {
            mutation.observed.push(record.identity.clone());
        }
        mutation.orders.push(record);
    }
    mutation
}
