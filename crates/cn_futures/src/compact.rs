//! Owned, bounded audit buffers. They live directly in the preallocated queue;
//! no borrowed SDK memory or truncated identifiers cross a thread boundary.
use crate::model::*;
use anyhow::{ensure, Result};
use persist_common::cn_futures::{NativeMutation, NativeOrderRecord, SendState};
use std::sync::Arc;
#[derive(Clone, Copy)]
pub(crate) struct FixedText<const N: usize> {
    bytes: [u8; N],
    len: usize,
}
impl<const N: usize> FixedText<N> {
    pub fn new(value: &str) -> Result<Self> {
        ensure!(
            value.len() <= N,
            "native audit text exceeds its documented bound"
        );
        let mut bytes = [0; N];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(Self {
            bytes,
            len: value.len(),
        })
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len])
            .expect("FixedText copies complete Rust UTF-8 strings")
    }
}
#[derive(Clone, Copy)]
struct FixedKey {
    session: FixedText<128>,
    reference: FixedText<32>,
}
impl FixedKey {
    fn new(key: &OrderKey) -> Result<Self> {
        Ok(Self {
            session: FixedText::new(&key.session)?,
            reference: FixedText::new(&key.order_ref)?,
        })
    }
    fn owned(self) -> OrderKey {
        OrderKey {
            session: self.session.as_str().into(),
            order_ref: self.reference.as_str().into(),
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct SubmitAudit {
    key: FixedKey,
    symbol: FixedText<81>,
    day: FixedText<16>,
    venue: Venue,
    side: Side,
    offset: Offset,
    lots: i32,
    price_type: PriceType,
    price_ticks: i64,
    tif: TimeInForce,
    route: Option<ExecutionRoute>,
    price: f64,
    ceiling: f64,
    pub received_us: u64,
}
impl SubmitAudit {
    pub fn new(day: &str, p: &PreparedOrder, received_us: u64) -> Result<Self> {
        Self::parts(
            day,
            &p.key,
            &p.intent,
            p.limit_price,
            p.option_market_buy_ceiling,
            received_us,
        )
    }
    fn parts(
        day: &str,
        key: &OrderKey,
        i: &Intent,
        price: f64,
        ceiling: f64,
        received_us: u64,
    ) -> Result<Self> {
        Ok(Self {
            key: FixedKey::new(key)?,
            symbol: FixedText::new(&i.symbol)?,
            day: FixedText::new(day)?,
            venue: i.venue,
            side: i.side,
            offset: i.offset,
            lots: i.lots,
            price_type: i.price_type,
            price_ticks: i.price_ticks,
            tif: i.time_in_force,
            route: i.execution_route,
            price,
            ceiling,
            received_us,
        })
    }
    pub fn day(&self) -> &str {
        self.day.as_str()
    }
    pub fn owned(self) -> PreparedOrder {
        PreparedOrder {
            key: self.key.owned(),
            intent: Intent {
                venue: self.venue,
                symbol: self.symbol.as_str().into(),
                side: self.side,
                offset: self.offset,
                lots: self.lots,
                price_type: self.price_type,
                price_ticks: self.price_ticks,
                time_in_force: self.tif,
                execution_route: self.route,
            },
            limit_price: self.price,
            option_market_buy_ceiling: self.ceiling,
        }
    }
}

/// One immutable point-in-time OMS projection. The Arc pins exactly the rule
/// revision seen by the producer; the writer never reads a mutable TradingCore.
pub(crate) struct OrderAudit {
    submitted: SubmitAudit,
    system_id: FixedText<64>,
    status: OrderStatus,
    filled: i32,
    reported_filled: i32,
    cancelled: i32,
    reserved_today: i32,
    reserved_yesterday: i32,
    local_cash_held: f64,
    cash_reserved_per_lot: f64,
    cancel_pending: bool,
    owned: bool,
    instrument: Option<Arc<Instrument>>,
    send_state: SendState,
    observed: bool,
}
impl OrderAudit {
    pub fn new(
        day: &str,
        order: &Order,
        instrument: Option<Arc<Instrument>>,
        ready: bool,
        observed: bool,
        received_us: u64,
    ) -> Result<Self> {
        Ok(Self {
            submitted: SubmitAudit::parts(
                day,
                &order.key,
                &order.request,
                order.limit_price,
                0.0,
                received_us,
            )?,
            system_id: FixedText::new(&order.system_id)?,
            status: order.status,
            filled: order.filled,
            reported_filled: order.reported_filled,
            cancelled: order.cancelled,
            reserved_today: order.reserved_today,
            reserved_yesterday: order.reserved_yesterday,
            local_cash_held: order.local_cash_held,
            cash_reserved_per_lot: order.cash_reserved_per_lot,
            cancel_pending: order.cancel_pending,
            owned: order.owned,
            instrument,
            send_state: if !order.owned {
                SendState::Observed
            } else if order.status == OrderStatus::Rejected {
                SendState::RejectedLocally
            } else if ready {
                SendState::SubmittedLocally
            } else {
                SendState::Uncertain
            },
            observed,
        })
    }
    fn owned(self) -> NativeOrderRecord {
        let prepared = self.submitted.owned();
        let order = Order {
            request: prepared.intent,
            key: prepared.key,
            system_id: self.system_id.as_str().into(),
            limit_price: prepared.limit_price,
            status: self.status,
            filled: self.filled,
            reported_filled: self.reported_filled,
            cancelled: self.cancelled,
            reserved_today: self.reserved_today,
            reserved_yesterday: self.reserved_yesterday,
            local_cash_held: self.local_cash_held,
            cash_reserved_per_lot: self.cash_reserved_per_lot,
            cancel_pending: self.cancel_pending,
            owned: self.owned,
        };
        crate::persistence::record_owned_at(
            self.submitted.day(),
            order,
            self.instrument.as_deref().cloned(),
            self.send_state,
            self.submitted.received_us,
        )
    }
    pub fn append(self, mutation: &mut NativeMutation) {
        let observed = self.observed;
        let record = self.owned();
        if observed {
            mutation.observed.push(record.identity.clone());
        }
        mutation.orders.push(record);
    }
}

pub(crate) struct FillAudit {
    venue: Venue,
    symbol: FixedText<81>,
    day: FixedText<16>,
    trade_id: FixedText<64>,
    side: Side,
    reference: FixedText<32>,
    key: Option<FixedKey>,
    system_id: FixedText<64>,
    offset: Offset,
    lots: i32,
    price: f64,
}
impl FillAudit {
    pub fn new(fill: &Fill) -> Result<Self> {
        Ok(Self {
            venue: fill.identity.venue,
            symbol: FixedText::new(&fill.identity.symbol)?,
            day: FixedText::new(&fill.identity.trading_day)?,
            trade_id: FixedText::new(&fill.identity.trade_id)?,
            side: fill.identity.side,
            reference: FixedText::new(&fill.order_ref)?,
            key: fill.order_key.as_ref().map(FixedKey::new).transpose()?,
            system_id: FixedText::new(&fill.system_id)?,
            offset: fill.offset,
            lots: fill.lots,
            price: fill.price,
        })
    }
    pub fn owned(self) -> Fill {
        Fill {
            identity: TradeIdentity {
                venue: self.venue,
                symbol: self.symbol.as_str().into(),
                trading_day: self.day.as_str().into(),
                trade_id: self.trade_id.as_str().into(),
                side: self.side,
            },
            order_ref: self.reference.as_str().into(),
            order_key: self.key.map(FixedKey::owned),
            system_id: self.system_id.as_str().into(),
            offset: self.offset,
            lots: self.lots,
            price: self.price,
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct CancelAudit {
    key: FixedKey,
    symbol: FixedText<81>,
    venue: Venue,
    system_id: FixedText<32>,
    route: Option<ExecutionRoute>,
}
impl CancelAudit {
    pub fn new(p: &PreparedCancel) -> Result<Self> {
        Ok(Self {
            key: FixedKey::new(&p.key)?,
            symbol: FixedText::new(&p.symbol)?,
            venue: p.venue,
            system_id: FixedText::new(&p.system_id)?,
            route: p.execution_route,
        })
    }
    pub fn owned(self) -> PreparedCancel {
        PreparedCancel {
            key: self.key.owned(),
            symbol: self.symbol.as_str().into(),
            venue: self.venue,
            system_id: self.system_id.as_str().into(),
            execution_route: self.route,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn rule() -> Instrument {
        Instrument {
            venue: Venue::Shfe,
            symbol: "cu2612C106000".into(),
            price_tick: 2.0,
            contract_multiple: 5,
            expiry_date: 20261124,
            commission: None,
            margin: None,
            min_lots: 1,
            max_lots: 100,
            market_lots: Some(LotLimits {
                minimum: 1,
                maximum: 30,
            }),
            active: true,
            cash_budget_per_open_lot: 100000.0,
            option: Some(OptionContract {
                right: OptionRight::Call,
                underlying: "cu2612".into(),
                strike_price: 106000.0,
                contract_multiple: 5,
                expiry_date: 20261124,
                upper_limit_price: 12000.0,
                cash_buffer_per_buy_lot: 10.0,
            }),
        }
    }
    fn order() -> Order {
        Order {
            request: Intent {
                venue: Venue::Shfe,
                symbol: "cu2612C106000".into(),
                side: Side::Buy,
                offset: Offset::Close,
                lots: 3,
                price_type: PriceType::Limit,
                price_ticks: 50,
                time_in_force: TimeInForce::Fak,
                execution_route: Some(ExecutionRoute {
                    mode: ConnectionSelection::Preferred,
                    connection_id: Some(63),
                }),
            },
            key: OrderKey {
                session: "yida:20261110:group:3".into(),
                order_ref: "17".into(),
            },
            system_id: "9".repeat(63),
            limit_price: 100.00000000000001,
            status: OrderStatus::Working,
            filled: 1,
            reported_filled: 2,
            cancelled: 0,
            reserved_today: 1,
            reserved_yesterday: 1,
            local_cash_held: 123.45600000000002,
            cash_reserved_per_lot: 45.67000000000001,
            cancel_pending: true,
            owned: true,
        }
    }
    #[test]
    fn compact_projection_preserves_the_complete_canonical_record() {
        for owned in [false, true] {
            for ready in [false, true] {
                for status in [
                    OrderStatus::Pending,
                    OrderStatus::Working,
                    OrderStatus::Filled,
                    OrderStatus::Cancelled,
                    OrderStatus::Rejected,
                ] {
                    let mut order = order();
                    order.owned = owned;
                    order.status = status;
                    let state = if !owned {
                        SendState::Observed
                    } else if status == OrderStatus::Rejected {
                        SendState::RejectedLocally
                    } else if ready {
                        SendState::SubmittedLocally
                    } else {
                        SendState::Uncertain
                    };
                    let expected =
                        crate::persistence::record_at("20261110", &order, Some(rule()), state, 123);
                    let audit = OrderAudit::new(
                        "20261110",
                        &order,
                        Some(Arc::new(rule())),
                        ready,
                        true,
                        123,
                    )
                    .unwrap();
                    let mut mutation = NativeMutation::default();
                    audit.append(&mut mutation);
                    assert_eq!(mutation.observed, vec![expected.identity.clone()]);
                    let actual = mutation.orders.pop().unwrap();
                    assert_eq!(
                        serde_json::to_value(&actual).unwrap(),
                        serde_json::to_value(&expected).unwrap()
                    );
                    assert_eq!(
                        actual.order.local_cash_held.to_bits(),
                        order.local_cash_held.to_bits()
                    );
                }
            }
        }
    }
    #[test]
    fn queued_projection_pins_metadata_and_state_across_core_mutation() {
        use crate::core::TradingCore;
        let mut core = TradingCore::new(
            RiskLimits {
                max_order_lots: 5,
                max_position_lots_per_side: 5,
                max_working_lots: 5,
                max_actions_per_second: 5,
                minimum_cash_remaining: 0.0,
                enable_fak_fok: true,
                close_priority: ClosePriority::TodayFirst,
            },
            crate::yida::scope(),
        )
        .unwrap();
        core.restore(Snapshot {
            trading_day: "20261110".into(),
            session: "yida:20261110:group:3".into(),
            max_order_ref: 16,
            available_cash: 1000000.0,
            instruments: vec![rule()],
            positions: vec![],
            orders: vec![],
            trades: vec![],
        })
        .unwrap();
        let mut intent = order().request;
        intent.offset = Offset::Open;
        let prepared = core.prepare(intent, std::time::Instant::now()).unwrap();
        let expected = crate::persistence::record_at(
            "20261110",
            core.order(&prepared.key).unwrap(),
            Some(rule()),
            SendState::SubmittedLocally,
            456,
        );
        let audit = OrderAudit::new(
            "20261110",
            core.order(&prepared.key).unwrap(),
            core.instrument_handle(Venue::Shfe, "cu2612C106000"),
            true,
            false,
            456,
        )
        .unwrap();
        core.reject(&prepared.key).unwrap();
        let mut revised = rule();
        revised.option.as_mut().unwrap().upper_limit_price = 24000.0;
        core.replace_instruments(vec![revised]).unwrap();
        let actual = std::thread::spawn(move || audit.owned()).join().unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(
            core.order(&prepared.key).unwrap().status,
            OrderStatus::Rejected
        );
        assert_eq!(
            core.instrument(Venue::Shfe, "cu2612C106000")
                .unwrap()
                .option
                .as_ref()
                .unwrap()
                .upper_limit_price,
            24000.0
        );
    }
    #[test]
    fn compact_fill_keeps_full_ids_and_float_bits_and_never_truncates() {
        let order = order();
        let mut fill = Fill {
            identity: TradeIdentity {
                venue: Venue::Shfe,
                symbol: order.request.symbol.clone(),
                trading_day: "20261110".into(),
                trade_id: "T".repeat(63),
                side: Side::Buy,
            },
            order_ref: order.key.order_ref.clone(),
            order_key: Some(order.key),
            system_id: order.system_id,
            offset: Offset::CloseToday,
            lots: 1,
            price: 104820.00000000001,
        };
        let actual = FillAudit::new(&fill).unwrap().owned();
        assert_eq!(actual, fill);
        assert_eq!(actual.price.to_bits(), fill.price.to_bits());
        fill.identity.trade_id.push_str("XX");
        assert!(FillAudit::new(&fill).is_err());
        let mut order = self::order();
        order.system_id.push_str("XX");
        assert!(OrderAudit::new("20261110", &order, None, true, false, 1).is_err());
    }
    #[test]
    fn text_bounds_preserve_complete_unicode_without_truncation() {
        let s = FixedText::<4>::new("中a").unwrap();
        assert_eq!(s.as_str(), "中a");
        assert!(FixedText::<3>::new("中a").is_err());
    }
}
