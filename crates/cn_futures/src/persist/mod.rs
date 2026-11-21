//! Exec → persist_manager evidence channel, aligned with crypto `PersistChannel`:
//! each call encodes one record into a reused buffer and publishes it to shared
//! memory from the business loop, without waiting for anyone.
//!
//! Only two kinds are stored, both in crypto formats: uniform orders (order
//! lifecycle, per-strategy fills, internal crosses) and trade updates (factual
//! exchange fills). Everything else is a trader-log line. Domestic facts
//! (open/close, trading day, trade id, commission, native order identity) ride
//! in the optional record extension.
pub mod ipc;
pub mod recovery;
pub mod wire;

pub use recovery::{DurableRecovery, EXEC_FROM_KEY, OBSERVED_FROM_KEY};
pub use wire::{materialize, render_stored, Materialized};

use crate::model::{Day, Fill, OrderKey, OrderView, PreparedOrder, Side, Venue};
use anyhow::{Context, Result};
use cn_futures_target::ledger::{Row, UNALLOCATED};
use order_common::{OrderType, PositionEffect, TradeUpdate, TradingVenue};
use persist_common::cn_futures::{ClientOrderText, PersistedOrderKey, Text};
use persist_common::record_extension::ExtensionRef;
use persist_common::{
    native_codec, FillLiquidity, UniformOrderRef, UNIFORM_ORDER_TYPE_INTERNAL_CROSS,
};
use serde::Serialize;
use serde_json::Value;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::Path;
use wire::{Kind, DISPLAY, SHUTDOWN_TAIL};

/// Destination of encoded records. Production is the iceoryx2 publisher.
pub trait RecordSink {
    fn publish(&self, record: &[u8]) -> Result<()>;
}
impl RecordSink for ipc::Publisher {
    fn publish(&self, record: &[u8]) -> Result<()> {
        ipc::Publisher::publish(self, record)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PersistStats {
    pub published: u64,
    pub failures: u64,
}

/// `from_key` of a lifecycle record, fixed for the order's lifetime here.
type FromKey = Text<136>;

/// Last lifecycle published for one order, so unchanged reports are not
/// recorded twice.
#[derive(Clone, Copy)]
struct Emitted {
    create_us: i64,
    status: u8,
    filled: i32,
    cancelled: i32,
    from_key: FromKey,
}

pub struct PersistChannel {
    sink: Box<dyn RecordSink>,
    redactions: Vec<String>,
    scratch: RefCell<Vec<u8>>,
    failed: Cell<bool>,
    stats: Cell<PersistStats>,
    emitted: RefCell<HashMap<i64, Emitted>>,
    /// Read once at startup; a previous process's unresolved requests must
    /// appear in this process's first complete recovery.
    previous: RefCell<DurableRecovery>,
}

/// Initial record buffer; trading records never grow it.
const SCRATCH_BYTES: usize = 64 * 1024;
/// Orders tracked before the lifecycle index grows.
const ORDER_RESERVE: usize = 8192;

impl PersistChannel {
    pub fn start(
        database: &Path,
        redactions: Vec<String>,
        sink: Box<dyn RecordSink>,
    ) -> Result<Self> {
        let previous = DurableRecovery::load(database)?;
        Ok(Self {
            sink,
            redactions: redactions.into_iter().filter(|s| !s.is_empty()).collect(),
            scratch: RefCell::new(Vec::with_capacity(SCRATCH_BYTES)),
            failed: Cell::new(false),
            stats: Cell::new(PersistStats::default()),
            emitted: RefCell::new(HashMap::with_capacity(ORDER_RESERVE)),
            previous: RefCell::new(previous),
        })
    }

    pub fn persist_manager_sink() -> Result<Box<dyn RecordSink>> {
        Ok(Box::new(ipc::Publisher::new()?))
    }

    /// A local publish failure latches; trading stops until restart.
    pub fn failed(&self) -> bool {
        self.failed.get()
    }

    pub fn stats(&self) -> PersistStats {
        self.stats.get()
    }

    fn publish(
        &self,
        kind: Kind,
        flags: u8,
        received_us: u64,
        body: impl FnOnce(&mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        anyhow::ensure!(!self.failed.get(), "native persistence channel failed");
        let mut buf = self.scratch.borrow_mut();
        let result = (|| {
            let at = wire::begin(&mut buf, kind, flags, received_us);
            body(&mut buf)?;
            wire::finish(&mut buf, at)?;
            self.sink.publish(&buf)
        })();
        let mut stats = self.stats.get();
        match &result {
            Ok(()) => stats.published += 1,
            Err(_) => {
                stats.failures += 1;
                self.failed.set(true);
            }
        }
        self.stats.set(stats);
        result.context("publish native persistence record")
    }

    fn json(&self, display: bool, record: &impl Serialize) -> Result<()> {
        if !display {
            return Ok(());
        }
        self.publish(Kind::Json, DISPLAY, now_us(), |buf| {
            Ok(serde_json::to_writer(buf, record)?)
        })
    }

    /// Rare lifecycle/operator line for the trader log; never stored.
    /// Redaction happens here, before any byte leaves the process.
    pub fn record(&self, mut value: Value, display: bool) -> Result<()> {
        redact(&mut value, &self.redactions);
        self.json(display, &value)
    }

    /// Bincode of a borrowed record; persist_manager renders it.
    fn binary(&self, kind: Kind, display: bool, record: &impl Serialize) -> Result<()> {
        use bincode::Options;
        if !display {
            return Ok(());
        }
        self.publish(kind, DISPLAY, now_us(), |buf| {
            Ok(wire::bincode().serialize_into(buf, record)?)
        })
    }

    pub fn quote_decision(
        &self,
        note: &crate::ctp::exec::WaitNote,
        quote_received_us: u64,
        quote_generation: u64,
    ) -> Result<()> {
        self.binary(
            Kind::QuoteDecision,
            true,
            &wire::QuoteDecision {
                source_id: note.target.source_id.as_str(),
                batch_id: note.target.batch_id.as_str(),
                target_revision: note.target.target_revision,
                market: note.market.as_str(),
                reason: note.reason,
                quote_received_us,
                quote_generation,
            },
        )
    }

    pub fn target_accepted(&self, target: &crate::ctp::exec::Target) -> Result<()> {
        #[derive(Serialize)]
        struct Record<'a> {
            r#type: &'static str,
            target: &'a crate::ctp::exec::Target,
            venue_accepted: bool,
        }
        self.json(
            true,
            &Record {
                r#type: "exec_target_accepted",
                target,
                venue_accepted: false,
            },
        )
    }

    #[cfg(feature = "ctp-sdk")]
    pub fn target_observed(&self, target: &crate::ctp::exec::Target) -> Result<()> {
        #[derive(Serialize)]
        struct Record<'a> {
            r#type: &'static str,
            target: &'a crate::ctp::exec::Target,
            execution_enabled: bool,
        }
        self.json(
            true,
            &Record {
                r#type: "exec_target_observed",
                target,
                execution_enabled: false,
            },
        )
    }

    pub fn strategy_result(
        &self,
        outcome: &crate::ctp::strategy::ActionOutcome,
        display: bool,
    ) -> Result<()> {
        use crate::ctp::strategy::ActionOutcome;
        use std::borrow::Cow;
        use wire::StrategyResult;
        let record = match outcome {
            ActionOutcome::Submitted { tag, receipt } => StrategyResult::SubmitReceipt {
                tag: *tag,
                receipt: Cow::Borrowed(receipt),
            },
            ActionOutcome::CancelledLocally { tag, key } => {
                StrategyResult::CancelSubmittedLocally {
                    tag: *tag,
                    key: *key,
                }
            }
            ActionOutcome::Rejected { tag, error } => StrategyResult::ActionRejected {
                tag: *tag,
                error: redact_text(error, &self.redactions),
            },
            ActionOutcome::CancelRejected { tag, key, error } => StrategyResult::CancelRejected {
                tag: *tag,
                key: *key,
                error: redact_text(error, &self.redactions),
            },
            #[cfg(any(feature = "ctp-sdk", test))]
            ActionOutcome::CtpDispatchBlocked {
                action,
                checks,
                quote_received_us,
                quote_generation,
            } => {
                use crate::ctp::strategy::Action as Native;
                use wire::ActionRecord;
                StrategyResult::DispatchBlocked {
                    retryable: checks.can_retry(),
                    checks: *checks,
                    action: match action {
                        Native::Submit { tag, intent } => ActionRecord::Submit {
                            tag: *tag,
                            intent: *intent,
                        },
                        Native::SubmitExec {
                            tag,
                            intent,
                            target,
                        } => ActionRecord::SubmitExec {
                            tag: *tag,
                            intent: *intent,
                            source_id: Cow::Borrowed(&target.source_id),
                            batch_id: Cow::Borrowed(&target.batch_id),
                            target_revision: target.target_revision,
                            trading_day: Cow::Borrowed(&target.trading_day),
                            expires_at_us: target.expires_at_us,
                        },
                        Native::Cancel { tag, key } => ActionRecord::Cancel {
                            tag: *tag,
                            key: *key,
                        },
                    },
                    quote_received_us: *quote_received_us,
                    quote_generation: *quote_generation,
                }
            }
        };
        self.binary(Kind::StrategyResult, display, &record)
    }

    pub fn yida_event(
        &self,
        event: &crate::yida::RawEvent,
        received_us: u64,
        shutdown_tail: bool,
    ) -> Result<()> {
        // Order, fill and error callbacks are operator facts; account and
        // catalogue rows only feed in-memory state.
        if event.error == 0 && !matches!(event.kind, crate::yida::ORDER | crate::yida::TRADE) {
            return Ok(());
        }
        let flags = DISPLAY | if shutdown_tail { SHUTDOWN_TAIL } else { 0 };
        self.publish(Kind::Yida, flags, received_us, |buf| {
            buf.extend_from_slice(bytemuck::bytes_of(event));
            Ok(())
        })
    }

    pub fn ctp_event(
        &self,
        event: &crate::ctp::ingress::Event,
        received_us: u64,
        shutdown_tail: bool,
    ) -> Result<()> {
        // Successful catalogue rows only refresh in-memory contract rules.
        if event.kind == crate::ctp::INSTRUMENT && event.error == 0 {
            return Ok(());
        }
        // Order, fill, cancel and error callbacks are low-volume operator
        // facts; account, position and rate query rows remain audit-only.
        let display = event.error != 0
            || matches!(
                event.kind,
                crate::ctp::ORDER
                    | crate::ctp::TRADE
                    | crate::ctp::INSERT_RESPONSE
                    | crate::ctp::CANCEL_RESPONSE
                    | crate::ctp::ERROR
            );
        // Account, position and rate query rows only feed in-memory state.
        if !display {
            return Ok(());
        }
        let flags = DISPLAY | if shutdown_tail { SHUTDOWN_TAIL } else { 0 };
        let redact = event.error != 0 && !self.redactions.is_empty();
        match event {
            crate::ctp::ingress::Event::Hot(hot) => {
                self.publish(Kind::CtpHot, flags, received_us, |buf| {
                    if redact {
                        let mut copy = *hot;
                        redact_bytes(&mut copy.message, &self.redactions);
                        buf.extend_from_slice(bytemuck::bytes_of(&copy));
                    } else {
                        buf.extend_from_slice(bytemuck::bytes_of(hot));
                    }
                    Ok(())
                })
            }
            crate::ctp::ingress::Event::Control(control) => {
                self.publish(Kind::CtpControl, flags, received_us, |buf| {
                    if redact {
                        let mut copy: crate::ctp::RawEvent = **control;
                        redact_bytes(&mut copy.hot.message, &self.redactions);
                        buf.extend_from_slice(bytemuck::bytes_of(&copy));
                    } else {
                        buf.extend_from_slice(bytemuck::bytes_of(&**control));
                    }
                    Ok(())
                })
            }
        }
    }

    fn uniform(&self, record: &UniformOrderRef<'_>) -> Result<()> {
        let now = now_us();
        self.publish(Kind::UniformOrder, DISPLAY, now, |buf| {
            native_codec::write_stored_uniform_order(buf, now as i64, "", record);
            Ok(())
        })
    }

    /// Lifecycle record of an order just handed to the SDK (status New, no fill).
    /// Called right after the SDK call; it never delays the order.
    pub fn prepared(
        &self,
        day: Day,
        p: &PreparedOrder,
        target: Option<&crate::ctp::exec::Target>,
    ) -> Result<()> {
        let from_key = match target {
            Some(target) => FromKey::format(format_args!("{EXEC_FROM_KEY}:{}", target.batch_id))?,
            None => FromKey::new(EXEC_FROM_KEY)?,
        };
        let order = crate::model::Order {
            request: p.intent,
            key: p.key,
            system_id: Default::default(),
            limit_price: p.limit_price,
            status: crate::model::OrderStatus::Pending,
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
        self.lifecycle(day, OrderView::from(&order), Some(from_key))
    }

    pub fn prepared_cancel(&self, p: &crate::model::PreparedCancel) -> Result<()> {
        self.record(
            serde_json::json!({"type":"prepared_cancel","key":p.key,"venue":p.venue,
                "symbol":p.symbol,"system_id":p.system_id,"execution_route":p.execution_route}),
            true,
        )
    }

    /// Publishes the order's lifecycle record when its status or quantities changed.
    fn lifecycle(&self, day: Day, order: OrderView<'_>, from_key: Option<FromKey>) -> Result<()> {
        let identity = PersistedOrderKey {
            trading_day: day,
            venue: order.request.venue,
            native: *order.key,
        };
        let id = identity.client_order_id();
        let status = order.status.uniform(order.filled).to_u8();
        let now = now_us() as i64;
        let previous = self.emitted.borrow().get(&id).copied();
        if previous.is_some_and(|e| {
            e.status == status && e.filled == order.filled && e.cancelled == order.cancelled
        }) {
            return Ok(());
        }
        let emitted = Emitted {
            create_us: previous.map_or(now, |e| e.create_us),
            status,
            filled: order.filled,
            cancelled: order.cancelled,
            from_key: previous
                .map(|e| e.from_key)
                .or(from_key)
                .unwrap_or(if order.owned {
                    FromKey::new(EXEC_FROM_KEY)?
                } else {
                    FromKey::new(OBSERVED_FROM_KEY)?
                }),
        };
        let text = identity.client_order_text();
        let record = order_record(&identity, &order, &emitted, &text, now, OrderFill::None);
        self.uniform(&record)?;
        self.emitted.borrow_mut().insert(id, emitted);
        Ok(())
    }

    pub fn orders(&self, core: &crate::core::TradingCore, keys: &[OrderKey]) -> Result<()> {
        for key in keys {
            if let Some(order) = core.order(key) {
                self.lifecycle(core.trading_day(), order, None)?;
            }
        }
        Ok(())
    }

    /// Lifecycle of every changed order; used on rejection, recovery and shutdown.
    /// Leaves the business loop for a cold restart (CTP and Yida alike): the core
    /// stops trading, open order state and the reason are recorded, venue orders
    /// are left untouched. Returns the error the runtime propagates; a journal
    /// failure wins, so it exits for an operator instead of restarting.
    pub fn restart_required(
        &self,
        core: &mut crate::core::TradingCore,
        restart: crate::restart::ColdRestartRequired,
        detail: Value,
    ) -> anyhow::Error {
        core.disconnect();
        let recorded = self.state(core).and_then(|()| {
            self.record(
                serde_json::json!({"type":"restart_required","reason":restart.reason(),"detail":detail,"venue_orders_cancelled":false}),
                true,
            )
        });
        match recorded {
            Ok(()) => restart.into(),
            Err(error) => error,
        }
    }

    pub fn state(&self, core: &crate::core::TradingCore) -> Result<()> {
        for order in core.view().orders {
            self.lifecycle(core.trading_day(), order, None)?;
        }
        Ok(())
    }

    pub fn command_state(
        &self,
        core: &crate::core::TradingCore,
        result: &Result<Value>,
        mut keys: Vec<OrderKey>,
    ) -> Result<()> {
        if let Ok(value) = result {
            if let Some(children) = value["receipt"]["children"].as_array() {
                for child in children {
                    let key: OrderKey = serde_json::from_value(child["key"].clone())?;
                    if !keys.contains(&key) {
                        keys.push(key);
                    }
                }
            }
        }
        self.orders(core, &keys)
    }

    /// A factual fill new to this store: one trade update, then the fill split
    /// into uniform fills by strategy (`allocation`; otherwise wholly
    /// `__unallocated__`), then the order's lifecycle. `commission` is the
    /// venue-reported fee; without one the contract's queried rates price it.
    pub fn fill(
        &self,
        core: &crate::core::TradingCore,
        fill: &Fill,
        commission: Option<f64>,
        received_us: u64,
        allocation: Option<&crate::ctp::exec::FillAllocation>,
    ) -> Result<()> {
        let trade_key = (
            fill.identity.venue,
            fill.identity.trading_day,
            fill.identity.trade_id,
        );
        if !self.previous.borrow_mut().known_trades.insert(trade_key) {
            return Ok(());
        }
        let key = core.fill_order_key(fill).copied();
        let order = key.and_then(|key| core.order(&key));
        let identity = PersistedOrderKey {
            trading_day: fill.identity.trading_day,
            venue: fill.identity.venue,
            native: key.unwrap_or(OrderKey {
                session: Default::default(),
                order_ref: fill.order_ref,
            }),
        };
        let text = identity.client_order_text();
        let commission = commission.or_else(|| {
            let rule = core.instrument(fill.identity.venue, &fill.identity.symbol)?;
            rule.commission?
                .estimate(fill.offset, fill.price, rule.contract_multiple, fill.lots)
                .ok()
        });
        let trade = NativeTrade {
            fill,
            client_order_id: identity.client_order_id(),
            order_id: fill.system_id.trim().parse().unwrap_or(0),
            cumulative: order.map_or(fill.lots, |o| o.filled),
            status: order.map(|o| o.status.uniform(o.filled)),
            commission,
            received_us: received_us as i64,
            text: &text,
        };
        self.publish(Kind::TradeUpdate, DISPLAY, received_us, |buf| {
            native_codec::write_trade_update(
                buf,
                &trade,
                received_us as i64,
                &fill.identity.symbol,
                f64::from(trade.cumulative),
            );
            Ok(())
        })?;
        let signed = i64::from(fill.lots)
            * if fill.identity.side == Side::Buy {
                1
            } else {
                -1
            };
        let whole;
        let rows = match allocation.filter(|a| a.trade == fill.identity) {
            Some(allocation) => &allocation.allocations,
            None => {
                whole = Row::from([(UNALLOCATED.to_owned(), signed)]);
                &whole
            }
        };
        let now = now_us() as i64;
        let emitted = order.map(|o| {
            let id = identity.client_order_id();
            self.emitted.borrow().get(&id).copied().unwrap_or(Emitted {
                create_us: now,
                status: o.status.uniform(o.filled).to_u8(),
                filled: o.filled,
                cancelled: o.cancelled,
                from_key: FromKey::default(),
            })
        });
        for (strategy, lots) in rows {
            if *lots == 0 {
                continue;
            }
            let share = commission.map(|c| c * lots.unsigned_abs() as f64 / f64::from(fill.lots));
            let record = match (order.as_ref(), emitted.as_ref()) {
                (Some(order), Some(emitted)) => order_record(
                    &identity,
                    order,
                    emitted,
                    &text,
                    now,
                    OrderFill::Fill {
                        fill,
                        strategy,
                        lots: lots.unsigned_abs(),
                        commission: share,
                    },
                ),
                _ => untracked_fill(
                    &identity,
                    fill,
                    strategy,
                    lots.unsigned_abs(),
                    share,
                    &text,
                    now,
                ),
            };
            self.uniform(&record)?;
        }
        if let Some(order) = order {
            self.lifecycle(fill.identity.trading_day, order, None)?;
        }
        Ok(())
    }

    /// Records strategy-ledger changes: a fill's allocation travels with that
    /// fill (pass it as `fill`), crosses become internal-cross uniform orders.
    pub fn ledger(
        &self,
        core: &crate::core::TradingCore,
        events: &[crate::ctp::exec::LedgerEvent],
        fill: Option<(&Fill, Option<f64>, u64)>,
    ) -> Result<()> {
        use crate::ctp::exec::LedgerEvent;
        if let Some((fill, commission, received_us)) = fill {
            let allocation = events.iter().find_map(|event| match event {
                LedgerEvent::StrategyFillAllocation(a) if a.trade == fill.identity => Some(a),
                _ => None,
            });
            self.fill(core, fill, commission, received_us, allocation)?;
        }
        for event in events {
            if let LedgerEvent::StrategyCross(cross) = event {
                self.cross(core.trading_day(), cross)?;
            }
        }
        Ok(())
    }

    /// Internal cross: one synthetic filled uniform order per transfer leg at
    /// the mid, `ttype` internal cross, like crypto BatchExec.
    pub fn cross(&self, day: Day, cross: &crate::ctp::exec::StrategyCross) -> Result<()> {
        let (exchange, symbol) = cross
            .market
            .split_once(':')
            .context("cross market without venue")?;
        let venue = Venue::parse(exchange)?;
        let now = now_us() as i64;
        for (index, transfer) in cross.transfers.iter().enumerate() {
            for (side, strategy) in [(Side::Buy, &transfer.buyer), (Side::Sell, &transfer.seller)] {
                let identity = PersistedOrderKey {
                    trading_day: day,
                    venue,
                    native: OrderKey {
                        session: Text::format(format_args!("cross:{}", cross.quote_received_us))?,
                        order_ref: Text::format(format_args!(
                            "{index}{}",
                            if side == Side::Buy { "b" } else { "s" }
                        ))?,
                    },
                };
                let lots = transfer.lots as f64;
                self.uniform(&UniformOrderRef {
                    symbol: symbol.as_bytes(),
                    create_ts: now,
                    update_ts: now,
                    signal_ts: cross.quote_received_us as i64,
                    submit_ts: now,
                    local_ts: now,
                    mkt_ts: cross.quote_received_us as i64,
                    client_order_id: identity.client_order_id(),
                    venue: venue.trading_venue().to_u8(),
                    ttype: UNIFORM_ORDER_TYPE_INTERNAL_CROSS,
                    side: uniform_side(side),
                    price: cross.price,
                    price_offset: 0.0,
                    amount_init: lots,
                    amount_update: lots,
                    status: order_common::OrderStatus::Filled.to_u8(),
                    from_key: strategy.as_bytes(),
                    signal_bbo: None,
                    fill_liquidity: FillLiquidity::Unknown,
                    extension: ExtensionRef {
                        trading_day: Some(&day),
                        ..ExtensionRef::default()
                    },
                })?;
            }
        }
        Ok(())
    }

    /// Strategy attribution persisted by an earlier process; read once at startup.
    pub fn take_previous_ledger(&self) -> Vec<(String, Row)> {
        std::mem::take(&mut self.previous.borrow_mut().ledger)
    }

    /// Complete venue recovery. Requests a previous process left open must be
    /// present; recovered fills new to the store are recorded unallocated.
    pub fn reconciled(
        &self,
        core: &mut crate::core::TradingCore,
        trades: Vec<crate::model::Fill>,
    ) -> Result<()> {
        let mut previous = self.previous.borrow_mut();
        core.apply_reference_floors(&previous.reference_floors)?;
        let day = core.trading_day();
        for key in &previous.unresolved {
            anyhow::ensure!(
                key.trading_day != day || core.order(&key.native).is_some(),
                "durable local order absent from complete recovery: {} {} session={} ref={}; reconciliation required",
                key.trading_day,
                key.venue.as_str(),
                key.native.session,
                key.native.order_ref
            );
        }
        previous.unresolved.clear();
        drop(previous);
        self.state(core)?;
        let received_us = now_us();
        for fill in &trades {
            self.fill(core, fill, None, received_us, None)?;
        }
        self.record(
            serde_json::json!({"type":"reconciled","state":core.view()}),
            true,
        )
    }

    pub fn finish(self) -> Result<()> {
        self.record(serde_json::json!({"type":"persist_closed"}), true)?;
        anyhow::ensure!(
            !self.failed.get(),
            "native persistence channel failed before close"
        );
        Ok(())
    }
}

fn now_us() -> u64 {
    crate::persistence::now_us()
}

fn uniform_side(side: Side) -> u8 {
    match side {
        Side::Buy => order_common::Side::Buy.to_u8(),
        Side::Sell => order_common::Side::Sell.to_u8(),
    }
}

fn order_type(price_type: crate::model::PriceType) -> u8 {
    match price_type {
        crate::model::PriceType::Limit => OrderType::Limit.to_u8(),
        crate::model::PriceType::Market => OrderType::Market.to_u8(),
    }
}

enum OrderFill<'a> {
    None,
    Fill {
        fill: &'a Fill,
        strategy: &'a str,
        lots: u64,
        commission: Option<f64>,
    },
}

fn order_record<'a>(
    identity: &'a PersistedOrderKey,
    order: &'a OrderView<'a>,
    emitted: &'a Emitted,
    text: &'a ClientOrderText,
    now: i64,
    fill: OrderFill<'a>,
) -> UniformOrderRef<'a> {
    let (price, amount_update, from_key, extension) = match fill {
        OrderFill::None => (
            order.limit_price,
            0.0,
            emitted.from_key.as_bytes(),
            ExtensionRef {
                position_effect: Some(order.request.offset.position_effect()),
                trading_day: Some(&identity.trading_day),
                client_order_text: Some(text),
                ..ExtensionRef::default()
            },
        ),
        OrderFill::Fill {
            fill,
            strategy,
            lots,
            commission,
        } => (
            fill.price,
            lots as f64,
            strategy.as_bytes(),
            ExtensionRef {
                position_effect: Some(fill.offset.position_effect()),
                trading_day: Some(&fill.identity.trading_day),
                trade_id: Some(&fill.identity.trade_id),
                last_filled_quantity: Some(f64::from(fill.lots)),
                commission,
                client_order_text: Some(text),
            },
        ),
    };
    UniformOrderRef {
        symbol: order.request.symbol.as_bytes(),
        create_ts: emitted.create_us,
        update_ts: now,
        signal_ts: 0,
        submit_ts: if order.owned { emitted.create_us } else { 0 },
        local_ts: now,
        mkt_ts: 0,
        client_order_id: identity.client_order_id(),
        venue: order.request.venue.trading_venue().to_u8(),
        ttype: order_type(order.request.price_type),
        side: uniform_side(order.request.side),
        price,
        price_offset: 0.0,
        amount_init: f64::from(order.request.lots),
        amount_update,
        status: order.status.uniform(order.filled).to_u8(),
        from_key,
        signal_bbo: None,
        fill_liquidity: FillLiquidity::Unknown,
        extension,
    }
}

/// A fill whose order the OMS does not know (manual or foreign session).
fn untracked_fill<'a>(
    identity: &'a PersistedOrderKey,
    fill: &'a Fill,
    strategy: &'a str,
    lots: u64,
    commission: Option<f64>,
    text: &'a ClientOrderText,
    now: i64,
) -> UniformOrderRef<'a> {
    UniformOrderRef {
        symbol: fill.identity.symbol.as_bytes(),
        create_ts: now,
        update_ts: now,
        signal_ts: 0,
        submit_ts: 0,
        local_ts: now,
        mkt_ts: 0,
        client_order_id: identity.client_order_id(),
        venue: fill.identity.venue.trading_venue().to_u8(),
        ttype: OrderType::Limit.to_u8(),
        side: uniform_side(fill.identity.side),
        price: fill.price,
        price_offset: 0.0,
        amount_init: f64::from(fill.lots),
        amount_update: lots as f64,
        status: order_common::OrderStatus::PartiallyFilled.to_u8(),
        from_key: strategy.as_bytes(),
        signal_bbo: None,
        fill_liquidity: FillLiquidity::Unknown,
        extension: ExtensionRef {
            position_effect: Some(fill.offset.position_effect()),
            trading_day: Some(&fill.identity.trading_day),
            trade_id: Some(&fill.identity.trade_id),
            last_filled_quantity: Some(f64::from(fill.lots)),
            commission,
            client_order_text: Some(text),
        },
    }
}

/// A domestic fill as the crypto `TradeUpdate`; quantities are lots.
struct NativeTrade<'a> {
    fill: &'a Fill,
    client_order_id: i64,
    order_id: i64,
    cumulative: i32,
    status: Option<order_common::OrderStatus>,
    commission: Option<f64>,
    received_us: i64,
    text: &'a ClientOrderText,
}
impl TradeUpdate for NativeTrade<'_> {
    fn event_time(&self) -> i64 {
        self.received_us
    }
    // Domestic SDK trade times are local wall-clock text or native integers,
    // not Unix time; the receipt time is the comparable instant.
    fn trade_time(&self) -> i64 {
        self.received_us
    }
    fn symbol(&self) -> &str {
        &self.fill.identity.symbol
    }
    fn order_id(&self) -> i64 {
        self.order_id
    }
    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }
    fn side(&self) -> order_common::Side {
        match self.fill.identity.side {
            Side::Buy => order_common::Side::Buy,
            Side::Sell => order_common::Side::Sell,
        }
    }
    fn price(&self) -> f64 {
        self.fill.price
    }
    fn is_maker(&self) -> bool {
        false
    }
    fn trading_venue(&self) -> TradingVenue {
        self.fill.identity.venue.trading_venue()
    }
    fn cumulative_filled_quantity(&self) -> f64 {
        f64::from(self.cumulative)
    }
    fn order_status(&self) -> Option<order_common::OrderStatus> {
        self.status
    }
    fn position_effect(&self) -> Option<PositionEffect> {
        Some(self.fill.offset.position_effect())
    }
    fn trading_day(&self) -> Option<&str> {
        Some(&self.fill.identity.trading_day)
    }
    fn trade_id(&self) -> Option<&str> {
        Some(&self.fill.identity.trade_id)
    }
    fn last_filled_quantity(&self) -> Option<f64> {
        Some(f64::from(self.fill.lots))
    }
    fn commission(&self) -> Option<f64> {
        self.commission
    }
    fn client_order_text(&self) -> Option<&str> {
        Some(self.text)
    }
}

pub(crate) fn redact(value: &mut Value, secrets: &[String]) {
    match value {
        Value::String(text) => {
            for secret in secrets {
                if !secret.is_empty() && text.contains(secret.as_str()) {
                    *text = text.replace(secret.as_str(), "[redacted]");
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact(value, secrets);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                redact(value, secrets);
            }
        }
        _ => {}
    }
}

fn redact_text<'a>(text: &'a str, secrets: &[String]) -> std::borrow::Cow<'a, str> {
    let mut out = std::borrow::Cow::Borrowed(text);
    for secret in secrets {
        if out.contains(secret.as_str()) {
            out = std::borrow::Cow::Owned(out.replace(secret.as_str(), "[redacted]"));
        }
    }
    out
}

/// Masks secrets inside a fixed SDK text field without changing its size.
fn redact_bytes(field: &mut [u8], secrets: &[String]) {
    for secret in secrets {
        let needle = secret.as_bytes();
        if needle.is_empty() || needle.len() > field.len() {
            continue;
        }
        let mut at = 0;
        while at + needle.len() <= field.len() {
            if &field[at..at + needle.len()] == needle {
                field[at..at + needle.len()].fill(b'*');
                at += needle.len();
            } else {
                at += 1;
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
