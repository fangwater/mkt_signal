use crate::model::*;
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Not Send/Sync: only the business-loop thread may own or mutate this state.
pub struct TradingCore {
    limits: RiskLimits,
    day: String,
    front: i32,
    session: i32,
    next_ref: u64,
    ready: bool,
    connected: bool,
    reason: String,
    cash: f64,
    instruments: HashMap<String, Instrument>,
    continuous: HashMap<String, bool>,
    positions: HashMap<(String, Side), Position>,
    orders: HashMap<OrderKey, Order>,
    system_ids: HashMap<String, OrderKey>,
    fills: HashMap<TradeIdentity, Fill>,
    actions: VecDeque<Instant>,
    _single_owner: PhantomData<Rc<()>>,
}

#[derive(Debug, Serialize)]
pub struct CoreView<'a> {
    pub ready: bool,
    pub connected: bool,
    pub reason: &'a str,
    pub trading_day: &'a str,
    pub available_local_budget: f64,
    pub orders: Vec<&'a Order>,
    pub positions: Vec<PositionView<'a>>,
}
#[derive(Debug, Serialize)]
pub struct PositionView<'a> {
    pub symbol: &'a str,
    pub side: Side,
    pub position: &'a Position,
}

impl TradingCore {
    pub fn new(limits: RiskLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            day: String::new(),
            front: 0,
            session: 0,
            next_ref: 1,
            ready: false,
            connected: false,
            reason: "not recovered".into(),
            cash: 0.0,
            instruments: HashMap::new(),
            continuous: HashMap::new(),
            positions: HashMap::new(),
            orders: HashMap::new(),
            system_ids: HashMap::new(),
            fills: HashMap::new(),
            actions: VecDeque::new(),
            _single_owner: PhantomData,
        })
    }
    pub fn is_ready(&self) -> bool {
        self.ready && self.connected
    }
    pub fn block(&mut self, reason: impl Into<String>) {
        self.ready = false;
        self.reason = reason.into();
    }
    pub fn disconnect(&mut self) {
        self.connected = false;
        self.block("CTP disconnected; reconciliation required");
    }
    pub fn connected(&mut self) {
        self.connected = true;
        self.continuous.clear();
        self.block("session/bootstrap incomplete");
    }
    pub fn order(&self, key: &OrderKey) -> Option<&Order> {
        self.orders.get(key)
    }
    pub fn position(&self, symbol: &str, side: Side) -> Position {
        self.positions
            .get(&(symbol.to_owned(), side))
            .cloned()
            .unwrap_or_default()
    }
    pub fn view(&self) -> CoreView<'_> {
        CoreView {
            ready: self.is_ready(),
            connected: self.connected,
            reason: &self.reason,
            trading_day: &self.day,
            available_local_budget: self.cash,
            orders: self.orders.values().collect(),
            positions: self
                .positions
                .iter()
                .map(|((symbol, side), position)| PositionView {
                    symbol,
                    side: *side,
                    position,
                })
                .collect(),
        }
    }
    pub fn set_trading_status(&mut self, symbol_or_product: &str, continuous: bool) {
        self.continuous
            .insert(symbol_or_product.to_owned(), continuous);
    }
    /// Apply only a complete new rule set; omission immediately removes trading permission.
    pub fn replace_instruments(&mut self, instruments: Vec<Instrument>) -> Result<()> {
        let mut next = HashMap::new();
        for rule in instruments {
            rule.validate()?;
            ensure!(
                !next.contains_key(&rule.symbol),
                "duplicate instrument in rule refresh"
            );
            next.insert(rule.symbol.clone(), rule);
        }
        self.instruments = next;
        Ok(())
    }
    /// A cash poll may tighten the budget. Only a full reconciliation can replenish it.
    pub fn constrain_cash(&mut self, available: f64) -> Result<()> {
        ensure!(available.is_finite(), "nonfinite cash refresh");
        self.cash = self.cash.min(available);
        Ok(())
    }
    fn trading_status(&self, symbol: &str) -> bool {
        self.continuous
            .get(symbol)
            .or_else(|| {
                let n = symbol.len().saturating_sub(4);
                symbol.get(..n).and_then(|p| self.continuous.get(p))
            })
            .copied()
            .unwrap_or(false)
    }

    /// Caller must supply a complete, reconciled account snapshot, never query fragments.
    /// Validation and reconstruction are atomic with respect to the existing core.
    pub fn restore(&mut self, s: Snapshot) -> Result<()> {
        validate_snapshot(&s)?;
        let mut next = Self::new(self.limits.clone())?;
        next.continuous = self.continuous.clone();
        next.day = s.trading_day;
        next.front = s.front_id;
        next.session = s.session_id;
        next.next_ref = s
            .max_order_ref
            .checked_add(1)
            .context("order ref exhausted")?;
        if self.day == next.day && self.front == next.front && self.session == next.session {
            // Login MaxOrderRef can predate locally rejected attempts and manual reconciliation.
            next.next_ref = next.next_ref.max(self.next_ref);
        }
        ensure!(
            next.next_ref <= 999_999_999_999,
            "CTP order ref exceeds 12 digits"
        );
        next.cash = s.available_cash;
        next.instruments = s
            .instruments
            .into_iter()
            .map(|r| (r.symbol.clone(), r))
            .collect();
        for p in s.positions {
            next.positions.insert(
                (p.symbol, p.side),
                Position {
                    today: p.today,
                    yesterday: p.yesterday,
                    ..Position::default()
                },
            );
        }
        for f in s.trades {
            next.fills.insert(f.identity.clone(), f);
        }
        for update in s.orders {
            ensure!(
                !next.orders.contains_key(&update.key),
                "duplicate order identity in snapshot"
            );
            if update.key.front_id == next.front && update.key.session_id == next.session {
                if let Ok(observed) = update.key.order_ref.trim().parse::<u64>() {
                    next.next_ref = next.next_ref.max(
                        observed
                            .checked_add(1)
                            .context("observed order reference exhausted")?,
                    );
                    ensure!(
                        next.next_ref <= 999_999_999_999,
                        "observed order reference exceeds CTP capacity"
                    );
                }
            }
            let remaining = if update.status.terminal() {
                0
            } else {
                update.lots - update.traded
            };
            let mut order = Order {
                request: Intent {
                    symbol: update.symbol.clone(),
                    side: update.side,
                    offset: update.offset,
                    lots: update.lots,
                    price_ticks: 0,
                    time_in_force: TimeInForce::Day,
                },
                key: update.key.clone(),
                system_id: update.system_id,
                limit_price: update.limit_price,
                status: update.status,
                filled: update.traded,
                reported_filled: update.traded,
                cancelled: if update.status.terminal() {
                    update.lots - update.traded
                } else {
                    0
                },
                reserved_today: 0,
                reserved_yesterday: 0,
                local_cash_held: 0.0,
                cancel_pending: false,
            };
            if remaining > 0 && update.offset == Offset::Close {
                let (today, yesterday) =
                    next.allocate_close(&update.symbol, update.side.opposite(), remaining)?;
                order.reserved_today = today;
                order.reserved_yesterday = yesterday;
                next.adjust_frozen(&order, today, yesterday)?;
            }
            if !order.system_id.is_empty() {
                ensure!(
                    !next.system_ids.contains_key(&order.system_id),
                    "duplicate CFFEX system order identity"
                );
                next.system_ids
                    .insert(order.system_id.clone(), order.key.clone());
            }
            next.orders.insert(order.key.clone(), order);
        }
        next.connected = true;
        next.ready = true;
        next.reason.clear();
        *self = next;
        Ok(())
    }

    fn allocate_close(&self, symbol: &str, side: Side, lots: i32) -> Result<(i32, i32)> {
        let p = self.position(symbol, side);
        let today = p.today - p.frozen_today;
        let yesterday = p.yesterday - p.frozen_yesterday;
        ensure!(
            i64::from(lots) <= i64::from(today) + i64::from(yesterday),
            "insufficient available close position"
        );
        Ok(match self.limits.close_priority {
            ClosePriority::TodayFirst => {
                let t = lots.min(today);
                (t, lots - t)
            }
            ClosePriority::YesterdayFirst => {
                let y = lots.min(yesterday);
                (lots - y, y)
            }
        })
    }
    fn adjust_frozen(&mut self, order: &Order, today: i32, yesterday: i32) -> Result<()> {
        let p = self
            .positions
            .entry((order.request.symbol.clone(), order.request.side.opposite()))
            .or_default();
        p.frozen_today += today;
        p.frozen_yesterday += yesterday;
        p.validate()
    }
    fn rate_check(&mut self, now: Instant) -> Result<()> {
        while self
            .actions
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= Duration::from_secs(1))
        {
            self.actions.pop_front();
        }
        ensure!(
            self.actions.len() < self.limits.max_actions_per_second,
            "account action rate limit"
        );
        self.actions.push_back(now);
        Ok(())
    }

    pub fn prepare(&mut self, intent: Intent, now: Instant) -> Result<PreparedOrder> {
        ensure!(self.is_ready(), "trading blocked: {}", self.reason);
        let rule = self
            .instruments
            .get(&intent.symbol)
            .context("symbol absent from complete rule snapshot")?
            .clone();
        ensure!(
            rule.active && self.trading_status(&intent.symbol),
            "symbol inactive or outside continuous trading"
        );
        ensure!(
            intent.lots >= rule.min_lots
                && intent.lots <= rule.max_lots
                && intent.lots <= self.limits.max_order_lots,
            "invalid order quantity"
        );
        ensure!(intent.price_ticks > 0, "limit price must be positive ticks");
        ensure!(
            intent.time_in_force == TimeInForce::Day || self.limits.enable_fak_fok,
            "FAK/FOK not enabled for this tested venue/account"
        );
        let price = intent.price_ticks as f64 * rule.price_tick;
        ensure!(
            price.is_finite() && price > 0.0 && intent.price_ticks <= 9_007_199_254_740_991,
            "limit price overflow"
        );
        let working: i64 = self.orders.values().map(|o| i64::from(o.remaining())).sum();
        ensure!(
            working + i64::from(intent.lots) <= i64::from(self.limits.max_working_lots),
            "working lot limit"
        );
        ensure!(
            !self
                .orders
                .values()
                .any(|o| o.request.symbol == intent.symbol
                    && o.request.side != intent.side
                    && o.remaining() > 0
                    && match intent.side {
                        Side::Buy => price + rule.price_tick * 1e-6 >= o.limit_price,
                        Side::Sell => price - rule.price_tick * 1e-6 <= o.limit_price,
                    }),
            "self-trade prevention"
        );
        let mut today = 0;
        let mut yesterday = 0;
        let mut cash = 0.0;
        if intent.offset == Offset::Open {
            let pending: i64 = self
                .orders
                .values()
                .filter(|o| {
                    o.request.symbol == intent.symbol
                        && o.request.side == intent.side
                        && o.request.offset == Offset::Open
                })
                .map(|o| i64::from(o.remaining()))
                .sum();
            ensure!(
                self.position(&intent.symbol, intent.side).total()
                    + pending
                    + i64::from(intent.lots)
                    <= i64::from(self.limits.max_position_lots_per_side),
                "position limit"
            );
            cash = rule.cash_budget_per_open_lot * f64::from(intent.lots);
            ensure!(
                cash.is_finite() && self.cash - cash >= self.limits.minimum_cash_remaining,
                "local cash budget insufficient"
            );
        } else {
            (today, yesterday) =
                self.allocate_close(&intent.symbol, intent.side.opposite(), intent.lots)?;
        }
        ensure!(self.next_ref <= 999_999_999_999, "CTP order ref exhausted");
        self.rate_check(now)?;
        let key = OrderKey {
            front_id: self.front,
            session_id: self.session,
            order_ref: self.next_ref.to_string(),
        };
        self.next_ref += 1;
        let order = Order {
            request: intent.clone(),
            key: key.clone(),
            system_id: String::new(),
            limit_price: price,
            status: OrderStatus::Pending,
            filled: 0,
            reported_filled: 0,
            cancelled: 0,
            reserved_today: today,
            reserved_yesterday: yesterday,
            local_cash_held: cash,
            cancel_pending: false,
        };
        if intent.offset == Offset::Close {
            self.adjust_frozen(&order, today, yesterday)?;
        }
        self.cash -= cash;
        self.orders.insert(key.clone(), order);
        Ok(PreparedOrder {
            key,
            intent,
            limit_price: price,
        })
    }

    pub fn reject(&mut self, key: &OrderKey) -> Result<()> {
        let mut order = self
            .orders
            .get(key)
            .context("rejection without known order")?
            .clone();
        if order.status == OrderStatus::Rejected {
            return Ok(());
        }
        ensure!(
            order.filled == 0 && order.reported_filled == 0,
            "rejection conflicts with factual or reported fill"
        );
        let lots = order.request.lots;
        self.release_cancelled(&mut order, lots)?;
        order.status = OrderStatus::Rejected;
        order.cancel_pending = false;
        self.orders.insert(key.clone(), order);
        Ok(())
    }
    fn release_cancelled(&mut self, order: &mut Order, total_cancelled: i32) -> Result<()> {
        ensure!(
            total_cancelled >= order.cancelled
                && total_cancelled <= order.request.lots - order.filled,
            "inconsistent cancelled quantity"
        );
        let release = total_cancelled - order.cancelled;
        if order.request.offset == Offset::Open {
            let unit = self
                .instruments
                .get(&order.request.symbol)
                .map(|r| r.cash_budget_per_open_lot)
                .unwrap_or(0.0);
            let refund = (f64::from(release) * unit).min(order.local_cash_held);
            order.local_cash_held -= refund;
            self.cash += refund;
        } else {
            let (today, yesterday) = match self.limits.close_priority {
                ClosePriority::TodayFirst => {
                    let y = release.min(order.reserved_yesterday);
                    (release - y, y)
                }
                ClosePriority::YesterdayFirst => {
                    let t = release.min(order.reserved_today);
                    (t, release - t)
                }
            };
            ensure!(
                today <= order.reserved_today && yesterday <= order.reserved_yesterday,
                "close cancellation exceeds frozen inventory"
            );
            self.adjust_frozen(order, -today, -yesterday)?;
            order.reserved_today -= today;
            order.reserved_yesterday -= yesterday;
        }
        order.cancelled = total_cancelled;
        Ok(())
    }

    pub fn on_order(&mut self, update: OrderUpdate) -> Result<()> {
        ensure!(
            update.exchange == "CFFEX" && update.trading_day == self.day,
            "order outside current account day/scope"
        );
        let mut order = self
            .orders
            .get(&update.key)
            .context("untracked account order requires reconciliation")?
            .clone();
        ensure!(
            order.request.symbol == update.symbol
                && order.request.side == update.side
                && order.request.offset == update.offset
                && order.request.lots == update.lots,
            "order identity fields changed"
        );
        ensure!(
            update.traded >= 0 && update.traded <= update.lots,
            "invalid order cumulative fill"
        );
        if !update.system_id.is_empty() {
            ensure!(
                order.system_id.is_empty() || order.system_id == update.system_id,
                "system identity changed"
            );
            if let Some(existing) = self.system_ids.get(&update.system_id) {
                ensure!(existing == &update.key, "system identity collision");
            }
            order.system_id = update.system_id;
            self.system_ids
                .insert(order.system_id.clone(), update.key.clone());
        }
        order.reported_filled = order.reported_filled.max(update.traded).max(order.filled);
        if update.cancel_rejected {
            order.cancel_pending = false;
        }
        if update.status == OrderStatus::Rejected {
            return self.reject(&update.key);
        }
        if update.status == OrderStatus::Cancelled {
            let cancelled = order.request.lots - order.reported_filled;
            if cancelled >= order.cancelled {
                self.release_cancelled(&mut order, cancelled)?;
            }
            if order.status != OrderStatus::Filled {
                order.status = OrderStatus::Cancelled;
            }
            order.cancel_pending = false;
        } else if update.status == OrderStatus::Filled {
            ensure!(
                update.traded == order.request.lots && order.cancelled == 0,
                "all-traded conflicts with order quantities"
            );
            order.status = OrderStatus::Filled;
            order.cancel_pending = false;
        } else if !order.status.terminal() {
            order.status = update.status;
        }
        self.orders.insert(update.key, order);
        Ok(())
    }

    pub fn on_fill(&mut self, fill: Fill) -> Result<bool> {
        validate_fill(&fill, &self.day)?;
        if let Some(previous) = self.fills.get(&fill.identity) {
            ensure!(previous == &fill, "conflicting duplicate trade identity");
            return Ok(false);
        }
        let key = self
            .system_ids
            .get(&fill.system_id)
            .cloned()
            .or_else(|| {
                let key = OrderKey {
                    front_id: self.front,
                    session_id: self.session,
                    order_ref: fill.order_ref.clone(),
                };
                self.orders
                    .get(&key)
                    .filter(|o| o.system_id.is_empty() || o.system_id == fill.system_id)
                    .map(|_| key)
            })
            .context("untracked factual fill: preserve evidence and reconcile, never discard it")?;
        let mut order = self
            .orders
            .get(&key)
            .context("missing correlated order")?
            .clone();
        ensure!(
            fill.order_ref.is_empty() || fill.order_ref == order.key.order_ref,
            "fill order reference conflicts with exchange order identity"
        );
        ensure!(
            order.request.symbol == fill.identity.symbol
                && order.request.side == fill.identity.side
                && order.request.offset == fill.offset,
            "fill conflicts with order identity"
        );
        ensure!(
            fill.lots <= order.remaining(),
            "fill exceeds unaccounted order quantity"
        );
        let side = if fill.offset == Offset::Open {
            fill.identity.side
        } else {
            fill.identity.side.opposite()
        };
        let mut position = self.position(&fill.identity.symbol, side);
        if fill.offset == Offset::Open {
            position.today = position
                .today
                .checked_add(fill.lots)
                .context("position overflow")?;
        } else {
            let (today, yesterday) = match self.limits.close_priority {
                ClosePriority::TodayFirst => {
                    let t = fill.lots.min(order.reserved_today);
                    (t, fill.lots - t)
                }
                ClosePriority::YesterdayFirst => {
                    let y = fill.lots.min(order.reserved_yesterday);
                    (fill.lots - y, y)
                }
            };
            ensure!(
                today <= order.reserved_today && yesterday <= order.reserved_yesterday,
                "fill exceeds close reservation"
            );
            position.today -= today;
            position.yesterday -= yesterday;
            position.frozen_today -= today;
            position.frozen_yesterday -= yesterday;
            order.reserved_today -= today;
            order.reserved_yesterday -= yesterday;
        }
        position.validate()?;
        if !fill.system_id.is_empty() {
            ensure!(
                order.system_id.is_empty() || order.system_id == fill.system_id,
                "fill system identity changed"
            );
            order.system_id = fill.system_id.clone();
            self.system_ids.insert(fill.system_id.clone(), key.clone());
        }
        order.filled += fill.lots;
        if order.filled == order.request.lots {
            order.status = OrderStatus::Filled;
        }
        self.positions
            .insert((fill.identity.symbol.clone(), side), position);
        self.orders.insert(key, order);
        self.fills.insert(fill.identity.clone(), fill);
        Ok(true)
    }

    pub fn prepare_cancel(&mut self, key: &OrderKey, now: Instant) -> Result<PreparedCancel> {
        ensure!(self.connected, "cannot cancel while transport disconnected");
        let order = self.orders.get(key).context("unknown order to cancel")?;
        ensure!(
            !order.status.terminal() && !order.cancel_pending && order.remaining() > 0,
            "order not cancellable"
        );
        let request = PreparedCancel {
            key: key.clone(),
            symbol: order.request.symbol.clone(),
            system_id: order.system_id.clone(),
        };
        self.rate_check(now)?;
        self.orders
            .get_mut(key)
            .context("order disappeared")?
            .cancel_pending = true;
        Ok(request)
    }
    pub fn cancel_failed(&mut self, key: &OrderKey) {
        if let Some(order) = self.orders.get_mut(key) {
            order.cancel_pending = false;
        }
    }
}

/// Direct function calls on one thread: no IPC between risk, OMS and SDK submission.
pub struct SingleLoop<T: Trader> {
    pub core: TradingCore,
    pub trader: T,
    pub execute: bool,
}
impl<T: Trader> SingleLoop<T> {
    pub fn submit(&mut self, intent: Intent, now: Instant) -> Result<OrderKey> {
        self.submit_with(intent, now, |_| Ok(()))
    }
    pub fn submit_with(
        &mut self,
        intent: Intent,
        now: Instant,
        before_native: impl FnOnce(&PreparedOrder) -> Result<()>,
    ) -> Result<OrderKey> {
        ensure!(self.execute, "read-only runtime: submissions disabled");
        let request = self.core.prepare(intent, now)?;
        if let Err(error) = before_native(&request) {
            self.core.reject(&request.key)?;
            self.core.block("submission audit failed before SDK call");
            return Err(error.context("order was not submitted"));
        }
        match self.trader.insert(&request) {
            Ok(0) => Ok(request.key),
            Ok(code) => {
                self.core.reject(&request.key)?;
                bail!("CTP local insert rejected: {code}");
            }
            Err(error) => {
                self.core
                    .block("unknown submission outcome; reconcile before retry");
                Err(error.context("native submission outcome uncertain"))
            }
        }
    }
    pub fn cancel(&mut self, key: &OrderKey, now: Instant) -> Result<()> {
        self.cancel_with(key, now, |_| Ok(()))
    }
    pub fn cancel_with(
        &mut self,
        key: &OrderKey,
        now: Instant,
        before_native: impl FnOnce(&PreparedCancel) -> Result<()>,
    ) -> Result<()> {
        ensure!(self.execute, "read-only runtime: cancellations disabled");
        let request = self.core.prepare_cancel(key, now)?;
        if let Err(error) = before_native(&request) {
            self.core.cancel_failed(key);
            self.core.block("cancellation audit failed before SDK call");
            return Err(error.context("cancellation was not submitted"));
        }
        match self.trader.cancel(&request) {
            Ok(0) => Ok(()),
            Ok(code) => {
                self.core.cancel_failed(key);
                bail!("CTP local cancel rejected: {code}");
            }
            Err(error) => {
                self.core.block("unknown cancellation outcome; reconcile");
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests;
