//! Correlated, throttled bootstrap. Two quiet identical position/order/trade passes
//! are required; this is snapshot convergence, not a vendor-provided atomic barrier.
use super::*;
use crate::config::Config;
use crate::restart::ColdRestartRequired;
use anyhow::{ensure, Context, Result};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// A full catalogue pass returns ~29k rows and re-queries every known rate.
/// Login/reconciliation always performs a complete pass; intraday contract
/// rules and rates change rarely, so the background refresh is infrequent to
/// avoid flooding the callback queue that gates order dispatch.
/// CTP `ErrorID` 7, 「CTP:还没有初始化」.
const CTP_NOT_INITIALIZED: i32 = 7;

pub(crate) const RULE_REFRESH_INTERVAL: Duration = Duration::from_secs(30 * 60);

pub struct Session {
    config: Config,
    investor: String,
    next_kind: Option<i32>,
    waiting: Option<(i32, i32, Instant)>,
    next_send: Instant,
    login: Option<(Day, i32, i32, u64)>,
    catalogue: HashMap<(Venue, Symbol), Instrument>,
    instruments: HashMap<(Venue, Symbol), Instrument>,
    rate_rule: Option<Instrument>,
    rate_retry_after: HashMap<(Venue, Symbol), Instant>,
    products: HashMap<(Venue, Symbol), Symbol>,
    rate_targets: VecDeque<(Venue, Symbol)>,
    rate_seen: bool,
    relative_margin: bool,
    account: Option<AccountSnapshot>,
    account_funds: Option<cn_futures_state::AccountFunds>,
    account_not_ready_retries: u8,
    settlement_confirmed: bool,
    available: Option<f64>,
    orders: Vec<OrderUpdate>,
    trades: Vec<Fill>,
    positions: HashMap<(Venue, Symbol, Side, i32), PositionSnapshot>,
    revision: u64,
    pass_revision: u64,
    previous: Option<(String, f64)>,
    passes: usize,
    ready: bool,
    maintenance: bool,
    last_cash_poll: Instant,
    last_rule_poll: Instant,
    cash_update: Option<f64>,
    rule_update: Option<Vec<Instrument>>,
    /// Background order query for unconfirmed orders: requested, then its rows.
    order_check_requested: bool,
    order_check: Option<Vec<OrderUpdate>>,
}
impl Session {
    pub fn new(config: Config, investor: String, now: Instant) -> Self {
        Self {
            config,
            investor,
            next_kind: None,
            waiting: None,
            next_send: now,
            login: None,
            catalogue: HashMap::new(),
            instruments: HashMap::new(),
            rate_rule: None,
            rate_retry_after: HashMap::new(),
            products: HashMap::new(),
            rate_targets: VecDeque::new(),
            rate_seen: false,
            relative_margin: false,
            account: None,
            account_funds: None,
            account_not_ready_retries: 0,
            settlement_confirmed: false,
            available: None,
            orders: Vec::new(),
            trades: Vec::new(),
            positions: HashMap::new(),
            revision: 0,
            pass_revision: 0,
            previous: None,
            passes: 0,
            ready: false,
            maintenance: false,
            last_cash_poll: now,
            last_rule_poll: now,
            cash_update: None,
            rule_update: None,
            order_check_requested: false,
            order_check: None,
        }
    }
    pub fn account(&self) -> Option<&AccountSnapshot> {
        self.account.as_ref()
    }
    pub fn account_funds(&self) -> Option<&cn_futures_state::AccountFunds> {
        self.account_funds.as_ref()
    }
    pub fn retries_account_query(&self, request: i32, error: i32) -> bool {
        error == 90
            && self.available.is_none()
            && self
                .waiting
                .is_some_and(|(kind, id, _)| kind == ACCOUNT && id == request)
    }
    pub fn settlement_confirmed(&self) -> bool {
        self.settlement_confirmed
    }
    pub fn require_active_future(&self, venue: Venue, symbol: &str) -> Result<()> {
        ensure!(
            self.next_kind != Some(INSTRUMENT),
            "CTP instrument catalogue refresh pending"
        );
        ensure!(
            self.catalogue.contains_key(&(venue, Symbol::new(symbol)?)),
            "CTP catalogue has no active future {}:{symbol}",
            venue.as_str()
        );
        Ok(())
    }
    pub fn request_rates(&mut self, venue: Venue, symbol: &str, now: Instant) -> Result<()> {
        let key = (venue, Symbol::new(symbol)?);
        if !self.instruments.contains_key(&key)
            && !self.rate_targets.contains(&key)
            && !self
                .rate_retry_after
                .get(&key)
                .is_some_and(|retry| now < *retry)
        {
            self.rate_targets.push_back(key);
        }
        Ok(())
    }
    pub fn query_target(&self) -> Option<(Venue, &str)> {
        self.rate_targets
            .front()
            .map(|(venue, symbol)| (*venue, symbol.as_str()))
    }
    pub fn ready(&self) -> bool {
        self.ready
    }
    pub fn timed_out(&self, now: Instant) -> bool {
        self.waiting.is_some_and(|(_, _, sent)| {
            now.saturating_duration_since(sent)
                > Duration::from_millis(self.config.response_timeout_ms)
        })
    }
    #[cfg(feature = "ctp-sdk")]
    pub(crate) fn account_query_in_flight(&self) -> bool {
        self.waiting.is_some_and(|(kind, _, _)| kind == ACCOUNT)
    }
    pub fn login_identity(&self) -> Option<(i32, i32)> {
        self.login.as_ref().map(|(_, f, s, _)| (*f, *s))
    }
    pub fn take_cash_update(&mut self) -> Option<f64> {
        self.cash_update.take()
    }
    pub fn take_rule_update(&mut self) -> Option<Vec<Instrument>> {
        self.rule_update.take()
    }
    /// Queues one complete order query ahead of other background queries.
    pub fn request_order_check(&mut self) {
        self.order_check_requested = true;
    }
    pub fn order_check_pending(&self) -> bool {
        self.order_check_requested || (self.maintenance && self.next_kind == Some(QUERY_ORDER))
    }
    /// Rows of a completed background order query.
    pub fn take_order_check(&mut self) -> Option<Vec<OrderUpdate>> {
        self.order_check.take()
    }
    pub fn request_due(&mut self, now: Instant) -> Result<Option<i32>> {
        if let Some((_, _, sent)) = self.waiting {
            ensure!(
                now.saturating_duration_since(sent)
                    <= Duration::from_millis(self.config.response_timeout_ms),
                "CTP response timeout; session must be recovered"
            );
            return Ok(None);
        }
        if self.ready && self.next_kind.is_none() {
            if self.order_check_requested {
                self.order_check_requested = false;
                self.maintenance = true;
                self.orders.clear();
                self.next_kind = Some(QUERY_ORDER);
            } else if now.saturating_duration_since(self.last_rule_poll) >= RULE_REFRESH_INTERVAL {
                self.maintenance = true;
                self.next_kind = Some(INSTRUMENT);
                self.catalogue.clear();
                self.products.clear();
            } else if now.saturating_duration_since(self.last_cash_poll) >= Duration::from_secs(5) {
                self.maintenance = true;
                self.next_kind = Some(ACCOUNT);
                self.available = None;
            } else if let Some(key) = self.rate_targets.front() {
                self.rate_rule = Some(
                    self.catalogue
                        .get(key)
                        .context("rate contract absent from catalogue")?
                        .clone(),
                );
                self.next_kind = Some(COMMISSION);
            }
        }
        Ok(if now >= self.next_send {
            self.next_kind
        } else {
            None
        })
    }
    pub fn sent(&mut self, kind: i32, id: i32, code: i32, now: Instant) -> Result<()> {
        ensure!(
            self.next_kind == Some(kind) && self.waiting.is_none(),
            "unexpected CTP control dispatch"
        );
        self.next_send = now + Duration::from_millis(self.config.query_interval_ms);
        match code {
            0 => {
                if kind == ACCOUNT {
                    self.pass_revision = self.revision;
                }
                self.waiting = Some((kind, id, now));
            }
            -2 | -3 => {} // Documented CTP in-flight/rate-limit rejection; retry with the configured spacing.
            _ => anyhow::bail!("CTP control request rejected locally ({code})"),
        }
        Ok(())
    }
    pub fn restart_queries(&mut self, now: Instant) -> Result<()> {
        ensure!(
            self.login.is_some() && self.waiting.is_none(),
            "cannot start reconciliation before login or while a query is in flight"
        );
        self.ready = false;
        self.maintenance = false;
        self.cash_update = None;
        self.rule_update = None;
        self.order_check_requested = false;
        self.order_check = None;
        self.previous = None;
        self.passes = 0;
        self.reset_rules();
        self.clear_pass();
        self.next_kind = Some(INSTRUMENT);
        self.next_send = now;
        Ok(())
    }
    fn reset_rules(&mut self) {
        self.catalogue.clear();
        self.instruments.clear();
        self.products.clear();
        self.rate_targets.clear();
        self.rate_retry_after.clear();
        self.rate_rule = None;
        self.rate_seen = false;
        self.relative_margin = false;
    }
    fn finish_rates(&mut self, now: Instant, complete: bool) -> Result<()> {
        let key = self
            .rate_targets
            .pop_front()
            .context("missing rate target")?;
        let rule = self.rate_rule.take().context("missing pending rates")?;
        if complete {
            rule.validate()?;
            self.instruments.insert(key, rule);
            self.rate_retry_after.remove(&key);
        } else {
            self.instruments.remove(&key);
            self.rate_retry_after
                .insert(key, now + Duration::from_secs(60));
        }
        self.rule_update = Some(self.instruments.values().cloned().collect());
        self.next_kind = None;
        self.rate_seen = false;
        self.relative_margin = false;
        Ok(())
    }
    fn clear_pass(&mut self) {
        self.available = None;
        self.orders.clear();
        self.trades.clear();
        self.positions.clear();
    }
    fn owns(&self, e: &HotEvent) -> Result<bool> {
        if text_ref(&e.investor)? != self.investor {
            return Ok(false);
        }
        Venue::parse(text_ref(&e.exchange)?)?;
        Ok(true)
    }

    pub fn on_event<'a>(
        &mut self,
        event: impl Into<super::ingress::EventRef<'a>>,
        now: Instant,
        decoded: &Result<DecodedUpdate>,
    ) -> Result<Option<Snapshot>> {
        let e = event.into();
        if e.kind == CONNECTED {
            self.ready = false;
            self.maintenance = false;
            self.cash_update = None;
            self.rule_update = None;
            self.order_check_requested = false;
            self.order_check = None;
            self.login = None;
            self.account = None;
            self.account_funds = None;
            self.account_not_ready_retries = 0;
            self.settlement_confirmed = false;
            self.reset_rules();
            self.waiting = None;
            self.previous = None;
            self.passes = 0;
            self.clear_pass();
            self.next_kind = Some(if self.config.require_authentication {
                AUTHENTICATED
            } else {
                LOGGED_IN
            });
            self.next_send = now;
            return Ok(None);
        }
        if e.kind == DISCONNECTED {
            self.ready = false;
            self.next_kind = None;
            self.waiting = None;
            self.login = None;
            self.account = None;
            self.account_funds = None;
            self.settlement_confirmed = false;
            self.reset_rules();
            return Ok(None);
        }
        if matches!(e.kind, ORDER | TRADE) {
            if self.owns(&e)? {
                self.revision = self
                    .revision
                    .checked_add(1)
                    .context("event revision overflow")?;
            }
            return Ok(None);
        }
        let Some((kind, id, _)) = self.waiting else {
            return Ok(None);
        };
        if e.request != id {
            return Ok(None);
        } // Late response stays correlated to its original request.
          // The broker explicitly asks to retry an account query that has delivered no rows.
          // Retain the last factual funds timestamp; never turn a retry into a fresh balance.
        if e.kind == ERROR && self.retries_account_query(e.request, e.error) {
            if self.account_not_ready_retries >= 3 {
                return Err(ColdRestartRequired::AccountQuery(
                    "CTP account query remained unavailable after three retries",
                )
                .into());
            }
            self.account_not_ready_retries += 1;
            self.waiting = None;
            self.next_send = now + Duration::from_millis(self.config.query_interval_ms.max(2000));
            return Ok(None);
        }
        // The front accepts the connection while its counter is still initialising
        // (post-close settlement); only a new SDK session can retry the bootstrap.
        if e.error == CTP_NOT_INITIALIZED {
            return Err(ColdRestartRequired::VenueNotInitialized.into());
        }
        ensure!(
            e.error == 0,
            "CTP request {id} failed, kind={kind}({}) error_code={} message={}",
            kind_name(kind),
            e.error,
            error_text(&e.message)
        );
        ensure!(
            e.kind == kind,
            "CTP response kind does not match in-flight request"
        );
        match kind {
            LOGGED_IN => {
                ensure!(e.present != 0, "login success without response fields");
                let day: Day = text(&e.trading_day)?;
                ensure!(day.len() == 8, "invalid login trading day");
                let order_ref = text_ref(&e.control()?.max_order_ref)?;
                let max_ref = if order_ref.trim().is_empty() {
                    0
                } else {
                    order_ref
                        .trim()
                        .parse::<u64>()
                        .context("nonnumeric login MaxOrderRef")?
                };
                self.login = Some((day, e.front, e.session, max_ref));
            }
            INSTRUMENT if e.present != 0 => {
                let control = e.control()?;
                if control.product_class == i32::from(b'1') && control.active != 0 {
                    if let Ok(venue) = Venue::parse(text_ref(&e.exchange)?) {
                        let symbol: Symbol = text(&e.symbol)?;
                        let expiry = control.expiry;
                        if is_future_symbol(venue, &symbol)
                            && self.login.as_ref().is_some_and(|login| {
                                login.0.parse::<i32>().is_ok_and(|day| expiry >= day)
                            })
                            && control.price_tick.is_finite()
                            && control.price_tick > 0.0
                            && control.contract_multiple > 0
                            && control.minimum > 0
                            && control.maximum >= control.minimum
                        {
                            let instrument = Instrument {
                                option: None,
                                venue,
                                symbol,
                                price_tick: control.price_tick,
                                contract_multiple: control.contract_multiple,
                                expiry_date: expiry,
                                commission: None,
                                margin: None,
                                min_lots: control.minimum,
                                max_lots: control.maximum,
                                market_lots: None,
                                active: true,
                                cash_budget_per_open_lot: 0.0,
                            };
                            let key = (venue, symbol);
                            let product: Symbol = text(&control.product)?;
                            if let Some(old) = self.products.insert(key, product) {
                                ensure!(old == product, "conflicting duplicate instrument product");
                            }
                            if let Some(old) = self.catalogue.insert(key, instrument.clone()) {
                                ensure!(old == instrument, "conflicting duplicate instrument row");
                            }
                        }
                    }
                }
            }
            ACCOUNT if e.present != 0 => {
                ensure!(
                    text_ref(&e.investor)? == self.investor && text_ref(&e.exchange)? == "CNY",
                    "unexpected CTP account/currency snapshot"
                );
                ensure!(
                    e.control()?.available.is_finite(),
                    "nonfinite CTP account available"
                );
                let day = text_ref(&e.trading_day)?;
                ensure!(
                    day.is_empty() || self.login.as_ref().is_some_and(|login| login.0 == day),
                    "account trading day changed; reconnect and reconcile"
                );
                ensure!(self.available.is_none(), "duplicate CNY account row");
                let account = AccountSnapshot::from_event(e.control()?)?;
                let login_day = self.login.as_ref().map_or("", |login| login.0.as_str());
                // Display-only funds never stop the trading session.
                self.account_funds = account
                    .funds(crate::persistence::now_us(), login_day)
                    .inspect_err(|error| log::warn!("CTP account funds unavailable: {error:#}"))
                    .ok();
                self.account_not_ready_retries = 0;
                self.account = Some(account);
                self.available = Some(e.control()?.available);
            }
            SETTLEMENT_STATUS => {
                if e.present != 0 {
                    ensure!(
                        text_ref(&e.investor)? == self.investor,
                        "settlement status for another investor"
                    );
                    let day = text_ref(&e.trading_day)?;
                    self.settlement_confirmed = self
                        .login
                        .as_ref()
                        .is_some_and(|login| day >= login.0.as_str());
                }
            }
            SETTLED => self.settlement_confirmed = true,
            COMMISSION | MARGIN | EXCHANGE_MARGIN if e.present != 0 => {
                let key = *self
                    .rate_targets
                    .front()
                    .context("rate response without target")?;
                let exchange = text_ref(&e.exchange)?;
                let symbol = text_ref(&e.symbol)?;
                ensure!(
                    (exchange.is_empty() || exchange == key.0.as_str())
                        && (symbol == key.1
                            || self
                                .products
                                .get(&key)
                                .is_some_and(|product| !product.is_empty() && *product == symbol)),
                    "CTP rate response differs from requested contract"
                );
                if kind != EXCHANGE_MARGIN {
                    ensure!(
                        e.control()?.investor_range != i32::from(b'3')
                            || text_ref(&e.investor)? == self.investor,
                        "rate response belongs to another investor"
                    );
                }
                ensure!(!self.rate_seen, "ambiguous duplicate CTP rate row");
                self.rate_seen = true;
                let rule = self.rate_rule.as_mut().context("rate contract absent")?;
                if kind == COMMISSION {
                    let r = e.control()?.commission_rates;
                    let rate = CommissionRate {
                        open_by_money: r[0],
                        open_by_volume: r[1],
                        close_by_money: r[2],
                        close_by_volume: r[3],
                        close_today_by_money: r[4],
                        close_today_by_volume: r[5],
                    };
                    rate.validate()?;
                    rule.commission = Some(rate);
                } else {
                    ensure!(
                        e.hedge == i32::from(b'1'),
                        "non-speculative margin response"
                    );
                    let r = e.control()?.margin_rates;
                    let mut rate = MarginRate {
                        long_by_money: r[0],
                        long_by_volume: r[1],
                        short_by_money: r[2],
                        short_by_volume: r[3],
                    };
                    rate.validate()?;
                    if kind == MARGIN {
                        ensure!(
                            matches!(e.control()?.relative, 0 | 1),
                            "invalid relative margin flag"
                        );
                        self.relative_margin = e.control()?.relative != 0;
                    } else {
                        let addon = rule.margin.context("missing broker margin add-on")?;
                        rate.long_by_money += addon.long_by_money;
                        rate.long_by_volume += addon.long_by_volume;
                        rate.short_by_money += addon.short_by_money;
                        rate.short_by_volume += addon.short_by_volume;
                        rate.validate()?;
                    }
                    rule.margin = Some(rate);
                }
            }
            QUERY_ORDER if e.present != 0 && self.owns(&e)? => {
                self.orders.push(
                    *decoded
                        .as_ref()
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?
                        .order()
                        .context("missing decoded CTP order")?,
                );
            }
            QUERY_TRADE if e.present != 0 && self.owns(&e)? => {
                self.trades.push(
                    *decoded
                        .as_ref()
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?
                        .fill()
                        .context("missing decoded CTP fill")?,
                );
            }
            POSITION if e.present != 0 && self.owns(&e)? && e.volume != 0 => {
                ensure!(
                    e.hedge == i32::from(b'1'),
                    "non-speculative native position requires a separate hedge ledger"
                );
                ensure!(
                    e.volume >= 0 && e.control()?.today >= 0 && e.control()?.today <= e.volume,
                    "invalid CTP position row"
                );
                let side = match e.control()?.position_direction {
                    50 => Side::Buy,
                    51 => Side::Sell,
                    _ => anyhow::bail!("unsupported CTP position direction"),
                };
                let symbol: Symbol = text(&e.symbol)?;
                // YdPosition is the opening historical inventory, not necessarily today's remaining yesterday position.
                let venue = Venue::parse(text_ref(&e.exchange)?)?;
                let today = if venue.separate_close_dates() {
                    ensure!(
                        matches!(e.control()?.position_date, 49 | 50),
                        "invalid SHFE/INE position date"
                    );
                    if e.control()?.position_date == 49 {
                        e.volume
                    } else {
                        0
                    }
                } else {
                    e.control()?.today
                };
                let row = PositionSnapshot {
                    venue,
                    symbol,
                    side,
                    today,
                    yesterday: e.volume - today,
                };
                ensure!(
                    self.positions
                        .insert((venue, symbol, side, e.control()?.position_date), row)
                        .is_none(),
                    "duplicate position-date row; investment-unit split is not supported"
                );
            }
            _ => {}
        }
        ensure!(
            self.positions.len() <= 20_000,
            "CTP bootstrap position snapshot capacity exceeded"
        );
        if e.last == 0 {
            return Ok(None);
        }
        self.waiting = None;
        if kind == INSTRUMENT {
            // Preserve rates for unchanged instruments while refreshing them serially.
            // Drop omitted/changed rules before another order can use them.
            if self.maintenance {
                for key in self.instruments.keys() {
                    if self.catalogue.contains_key(key) && !self.rate_targets.contains(key) {
                        self.rate_targets.push_back(*key);
                    }
                }
                self.rate_targets
                    .retain(|key| self.catalogue.contains_key(key));
                self.rate_retry_after
                    .retain(|key, _| self.catalogue.contains_key(key));
                self.instruments.retain(|key, rule| {
                    self.catalogue.get(key).is_some_and(|new| {
                        new.price_tick == rule.price_tick
                            && new.contract_multiple == rule.contract_multiple
                            && new.expiry_date == rule.expiry_date
                            && new.min_lots == rule.min_lots
                            && new.max_lots == rule.max_lots
                    })
                });
                self.rule_update = Some(self.instruments.values().cloned().collect());
                self.maintenance = false;
                self.next_kind = None;
            } else {
                self.clear_pass();
                self.next_kind = Some(ACCOUNT);
            }
            self.last_rule_poll = now;
            return Ok(None);
        }
        if matches!(kind, COMMISSION | MARGIN | EXCHANGE_MARGIN) {
            if !self.rate_seen {
                self.finish_rates(now, false)?;
            } else if kind == COMMISSION || (kind == MARGIN && self.relative_margin) {
                self.rate_seen = false;
                self.next_kind = Some(if kind == COMMISSION {
                    MARGIN
                } else {
                    EXCHANGE_MARGIN
                });
            } else {
                self.finish_rates(now, true)?;
            }
            return Ok(None);
        }
        if self.maintenance {
            match kind {
                ACCOUNT => {
                    self.cash_update = Some(self.available.context("empty CNY account poll")?);
                    self.last_cash_poll = now;
                }
                QUERY_ORDER => self.order_check = Some(std::mem::take(&mut self.orders)),
                _ => anyhow::bail!("unexpected maintenance request"),
            }
            self.maintenance = false;
            self.next_kind = None;
            return Ok(None);
        }
        self.next_kind = Some(match kind {
            AUTHENTICATED => LOGGED_IN,
            LOGGED_IN => SETTLEMENT_STATUS,
            SETTLEMENT_STATUS => {
                if !self.settlement_confirmed && self.config.confirm_settlement {
                    SETTLED
                } else {
                    INSTRUMENT
                }
            }
            SETTLED => INSTRUMENT,
            ACCOUNT => {
                ensure!(self.available.is_some(), "empty CNY account snapshot");
                QUERY_ORDER
            }
            QUERY_ORDER => QUERY_TRADE,
            QUERY_TRADE => POSITION,
            POSITION => {
                return self.finish_pass(now);
            }
            _ => anyhow::bail!("unexpected bootstrap stage"),
        });
        Ok(None)
    }

    fn finish_pass(&mut self, now: Instant) -> Result<Option<Snapshot>> {
        self.passes += 1;
        ensure!(
            self.passes <= 12,
            "account snapshot did not converge; keep new orders blocked"
        );
        let mut positions = HashMap::<(Venue, Symbol, Side), PositionSnapshot>::new();
        for row in self.positions.values() {
            let p = positions
                .entry((row.venue, row.symbol, row.side))
                .or_insert_with(|| PositionSnapshot {
                    venue: row.venue,
                    symbol: row.symbol,
                    side: row.side,
                    today: 0,
                    yesterday: 0,
                });
            p.today = p
                .today
                .checked_add(row.today)
                .context("position sum overflow")?;
            p.yesterday = p
                .yesterday
                .checked_add(row.yesterday)
                .context("position sum overflow")?;
        }
        let mut positions: Vec<_> = positions.into_values().collect();
        positions.sort_by_key(|p| {
            (
                p.venue.as_str(),
                p.symbol,
                match p.side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
            )
        });
        self.orders
            .sort_by_key(|o| (o.key.session, o.key.order_ref));
        self.trades.sort_by_key(|f| {
            (
                f.identity.venue.as_str(),
                f.identity.symbol,
                f.identity.trade_id,
                match f.identity.side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
            )
        });
        let signature = serde_json::to_string(&(&positions, &self.orders, &self.trades))
            .context("encode snapshot signature")?;
        let available = self.available.context("missing available cash")?;
        if self.revision == self.pass_revision {
            if let Some((previous, previous_cash)) = &self.previous {
                if previous == &signature {
                    let (day, front, session, max_ref) =
                        self.login.context("missing login state")?;
                    self.ready = true;
                    self.next_kind = None;
                    self.last_cash_poll = now;
                    self.last_rule_poll = now;
                    return Ok(Some(Snapshot {
                        trading_day: day,
                        session: session_key(front, session),
                        max_order_ref: max_ref,
                        available_cash: available.min(*previous_cash),
                        instruments: self.instruments.values().cloned().collect(),
                        positions,
                        orders: self.orders.clone(),
                        trades: self.trades.clone(),
                    }));
                }
            }
            self.previous = Some((signature, available));
        } else {
            self.previous = None;
        }
        self.clear_pass();
        self.next_kind = Some(ACCOUNT);
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
