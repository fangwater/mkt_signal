//! Native Binance PM FR cash buying power, independent of portfolio margin headroom.
use crate::pre_trade::{monitor_channel::MonitorChannel, QueryEngHub};
use bytes::Bytes;
use log::{info, warn};
use once_cell::sync::Lazy;
use order_common::{trade_error_code::binance, Side, TradingVenue};
use parking_lot::Mutex;
use runtime_common::time_util::get_timestamp_us;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use viz_common::resample::BinancePmUsdtCapacity;

const POLL_INTERVAL_US: i64 = 30_000_000;
const MAX_AGE_US: i64 = 180_000_000;
const RETRY_US: i64 = 60_000_000;
const MAX_RETRY_US: i64 = 15 * RETRY_US;
const BUY_BUFFER: f64 = 1.001;

#[derive(Clone, Copy)]
struct Snapshot {
    cash: f64,
    borrow: f64,
    started_us: i64,
}

#[derive(Default)]
struct State {
    poll_us: i64,
    cash_id: Option<i64>,
    borrow_id: Option<i64>,
    rules_id: Option<i64>,
    min_borrow: Option<(f64, i64)>,
    last_buy_notional: Option<f64>,
    cash: Option<f64>,
    borrow: Option<f64>,
    snapshot: Option<Snapshot>,
    // Do not reuse the same REST buying power for a burst of submissions.
    // Keep consumption until a later complete exchange snapshot covers it.
    submitted: VecDeque<(i64, f64)>,
    borrow_failed_us: i64,
    borrow_retry_us: i64,
    borrow_error_code: i32,
    frequency_retry_us: i64,
    frequency_delay_us: i64,
}

impl State {
    fn reserved(&self) -> f64 {
        self.submitted.iter().map(|(_, amount)| amount).sum()
    }

    fn fresh(&self, now: i64) -> Option<Snapshot> {
        self.snapshot
            .filter(|s| now >= s.started_us && now - s.started_us <= MAX_AGE_US)
    }

    fn usable_borrow(&self, snapshot: Snapshot, now: i64) -> f64 {
        if now < self.borrow_retry_us || snapshot.started_us <= self.borrow_failed_us {
            0.0
        } else {
            snapshot.borrow
        }
    }

    fn check(
        &self,
        side: Side,
        quote_is_usdt: bool,
        required: f64,
        now: i64,
    ) -> Result<(), &'static str> {
        if now < self.frequency_retry_us {
            return Err("binance_margin_frequency_cooldown");
        }
        if side != Side::Buy || !quote_is_usdt {
            return Ok(());
        }
        let snapshot = self.fresh(now).ok_or("binance_usdt_capacity_not_ready")?;
        let cash_after_pending = (snapshot.cash - self.reserved()).max(0.0);
        if required.is_finite() && required > cash_after_pending + 1e-8 {
            if now < self.borrow_retry_us && self.borrow_error_code == binance::BORROW_BELOW_MINIMUM
            {
                return Err("binance_usdt_borrow_below_minimum");
            }
            let minimum = self
                .min_borrow
                .filter(|(_, ts)| now >= *ts && now - *ts <= MAX_AGE_US)
                .map(|(value, _)| value)
                .ok_or("binance_usdt_borrow_rules_not_ready")?;
            // Pending orders may not have spent their reserved cash yet. Never
            // subtract reservations to pretend this order will borrow more.
            // Reject an uncertain cash/borrowing split until a new snapshot.
            if required - snapshot.cash + 1e-8 < minimum {
                return Err("binance_usdt_borrow_below_minimum");
            }
        }
        let available = snapshot.cash + self.usable_borrow(snapshot, now) - self.reserved();
        if !required.is_finite()
            || required < 0.0
            || available <= 0.0
            || available + 1e-8 < required * BUY_BUFFER
        {
            return Err("binance_usdt_buy_capacity_insufficient");
        }
        Ok(())
    }

    fn reject(&mut self, side: Side, quote_is_usdt: bool, code: i32, now: i64) {
        if code == binance::OPERATION_TOO_FREQUENT {
            // Responses already in flight belong to the same cooldown; do not
            // multiply the delay once per rejected order in that burst.
            if now >= self.frequency_retry_us {
                self.frequency_delay_us = if now - self.frequency_retry_us > MAX_RETRY_US {
                    RETRY_US
                } else {
                    (self.frequency_delay_us * 2).clamp(RETRY_US, MAX_RETRY_US)
                };
                self.frequency_retry_us = now + self.frequency_delay_us;
            }
        } else if side == Side::Buy
            && quote_is_usdt
            && matches!(
                code,
                binance::ASSET_NOT_BORROWABLE | binance::BORROW_BELOW_MINIMUM | 51006 | 51061
            )
        {
            self.borrow_failed_us = now;
            self.borrow_retry_us = now + RETRY_US;
            self.borrow_error_code = code;
        }
    }

    fn accept(&mut self, req_type: QueryRequestType, query_id: i64, body: &[u8]) {
        if req_type == QueryRequestType::BinanceUsdtBorrowRules {
            if self.rules_id == Some(query_id) {
                self.min_borrow = parse_value(req_type, body).map(|v| (v, self.poll_us));
                if self.min_borrow.is_none() {
                    warn!(
                        "BinancePmBuyCapacity: invalid USDT minimum-borrow rules query_id={}",
                        query_id
                    );
                }
            }
            return;
        }
        let expected = match req_type {
            QueryRequestType::BinancePmUsdtCash => self.cash_id,
            QueryRequestType::BinancePmUsdtMaxBorrowable => self.borrow_id,
            _ => return,
        };
        if expected != Some(query_id) {
            return;
        }
        let value = parse_value(req_type, body);
        match req_type {
            QueryRequestType::BinancePmUsdtCash => self.cash = value,
            QueryRequestType::BinancePmUsdtMaxBorrowable => self.borrow = value,
            _ => return,
        }
        if value.is_none() {
            // An exchange error is unknown capacity, never a fabricated zero or
            // permission to keep spending a previous successful snapshot.
            self.snapshot = None;
            warn!(
                "BinancePmBuyCapacity: invalid/error query response type={:?} query_id={}",
                req_type, query_id
            );
            return;
        }
        if let (Some(cash), Some(borrow)) = (self.cash, self.borrow) {
            self.snapshot = Some(Snapshot {
                cash,
                borrow,
                started_us: self.poll_us,
            });
            // Keep a short settlement allowance for submissions still in transit
            // when the exchange took the two independent REST snapshots.
            while self
                .submitted
                .front()
                .is_some_and(|(ts, _)| *ts < self.poll_us - 5_000_000)
            {
                self.submitted.pop_front();
            }
            info!("BinancePmBuyCapacity: cash={:.8} max_borrowable={:.8} reserved={:.8} snapshot_us={}", cash, borrow, self.reserved(), self.poll_us);
        }
    }
}

static STATE: Lazy<Mutex<State>> = Lazy::new(|| Mutex::new(State::default()));
static NEXT_QUERY_ID: AtomicI64 = AtomicI64::new(-9_200_000);

pub fn enabled() -> bool {
    if MonitorChannel::try_venues()
        != Some((TradingVenue::BinanceMargin, TradingVenue::BinanceFutures))
    {
        return false;
    }
    MonitorChannel::instance().arb_mode() == trade_signal::ArbMode::FundingArb
        && MonitorChannel::try_order_manager().is_some_and(|m| !m.borrow().binance_is_standard())
}

pub fn drive_poll(now: i64) {
    if !enabled() {
        return;
    }
    let (cash_id, borrow_id, rules_id) = {
        let mut s = STATE.lock();
        if s.poll_us > 0 && now - s.poll_us < POLL_INTERVAL_US {
            return;
        }
        let cash_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        let borrow_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        let rules_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        s.poll_us = now;
        s.cash_id = Some(cash_id);
        s.borrow_id = Some(borrow_id);
        s.rules_id = Some(rules_id);
        s.cash = None;
        s.borrow = None;
        (cash_id, borrow_id, rules_id)
    };
    for (kind, id) in [
        (QueryRequestType::BinancePmUsdtCash, cash_id),
        (QueryRequestType::BinancePmUsdtMaxBorrowable, borrow_id),
        (QueryRequestType::BinanceUsdtBorrowRules, rules_id),
    ] {
        let params = if kind == QueryRequestType::BinanceUsdtBorrowRules {
            Bytes::new()
        } else {
            Bytes::from_static(b"asset=USDT")
        };
        let request = GenericQueryRequest::create(kind, now, id, params);
        if let Err(error) = QueryEngHub::publish_query_request("binance", &request.to_bytes()) {
            if kind == QueryRequestType::BinanceUsdtBorrowRules {
                STATE.lock().min_borrow = None;
            } else {
                STATE.lock().snapshot = None;
            }
            warn!(
                "BinancePmBuyCapacity: query publish failed type={:?}: {:#}",
                kind, error
            );
        }
    }
}

pub fn handle_query_response(kind: QueryRequestType, id: i64, body: &[u8]) -> bool {
    if !matches!(
        kind,
        QueryRequestType::BinancePmUsdtCash
            | QueryRequestType::BinancePmUsdtMaxBorrowable
            | QueryRequestType::BinanceUsdtBorrowRules
    ) {
        return false;
    }
    STATE.lock().accept(kind, id, body);
    true
}

fn parse_value(kind: QueryRequestType, body: &[u8]) -> Option<f64> {
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let json: Value = serde_json::from_slice(&body[..end]).ok()?;
    let value = match kind {
        QueryRequestType::BinancePmUsdtCash => {
            let row = if let Some(rows) = json.as_array() {
                rows.iter()
                    .find(|row| row.get("asset").and_then(Value::as_str) == Some("USDT"))?
            } else {
                &json
            };
            if row.get("asset").and_then(Value::as_str) != Some("USDT") {
                return None;
            }
            row.get("crossMarginFree")?
        }
        QueryRequestType::BinancePmUsdtMaxBorrowable => json.get("amount")?,
        QueryRequestType::BinanceUsdtBorrowRules => {
            if json.get("assetName").and_then(Value::as_str) != Some("USDT") {
                return None;
            }
            json.get("userMinBorrow")?
        }
        _ => return None,
    };
    let number = value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())?;
    (number.is_finite()
        && number >= 0.0
        && (kind != QueryRequestType::BinanceUsdtBorrowRules || number > 0.0))
        .then_some(number)
}

pub fn check_open(
    venue: TradingVenue,
    side: Side,
    symbol: &str,
    required_usdt: f64,
) -> Result<(), &'static str> {
    if venue != TradingVenue::BinanceMargin || !enabled() {
        return Ok(());
    }
    let mut s = STATE.lock();
    if side == Side::Buy
        && symbol.ends_with("USDT")
        && required_usdt.is_finite()
        && required_usdt > 0.0
    {
        s.last_buy_notional = Some(required_usdt);
    }
    s.check(
        side,
        symbol.ends_with("USDT"),
        required_usdt,
        get_timestamp_us(),
    )
}

pub fn record_submission(venue: TradingVenue, side: Side, symbol: &str, notional: f64) {
    if venue == TradingVenue::BinanceMargin
        && side == Side::Buy
        && symbol.ends_with("USDT")
        && enabled()
    {
        STATE
            .lock()
            .submitted
            .push_back((get_timestamp_us(), notional * BUY_BUFFER));
    }
}

pub fn record_rejection(venue: TradingVenue, side: Side, symbol: &str, code: i32) {
    if venue != TradingVenue::BinanceMargin || !enabled() {
        return;
    }
    if matches!(
        code,
        binance::ASSET_NOT_BORROWABLE
            | binance::BORROW_BELOW_MINIMUM
            | binance::OPERATION_TOO_FREQUENT
            | 51006
            | 51061
    ) {
        let mut s = STATE.lock();
        s.reject(side, symbol.ends_with("USDT"), code, get_timestamp_us());
        warn!("BinancePmBuyCapacity: rejected symbol={} side={:?} code={} borrow_retry_us={} frequency_retry_us={}", symbol, side, code, s.borrow_retry_us, s.frequency_retry_us);
    }
}

pub fn snapshot() -> Option<BinancePmUsdtCapacity> {
    if !enabled() {
        return None;
    }
    let now = get_timestamp_us();
    let s = STATE.lock();
    let snapshot = s.fresh(now);
    Some(BinancePmUsdtCapacity {
        cash_available: snapshot.map(|v| v.cash),
        max_borrowable: snapshot.map(|v| v.borrow),
        buy_capacity: snapshot.map(|v| (v.cash + s.usable_borrow(v, now) - s.reserved()).max(0.0)),
        reserved_usdt: s.reserved(),
        snapshot_ts_ms: s.snapshot.map(|v| v.started_us / 1000),
        state: s
            .check(Side::Buy, true, s.last_buy_notional.unwrap_or(0.0), now)
            .err()
            .unwrap_or("ready")
            .to_string(),
        retry_after_ms: (s.frequency_retry_us.max(s.borrow_retry_us) - now).max(0) / 1000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 1_000_000_000;
    fn ready(cash: f64, borrow: f64) -> State {
        State {
            min_borrow: Some((5.0, NOW)),
            snapshot: Some(Snapshot {
                cash,
                borrow,
                started_us: NOW,
            }),
            ..State::default()
        }
    }

    #[test]
    fn below_minimum_borrow_is_blocked_before_cash_or_credit_can_mask_it() {
        let s = ready(95.0175808, 948126.66487636);
        assert_eq!(
            s.check(Side::Buy, true, 2668.0 * 0.03747, NOW),
            Err("binance_usdt_borrow_below_minimum")
        );
        assert!(s.check(Side::Buy, true, 90.0, NOW).is_ok());
        assert!(s.check(Side::Buy, true, 100.0175808, NOW).is_ok());
        assert!(s.check(Side::Buy, true, 110.0, NOW).is_ok());
        assert!(s.check(Side::Sell, true, 99.96996, NOW).is_ok());
        assert!(s.check(Side::Buy, false, 99.96996, NOW).is_ok());
        let mut pending = ready(95.0175808, 948126.66487636);
        pending.submitted.push_back((NOW, 200.0));
        assert_eq!(
            pending.check(Side::Buy, true, 99.96996, NOW),
            Err("binance_usdt_borrow_below_minimum")
        );
        let mut uncertain = ready(150.0, 1000.0);
        uncertain.submitted.push_back((NOW, 100.0));
        assert_eq!(
            uncertain.check(Side::Buy, true, 100.0, NOW),
            Err("binance_usdt_borrow_below_minimum")
        );
        assert!(uncertain.check(Side::Buy, true, 40.0, NOW).is_ok());
    }

    #[test]
    fn unavailable_borrow_rules_only_block_borrowing_buys_and_rejects_back_off() {
        let mut s = ready(95.0, 1000.0);
        s.min_borrow = None;
        assert!(s.check(Side::Buy, true, 90.0, NOW).is_ok());
        assert_eq!(
            s.check(Side::Buy, true, 100.0, NOW),
            Err("binance_usdt_borrow_rules_not_ready")
        );
        s.min_borrow = Some((5.0, NOW - MAX_AGE_US - 1));
        assert_eq!(
            s.check(Side::Buy, true, 100.0, NOW),
            Err("binance_usdt_borrow_rules_not_ready")
        );
        s.min_borrow = Some((5.0, NOW));
        s.reject(Side::Buy, true, binance::BORROW_BELOW_MINIMUM, NOW + 1);
        assert!(s.check(Side::Buy, true, 90.0, NOW + 2).is_ok());
        assert_eq!(
            s.check(Side::Buy, true, 110.0, NOW + 2),
            Err("binance_usdt_borrow_below_minimum")
        );
        assert!(s.check(Side::Buy, true, 110.0, NOW + RETRY_US + 2).is_err());
        s.snapshot.as_mut().unwrap().started_us = NOW + RETRY_US + 2;
        assert!(s.check(Side::Buy, true, 110.0, NOW + RETRY_US + 2).is_ok());
    }

    #[test]
    fn minimum_borrow_rules_require_matching_query_and_valid_usdt_value() {
        let mut s = State {
            poll_us: NOW,
            rules_id: Some(-3),
            ..State::default()
        };
        let body = br#"{"assetName":"USDT","userMinBorrow":"5"}"#;
        s.accept(QueryRequestType::BinanceUsdtBorrowRules, -4, body);
        assert!(s.min_borrow.is_none());
        s.accept(QueryRequestType::BinanceUsdtBorrowRules, -3, body);
        assert_eq!(s.min_borrow, Some((5.0, NOW)));
        for body in [
            br#"{"assetName":"BTC","userMinBorrow":"5"}"#.as_slice(),
            br#"{"assetName":"USDT","userMinBorrow":"0"}"#,
            br#"{"assetName":"USDT","userMinBorrow":"NaN"}"#,
            br#"{"code":-1}"#,
        ] {
            s.accept(QueryRequestType::BinanceUsdtBorrowRules, -3, body);
            assert!(s.min_borrow.is_none());
        }
    }

    #[test]
    fn buying_requires_cash_or_borrow_even_with_portfolio_margin_headroom() {
        let s = ready(0.0, 0.0);
        assert_eq!(
            s.check(Side::Buy, true, 100.0, NOW),
            Err("binance_usdt_buy_capacity_insufficient")
        );
        assert!(s.check(Side::Sell, true, 100.0, NOW).is_ok());
        assert!(s.check(Side::Buy, false, 100.0, NOW).is_ok());
        assert!(ready(101.0, 0.0).check(Side::Buy, true, 100.0, NOW).is_ok());
        assert!(ready(0.0, 101.0).check(Side::Buy, true, 100.0, NOW).is_ok());
    }

    #[test]
    fn unknown_stale_and_burst_budget_fail_closed() {
        assert!(State::default().check(Side::Buy, true, 100.0, NOW).is_err());
        let mut s = ready(150.0, 0.0);
        assert!(s
            .check(Side::Buy, true, 100.0, NOW + MAX_AGE_US + 1)
            .is_err());
        s.submitted.push_back((NOW, 100.1));
        assert!(s.check(Side::Buy, true, 100.0, NOW).is_err());
    }

    #[test]
    fn borrow_failure_does_not_block_cash_and_requires_new_snapshot_to_recover() {
        let mut s = ready(101.0, 500.0);
        s.reject(Side::Buy, true, 51014, NOW + 1);
        assert!(s.check(Side::Buy, true, 100.0, NOW + 2).is_ok());
        assert!(s.check(Side::Buy, true, 200.0, NOW + RETRY_US + 2).is_err());
        s.snapshot.as_mut().unwrap().started_us = NOW + RETRY_US + 2;
        assert!(s.check(Side::Buy, true, 200.0, NOW + RETRY_US + 2).is_ok());
    }

    #[test]
    fn frequency_cooldown_spans_symbols_and_both_sides_and_backs_off() {
        let mut s = ready(1000.0, 0.0);
        s.reject(Side::Buy, true, 51042, NOW);
        s.reject(Side::Sell, true, 51042, NOW + 1);
        assert_eq!(s.frequency_retry_us, NOW + RETRY_US);
        assert!(s.check(Side::Sell, true, 10.0, NOW + 2).is_err());
        assert!(s.check(Side::Buy, true, 10.0, NOW + RETRY_US).is_ok());
        s.reject(Side::Buy, true, 51042, NOW + RETRY_US);
        assert_eq!(s.frequency_retry_us, NOW + 3 * RETRY_US);
    }

    #[test]
    fn queries_require_matching_ids_complete_pair_and_valid_finite_values() {
        let mut s = State {
            poll_us: NOW,
            cash_id: Some(-1),
            borrow_id: Some(-2),
            ..State::default()
        };
        let cash = br#"{"asset":"USDT","crossMarginFree":"10"}"#;
        s.accept(QueryRequestType::BinancePmUsdtCash, -3, cash);
        assert!(s.cash.is_none());
        s.accept(QueryRequestType::BinancePmUsdtCash, -1, cash);
        assert!(s.snapshot.is_none());
        s.accept(
            QueryRequestType::BinancePmUsdtMaxBorrowable,
            -2,
            br#"{"amount":"200","borrowLimit":"0"}"#,
        );
        assert_eq!(s.snapshot.unwrap().borrow, 200.0);
        for body in [
            br#"{"code":51042}"#.as_slice(),
            br#"{"amount":"NaN"}"#,
            br#"{"amount":"-1"}"#,
            br#"{}"#,
        ] {
            s.accept(QueryRequestType::BinancePmUsdtMaxBorrowable, -2, body);
            assert!(s.snapshot.is_none());
        }
        assert_eq!(
            parse_value(
                QueryRequestType::BinancePmUsdtCash,
                br#"[{"asset":"USDT","crossMarginFree":"0"}]"#
            ),
            Some(0.0)
        );
    }
}
