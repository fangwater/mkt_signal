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
    cash: Option<f64>,
    borrow: Option<f64>,
    snapshot: Option<Snapshot>,
    // Do not reuse the same REST buying power for a burst of submissions.
    // Keep consumption until a later complete exchange snapshot covers it.
    submitted: VecDeque<(i64, f64)>,
    borrow_failed_us: i64,
    borrow_retry_us: i64,
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
        // Let the venue decide the actual loan amount. Only an observed borrow
        // rejection starts this timer; pending orders and possible cancellation
        // refunds must not predict a minimum-loan rejection.
        if required.is_finite()
            && required * BUY_BUFFER > cash_after_pending + 1e-8
            && now < self.borrow_retry_us
        {
            return Err("binance_usdt_borrow_cooldown");
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
        }
    }

    fn accept(&mut self, req_type: QueryRequestType, query_id: i64, body: &[u8]) {
        let expected = match req_type {
            QueryRequestType::BinancePmUsdtCash => self.cash_id,
            QueryRequestType::BinancePmUsdtMaxBorrowable => self.borrow_id,
            _ => return,
        };
        if expected != Some(query_id) {
            return;
        }
        let valid = match req_type {
            QueryRequestType::BinancePmUsdtCash => {
                self.cash = parse_cash(body);
                self.cash.is_some()
            }
            QueryRequestType::BinancePmUsdtMaxBorrowable => {
                self.borrow = parse_max_borrowable(body);
                self.borrow.is_some()
            }
            _ => return,
        };
        if !valid {
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
    let (cash_id, borrow_id) = {
        let mut s = STATE.lock();
        if s.poll_us > 0 && now - s.poll_us < POLL_INTERVAL_US {
            return;
        }
        let cash_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        let borrow_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        s.poll_us = now;
        s.cash_id = Some(cash_id);
        s.borrow_id = Some(borrow_id);
        s.cash = None;
        s.borrow = None;
        (cash_id, borrow_id)
    };
    for (kind, id) in [
        (QueryRequestType::BinancePmUsdtCash, cash_id),
        (QueryRequestType::BinancePmUsdtMaxBorrowable, borrow_id),
    ] {
        let params = Bytes::from_static(b"asset=USDT");
        let request = GenericQueryRequest::create(kind, now, id, params);
        if let Err(error) = QueryEngHub::publish_query_request("binance", &request.to_bytes()) {
            STATE.lock().snapshot = None;
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
        QueryRequestType::BinancePmUsdtCash | QueryRequestType::BinancePmUsdtMaxBorrowable
    ) {
        return false;
    }
    STATE.lock().accept(kind, id, body);
    true
}

fn parse_cash(body: &[u8]) -> Option<f64> {
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let json: Value = serde_json::from_slice(&body[..end]).ok()?;
    let row = if let Some(rows) = json.as_array() {
        let mut usdt = rows.iter().filter(|r| r["asset"] == "USDT");
        let row = usdt.next()?;
        if usdt.next().is_some() {
            return None;
        }
        row
    } else {
        &json
    };
    if row["asset"] != "USDT" {
        return None;
    }
    let amount = |field: &str| {
        let value = row.get(field)?;
        let n = value
            .as_f64()
            .or_else(|| value.as_str()?.parse::<f64>().ok())?;
        (n.is_finite() && n >= 0.0).then_some(n)
    };
    amount("crossMarginFree")
}

fn parse_max_borrowable(body: &[u8]) -> Option<f64> {
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let json: Value = serde_json::from_slice(&body[..end]).ok()?;
    // Queried with asset=USDT: amount is already a USDT quantity.
    let value = json.get("amount")?;
    let number = value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())?;
    (number.is_finite() && number >= 0.0).then_some(number)
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
    STATE.lock().check(
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

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 1_000_000_000;
    fn ready(cash: f64, borrow: f64) -> State {
        State {
            snapshot: Some(Snapshot {
                cash,
                borrow,
                started_us: NOW,
            }),
            ..State::default()
        }
    }

    #[test]
    fn small_borrow_and_pending_refunds_do_not_predict_exchange_rejections() {
        let mut s = ready(95.0175808, 948126.66487636);
        assert!(s.check(Side::Buy, true, 99.96996, NOW).is_ok());
        s.submitted.push_back((NOW, 200.0));
        assert!(s.check(Side::Buy, true, 99.96996, NOW).is_ok());
        assert!(ready(99.99913, 948000.0)
            .check(Side::Buy, true, 99.99, NOW)
            .is_ok());
        let mut no_cash = ready(0.0, 948000.0);
        no_cash.submitted.push_back((NOW, 100.1));
        assert!(no_cash.check(Side::Buy, true, 100.0, NOW).is_ok());
    }

    #[test]
    fn borrow_rejections_cool_down_then_retry_with_a_new_snapshot() {
        for code in [
            binance::BORROW_BELOW_MINIMUM,
            binance::ASSET_NOT_BORROWABLE,
            51006,
            51061,
        ] {
            let mut s = ready(95.0, 1000.0);
            s.reject(Side::Buy, true, code, NOW + 1);
            assert!(s.check(Side::Buy, true, 90.0, NOW + 2).is_ok());
            assert!(s.check(Side::Sell, true, 100.0, NOW + 2).is_ok());
            assert_eq!(
                s.check(Side::Buy, true, 100.0, NOW + 2),
                Err("binance_usdt_borrow_cooldown")
            );
            // A fresh query cannot bypass the error-code timer.
            s.snapshot.as_mut().unwrap().started_us = NOW + 2;
            assert_eq!(
                s.check(Side::Buy, true, 100.0, NOW + RETRY_US),
                Err("binance_usdt_borrow_cooldown")
            );
            assert!(s.check(Side::Buy, true, 100.0, NOW + RETRY_US + 1).is_ok());
            // A rejected retry starts a new bounded interval.
            s.reject(Side::Buy, true, code, NOW + RETRY_US + 2);
            assert_eq!(
                s.check(Side::Buy, true, 100.0, NOW + RETRY_US + 3),
                Err("binance_usdt_borrow_cooldown")
            );
        }
    }

    #[test]
    fn unrelated_rejections_do_not_start_a_borrow_cooldown() {
        for (side, usdt, code) in [
            (Side::Sell, true, 51015),
            (Side::Buy, false, 51015),
            (Side::Buy, true, 12345),
        ] {
            let mut s = ready(95.0, 1000.0);
            s.reject(side, usdt, code, NOW + 1);
            assert!(s.check(Side::Buy, true, 100.0, NOW + 2).is_ok());
        }
    }

    #[test]
    fn cash_uses_free_usdt_without_assuming_locked_funds_will_be_refunded() {
        assert_eq!(
            parse_cash(br#"{"asset":"USDT","crossMarginFree":"0","crossMarginLocked":"99.99738"}"#),
            Some(0.0)
        );
        assert_eq!(
            parse_cash(br#"{"asset":"USDT","crossMarginFree":"10"}"#),
            Some(10.0)
        );
        for body in [
            br#"{"asset":"BTC","crossMarginFree":"10"}"#.as_slice(),
            br#"{"asset":"USDT","crossMarginFree":"NaN"}"#,
            br#"{"asset":"USDT","crossMarginFree":"-1"}"#,
            br#"{"asset":"USDT"}"#,
        ] {
            assert!(parse_cash(body).is_none());
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
        let cash = br#"{"asset":"USDT","crossMarginFree":"10","crossMarginLocked":"0"}"#;
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
            parse_cash(br#"[{"asset":"USDT","crossMarginFree":"0","crossMarginLocked":"0"}]"#),
            Some(0.0)
        );
    }
}
