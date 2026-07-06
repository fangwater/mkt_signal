use anyhow::{anyhow, Result};
use log::{info, warn};
use order_common::{TradeEngineResponse, TradeRequestType, TradingVenue};
use runtime_common::time_util::get_timestamp_us;
use std::cell::{OnceCell, RefCell};
use std::sync::atomic::{AtomicI64, Ordering};
use trade_engine::trade_request::{PreparedTradeRequest, TradeRequestParams};

use crate::pre_trade::account_open_block::{
    clear_account_open_block, register_account_open_block, AccountOpenBlockReason,
};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::trade_eng_channel::TradeEngHub;

const DEFAULT_THRESHOLD_USDT: f64 = 1_000.0;
const DEFAULT_MIN_TRANSFER_USDT: f64 = 1.0;
const DEFAULT_COOLDOWN_SECS: i64 = 20;
const DEFAULT_RESPONSE_TIMEOUT_SECS: i64 = 30;
const DEFAULT_MAX_SPOT_AGE_SECS: i64 = 180;
const DEFAULT_MAX_UM_AGE_SECS: i64 = 60;

static NEXT_REBALANCE_REQUEST_ID: AtomicI64 = AtomicI64::new(-9_500_000);

thread_local! {
    static REBALANCE_USDT_SERVICE: OnceCell<RefCell<RebalanceUsdtService>> = const { OnceCell::new() };
}

#[derive(Debug, Clone)]
pub struct RebalanceUsdtConfig {
    pub threshold_usdt: f64,
    pub min_transfer_usdt: f64,
    pub cooldown_us: i64,
    pub response_timeout_us: i64,
    pub max_spot_snapshot_age_us: i64,
    pub max_um_snapshot_age_us: i64,
}

impl RebalanceUsdtConfig {
    pub fn from_env() -> Self {
        Self {
            threshold_usdt: env_f64(
                "BINANCE_STD_REBALANCE_USDT_THRESHOLD",
                DEFAULT_THRESHOLD_USDT,
            ),
            min_transfer_usdt: env_f64(
                "BINANCE_STD_REBALANCE_USDT_MIN_TRANSFER",
                DEFAULT_MIN_TRANSFER_USDT,
            ),
            cooldown_us: env_i64_secs(
                "BINANCE_STD_REBALANCE_USDT_COOLDOWN_SECS",
                DEFAULT_COOLDOWN_SECS,
            )
            .saturating_mul(1_000_000),
            response_timeout_us: env_i64_secs(
                "BINANCE_STD_REBALANCE_USDT_RESPONSE_TIMEOUT_SECS",
                DEFAULT_RESPONSE_TIMEOUT_SECS,
            )
            .saturating_mul(1_000_000),
            max_spot_snapshot_age_us: env_i64_secs(
                "BINANCE_STD_REBALANCE_USDT_MAX_SPOT_AGE_SECS",
                DEFAULT_MAX_SPOT_AGE_SECS,
            )
            .saturating_mul(1_000_000),
            max_um_snapshot_age_us: env_i64_secs(
                "BINANCE_STD_REBALANCE_USDT_MAX_UM_AGE_SECS",
                DEFAULT_MAX_UM_AGE_SECS,
            )
            .saturating_mul(1_000_000),
        }
    }

    fn normalized(mut self) -> Self {
        if !self.threshold_usdt.is_finite() || self.threshold_usdt <= 0.0 {
            self.threshold_usdt = DEFAULT_THRESHOLD_USDT;
        }
        if !self.min_transfer_usdt.is_finite() || self.min_transfer_usdt < 0.0 {
            self.min_transfer_usdt = DEFAULT_MIN_TRANSFER_USDT;
        }
        if self.cooldown_us < 0 {
            self.cooldown_us = DEFAULT_COOLDOWN_SECS * 1_000_000;
        }
        if self.response_timeout_us <= 0 {
            self.response_timeout_us = DEFAULT_RESPONSE_TIMEOUT_SECS * 1_000_000;
        }
        if self.max_spot_snapshot_age_us <= 0 {
            self.max_spot_snapshot_age_us = DEFAULT_MAX_SPOT_AGE_SECS * 1_000_000;
        }
        if self.max_um_snapshot_age_us <= 0 {
            self.max_um_snapshot_age_us = DEFAULT_MAX_UM_AGE_SECS * 1_000_000;
        }
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceUsdtDirection {
    MainToUm,
    UmToMain,
}

impl RebalanceUsdtDirection {
    fn req_type(self) -> TradeRequestType {
        match self {
            Self::MainToUm => TradeRequestType::BinanceStdMainToUmTransfer,
            Self::UmToMain => TradeRequestType::BinanceStdUmToMainTransfer,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::MainToUm => "MAIN_UMFUTURE",
            Self::UmToMain => "UMFUTURE_MAIN",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RebalanceUsdtPlan {
    pub direction: RebalanceUsdtDirection,
    pub amount: f64,
    pub spot_available_usdt: f64,
    pub um_available_usdt: f64,
    pub um_max_withdraw: f64,
    pub threshold_usdt: f64,
}

#[derive(Debug, Clone, Copy)]
struct RebalanceUsdtInputs {
    spot_available_usdt: f64,
    um_available_usdt: f64,
    um_max_withdraw: f64,
    spot_age_us: i64,
    um_age_us: i64,
}

impl RebalanceUsdtInputs {
    fn imbalance(self) -> f64 {
        self.spot_available_usdt - self.um_available_usdt
    }
}

#[derive(Debug, Clone, Copy)]
struct InFlightTransfer {
    request_id: i64,
    direction: RebalanceUsdtDirection,
    amount: f64,
    sent_us: i64,
}

#[derive(Debug)]
pub struct RebalanceUsdtService {
    config: RebalanceUsdtConfig,
    in_flight: Option<InFlightTransfer>,
    last_terminal_us: i64,
}

impl RebalanceUsdtService {
    pub fn initialize(config: RebalanceUsdtConfig) -> Result<()> {
        let config = config.normalized();
        REBALANCE_USDT_SERVICE.with(|cell| {
            cell.set(RefCell::new(Self {
                config: config.clone(),
                in_flight: None,
                last_terminal_us: 0,
            }))
            .map_err(|_| anyhow!("Binance std USDT rebalance service already initialized"))
        })?;
        info!(
            "Binance std USDT rebalance enabled threshold={:.8} min_transfer={:.8} cooldown_us={} response_timeout_us={} max_spot_age_us={} max_um_age_us={}",
            config.threshold_usdt,
            config.min_transfer_usdt,
            config.cooldown_us,
            config.response_timeout_us,
            config.max_spot_snapshot_age_us,
            config.max_um_snapshot_age_us
        );
        Ok(())
    }

    pub fn drive_from_account_update(trigger: &'static str) {
        REBALANCE_USDT_SERVICE.with(|cell| {
            let Some(service) = cell.get() else {
                return;
            };
            service.borrow_mut().drive(trigger);
        });
    }

    pub fn handle_trade_engine_response(response: &impl TradeEngineResponse) -> bool {
        let Ok(req_type) = TradeRequestType::try_from(response.req_type()) else {
            return false;
        };
        if !matches!(
            req_type,
            TradeRequestType::BinanceStdMainToUmTransfer
                | TradeRequestType::BinanceStdUmToMainTransfer
        ) {
            return false;
        }

        REBALANCE_USDT_SERVICE.with(|cell| {
            let Some(service) = cell.get() else {
                warn!(
                    "Binance std USDT rebalance response received but service is not initialized request_id={} req_type={:?} status={} error_code={}",
                    response.client_order_id(),
                    req_type,
                    response.status(),
                    response.error_code()
                );
                return;
            };
            service.borrow_mut().handle_response(req_type, response);
        });
        true
    }

    fn drive(&mut self, trigger: &'static str) {
        let now_us = get_timestamp_us();
        if let Some(in_flight) = self.in_flight {
            if now_us.saturating_sub(in_flight.sent_us) >= self.config.response_timeout_us {
                warn!(
                    "Binance std USDT rebalance timeout request_id={} direction={} amount={:.8} sent_us={} now_us={}; unlock ArbOpen",
                    in_flight.request_id,
                    in_flight.direction.as_str(),
                    in_flight.amount,
                    in_flight.sent_us,
                    now_us
                );
                self.in_flight = None;
                self.last_terminal_us = now_us;
                let cleared =
                    clear_account_open_block(AccountOpenBlockReason::BinanceStdUsdtRebalance);
                log_unlock_table(
                    "timeout",
                    in_flight,
                    None,
                    None,
                    now_us.saturating_sub(in_flight.sent_us),
                    cleared,
                    now_us,
                );
            } else {
                return;
            }
        }

        if self.last_terminal_us > 0
            && now_us.saturating_sub(self.last_terminal_us) < self.config.cooldown_us
        {
            return;
        }

        let inputs = match self.inputs_from_monitor(now_us) {
            Ok(inputs) => inputs,
            Err(reason) => {
                if should_log_snapshot_no_transfer(trigger) {
                    log_unavailable_snapshot_table(trigger, reason, &self.config);
                }
                return;
            }
        };
        let plan = compute_rebalance_plan(
            inputs.spot_available_usdt,
            inputs.um_available_usdt,
            inputs.um_max_withdraw,
            self.config.threshold_usdt,
            self.config.min_transfer_usdt,
        );
        if let Some(plan) = plan {
            self.publish_plan(plan, inputs, now_us, trigger);
        } else if should_log_snapshot_no_transfer(trigger) {
            log_decision_table(
                trigger,
                "no_transfer",
                no_transfer_reason(&inputs, &self.config),
                None,
                inputs,
                &self.config,
            );
        }
    }

    fn inputs_from_monitor(&self, now_us: i64) -> Result<RebalanceUsdtInputs, &'static str> {
        let monitor = MonitorChannel::instance();
        if monitor.open_venue() != TradingVenue::BinanceMargin
            || monitor.hedge_venue() != TradingVenue::BinanceFutures
        {
            return Err("not_binance_intra");
        }

        let spot = monitor
            .usdt_snapshot_for_venue(TradingVenue::BinanceMargin)
            .ok_or("missing_spot_available_usdt")?;
        let um = monitor
            .binance_std_um_wallet_snapshot()
            .ok_or("missing_um_wallet_snapshot")?;
        if spot.last_timestamp <= 0 || um.timestamp <= 0 {
            return Err("incomplete_timestamp");
        }
        let spot_age_us = now_us.saturating_sub(spot.last_timestamp.saturating_mul(1000));
        if spot_age_us > self.config.max_spot_snapshot_age_us {
            return Err("stale_spot_snapshot");
        }
        let um_age_us = now_us.saturating_sub(um.timestamp.saturating_mul(1000));
        if um_age_us > self.config.max_um_snapshot_age_us {
            return Err("stale_um_snapshot");
        }

        Ok(RebalanceUsdtInputs {
            spot_available_usdt: spot.wallet,
            um_available_usdt: um.available_balance,
            um_max_withdraw: um.max_withdraw_amount,
            spot_age_us,
            um_age_us,
        })
    }

    fn publish_plan(
        &mut self,
        plan: RebalanceUsdtPlan,
        inputs: RebalanceUsdtInputs,
        now_us: i64,
        trigger: &'static str,
    ) {
        let request_id = NEXT_REBALANCE_REQUEST_ID.fetch_sub(1, Ordering::Relaxed);
        let amount_text = format_transfer_amount(plan.amount);
        let params_text = format!("amount={amount_text}");
        let Some(params) = TradeRequestParams::try_from_slice(params_text.as_bytes()) else {
            warn!(
                "Binance std USDT rebalance build params failed request_id={} params_len={}",
                request_id,
                params_text.len()
            );
            return;
        };
        let request =
            PreparedTradeRequest::new(plan.direction.req_type(), now_us, request_id, params);

        register_account_open_block(AccountOpenBlockReason::BinanceStdUsdtRebalance, 0);
        match TradeEngHub::publish_prepared_order_request("binance", &request) {
            Ok(()) => {
                self.in_flight = Some(InFlightTransfer {
                    request_id,
                    direction: plan.direction,
                    amount: plan.amount,
                    sent_us: now_us,
                });
                log_decision_table(
                    trigger,
                    "transfer_required",
                    "imbalance_exceeds_threshold",
                    Some(plan),
                    inputs,
                    &self.config,
                );
                warn!(
                    "Binance std USDT rebalance transfer requested trigger={} request_id={} direction={} amount={} spot_available_usdt={:.8} um_available_usdt={:.8} um_max_withdraw={:.8} threshold={:.8}",
                    trigger,
                    request_id,
                    plan.direction.as_str(),
                    amount_text,
                    plan.spot_available_usdt,
                    plan.um_available_usdt,
                    plan.um_max_withdraw,
                    plan.threshold_usdt
                );
            }
            Err(err) => {
                let cleared =
                    clear_account_open_block(AccountOpenBlockReason::BinanceStdUsdtRebalance);
                self.last_terminal_us = now_us;
                log_unlock_table(
                    "publish_failed",
                    InFlightTransfer {
                        request_id,
                        direction: plan.direction,
                        amount: plan.amount,
                        sent_us: now_us,
                    },
                    None,
                    None,
                    0,
                    cleared,
                    now_us,
                );
                warn!(
                    "Binance std USDT rebalance publish failed trigger={} request_id={} direction={} amount={} err={err:#}",
                    trigger,
                    request_id,
                    plan.direction.as_str(),
                    amount_text
                );
            }
        }
    }

    fn handle_response(&mut self, req_type: TradeRequestType, response: &impl TradeEngineResponse) {
        let now_us = get_timestamp_us();
        let request_id = response.client_order_id();
        let Some(in_flight) = self.in_flight else {
            warn!(
                "Binance std USDT rebalance unexpected response request_id={} req_type={:?} status={} error_code={}",
                request_id,
                req_type,
                response.status(),
                response.error_code()
            );
            return;
        };
        if in_flight.request_id != request_id {
            warn!(
                "Binance std USDT rebalance stale response request_id={} expected_request_id={} req_type={:?} status={} error_code={}",
                request_id,
                in_flight.request_id,
                req_type,
                response.status(),
                response.error_code()
            );
            return;
        }

        self.in_flight = None;
        self.last_terminal_us = now_us;
        let cleared = clear_account_open_block(AccountOpenBlockReason::BinanceStdUsdtRebalance);
        let latency_us = now_us.saturating_sub(in_flight.sent_us);
        log_unlock_table(
            if response.is_request_success() {
                "success"
            } else {
                "failed"
            },
            in_flight,
            Some(response.status()),
            Some(response.error_code()),
            latency_us,
            cleared,
            now_us,
        );
        if response.is_request_success() {
            warn!(
                "Binance std USDT rebalance transfer success request_id={} direction={} amount={:.8} status={} latency_us={} block_cleared={}",
                request_id,
                in_flight.direction.as_str(),
                in_flight.amount,
                response.status(),
                latency_us,
                cleared
            );
        } else {
            warn!(
                "Binance std USDT rebalance transfer failed request_id={} direction={} amount={:.8} status={} error_code={} latency_us={} block_cleared={}",
                request_id,
                in_flight.direction.as_str(),
                in_flight.amount,
                response.status(),
                response.error_code(),
                latency_us,
                cleared
            );
        }
    }
}

fn should_log_snapshot_no_transfer(trigger: &'static str) -> bool {
    trigger == "um_wallet_snapshot"
}

fn no_transfer_reason(inputs: &RebalanceUsdtInputs, config: &RebalanceUsdtConfig) -> &'static str {
    let imbalance = inputs.imbalance();
    if !imbalance.is_finite() {
        return "invalid_imbalance";
    }
    if imbalance.abs() <= config.threshold_usdt {
        return "within_threshold";
    }
    let amount = if imbalance > 0.0 {
        imbalance / 2.0
    } else {
        (-imbalance / 2.0).min(inputs.um_max_withdraw.max(0.0))
    };
    if amount < config.min_transfer_usdt {
        return "amount_below_min_transfer";
    }
    "no_valid_plan"
}

fn log_decision_table(
    trigger: &'static str,
    action: &'static str,
    reason: &'static str,
    plan: Option<RebalanceUsdtPlan>,
    inputs: RebalanceUsdtInputs,
    config: &RebalanceUsdtConfig,
) {
    let direction = plan.map(|plan| plan.direction.as_str()).unwrap_or("-");
    let amount = plan
        .map(|plan| format_transfer_amount(plan.amount))
        .unwrap_or_else(|| "-".to_string());
    info!(
        "Binance std USDT rebalance decision
\
| trigger | action | reason | direction | amount |
\
| {} | {} | {} | {} | {} |
\
| spot_available_usdt | um_available_usdt | imbalance | threshold | max_withdraw |
\
| {:.8} | {:.8} | {:.8} | {:.8} | {:.8} |
\
| spot_age_us | um_age_us | min_transfer | cooldown_us | response_timeout_us |
\
| {} | {} | {:.8} | {} | {} |",
        trigger,
        action,
        reason,
        direction,
        amount,
        inputs.spot_available_usdt,
        inputs.um_available_usdt,
        inputs.imbalance(),
        config.threshold_usdt,
        inputs.um_max_withdraw,
        inputs.spot_age_us,
        inputs.um_age_us,
        config.min_transfer_usdt,
        config.cooldown_us,
        config.response_timeout_us
    );
}

fn log_unavailable_snapshot_table(
    trigger: &'static str,
    reason: &'static str,
    config: &RebalanceUsdtConfig,
) {
    info!(
        "Binance std USDT rebalance decision
\
| trigger | action | reason | threshold | min_transfer |
\
| {} | no_transfer | {} | {:.8} | {:.8} |",
        trigger, reason, config.threshold_usdt, config.min_transfer_usdt
    );
}

fn log_unlock_table(
    event: &'static str,
    in_flight: InFlightTransfer,
    status: Option<u16>,
    error_code: Option<i32>,
    latency_us: i64,
    block_cleared: bool,
    terminal_us: i64,
) {
    let status = status
        .map(|status| status.to_string())
        .unwrap_or_else(|| "-".to_string());
    let error_code = error_code
        .map(|error_code| error_code.to_string())
        .unwrap_or_else(|| "-".to_string());
    info!(
        "Binance std USDT rebalance unlock
\
| event | request_id | direction | amount | block_cleared |
\
| {} | {} | {} | {} | {} |
\
| status | error_code | latency_us | sent_us | terminal_us |
\
| {} | {} | {} | {} | {} |",
        event,
        in_flight.request_id,
        in_flight.direction.as_str(),
        format_transfer_amount(in_flight.amount),
        block_cleared,
        status,
        error_code,
        latency_us,
        in_flight.sent_us,
        terminal_us
    );
}

pub fn compute_rebalance_plan(
    spot_available_usdt: f64,
    um_available_usdt: f64,
    um_max_withdraw: f64,
    threshold_usdt: f64,
    min_transfer_usdt: f64,
) -> Option<RebalanceUsdtPlan> {
    if !spot_available_usdt.is_finite()
        || !um_available_usdt.is_finite()
        || !um_max_withdraw.is_finite()
        || !threshold_usdt.is_finite()
        || !min_transfer_usdt.is_finite()
    {
        return None;
    }
    if threshold_usdt <= 0.0 || min_transfer_usdt < 0.0 {
        return None;
    }

    let imbalance = spot_available_usdt - um_available_usdt;
    if imbalance.abs() <= threshold_usdt {
        return None;
    }

    let (direction, amount) = if imbalance > 0.0 {
        (RebalanceUsdtDirection::MainToUm, imbalance / 2.0)
    } else {
        (
            RebalanceUsdtDirection::UmToMain,
            (-imbalance / 2.0).min(um_max_withdraw.max(0.0)),
        )
    };

    if amount < min_transfer_usdt || amount <= 0.0 || !amount.is_finite() {
        return None;
    }

    Some(RebalanceUsdtPlan {
        direction,
        amount,
        spot_available_usdt,
        um_available_usdt,
        um_max_withdraw,
        threshold_usdt,
    })
}

fn format_transfer_amount(amount: f64) -> String {
    let mut text = format!("{amount:.8}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

fn env_f64(name: &str, default_value: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(default_value)
}

fn env_i64_secs(name: &str, default_value: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(default_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_main_to_um_when_spot_exceeds_um_threshold() {
        let plan = compute_rebalance_plan(12_000.0, 8_000.0, 50_000.0, 1_000.0, 1.0).expect("plan");
        assert_eq!(plan.direction, RebalanceUsdtDirection::MainToUm);
        assert_eq!(plan.amount, 2_000.0);
    }

    #[test]
    fn plans_um_to_main_when_um_exceeds_spot_threshold() {
        let plan = compute_rebalance_plan(8_000.0, 12_000.0, 50_000.0, 1_000.0, 1.0).expect("plan");
        assert_eq!(plan.direction, RebalanceUsdtDirection::UmToMain);
        assert_eq!(plan.amount, 2_000.0);
    }

    #[test]
    fn caps_um_to_main_by_max_withdraw() {
        let plan = compute_rebalance_plan(8_000.0, 12_000.0, 1_200.0, 1_000.0, 1.0).expect("plan");
        assert_eq!(plan.direction, RebalanceUsdtDirection::UmToMain);
        assert_eq!(plan.amount, 1_200.0);
    }

    #[test]
    fn skips_when_within_threshold() {
        assert!(compute_rebalance_plan(10_400.0, 9_600.0, 50_000.0, 1_000.0, 1.0).is_none());
    }

    #[test]
    fn skips_when_capped_amount_is_below_min_transfer() {
        assert!(compute_rebalance_plan(8_000.0, 12_000.0, 0.5, 1_000.0, 1.0).is_none());
    }

    #[test]
    fn plans_main_to_um_when_um_available_is_depleted() {
        let plan = compute_rebalance_plan(6461.11019522, 0.0, 0.0, 1_000.0, 1.0).expect("plan");
        assert_eq!(plan.direction, RebalanceUsdtDirection::MainToUm);
        assert!((plan.amount - 3230.55509761).abs() < 1e-9);
    }

    #[test]
    fn formats_transfer_amount_without_scientific_notation() {
        assert_eq!(format_transfer_amount(1000.0), "1000.0");
        assert_eq!(format_transfer_amount(12.345678901), "12.3456789");
    }
}
