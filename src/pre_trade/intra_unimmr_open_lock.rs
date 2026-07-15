//! Intra UniMMR global ArbOpen gate.
//!
//! Unlike FR's `unimmr_close_symbols` flow, intra does not generate close
//! signals from a symbol list. A low unified-account margin ratio instead
//! locks new risk: `ArbOpen` may proceed only when both legs reduce their
//! current positions. The lock uses trigger/recover hysteresis from
//! `pre_trade_risk_params`.

use account_common::BinanceAccountMode;
use log::info;
use mkt_parsers::msg::basic_account_msg::{BasicAccountRiskMsg, BasicAccountScope};
use std::cell::RefCell;
use trade_signal::ArbMode;

use crate::pre_trade::params_load::PreTradeParamsLoader;

#[derive(Debug, Default)]
struct IntraUnimmrOpenLockState {
    enabled: bool,
    locked: bool,
}

thread_local! {
    static STATE: RefCell<IntraUnimmrOpenLockState> = RefCell::new(IntraUnimmrOpenLockState::default());
}

/// Per-process global lock for intra `ArbOpen` signals.
pub struct IntraUnimmrOpenLock;

impl IntraUnimmrOpenLock {
    /// Binance standard accounts do not publish UniMMR, so this control is
    /// intentionally disabled for that mode.
    pub fn initialize(arb_mode: ArbMode, binance_account_mode: Option<BinanceAccountMode>) {
        let enabled = arb_mode == ArbMode::IntraArb
            && !matches!(binance_account_mode, Some(BinanceAccountMode::Standard));
        STATE.with(|state| {
            *state.borrow_mut() = IntraUnimmrOpenLockState {
                enabled,
                locked: false,
            };
        });
        info!(
            "intra UniMMR ArbOpen lock initialized enabled={} arb_mode={} binance_account_mode={:?}",
            enabled,
            arb_mode.as_str(),
            binance_account_mode
        );
    }

    /// Values inside `[trigger, recover]` retain the existing state to avoid
    /// flapping. This is intentionally separate from FR's active-close gate.
    pub fn apply_account_risk(scope: BasicAccountScope, msg: &BasicAccountRiskMsg) {
        if msg.margin_ratio.is_nan() {
            return;
        }

        STATE.with(|state| {
            let mut state = state.borrow_mut();
            if !state.enabled {
                return;
            }

            let params = PreTradeParamsLoader::instance();
            let trigger = params.unimmr_trigger_line();
            let recover = params.unimmr_recover_line();
            if !(trigger.is_finite() && recover.is_finite() && trigger < recover) {
                return;
            }

            let next = if msg.margin_ratio < trigger {
                true
            } else if msg.margin_ratio > recover {
                false
            } else {
                state.locked
            };
            if next != state.locked {
                info!(
                    "intra UniMMR ArbOpen lock scope={} {} -> {} margin_ratio={:.6} trigger={:.3} recover={:.3}",
                    scope.as_str(),
                    if state.locked { "locked" } else { "normal" },
                    if next { "locked" } else { "normal" },
                    msg.margin_ratio,
                    trigger,
                    recover
                );
                state.locked = next;
            }
        });
    }

    pub fn is_locked() -> bool {
        STATE.with(|state| {
            let state = state.borrow();
            state.enabled && state.locked
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::BasicAccountEventType;

    fn risk_msg(margin_ratio: f64) -> BasicAccountRiskMsg {
        BasicAccountRiskMsg {
            msg_type: BasicAccountEventType::AccountRisk,
            timestamp: 0,
            adj_equity_usd: 0.0,
            actual_equity_usd: 0.0,
            maintenance_margin_usd: 0.0,
            initial_margin_usd: 0.0,
            margin_ratio,
            borrowed_usd: 0.0,
            notional_usd: 0.0,
        }
    }

    #[test]
    fn infinite_margin_ratio_recovers_locked_account() {
        IntraUnimmrOpenLock::initialize(ArbMode::IntraArb, None);
        IntraUnimmrOpenLock::apply_account_risk(BasicAccountScope::BybitUnified, &risk_msg(1.0));
        assert!(IntraUnimmrOpenLock::is_locked());
        IntraUnimmrOpenLock::apply_account_risk(
            BasicAccountScope::BybitUnified,
            &risk_msg(f64::INFINITY),
        );
        assert!(!IntraUnimmrOpenLock::is_locked());
    }

    #[test]
    fn binance_standard_is_disabled() {
        IntraUnimmrOpenLock::initialize(ArbMode::IntraArb, Some(BinanceAccountMode::Standard));
        IntraUnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceStdUm, &risk_msg(0.5));
        assert!(!IntraUnimmrOpenLock::is_locked());
    }
}
