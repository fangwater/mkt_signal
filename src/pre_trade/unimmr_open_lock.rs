//! Pre-trade UniMMR global ArbOpen gate for FR and intra strategies.
//!
//! A low unified-account margin ratio locks new risk: `ArbOpen` may proceed
//! only when both legs reduce their current positions. The lock uses the same
//! trigger/recover hysteresis as trade_signal FR close gate.

use account_common::BinanceAccountMode;
use anyhow::{Context, Result};
use log::info;
use mkt_parsers::msg::basic_account_msg::{BasicAccountRiskMsg, BasicAccountScope};
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use trade_signal::ArbMode;

use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::notification_client::{
    LocalNotificationClient, NotificationRequest, NotificationSeverity,
};
use crate::pre_trade::params_load::PreTradeParamsLoader;

#[derive(Debug, Clone, Copy)]
struct ScopeState {
    locked: bool,
    margin_ratio: f64,
    recovery_pending: bool,
}

#[derive(Debug, Default)]
struct UnimmrOpenLockState {
    enabled: bool,
    env_name: String,
    scopes: HashMap<BasicAccountScope, ScopeState>,
    notification: Option<LocalNotificationClient>,
    close_cancel_pending: bool,
    dump_symbols: BTreeSet<String>,
    pos_dump_symbols: BTreeSet<String>,
    unimmr_close_symbols: BTreeSet<String>,
}

thread_local! {
    static STATE: RefCell<UnimmrOpenLockState> = RefCell::new(UnimmrOpenLockState::default());
}

/// Per-process global lock for FR/intra `ArbOpen` signals.
pub struct UnimmrOpenLock;

impl UnimmrOpenLock {
    /// Binance standard accounts do not publish UniMMR, so this control is
    /// intentionally disabled for that mode.
    pub fn initialize(
        env_name: Option<String>,
        arb_mode: ArbMode,
        binance_account_mode: Option<BinanceAccountMode>,
    ) -> Result<()> {
        let enabled = matches!(arb_mode, ArbMode::FundingArb | ArbMode::IntraArb)
            && !matches!(binance_account_mode, Some(BinanceAccountMode::Standard));
        let notification = enabled
            .then(LocalNotificationClient::from_env)
            .transpose()
            .context("initialize local notification client for UniMMR open lock")?;
        let env_name = env_name
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "pre-trade".to_string());
        STATE.with(|state| {
            *state.borrow_mut() = UnimmrOpenLockState {
                enabled,
                env_name,
                scopes: HashMap::new(),
                notification,
                close_cancel_pending: false,
                dump_symbols: BTreeSet::new(),
                pos_dump_symbols: BTreeSet::new(),
                unimmr_close_symbols: BTreeSet::new(),
            };
        });
        info!(
            "UniMMR ArbOpen lock initialized enabled={} arb_mode={} binance_account_mode={:?}",
            enabled,
            arb_mode.as_str(),
            binance_account_mode
        );
        Ok(())
    }

    /// Values inside `[trigger, recover]` retain the existing state to avoid
    /// flapping. The lock complements FR active close by rejecting new risk.
    pub fn apply_account_risk(scope: BasicAccountScope, msg: &BasicAccountRiskMsg) {
        if msg.margin_ratio.is_nan()
            || matches!(
                scope,
                BasicAccountScope::Unknown
                    | BasicAccountScope::BinanceStdSpot
                    | BasicAccountScope::BinanceStdUm
            )
        {
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

            let global_was_locked = state.scopes.values().any(|scope_state| scope_state.locked);
            let previous = state.scopes.get(&scope).copied();
            let was_locked = previous.is_some_and(|scope_state| scope_state.locked);
            let next_locked = if msg.margin_ratio < trigger {
                true
            } else if msg.margin_ratio > recover {
                false
            } else {
                was_locked
            };
            let recovery_pending = if next_locked {
                false
            } else if was_locked {
                true
            } else {
                previous.is_some_and(|scope_state| scope_state.recovery_pending)
            };
            if next_locked != was_locked {
                info!(
                    "UniMMR ArbOpen lock scope={} {} -> {} margin_ratio={:.6} trigger={:.3} recover={:.3}",
                    scope.as_str(),
                    if was_locked { "locked" } else { "normal" },
                    if next_locked { "locked" } else { "normal" },
                    msg.margin_ratio,
                    trigger,
                    recover
                );
            }
            state.scopes.insert(
                scope,
                ScopeState {
                    locked: next_locked,
                    margin_ratio: msg.margin_ratio,
                    recovery_pending,
                },
            );
            let global_is_locked = state.scopes.values().any(|scope_state| scope_state.locked);
            if global_was_locked && !global_is_locked {
                state.close_cancel_pending = true;
            } else if global_is_locked {
                state.close_cancel_pending = false;
            }
        });
    }

    pub fn is_locked() -> bool {
        STATE.with(|state| {
            let state = state.borrow();
            state.enabled && state.scopes.values().any(|scope_state| scope_state.locked)
        })
    }

    pub fn replace_fr_close_symbol_lists(
        dump_symbols: &BTreeSet<String>,
        pos_dump_symbols: &BTreeSet<String>,
        unimmr_close_symbols: &BTreeSet<String>,
    ) {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.dump_symbols = dump_symbols.clone();
            state.pos_dump_symbols = pos_dump_symbols.clone();
            state.unimmr_close_symbols = unimmr_close_symbols.clone();
        });
    }

    /// Cancels only active ArbClose strategies whose symbols are exclusively
    /// in the UniMMR close list. Manual and position dump membership wins.
    pub fn cancel_recovered_fr_closes() -> usize {
        let symbols = STATE.with(|state| {
            let mut state = state.borrow_mut();
            if !state.enabled || !state.close_cancel_pending {
                return Vec::new();
            }
            state.close_cancel_pending = false;
            unimmr_only_symbols(&state)
        });
        if symbols.is_empty() {
            return 0;
        }

        let trigger_ts = get_timestamp_us();
        let strategy_mgr = MonitorChannel::instance().strategy_mgr();
        let mut strategy_ids = Vec::new();
        let mut cancelled = 0usize;
        for symbol in &symbols {
            strategy_mgr
                .borrow()
                .copy_ids_for_normalized_symbol_into(symbol, &mut strategy_ids);
            for strategy_id in strategy_ids.iter().copied() {
                if strategy_mgr
                    .borrow_mut()
                    .cancel_arb_close_for_unimmr_recover_by_id(strategy_id, trigger_ts)
                {
                    cancelled += 1;
                }
            }
        }
        info!(
            "UniMMR recover close cancel: candidate_symbols={} cancelled_strategies={}",
            symbols.len(),
            cancelled
        );
        cancelled
    }

    /// Runs inside the synchronous 60-second maintenance round after queued
    /// open signals have been dropped. Active risk repeats every round;
    /// recovery is sent once and retried if local delivery fails.
    pub fn flush_notification_blocking() -> Result<()> {
        let Some((client, request, recovered_scopes)) = STATE.with(|state| {
            let state = state.borrow();
            if !state.enabled {
                return None;
            }
            let client = state.notification.clone()?;
            let params = PreTradeParamsLoader::instance();
            build_notification(
                &state,
                params.unimmr_trigger_line(),
                params.unimmr_recover_line(),
            )
            .map(|(request, recovered_scopes)| (client, request, recovered_scopes))
        }) else {
            return Ok(());
        };

        client.send(&request)?;
        if !recovered_scopes.is_empty() {
            STATE.with(|state| {
                let mut state = state.borrow_mut();
                for scope in recovered_scopes {
                    if let Some(scope_state) = state.scopes.get_mut(&scope) {
                        if !scope_state.locked {
                            scope_state.recovery_pending = false;
                        }
                    }
                }
            });
        }
        Ok(())
    }
}

fn unimmr_only_symbols(state: &UnimmrOpenLockState) -> Vec<String> {
    state
        .unimmr_close_symbols
        .iter()
        .filter(|symbol| {
            !state.dump_symbols.contains(*symbol) && !state.pos_dump_symbols.contains(*symbol)
        })
        .cloned()
        .collect()
}

fn build_notification(
    state: &UnimmrOpenLockState,
    trigger: f64,
    recover: f64,
) -> Option<(NotificationRequest, Vec<BasicAccountScope>)> {
    let mut active = state
        .scopes
        .iter()
        .filter(|(_, scope_state)| scope_state.locked)
        .map(|(scope, scope_state)| (*scope, scope_state.margin_ratio))
        .collect::<Vec<_>>();
    let mut recovered = state
        .scopes
        .iter()
        .filter(|(_, scope_state)| !scope_state.locked && scope_state.recovery_pending)
        .map(|(scope, scope_state)| (*scope, scope_state.margin_ratio))
        .collect::<Vec<_>>();
    active.sort_by_key(|(scope, _)| scope.as_str());
    recovered.sort_by_key(|(scope, _)| scope.as_str());
    if active.is_empty() && recovered.is_empty() {
        return None;
    }

    let mut summary = Vec::with_capacity(2);
    if !active.is_empty() {
        summary.push(format!("风险{}", active.len()));
    }
    if !recovered.is_empty() {
        summary.push(format!("恢复{}", recovered.len()));
    }
    let mut lines = Vec::with_capacity(active.len() + recovered.len());
    for (scope, margin_ratio) in &active {
        lines.push(format!(
            "{} {:.2}<{:.2} 只减仓",
            scope_label(*scope),
            margin_ratio,
            trigger
        ));
    }
    for (scope, margin_ratio) in &recovered {
        lines.push(format!(
            "{} {:.2}>{:.2} 恢复",
            scope_label(*scope),
            margin_ratio,
            recover
        ));
    }
    let recovered_scopes = recovered.into_iter().map(|(scope, _)| scope).collect();

    Some((
        NotificationRequest {
            source: "pre_trade".to_string(),
            title: "UniMMR风控".to_string(),
            message: format!(
                "{}｜{}\n{}",
                state.env_name,
                summary.join("｜"),
                lines.join("\n")
            ),
            severity: if active.is_empty() {
                NotificationSeverity::Info
            } else {
                NotificationSeverity::Critical
            },
            fields: BTreeMap::new(),
            dedup_key: Some(format!("{}:unimmr_open_lock", state.env_name)),
        },
        recovered_scopes,
    ))
}

fn scope_label(scope: BasicAccountScope) -> &'static str {
    match scope {
        BasicAccountScope::BinanceUnified => "Binance统一账户",
        BasicAccountScope::OkexUnified => "OKX统一账户",
        BasicAccountScope::GateUnified => "Gate统一账户",
        BasicAccountScope::BitgetUnified => "Bitget统一账户",
        BasicAccountScope::BybitUnified => "Bybit统一账户",
        BasicAccountScope::Unknown => "未知账户",
        BasicAccountScope::BinanceStdSpot => "Binance现货账户",
        BasicAccountScope::BinanceStdUm => "Binance合约账户",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::BasicAccountEventType;
    use std::sync::Once;

    fn initialize_test(
        env_name: Option<String>,
        arb_mode: ArbMode,
        binance_account_mode: Option<BinanceAccountMode>,
    ) -> Result<()> {
        static NOTIFICATION_ENV: Once = Once::new();
        NOTIFICATION_ENV.call_once(|| {
            std::env::set_var(
                "PRE_TRADE_NOTIFICATION_URL",
                "http://127.0.0.1:18100/v1/notify",
            );
        });
        UnimmrOpenLock::initialize(env_name, arb_mode, binance_account_mode)
    }

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
    fn unified_account_lock_honors_trigger_and_recovery_hysteresis() {
        initialize_test(Some("test-intra".to_string()), ArbMode::IntraArb, None).unwrap();
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BybitUnified, &risk_msg(1.9));
        assert!(UnimmrOpenLock::is_locked());
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BybitUnified, &risk_msg(2.1));
        assert!(UnimmrOpenLock::is_locked());
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BybitUnified, &risk_msg(2.3));
        assert!(!UnimmrOpenLock::is_locked());
    }

    #[test]
    fn infinite_margin_ratio_recovers_locked_account() {
        initialize_test(Some("test-intra".to_string()), ArbMode::IntraArb, None).unwrap();
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BybitUnified, &risk_msg(1.0));
        assert!(UnimmrOpenLock::is_locked());
        UnimmrOpenLock::apply_account_risk(
            BasicAccountScope::BybitUnified,
            &risk_msg(f64::INFINITY),
        );
        assert!(!UnimmrOpenLock::is_locked());
    }

    #[test]
    fn binance_standard_is_disabled() {
        initialize_test(
            Some("test-intra".to_string()),
            ArbMode::IntraArb,
            Some(BinanceAccountMode::Standard),
        )
        .unwrap();
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceStdUm, &risk_msg(0.5));
        assert!(!UnimmrOpenLock::is_locked());
    }

    #[test]
    fn funding_arb_uses_the_same_unimmr_open_lock() {
        initialize_test(
            Some("binance-fr-arb01".to_string()),
            ArbMode::FundingArb,
            None,
        )
        .unwrap();
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceUnified, &risk_msg(1.99));
        assert!(UnimmrOpenLock::is_locked());
    }

    #[test]
    fn healthy_scope_does_not_unlock_another_risk_scope() {
        initialize_test(Some("test-fr".to_string()), ArbMode::FundingArb, None).unwrap();
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceUnified, &risk_msg(1.8));
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::GateUnified, &risk_msg(3.0));
        assert!(UnimmrOpenLock::is_locked());

        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceUnified, &risk_msg(2.3));
        assert!(!UnimmrOpenLock::is_locked());
    }

    #[test]
    fn recover_cancel_waits_until_all_scopes_are_safe() {
        initialize_test(Some("test-fr".to_string()), ArbMode::FundingArb, None).unwrap();
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceUnified, &risk_msg(1.8));
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::GateUnified, &risk_msg(1.9));
        UnimmrOpenLock::apply_account_risk(BasicAccountScope::BinanceUnified, &risk_msg(2.3));
        STATE.with(|state| assert!(!state.borrow().close_cancel_pending));

        UnimmrOpenLock::apply_account_risk(BasicAccountScope::GateUnified, &risk_msg(2.3));
        STATE.with(|state| assert!(state.borrow().close_cancel_pending));
    }

    #[test]
    fn dump_and_position_dump_override_unimmr_cancel_candidates() {
        let state = UnimmrOpenLockState {
            enabled: true,
            env_name: "test-fr".to_string(),
            scopes: HashMap::new(),
            notification: None,
            close_cancel_pending: true,
            dump_symbols: BTreeSet::from(["ETHUSDT".to_string()]),
            pos_dump_symbols: BTreeSet::from(["XRPUSDT".to_string()]),
            unimmr_close_symbols: BTreeSet::from([
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "XRPUSDT".to_string(),
            ]),
        };
        assert_eq!(unimmr_only_symbols(&state), vec!["BTCUSDT".to_string()]);
    }

    #[test]
    fn builds_concise_aggregate_risk_and_recovery_notification() {
        let mut state = UnimmrOpenLockState {
            enabled: true,
            env_name: "test-fr".to_string(),
            scopes: HashMap::new(),
            notification: None,
            close_cancel_pending: false,
            dump_symbols: BTreeSet::new(),
            pos_dump_symbols: BTreeSet::new(),
            unimmr_close_symbols: BTreeSet::new(),
        };
        state.scopes.insert(
            BasicAccountScope::BinanceUnified,
            ScopeState {
                locked: true,
                margin_ratio: 1.87,
                recovery_pending: false,
            },
        );
        state.scopes.insert(
            BasicAccountScope::GateUnified,
            ScopeState {
                locked: false,
                margin_ratio: 2.30,
                recovery_pending: true,
            },
        );

        let (notification, recovered) = build_notification(&state, 2.0, 2.2).unwrap();
        assert_eq!(notification.title, "UniMMR风控");
        assert_eq!(
            notification.message,
            "test-fr｜风险1｜恢复1\nBinance统一账户 1.87<2.00 只减仓\nGate统一账户 2.30>2.20 恢复"
        );
        assert_eq!(notification.severity, NotificationSeverity::Critical);
        assert!(notification.fields.is_empty());
        assert_eq!(recovered, vec![BasicAccountScope::GateUnified]);

        let (repeated, repeated_recovered) = build_notification(&state, 2.0, 2.2).unwrap();
        assert_eq!(repeated, notification);
        assert_eq!(repeated_recovered, recovered);

        for scope in repeated_recovered {
            state.scopes.get_mut(&scope).unwrap().recovery_pending = false;
        }
        let active = state
            .scopes
            .get_mut(&BasicAccountScope::BinanceUnified)
            .unwrap();
        active.locked = false;
        assert!(build_notification(&state, 2.0, 2.2).is_none());
    }
}
