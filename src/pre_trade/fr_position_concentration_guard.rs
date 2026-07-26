use anyhow::{bail, Context, Result};
use log::{debug, info};
use mkt_parsers::symbol_match::normalize_symbol_for_whitelist;
use order_common::TradingVenue;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use trade_signal::ArbMode;

use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;

const POSITION_EPSILON_USDT: f64 = 1e-6;

#[derive(Debug, Clone)]
struct GuardConfig {
    redis: RedisSettings,
    dump_key: String,
    pos_dump_key: String,
}

#[derive(Debug)]
struct GuardState {
    config: GuardConfig,
    alerting_symbols: BTreeSet<String>,
}

thread_local! {
    static GUARD_STATE: RefCell<Option<GuardState>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DumpAction {
    AddPosDump,
    HoldPosDump,
    ExistingDump,
    AlertOnly,
    RemovePosDump,
    None,
}

impl DumpAction {
    fn label(self, ratio: f64, dump_ratio: f64, in_dump: bool) -> &'static str {
        match self {
            Self::AddPosDump => "added_pos_dump",
            Self::HoldPosDump if ratio < dump_ratio => "held_pos_dump_until_recover",
            Self::HoldPosDump => "held_pos_dump",
            Self::ExistingDump => "existing_dump_preserved",
            Self::AlertOnly if in_dump => "existing_dump_alert_only",
            Self::AlertOnly => "alert_only",
            Self::RemovePosDump if in_dump => "removed_pos_dump_existing_dump_preserved",
            Self::RemovePosDump => "removed_pos_dump",
            Self::None if in_dump => "existing_dump_preserved",
            Self::None => "no_dump_change",
        }
    }
}

#[derive(Debug)]
struct ConcentrationEvent {
    symbol: String,
    position_usdt: f64,
    total_position_usdt: f64,
    ratio: f64,
    action: DumpAction,
    in_dump: bool,
    recovered: bool,
}

pub struct FrPositionConcentrationGuard;

impl FrPositionConcentrationGuard {
    pub async fn initialize(
        redis: &RedisSettings,
        env_name: Option<String>,
        arb_mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        if arb_mode != ArbMode::FundingArb {
            GUARD_STATE.with(|state| *state.borrow_mut() = None);
            info!(
                "FR position concentration guard disabled: mode={} open={:?} hedge={:?}",
                arb_mode.as_str(),
                open_venue,
                hedge_venue
            );
            return Ok(());
        }

        let Some(env_name) = env_name
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
        else {
            bail!("FR position concentration guard requires runtime env name from cwd");
        };
        let suffix = format!(
            "{}_{}",
            open_venue.data_pub_slug(),
            hedge_venue.data_pub_slug()
        );
        let mut settings = redis.clone();
        settings.prefix = None;
        let config = GuardConfig {
            redis: settings.clone(),
            dump_key: format!("{env_name}:fr_dump_symbols:{suffix}"),
            pos_dump_key: format!("{env_name}:fr_pos_dump_symbols:{suffix}"),
        };

        let mut client = RedisClient::connect(settings)
            .await
            .context("connect Redis for FR position concentration guard")?;
        load_symbol_list(&mut client, &config.dump_key).await?;
        load_symbol_list(&mut client, &config.pos_dump_key).await?;

        let params = PreTradeParamsLoader::instance();
        let alert_ratio = params.fr_position_concentration_alert_ratio();
        let dump_ratio = params.fr_position_concentration_dump_ratio();
        info!(
            "FR position concentration guard enabled: alert_lock={:.2}% dump={:.2}% recover_below={:.2}% thresholds=dynamic schedule=config_refresh dump_key={} pos_dump_key={}",
            alert_ratio * 100.0,
            dump_ratio * 100.0,
            alert_ratio * 100.0,
            config.dump_key,
            config.pos_dump_key
        );
        GUARD_STATE.with(|state| {
            *state.borrow_mut() = Some(GuardState {
                config,
                alerting_symbols: BTreeSet::new(),
            });
        });
        Ok(())
    }

    pub async fn refresh() -> Result<()> {
        let Some((config, mut alerting_symbols)) = GUARD_STATE.with(|state| {
            state
                .borrow()
                .as_ref()
                .map(|state| (state.config.clone(), state.alerting_symbols.clone()))
        }) else {
            return Ok(());
        };
        let status = MonitorChannel::instance().arb_startup_net_gate_status();
        if status.enabled && !status.ready {
            info!(
                "FR position concentration guard skipped config refresh: position snapshots not ready open_ready={} hedge_ready={}",
                status.open_ready, status.hedge_ready
            );
            return Ok(());
        }

        let mut client = RedisClient::connect(config.redis.clone())
            .await
            .context("connect Redis for FR position concentration refresh")?;
        let dump_values = load_symbol_list(&mut client, &config.dump_key).await?;
        let pos_dump_values = load_symbol_list(&mut client, &config.pos_dump_key).await?;
        let dump_symbols = normalized_set(dump_values.iter().map(String::as_str));
        let mut pos_dump_symbols = normalized_set(pos_dump_values.iter().map(String::as_str));
        let params = PreTradeParamsLoader::instance();
        let alert_ratio = params.fr_position_concentration_alert_ratio();
        let dump_ratio = params.fr_position_concentration_dump_ratio();
        let result = evaluate_and_sync(
            &mut client,
            &config,
            &dump_symbols,
            &mut pos_dump_symbols,
            &mut alerting_symbols,
            alert_ratio,
            dump_ratio,
        )
        .await;
        GUARD_STATE.with(|state| {
            if let Some(state) = state.borrow_mut().as_mut() {
                state.alerting_symbols = alerting_symbols;
            }
        });
        result
    }

    pub fn should_block_arb_open(opening_symbol: &str, reducing: bool) -> bool {
        let locked = GUARD_STATE.with(|state| {
            state
                .borrow()
                .as_ref()
                .is_some_and(|state| state.alerting_symbols.contains(opening_symbol))
        });

        if !locked {
            return false;
        }
        if reducing {
            debug!(
                "FR position concentration reduce-only lock allows reducing ArbOpen: symbol={}",
                opening_symbol
            );
            return false;
        }

        info!(
            "ArbOpen blocked by FR position concentration reduce-only lock: symbol={} alert_lock={:.2}% reducing=false",
            opening_symbol,
            PreTradeParamsLoader::instance().fr_position_concentration_alert_ratio() * 100.0
        );
        true
    }
}

async fn evaluate_and_sync(
    client: &mut RedisClient,
    config: &GuardConfig,
    dump_symbols: &BTreeSet<String>,
    pos_dump_symbols: &mut BTreeSet<String>,
    alerting_symbols: &mut BTreeSet<String>,
    alert_ratio: f64,
    dump_ratio: f64,
) -> Result<()> {
    let original_pos_dump_symbols = pos_dump_symbols.clone();

    let (positions_by_asset, total_position_usdt) =
        MonitorChannel::instance().gross_position_usdt_snapshot();
    let mut positions_by_symbol = BTreeMap::new();
    for (asset, position_usdt) in positions_by_asset {
        if !(position_usdt.is_finite() && position_usdt > POSITION_EPSILON_USDT) {
            continue;
        }
        let Some(symbol) = asset_to_symbol(&asset) else {
            continue;
        };
        *positions_by_symbol.entry(symbol).or_insert(0.0) += position_usdt;
    }

    let mut symbols: BTreeSet<String> = positions_by_symbol.keys().cloned().collect();
    symbols.extend(pos_dump_symbols.iter().cloned());
    symbols.extend(alerting_symbols.iter().cloned());

    let total_is_valid =
        total_position_usdt.is_finite() && total_position_usdt > POSITION_EPSILON_USDT;
    let mut events = Vec::new();
    for symbol in symbols {
        let position_usdt = positions_by_symbol.get(&symbol).copied().unwrap_or(0.0);
        let ratio = if total_is_valid {
            (position_usdt / total_position_usdt).max(0.0)
        } else {
            0.0
        };
        let in_dump = dump_symbols.contains(&symbol);
        let in_pos_dump = pos_dump_symbols.contains(&symbol);
        let was_alerting = alerting_symbols.contains(&symbol);
        let action = decide_dump_action(ratio, alert_ratio, dump_ratio, in_dump, in_pos_dump);

        match action {
            DumpAction::AddPosDump => {
                pos_dump_symbols.insert(symbol.clone());
            }
            DumpAction::RemovePosDump => {
                pos_dump_symbols.remove(&symbol);
            }
            _ => {}
        }

        if ratio >= alert_ratio {
            alerting_symbols.insert(symbol.clone());
            events.push(ConcentrationEvent {
                symbol,
                position_usdt,
                total_position_usdt,
                ratio,
                action,
                in_dump,
                recovered: false,
            });
        } else {
            alerting_symbols.remove(&symbol);
            if was_alerting || action == DumpAction::RemovePosDump {
                events.push(ConcentrationEvent {
                    symbol,
                    position_usdt,
                    total_position_usdt,
                    ratio,
                    action,
                    in_dump,
                    recovered: true,
                });
            }
        }
    }

    let changed = *pos_dump_symbols != original_pos_dump_symbols;
    let sync_result = if changed {
        let values = pos_dump_symbols.iter().cloned().collect::<Vec<_>>();
        client
            .set_json(&config.pos_dump_key, &values)
            .await
            .with_context(|| format!("write pos dump key={}", config.pos_dump_key))
    } else {
        Ok(())
    };
    let sync_label = if sync_result.is_ok() { "ok" } else { "failed" };
    if sync_result.is_err() {
        *pos_dump_symbols = original_pos_dump_symbols;
    }

    for event in events {
        if event.recovered {
            info!(
                "FR position concentration recovered: symbol={} symbol_position_usdt={:.2} total_position_usdt={:.2} ratio={:.4}% recover_below={:.2}% action={} sync={}",
                event.symbol,
                event.position_usdt,
                event.total_position_usdt,
                event.ratio * 100.0,
                alert_ratio * 100.0,
                event.action.label(event.ratio, dump_ratio, event.in_dump),
                sync_label
            );
        } else {
            info!(
                "FR position concentration warning: symbol={} symbol_position_usdt={:.2} total_position_usdt={:.2} ratio={:.4}% alert={:.2}% dump={:.2}% recover_below={:.2}% action={} sync={}",
                event.symbol,
                event.position_usdt,
                event.total_position_usdt,
                event.ratio * 100.0,
                alert_ratio * 100.0,
                dump_ratio * 100.0,
                alert_ratio * 100.0,
                event.action.label(event.ratio, dump_ratio, event.in_dump),
                sync_label
            );
        }
    }

    sync_result
}

fn decide_dump_action(
    ratio: f64,
    alert_ratio: f64,
    dump_ratio: f64,
    in_dump: bool,
    in_pos_dump: bool,
) -> DumpAction {
    if ratio >= dump_ratio {
        if in_pos_dump {
            DumpAction::HoldPosDump
        } else if in_dump {
            DumpAction::ExistingDump
        } else {
            DumpAction::AddPosDump
        }
    } else if ratio >= alert_ratio {
        if in_pos_dump {
            DumpAction::HoldPosDump
        } else if in_dump {
            DumpAction::ExistingDump
        } else {
            DumpAction::AlertOnly
        }
    } else if in_pos_dump {
        DumpAction::RemovePosDump
    } else {
        DumpAction::None
    }
}

async fn load_symbol_list(client: &mut RedisClient, key: &str) -> Result<Vec<String>> {
    let Some(raw) = client.get_string(key).await? else {
        return Ok(Vec::new());
    };
    let value: Value =
        serde_json::from_str(&raw).with_context(|| format!("parse Redis symbol list key={key}"))?;
    let Some(items) = value.as_array() else {
        bail!("Redis symbol list is not a JSON array: key={key}");
    };
    Ok(items
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect())
}

fn normalized_set<'a>(values: impl IntoIterator<Item = &'a str>) -> BTreeSet<String> {
    values.into_iter().filter_map(normalize_symbol).collect()
}

fn asset_to_symbol(asset: &str) -> Option<String> {
    let asset = asset.trim().to_ascii_uppercase();
    if asset.is_empty() || asset == "USDT" {
        return None;
    }
    if asset.ends_with("USDT") {
        normalize_symbol(&asset)
    } else {
        normalize_symbol(&format!("{asset}USDT"))
    }
}

fn normalize_symbol(symbol: &str) -> Option<String> {
    let normalized = normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures);
    (!normalized.is_empty()).then_some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALERT_RATIO: f64 = 0.10;
    const DUMP_RATIO: f64 = 0.12;

    #[test]
    fn alerts_at_ten_percent_without_dumping() {
        assert_eq!(
            decide_dump_action(ALERT_RATIO, ALERT_RATIO, DUMP_RATIO, false, false),
            DumpAction::AlertOnly
        );
    }

    #[test]
    fn adds_pos_dump_at_twelve_percent() {
        assert_eq!(
            decide_dump_action(DUMP_RATIO, ALERT_RATIO, DUMP_RATIO, false, false),
            DumpAction::AddPosDump
        );
    }

    #[test]
    fn existing_dump_is_never_claimed_by_pos_dump() {
        assert_eq!(
            decide_dump_action(DUMP_RATIO, ALERT_RATIO, DUMP_RATIO, true, false),
            DumpAction::ExistingDump
        );
    }

    #[test]
    fn pos_dump_is_held_until_below_recover_threshold() {
        assert_eq!(
            decide_dump_action(0.11, ALERT_RATIO, DUMP_RATIO, false, true),
            DumpAction::HoldPosDump
        );
        assert_eq!(
            decide_dump_action(ALERT_RATIO, ALERT_RATIO, DUMP_RATIO, false, true),
            DumpAction::HoldPosDump
        );
        assert_eq!(
            decide_dump_action(ALERT_RATIO - 0.0001, ALERT_RATIO, DUMP_RATIO, false, true),
            DumpAction::RemovePosDump
        );
    }

    #[test]
    fn normal_dump_is_preserved_on_recovery() {
        assert_eq!(
            decide_dump_action(0.08, ALERT_RATIO, DUMP_RATIO, true, false),
            DumpAction::None
        );
    }

    #[test]
    fn concentration_lock_blocks_only_non_reducing_arb_open() {
        GUARD_STATE.with(|state| {
            *state.borrow_mut() = Some(GuardState {
                config: GuardConfig {
                    redis: RedisSettings::default(),
                    dump_key: String::new(),
                    pos_dump_key: String::new(),
                },
                alerting_symbols: BTreeSet::from(["BTCUSDT".to_string()]),
            });
        });

        assert!(FrPositionConcentrationGuard::should_block_arb_open(
            "BTCUSDT", false
        ));
        assert!(!FrPositionConcentrationGuard::should_block_arb_open(
            "BTCUSDT", true
        ));
        assert!(!FrPositionConcentrationGuard::should_block_arb_open(
            "ETHUSDT", false
        ));
        GUARD_STATE.with(|state| *state.borrow_mut() = None);
    }

    #[test]
    fn normalizes_position_assets_to_internal_symbols() {
        assert_eq!(asset_to_symbol("btc"), Some("BTCUSDT".to_string()));
        assert_eq!(asset_to_symbol("BTC-USDT"), Some("BTCUSDT".to_string()));
        assert_eq!(asset_to_symbol("USDT"), None);
    }
}
