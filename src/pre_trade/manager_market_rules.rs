use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::symbol_util::min_qty_symbol_key;
use serde::Deserialize;
use signal_common::min_qty_table::{MarketType, MinQtyEntry};

use crate::pre_trade::monitor_channel::MonitorChannel;

const MARKET_RULES_KEY: &str = "market_rules";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagerMarketRule {
    status: String,
    base_asset: String,
    quote_asset: String,
    price_tick: String,
    qty_step: String,
    min_qty: String,
    min_notional: Option<String>,
    contract_multiplier: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagerMarketRulesSnapshot {
    venue: String,
    fetched_at_us: i64,
    symbols: BTreeMap<String, ManagerMarketRule>,
}

pub struct ManagerMarketRulesReloader {
    client: RedisClient,
    venue: TradingVenue,
    last_fetched_at_us: i64,
}

impl ManagerMarketRulesReloader {
    pub async fn connect(redis: RedisSettings, venue: TradingVenue) -> Result<Self> {
        if !matches!(
            venue,
            TradingVenue::BinanceFutures
                | TradingVenue::BinanceCoinFutures
                | TradingVenue::OkexFutures
        ) {
            bail!("Manager market-rules cache does not support venue {venue:?}");
        }
        Ok(Self {
            client: RedisClient::connect(redis).await?,
            venue,
            last_fetched_at_us: 0,
        })
    }

    pub async fn reload(&mut self) -> Result<bool> {
        let Some(snapshot) = self
            .client
            .get_json::<ManagerMarketRulesSnapshot>(MARKET_RULES_KEY)
            .await
            .with_context(|| format!("load Redis key {MARKET_RULES_KEY}"))?
        else {
            return Ok(false);
        };
        if snapshot.fetched_at_us == self.last_fetched_at_us {
            return Ok(false);
        }
        if snapshot.fetched_at_us < self.last_fetched_at_us {
            bail!(
                "Manager market-rules timestamp moved backwards: previous={} current={}",
                self.last_fetched_at_us,
                snapshot.fetched_at_us
            );
        }

        let fetched_at_us = snapshot.fetched_at_us;
        let (market_type, filters, multipliers, tradable_symbols) =
            snapshot.into_tables(self.venue)?;
        let symbol_count = filters.len();
        MonitorChannel::replace_manager_order_rules(
            self.venue,
            market_type,
            filters,
            multipliers,
            tradable_symbols,
        )
        .map_err(anyhow::Error::msg)?;
        self.last_fetched_at_us = fetched_at_us;
        info!(
            "Manager market-rules cache applied: venue={:?} fetched_at_us={} symbols={}",
            self.venue, fetched_at_us, symbol_count
        );
        Ok(true)
    }

    pub fn spawn(mut self, interval: Duration) {
        tokio::task::spawn_local(async move {
            let mut timer = tokio::time::interval(interval);
            timer.tick().await;
            loop {
                timer.tick().await;
                if let Err(err) = self.reload().await {
                    warn!(
                        "Manager market-rules reload failed; retaining last good in-memory snapshot: {err:#}"
                    );
                }
            }
        });
    }
}

impl ManagerMarketRulesSnapshot {
    fn into_tables(
        &self,
        expected_venue: TradingVenue,
    ) -> Result<(
        MarketType,
        HashMap<String, MinQtyEntry>,
        HashMap<String, f64>,
        HashSet<String>,
    )> {
        let expected_slug = expected_venue.data_pub_slug();
        if self.venue != expected_slug {
            bail!(
                "Manager market-rules venue mismatch: expected={} actual={}",
                expected_slug,
                self.venue
            );
        }
        if self.fetched_at_us <= 0 {
            bail!("Manager market-rules fetched_at_us must be positive");
        }
        if self.symbols.is_empty() {
            bail!("Manager market-rules snapshot is empty");
        }
        let market_type = match expected_venue {
            TradingVenue::BinanceFutures | TradingVenue::OkexFutures => MarketType::Futures,
            TradingVenue::BinanceCoinFutures => MarketType::CoinFutures,
            _ => unreachable!(),
        };
        let mut filters = HashMap::with_capacity(self.symbols.len());
        let mut multipliers = HashMap::new();
        let mut tradable_symbols = HashSet::new();
        for (raw_symbol, rule) in &self.symbols {
            let symbol = min_qty_symbol_key(expected_venue, raw_symbol);
            if symbol.is_empty() || symbol != *raw_symbol {
                bail!("Manager market-rules symbol is not normalized: {raw_symbol}");
            }
            if rule.status.trim().is_empty()
                || rule.base_asset.trim().is_empty()
                || rule.quote_asset.trim().is_empty()
            {
                bail!("Manager market-rules identity fields are empty: {symbol}");
            }
            let entry = MinQtyEntry {
                symbol: symbol.clone(),
                base_asset: rule.base_asset.to_uppercase(),
                quote_asset: rule.quote_asset.to_uppercase(),
                min_qty: positive_decimal(&symbol, "min_qty", &rule.min_qty)?,
                step_size: positive_decimal(&symbol, "qty_step", &rule.qty_step)?,
                price_tick: Some(positive_decimal(&symbol, "price_tick", &rule.price_tick)?),
                min_notional: rule
                    .min_notional
                    .as_deref()
                    .map(|value| positive_decimal(&symbol, "min_notional", value))
                    .transpose()?,
            };
            if filters.insert(symbol.clone(), entry).is_some() {
                bail!("duplicate Manager market-rules symbol: {symbol}");
            }
            if status_is_tradable(expected_venue, &rule.status) {
                tradable_symbols.insert(symbol.clone());
            }
            if let Some(value) = rule.contract_multiplier.as_deref() {
                multipliers.insert(
                    symbol.clone(),
                    positive_decimal(&symbol, "contract_multiplier", value)?,
                );
            }
        }
        Ok((market_type, filters, multipliers, tradable_symbols))
    }
}

fn status_is_tradable(venue: TradingVenue, status: &str) -> bool {
    match venue {
        TradingVenue::BinanceFutures | TradingVenue::BinanceCoinFutures => {
            status.eq_ignore_ascii_case("TRADING")
        }
        TradingVenue::OkexFutures => status.eq_ignore_ascii_case("live"),
        _ => false,
    }
}

fn positive_decimal(symbol: &str, field: &str, value: &str) -> Result<f64> {
    let parsed = value.parse::<f64>().with_context(|| {
        format!("Manager market-rules invalid decimal: {symbol}.{field}={value}")
    })?;
    if !parsed.is_finite() || parsed <= 0.0 {
        bail!("Manager market-rules decimal must be positive: {symbol}.{field}={value}");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot() -> ManagerMarketRulesSnapshot {
        ManagerMarketRulesSnapshot {
            venue: "binance-futures".to_string(),
            fetched_at_us: 123,
            symbols: BTreeMap::from([(
                "牛来USDT".to_string(),
                ManagerMarketRule {
                    status: "TRADING".to_string(),
                    base_asset: "牛来".to_string(),
                    quote_asset: "USDT".to_string(),
                    price_tick: "0.0001".to_string(),
                    qty_step: "1".to_string(),
                    min_qty: "1".to_string(),
                    min_notional: Some("5".to_string()),
                    contract_multiplier: Some("1".to_string()),
                },
            )]),
        }
    }

    #[test]
    fn converts_manager_snapshot_to_atomic_tables() {
        let (market_type, filters, multipliers, tradable_symbols) = sample_snapshot()
            .into_tables(TradingVenue::BinanceFutures)
            .unwrap();
        assert_eq!(market_type, MarketType::Futures);
        assert_eq!(filters["牛来USDT"].price_tick, Some(0.0001));
        assert_eq!(multipliers["牛来USDT"], 1.0);
        assert!(tradable_symbols.contains("牛来USDT"));
    }

    #[test]
    fn rejects_wrong_venue() {
        assert!(sample_snapshot()
            .into_tables(TradingVenue::OkexFutures)
            .is_err());
    }

    #[test]
    fn preserves_binance_coin_futures_rule_key() {
        let mut snapshot = sample_snapshot();
        snapshot.venue = "binance-coin-futures".to_string();
        snapshot.symbols = BTreeMap::from([(
            "BTCUSD_PERP".to_string(),
            ManagerMarketRule {
                status: "TRADING".to_string(),
                base_asset: "BTC".to_string(),
                quote_asset: "USD".to_string(),
                price_tick: "0.1".to_string(),
                qty_step: "1".to_string(),
                min_qty: "1".to_string(),
                min_notional: None,
                contract_multiplier: Some("100".to_string()),
            },
        )]);

        let (market_type, filters, multipliers, tradable_symbols) = snapshot
            .into_tables(TradingVenue::BinanceCoinFutures)
            .unwrap();
        assert_eq!(market_type, MarketType::CoinFutures);
        assert!(filters.contains_key("BTCUSD_PERP"));
        assert_eq!(multipliers["BTCUSD_PERP"], 100.0);
        assert!(tradable_symbols.contains("BTCUSD_PERP"));
    }

    #[test]
    fn rejects_invalid_decimal_without_partial_apply() {
        let mut snapshot = sample_snapshot();
        snapshot.symbols.get_mut("牛来USDT").unwrap().qty_step = "0".to_string();
        assert!(snapshot.into_tables(TradingVenue::BinanceFutures).is_err());
    }

    #[test]
    fn deserializes_manager_wire_contract_without_version_dispatch() {
        let raw = r#"{
            "venue":"binance-futures",
            "fetched_at_us":123,
            "symbols":{
                "BTCUSDT":{
                    "status":"TRADING",
                    "base_asset":"BTC",
                    "quote_asset":"USDT",
                    "price_tick":"0.1",
                    "qty_step":"0.001",
                    "min_qty":"0.001",
                    "min_notional":"5",
                    "contract_multiplier":"1"
                }
            }
        }"#;
        let snapshot: ManagerMarketRulesSnapshot = serde_json::from_str(raw).unwrap();
        let (_, filters, _, tradable_symbols) =
            snapshot.into_tables(TradingVenue::BinanceFutures).unwrap();
        assert_eq!(filters["BTCUSDT"].step_size, 0.001);
        assert!(tradable_symbols.contains("BTCUSDT"));
    }

    #[test]
    fn retains_delisted_rules_but_excludes_symbol_from_tradable_set() {
        let mut snapshot = sample_snapshot();
        snapshot.symbols.get_mut("牛来USDT").unwrap().status = "SETTLING".to_string();

        let (_, filters, _, tradable_symbols) =
            snapshot.into_tables(TradingVenue::BinanceFutures).unwrap();

        assert!(filters.contains_key("牛来USDT"));
        assert!(!tradable_symbols.contains("牛来USDT"));
    }
}
