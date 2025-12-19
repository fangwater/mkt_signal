use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use log::info;
use reqwest::Client;

use crate::common::exchange::Exchange;
use crate::common::min_qty_table::{
    BinanceProvider, BitgetProvider, GateProvider, MarketType, MinQtyEntry, OkexProvider,
};
use crate::signal::common::TradingVenue;

type EntryMap = HashMap<String, MinQtyEntry>;
type MultiplierMap = HashMap<String, f64>;

#[async_trait]
trait VenueInfoProvider: Send + Sync {
    async fn fetch(&self, client: &Client) -> Result<(EntryMap, MultiplierMap)>;
}

struct ExchangeVenueProvider {
    venue: TradingVenue,
    exchange: Exchange,
    market_type: MarketType,
}

impl ExchangeVenueProvider {
    fn new(venue: TradingVenue, exchange: Exchange, market_type: MarketType) -> Self {
        Self {
            venue,
            exchange,
            market_type,
        }
    }
}

#[async_trait]
impl VenueInfoProvider for ExchangeVenueProvider {
    async fn fetch(&self, client: &Client) -> Result<(EntryMap, MultiplierMap)> {
        match self.exchange {
            Exchange::Binance => {
                let provider = BinanceProvider::new();
                let entries = provider.fetch_filters(client, self.market_type).await?;
                Ok((entries, HashMap::new()))
            }
            Exchange::Gate => {
                let provider = GateProvider::new();
                provider
                    .fetch_filters_with_multipliers(client, self.market_type)
                    .await
            }
            Exchange::Bitget => {
                let provider = BitgetProvider::new();
                let entries = provider.fetch_filters(client, self.market_type).await?;
                Ok((entries, HashMap::new()))
            }
            Exchange::Okex => {
                let provider = OkexProvider::new();
                provider
                    .fetch_filters_with_multipliers(client, self.market_type)
                    .await
            }
            Exchange::Bybit => Err(anyhow!(
                "exchange {} not supported yet for venue {:?}",
                self.exchange,
                self.venue
            )),
        }
    }
}

fn provider_for_venue(venue: TradingVenue) -> ExchangeVenueProvider {
    let (exchange, market_type) = match venue {
        TradingVenue::BinanceMargin => (Exchange::Binance, MarketType::Margin),
        TradingVenue::BinanceFutures => (Exchange::Binance, MarketType::Futures),
        TradingVenue::OkexMargin => (Exchange::Okex, MarketType::Margin),
        TradingVenue::OkexFutures => (Exchange::Okex, MarketType::Futures),
        TradingVenue::BybitMargin => (Exchange::Bybit, MarketType::Margin),
        TradingVenue::BybitFutures => (Exchange::Bybit, MarketType::Futures),
        TradingVenue::BitgetMargin => (Exchange::Bitget, MarketType::Margin),
        TradingVenue::BitgetFutures => (Exchange::Bitget, MarketType::Futures),
        TradingVenue::GateMargin => (Exchange::Gate, MarketType::Margin),
        TradingVenue::GateFutures => (Exchange::Gate, MarketType::Futures),
    };
    ExchangeVenueProvider::new(venue, exchange, market_type)
}

/// 以固定 TradingVenue 为维度的 min_qty/price_tick 查询表
#[derive(Debug)]
pub struct VenueMinQtyTable {
    venue: TradingVenue,
    client: Client,
    filters: EntryMap,
    contract_multipliers: MultiplierMap,
}

impl VenueMinQtyTable {
    pub fn new(venue: TradingVenue) -> Self {
        Self {
            venue,
            client: Client::new(),
            filters: HashMap::new(),
            contract_multipliers: HashMap::new(),
        }
    }

    pub fn venue(&self) -> TradingVenue {
        self.venue
    }

    /// 刷新当前 venue 的交易对过滤器
    pub async fn refresh(&mut self) -> Result<()> {
        let provider = provider_for_venue(self.venue);
        let (entries, multipliers) = provider.fetch(&self.client).await?;
        info!(
            "刷新交易对过滤器: venue={:?} count={} multipliers={}",
            self.venue,
            entries.len(),
            multipliers.len()
        );
        self.filters = entries;
        self.contract_multipliers = multipliers;
        Ok(())
    }

    fn get_entry(&self, symbol: &str) -> Option<&MinQtyEntry> {
        let key = symbol.to_uppercase();
        self.filters.get(&key)
    }

    pub fn min_qty(&self, symbol: &str) -> Option<f64> {
        self.get_entry(symbol).map(|e| e.min_qty)
    }

    pub fn step_size(&self, symbol: &str) -> Option<f64> {
        self.get_entry(symbol)
            .map(|e| e.step_size)
            .filter(|v| *v > 0.0)
    }

    pub fn price_tick(&self, symbol: &str) -> Option<f64> {
        self.get_entry(symbol)
            .and_then(|e| e.price_tick)
            .filter(|v| *v > 0.0)
    }

    pub fn min_notional(&self, symbol: &str) -> Option<f64> {
        self.get_entry(symbol)
            .and_then(|e| e.min_notional)
            .filter(|v| *v > 0.0)
    }

    /// 返回合约面值（已将 ctVal × ctMult 合并为一个数），查不到则回退为 1
    pub fn contract_multiplier(&self, symbol: &str) -> f64 {
        let key = symbol.to_uppercase();
        self.contract_multipliers.get(&key).copied().unwrap_or(1.0)
    }

    /// 返回合约面值（ctVal × ctMult / quanto_multiplier）；若不存在则返回 None
    pub fn contract_multiplier_opt(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_uppercase();
        self.contract_multipliers.get(&key).copied()
    }
}

#[cfg(test)]
impl VenueMinQtyTable {
    pub fn set_contract_multiplier_for_test(&mut self, symbol: &str, multiplier: f64) {
        self.contract_multipliers
            .insert(symbol.to_uppercase(), multiplier);
    }
}
