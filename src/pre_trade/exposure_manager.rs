use std::collections::{BTreeSet, HashMap};

use crate::pre_trade::binance_pm_spot_manager::{BinanceSpotBalance, BinanceSpotBalanceSnapshot};
use crate::pre_trade::binance_pm_um_manager::{
    BinanceUmAccountSnapshot, BinanceUmPosition, PositionSide,
};
use log::warn;

#[derive(Debug, Clone)]
pub struct ExposureEntry {
    pub asset: String,
    pub spot_total_wallet: f64,
    pub spot_cross_free: f64,
    pub spot_cross_locked: f64,
    pub um_net_position: f64,
    pub um_position_initial_margin: f64,
    pub um_open_order_initial_margin: f64,
    pub exposure: f64,
}

#[derive(Debug, Clone)]
pub struct UsdtSummary {
    pub total_wallet_balance: f64,
    pub cross_margin_free: f64,
    pub cross_margin_locked: f64,
    pub um_wallet_balance: f64,
    pub cm_wallet_balance: f64,
}

#[derive(Debug, Default)]
struct UmAggregate {
    net_position: f64,
    position_initial_margin: f64,
    open_order_initial_margin: f64,
}

pub struct ExposureManager {
    exposures: Vec<ExposureEntry>,
    usdt: Option<UsdtSummary>,
}

impl ExposureManager {
    pub fn new(
        um_snapshot: &BinanceUmAccountSnapshot,
        spot_snapshot: &BinanceSpotBalanceSnapshot,
    ) -> Self {
        let mut spot_map: HashMap<String, BinanceSpotBalance> = HashMap::new();
        for bal in &spot_snapshot.balances {
            spot_map.insert(bal.asset.to_uppercase(), bal.clone());
        }

        let mut known_assets: BTreeSet<String> = spot_map.keys().cloned().collect();
        known_assets.insert("USDT".to_string());

        let usdt = spot_map.get("USDT").cloned().map(|bal| UsdtSummary {
            total_wallet_balance: bal.total_wallet_balance,
            cross_margin_free: bal.cross_margin_free,
            cross_margin_locked: bal.cross_margin_locked,
            um_wallet_balance: bal.um_wallet_balance,
            cm_wallet_balance: bal.cm_wallet_balance,
        });

        let mut um_map: HashMap<String, UmAggregate> = HashMap::new();
        for position in &um_snapshot.positions {
            let Some(base_asset) = derive_base_asset(&position.symbol, &known_assets) else {
                warn!("无法识别 UM 符号对应资产: {}", position.symbol);
                continue;
            };
            if base_asset.eq_ignore_ascii_case("USDT") {
                // USDT 敞口单独处理
                continue;
            }
            let aggregate = um_map
                .entry(base_asset)
                .or_insert_with(UmAggregate::default);
            let signed_amount = signed_position_amount(position);
            aggregate.net_position += signed_amount;
            aggregate.position_initial_margin += position.position_initial_margin;
            aggregate.open_order_initial_margin += position.open_order_initial_margin;
        }

        let mut keys: BTreeSet<String> = BTreeSet::new();
        keys.extend(spot_map.keys().cloned());
        keys.extend(um_map.keys().cloned());

        let exposures: Vec<ExposureEntry> = keys
            .into_iter()
            .filter(|asset| !asset.is_empty() && asset != "USDT")
            .map(|asset| {
                let spot = spot_map.get(&asset);
                let um = um_map.get(&asset);
                let spot_total_wallet = spot.map(|b| b.total_wallet_balance).unwrap_or(0.0);
                let spot_cross_free = spot.map(|b| b.cross_margin_free).unwrap_or(0.0);
                let spot_cross_locked = spot.map(|b| b.cross_margin_locked).unwrap_or(0.0);
                let um_net_position = um.map(|u| u.net_position).unwrap_or(0.0);
                let um_position_initial_margin =
                    um.map(|u| u.position_initial_margin).unwrap_or(0.0);
                let um_open_order_initial_margin =
                    um.map(|u| u.open_order_initial_margin).unwrap_or(0.0);
                ExposureEntry {
                    asset,
                    spot_total_wallet,
                    spot_cross_free,
                    spot_cross_locked,
                    um_net_position,
                    um_position_initial_margin,
                    um_open_order_initial_margin,
                    exposure: spot_total_wallet + um_net_position,
                }
            })
            .collect();

        Self { exposures, usdt }
    }

    pub fn exposures(&self) -> &[ExposureEntry] {
        &self.exposures
    }

    pub fn usdt_summary(&self) -> Option<&UsdtSummary> {
        self.usdt.as_ref()
    }
}

fn signed_position_amount(position: &BinanceUmPosition) -> f64 {
    match position.position_side {
        PositionSide::Both => position.position_amt,
        PositionSide::Long => position.position_amt.abs(),
        PositionSide::Short => -position.position_amt.abs(),
    }
}

fn derive_base_asset(symbol: &str, known_assets: &BTreeSet<String>) -> Option<String> {
    let upper = symbol.to_uppercase();

    let mut best_match: Option<String> = None;
    let mut best_len = 0usize;
    for asset in known_assets {
        if upper.starts_with(asset) && asset.len() > best_len {
            best_len = asset.len();
            best_match = Some(asset.clone());
        }
    }

    if let Some(asset) = best_match {
        return Some(asset);
    }

    const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
    for quote in QUOTES {
        if upper.ends_with(quote) && upper.len() > quote.len() {
            return Some(upper[..upper.len() - quote.len()].to_string());
        }
    }

    None
}
