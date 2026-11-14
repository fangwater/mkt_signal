//! Complete, native-unit COIN-M account observations. No strategy targets.
use serde::{Deserialize, Serialize};

pub const COIN_ACCOUNT_CHANNEL: &str = "coin_account_snapshot";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinAccountSnapshot {
    pub ts_ms: i64,
    pub venue: String,
    pub account_mode: String,
    pub assets: Vec<CoinAsset>,
    pub positions: Vec<CoinPosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinAsset {
    pub asset: String,
    pub wallet_balance: f64,
    pub unrealized_pnl: f64,
    pub equity: f64,
    pub available_balance: f64,
    pub initial_margin: f64,
    pub maintenance_margin: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinPosition {
    pub symbol: String,
    pub settlement_asset: String,
    pub side: String,
    /// Binance positionAmt, in contracts (not coins).
    pub contracts: f64,
    pub entry_price: f64,
    /// Binance notionalValue, in the settlement coin.
    pub notional_coin: f64,
    pub unrealized_pnl: f64,
    pub isolated: bool,
}
