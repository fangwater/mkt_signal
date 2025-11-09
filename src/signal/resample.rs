use anyhow::Result;
use serde::{Deserialize, Serialize};

// Iceoryx channel for streaming funding arbitrage resample entries
pub const FR_RESAMPLE_MSG_CHANNEL: &str = "binance_fr_signal_resample_msg";
pub const PRE_TRADE_POSITIONS_CHANNEL: &str = "pre_trade_positions_resample";
pub const PRE_TRADE_EXPOSURE_CHANNEL: &str = "pre_trade_exposure_resample";
pub const PRE_TRADE_RISK_CHANNEL: &str = "pre_trade_risk_resample";
pub const PRE_TRADE_RESAMPLE_MSG_CHANNEL: &str = "pre_trade_resample_msg";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRateArbResampleEntry {
    pub symbol: String,
    pub ts_ms: i64,
    pub funding_frequency: String,
    pub spot_bid: Option<f64>,
    pub spot_ask: Option<f64>,
    pub fut_bid: Option<f64>,
    pub fut_ask: Option<f64>,
    pub bidask_sr: Option<f64>,
    pub askbid_sr: Option<f64>,
    pub funding_rate: Option<f64>,
    pub funding_rate_ma: Option<f64>,
    pub funding_rate_ma_lower: Option<f64>,
    pub funding_rate_ma_upper: Option<f64>,
    pub predicted_rate: Option<f64>,
    pub predicted_rate_lower: Option<f64>,
    pub predicted_rate_upper: Option<f64>,
    pub loan_rate_8h: Option<f64>,
    pub bidask_lower: Option<f64>,
    pub bidask_upper: Option<f64>,
    pub askbid_lower: Option<f64>,
    pub askbid_upper: Option<f64>,
}

impl FundingRateArbResampleEntry {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let v: Self = bincode::deserialize(data)?;
        Ok(v)
    }
}

// Utility helpers
pub fn compute_bidask_sr(spot_bid: Option<f64>, fut_ask: Option<f64>) -> Option<f64> {
    match (spot_bid, fut_ask) {
        (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => Some((bid - ask) / bid),
        _ => None,
    }
}

pub fn compute_askbid_sr(spot_ask: Option<f64>, fut_bid: Option<f64>) -> Option<f64> {
    match (spot_ask, fut_bid) {
        (Some(ask), Some(bid)) if ask > 0.0 && bid > 0.0 => Some((ask - bid) / ask),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeUmPositionRow {
    pub symbol: String,
    pub side: String,
    pub position_amount: f64,
    pub entry_price: f64,
    pub leverage: f64,
    pub position_initial_margin: f64,
    pub open_order_initial_margin: f64,
    pub unrealized_profit: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeSpotBalanceRow {
    pub asset: String,
    pub total_wallet: f64,
    pub cross_free: f64,
    pub cross_locked: f64,
    pub cross_borrowed: f64,
    pub cross_interest: f64,
    pub um_wallet: f64,
    pub um_unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradePositionResampleEntry {
    pub ts_ms: i64,
    pub um_positions: Vec<PreTradeUmPositionRow>,
    pub spot_balances: Vec<PreTradeSpotBalanceRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeExposureRow {
    pub asset: String,
    pub spot_qty: Option<f64>,
    pub spot_usdt: Option<f64>,
    pub um_net_qty: Option<f64>,
    pub um_net_usdt: Option<f64>,
    pub exposure_qty: Option<f64>,
    pub exposure_usdt: Option<f64>,
    pub is_total: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeExposureResampleEntry {
    pub ts_ms: i64,
    pub rows: Vec<PreTradeExposureRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeRiskResampleEntry {
    pub ts_ms: i64,
    pub total_equity: f64,
    pub total_exposure: f64,
    pub total_position: f64,
    pub spot_equity_usd: f64,
    pub borrowed_usd: f64,
    pub interest_usd: f64,
    pub um_unrealized_usd: f64,
    pub leverage: f64,
    pub max_leverage: f64,
}

macro_rules! impl_codec {
    ($ty:ty) => {
        impl $ty {
            pub fn to_bytes(&self) -> Result<Vec<u8>> {
                Ok(bincode::serialize(self)?)
            }

            pub fn from_bytes(data: &[u8]) -> Result<Self> {
                Ok(bincode::deserialize(data)?)
            }
        }
    };
}

impl_codec!(PreTradePositionResampleEntry);
impl_codec!(PreTradeExposureResampleEntry);
impl_codec!(PreTradeRiskResampleEntry);
