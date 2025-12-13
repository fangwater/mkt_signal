use anyhow::Result;
use serde::{Deserialize, Serialize};

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
pub struct PreTradePositionRow {
    pub asset: String,
    pub open_qty: f64,
    pub hedge_qty: f64,
    pub net_qty: f64,
    pub net_usdt: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradePositionResampleEntry {
    pub ts_ms: i64,
    pub rows: Vec<PreTradePositionRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeExposureRow {
    pub asset: String,
    pub open_qty: Option<f64>,
    pub open_usdt: Option<f64>,
    pub hedge_qty: Option<f64>,
    pub hedge_usdt: Option<f64>,
    pub net_qty: Option<f64>,
    pub net_usdt: Option<f64>,
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
