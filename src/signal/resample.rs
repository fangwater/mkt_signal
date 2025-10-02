use anyhow::Result;
use serde::{Deserialize, Serialize};

// Iceoryx channels for resampled funding/market snapshot
pub const FR_RESAMPLE_CHANNEL: &str = "binance_fr_signal_resample";
pub const FR_RESAMPLE_MSG_CHANNEL: &str = "binance_fr_signal_resample_msg";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResampleItem {
    pub symbol: String,
    pub ts_ms: i64,
    pub spot_bid: Option<f64>,
    pub spot_ask: Option<f64>,
    pub fut_bid: Option<f64>,
    pub fut_ask: Option<f64>,
    pub bidask_sr: Option<f64>,
    pub askbid_sr: Option<f64>,
    pub funding_rate: Option<f64>,
    pub funding_rate_ma: Option<f64>,
    pub predicted_rate: Option<f64>,
    pub loan_rate_8h: Option<f64>,
}

impl ResampleItem {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let v: Self = bincode::deserialize(data)?;
        Ok(v)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResampleBatch {
    pub ts_ms: i64,
    pub items: Vec<ResampleItem>,
}

impl ResampleBatch {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
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

