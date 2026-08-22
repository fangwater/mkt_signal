use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
pub struct PreTradeExposureRow {
    pub asset: String,
    pub open_qty: Option<f64>,
    pub open_usdt: Option<f64>,
    pub hedge_qty: Option<f64>,
    pub hedge_usdt: Option<f64>,
    pub hedge_net_qty: Option<f64>,
    pub hedge_time_ms: Option<i64>,
    pub hedge_is_taker: Option<bool>,
    pub hedge_ret_qtl: Option<f64>,
    pub hedge_offset_low: Option<f64>,
    pub hedge_offset_high: Option<f64>,
    pub arb_hedge_net_qty: Option<f64>,
    pub arb_pending_hedge_qty: Option<f64>,
    pub arb_due_hedge_qty: Option<f64>,
    pub arb_hedge_time_ms: Option<i64>,
    pub arb_hedge_is_taker: Option<bool>,
    pub arb_hedge_ret_qtl: Option<f64>,
    pub arb_hedge_score: Option<f64>,
    /// arb hedge 单一档报单，单值即 last hedge order 的 price_offset。
    pub arb_hedge_offset: Option<f64>,
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
pub struct PreTradeAccountRiskView {
    pub ts_ms: i64,
    pub adj_equity_usd: f64,
    pub actual_equity_usd: f64,
    pub maintenance_margin_usd: f64,
    pub initial_margin_usd: f64,
    pub margin_ratio: f64,
    pub borrowed_usd: f64,
    pub notional_usd: f64,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeRiskResampleEntry {
    pub ts_ms: i64,
    pub signal_counts: HashMap<String, u64>,
    pub total_equity: f64,
    pub total_exposure: f64,
    pub total_position: f64,
    pub spot_equity_usd: f64,
    pub borrowed_usd: f64,
    pub interest_usd: f64,
    pub um_unrealized_usd: f64,
    pub leverage: f64,
    pub max_leverage: f64,
    pub usdt_max_available_margin: Option<f64>,
    pub open_leg: PreTradeVenueRiskResampleEntry,
    pub hedge_leg: PreTradeVenueRiskResampleEntry,
    pub unimmr_force_close_line: f64,
    pub unimmr_force_close_recover_line: f64,
    pub unimmr_trigger_line: f64,
    pub unimmr_recover_line: f64,
    pub account_risks: Vec<PreTradeAccountRiskView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeVenueRiskResampleEntry {
    pub venue: String,
    pub total_equity: f64,
    pub total_position: f64,
    pub spot_equity_usd: f64,
    pub borrowed_usd: f64,
    pub interest_usd: f64,
    pub um_unrealized_usd: f64,
    pub leverage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStrategyStateRow {
    pub strategy_name: String,
    pub source_updated_at_ms: i64,
    pub symbol: String,
    pub position_allocated: bool,
    pub account_position_qty: f64,
    pub target_qty: f64,
    pub current_qty: f64,
    pub effective_position_qty: f64,
    pub delta_qty: f64,
    pub live_order_qty: f64,
    pub pending_qty: f64,
    pub account_position_usdt: f64,
    pub target_usdt: f64,
    pub current_usdt: f64,
    pub delta_usdt: f64,
    pub live_order_usdt: f64,
    pub pending_usdt: f64,
    pub active_batches: u32,
    pub remaining_batches: u32,
    pub estimated_completion_ts_ms: i64,
    pub execution_complete: bool,
    pub completion_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStrategyStateResampleEntry {
    pub ts_ms: i64,
    pub position_ready: bool,
    pub rows: Vec<ExecStrategyStateRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecAccountRiskResampleEntry {
    pub ts_ms: i64,
    pub venue: String,
    pub equity_usdt: f64,
    pub long_notional_usdt: f64,
    pub short_notional_usdt: f64,
    pub net_notional_usdt: f64,
    pub gross_notional_usdt: f64,
    pub leverage: f64,
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

impl_codec!(PreTradeExposureResampleEntry);
impl_codec!(PreTradeRiskResampleEntry);
impl_codec!(ExecStrategyStateResampleEntry);
impl_codec!(ExecAccountRiskResampleEntry);

#[cfg(test)]
mod tests {
    use super::{ExecStrategyStateResampleEntry, ExecStrategyStateRow};

    #[test]
    fn exec_strategy_state_codec_round_trip() {
        let entry = ExecStrategyStateResampleEntry {
            ts_ms: 123,
            position_ready: true,
            rows: vec![ExecStrategyStateRow {
                strategy_name: "cta_alpha".to_string(),
                source_updated_at_ms: 1_700_000_000_000,
                symbol: "BTCUSDT".to_string(),
                position_allocated: true,
                account_position_qty: 0.25,
                target_qty: 1.0,
                current_qty: 0.25,
                effective_position_qty: 0.3,
                delta_qty: 0.7,
                live_order_qty: 0.05,
                pending_qty: 0.65,
                account_position_usdt: 25.0,
                target_usdt: 100.0,
                current_usdt: 25.0,
                delta_usdt: 70.0,
                live_order_usdt: 5.0,
                pending_usdt: 65.0,
                active_batches: 1,
                remaining_batches: 3,
                estimated_completion_ts_ms: 456,
                execution_complete: false,
                completion_reason: String::new(),
            }],
        };

        let decoded = ExecStrategyStateResampleEntry::from_bytes(&entry.to_bytes().unwrap())
            .expect("decode exec strategy state");
        assert_eq!(decoded.ts_ms, entry.ts_ms);
        assert!(decoded.position_ready);
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(decoded.rows[0].strategy_name, "cta_alpha");
        assert_eq!(decoded.rows[0].source_updated_at_ms, 1_700_000_000_000);
        assert_eq!(decoded.rows[0].pending_qty, 0.65);
        assert_eq!(decoded.rows[0].remaining_batches, 3);
        assert_eq!(decoded.rows[0].estimated_completion_ts_ms, 456);
        assert!(!decoded.rows[0].execution_complete);
        assert!(decoded.rows[0].completion_reason.is_empty());
    }
}
