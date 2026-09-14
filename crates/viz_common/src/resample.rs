use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

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
    pub adj_equity_usd: Option<f64>,
    pub actual_equity_usd: Option<f64>,
    pub maintenance_margin_usd: Option<f64>,
    pub initial_margin_usd: Option<f64>,
    pub margin_ratio: f64,
    pub borrowed_usd: Option<f64>,
    pub notional_usd: Option<f64>,
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
    pub algorithm: String,
    pub pov: Option<ExecPovState>,
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
    /// Used to rebuild derived USDT fields after compact IPC decoding.
    #[serde(skip)]
    pub mid_price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecPovState {
    pub status: String,
    pub participation_rate: f64,
    pub market_base_qty: f64,
    pub filled_base_qty: f64,
    pub reserved_base_qty: f64,
    pub available_base_qty: f64,
    pub last_trade_ts_ms: i64,
    pub deadline_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStrategyStateWirePov {
    pub status_index: u16,
    pub participation_rate: f64,
    pub market_base_qty: f64,
    pub filled_base_qty: f64,
    pub reserved_base_qty: f64,
    pub available_base_qty: f64,
    pub last_trade_ts_ms: i64,
    pub deadline_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStrategyStateWireRow {
    pub algorithm_index: u16,
    pub symbol_index: u16,
    pub source_updated_at_ms: i64,
    pub position_allocated: bool,
    pub account_position_qty: f64,
    pub target_qty: f64,
    pub current_qty: f64,
    pub effective_position_qty: f64,
    pub live_order_qty: f64,
    pub pending_qty: f64,
    pub mid_price: f64,
    pub pov: Option<ExecStrategyStateWirePov>,
    pub active_batches: u32,
    pub remaining_batches: u32,
    pub estimated_completion_ts_ms: i64,
    pub execution_complete: bool,
    pub completion_reason_index: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStrategyStateWireGroup {
    pub strategy_name: String,
    pub rows: Vec<ExecStrategyStateWireRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStrategyStateResampleEntry {
    pub ts_ms: i64,
    pub position_ready: bool,
    pub algorithms: Vec<String>,
    pub symbols: Vec<String>,
    pub pov_statuses: Vec<String>,
    pub completion_reasons: Vec<String>,
    pub groups: Vec<ExecStrategyStateWireGroup>,
}

impl ExecStrategyStateResampleEntry {
    pub fn from_rows(
        ts_ms: i64,
        position_ready: bool,
        mut rows: Vec<ExecStrategyStateRow>,
    ) -> Result<Self> {
        rows.sort_unstable_by(|lhs, rhs| {
            (&lhs.strategy_name, &lhs.symbol).cmp(&(&rhs.strategy_name, &rhs.symbol))
        });

        let mut algorithms = Vec::new();
        let mut algorithm_indices = BTreeMap::new();
        let mut symbols = Vec::new();
        let mut symbol_indices = BTreeMap::new();
        let mut pov_statuses = Vec::new();
        let mut pov_status_indices = BTreeMap::new();
        let mut completion_reasons = Vec::new();
        let mut completion_reason_indices = BTreeMap::new();
        let mut groups: Vec<ExecStrategyStateWireGroup> = Vec::new();

        for row in rows {
            let algorithm_index = intern_string(
                row.algorithm,
                &mut algorithms,
                &mut algorithm_indices,
                "algorithm",
            )?;
            let symbol_index =
                intern_string(row.symbol, &mut symbols, &mut symbol_indices, "symbol")?;
            let completion_reason_index = intern_string(
                row.completion_reason,
                &mut completion_reasons,
                &mut completion_reason_indices,
                "completion reason",
            )?;
            let pov = row
                .pov
                .map(|pov| {
                    let status_index = intern_string(
                        pov.status,
                        &mut pov_statuses,
                        &mut pov_status_indices,
                        "POV status",
                    )?;
                    Ok::<ExecStrategyStateWirePov, anyhow::Error>(ExecStrategyStateWirePov {
                        status_index,
                        participation_rate: pov.participation_rate,
                        market_base_qty: pov.market_base_qty,
                        filled_base_qty: pov.filled_base_qty,
                        reserved_base_qty: pov.reserved_base_qty,
                        available_base_qty: pov.available_base_qty,
                        last_trade_ts_ms: pov.last_trade_ts_ms,
                        deadline_ts_ms: pov.deadline_ts_ms,
                    })
                })
                .transpose()?;

            let wire_row = ExecStrategyStateWireRow {
                algorithm_index,
                symbol_index,
                source_updated_at_ms: row.source_updated_at_ms,
                position_allocated: row.position_allocated,
                account_position_qty: row.account_position_qty,
                target_qty: row.target_qty,
                current_qty: row.current_qty,
                effective_position_qty: row.effective_position_qty,
                live_order_qty: row.live_order_qty,
                pending_qty: row.pending_qty,
                mid_price: row.mid_price,
                pov,
                active_batches: row.active_batches,
                remaining_batches: row.remaining_batches,
                estimated_completion_ts_ms: row.estimated_completion_ts_ms,
                execution_complete: row.execution_complete,
                completion_reason_index,
            };

            match groups.last_mut() {
                Some(group) if group.strategy_name == row.strategy_name => {
                    group.rows.push(wire_row);
                }
                _ => groups.push(ExecStrategyStateWireGroup {
                    strategy_name: row.strategy_name,
                    rows: vec![wire_row],
                }),
            }
        }

        Ok(Self {
            ts_ms,
            position_ready,
            algorithms,
            symbols,
            pov_statuses,
            completion_reasons,
            groups,
        })
    }

    pub fn expanded_rows(&self) -> Result<Vec<ExecStrategyStateRow>> {
        let row_count = self.groups.iter().map(|group| group.rows.len()).sum();
        let mut rows = Vec::with_capacity(row_count);
        for group in &self.groups {
            for row in &group.rows {
                let algorithm =
                    dictionary_value(&self.algorithms, row.algorithm_index, "algorithm")?;
                let symbol = dictionary_value(&self.symbols, row.symbol_index, "symbol")?;
                let completion_reason = dictionary_value(
                    &self.completion_reasons,
                    row.completion_reason_index,
                    "completion reason",
                )?;
                let pov = row
                    .pov
                    .as_ref()
                    .map(|pov| {
                        Ok::<ExecPovState, anyhow::Error>(ExecPovState {
                            status: dictionary_value(
                                &self.pov_statuses,
                                pov.status_index,
                                "POV status",
                            )?,
                            participation_rate: pov.participation_rate,
                            market_base_qty: pov.market_base_qty,
                            filled_base_qty: pov.filled_base_qty,
                            reserved_base_qty: pov.reserved_base_qty,
                            available_base_qty: pov.available_base_qty,
                            last_trade_ts_ms: pov.last_trade_ts_ms,
                            deadline_ts_ms: pov.deadline_ts_ms,
                        })
                    })
                    .transpose()?;

                rows.push(ExecStrategyStateRow {
                    algorithm,
                    pov,
                    strategy_name: group.strategy_name.clone(),
                    source_updated_at_ms: row.source_updated_at_ms,
                    symbol,
                    position_allocated: row.position_allocated,
                    account_position_qty: row.account_position_qty,
                    target_qty: row.target_qty,
                    current_qty: row.current_qty,
                    effective_position_qty: row.effective_position_qty,
                    delta_qty: row.target_qty - row.effective_position_qty,
                    live_order_qty: row.live_order_qty,
                    pending_qty: row.pending_qty,
                    account_position_usdt: row.account_position_qty * row.mid_price,
                    target_usdt: row.target_qty * row.mid_price,
                    current_usdt: row.current_qty * row.mid_price,
                    delta_usdt: (row.target_qty - row.effective_position_qty) * row.mid_price,
                    live_order_usdt: row.live_order_qty * row.mid_price,
                    pending_usdt: row.pending_qty * row.mid_price,
                    active_batches: row.active_batches,
                    remaining_batches: row.remaining_batches,
                    estimated_completion_ts_ms: row.estimated_completion_ts_ms,
                    execution_complete: row.execution_complete,
                    completion_reason,
                    mid_price: row.mid_price,
                });
            }
        }
        Ok(rows)
    }
}

fn intern_string(
    value: String,
    values: &mut Vec<String>,
    indices: &mut BTreeMap<String, u16>,
    kind: &str,
) -> Result<u16> {
    if let Some(index) = indices.get(&value) {
        return Ok(*index);
    }
    let index = u16::try_from(values.len())
        .map_err(|_| anyhow::anyhow!("too many exec state {kind}s for compact index"))?;
    indices.insert(value.clone(), index);
    values.push(value);
    Ok(index)
}

fn dictionary_value<'a>(values: &'a [String], index: u16, kind: &str) -> Result<String> {
    values
        .get(usize::from(index))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("invalid exec state {kind} index: {index}"))
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
    use super::{ExecPovState, ExecStrategyStateResampleEntry, ExecStrategyStateRow};

    fn sample_row(strategy_name: &str, symbol: &str) -> ExecStrategyStateRow {
        ExecStrategyStateRow {
            algorithm: "batch".into(),
            pov: None,
            strategy_name: strategy_name.into(),
            source_updated_at_ms: 1_700_000_000_000,
            symbol: symbol.into(),
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
            completion_reason: "target_tolerance".into(),
            mid_price: 100.0,
        }
    }

    #[test]
    fn exec_strategy_state_codec_round_trip() {
        let entry = ExecStrategyStateResampleEntry::from_rows(
            123,
            true,
            vec![sample_row("cta_alpha", "BTCUSDT")],
        )
        .unwrap();

        let decoded = ExecStrategyStateResampleEntry::from_bytes(&entry.to_bytes().unwrap())
            .expect("decode exec strategy state");
        assert_eq!(decoded.ts_ms, entry.ts_ms);
        assert!(decoded.position_ready);
        let rows = decoded.expanded_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].strategy_name, "cta_alpha");
        assert_eq!(rows[0].source_updated_at_ms, 1_700_000_000_000);
        assert_eq!(rows[0].pending_qty, 0.65);
        assert_eq!(rows[0].remaining_batches, 3);
        assert_eq!(rows[0].estimated_completion_ts_ms, 456);
        assert!(!rows[0].execution_complete);
        assert_eq!(rows[0].completion_reason, "target_tolerance");
        assert_eq!(rows[0].delta_qty, 0.7);
        assert_eq!(rows[0].target_usdt, 100.0);

        let mut pov_row = sample_row("cta_alpha", "ETHUSDT");
        pov_row.algorithm = "pov".into();
        pov_row.pov = Some(ExecPovState {
            status: "expired".into(),
            participation_rate: 0.1,
            market_base_qty: 20.0,
            filled_base_qty: 0.5,
            reserved_base_qty: 1.5,
            available_base_qty: 0.0,
            last_trade_ts_ms: 123,
            deadline_ts_ms: 456,
        });
        let entry = ExecStrategyStateResampleEntry::from_rows(123, true, vec![pov_row]).unwrap();
        let decoded =
            ExecStrategyStateResampleEntry::from_bytes(&entry.to_bytes().unwrap()).unwrap();
        let rows = decoded.expanded_rows().unwrap();
        assert_eq!(rows[0].algorithm, "pov");
        assert_eq!(rows[0].pov.as_ref().unwrap().status, "expired");
        assert_eq!(rows[0].pov.as_ref().unwrap().participation_rate, 0.1);
    }

    #[test]
    fn exec_strategy_state_compact_payload_fits_ipc_limit() {
        let mut rows = Vec::new();
        for (strategy, count) in [
            ("rbf_small", 74),
            ("rbf_big", 41),
            ("SYSTEM_POSITION_CLOSE", 45),
            ("funding", 6),
            ("cta_alpha", 4),
            ("cta_beta", 4),
        ] {
            for index in 0..count {
                rows.push(sample_row(strategy, &format!("TOKEN{index:03}USDT")));
            }
        }
        let entry = ExecStrategyStateResampleEntry::from_rows(123, true, rows).unwrap();
        let payload_len = entry.to_bytes().unwrap().len() + 4;
        assert_eq!(entry.expanded_rows().unwrap().len(), 174);
        assert!(payload_len <= 32 * 1024, "payload_len={payload_len}");
    }
}
