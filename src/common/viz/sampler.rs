use anyhow::Result;
use log::debug;

use crate::common::time_util::get_timestamp_us;
use crate::exchange::Exchange;

use super::config::{SamplingCfg, ThresholdCfg};
use super::model::*;
use super::state::SharedState;

pub struct Sampler {
    state: SharedState,
    sampling: SamplingCfg,
    thresholds: ThresholdCfg,
    exchange: Exchange,
    last_json: Option<String>,
}

impl Sampler {
    pub fn new(state: SharedState, sampling: SamplingCfg, thresholds: ThresholdCfg, exchange: Exchange) -> Self {
        Self { state, sampling, thresholds, exchange, last_json: None }
    }

    pub fn build_snapshot(&mut self) -> Snapshot {
        let ts = get_timestamp_us() / 1000;
        let (total_eq, abs_exp, exposures_raw) = self.state.snapshot_account();
        let exposures = exposures_raw
            .into_iter()
            .map(|e| ExposureItem {
                asset: e.asset,
                spot_total_wallet: e.spot_total_wallet,
                spot_cross_free: e.spot_cross_free,
                spot_cross_locked: e.spot_cross_locked,
                um_net_position: e.um_net_position,
                um_position_initial_margin: e.um_position_initial_margin,
                um_open_order_initial_margin: e.um_open_order_initial_margin,
                exposure: e.exposure,
            })
            .collect();
        let account = AccountSummary { total_equity: total_eq, abs_total_exposure: abs_exp, exposures };

        let price_entries = self.state.snapshot_prices(self.sampling.symbols.as_deref());
        let mut prices: Vec<PriceItem> = Vec::new();
        for (symbol, entry) in price_entries {
            if let Some(e) = entry {
                prices.push(PriceItem { symbol, mark: Some(e.mark_price).filter(|v| *v != 0.0), index: Some(e.index_price).filter(|v| *v != 0.0), ts: e.update_time });
            } else {
                prices.push(PriceItem { symbol, mark: None, index: None, ts: 0 });
            }
        }

        // 基于价格计算 basis 因子
        let mut factors: Vec<FactorItem> = Vec::new();
        for p in &prices {
            let basis = match (p.mark, p.index) { (Some(m), Some(i)) if i.abs() > f64::EPSILON => Some((m - i) / i), _ => None };
            factors.push(FactorItem { symbol: p.symbol.clone(), basis, open_threshold: self.thresholds.open, close_threshold: self.thresholds.close });
        }

        // 资金费率/借贷（预测）
        let mut funding: Vec<FundingItem> = Vec::new();
        for p in &prices {
            let (funding_rate, predicted, next_funding_time, loan_rate_8h) = self.state.funding_for(&p.symbol, self.exchange, ts);
            funding.push(FundingItem { symbol: p.symbol.clone(), funding_rate, predicted_funding_rate: predicted, next_funding_time, loan_rate_8h });
        }

        let pnl = PnlSummary { unrealized: self.state.compute_unrealized_pnl() };

        // stale 判断
        let now_us = get_timestamp_us();
        let acc_stale = now_us - self.state.last_account_update_us() > 5_000_000; // 5s
        let px_stale = now_us - self.state.last_price_update_us() > 5_000_000; // 5s
        let stale = StaleFlags { account: acc_stale, prices: px_stale, funding: false };

        Snapshot { ts, account, prices, funding, factors, pnl, stale }
    }

    pub fn encode_json(&mut self, snap: &Snapshot) -> Result<Option<String>> {
        let json = serde_json::to_string(snap)?;
        if self.sampling.send_if_changed {
            if let Some(prev) = &self.last_json {
                if prev == &json { return Ok(None); }
            }
        }
        self.last_json = Some(json.clone());
        Ok(Some(json))
    }
}
