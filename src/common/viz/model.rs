use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Snapshot {
    pub ts: i64,
    pub account: AccountSummary,
    pub prices: Vec<PriceItem>,
    pub funding: Vec<FundingItem>,
    pub factors: Vec<FactorItem>,
    pub pnl: PnlSummary,
    pub stale: StaleFlags,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct AccountSummary {
    pub total_equity: f64,
    pub abs_total_exposure: f64,
    pub exposures: Vec<ExposureItem>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ExposureItem {
    pub asset: String,
    pub spot_total_wallet: f64,
    pub spot_cross_free: f64,
    pub spot_cross_locked: f64,
    pub um_net_position: f64,
    pub um_position_initial_margin: f64,
    pub um_open_order_initial_margin: f64,
    pub exposure: f64,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct PnlSummary {
    pub unrealized: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct PriceItem {
    pub symbol: String,
    pub mark: Option<f64>,
    pub index: Option<f64>,
    pub ts: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct FundingItem {
    pub symbol: String,
    pub funding_rate: Option<f64>,
    pub predicted_funding_rate: f64,
    pub next_funding_time: Option<i64>,
    pub loan_rate_8h: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct FactorItem {
    pub symbol: String,
    pub basis: Option<f64>,
    pub open_threshold: f64,
    pub close_threshold: f64,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct StaleFlags {
    pub account: bool,
    pub prices: bool,
    pub funding: bool,
}
