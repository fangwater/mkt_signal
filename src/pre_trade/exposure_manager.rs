use std::collections::{BTreeSet, HashMap};

use crate::pre_trade::binance_pm_spot_manager::{BinanceSpotBalance, BinanceSpotBalanceSnapshot};
use crate::pre_trade::binance_pm_um_manager::{
    BinanceUmAccountSnapshot, BinanceUmPosition, PositionSide,
};
use log::{debug, warn};

/// 单个资产维度的敞口信息。
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

/// USDT 资产汇总，用于计算账户总权益。
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

/// 敞口管理器，负责汇总现货与合约的资产敞口，并提供风控辅助查询。
pub struct ExposureManager {
    exposures: Vec<ExposureEntry>,
    usdt: Option<UsdtSummary>,
    total_equity: f64,
    abs_total_exposure: f64,
}

struct ExposureState {
    exposures: Vec<ExposureEntry>,
    usdt: Option<UsdtSummary>,
    total_equity: f64,
    abs_total_exposure: f64,
}

impl ExposureManager {
    pub fn new(
        um_snapshot: &BinanceUmAccountSnapshot,
        spot_snapshot: &BinanceSpotBalanceSnapshot,
    ) -> Self {
        let state = Self::compute_state(um_snapshot, spot_snapshot);
        Self::log_state("初始化", &state);
        let ExposureState {
            exposures,
            usdt,
            total_equity,
            abs_total_exposure,
        } = state;

        Self {
            exposures,
            usdt,
            total_equity,
            abs_total_exposure,
        }
    }

    pub fn exposures(&self) -> &[ExposureEntry] {
        &self.exposures
    }

    pub fn usdt_summary(&self) -> Option<&UsdtSummary> {
        self.usdt.as_ref()
    }

    /// 根据资产名称（大写）查找敞口信息。
    pub fn exposure_for_asset(&self, asset: &str) -> Option<&ExposureEntry> {
        self.exposures
            .iter()
            .find(|entry| entry.asset.eq_ignore_ascii_case(asset))
    }

    /// 返回账户总权益（近似值），用于敞口比例计算。
    pub fn total_equity(&self) -> f64 {
        self.total_equity
    }

    /// 返回所有资产敞口绝对值之和。
    pub fn total_abs_exposure(&self) -> f64 {
        self.abs_total_exposure
    }

    pub fn recompute(
        &mut self,
        um_snapshot: &BinanceUmAccountSnapshot,
        spot_snapshot: &BinanceSpotBalanceSnapshot,
    ) {
        let state = Self::compute_state(um_snapshot, spot_snapshot);
        Self::log_state("重算", &state);
        let ExposureState {
            exposures,
            usdt,
            total_equity,
            abs_total_exposure,
        } = state;

        self.exposures = exposures;
        self.usdt = usdt;
        self.total_equity = total_equity;
        self.abs_total_exposure = abs_total_exposure;
    }

    fn compute_state(
        um_snapshot: &BinanceUmAccountSnapshot,
        spot_snapshot: &BinanceSpotBalanceSnapshot,
    ) -> ExposureState {
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

        let abs_total_exposure = exposures.iter().map(|e| e.exposure.abs()).sum::<f64>();
        let total_spot_abs = exposures
            .iter()
            .map(|e| e.spot_total_wallet.abs())
            .sum::<f64>();
        let total_um_abs = exposures
            .iter()
            .map(|e| e.um_net_position.abs())
            .sum::<f64>();
        let usdt_total_wallet = usdt.as_ref().map(|u| u.total_wallet_balance).unwrap_or(0.0);
        let total_equity = usdt_total_wallet + total_spot_abs + total_um_abs;

        ExposureState {
            exposures,
            usdt,
            total_equity,
            abs_total_exposure,
        }
    }

    fn log_state(stage: &str, state: &ExposureState) {
        if state.exposures.is_empty() {
            debug!(
                "敞口管理器{}完成: 总权益={:.6}, 总敞口绝对值={:.6}, 非 USDT 敞口为空",
                stage, state.total_equity, state.abs_total_exposure,
            );
            return;
        }

        let rows: Vec<Vec<String>> = state
            .exposures
            .iter()
            .map(|entry| {
                vec![
                    entry.asset.clone(),
                    fmt_decimal(entry.spot_total_wallet),
                    fmt_decimal(entry.um_net_position),
                    fmt_decimal(entry.um_position_initial_margin),
                    fmt_decimal(entry.um_open_order_initial_margin),
                    fmt_decimal(entry.exposure),
                ]
            })
            .collect();

        let table = render_three_line_table(
            &[
                "Asset",
                "SpotTotal",
                "UMNet",
                "UMPosIM",
                "UMOpenIM",
                "Exposure",
            ],
            &rows,
        );

        debug!(
            "敞口管理器{}完成: 总权益={:.6}, 总敞口绝对值={:.6}\n{}",
            stage, state.total_equity, state.abs_total_exposure, table
        );
    }
}

fn signed_position_amount(position: &BinanceUmPosition) -> f64 {
    match position.position_side {
        PositionSide::Both => position.position_amt,
        PositionSide::Long => position.position_amt.abs(),
        PositionSide::Short => -position.position_amt.abs(),
    }
}

fn fmt_decimal(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let mut s = format!("{:.6}", value);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths = compute_widths(headers, rows);
    let mut out = String::new();
    out.push_str(&build_separator(&widths, '-'));
    out.push('\n');
    out.push_str(&build_row(
        headers
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&build_separator(&widths, '='));
    if rows.is_empty() {
        out.push('\n');
        out.push_str(&build_separator(&widths, '-'));
        return out;
    }
    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row.clone(), &widths));
    }
    out.push('\n');
    out.push_str(&build_separator(&widths, '-'));
    out
}

fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                continue;
            }
            widths[idx] = widths[idx].max(cell.len());
        }
    }
    widths
}

fn build_separator(widths: &[usize], fill: char) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&fill.to_string().repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    for (cell, width) in cells.iter().zip(widths.iter()) {
        row.push(' ');
        row.push_str(&format!("{:<width$}", cell, width = *width));
        row.push(' ');
        row.push('|');
    }
    row
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
