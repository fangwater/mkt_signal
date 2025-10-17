use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};

use crate::pre_trade::binance_pm_spot_manager::{BinanceSpotBalance, BinanceSpotBalanceSnapshot};
use crate::pre_trade::binance_pm_um_manager::{
    BinanceUmAccountSnapshot, BinanceUmPosition, PositionSide,
};
use crate::pre_trade::price_table::PriceEntry;
use log::{debug, warn};

/// 单个资产维度的敞口信息。
#[derive(Debug, Clone)]
pub struct ExposureEntry {
    pub asset: String,
    pub spot_total_wallet: f64,
    pub spot_cross_free: f64,
    pub spot_cross_locked: f64,
    pub spot_cross_borrowed: f64,
    pub spot_cross_interest: f64,
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
    total_position: f64,
    total_um_unrealized: f64,
    total_spot_value_usd: f64,
    total_borrowed_usd: f64,
    total_interest_usd: f64,
}

struct ExposureState {
    exposures: Vec<ExposureEntry>,
    usdt: Option<UsdtSummary>,
    total_equity: f64,
    abs_total_exposure: f64,
    total_position: f64,
    total_um_unrealized: f64,
    total_spot_value_usd: f64,
    total_borrowed_usd: f64,
    total_interest_usd: f64,
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
            total_position,
            total_um_unrealized,
            total_spot_value_usd,
            total_borrowed_usd,
            total_interest_usd,
        } = state;

        Self {
            exposures,
            usdt,
            total_equity,
            abs_total_exposure,
            total_position,
            total_um_unrealized,
            total_spot_value_usd,
            total_borrowed_usd,
            total_interest_usd,
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

    pub fn total_position(&self) -> f64 {
        self.total_position
    }

    pub fn total_um_unrealized(&self) -> f64 {
        self.total_um_unrealized
    }

    pub fn total_spot_value_usd(&self) -> f64 {
        self.total_spot_value_usd
    }

    pub fn total_borrowed_usd(&self) -> f64 {
        self.total_borrowed_usd
    }

    pub fn total_interest_usd(&self) -> f64 {
        self.total_interest_usd
    }

    pub fn log_summary(&self, stage: &str) {
        if self.exposures.is_empty() {
            debug!(
                "敞口管理器{}完成: 总权益={:.6}, 总敞口绝对值={:.6}, 非 USDT 敞口为空",
                stage, self.total_equity, self.abs_total_exposure,
            );
            return;
        }
        if let Some(u) = &self.usdt {
            debug!(
                "USDT 汇总: spot={:.6} um={:.6} cm={:.6}",
                u.total_wallet_balance, u.um_wallet_balance, u.cm_wallet_balance
            );
        } else {
            debug!("USDT 汇总: 无记录 (spot=0 um=0 cm=0)");
        }
        debug!(
            "敞口管理器{}完成: 总权益(暂估)={:.6}, 总敞口绝对值(数量)={:.6}, 总头寸(暂估)={:.6}, UM未实现PnL={:.6}, Spot估值={:.6}, 借币={:.6}, 利息={:.6}",
            stage,
            self.total_equity,
            self.abs_total_exposure,
            self.total_position,
            self.total_um_unrealized,
            self.total_spot_value_usd,
            self.total_borrowed_usd,
            self.total_interest_usd,
        );
    }

    pub fn recompute(
        &mut self,
        um_snapshot: &BinanceUmAccountSnapshot,
        spot_snapshot: &BinanceSpotBalanceSnapshot,
    ) -> bool {
        // 先按当前快照计算
        let mut state = Self::compute_state(um_snapshot, spot_snapshot);

        // 若本次缺失 USDT 汇总而历史存在，则沿用历史值，保证总权益包含 USDT
        if state.usdt.is_none() {
            if let Some(prev) = self.usdt.clone() {
                state.usdt = Some(prev);
                state.total_equity = self.total_equity;
                state.abs_total_exposure = self.abs_total_exposure;
                state.total_position = self.total_position;
                state.total_um_unrealized = self.total_um_unrealized;
                state.total_spot_value_usd = self.total_spot_value_usd;
                state.total_borrowed_usd = self.total_borrowed_usd;
                state.total_interest_usd = self.total_interest_usd;
            }
        }

        let positions_changed = Self::positions_changed(&self.exposures, &state.exposures);
        if positions_changed {
            Self::log_state("重算", &state);
        }

        // 提交状态
        self.exposures = state.exposures;
        self.usdt = state.usdt;
        self.total_equity = state.total_equity;
        self.abs_total_exposure = state.abs_total_exposure;
        self.total_position = state.total_position;
        self.total_um_unrealized = state.total_um_unrealized;
        self.total_spot_value_usd = state.total_spot_value_usd;
        self.total_borrowed_usd = state.total_borrowed_usd;
        self.total_interest_usd = state.total_interest_usd;

        positions_changed
    }

    /// 基于标记价格表将资产敞口估值为 USDT，并更新总权益、总敞口与总头寸。
    pub fn revalue_with_prices(&mut self, price_map: &BTreeMap<String, PriceEntry>) {
        let mut total_spot_value = 0.0_f64;
        let mut total_borrowed_value = 0.0_f64;
        let mut total_interest_value = 0.0_f64;
        let mut total_position_value = 0.0_f64;
        let mut abs_total_exposure_usdt = 0.0_f64;

        for e in &self.exposures {
            let asset = e.asset.to_uppercase();
            let mark = if asset == "USDT" {
                1.0
            } else {
                price_map
                    .get(&format!("{}USDT", asset))
                    .map(|p| p.mark_price)
                    .unwrap_or(0.0)
            };

            if mark == 0.0 {
                if asset != "USDT" && (e.spot_total_wallet != 0.0 || e.um_net_position != 0.0) {
                    debug!("缺少 {} 的标记价格, 估值按 0 处理", asset);
                }
                continue;
            }

            let spot_value = e.spot_total_wallet * mark;
            let um_value = e.um_net_position * mark;
            let borrowed_value = e.spot_cross_borrowed * mark;
            let interest_value = e.spot_cross_interest * mark;

            total_spot_value += spot_value;
            total_borrowed_value += borrowed_value;
            total_interest_value += interest_value;

            if asset != "USDT" {
                total_position_value += spot_value.abs() + um_value.abs();
                abs_total_exposure_usdt += (e.spot_total_wallet + e.um_net_position).abs() * mark;
            }
        }

        // Equity = spot估值 - 借币 - 利息 + UM未实现PnL（UM钱包余额已体现在 USDT 现货余额中）
        self.total_spot_value_usd = total_spot_value;
        self.total_borrowed_usd = total_borrowed_value;
        self.total_interest_usd = total_interest_value;
        self.total_equity = (total_spot_value - total_borrowed_value - total_interest_value)
            + self.total_um_unrealized;
        self.abs_total_exposure = abs_total_exposure_usdt;
        self.total_position = total_position_value;
    }

    fn compute_state(
        um_snapshot: &BinanceUmAccountSnapshot,
        spot_snapshot: &BinanceSpotBalanceSnapshot,
    ) -> ExposureState {
        let total_um_unrealized: f64 = um_snapshot
            .positions
            .iter()
            .map(|pos| pos.unrealized_profit)
            .sum();
        let mut spot_map: HashMap<String, BinanceSpotBalance> = HashMap::new();
        for bal in &spot_snapshot.balances {
            spot_map.insert(bal.asset.to_uppercase(), bal.clone());
        }

        let mut known_assets: BTreeSet<String> = spot_map.keys().cloned().collect();
        known_assets.insert("USDT".to_string());

        let usdt_balance = spot_map.get("USDT");
        let usdt = usdt_balance.cloned().map(|bal| UsdtSummary {
            total_wallet_balance: bal.total_wallet_balance,
            cross_margin_free: bal.cross_margin_free,
            cross_margin_locked: bal.cross_margin_locked,
            um_wallet_balance: bal.um_wallet_balance,
            cm_wallet_balance: bal.cm_wallet_balance,
        });
        let usdt_borrowed = usdt_balance
            .map(|bal| bal.cross_margin_borrowed)
            .unwrap_or(0.0);
        let usdt_interest = usdt_balance
            .map(|bal| bal.cross_margin_interest)
            .unwrap_or(0.0);

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

        let mut exposures: Vec<ExposureEntry> = keys
            .into_iter()
            .filter(|asset| !asset.is_empty() && asset != "USDT")
            .map(|asset| {
                let spot = spot_map.get(&asset);
                let um = um_map.get(&asset);
                let spot_total_wallet = spot.map(|b| b.total_wallet_balance).unwrap_or(0.0);
                let spot_cross_free = spot.map(|b| b.cross_margin_free).unwrap_or(0.0);
                let spot_cross_locked = spot.map(|b| b.cross_margin_locked).unwrap_or(0.0);
                let spot_cross_borrowed = spot.map(|b| b.cross_margin_borrowed).unwrap_or(0.0);
                let spot_cross_interest = spot.map(|b| b.cross_margin_interest).unwrap_or(0.0);
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
                    spot_cross_borrowed,
                    spot_cross_interest,
                    um_net_position,
                    um_position_initial_margin,
                    um_open_order_initial_margin,
                    exposure: spot_total_wallet + um_net_position,
                }
            })
            .collect();

        let abs_total_exposure = exposures.iter().map(|e| e.exposure.abs()).sum::<f64>();
        // 初始阶段无法获取全部资产价格，仅保留 USDT spot 余额作为基准，
        // 等待后续 revalue_with_prices 依据行情重新估值。
        let usdt_spot = usdt.as_ref().map(|u| u.total_wallet_balance).unwrap_or(0.0);
        let total_equity = usdt_spot;
        let total_position = 0.0;

        // 在表格中强制加入 USDT（非敞口），便于对账与肉眼核对
        let usdt_entry = ExposureEntry {
            asset: "USDT".to_string(),
            spot_total_wallet: usdt_spot,
            spot_cross_free: usdt.as_ref().map(|u| u.cross_margin_free).unwrap_or(0.0),
            spot_cross_locked: usdt.as_ref().map(|u| u.cross_margin_locked).unwrap_or(0.0),
            spot_cross_borrowed: usdt_borrowed,
            spot_cross_interest: usdt_interest,
            um_net_position: 0.0,
            um_position_initial_margin: 0.0,
            um_open_order_initial_margin: 0.0,
            // USDT 不计入敞口，显式置 0
            exposure: 0.0,
        };
        // 保证在最末尾显示 USDT 行
        exposures.push(usdt_entry);

        ExposureState {
            exposures,
            usdt,
            total_equity,
            abs_total_exposure,
            total_position,
            total_um_unrealized,
            total_spot_value_usd: 0.0,
            total_borrowed_usd: 0.0,
            total_interest_usd: 0.0,
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

        if let Some(u) = &state.usdt {
            debug!(
                "USDT 汇总: spot={:.6} um={:.6} cm={:.6}",
                u.total_wallet_balance, u.um_wallet_balance, u.cm_wallet_balance
            );
        } else {
            debug!("USDT 汇总: 无记录 (spot=0 um=0 cm=0)");
        }

        // 仅打印汇总信息（注意：总敞口绝对值为数量绝对值之和，非 USDT 估值）；
        // 详细三线表（含 USDT 估值）由 runner 结合 mark price 打印
        debug!(
            "敞口管理器{}完成: 总权益(暂估)={:.6}, 总敞口绝对值(数量)={:.6}, 总头寸(暂估)={:.6}, UM未实现PnL={:.6}, Spot估值={:.6}, 借币={:.6}, 利息={:.6}",
            stage,
            state.total_equity,
            state.abs_total_exposure,
            state.total_position,
            state.total_um_unrealized,
            state.total_spot_value_usd,
            state.total_borrowed_usd,
            state.total_interest_usd,
        );
    }
}

impl ExposureManager {
    fn positions_changed(prev: &[ExposureEntry], next: &[ExposureEntry]) -> bool {
        let mut prev_map: HashMap<String, (f64, f64)> = HashMap::new();
        for entry in prev {
            if entry.asset.eq_ignore_ascii_case("USDT") {
                continue;
            }
            prev_map.insert(
                entry.asset.to_uppercase(),
                (entry.spot_total_wallet, entry.um_net_position),
            );
        }

        for entry in next {
            if entry.asset.eq_ignore_ascii_case("USDT") {
                continue;
            }
            let key = entry.asset.to_uppercase();
            let current = (entry.spot_total_wallet, entry.um_net_position);
            match prev_map.remove(&key) {
                Some(prev_vals) => {
                    if !Self::amount_approx_eq(current.0, prev_vals.0)
                        || !Self::amount_approx_eq(current.1, prev_vals.1)
                    {
                        return true;
                    }
                }
                None => {
                    if !Self::amount_approx_eq(current.0, 0.0)
                        || !Self::amount_approx_eq(current.1, 0.0)
                    {
                        return true;
                    }
                }
            }
        }

        for (_, (spot, um)) in prev_map.into_iter() {
            if !Self::amount_approx_eq(spot, 0.0) || !Self::amount_approx_eq(um, 0.0) {
                return true;
            }
        }

        false
    }

    fn amount_approx_eq(lhs: f64, rhs: f64) -> bool {
        const EPS_ABS: f64 = 1e-9;
        const EPS_REL: f64 = 1e-8;
        let diff = (lhs - rhs).abs();
        if diff <= EPS_ABS {
            return true;
        }
        let scale = lhs.abs().max(rhs.abs()).max(1.0);
        diff <= EPS_REL * scale
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
