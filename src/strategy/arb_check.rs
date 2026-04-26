use crate::common::symbol_util::extract_assets_from_symbol;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::TradingVenue;
use crate::signal::open_signal::ArbOpenCtx;
use crate::strategy::arb_helper::{
    log_force_close_skip, opposite_side, refresh_qty_multipliers, signed_qty_for_open_side,
    ArbOpenSignalParams,
};
use log::error;

const OPEN_BALANCE_EPS: f64 = 1e-12;

pub(crate) struct OpenCheckOutcome {
    pub(crate) open_qty_multiplier: f64,
    pub(crate) hedge_qty_multiplier: f64,
    pub(crate) hedge_side: Side,
}

pub(crate) struct OpenCheckContext<'a> {
    strategy_id: i32,
    strategy_symbol: &'a str,
    force_close: bool,
    ctx: &'a ArbOpenCtx,
    symbol: &'a str,
    hedge_symbol: &'a str,
    venue: TradingVenue,
    hedge_venue: TradingVenue,
    order_type: OrderType,
    open_side: Side,
    aligned_qty: f64,
    aligned_price: f64,
    signed_qty: Option<f64>,
    open_qty_multiplier: Option<f64>,
    hedge_qty_multiplier: Option<f64>,
    hedge_side: Option<Side>,
}

impl<'a> OpenCheckContext<'a> {
    pub(crate) fn from_open_params(
        strategy_id: i32,
        strategy_symbol: &'a str,
        force_close: bool,
        ctx: &'a ArbOpenCtx,
        params: &'a ArbOpenSignalParams,
    ) -> Self {
        Self {
            strategy_id,
            strategy_symbol,
            force_close,
            ctx,
            symbol: &params.symbol,
            hedge_symbol: &params.hedge_symbol,
            venue: params.venue,
            hedge_venue: params.hedge_venue,
            order_type: params.order_type,
            open_side: params.open_side,
            aligned_qty: params.aligned_qty,
            aligned_price: params.aligned_price,
            signed_qty: None,
            open_qty_multiplier: None,
            hedge_qty_multiplier: None,
            hedge_side: None,
        }
    }

    pub(crate) fn run_all(mut self) -> Option<OpenCheckOutcome> {
        if !run_open_pre_checks(&mut self, &OPEN_PRE_CHECKS) {
            return None;
        }

        let (Some(open_qty_multiplier), Some(hedge_qty_multiplier), Some(hedge_side)) = (
            self.open_qty_multiplier,
            self.hedge_qty_multiplier,
            self.hedge_side,
        ) else {
            error!(
                "HedgeArbStrategy: strategy_id={} 开仓检查缺少数量上下文，标记策略为不活跃",
                self.strategy_id
            );
            return None;
        };

        Some(OpenCheckOutcome {
            open_qty_multiplier,
            hedge_qty_multiplier,
            hedge_side,
        })
    }
}

// 开仓前检查是无状态单例；静态 trait object 切片避免每个信号重新分配 Vec/Box。
trait OpenPreCheck: Sync {
    fn name(&self) -> &'static str;

    fn skip_when_force_close(&self) -> bool {
        true
    }

    fn log_force_close_skip(&self) -> bool {
        true
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool;
}

// 单品种敞口检查：限制某个 symbol/base asset 的净敞口占配置上限的比例，
// 防止单一币种风险过度集中。
struct SymbolExposureCheck;

// 总体敞口检查：限制账户整体绝对敞口占权益的比例，
// 防止多个品种叠加后整体风险超过账户承受范围。
struct TotalExposureCheck;

// 限价挂单数量检查：限制同一 symbol/side 上未完成的限价开仓单数量，
// 防止重复挂单堆积。该检查保持原行为，force close 时也会执行。
struct PendingLimitOrderCheck;

// 数量乘数检查：确认 open/hedge 两腿的 venue qty -> base qty 乘数可用，
// 并生成后续检查和下单状态需要的 signed_qty、hedge_side、qty_multiplier。
struct QtyMultiplierCheck;

// Binance STANDARD 余额检查：币安现货/杠杆非统一账户不能自动借币，
// 开仓前必须确认本次买入/卖出需要消耗的资产余额足够。
struct BinanceStandardBalanceCheck;

// 杠杆检查：只有当本次开仓会增加当前绝对持仓时才校验账户杠杆，
// 减仓或降低绝对风险的方向不因为杠杆检查被拦截。
struct LeverageCheck;

// 持仓上限检查：按信号修正后的 signed_qty 和价格估算开仓后仓位，
// 确认不会超过配置的 max_pos_u。
struct MaxPosUCheck;

static SYMBOL_EXPOSURE_CHECK: SymbolExposureCheck = SymbolExposureCheck;
static TOTAL_EXPOSURE_CHECK: TotalExposureCheck = TotalExposureCheck;
static PENDING_LIMIT_ORDER_CHECK: PendingLimitOrderCheck = PendingLimitOrderCheck;
static QTY_MULTIPLIER_CHECK: QtyMultiplierCheck = QtyMultiplierCheck;
static BINANCE_STANDARD_BALANCE_CHECK: BinanceStandardBalanceCheck = BinanceStandardBalanceCheck;
static LEVERAGE_CHECK: LeverageCheck = LeverageCheck;
static MAX_POS_U_CHECK: MaxPosUCheck = MaxPosUCheck;

// 检查顺序有依赖：先做不依赖数量乘数的账户风控，再初始化数量上下文，
// 最后执行需要 signed_qty/open_qty_multiplier 的余额、杠杆和仓位上限检查。
static OPEN_PRE_CHECKS: [&'static dyn OpenPreCheck; 7] = [
    &SYMBOL_EXPOSURE_CHECK,
    &TOTAL_EXPOSURE_CHECK,
    &PENDING_LIMIT_ORDER_CHECK,
    &QTY_MULTIPLIER_CHECK,
    &BINANCE_STANDARD_BALANCE_CHECK,
    &LEVERAGE_CHECK,
    &MAX_POS_U_CHECK,
];

fn run_open_pre_checks(
    input: &mut OpenCheckContext<'_>,
    checks: &[&'static dyn OpenPreCheck],
) -> bool {
    for check in checks {
        if input.force_close && check.skip_when_force_close() {
            if check.log_force_close_skip() {
                log_force_close_skip(input.strategy_id, check.name(), input.ctx);
            }
            continue;
        }

        if !check.check(input) {
            return false;
        }
    }

    true
}

impl OpenPreCheck for SymbolExposureCheck {
    fn name(&self) -> &'static str {
        "单品种敞口"
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        if let Err(e) = MonitorChannel::instance().check_symbol_exposure(input.strategy_symbol) {
            error!("HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃", input.strategy_id, input.strategy_symbol, e);
            return false;
        }

        true
    }
}

impl OpenPreCheck for TotalExposureCheck {
    fn name(&self) -> &'static str {
        "总体敞口"
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        if let Err(e) = MonitorChannel::instance().check_total_exposure() {
            error!(
                "HedgeArbStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                input.strategy_id, e
            );
            return false;
        }

        true
    }
}

impl OpenPreCheck for PendingLimitOrderCheck {
    fn name(&self) -> &'static str {
        "限价挂单数量"
    }

    fn skip_when_force_close(&self) -> bool {
        false
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        if input.order_type != OrderType::Limit {
            return true;
        }

        if let Err(e) = MonitorChannel::instance()
            .check_pending_limit_order(input.strategy_symbol, input.open_side)
        {
            error!("HedgeArbStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃", input.strategy_id, input.strategy_symbol, e);
            return false;
        }

        true
    }
}

impl OpenPreCheck for QtyMultiplierCheck {
    fn name(&self) -> &'static str {
        "数量乘数"
    }

    fn skip_when_force_close(&self) -> bool {
        false
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        let (open_qty_multiplier, hedge_qty_multiplier) = match refresh_qty_multipliers(
            input.venue,
            input.symbol,
            input.hedge_venue,
            input.hedge_symbol,
        ) {
            Ok((open_qty_multiplier, hedge_qty_multiplier)) => {
                (open_qty_multiplier, hedge_qty_multiplier)
            }
            Err(err) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 初始化数量乘数失败: {}",
                    input.strategy_id, err
                );
                return false;
            }
        };

        input.open_qty_multiplier = Some(open_qty_multiplier);
        input.hedge_qty_multiplier = Some(hedge_qty_multiplier);
        input.hedge_side = Some(opposite_side(input.open_side));
        input.signed_qty = Some(signed_qty_for_open_side(input.open_side, input.aligned_qty));
        true
    }
}

impl OpenPreCheck for BinanceStandardBalanceCheck {
    fn name(&self) -> &'static str {
        "BinanceMargin STANDARD 余额"
    }

    fn log_force_close_skip(&self) -> bool {
        false
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        if input.venue != TradingVenue::BinanceMargin
            || !MonitorChannel::instance()
                .order_manager()
                .borrow()
                .binance_is_standard()
        {
            return true;
        }

        // 币安现货/杠杆 STANDARD（非统一账户）模式无法在下单时自动借币，
        // 不能像统一账户或自动借贷模式一样依赖系统隐式补足缺口。
        // 因此开仓前必须按方向检查本次下单实际需要消耗的资产余额。
        let (base_asset, quote_asset) = extract_assets_from_symbol(input.symbol);
        let (check_asset, required_amount) = match input.open_side {
            // 买入需要消耗计价币，例如 BTCUSDT 买入时检查 USDT 是否足够。
            Side::Buy => (quote_asset, input.aligned_qty * input.aligned_price),
            // 卖出需要消耗基础币，例如 BTCUSDT 卖出时检查 BTC 是否足够。
            Side::Sell => (base_asset, input.aligned_qty),
        };
        let available_balance =
            MonitorChannel::instance().balance_position_for_venue(input.venue, &check_asset);

        if available_balance + OPEN_BALANCE_EPS >= required_amount {
            return true;
        }

        // 余额不足时拒绝继续开仓，并将策略标记为不活跃，避免后续反复触发失败下单。
        if input.open_side != Side::Sell {
            error!(
                "HedgeArbStrategy: strategy_id={} BinanceMargin STANDARD 余额不足，拒绝开仓并标记策略不活跃 symbol={} side={:?} asset={} required={:.8} available={:.8}",
                input.strategy_id,
                input.symbol,
                input.open_side,
                check_asset,
                required_amount,
                available_balance
            );
        }

        false
    }
}

impl OpenPreCheck for LeverageCheck {
    fn name(&self) -> &'static str {
        "杠杆"
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        let Some(signed_qty) = input.signed_qty else {
            error!(
                "HedgeArbStrategy: strategy_id={} 杠杆风控检查缺少 signed_qty，标记策略为不活跃",
                input.strategy_id
            );
            return false;
        };
        let Some(open_qty_multiplier) = input.open_qty_multiplier else {
            error!(
                "HedgeArbStrategy: strategy_id={} 杠杆风控检查缺少 open_qty_multiplier，标记策略为不活跃",
                input.strategy_id
            );
            return false;
        };

        let add_base_qty = signed_qty * open_qty_multiplier;
        let current_base_qty =
            MonitorChannel::instance().get_position_qty(input.symbol, input.venue);
        let projected_base_qty = current_base_qty + add_base_qty;
        let reduce_eps = 1e-12_f64;

        if projected_base_qty.abs() <= current_base_qty.abs() + reduce_eps {
            return true;
        }

        if let Err(e) = MonitorChannel::instance().check_leverage() {
            error!(
                "HedgeArbStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                input.strategy_id, e
            );
            return false;
        }

        true
    }
}

impl OpenPreCheck for MaxPosUCheck {
    fn name(&self) -> &'static str {
        "持仓上限 (max_pos_u)"
    }

    fn check(&self, input: &mut OpenCheckContext<'_>) -> bool {
        let Some(signed_qty) = input.signed_qty else {
            error!(
                "HedgeArbStrategy: strategy_id={} 仓位限制检查缺少 signed_qty，标记策略为不活跃",
                input.strategy_id
            );
            return false;
        };

        if let Err(e) = MonitorChannel::instance().ensure_max_pos_u(
            input.symbol,
            signed_qty,
            input.aligned_price,
        ) {
            error!(
                "HedgeArbStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                input.strategy_id, e
            );
            return false;
        }

        true
    }
}
