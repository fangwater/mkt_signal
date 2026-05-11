use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::{ResamplePublisher, RESAMPLE_PAYLOAD};
use crate::common::min_qty_table::MinQtyTable;
use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::basic_balance_manager::BasicBalanceManager;
use crate::pre_trade::basic_exposure_manager::BasicExposureManager;
use crate::pre_trade::basic_um_manager::BasicUmManager;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::price_table::PriceEntry;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::symbol_util::extract_base_asset;
use crate::signal::common::TradingVenue;
use crate::viz::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradeRiskResampleEntry,
    PreTradeVenueRiskResampleEntry,
};
use anyhow::Result;
use log::{debug, info, trace, warn};
use std::cell::OnceCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

fn print_exposure_table(
    ts_ms: i64,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    mut rows: Vec<(String, f64, f64, f64, f64)>,
    abs_sum_usdt: f64,
) {
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    const FOOTER_LABEL: &str = "ABS_SUM";

    let open_hdr = format!("OpenQty({})", open_venue.data_pub_slug());
    let hedge_hdr = format!("HedgeQty({})", hedge_venue.data_pub_slug());

    let headers = [
        "Asset",
        open_hdr.as_str(),
        hedge_hdr.as_str(),
        "ExposureQty",
        "ExposureUSDT",
        "AbsSumUSDT",
    ];

    let fmt_qty = |v: f64| format!("{:.8}", v);
    let fmt_u = |v: f64| format!("{:.2}", v);

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for (asset, open, hedge, net, net_usdt) in &rows {
        widths[0] = widths[0].max(asset.len());
        widths[1] = widths[1].max(fmt_qty(*open).len());
        widths[2] = widths[2].max(fmt_qty(*hedge).len());
        widths[3] = widths[3].max(fmt_qty(*net).len());
        widths[4] = widths[4].max(fmt_u(*net_usdt).len());
    }
    widths[0] = widths[0].max(FOOTER_LABEL.len());
    widths[5] = widths[5].max(fmt_u(abs_sum_usdt).len());

    let rule = |widths: &[usize]| -> String {
        let mut s = String::new();
        s.push('+');
        for w in widths {
            s.push_str(&"-".repeat(*w + 2));
            s.push('+');
        }
        s
    };

    let fmt_row = |cols: [&str; 6], widths: &[usize]| -> String {
        let mut out = String::new();
        out.push('|');
        for (i, (c, w)) in cols.iter().zip(widths.iter()).enumerate() {
            out.push(' ');
            if i == 0 {
                out.push_str(&format!("{:<width$}", c, width = *w));
            } else {
                out.push_str(&format!("{:>width$}", c, width = *w));
            }
            out.push(' ');
            out.push('|');
        }
        out
    };

    let ts_utc = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts_ms)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| "<invalid_ts_ms>".to_string());

    let top_rule = rule(&widths);
    let mid_rule = rule(&widths);

    println!("\nts_utc={} (ts_ms={})", ts_utc, ts_ms);
    println!("{top_rule}");
    println!("{}", fmt_row(headers, &widths));
    println!("{mid_rule}");
    for (asset, open, hedge, net, net_usdt) in &rows {
        println!(
            "{}",
            fmt_row(
                [
                    asset.as_str(),
                    fmt_qty(*open).as_str(),
                    fmt_qty(*hedge).as_str(),
                    fmt_qty(*net).as_str(),
                    fmt_u(*net_usdt).as_str(),
                    "",
                ],
                &widths
            )
        );
    }
    println!("{mid_rule}");
    println!(
        "{}",
        fmt_row(
            [FOOTER_LABEL, "", "", "", "", fmt_u(abs_sum_usdt).as_str()],
            &widths
        )
    );
    println!("{top_rule}");
}

fn sum_position_usd(
    exchange: Exchange,
    balance_mgrs: &[&BasicBalanceManager],
    um_mgrs: &[(&BasicUmManager, &MinQtyTable)],
    price_snapshot: &BTreeMap<String, PriceEntry>,
) -> f64 {
    let price_mapper = create_symbol_mapper(exchange);
    BasicExposureManager::compute_exposures_for_exchange(exchange, balance_mgrs, um_mgrs)
        .into_iter()
        .filter(|entry| !entry.asset.eq_ignore_ascii_case("USDT"))
        .filter_map(|entry| {
            let symbol = price_mapper.asset_to_price_symbol(&entry.asset);
            let mark = price_snapshot
                .get(&symbol)
                .map(|p| p.mark_price)
                .unwrap_or(0.0);
            if mark <= 0.0 {
                None
            } else {
                Some(entry.exposure.abs() * mark)
            }
        })
        .sum()
}

fn sum_borrow_interest_usd(
    balance_mgr: &BasicBalanceManager,
    price_snapshot: &BTreeMap<String, PriceEntry>,
) -> (f64, f64) {
    let exchange = balance_mgr.exchange();
    let price_mapper = create_symbol_mapper(exchange);
    let mut borrowed_usd = 0.0_f64;
    let mut interest_usd = 0.0_f64;
    for entry in BasicExposureManager::compute_exposures_for_exchange(
        exchange,
        std::slice::from_ref(&balance_mgr),
        &[],
    ) {
        if entry.borrowed.abs() <= 1e-12 && entry.interest.abs() <= 1e-12 {
            continue;
        }
        if entry.asset.eq_ignore_ascii_case("USDT") {
            continue;
        }
        let symbol = price_mapper.asset_to_price_symbol(&entry.asset);
        let mark = price_snapshot
            .get(&symbol)
            .map(|p| p.mark_price)
            .unwrap_or(0.0);
        if mark <= 0.0 {
            continue;
        }
        borrowed_usd += entry.borrowed * mark;
        interest_usd += entry.interest * mark;
    }
    (borrowed_usd, interest_usd)
}

fn hedge_snapshot_symbol_key(symbol: &str) -> String {
    normalize_symbol_for_internal(symbol)
}

fn arb_hedge_snapshot_asset_key(symbol: &str) -> String {
    extract_base_asset(symbol)
        .unwrap_or_else(|| normalize_symbol_for_internal(symbol))
        .to_ascii_uppercase()
}

fn compute_leg_risk_entry(
    mon: &MonitorChannel,
    venue: TradingVenue,
    include_usdt_scope: bool,
    price_snapshot: &BTreeMap<String, PriceEntry>,
) -> PreTradeVenueRiskResampleEntry {
    let exchange = Exchange::from_str(venue.trade_engine_exchange()).unwrap_or(Exchange::Binance);
    let mut total_equity = 0.0_f64;
    let mut total_position = 0.0_f64;
    let mut borrowed_usd = 0.0_f64;
    let mut interest_usd = 0.0_f64;
    let mut um_unrealized_usd = 0.0_f64;

    let balance_mgr: Option<Rc<_>> = if mon.open_venue() == venue {
        mon.open_balance_mgr()
    } else if mon.hedge_venue() == venue {
        mon.hedge_balance_mgr()
    } else {
        None
    };
    if let Some(balance_mgr) = balance_mgr {
        let mgr = balance_mgr.borrow();
        let mgr_ref: &BasicBalanceManager = &mgr;
        let mut exposure_mgr =
            BasicExposureManager::new_from_sources(exchange, std::slice::from_ref(&mgr_ref), &[]);
        exposure_mgr.revalue_with_prices(price_snapshot);
        total_equity += exposure_mgr.total_equity();
        total_position += sum_position_usd(
            exchange,
            std::slice::from_ref(&mgr_ref),
            &[],
            price_snapshot,
        );
        let (borrowed, interest) = sum_borrow_interest_usd(mgr_ref, price_snapshot);
        borrowed_usd += borrowed;
        interest_usd += interest;
    }

    let um_mgr: Option<(Rc<_>, Rc<_>)> = if mon.open_venue() == venue {
        mon.open_um_mgr()
    } else if mon.hedge_venue() == venue {
        mon.hedge_um_mgr()
    } else {
        None
    };
    if let Some((um_mgr, min_qty_table)) = um_mgr {
        let um = um_mgr.borrow();
        let min_qty = min_qty_table.borrow();
        let um_ref: &BasicUmManager = &um;
        let min_qty_ref: &MinQtyTable = &min_qty;
        total_position += sum_position_usd(
            exchange,
            &[],
            std::slice::from_ref(&(um_ref, min_qty_ref)),
            price_snapshot,
        );
        let upl = um_ref.total_unrealized_pnl_usdt();
        um_unrealized_usd += upl;
        if !matches!(exchange, Exchange::Gate | Exchange::Okex) {
            total_equity += upl;
        }
    }

    if include_usdt_scope {
        if let Some(snapshot) = mon.usdt_snapshot_for_venue(venue) {
            total_equity += snapshot.balance;
            borrowed_usd += snapshot.borrowed;
            interest_usd += snapshot.cumulative_interest;
        }
    }

    let spot_equity_usd = total_equity - um_unrealized_usd;
    let leverage = if total_equity.abs() <= f64::EPSILON {
        0.0
    } else {
        total_position / total_equity
    };

    PreTradeVenueRiskResampleEntry {
        venue: venue.data_pub_slug().to_string(),
        total_equity,
        total_position,
        spot_equity_usd,
        borrowed_usd,
        interest_usd,
        um_unrealized_usd,
        leverage,
    }
}

fn print_usdt_summary(open_venue: TradingVenue, hedge_venue: TradingVenue, mon: &MonitorChannel) {
    let Some(open_ex) = Exchange::from_str(open_venue.trade_engine_exchange()) else {
        return;
    };
    let Some(hedge_ex) = Exchange::from_str(hedge_venue.trade_engine_exchange()) else {
        return;
    };

    let open_snap = mon.usdt_snapshot_for_venue(open_venue).unwrap_or_default();
    let hedge_snap = mon.usdt_snapshot_for_venue(hedge_venue).unwrap_or_default();

    let fmt_u = |v: f64| format!("{:.2}", v);
    let open_net = open_snap.balance;
    let hedge_net = hedge_snap.balance;

    let (total_balance, total_borrowed, total_interest, total_net) = if open_ex == hedge_ex {
        (
            open_snap.balance,
            open_snap.borrowed,
            open_snap.cumulative_interest,
            open_net,
        )
    } else {
        (
            open_snap.balance + hedge_snap.balance,
            open_snap.borrowed + hedge_snap.borrowed,
            open_snap.cumulative_interest + hedge_snap.cumulative_interest,
            open_net + hedge_net,
        )
    };

    // 若完全没有更新过（避免刷屏）
    if open_snap.last_timestamp == 0
        && hedge_snap.last_timestamp == 0
        && total_balance.abs() <= 1e-12
        && total_borrowed.abs() <= 1e-12
        && total_interest.abs() <= 1e-12
    {
        return;
    }

    trace!(
        "USDT(open:{}): balance={} borrowed={} interest={} net={}",
        open_ex.as_str(),
        fmt_u(open_snap.balance),
        fmt_u(open_snap.borrowed),
        fmt_u(open_snap.cumulative_interest),
        fmt_u(open_net)
    );
    trace!(
        "USDT(hedge:{}): balance={} borrowed={} interest={} net={}",
        hedge_ex.as_str(),
        fmt_u(hedge_snap.balance),
        fmt_u(hedge_snap.borrowed),
        fmt_u(hedge_snap.cumulative_interest),
        fmt_u(hedge_net)
    );
    trace!(
        "USDT(total): balance={} borrowed={} interest={} net={}",
        fmt_u(total_balance),
        fmt_u(total_borrowed),
        fmt_u(total_interest),
        fmt_u(total_net)
    );
}

thread_local! {
    static RESAMPLE_CHANNEL: OnceCell<ResampleChannel> = const { OnceCell::new() };
}

/// 默认敞口采样频道名称
pub const DEFAULT_EXPOSURE_CHANNEL: &str = "pre_trade_exposure";

/// 默认风险采样频道名称
pub const DEFAULT_RISK_CHANNEL: &str = "pre_trade_risk";

/// 前端展示采样频道 - 负责发布敞口、风险采样数据
///
/// 采用线程本地单例模式，通过 `ResampleChannel::with()` 访问
///
/// # 使用示例
/// ```ignore
/// use crate::pre_trade::ResampleChannel;
///
/// // 显式初始化自定义频道
/// ResampleChannel::initialize("custom_exposure", "custom_risk")?;
/// ```
pub struct ResampleChannel {
    exposure_pub: Option<ResamplePublisher>,
    risk_pub: Option<ResamplePublisher>,
}

impl ResampleChannel {
    /// 在当前线程的 ResampleChannel 单例上执行操作
    ///
    /// 第一次调用时会自动初始化默认频道，后续调用直接使用已初始化的实例
    ///
    /// # 使用示例
    /// ```ignore
    /// // 发布敞口/风险数据
    /// ResampleChannel::with(|ch| {
    ///     let _ = ch;
    /// });
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&ResampleChannel) -> R,
    {
        RESAMPLE_CHANNEL.with(|cell| {
            let channel = cell.get_or_init(|| {
                info!("Initializing thread-local ResampleChannel singleton with default config");
                ResampleChannel::new(DEFAULT_EXPOSURE_CHANNEL, DEFAULT_RISK_CHANNEL)
            });
            f(channel)
        })
    }

    /// 显式初始化采样频道（可选）
    ///
    /// 如果在首次调用 `with()` 之前调用此方法，可以自定义频道名称
    ///
    /// # 参数
    /// * `exposure_channel` - 敞口数据频道名称
    /// * `risk_channel` - 风险数据频道名称
    ///
    /// # 错误
    /// 如果已经初始化，返回错误
    pub fn initialize(exposure_channel: &str, risk_channel: &str) -> Result<()> {
        RESAMPLE_CHANNEL.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow::anyhow!("ResampleChannel already initialized"));
            }
            cell.set(ResampleChannel::new(exposure_channel, risk_channel))
                .map_err(|_| anyhow::anyhow!("Failed to set ResampleChannel (race condition)"))
        })
    }

    /// 创建 ResampleChannel，尝试初始化所有 publisher
    ///
    /// 如果某个 publisher 创建失败，会记录警告但不会导致整体失败
    ///
    /// 注意：通常应使用 `ResampleChannel::with()` 访问线程本地单例，
    /// 而不是直接调用 `new()` 创建多个实例
    fn new(exposure_channel: &str, risk_channel: &str) -> Self {
        let make_pub = |channel: &str, desc: &str| {
            ResamplePublisher::new_with_prefix("viz_pubs", channel)
                .map_err(|e| warn!("ResampleChannel {} failed: {e:#}", desc))
                .ok()
        };

        Self {
            exposure_pub: make_pub(exposure_channel, "exposure"),
            risk_pub: make_pub(risk_channel, "risk"),
        }
    }

    pub fn start_exposure_table_printer(interval: Duration) {
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;
                ResampleChannel::with(|ch| ch.print_exposure_table_snapshot());
            }
        });
        info!(
            "pre_trade exposure table printer started (interval={:?})",
            interval
        );
    }

    fn print_exposure_table_snapshot(&self) {
        let mon = MonitorChannel::instance();
        let price_snapshot = mon.price_table().borrow().snapshot();
        let ts_ms = get_timestamp_us() / 1000;
        let (exposures, _total_equity, total_abs_exposure, _total_position, _um_unrealized_usd) =
            mon.basic_state_snapshot();
        let price_mapper = create_symbol_mapper(mon.mark_price_exchange());

        let mut rows = Vec::new();
        for (asset, &(open_qty, hedge_qty)) in &exposures {
            let asset_upper = asset.to_ascii_uppercase();
            if open_qty.abs() <= 1e-12 && hedge_qty.abs() <= 1e-12 {
                continue;
            }
            let net_qty = open_qty + hedge_qty;
            let symbol = price_mapper.asset_to_price_symbol(&asset_upper);
            let mark = price_snapshot
                .get(&symbol)
                .map(|p| p.mark_price)
                .unwrap_or(0.0);
            let net_usdt = net_qty * mark;
            rows.push((asset_upper, open_qty, hedge_qty, net_qty, net_usdt));
        }

        let open_venue = mon.open_venue();
        let hedge_venue = mon.hedge_venue();
        if !rows.is_empty() {
            print_exposure_table(ts_ms, open_venue, hedge_venue, rows, total_abs_exposure);
        }
        // USDT 不在敞口表内，单独输出（按 exchange 维度）
        print_usdt_summary(open_venue, hedge_venue, &mon);
    }

    /// 获取敞口数据 publisher 的引用
    pub fn exposure_pub(&self) -> Option<&ResamplePublisher> {
        self.exposure_pub.as_ref()
    }

    /// 获取风险数据 publisher 的引用
    pub fn risk_pub(&self) -> Option<&ResamplePublisher> {
        self.risk_pub.as_ref()
    }

    /// 检查敞口 publisher 是否可用
    pub fn is_exposure_publisher_available(&self) -> bool {
        self.exposure_pub.is_some()
    }

    /// 检查风险 publisher 是否可用
    pub fn is_risk_publisher_available(&self) -> bool {
        self.risk_pub.is_some()
    }

    /// basic 模式下的重采样（发布敞口/风险）
    pub fn publish_resample_entries(&self) -> Result<usize> {
        if self.exposure_pub.is_none() && self.risk_pub.is_none() {
            return Ok(0);
        }

        let mon = MonitorChannel::instance();
        let price_snapshot = mon.price_table().borrow().snapshot();
        let ts_ms = get_timestamp_us() / 1000;
        let exposure_price_mapper = create_symbol_mapper(mon.mark_price_exchange());

        let (exposures, total_equity, total_abs_exposure, total_position, um_unrealized_usd) =
            mon.basic_state_snapshot();

        let mut published = 0usize;

        // 发布敞口数据
        if let Some(publisher) = self.exposure_pub.as_ref() {
            let mut rows: Vec<PreTradeExposureRow> = Vec::new();
            let mut exposure_sum_usdt = 0.0_f64;
            let hedge_snapshots = mon.strategy_mgr().borrow().mm_hedge_snapshots();
            let arb_hedge_snapshots = mon
                .strategy_mgr()
                .borrow()
                .arb_hedge_snapshots(ts_ms * 1000);
            let hedge_snapshot_by_symbol: HashMap<
                String,
                (
                    f64,
                    Option<i64>,
                    Option<bool>,
                    Option<f64>,
                    Option<f64>,
                    Option<f64>,
                ),
            > = hedge_snapshots
                .into_iter()
                .map(|snap| {
                    (
                        hedge_snapshot_symbol_key(&snap.symbol),
                        (
                            snap.net_qty,
                            snap.hedge_ts_ms,
                            snap.hedge_is_taker,
                            snap.ret_qtl,
                            snap.offset_low,
                            snap.offset_high,
                        ),
                    )
                })
                .collect();
            let arb_hedge_snapshot_by_asset: HashMap<
                String,
                (
                    f64,
                    f64,
                    f64,
                    Option<i64>,
                    Option<bool>,
                    Option<f64>,
                    Option<f64>,
                ),
            > = arb_hedge_snapshots
                .into_iter()
                .map(|snap| {
                    (
                        arb_hedge_snapshot_asset_key(&snap.symbol),
                        (
                            snap.net_qty,
                            snap.pending_hedge_qty,
                            snap.due_hedge_qty,
                            snap.hedge_ts_ms,
                            snap.hedge_is_taker,
                            snap.ret_qtl,
                            snap.offset,
                        ),
                    )
                })
                .collect();

            let mut exposure_items: Vec<(String, f64, f64)> = exposures
                .iter()
                .map(|(asset, &(open_qty, hedge_qty))| (asset.to_uppercase(), open_qty, hedge_qty))
                .collect();
            for asset in arb_hedge_snapshot_by_asset.keys() {
                if asset == "USDT" {
                    continue;
                }
                exposure_items.push((asset.clone(), 0.0, 0.0));
            }
            exposure_items.sort_by(|a, b| a.0.cmp(&b.0));
            exposure_items.dedup_by(|a, b| a.0 == b.0);

            for (asset_upper, open_qty, hedge_qty) in exposure_items {
                if asset_upper == "USDT" {
                    continue;
                }
                let (
                    arb_hedge_net_qty,
                    arb_pending_hedge_qty,
                    arb_due_hedge_qty,
                    arb_hedge_time_ms,
                    arb_hedge_is_taker,
                    arb_hedge_ret_qtl,
                    arb_hedge_offset,
                ) = arb_hedge_snapshot_by_asset
                    .get(&asset_upper)
                    .map(
                        |(
                            net_qty,
                            pending_hedge_qty,
                            due_hedge_qty,
                            hedge_ts_ms,
                            hedge_is_taker,
                            ret_qtl,
                            offset,
                        )| {
                            (
                                Some(*net_qty),
                                Some(*pending_hedge_qty),
                                Some(*due_hedge_qty),
                                *hedge_ts_ms,
                                *hedge_is_taker,
                                *ret_qtl,
                                *offset,
                            )
                        },
                    )
                    .unwrap_or((None, None, None, None, None, None, None));
                let has_arb_hedge = arb_hedge_net_qty.map(|v| v.abs() > 1e-12).unwrap_or(false)
                    || arb_pending_hedge_qty
                        .map(|v| v.abs() > 1e-12)
                        .unwrap_or(false)
                    || arb_due_hedge_qty.map(|v| v.abs() > 1e-12).unwrap_or(false)
                    || arb_hedge_time_ms.is_some()
                    || arb_hedge_is_taker.is_some()
                    || arb_hedge_ret_qtl.is_some();
                if open_qty.abs() <= 1e-12 && hedge_qty.abs() <= 1e-12 && !has_arb_hedge {
                    continue;
                }

                let symbol = exposure_price_mapper.asset_to_price_symbol(&asset_upper);
                let mark = price_snapshot
                    .get(&symbol)
                    .map(|p| p.mark_price)
                    .unwrap_or(0.0);
                if mark == 0.0 {
                    debug!("missing mark price for {} when resampling exposure", symbol);
                }

                let open_usdt = open_qty * mark;
                let hedge_usdt = hedge_qty * mark;
                let hedge_snapshot_key = hedge_snapshot_symbol_key(&symbol);
                let (
                    hedge_net_qty,
                    hedge_time_ms,
                    hedge_is_taker,
                    hedge_ret_qtl,
                    hedge_offset_low,
                    hedge_offset_high,
                ) = hedge_snapshot_by_symbol
                    .get(&hedge_snapshot_key)
                    .map(
                        |(
                            net_qty,
                            hedge_ts_ms,
                            hedge_is_taker,
                            hedge_ret_qtl,
                            hedge_offset_low,
                            hedge_offset_high,
                        )| {
                            (
                                Some(*net_qty),
                                *hedge_ts_ms,
                                *hedge_is_taker,
                                *hedge_ret_qtl,
                                *hedge_offset_low,
                                *hedge_offset_high,
                            )
                        },
                    )
                    .unwrap_or((None, None, None, None, None, None));
                let net_qty = open_qty + hedge_qty;
                let net_usdt = open_usdt + hedge_usdt;
                exposure_sum_usdt += net_usdt;

                rows.push(PreTradeExposureRow {
                    asset: asset_upper.clone(),
                    open_qty: Some(open_qty),
                    open_usdt: Some(open_usdt),
                    hedge_qty: Some(hedge_qty),
                    hedge_usdt: Some(hedge_usdt),
                    hedge_net_qty,
                    hedge_time_ms,
                    hedge_is_taker,
                    hedge_ret_qtl,
                    hedge_offset_low,
                    hedge_offset_high,
                    arb_hedge_net_qty,
                    arb_pending_hedge_qty,
                    arb_due_hedge_qty,
                    arb_hedge_time_ms,
                    arb_hedge_is_taker,
                    arb_hedge_ret_qtl,
                    arb_hedge_offset,
                    net_qty: Some(net_qty),
                    net_usdt: Some(net_usdt),
                    is_total: false,
                });
            }

            if !rows.is_empty() {
                rows.push(PreTradeExposureRow {
                    asset: "TOTAL".to_string(),
                    open_qty: None,
                    open_usdt: None,
                    hedge_qty: None,
                    hedge_usdt: None,
                    hedge_net_qty: None,
                    hedge_time_ms: None,
                    hedge_is_taker: None,
                    hedge_ret_qtl: None,
                    hedge_offset_low: None,
                    hedge_offset_high: None,
                    arb_hedge_net_qty: None,
                    arb_pending_hedge_qty: None,
                    arb_due_hedge_qty: None,
                    arb_hedge_time_ms: None,
                    arb_hedge_is_taker: None,
                    arb_hedge_ret_qtl: None,
                    arb_hedge_offset: None,
                    net_qty: None,
                    net_usdt: Some(exposure_sum_usdt),
                    is_total: true,
                });
            }

            let entry = PreTradeExposureResampleEntry { ts_ms, rows };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        // 发布风险数据
        if let Some(publisher) = self.risk_pub.as_ref() {
            let open_scope = mon.account_scope_for_venue(mon.open_venue());
            let hedge_scope = mon.account_scope_for_venue(mon.hedge_venue());
            let open_leg = compute_leg_risk_entry(&mon, mon.open_venue(), true, &price_snapshot);
            let hedge_leg = compute_leg_risk_entry(
                &mon,
                mon.hedge_venue(),
                open_scope != hedge_scope,
                &price_snapshot,
            );
            let borrowed_usd = open_leg.borrowed_usd + hedge_leg.borrowed_usd;
            let interest_usd = open_leg.interest_usd + hedge_leg.interest_usd;
            let max_leverage = PreTradeParamsLoader::instance().max_leverage();
            // total_equity 口径：若涉及合约 venue，已包含 UPL。
            let leverage = if total_equity.abs() <= f64::EPSILON {
                0.0
            } else {
                total_position / total_equity
            };
            // 这里显式保留“纯现货权益”字段，便于与 total_equity(eq) 对照排查。
            let spot_equity_usd = total_equity - um_unrealized_usd;

            let entry = PreTradeRiskResampleEntry {
                ts_ms,
                total_equity,
                total_exposure: total_abs_exposure,
                total_position,
                spot_equity_usd,
                borrowed_usd,
                interest_usd,
                um_unrealized_usd,
                leverage,
                max_leverage,
                open_leg,
                hedge_leg,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        Ok(published)
    }

    /// 发布编码后的数据
    #[allow(dead_code)]
    fn publish_encoded(bytes: Vec<u8>, publisher: &ResamplePublisher) -> Result<bool> {
        if bytes.is_empty() {
            return Ok(false);
        }
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        let len = bytes.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&bytes);
        if buf.len() > RESAMPLE_PAYLOAD {
            warn!(
                "pre_trade重采样载荷过大 ({} 字节，阈值 {} 字节)，已跳过",
                buf.len(),
                RESAMPLE_PAYLOAD
            );
            return Ok(false);
        }
        publisher.publish(&buf)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{arb_hedge_snapshot_asset_key, hedge_snapshot_symbol_key};

    #[test]
    fn hedge_snapshot_symbol_key_normalizes_internal_and_price_symbols() {
        assert_eq!(hedge_snapshot_symbol_key("SOLUSDT"), "SOLUSDT");
        assert_eq!(hedge_snapshot_symbol_key("SOL_USDT"), "SOLUSDT");
        assert_eq!(hedge_snapshot_symbol_key("SOL-USDT-SWAP"), "SOLUSDT");
    }

    #[test]
    fn arb_hedge_snapshot_asset_key_extracts_base_asset() {
        assert_eq!(arb_hedge_snapshot_asset_key("SOLUSDT"), "SOL");
        assert_eq!(arb_hedge_snapshot_asset_key("SOL_USDT"), "SOL");
        assert_eq!(arb_hedge_snapshot_asset_key("SOL-USDT-SWAP"), "SOL");
    }
}
