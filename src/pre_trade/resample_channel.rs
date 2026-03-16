use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::{ResamplePublisher, RESAMPLE_PAYLOAD};
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::basic_balance_manager::BasicBalanceManager;
use crate::pre_trade::basic_exposure_manager::BasicExposureManager;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::usdt_balance_manager::UsdtBalanceSnapshot;
use crate::signal::common::TradingVenue;
use crate::viz::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradeRiskResampleEntry,
};
use anyhow::Result;
use log::{debug, info, trace, warn};
use std::cell::OnceCell;
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

fn usdt_net_position(exchange: Exchange, s: &UsdtBalanceSnapshot) -> f64 {
    match exchange {
        Exchange::Okex => s.balance,
        Exchange::Binance | Exchange::Gate | Exchange::Hyperliquid => {
            s.balance - s.borrowed - s.cumulative_interest
        }
        _ => s.balance,
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
    let open_net = usdt_net_position(open_ex, &open_snap);
    let hedge_net = usdt_net_position(hedge_ex, &hedge_snap);

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
    static RESAMPLE_CHANNEL: OnceCell<ResampleChannel> = OnceCell::new();
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
        let ts_ms = (get_timestamp_us() / 1000) as i64;
        let (exposures, _total_equity, total_abs_exposure, _total_position, _um_unrealized_usd) =
            mon.basic_state_snapshot();
        let price_mapper = create_symbol_mapper(Exchange::Binance);

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
        let ts_ms = (get_timestamp_us() / 1000) as i64;
        let exposure_price_mapper = create_symbol_mapper(Exchange::Binance);

        let (exposures, total_equity, total_abs_exposure, total_position, um_unrealized_usd) =
            mon.basic_state_snapshot();

        let mut published = 0usize;

        // 发布敞口数据
        if let Some(publisher) = self.exposure_pub.as_ref() {
            let mut rows: Vec<PreTradeExposureRow> = Vec::new();
            let mut exposure_sum_usdt = 0.0_f64;

            let mut exposure_items: Vec<(String, f64, f64)> = exposures
                .iter()
                .map(|(asset, &(open_qty, hedge_qty))| (asset.to_uppercase(), open_qty, hedge_qty))
                .collect();
            exposure_items.sort_by(|a, b| a.0.cmp(&b.0));

            for (asset_upper, open_qty, hedge_qty) in exposure_items {
                if asset_upper == "USDT" {
                    continue;
                }
                if open_qty.abs() <= 1e-12 && hedge_qty.abs() <= 1e-12 {
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
                let net_qty = open_qty + hedge_qty;
                let net_usdt = open_usdt + hedge_usdt;
                exposure_sum_usdt += net_usdt;

                rows.push(PreTradeExposureRow {
                    asset: asset_upper.clone(),
                    open_qty: Some(open_qty),
                    open_usdt: Some(open_usdt),
                    hedge_qty: Some(hedge_qty),
                    hedge_usdt: Some(hedge_usdt),
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
            // borrowed/interest 只能从 basic balance stream 得到，按现货资产估值
            let mut borrowed_usd = 0.0_f64;
            let mut interest_usd = 0.0_f64;
            for bal_mgr in [mon.open_balance_mgr(), mon.hedge_balance_mgr()]
                .into_iter()
                .flatten()
            {
                let mgr = bal_mgr.borrow();
                let mgr_ref: &BasicBalanceManager = &*mgr;
                let exchange = mgr.exchange();
                let price_mapper = create_symbol_mapper(exchange);
                let entries = BasicExposureManager::compute_exposures_for_exchange(
                    exchange,
                    std::slice::from_ref(&mgr_ref),
                    &[],
                );
                for entry in entries {
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
            }
            // USDT 借贷/利息单独维护：USDT 本身按 1:1 计价
            for (scope, s) in mon.usdt_snapshot_all() {
                let _ = scope; // 仅用于日志与排查，此处汇总不区分账户 scope
                borrowed_usd += s.borrowed;
                interest_usd += s.cumulative_interest;
            }

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
