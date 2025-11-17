use crate::common::iceoryx_publisher::{ResamplePublisher, RESAMPLE_PAYLOAD};
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::signal::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradePositionResampleEntry,
    PreTradeRiskResampleEntry, PreTradeSpotBalanceRow, PreTradeUmPositionRow,
};
use anyhow::Result;
use log::{debug, info, warn};
use std::cell::OnceCell;

thread_local! {
    static RESAMPLE_CHANNEL: OnceCell<ResampleChannel> = OnceCell::new();
}

/// 默认持仓采样频道名称
pub const DEFAULT_POSITIONS_CHANNEL: &str = "pre_trade_positions";

/// 默认敞口采样频道名称
pub const DEFAULT_EXPOSURE_CHANNEL: &str = "pre_trade_exposure";

/// 默认风险采样频道名称
pub const DEFAULT_RISK_CHANNEL: &str = "pre_trade_risk";

/// 前端展示采样频道 - 负责发布持仓、风险、敞口等采样数据
///
/// 采用线程本地单例模式，通过 `ResampleChannel::with()` 访问
///
/// # 使用示例
/// ```ignore
/// use crate::pre_trade::ResampleChannel;
///
/// // 方式1: 使用默认配置（自动初始化）
/// ResampleChannel::with(|ch| {
///     if let Some(pub) = ch.positions_pub() {
///         pub.publish(&data)?;
///     }
/// });
///
/// // 方式2: 显式初始化自定义频道
/// ResampleChannel::initialize("custom_pos", "custom_exp", "custom_risk")?;
/// ```
pub struct ResampleChannel {
    positions_pub: Option<ResamplePublisher>,
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
    /// // 发布持仓数据
    /// ResampleChannel::with(|ch| {
    ///     if let Some(pub) = ch.positions_pub() {
    ///         pub.publish(&data)?;
    ///     }
    /// });
    ///
    /// // 发布敞口数据
    /// ResampleChannel::with(|ch| {
    ///     if let Some(pub) = ch.exposure_pub() {
    ///         pub.publish(&data)?;
    ///     }
    /// });
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&ResampleChannel) -> R,
    {
        RESAMPLE_CHANNEL.with(|cell| {
            let channel = cell.get_or_init(|| {
                info!("Initializing thread-local ResampleChannel singleton with default config");
                ResampleChannel::new(
                    DEFAULT_POSITIONS_CHANNEL,
                    DEFAULT_EXPOSURE_CHANNEL,
                    DEFAULT_RISK_CHANNEL,
                )
            });
            f(channel)
        })
    }

    /// 显式初始化采样频道（可选）
    ///
    /// 如果在首次调用 `with()` 之前调用此方法，可以自定义频道名称
    ///
    /// # 参数
    /// * `positions_channel` - 持仓数据频道名称
    /// * `exposure_channel` - 敞口数据频道名称
    /// * `risk_channel` - 风险数据频道名称
    ///
    /// # 错误
    /// 如果已经初始化，返回错误
    pub fn initialize(
        positions_channel: &str,
        exposure_channel: &str,
        risk_channel: &str,
    ) -> Result<()> {
        RESAMPLE_CHANNEL.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow::anyhow!("ResampleChannel already initialized"));
            }
            cell.set(ResampleChannel::new(
                positions_channel,
                exposure_channel,
                risk_channel,
            ))
            .map_err(|_| anyhow::anyhow!("Failed to set ResampleChannel (race condition)"))
        })
    }

    /// 创建 ResampleChannel，尝试初始化所有 publisher
    ///
    /// 如果某个 publisher 创建失败，会记录警告但不会导致整体失败
    ///
    /// 注意：通常应使用 `ResampleChannel::with()` 访问线程本地单例，
    /// 而不是直接调用 `new()` 创建多个实例
    fn new(positions_channel: &str, exposure_channel: &str, risk_channel: &str) -> Self {
        let make_pub = |channel: &str, desc: &str| match ResamplePublisher::new(channel) {
            Ok(p) => {
                info!(
                    "ResampleChannel: {} publisher created on '{}'",
                    desc, channel
                );
                Some(p)
            }
            Err(err) => {
                warn!(
                    "ResampleChannel: failed to create {} publisher on '{}': {err:#}",
                    desc, channel
                );
                None
            }
        };

        Self {
            positions_pub: make_pub(positions_channel, "positions"),
            exposure_pub: make_pub(exposure_channel, "exposure"),
            risk_pub: make_pub(risk_channel, "risk"),
        }
    }

    /// 获取持仓数据 publisher 的引用
    pub fn positions_pub(&self) -> Option<&ResamplePublisher> {
        self.positions_pub.as_ref()
    }

    /// 获取敞口数据 publisher 的引用
    pub fn exposure_pub(&self) -> Option<&ResamplePublisher> {
        self.exposure_pub.as_ref()
    }

    /// 获取风险数据 publisher 的引用
    pub fn risk_pub(&self) -> Option<&ResamplePublisher> {
        self.risk_pub.as_ref()
    }

    /// 检查持仓 publisher 是否可用
    pub fn is_positions_publisher_available(&self) -> bool {
        self.positions_pub.is_some()
    }

    /// 检查敞口 publisher 是否可用
    pub fn is_exposure_publisher_available(&self) -> bool {
        self.exposure_pub.is_some()
    }

    /// 检查风险 publisher 是否可用
    pub fn is_risk_publisher_available(&self) -> bool {
        self.risk_pub.is_some()
    }

    /// 发布重采样条目（持仓、敞口、风险）
    ///
    /// 通过 MonitorChannel::instance() 访问所需的管理器数据
    /// 返回成功发布的条目数量
    pub fn publish_resample_entries(&self) -> Result<usize> {
        if self.positions_pub.is_none() && self.exposure_pub.is_none() && self.risk_pub.is_none() {
            return Ok(0);
        }

        // 通过 MonitorChannel 获取快照
        let mon_ch = MonitorChannel::instance();

        let Some(spot_snapshot) = mon_ch.spot_manager().borrow().snapshot() else {
            return Ok(0);
        };
        let Some(um_snapshot) = mon_ch.um_manager().borrow().snapshot() else {
            return Ok(0);
        };

        let price_snapshot = mon_ch.price_table().borrow().snapshot();
        let ts_ms = (get_timestamp_us() / 1000) as i64;

        // 重估敞口
        let binding = mon_ch.exposure_manager();
        let mut exposures_mgr = binding.borrow_mut();
        exposures_mgr.revalue_with_prices(&price_snapshot);
        exposures_mgr.log_summary("resample估值");
        let exposures_vec = exposures_mgr.exposures().to_vec();
        let total_equity = exposures_mgr.total_equity();
        let total_abs_exposure = exposures_mgr.total_abs_exposure();
        let total_position = exposures_mgr.total_position();
        let spot_equity_usd = exposures_mgr.total_spot_value_usd();
        let borrowed_usd = exposures_mgr.total_borrowed_usd();
        let interest_usd = exposures_mgr.total_interest_usd();
        let um_unrealized_usd = exposures_mgr.total_um_unrealized();
        drop(exposures_mgr);

        let max_leverage = PreTradeParamsLoader::instance().max_leverage();

        let mut published = 0usize;

        // 发布持仓数据
        if let Some(publisher) = self.positions_pub.as_ref() {
            let um_rows: Vec<PreTradeUmPositionRow> = um_snapshot
                .positions
                .iter()
                .map(|pos| PreTradeUmPositionRow {
                    symbol: pos.symbol.clone(),
                    side: pos.position_side.to_string(),
                    position_amount: pos.position_amt,
                    entry_price: pos.entry_price,
                    leverage: pos.leverage,
                    position_initial_margin: pos.position_initial_margin,
                    open_order_initial_margin: pos.open_order_initial_margin,
                    unrealized_profit: pos.unrealized_profit,
                })
                .collect();

            let spot_rows: Vec<PreTradeSpotBalanceRow> = spot_snapshot
                .balances
                .iter()
                .map(|bal| PreTradeSpotBalanceRow {
                    asset: bal.asset.clone(),
                    total_wallet: bal.total_wallet_balance,
                    cross_free: bal.cross_margin_free,
                    cross_locked: bal.cross_margin_locked,
                    cross_borrowed: bal.cross_margin_borrowed,
                    cross_interest: bal.cross_margin_interest,
                    um_wallet: bal.um_wallet_balance,
                    um_unrealized_pnl: bal.um_unrealized_pnl,
                })
                .collect();

            let entry = PreTradePositionResampleEntry {
                ts_ms,
                um_positions: um_rows,
                spot_balances: spot_rows,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        // 发布敞口数据
        if let Some(publisher) = self.exposure_pub.as_ref() {
            let mut rows: Vec<PreTradeExposureRow> = Vec::new();
            let mut exposure_sum_usdt = 0.0_f64;
            for entry in &exposures_vec {
                let asset_upper = entry.asset.to_uppercase();
                if asset_upper == "USDT" {
                    continue;
                }
                let symbol = format!("{}USDT", asset_upper);
                let mark = price_snapshot
                    .get(&symbol)
                    .map(|p| p.mark_price)
                    .unwrap_or(0.0);
                if mark == 0.0 && (entry.spot_total_wallet != 0.0 || entry.um_net_position != 0.0) {
                    debug!("missing mark price for {} when resampling exposure", symbol);
                }
                let spot_usdt = entry.spot_total_wallet * mark;
                let um_usdt = entry.um_net_position * mark;
                let exposure_usdt = spot_usdt + um_usdt;
                exposure_sum_usdt += exposure_usdt;
                rows.push(PreTradeExposureRow {
                    asset: entry.asset.clone(),
                    spot_qty: Some(entry.spot_total_wallet),
                    spot_usdt: Some(spot_usdt),
                    um_net_qty: Some(entry.um_net_position),
                    um_net_usdt: Some(um_usdt),
                    exposure_qty: Some(entry.exposure),
                    exposure_usdt: Some(exposure_usdt),
                    is_total: false,
                });
            }
            if !rows.is_empty() {
                rows.push(PreTradeExposureRow {
                    asset: "TOTAL".to_string(),
                    spot_qty: None,
                    spot_usdt: None,
                    um_net_qty: None,
                    um_net_usdt: None,
                    exposure_qty: None,
                    exposure_usdt: Some(exposure_sum_usdt),
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
            let leverage = if total_equity.abs() <= f64::EPSILON {
                0.0
            } else {
                total_position / total_equity
            };
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
