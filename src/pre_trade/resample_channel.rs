use crate::common::iceoryx_publisher::ResamplePublisher;
use anyhow::Result;
use log::{info, warn};
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
}
