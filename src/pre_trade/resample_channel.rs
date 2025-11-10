use crate::common::iceoryx_publisher::ResamplePublisher;
use log::warn;

/// 前端展示采样频道 - 负责发布持仓、风险、敞口等采样数据
pub struct ResampleChannel {
    positions_pub: Option<ResamplePublisher>,
    exposure_pub: Option<ResamplePublisher>,
    risk_pub: Option<ResamplePublisher>,
}

impl ResampleChannel {
    /// 创建 ResampleChannel，尝试初始化所有 publisher
    /// 如果某个 publisher 创建失败，会记录警告但不会导致整体失败
    pub fn new(
        positions_channel: &str,
        exposure_channel: &str,
        risk_channel: &str,
    ) -> Self {
        let make_pub = |channel: &str| match ResamplePublisher::new(channel) {
            Ok(p) => Some(p),
            Err(err) => {
                warn!(
                    "failed to create pre_trade resample publisher on {}: {err:#}",
                    channel
                );
                None
            }
        };

        Self {
            positions_pub: make_pub(positions_channel),
            exposure_pub: make_pub(exposure_channel),
            risk_pub: make_pub(risk_channel),
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
}
