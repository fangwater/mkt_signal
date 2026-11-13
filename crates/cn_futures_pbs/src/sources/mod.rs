#[cfg(feature = "ctp-sdk")]
pub mod ctp;
pub mod zmq;

/// Only ingress differs. All downstream code consumes MarketDataMsg.
pub trait Source {
    fn poll(&mut self) -> anyhow::Result<Option<crate::msg::MarketDataMsg>>;
}
