//! Native Hanlin YDApi (not the vendor's CTP-compatible adapter).
pub mod config;
#[cfg(feature = "yida-sdk")]
pub mod runtime;
#[cfg(feature = "yida-sdk")]
pub mod sdk;
pub mod session;

use crate::model::*;
use anyhow::{bail, ensure, Result};
use serde::Serialize;

pub const CONNECTED: i32 = 1;
pub const DISCONNECTED: i32 = 2;
pub const READY_LOGIN: i32 = 3;
pub const LOGIN: i32 = 4;
pub const INIT: i32 = 5;
pub const BEGIN: i32 = 6;
pub const INSTRUMENT: i32 = 7;
pub const ACCOUNT: i32 = 8;
pub const POSITION: i32 = 9;
pub const ORDER: i32 = 10;
pub const TRADE: i32 = 11;
pub const END: i32 = 12;
pub const FAILED_ORDER: i32 = 13;
pub const FAILED_CANCEL: i32 = 14;
pub const INVALIDATED: i32 = 15;
pub const SEGMENT: i32 = 16;
pub const FATAL: i32 = 17;

pub fn scope() -> ExecutionScope {
    ExecutionScope {
        venues: [Venue::Shfe, Venue::Ine].into_iter().collect(),
        reference_limit: i32::MAX as u64,
        require_market_status: false,
        cancel_requires_system_id: true,
    }
}
pub fn session_key(day: i32) -> String {
    format!("yida:{day}")
}

#[repr(C)]
#[derive(Clone, Copy, Serialize)]
pub struct RawEvent {
    pub kind: i32,
    pub error: i32,
    pub day: i32,
    pub reference: i32,
    pub system_id: i32,
    pub trade_id: i32,
    pub direction: i32,
    pub offset: i32,
    pub hedge: i32,
    pub status: i32,
    pub lots: i32,
    pub traded: i32,
    pub date: i32,
    pub position_direction: i32,
    pub frozen: i32,
    pub minimum: i32,
    pub maximum: i32,
    pub replay: i32,
    pub product_class: i32,
    pub expiry: i32,
    pub right: i32,
    pub login_count: i32,
    pub flag: i32,
    pub native_time: i32,
    pub price: f64,
    pub tick: f64,
    pub available: f64,
    pub commission: f64,
    pub symbol: [u8; 32],
    pub exchange: [u8; 12],
    #[serde(serialize_with = "serialize_bytes")]
    pub original_order_id: [u8; 64],
    #[serde(serialize_with = "serialize_bytes")]
    pub original_trade_id: [u8; 64],
}
fn serialize_bytes<S: serde::Serializer>(
    bytes: &[u8; 64],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_bytes(bytes)
}
impl Default for RawEvent {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}
pub fn text(bytes: &[u8]) -> Result<String> {
    crate::ctp::text(bytes)
}
impl RawEvent {
    pub fn venue(&self) -> Result<Venue> {
        Venue::parse(&text(&self.exchange)?)
    }
    pub fn side(&self) -> Result<Side> {
        match self.direction {
            0 => Ok(Side::Buy),
            1 => Ok(Side::Sell),
            v => bail!("unsupported Yida direction {v}"),
        }
    }
    pub fn offset(&self) -> Result<Offset> {
        match self.offset {
            0 => Ok(Offset::Open),
            3 => Ok(Offset::CloseToday),
            4 => Ok(Offset::CloseYesterday),
            v => bail!("unsupported Yida SHFE/INE offset {v}; raw evidence retained"),
        }
    }
    pub fn recovered_key(&self) -> Result<OrderKey> {
        ensure!(
            self.system_id > 0,
            "recovered order without system identity"
        );
        Ok(OrderKey {
            session: format!(
                "{}:{}:sys:{}",
                session_key(self.day),
                self.venue()?.as_str(),
                self.system_id
            ),
            order_ref: self.reference.to_string(),
        })
    }
    pub fn to_order(&self, key: OrderKey) -> Result<OrderUpdate> {
        ensure!(
            self.hedge == 1 && self.flag == 0 && self.product_class == 1,
            "unsupported Yida hedge/order flag/product class"
        );
        let status = match self.status {
            0 => OrderStatus::Pending,
            1 => OrderStatus::Working,
            2 => OrderStatus::Cancelled,
            3 => OrderStatus::Filled,
            4 => OrderStatus::Rejected,
            v => bail!("unsupported Yida status {v}"),
        };
        ensure!(
            self.system_id > 0
                || (matches!(status, OrderStatus::Pending | OrderStatus::Rejected)
                    && self.reference > 0
                    && key.session == session_key(self.day)),
            "order without native system identity or known local pending identity"
        );
        Ok(OrderUpdate {
            key,
            symbol: text(&self.symbol)?,
            venue: self.venue()?,
            system_id: if self.system_id > 0 {
                self.system_id.to_string()
            } else {
                String::new()
            },
            side: self.side()?,
            offset: self.offset()?,
            lots: self.lots,
            traded: self.traded,
            limit_price: self.price,
            status,
            cancel_rejected: false,
            trading_day: self.day.to_string(),
        })
    }
    pub fn to_fill(&self) -> Result<Fill> {
        ensure!(
            self.hedge == 1
                && self.product_class == 1
                && self.trade_id > 0
                && self.system_id > 0
                && self.commission.is_finite(),
            "unsupported/malformed native fill; retain evidence"
        );
        Ok(Fill {
            identity: TradeIdentity {
                venue: self.venue()?,
                symbol: text(&self.symbol)?,
                trading_day: self.day.to_string(),
                trade_id: self.trade_id.to_string(),
                side: self.side()?,
            },
            order_ref: self.reference.to_string(),
            system_id: self.system_id.to_string(),
            offset: self.offset()?,
            lots: self.lots,
            price: self.price,
        })
    }
}

#[cfg(test)]
mod tests;
