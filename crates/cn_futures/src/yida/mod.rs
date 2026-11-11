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
pub const FAILED_CANCEL: i32 = 13;
pub const INVALIDATED: i32 = 14;
pub const SEGMENT: i32 = 15;
pub const FATAL: i32 = 16;
pub const TRANSPORT: i32 = 17;
pub const CONNECTION: i32 = 18;
pub const SDK_REFERENCE: &str = "1.502.53.182";

pub fn scope() -> ExecutionScope {
    ExecutionScope {
        venues: [Venue::Shfe, Venue::Ine].into_iter().collect(),
        reference_limit: i32::MAX as u64,
        require_market_status: false,
        cancel_requires_system_id: false,
        allow_implicit_fill_reference: false,
        allow_options: true,
    }
}
pub fn session_key(day: i32) -> String {
    format!("yida:{day}")
}
pub fn group_session_key(day: i32, group: i32) -> String {
    if group == 0 {
        session_key(day)
    } else {
        format!("yida:{day}:group:{group}")
    }
}
pub fn parse_group_key(key: &OrderKey) -> Result<(i32, i32, i32)> {
    let mut parts = key.session.split(':');
    ensure!(
        parts.next() == Some("yida"),
        "invalid native group identity"
    );
    let day = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing native trading day"))?
        .parse::<i32>()?;
    ensure!(
        parts.next() == Some("group"),
        "order has no native group identity"
    );
    let group = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing native order group"))?
        .parse::<i32>()?;
    ensure!(parts.next().is_none(), "invalid native group identity");
    let reference = key.order_ref.parse::<i32>()?;
    ensure!(
        day.to_string().len() == 8 && (1..=255).contains(&group) && reference > 0,
        "invalid native group identity"
    );
    Ok((day, group, reference))
}

#[repr(C)]
#[derive(Clone, Copy, Serialize)]
pub struct RawEvent {
    pub kind: i32,
    pub error: i32,
    pub day: i32,
    pub reference: i32,
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
    pub order_group: i32,
    pub trade_flag: i32,
    pub no_order: i32,
    pub legacy_system_id: i32,
    pub legacy_trade_id: i32,
    pub order_type: i32,
    pub order_attribute: i32,
    pub market_minimum: i32,
    pub market_maximum: i32,
    pub selection_type: i32,
    pub connection_id: i32,
    pub real_connection_id: i32,
    pub connection_status: i32,
    pub connection_count: i32,
    pub public_connection: i32,
    pub dedicated_connection: i32,
    pub options_type: i32,
    pub contract_multiple: i32,
    pub system_id: i64,
    pub trade_id: i64,
    pub price: f64,
    pub tick: f64,
    pub available: f64,
    pub commission: f64,
    pub strike_price: f64,
    pub upper_limit_price: f64,
    pub symbol: [u8; 32],
    pub exchange: [u8; 12],
    #[serde(serialize_with = "serialize_bytes")]
    pub original_order_id: [u8; 64],
    #[serde(serialize_with = "serialize_bytes")]
    pub original_trade_id: [u8; 64],
    pub connection_info: [u8; 28],
    pub insert_flow: [u8; 32],
    pub cancel_flow: [u8; 32],
    pub underlying: [u8; 32],
    pub underlying_exchange: [u8; 12],
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
    pub fn option_contract(&self, cash_buffer: f64) -> Result<Option<OptionContract>> {
        let venue = self.venue()?;
        let symbol = crate::ctp::text_ref(&self.symbol)?;
        ensure!(
            matches!(venue, Venue::Shfe | Venue::Ine),
            "native contract outside SHFE/INE scope"
        );
        if self.product_class == 1 {
            ensure!(
                is_future_symbol(venue, symbol) && self.options_type == 0,
                "native future class/symbol mismatch"
            );
            return Ok(None);
        }
        ensure!(self.product_class == 2, "unsupported native product class");
        ensure!(
            text(&self.underlying_exchange)? == venue.as_str(),
            "option underlying venue mismatch"
        );
        let option = OptionContract {
            right: match self.options_type {
                1 => OptionRight::Call,
                2 => OptionRight::Put,
                _ => bail!("unsupported native option right"),
            },
            underlying: text(&self.underlying)?,
            strike_price: self.strike_price,
            contract_multiple: self.contract_multiple,
            expiry_date: self.expiry,
            upper_limit_price: self.upper_limit_price,
            cash_buffer_per_buy_lot: cash_buffer,
        };
        option.validate(venue, symbol)?;
        Ok(Some(option))
    }
    pub fn venue(&self) -> Result<Venue> {
        Venue::parse(crate::ctp::text_ref(&self.exchange)?)
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
            (0..=255).contains(&self.order_group),
            "invalid native order group"
        );
        if self.order_group != 0 {
            ensure!(
                self.reference > 0,
                "nonzero group requires positive reference"
            );
            return Ok(OrderKey {
                session: group_session_key(self.day, self.order_group),
                order_ref: self.reference.to_string(),
            });
        }
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
        self.option_contract(0.0)?;
        ensure!(
            self.kind == ORDER
                && self.hedge == 1
                && self.flag == 0
                && (0..=255).contains(&self.order_group),
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
            matches!(self.order_type, 0 | 1 | 3),
            "unsupported native SHFE/INE order type"
        );
        let price_type = match self.order_attribute {
            0 => PriceType::Limit,
            1 => PriceType::Market,
            _ => bail!("unsupported native SHFE/INE order attributes"),
        };
        ensure!(
            self.lots > 0
                && self.traded >= 0
                && self.traded <= self.lots
                && self.price.is_finite()
                && self.price >= 0.0
                && (price_type == PriceType::Market
                    || status == OrderStatus::Rejected
                    || self.price > 0.0)
                && (self.error == 0 || status == OrderStatus::Rejected)
                && (status != OrderStatus::Filled || self.traded == self.lots)
                && (status != OrderStatus::Rejected || self.traded == 0),
            "inconsistent native order status/error/quantity/price; evidence retained"
        );
        let venue = self.venue()?;
        let symbol = text(&self.symbol)?;
        ensure!(
            matches!(venue, Venue::Shfe | Venue::Ine)
                && scope().supports_symbol(venue, &symbol)
                && self.day.to_string().len() == 8,
            "native order outside supported futures/day scope"
        );
        ensure!(
            self.system_id > 0
                || (matches!(status, OrderStatus::Pending | OrderStatus::Rejected)
                    && self.reference > 0
                    && key.session == group_session_key(self.day, self.order_group)),
            "order without native system identity or known local pending identity"
        );
        Ok(OrderUpdate {
            key,
            symbol,
            venue,
            system_id: if self.system_id > 0 {
                self.system_id.to_string()
            } else {
                String::new()
            },
            side: self.side()?,
            offset: self.offset()?,
            lots: self.lots,
            traded: self.traded,
            price_type,
            time_in_force: match self.order_type {
                0 => TimeInForce::Day,
                1 => TimeInForce::Fak,
                3 => TimeInForce::Fok,
                _ => unreachable!("validated native order type"),
            },
            limit_price: self.price,
            status,
            cancel_rejected: false,
            trading_day: self.day.to_string(),
        })
    }
    pub fn to_fill(&self) -> Result<Fill> {
        self.option_contract(0.0)?;
        ensure!(
            self.kind == TRADE
                && self.hedge == 1
                && self.trade_id > 0
                && self.system_id > 0
                && (0..=255).contains(&self.order_group)
                && self.trade_flag == 0
                && self.no_order == 0
                && self.commission.is_finite()
                && self.lots > 0
                && self.price.is_finite()
                && self.price > 0.0,
            "unsupported/malformed native fill; retain evidence"
        );
        let venue = self.venue()?;
        let symbol = text(&self.symbol)?;
        ensure!(
            matches!(venue, Venue::Shfe | Venue::Ine)
                && scope().supports_symbol(venue, &symbol)
                && self.day.to_string().len() == 8,
            "native fill outside supported futures/day scope"
        );
        Ok(Fill {
            identity: TradeIdentity {
                venue,
                symbol,
                trading_day: self.day.to_string(),
                trade_id: self.trade_id.to_string(),
                side: self.side()?,
            },
            order_ref: self.reference.to_string(),
            order_key: if self.order_group != 0 {
                Some(self.recovered_key()?)
            } else {
                None
            },
            system_id: self.system_id.to_string(),
            offset: self.offset()?,
            lots: self.lots,
            price: self.price,
        })
    }
}

#[cfg(test)]
mod tests;

pub fn decode_event(e: &RawEvent, historical_rejection: bool) -> Result<DecodedUpdate> {
    Ok(match e.kind {
        ORDER => {
            let key = if historical_rejection {
                OrderKey {
                    session: group_session_key(e.day, e.order_group),
                    order_ref: e.reference.to_string(),
                }
            } else {
                e.recovered_key()?
            };
            DecodedUpdate::Order(e.to_order(key)?)
        }
        TRADE => DecodedUpdate::Trade(e.to_fill()?),
        _ => DecodedUpdate::None,
    })
}
