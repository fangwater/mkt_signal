//! Official CTP 6.7.11 Trader API mapping; no CTP-compatible replacement libraries.
#[cfg(feature = "ctp-sdk")]
pub mod sdk;
pub mod session;

use crate::model::*;
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub const CONNECTED: i32 = 1;
pub const DISCONNECTED: i32 = 2;
pub const AUTHENTICATED: i32 = 3;
pub const LOGGED_IN: i32 = 4;
pub const SETTLED: i32 = 5;
pub const INSTRUMENT: i32 = 6;
pub const ACCOUNT: i32 = 7;
pub const POSITION: i32 = 8;
pub const ORDER: i32 = 9;
pub const TRADE: i32 = 10;
pub const INSERT_RESPONSE: i32 = 11;
pub const CANCEL_RESPONSE: i32 = 12;
pub const QUERY_ORDER: i32 = 13;
pub const QUERY_TRADE: i32 = 14;
pub const INSTRUMENT_STATUS: i32 = 15;
pub const ERROR: i32 = 16;

/// Correlate business responses across reconnects. OrderRef alone is not a session identity.
#[derive(Default)]
pub struct ResponseCorrelation {
    requests: HashMap<i32, (i32, OrderKey)>,
}
impl ResponseCorrelation {
    pub fn register(&mut self, id: i32, kind: i32, key: OrderKey) -> Result<()> {
        ensure!(
            self.requests.len() < 200_000,
            "CTP response correlation capacity exhausted"
        );
        ensure!(
            !self.requests.contains_key(&id),
            "duplicate native request ID"
        );
        self.requests.insert(id, (kind, key));
        Ok(())
    }
    pub fn resolve(&self, e: &RawEvent) -> Result<OrderKey> {
        if e.request > 0 {
            let (kind, key) = self
                .requests
                .get(&e.request)
                .context("unknown CTP business response request ID")?;
            ensure!(*kind == e.kind, "CTP business response kind mismatch");
            return Ok(key.clone());
        }
        let order_ref = text(&e.order_ref)?;
        let mut keys = self
            .requests
            .values()
            .filter(|(kind, key)| *kind == e.kind && key.order_ref == order_ref)
            .map(|(_, key)| key);
        let key = keys
            .next()
            .context("unmatched asynchronous CTP rejection")?;
        ensure!(
            keys.all(|next| next == key),
            "OrderRef is ambiguous across sessions"
        );
        Ok(key.clone())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawEvent {
    pub kind: i32,
    pub request: i32,
    pub error: i32,
    pub last: i32,
    pub present: i32,
    pub front: i32,
    pub session: i32,
    pub volume: i32,
    pub traded: i32,
    pub remaining: i32,
    pub today: i32,
    pub yesterday: i32,
    pub status: i32,
    pub submit_status: i32,
    pub direction: i32,
    pub offset: i32,
    pub hedge: i32,
    pub position_direction: i32,
    pub position_date: i32,
    pub active: i32,
    pub product_class: i32,
    pub minimum: i32,
    pub maximum: i32,
    pub price: f64,
    pub price_tick: f64,
    pub available: f64,
    pub symbol: [u8; 81],
    pub exchange: [u8; 9],
    pub order_ref: [u8; 13],
    pub system_id: [u8; 21],
    pub trade_id: [u8; 21],
    pub trading_day: [u8; 9],
    pub investor: [u8; 13],
    pub max_order_ref: [u8; 13],
    pub message: [u8; 256],
}
impl Default for RawEvent {
    fn default() -> Self {
        // All fields are plain numeric/byte values.
        unsafe { std::mem::zeroed() }
    }
}

pub fn text(bytes: &[u8]) -> Result<String> {
    let n = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| anyhow::anyhow!("unterminated CTP text field"))?;
    ensure!(bytes[..n].is_ascii(), "non-ASCII CTP identifier");
    Ok(std::str::from_utf8(&bytes[..n])?.to_owned())
}
pub fn error_text(bytes: &[u8]) -> String {
    let n = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    encoding_rs::GBK.decode(&bytes[..n]).0.into_owned()
}
fn side(raw: i32) -> Result<Side> {
    match raw {
        48 => Ok(Side::Buy),
        49 => Ok(Side::Sell),
        _ => bail!("unsupported CTP direction code {raw}"),
    }
}
fn offset(raw: i32) -> Result<Offset> {
    match raw {
        48 => Ok(Offset::Open),
        49 | 50 | 53 | 54 => Ok(Offset::Close),
        _ => bail!("unsupported CFFEX offset code {raw}; retain raw evidence and reconcile"),
    }
}
impl RawEvent {
    pub fn key(&self) -> Result<OrderKey> {
        Ok(OrderKey {
            session: session_key(self.front, self.session),
            order_ref: text(&self.order_ref)?,
        })
    }
    pub fn to_order(&self) -> Result<OrderUpdate> {
        ensure!(
            self.present != 0 && self.hedge == i32::from(b'1'),
            "unsupported/missing order or hedge flag"
        );
        let status = if self.submit_status == i32::from(b'4') {
            OrderStatus::Rejected
        } else {
            match self.status {
                48 => OrderStatus::Filled,
                49 | 51 => OrderStatus::Working,
                50 | 52 | 53 => OrderStatus::Cancelled,
                97 => OrderStatus::Pending,
                other => bail!("unsupported CTP order status {other}"),
            }
        };
        Ok(OrderUpdate {
            key: self.key()?,
            symbol: text(&self.symbol)?,
            venue: Venue::parse(&text(&self.exchange)?)?,
            system_id: text(&self.system_id)?,
            side: side(self.direction)?,
            offset: offset(self.offset)?,
            lots: self.volume,
            traded: self.traded,
            price_type: PriceType::Limit,
            limit_price: self.price,
            status,
            cancel_rejected: self.submit_status == i32::from(b'5'),
            trading_day: text(&self.trading_day)?,
        })
    }
    pub fn to_fill(&self) -> Result<Fill> {
        ensure!(
            self.present != 0 && self.hedge == i32::from(b'1'),
            "unsupported/missing fill or hedge flag"
        );
        Ok(Fill {
            identity: TradeIdentity {
                venue: Venue::parse(&text(&self.exchange)?)?,
                symbol: text(&self.symbol)?,
                trading_day: text(&self.trading_day)?,
                trade_id: text(&self.trade_id)?,
                side: side(self.direction)?,
            },
            order_ref: text(&self.order_ref)?,
            order_key: None,
            system_id: text(&self.system_id)?,
            offset: offset(self.offset)?,
            lots: self.volume,
            price: self.price,
        })
    }
    /// The CTP error message may contain GBK. Credentials are removed by the runtime writer.
    pub fn audit(&self, received_us: u64) -> Value {
        let lossy = |b: &[u8]| {
            String::from_utf8_lossy(&b[..b.iter().position(|v| *v == 0).unwrap_or(b.len())])
                .into_owned()
        };
        json!({"received_us":received_us,"kind":self.kind,"request_id":self.request,"error_code":self.error,"last":self.last,"present":self.present,
            "front_id":self.front,"session_id":self.session,"symbol":lossy(&self.symbol),"exchange":lossy(&self.exchange),"order_ref":lossy(&self.order_ref),
            "system_id":lossy(&self.system_id),"trade_id":lossy(&self.trade_id),"trading_day":lossy(&self.trading_day),"volume":self.volume,"traded":self.traded,
            "remaining":self.remaining,"today":self.today,"yesterday":self.yesterday,"direction":self.direction,"offset":self.offset,"hedge":self.hedge,
            "position_direction":self.position_direction,"position_date":self.position_date,"status":self.status,"submit_status":self.submit_status,
            "price":self.price,"price_tick":self.price_tick,"available":self.available,"active":self.active,"product_class":self.product_class,
            "minimum":self.minimum,"maximum":self.maximum,"message":error_text(&self.message)})
    }
}

pub fn scope() -> ExecutionScope {
    ExecutionScope {
        venues: [Venue::Cffex].into_iter().collect(),
        reference_limit: 999_999_999_999,
        require_market_status: true,
        cancel_requires_system_id: false,
        allow_implicit_fill_reference: true,
    }
}
pub fn session_key(front: i32, session: i32) -> String {
    format!("ctp:{front}:{session}")
}
pub fn parse_session(value: &str) -> Result<(i32, i32)> {
    let parts: Vec<_> = value.split(':').collect();
    ensure!(parts.len() == 3 && parts[0] == "ctp", "invalid CTP session");
    Ok((parts[1].parse()?, parts[2].parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifiers_must_be_terminated_and_ascii() {
        assert!(text(&[b'x'; 13]).is_err());
        assert!(text(&[255, 0]).is_err());
        assert_eq!(text(b"123\0").unwrap(), "123");
    }
    #[test]
    fn gbk_error_is_decoded() {
        assert_eq!(error_text(&[0xb4, 0xed, 0xce, 0xf3, 0]), "错误");
    }
    #[test]
    fn forced_close_is_a_close_fact_not_an_unknown_id_filter() {
        assert_eq!(offset(i32::from(b'2')).unwrap(), Offset::Close);
    }
    #[test]
    fn response_ids_keep_the_original_session_even_after_reconnect() {
        let old = OrderKey {
            session: session_key(1, 2),
            order_ref: "17".into(),
        };
        let new = OrderKey {
            session: session_key(1, 3),
            order_ref: "17".into(),
        };
        let mut correlation = ResponseCorrelation::default();
        correlation
            .register(100, INSERT_RESPONSE, old.clone())
            .unwrap();
        correlation.register(101, INSERT_RESPONSE, new).unwrap();
        let mut event = RawEvent {
            kind: INSERT_RESPONSE,
            request: 100,
            ..RawEvent::default()
        };
        assert_eq!(correlation.resolve(&event).unwrap(), old);
        event.request = 0;
        event.order_ref[..2].copy_from_slice(b"17");
        assert!(correlation.resolve(&event).is_err());
    }
}
