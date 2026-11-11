//! Owned, bounded audit buffers. They live directly in the preallocated queue;
//! no borrowed SDK memory or truncated identifiers cross a thread boundary.
use crate::model::*;
use anyhow::{ensure, Result};
#[derive(Clone, Copy)]
pub(crate) struct FixedText<const N: usize> {
    bytes: [u8; N],
    len: usize,
}
impl<const N: usize> FixedText<N> {
    pub fn new(value: &str) -> Result<Self> {
        ensure!(
            value.len() <= N,
            "native audit text exceeds its documented bound"
        );
        let mut bytes = [0; N];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(Self {
            bytes,
            len: value.len(),
        })
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len])
            .expect("FixedText copies complete Rust UTF-8 strings")
    }
}
#[derive(Clone, Copy)]
struct FixedKey {
    session: FixedText<128>,
    reference: FixedText<32>,
}
impl FixedKey {
    fn new(key: &OrderKey) -> Result<Self> {
        Ok(Self {
            session: FixedText::new(&key.session)?,
            reference: FixedText::new(&key.order_ref)?,
        })
    }
    fn owned(self) -> OrderKey {
        OrderKey {
            session: self.session.as_str().into(),
            order_ref: self.reference.as_str().into(),
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct SubmitAudit {
    key: FixedKey,
    symbol: FixedText<81>,
    day: FixedText<16>,
    venue: Venue,
    side: Side,
    offset: Offset,
    lots: i32,
    price_type: PriceType,
    price_ticks: i64,
    tif: TimeInForce,
    route: Option<ExecutionRoute>,
    price: f64,
    ceiling: f64,
    pub received_us: u64,
}
impl SubmitAudit {
    pub fn new(day: &str, p: &PreparedOrder, received_us: u64) -> Result<Self> {
        let i = &p.intent;
        Ok(Self {
            key: FixedKey::new(&p.key)?,
            symbol: FixedText::new(&i.symbol)?,
            day: FixedText::new(day)?,
            venue: i.venue,
            side: i.side,
            offset: i.offset,
            lots: i.lots,
            price_type: i.price_type,
            price_ticks: i.price_ticks,
            tif: i.time_in_force,
            route: i.execution_route,
            price: p.limit_price,
            ceiling: p.option_market_buy_ceiling,
            received_us,
        })
    }
    pub fn day(&self) -> &str {
        self.day.as_str()
    }
    pub fn owned(self) -> PreparedOrder {
        PreparedOrder {
            key: self.key.owned(),
            intent: Intent {
                venue: self.venue,
                symbol: self.symbol.as_str().into(),
                side: self.side,
                offset: self.offset,
                lots: self.lots,
                price_type: self.price_type,
                price_ticks: self.price_ticks,
                time_in_force: self.tif,
                execution_route: self.route,
            },
            limit_price: self.price,
            option_market_buy_ceiling: self.ceiling,
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct CancelAudit {
    key: FixedKey,
    symbol: FixedText<81>,
    venue: Venue,
    system_id: FixedText<32>,
    route: Option<ExecutionRoute>,
}
impl CancelAudit {
    pub fn new(p: &PreparedCancel) -> Result<Self> {
        Ok(Self {
            key: FixedKey::new(&p.key)?,
            symbol: FixedText::new(&p.symbol)?,
            venue: p.venue,
            system_id: FixedText::new(&p.system_id)?,
            route: p.execution_route,
        })
    }
    pub fn owned(self) -> PreparedCancel {
        PreparedCancel {
            key: self.key.owned(),
            symbol: self.symbol.as_str().into(),
            venue: self.venue,
            system_id: self.system_id.as_str().into(),
            execution_route: self.route,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_bounds_preserve_complete_unicode_without_truncation() {
        let s = FixedText::<4>::new("中a").unwrap();
        assert_eq!(s.as_str(), "中a");
        assert!(FixedText::<3>::new("中a").is_err());
    }
}
