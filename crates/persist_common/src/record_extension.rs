//! Optional cross-market facts appended after the crypto trade-update and
//! uniform-order layouts. Crypto producers write nothing, so their bytes and
//! existing decoders are unchanged; domestic futures always write the block.
//!
//! Layout (little endian): `u32 EXTENSION_MAGIC`, `u8 flags`, then present
//! fields in flag order: position effect `u8`; trading day, trade id and client
//! order text as `u16 len + UTF-8`; last filled quantity and commission `f64`.
//! The magic is larger than any uniform provenance length, so a uniform decoder
//! can tell the two tails apart.
use anyhow::{bail, ensure, Context, Result};
use bytes::BufMut;
use order_common::{PositionEffect, TradeUpdate};

pub const EXTENSION_MAGIC: u32 = 0xFFFF_FFFE;

const POSITION_EFFECT: u8 = 1 << 0;
const TRADING_DAY: u8 = 1 << 1;
const TRADE_ID: u8 = 1 << 2;
const LAST_FILLED: u8 = 1 << 3;
const COMMISSION: u8 = 1 << 4;
const CLIENT_ORDER_TEXT: u8 = 1 << 5;
const KNOWN: u8 = (1 << 6) - 1;

/// Borrowed view for writers; never allocates.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ExtensionRef<'a> {
    pub position_effect: Option<PositionEffect>,
    pub trading_day: Option<&'a str>,
    pub trade_id: Option<&'a str>,
    pub last_filled_quantity: Option<f64>,
    pub commission: Option<f64>,
    pub client_order_text: Option<&'a str>,
}

/// Decoded, owned form.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordExtension {
    pub position_effect: Option<PositionEffect>,
    pub trading_day: Option<String>,
    pub trade_id: Option<String>,
    pub last_filled_quantity: Option<f64>,
    pub commission: Option<f64>,
    pub client_order_text: Option<String>,
}

impl<'a> ExtensionRef<'a> {
    pub fn of_trade(trade: &'a dyn TradeUpdate) -> Self {
        Self {
            position_effect: trade.position_effect(),
            trading_day: trade.trading_day(),
            trade_id: trade.trade_id(),
            last_filled_quantity: trade.last_filled_quantity(),
            commission: trade.commission(),
            client_order_text: trade.client_order_text(),
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Appends the block unless every field is absent.
    pub fn write(&self, buf: &mut impl BufMut) {
        if self.is_empty() {
            return;
        }
        let flags = flag(self.position_effect.is_some(), POSITION_EFFECT)
            | flag(self.trading_day.is_some(), TRADING_DAY)
            | flag(self.trade_id.is_some(), TRADE_ID)
            | flag(self.last_filled_quantity.is_some(), LAST_FILLED)
            | flag(self.commission.is_some(), COMMISSION)
            | flag(self.client_order_text.is_some(), CLIENT_ORDER_TEXT);
        buf.put_u32_le(EXTENSION_MAGIC);
        buf.put_u8(flags);
        if let Some(effect) = self.position_effect {
            buf.put_u8(effect as u8);
        }
        for text in [self.trading_day, self.trade_id].into_iter().flatten() {
            put_text(buf, text);
        }
        for value in [self.last_filled_quantity, self.commission]
            .into_iter()
            .flatten()
        {
            buf.put_f64_le(value);
        }
        if let Some(text) = self.client_order_text {
            put_text(buf, text);
        }
    }
}

impl RecordExtension {
    pub fn as_ref(&self) -> ExtensionRef<'_> {
        ExtensionRef {
            position_effect: self.position_effect,
            trading_day: self.trading_day.as_deref(),
            trade_id: self.trade_id.as_deref(),
            last_filled_quantity: self.last_filled_quantity,
            commission: self.commission,
            client_order_text: self.client_order_text.as_deref(),
        }
    }

    /// Whether `input` starts with an extension block.
    pub fn present(input: &[u8]) -> bool {
        input.len() >= 4 && u32::from_le_bytes(input[..4].try_into().unwrap()) == EXTENSION_MAGIC
    }

    /// Reads a block when one starts at `input`; advances past it.
    pub fn read(input: &mut &[u8]) -> Result<Option<Self>> {
        if !Self::present(input) {
            return Ok(None);
        }
        *input = &input[4..];
        let flags = take(input, 1)?[0];
        ensure!(
            flags & !KNOWN == 0,
            "unknown record extension flags {flags:#x}"
        );
        let mut value = Self::default();
        if flags & POSITION_EFFECT != 0 {
            let raw = take(input, 1)?[0];
            value.position_effect =
                Some(PositionEffect::from_u8(raw).context("invalid position effect")?);
        }
        if flags & TRADING_DAY != 0 {
            value.trading_day = Some(text(input)?);
        }
        if flags & TRADE_ID != 0 {
            value.trade_id = Some(text(input)?);
        }
        if flags & LAST_FILLED != 0 {
            value.last_filled_quantity = Some(f64_value(input)?);
        }
        if flags & COMMISSION != 0 {
            value.commission = Some(f64_value(input)?);
        }
        if flags & CLIENT_ORDER_TEXT != 0 {
            value.client_order_text = Some(text(input)?);
        }
        Ok(Some(value))
    }
}

fn flag(set: bool, bit: u8) -> u8 {
    if set {
        bit
    } else {
        0
    }
}
fn put_text(buf: &mut impl BufMut, text: &str) {
    // Identifiers are short; an over-long one is cut at a UTF-8 boundary
    // rather than corrupting the record.
    let mut end = text.len().min(u16::MAX as usize);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    buf.put_u16_le(end as u16);
    buf.put_slice(&text.as_bytes()[..end]);
}
fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if input.len() < n {
        bail!("truncated record extension");
    }
    let (head, tail) = input.split_at(n);
    *input = tail;
    Ok(head)
}
fn text(input: &mut &[u8]) -> Result<String> {
    let len = u16::from_le_bytes(take(input, 2)?.try_into()?) as usize;
    Ok(std::str::from_utf8(take(input, len)?)
        .context("record extension text is not UTF-8")?
        .to_owned())
}
fn f64_value(input: &mut &[u8]) -> Result<f64> {
    Ok(f64::from_le_bytes(take(input, 8)?.try_into()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_fields_write_nothing_and_present_fields_round_trip() {
        let mut buf = Vec::new();
        ExtensionRef::default().write(&mut buf);
        assert!(buf.is_empty());
        let full = RecordExtension {
            position_effect: Some(PositionEffect::CloseToday),
            trading_day: Some("20261120".into()),
            trade_id: Some("  123456".into()),
            last_filled_quantity: Some(2.0),
            commission: Some(3.25),
            client_order_text: Some("ctp:1:2/17".into()),
        };
        full.as_ref().write(&mut buf);
        buf.extend_from_slice(b"rest");
        let mut input = buf.as_slice();
        assert_eq!(RecordExtension::read(&mut input).unwrap(), Some(full));
        assert_eq!(input, b"rest");
        let partial = RecordExtension {
            trade_id: Some("9".into()),
            ..Default::default()
        };
        buf.clear();
        partial.as_ref().write(&mut buf);
        let mut input = buf.as_slice();
        assert_eq!(RecordExtension::read(&mut input).unwrap(), Some(partial));
        assert!(input.is_empty());
        assert_eq!(
            RecordExtension::read(&mut &b"\x01\x00\x00\x00"[..]).unwrap(),
            None
        );
    }
}
