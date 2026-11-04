//! Exchange-owned fills retain their native identities without a CTA owner.
use super::basic_account_msg::{BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope};
use crate::account_event::AccountEventSink;
use anyhow::{ensure, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalFuturesFill {
    pub venue: u8,
    pub symbol: String,
    pub order_id: String,
    pub trade_id: String,
    pub timestamp_us: i64,
    pub side: u8,
    pub price: f64,
    pub quantity: f64,
    pub is_maker: Option<bool>,
    pub reason: Option<String>,
}

pub fn bybit_reason(value: &str) -> Option<&'static str> {
    match value {
        "BustTrade" => Some("liquidation"),
        "AdlTrade" => Some("adl"),
        "Delivery" => Some("delivery"),
        _ => None,
    }
}
pub fn okex_reason(category: &str) -> Option<&'static str> {
    match category {
        "adl" => Some("adl"),
        "full_liquidation" | "partial_liquidation" => Some("liquidation"),
        "delivery" => Some("delivery"),
        _ => None,
    }
}
pub fn bitget_reason(value: &str) -> Option<&'static str> {
    match value {
        "liquidation" => Some("liquidation"),
        "reduce" => Some("forced_reduction"),
        "delivery" => Some("delivery"),
        _ => None,
    }
}

/// UTA order/fill `tradeSide` identities from the venue's enumeration.
/// `execType=reduce` is a separate forced reduction, not proof of ADL.
pub fn bitget_trade_side_reason(value: &str) -> Option<&'static str> {
    match value {
        "dte_sys_adl_close_long"
        | "dte_sys_adl_close_short"
        | "dte_sys_adl_buy_in_single_side_mode"
        | "dte_sys_adl_sell_in_single_side_mode" => Some("adl"),
        _ => None,
    }
}

impl ExternalFuturesFill {
    /// OKX's VIP `fills` channel excludes ADL. Only actual `orders` fills
    /// supply repair evidence; cumulative order quantities are not new fills.
    pub fn from_okex_order_fill(row: &Value) -> Option<Self> {
        let reason = okex_reason(row["category"].as_str()?)?;
        if row["instType"].as_str()? != "SWAP" {
            return None;
        }
        let base = row["instId"].as_str()?.strip_suffix("-USDT-SWAP")?;
        if base.is_empty() || base.contains('-') {
            return None;
        }
        let order_id = row["ordId"].as_str()?;
        let trade_id = row["tradeId"].as_str()?;
        // Forced-close native trade IDs may be negative. Zero/empty IDs do
        // not identify a factual fill and must not be synthesized from times.
        if order_id.parse::<u64>().ok()? == 0 || trade_id.parse::<i64>().ok()? == 0 {
            return None;
        }
        let number = |key: &str| {
            row[key]
                .as_f64()
                .or_else(|| row[key].as_str()?.parse().ok())
        };
        let timestamp = row["fillTime"]
            .as_i64()
            .or_else(|| row["fillTime"].as_str()?.parse::<i64>().ok())?;
        let value = Self {
            venue: 3,
            symbol: format!("{base}USDT"),
            order_id: order_id.to_owned(),
            trade_id: trade_id.to_owned(),
            timestamp_us: timestamp.checked_mul(1_000)?,
            side: match row["side"].as_str()? {
                "buy" => 1,
                "sell" => 2,
                _ => return None,
            },
            price: number("fillPx")?,
            quantity: number("fillSz")?,
            is_maker: match row["execType"].as_str() {
                Some("M") => Some(true),
                Some("T") => Some(false),
                _ => None,
            },
            reason: Some(reason.to_owned()),
        };
        value.validate().ok()?;
        Some(value)
    }

    /// Gate user trades are authoritative fills; order notifications only carry
    /// cumulative quantities and must not create a second ADL fill.
    pub fn from_gate_adl(row: &Value) -> Option<Self> {
        if row["text"].as_str()? != "auto_deleveraging" {
            return None;
        }
        let identity = |key: &str| match &row[key] {
            Value::String(value) if !value.is_empty() => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        };
        let number = |key: &str| {
            row[key]
                .as_f64()
                .or_else(|| row[key].as_str()?.parse().ok())
                .filter(|value| value.is_finite())
        };
        let signed_size = number("size")?;
        let timestamp = number("create_time_ms").or_else(|| number("create_time"))?;
        let timestamp_us = if timestamp >= 1_000_000_000_000.0 {
            timestamp * 1_000.0
        } else {
            timestamp * 1_000_000.0
        };
        let value = Self {
            venue: 9,
            symbol: row["contract"].as_str()?.replace('_', ""),
            order_id: identity("order_id")?,
            trade_id: identity("id")?,
            timestamp_us: timestamp_us.round() as i64,
            side: if signed_size > 0.0 { 1 } else { 2 },
            price: number("price")?,
            // Preserve native contracts in the factual record. Pre-trade
            // converts through the venue multiplier before hedging spot.
            quantity: signed_size.abs(),
            is_maker: match row["role"].as_str() {
                Some("maker") => Some(true),
                Some("taker") => Some(false),
                _ => None,
            },
            reason: Some("adl".to_string()),
        };
        value.validate().ok()?;
        Some(value)
    }

    pub fn from_exchange(venue: u8, row: &Value, reason: Option<&str>) -> Option<Self> {
        let bybit = venue == 5;
        if !matches!(venue, 5 | 7) {
            return None;
        }
        let category = row["category"].as_str()?.to_ascii_lowercase();
        if category != if bybit { "linear" } else { "usdt-futures" } {
            return None;
        }
        let string = |key: &str| {
            row[key]
                .as_str()
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let number = |key: &str| {
            row[key]
                .as_f64()
                .or_else(|| row[key].as_str()?.parse().ok())
        };
        let timestamp = row["execTime"]
            .as_i64()
            .or_else(|| row["execTime"].as_str()?.parse().ok())?;
        let value = Self {
            venue,
            symbol: string("symbol")?,
            order_id: string("orderId")?,
            trade_id: string("execId").or_else(|| string("tradeId"))?,
            timestamp_us: timestamp.checked_mul(1000)?,
            side: match row["side"].as_str()?.to_ascii_lowercase().as_str() {
                "buy" => 1,
                "sell" => 2,
                _ => return None,
            },
            price: number("execPrice")?,
            quantity: number("execQty")?,
            is_maker: if bybit {
                row["isMaker"].as_bool()
            } else {
                match row["tradeScope"].as_str() {
                    Some("maker") => Some(true),
                    Some("taker") => Some(false),
                    _ => None,
                }
            },
            reason: reason.map(str::to_owned),
        };
        value.validate().ok()?;
        Some(value)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.venue, 1 | 3 | 5 | 7 | 9)
                && self.symbol.len() > 4
                && self.symbol.ends_with("USDT"),
            "external fill market mismatch"
        );
        ensure!(
            !self.order_id.is_empty()
                && !self.trade_id.is_empty()
                && self.timestamp_us > 0
                && matches!(self.side, 1 | 2),
            "external fill identity missing"
        );
        ensure!(
            self.price.is_finite()
                && self.price > 0.0
                && self.quantity.is_finite()
                && self.quantity > 0.0,
            "invalid external fill price/quantity"
        );
        ensure!(
            self.reason.as_deref().is_none_or(|r| matches!(
                r,
                "liquidation" | "adl" | "delivery" | "forced_reduction"
            )),
            "unknown forced-close reason"
        );
        Ok(())
    }
    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        let value: Self = serde_json::from_slice(raw)?;
        value.validate()?;
        Ok(value)
    }
    pub fn emit<S: AccountEventSink>(&self, sink: &S) -> bool {
        let Ok(raw) = serde_json::to_vec(self) else {
            return false;
        };
        let scope = match self.venue {
            // Binance retains its PM/standard scope on its existing native
            // order-update stream. Its local ADL adapter is not emitted here.
            1 => return false,
            3 => BasicAccountScope::OkexUnified,
            5 => BasicAccountScope::BybitUnified,
            9 => BasicAccountScope::GateUnified,
            _ => BasicAccountScope::BitgetUnified,
        };
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::ExternalFuturesFill,
            scope,
            Bytes::from(raw),
        );
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        (
            "external_futures_fill",
            self.venue,
            &self.order_id,
            &self.trade_id,
            &self.reason,
        )
            .hash(&mut hash);
        sink.emit_with_dedup_key(event.to_bytes(), hash.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_documented_close_identities_and_factual_fills_are_accepted() {
        assert_eq!(bybit_reason("AdlTrade"), Some("adl"));
        assert_eq!(bybit_reason("Funding"), None);
        assert_eq!(bitget_reason("offset"), None);
        assert_eq!(bitget_reason("reduce"), Some("forced_reduction"));
        for trade_side in [
            "dte_sys_adl_close_long",
            "dte_sys_adl_close_short",
            "dte_sys_adl_buy_in_single_side_mode",
            "dte_sys_adl_sell_in_single_side_mode",
        ] {
            assert_eq!(bitget_trade_side_reason(trade_side), Some("adl"));
        }
        for trade_side in ["close", "reduce_close_long", "offset_close_short", "adl"] {
            assert_eq!(bitget_trade_side_reason(trade_side), None);
        }
        let mut row = serde_json::json!({"category":"linear","symbol":"BTCUSDT","orderId":"exchange-owned","orderLinkId":"non-numeric","execId":"trade-1","execTime":"1700000000000","side":"Sell","execPrice":"60000","execQty":"0.1"});
        let fill = ExternalFuturesFill::from_exchange(5, &row, bybit_reason("Delivery")).unwrap();
        assert_eq!(fill.order_id, "exchange-owned");
        assert_eq!(fill.timestamp_us, 1_700_000_000_000_000);
        assert_eq!(fill.is_maker, None);
        row["execQty"] = serde_json::json!("0");
        assert!(ExternalFuturesFill::from_exchange(5, &row, Some("delivery")).is_none());
    }
}
