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
pub fn bitget_reason(value: &str) -> Option<&'static str> {
    match value {
        "liquidation" => Some("liquidation"),
        "reduce" => Some("forced_reduction"),
        "delivery" => Some("delivery"),
        _ => None,
    }
}

impl ExternalFuturesFill {
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
            matches!(self.venue, 5 | 7) && self.symbol.len() > 4 && self.symbol.ends_with("USDT"),
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
        let scope = if self.venue == 5 {
            BasicAccountScope::BybitUnified
        } else {
            BasicAccountScope::BitgetUnified
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
        let mut row = serde_json::json!({"category":"linear","symbol":"BTCUSDT","orderId":"exchange-owned","orderLinkId":"non-numeric","execId":"trade-1","execTime":"1700000000000","side":"Sell","execPrice":"60000","execQty":"0.1"});
        let fill = ExternalFuturesFill::from_exchange(5, &row, bybit_reason("Delivery")).unwrap();
        assert_eq!(fill.order_id, "exchange-owned");
        assert_eq!(fill.timestamp_us, 1_700_000_000_000_000);
        assert_eq!(fill.is_maker, None);
        row["execQty"] = serde_json::json!("0");
        assert!(ExternalFuturesFill::from_exchange(5, &row, Some("delivery")).is_none());
    }
}
