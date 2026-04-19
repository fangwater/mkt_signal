//! Bybit V5 私有账户事件解析器（wallet / position / order / execution）

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope, BasicBalanceMsg,
    BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
};
use crate::common::bybit_account_msg::BybitBasicOrderMsg;
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BybitAccountEventParser;

#[derive(Debug, Deserialize)]
struct BybitWalletChannelRow {
    #[serde(default, rename = "updatedTime")]
    updated_time: String,
    #[serde(default)]
    coin: Vec<BybitWalletChannelCoin>,
}

#[derive(Debug, Deserialize)]
struct BybitWalletChannelCoin {
    #[serde(default)]
    coin: String,
    #[serde(default, rename = "walletBalance")]
    wallet_balance: String,
    #[serde(default, rename = "borrowAmount")]
    borrow_amount: String,
    #[serde(default, rename = "spotBorrow")]
    spot_borrow: String,
    #[serde(default, rename = "accruedInterest")]
    accrued_interest: String,
    #[serde(default, rename = "borrowInterest")]
    borrow_interest: String,
}

impl BybitAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_wallet_channel(&self, json_value: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("creationTime"))
            .or_else(|| parse_i64_str_or_num(json_value.get("ts")))
            .unwrap_or(0);

        for wallet in collect_data_objects(json_value) {
            let Ok(wallet_row) =
                serde_json::from_value::<BybitWalletChannelRow>(Value::Object(wallet.clone()))
            else {
                continue;
            };
            let timestamp = parse_i64_str_or_num(Some(&Value::String(wallet_row.updated_time)))
                .unwrap_or(top_timestamp);

            for coin_item in wallet_row.coin {
                let coin = coin_item.coin;
                if coin.is_empty() {
                    continue;
                }

                let balance = parse_f64_str(&coin_item.wallet_balance).unwrap_or(0.0);
                let balance_msg = BasicBalanceMsg::create(timestamp, coin.clone(), balance);
                let balance_event = BasicAccountEventMsg::create(
                    BasicAccountEventType::BalanceUpdate,
                    BasicAccountScope::BybitUnified,
                    balance_msg.to_bytes(),
                );
                if tx.send(balance_event.to_bytes()).is_ok() {
                    count += 1;
                }

                let borrowed = parse_f64_str(&coin_item.borrow_amount)
                    .or_else(|| parse_f64_str(&coin_item.spot_borrow))
                    .unwrap_or(0.0);
                let interest = parse_f64_str(&coin_item.accrued_interest)
                    .or_else(|| parse_f64_str(&coin_item.borrow_interest))
                    .unwrap_or(0.0);
                if borrowed > 0.0 || interest > 0.0 {
                    let interest_msg =
                        BasicBorrowInterestMsg::create(timestamp, coin, borrowed, interest);
                    let interest_event = BasicAccountEventMsg::create(
                        BasicAccountEventType::BorrowInterest,
                        BasicAccountScope::BybitUnified,
                        interest_msg.to_bytes(),
                    );
                    if tx.send(interest_event.to_bytes()).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    fn parse_position_channel(
        &self,
        json_value: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("creationTime"))
            .or_else(|| parse_i64_str_or_num(json_value.get("ts")))
            .unwrap_or(0);

        for obj in collect_data_objects(json_value) {
            let inst_id = obj
                .get("symbol")
                .or_else(|| obj.get("instId"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if inst_id.is_empty() {
                continue;
            }

            let side = obj.get("side").and_then(|v| v.as_str()).unwrap_or("");
            let position_side = match side.to_ascii_lowercase().as_str() {
                "buy" | "long" => 'L',
                "sell" | "short" => 'S',
                _ => 'N',
            };

            let position_amount = parse_f64_str_or_num(obj.get("size"))
                .or_else(|| parse_f64_str_or_num(obj.get("positionValue")))
                .unwrap_or(0.0) as f32;

            let timestamp = parse_i64_str_or_num(obj.get("updatedTime"))
                .or_else(|| parse_i64_str_or_num(obj.get("creationTime")))
                .unwrap_or(top_timestamp);

            let position_msg = BasicPositionMsg::create(
                timestamp,
                inst_id.clone(),
                position_side,
                position_amount,
            );
            let position_event = BasicAccountEventMsg::create(
                BasicAccountEventType::PositionUpdate,
                BasicAccountScope::BybitUnified,
                position_msg.to_bytes(),
            );
            if tx.send(position_event.to_bytes()).is_ok() {
                count += 1;
            }

            if let Some(pnl) = parse_f64_str_or_num(obj.get("unrealisedPnl"))
                .or_else(|| parse_f64_str_or_num(obj.get("unrealizedPnl")))
            {
                let pnl_msg = BasicUmUnrealizedMsg::create(timestamp, inst_id, position_side, pnl);
                let pnl_event = BasicAccountEventMsg::create(
                    BasicAccountEventType::UnrealizedPnlUpdate,
                    BasicAccountScope::BybitUnified,
                    pnl_msg.to_bytes(),
                );
                if tx.send(pnl_event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        count
    }

    fn parse_order_channel(&self, json_value: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("creationTime"))
            .or_else(|| parse_i64_str_or_num(json_value.get("ts")))
            .unwrap_or(0);
        let raw_json = json_value.to_string();

        for order_obj in collect_data_objects(json_value) {
            let order_status_text = order_obj
                .get("orderStatus")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // order topic is intentionally narrowed to the subset we want to mirror into the
            // unified pipeline:
            // - NEW / REJECTED / CANCELLED / PARTIALLY_FILLED_CANCELED => OrderUpdate
            // - maker-only PARTIALLY_FILLED / FILLED => TradeUpdate supplement
            // - other Bybit-only transitional states are logged and ignored
            match classify_bybit_order_status(order_status_text) {
                BybitOrderChannelKind::OrderUpdate => {
                    let Some(msg) = self.parse_order_like(order_obj, top_timestamp, &raw_json)
                    else {
                        continue;
                    };
                    let event = BasicAccountEventMsg::create(
                        BasicAccountEventType::OrderUpdate,
                        BasicAccountScope::BybitUnified,
                        msg.to_bytes(),
                    );
                    if tx.send(event.to_bytes()).is_ok() {
                        count += 1;
                    }
                }
                BybitOrderChannelKind::TradeUpdate => {
                    let Some(msg) =
                        self.parse_order_trade_only(order_obj, top_timestamp, &raw_json)
                    else {
                        continue;
                    };
                    let event = BasicAccountEventMsg::create(
                        BasicAccountEventType::OrderUpdate,
                        BasicAccountScope::BybitUnified,
                        msg.to_bytes(),
                    );
                    if tx.send(event.to_bytes()).is_ok() {
                        count += 1;
                    }
                }
                BybitOrderChannelKind::Ignore => {
                    warn!(
                        "Bybit: ignored order status='{}', payload={}",
                        order_status_text, raw_json
                    );
                }
            }
        }

        count
    }

    fn parse_execution_channel(
        &self,
        json_value: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("creationTime"))
            .or_else(|| parse_i64_str_or_num(json_value.get("ts")))
            .unwrap_or(0);
        let raw_json = json_value.to_string();

        for exec_obj in collect_data_objects(json_value) {
            let Some(msg) = self.parse_execution_trade_only(exec_obj, top_timestamp, &raw_json)
            else {
                continue;
            };
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::OrderUpdate,
                BasicAccountScope::BybitUnified,
                msg.to_bytes(),
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
    }

    fn parse_order_like(
        &self,
        obj: &Map<String, Value>,
        top_timestamp: i64,
        raw_json: &str,
    ) -> Option<BybitBasicOrderMsg> {
        let symbol = obj.get("symbol")?.as_str()?.to_string();
        let order_id_str = obj
            .get("orderId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let client_order_id_str = obj
            .get("orderLinkId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let order_id = BybitBasicOrderMsg::stable_i64_from_str(&order_id_str);
        let client_order_id = parse_required_client_order_id(&client_order_id_str, raw_json)?;

        let side =
            BybitBasicOrderMsg::side_to_u8(obj.get("side").and_then(|v| v.as_str()).unwrap_or(""));
        let order_type = BybitBasicOrderMsg::order_type_to_u8(
            obj.get("orderType").and_then(|v| v.as_str()).unwrap_or(""),
        );
        let time_in_force = BybitBasicOrderMsg::time_in_force_to_u8(
            obj.get("timeInForce")
                .and_then(|v| v.as_str())
                .unwrap_or("gtc"),
        );

        let raw_status = obj
            .get("orderStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let execution_type = order_channel_execution_type(&raw_status);
        let raw_execution_type = match execution_type {
            5 => "Trade".to_string(),
            2 => "Canceled".to_string(),
            6 => "Expired".to_string(),
            8 => "Rejected".to_string(),
            _ => "New".to_string(),
        };
        let order_status = order_channel_order_status(&raw_status);

        let order_price = parse_f64_str_or_num(obj.get("price"))
            .or_else(|| parse_f64_str_or_num(obj.get("orderPrice")))
            .unwrap_or(0.0);
        let avg_price = parse_f64_str_or_num(obj.get("avgPrice"));
        let price = if execution_type == 5 {
            avg_price.unwrap_or(order_price)
        } else {
            order_price
        };

        let quantity = parse_f64_str_or_num(obj.get("qty"))
            .or_else(|| parse_f64_str_or_num(obj.get("orderQty")))
            .unwrap_or(0.0);

        let cumulative_filled_quantity = parse_f64_str_or_num(obj.get("cumExecQty"))
            .or_else(|| parse_f64_str_or_num(obj.get("cumFilledQty")))
            .unwrap_or(0.0);

        let last_executed_price = if execution_type == 5 {
            avg_price.unwrap_or(order_price)
        } else {
            0.0
        };

        let is_maker = obj
            .get("isMaker")
            .and_then(|v| v.as_bool())
            .map(|v| if v { 1 } else { 0 })
            .unwrap_or(0);

        let commission_asset = obj
            .get("feeCurrency")
            .or_else(|| obj.get("cumExecFeeAsset"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let timestamp = parse_i64_str_or_num(obj.get("execTime"))
            .or_else(|| parse_i64_str_or_num(obj.get("updatedTime")))
            .or_else(|| parse_i64_str_or_num(obj.get("creationTime")))
            .unwrap_or(top_timestamp);

        Some(BybitBasicOrderMsg::create(
            detect_order_venue(obj),
            timestamp,
            symbol,
            order_id,
            order_id_str,
            client_order_id,
            client_order_id_str,
            side,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            is_maker,
            price,
            quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_asset,
            raw_status,
            raw_execution_type,
        ))
    }

    fn parse_order_trade_only(
        &self,
        obj: &Map<String, Value>,
        top_timestamp: i64,
        raw_json: &str,
    ) -> Option<BybitBasicOrderMsg> {
        let raw_status = obj
            .get("orderStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !matches!(
            classify_bybit_order_status(raw_status),
            BybitOrderChannelKind::TradeUpdate
        ) {
            return None;
        }

        // For order-topic trade supplementation we only trust positive order price, which in this
        // pipeline is used as the maker discriminator. Taker or ambiguous fills fall back to the
        // execution topic instead of emitting an incomplete TradeUpdate here.
        let order_price = parse_f64_str_or_num(obj.get("price"))
            .or_else(|| parse_f64_str_or_num(obj.get("orderPrice")));
        let Some(price) = order_price.filter(|p| *p > 0.0) else {
            warn!(
                "Bybit: order trade supplement skipped due to missing/non-positive price, payload={}",
                raw_json
            );
            return None;
        };

        let symbol = obj.get("symbol")?.as_str()?.to_string();
        let order_id_str = obj
            .get("orderId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let client_order_id_str = obj
            .get("orderLinkId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let order_id = BybitBasicOrderMsg::stable_i64_from_str(&order_id_str);
        let client_order_id = parse_required_client_order_id(&client_order_id_str, raw_json)?;
        let order_status = match raw_status.to_ascii_lowercase().as_str() {
            "filled" => 3,
            _ => 2,
        };

        let side =
            BybitBasicOrderMsg::side_to_u8(obj.get("side").and_then(|v| v.as_str()).unwrap_or(""));
        let order_type = BybitBasicOrderMsg::order_type_to_u8(
            obj.get("orderType").and_then(|v| v.as_str()).unwrap_or(""),
        );
        let time_in_force = BybitBasicOrderMsg::time_in_force_to_u8(
            obj.get("timeInForce")
                .and_then(|v| v.as_str())
                .unwrap_or("gtc"),
        );
        let quantity = parse_f64_str_or_num(obj.get("qty"))
            .or_else(|| parse_f64_str_or_num(obj.get("orderQty")))
            .unwrap_or(0.0);
        let cumulative_filled_quantity = parse_f64_str_or_num(obj.get("cumExecQty"))
            .or_else(|| parse_f64_str_or_num(obj.get("cumFilledQty")))
            .unwrap_or(0.0);
        if cumulative_filled_quantity <= 0.0 {
            warn!(
                "Bybit: order trade supplement skipped due to non-positive cumExecQty, payload={}",
                raw_json
            );
            return None;
        }
        let timestamp = parse_i64_str_or_num(obj.get("updatedTime"))
            .or_else(|| parse_i64_str_or_num(obj.get("creationTime")))
            .unwrap_or(top_timestamp);

        Some(BybitBasicOrderMsg::create(
            detect_order_venue(obj),
            timestamp,
            symbol,
            order_id,
            order_id_str,
            client_order_id,
            client_order_id_str,
            side,
            order_type,
            time_in_force,
            5,
            order_status,
            1,
            price,
            quantity,
            cumulative_filled_quantity,
            price,
            String::new(),
            raw_status.to_string(),
            "Trade".to_string(),
        ))
    }

    fn parse_execution_trade_only(
        &self,
        obj: &Map<String, Value>,
        top_timestamp: i64,
        raw_json: &str,
    ) -> Option<BybitBasicOrderMsg> {
        let symbol = obj.get("symbol")?.as_str()?.to_string();
        let order_id_str = obj
            .get("orderId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let client_order_id_str = obj
            .get("orderLinkId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let order_id = BybitBasicOrderMsg::stable_i64_from_str(&order_id_str);
        let client_order_id = parse_required_client_order_id(&client_order_id_str, raw_json)?;
        let order_status = derive_execution_order_status(obj)?;

        let side =
            BybitBasicOrderMsg::side_to_u8(obj.get("side").and_then(|v| v.as_str()).unwrap_or(""));
        let order_type = BybitBasicOrderMsg::order_type_to_u8(
            obj.get("orderType").and_then(|v| v.as_str()).unwrap_or(""),
        );
        let time_in_force = BybitBasicOrderMsg::time_in_force_to_u8(
            obj.get("timeInForce")
                .and_then(|v| v.as_str())
                .unwrap_or("gtc"),
        );
        let exec_price = parse_f64_str_or_num(obj.get("execPrice"))
            .or_else(|| parse_f64_str_or_num(obj.get("avgPrice")))
            .unwrap_or(0.0);
        let quantity = parse_f64_str_or_num(obj.get("orderQty"))
            .or_else(|| parse_f64_str_or_num(obj.get("qty")))
            .unwrap_or(0.0);
        let leaves_qty = parse_f64_str_or_num(obj.get("leavesQty"))?;
        let cumulative_filled_quantity = (quantity - leaves_qty).max(0.0);
        let is_maker = obj
            .get("isMaker")
            .and_then(|v| v.as_bool())
            .map(|v| if v { 1 } else { 0 })
            .unwrap_or(0);
        let commission_asset = obj
            .get("feeCurrency")
            .or_else(|| obj.get("cumExecFeeAsset"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let timestamp = parse_i64_str_or_num(obj.get("execTime"))
            .or_else(|| parse_i64_str_or_num(obj.get("updatedTime")))
            .or_else(|| parse_i64_str_or_num(obj.get("creationTime")))
            .unwrap_or(top_timestamp);
        let raw_execution_type = obj
            .get("execType")
            .and_then(|v| v.as_str())
            .unwrap_or("Trade")
            .to_string();
        let raw_status = if order_status == 3 {
            "Filled".to_string()
        } else {
            "PartiallyFilled".to_string()
        };

        Some(BybitBasicOrderMsg::create(
            detect_order_venue(obj),
            timestamp,
            symbol,
            order_id,
            order_id_str,
            client_order_id,
            client_order_id_str,
            side,
            order_type,
            time_in_force,
            5,
            order_status,
            is_maker,
            exec_price,
            quantity,
            cumulative_filled_quantity,
            exec_price,
            commission_asset,
            raw_status,
            raw_execution_type,
        ))
    }
}

impl Parser for BybitAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let json_value: Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return 0,
        };

        if let Some(op) = json_value.get("op").and_then(|v| v.as_str()) {
            if matches!(op, "auth" | "subscribe" | "ping" | "pong") {
                return 0;
            }
        }
        if let Some(success) = json_value.get("success").and_then(|v| v.as_bool()) {
            if !success {
                warn!("Bybit: private ws non-success payload={}", json_str);
                return 0;
            }
        }

        let route = json_value
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match route {
            "wallet" => self.parse_wallet_channel(&json_value, tx),
            "position" => self.parse_position_channel(&json_value, tx),
            "order" => self.parse_order_channel(&json_value, tx),
            "execution" => self.parse_execution_channel(&json_value, tx),
            _ => {
                if !route.is_empty() {
                    debug!("Bybit: ignored route={} payload={}", route, json_str);
                } else {
                    debug!("Bybit: ignored private payload={}", json_str);
                }
                0
            }
        }
    }
}

fn collect_data_objects<'a>(json_value: &'a Value) -> Vec<&'a Map<String, Value>> {
    match json_value.get("data") {
        Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_object()).collect(),
        Some(Value::Object(obj)) => vec![obj],
        _ => Vec::new(),
    }
}

fn detect_order_venue(order_obj: &Map<String, Value>) -> u8 {
    let category = order_obj
        .get("category")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if category.contains("linear")
        || category.contains("inverse")
        || category.contains("option")
        || category.contains("future")
    {
        BybitBasicOrderMsg::VENUE_FUTURES
    } else {
        BybitBasicOrderMsg::VENUE_SPOT
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BybitOrderChannelKind {
    OrderUpdate,
    TradeUpdate,
    Ignore,
}

fn classify_bybit_order_status(status: &str) -> BybitOrderChannelKind {
    match status.to_ascii_lowercase().as_str() {
        // Spot PARTIALLY_FILLED_CANCELED is treated as a plain cancel update here.
        "new" | "rejected" | "cancelled" | "canceled" | "partiallyfilledcanceled" => {
            BybitOrderChannelKind::OrderUpdate
        }
        // These are only used to supplement maker fills from order topic.
        "partiallyfilled" | "partially_filled" | "partialfill" | "partial-fill" | "filled" => {
            BybitOrderChannelKind::TradeUpdate
        }
        _ => BybitOrderChannelKind::Ignore,
    }
}

fn order_channel_execution_type(status: &str) -> u8 {
    match status.to_ascii_lowercase().as_str() {
        "cancelled" | "canceled" | "partiallyfilledcanceled" => 2,
        "rejected" => 8,
        _ => 1,
    }
}

fn order_channel_order_status(status: &str) -> u8 {
    match status.to_ascii_lowercase().as_str() {
        "cancelled" | "canceled" | "partiallyfilledcanceled" => 4,
        "rejected" => 5,
        _ => 1,
    }
}

fn derive_execution_order_status(obj: &Map<String, Value>) -> Option<u8> {
    let leaves_qty = parse_f64_str_or_num(obj.get("leavesQty"))?;
    Some(if leaves_qty <= 0.0 { 3 } else { 2 })
}

fn parse_f64_str_or_num(v: Option<&Value>) -> Option<f64> {
    v.and_then(|val| {
        if let Some(f) = val.as_f64() {
            Some(f)
        } else if let Some(i) = val.as_i64() {
            Some(i as f64)
        } else if let Some(u) = val.as_u64() {
            Some(u as f64)
        } else if let Some(s) = val.as_str() {
            s.parse::<f64>().ok()
        } else {
            None
        }
    })
}

fn parse_f64_str(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

fn parse_i64_str_or_num(v: Option<&Value>) -> Option<i64> {
    v.and_then(|val| {
        if let Some(i) = val.as_i64() {
            Some(i)
        } else if let Some(u) = val.as_u64() {
            Some(u as i64)
        } else if let Some(s) = val.as_str() {
            s.parse::<i64>().ok()
        } else {
            None
        }
    })
}

fn parse_required_client_order_id(value: &str, raw_json: &str) -> Option<i64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        warn!(
            "Bybit: missing/empty orderLinkId, drop payload={}",
            raw_json
        );
        return None;
    }
    match trimmed.parse::<i64>() {
        Ok(v) => Some(v),
        Err(_) => {
            warn!(
                "Bybit: non-i64 orderLinkId='{}', drop payload={}",
                trimmed, raw_json
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wallet_position_order_and_execution() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let wallet = Bytes::from_static(
            br#"{"id":"1","topic":"wallet","creationTime":1710000000000,"data":[{"accountType":"UNIFIED","coin":[{"coin":"BTC","walletBalance":"1.25","borrowAmount":"0.10","accruedInterest":"0.01"},{"coin":"USDT","walletBalance":"1000","borrowAmount":"50","accruedInterest":"2"}]}]}"#,
        );
        assert_eq!(parser.parse(wallet, &tx), 4);

        let position = Bytes::from_static(
            br#"{"id":"2","topic":"position","creationTime":1710000000100,"data":[{"symbol":"BTCUSDT","side":"Buy","size":"0.5","unrealisedPnl":"12.5","updatedTime":"1710000000001"}]}"#,
        );
        assert_eq!(parser.parse(position, &tx), 2);

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"abcdef","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"PartiallyFilled","price":"100000","qty":"0.5","cumExecQty":"0.1","avgPrice":"99999.5","updatedTime":"1710000000002"}]}"#,
        );
        assert_eq!(parser.parse(order, &tx), 1);

        let execution = Bytes::from_static(
            br#"{"id":"4","topic":"execution","creationTime":1710000000300,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"abcdef","orderLinkId":"12345","side":"Buy","orderType":"Limit","execType":"Trade","isMaker":true,"execPrice":"99998","execQty":"0.1","orderQty":"0.5","leavesQty":"0.3","execTime":"1710000000003","feeCurrency":"USDT"}]}"#,
        );
        assert_eq!(parser.parse(execution, &tx), 1);

        let mut seen_balance = 0;
        let mut seen_borrow = 0;
        let mut seen_position = 0;
        let mut seen_pnl = 0;
        let mut seen_order = 0;
        let mut seen_trade = 0;

        while let Ok(msg) = rx.try_recv() {
            let (ty, scope, body) =
                crate::common::basic_account_msg::split_basic_account_event(&msg).expect("split");
            assert_eq!(scope, BasicAccountScope::BybitUnified);
            match ty {
                BasicAccountEventType::BalanceUpdate => {
                    let bal = BasicBalanceMsg::from_bytes(body).expect("balance");
                    if bal.symbol == "BTC" {
                        assert!((bal.balance - 1.25).abs() < 1e-9);
                    }
                    seen_balance += 1;
                }
                BasicAccountEventType::BorrowInterest => {
                    let borrow = BasicBorrowInterestMsg::from_bytes(body).expect("borrow");
                    if borrow.symbol == "USDT" {
                        assert!((borrow.borrowed - 50.0).abs() < 1e-9);
                    }
                    seen_borrow += 1;
                }
                BasicAccountEventType::PositionUpdate => {
                    let pos = BasicPositionMsg::from_bytes(body).expect("position");
                    assert_eq!(pos.inst_id, "BTCUSDT");
                    assert_eq!(pos.position_side, 'L');
                    seen_position += 1;
                }
                BasicAccountEventType::UnrealizedPnlUpdate => {
                    let pnl = BasicUmUnrealizedMsg::from_bytes(body).expect("pnl");
                    assert_eq!(pnl.inst_id, "BTCUSDT");
                    seen_pnl += 1;
                }
                BasicAccountEventType::OrderUpdate => {
                    let order = BybitBasicOrderMsg::from_bytes(body).expect("order");
                    assert_eq!(order.symbol, "BTCUSDT");
                    assert_eq!(order.client_order_id, 12345);
                    if order.execution_type == 5 {
                        seen_trade += 1;
                        if !order.commission_asset.is_empty() {
                            assert!((order.price - 99998.0).abs() < 1e-9);
                        } else {
                            assert!((order.price - 100000.0).abs() < 1e-9);
                        }
                    } else {
                        seen_order += 1;
                    }
                }
                _ => {}
            }
        }

        assert_eq!(seen_balance, 2);
        assert_eq!(seen_borrow, 2);
        assert_eq!(seen_position, 1);
        assert_eq!(seen_pnl, 1);
        assert_eq!(seen_order, 0);
        assert_eq!(seen_trade, 2);
    }

    #[test]
    fn drops_order_when_order_link_id_is_not_i64() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"abcdef","orderLinkId":"not-i64","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"New","price":"100000","qty":"0.5","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &tx), 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn drops_execution_when_leaves_qty_is_missing() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let execution = Bytes::from_static(
            br#"{"id":"4","topic":"execution","creationTime":1710000000300,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","execType":"Trade","isMaker":true,"execPrice":"99998","execQty":"0.1","orderQty":"0.5","execTime":"1710000000003","feeCurrency":"USDT"}]}"#,
        );

        assert_eq!(parser.parse(execution, &tx), 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn order_new_emits_order_update_only() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"New","price":"100000","qty":"0.5","cumExecQty":"0","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &tx), 1);
        let msg = rx.try_recv().expect("one event");
        let (ty, _, body) =
            crate::common::basic_account_msg::split_basic_account_event(&msg).expect("split");
        assert_eq!(ty, BasicAccountEventType::OrderUpdate);
        let order = BybitBasicOrderMsg::from_bytes(body).expect("decode");
        assert_eq!(order.execution_type, 1);
        assert_eq!(order.order_status, 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn order_triggered_is_ignored() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"Triggered","price":"100000","qty":"0.5","cumExecQty":"0","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &tx), 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn order_trade_without_price_is_ignored() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"Filled","price":"0","qty":"0.5","cumExecQty":"0.5","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &tx), 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn order_partially_filled_canceled_emits_canceled_order_update() {
        let parser = BybitAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"spot","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"PartiallyFilledCanceled","price":"100000","qty":"0.5","cumExecQty":"0.1","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &tx), 1);
        let msg = rx.try_recv().expect("one event");
        let (ty, _, body) =
            crate::common::basic_account_msg::split_basic_account_event(&msg).expect("split");
        assert_eq!(ty, BasicAccountEventType::OrderUpdate);
        let order = BybitBasicOrderMsg::from_bytes(body).expect("decode");
        assert_eq!(order.execution_type, 2);
        assert_eq!(order.order_status, 4);
        assert_eq!(order.raw_status, "PartiallyFilledCanceled");
        assert!(rx.try_recv().is_err());
    }
}
