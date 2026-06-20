//! Bybit V5 私有账户事件解析器（wallet / position / order）

use super::{
    account_risk_dedup_key, balance_dedup_key, borrow_interest_dedup_key, bybit_order_dedup_key,
    lazy_json, position_dedup_key, trade_lite_dedup_key, unrealized_pnl_dedup_key,
    AccountEventSink, Parser,
};
use crate::msg::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg,
    BasicUmUnrealizedMsg,
};
use crate::msg::bybit_account_msg::BybitBasicOrderMsg;
use crate::msg::order_codes::{self, BybitOrderTopicKind};
use bytes::Bytes;
use log::{debug, warn};
use serde_json::{Map, Value};
use sonic_rs::{JsonValueTrait, LazyValue};

#[derive(Clone)]
pub struct BybitAccountEventParser;

impl Default for BybitAccountEventParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BybitAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_wallet_channel<S: AccountEventSink>(&self, root: &LazyValue<'_>, tx: &S) -> usize {
        let mut count = 0;
        let top_timestamp = lazy_json::get_i64(root, &["creationTime", "ts"]).unwrap_or(0);

        for wallet in lazy_json::collect_data_values(root) {
            let timestamp = lazy_json::get_i64(&wallet, &["updatedTime"]).unwrap_or(top_timestamp);

            let has_risk_fields = [
                "totalEquity",
                "totalMarginBalance",
                "totalInitialMargin",
                "totalMaintenanceMargin",
                "accountMMRate",
                "accountIMRate",
            ]
            .iter()
            .any(|key| lazy_json::field_present_non_empty(&wallet, key));
            if has_risk_fields {
                let total_equity = lazy_json::get_f64(&wallet, &["totalEquity"]).unwrap_or(0.0);
                let total_margin_balance =
                    lazy_json::get_f64(&wallet, &["totalMarginBalance"]).unwrap_or(total_equity);
                let maintenance_margin =
                    lazy_json::get_f64(&wallet, &["totalMaintenanceMargin"]).unwrap_or(0.0);
                let account_mm_rate =
                    lazy_json::get_f64(&wallet, &["accountMMRate"]).unwrap_or(0.0);
                let margin_ratio = if account_mm_rate.abs() > f64::EPSILON {
                    1.0 / account_mm_rate
                } else if maintenance_margin.abs() > f64::EPSILON {
                    total_margin_balance / maintenance_margin
                } else {
                    0.0
                };
                let msg = BasicAccountRiskMsg::create(
                    timestamp,
                    total_margin_balance,
                    total_equity,
                    maintenance_margin,
                    lazy_json::get_f64(&wallet, &["totalInitialMargin"]).unwrap_or(0.0),
                    margin_ratio,
                    lazy_json::get_f64(&wallet, &["accountLTV"]).unwrap_or(0.0),
                    0.0,
                );
                let event = BasicAccountEventMsg::create(
                    BasicAccountEventType::AccountRisk,
                    BasicAccountScope::BybitUnified,
                    msg.to_bytes(),
                );
                if tx.emit_with_dedup_key(
                    event.to_bytes(),
                    account_risk_dedup_key(BasicAccountScope::BybitUnified, &msg),
                ) {
                    count += 1;
                }
            }

            let Some(coins) = wallet.get("coin").and_then(|value| value.into_array_iter()) else {
                continue;
            };

            for coin_item in coins
                .filter_map(Result::ok)
                .filter(|value| value.is_object())
            {
                let coin = lazy_json::get_string(&coin_item, &["coin"]).unwrap_or_default();
                if coin.is_empty() {
                    continue;
                }

                let wallet_balance =
                    lazy_json::get_f64(&coin_item, &["walletBalance"]).unwrap_or(0.0);
                let has_liability_fields = [
                    "borrowAmount",
                    "spotBorrow",
                    "accruedInterest",
                    "borrowInterest",
                ]
                .iter()
                .any(|key| lazy_json::field_present_non_empty(&coin_item, key));
                let borrowed =
                    lazy_json::get_f64(&coin_item, &["spotBorrow", "borrowAmount"]).unwrap_or(0.0);
                let interest =
                    lazy_json::get_f64(&coin_item, &["accruedInterest", "borrowInterest"])
                        .unwrap_or(0.0);
                let balance_msg = BasicBalanceMsg::create(timestamp, coin.clone(), wallet_balance);
                let balance_event = BasicAccountEventMsg::create(
                    BasicAccountEventType::BalanceUpdate,
                    BasicAccountScope::BybitUnified,
                    balance_msg.to_bytes(),
                );
                if tx.emit_with_dedup_key(
                    balance_event.to_bytes(),
                    balance_dedup_key(BasicAccountScope::BybitUnified, &balance_msg),
                ) {
                    count += 1;
                }

                if borrowed > 0.0 || interest > 0.0 || has_liability_fields {
                    let interest_msg =
                        BasicBorrowInterestMsg::create(timestamp, coin, borrowed, interest);
                    let interest_event = BasicAccountEventMsg::create(
                        BasicAccountEventType::BorrowInterest,
                        BasicAccountScope::BybitUnified,
                        interest_msg.to_bytes(),
                    );
                    if tx.emit_with_dedup_key(
                        interest_event.to_bytes(),
                        borrow_interest_dedup_key(BasicAccountScope::BybitUnified, &interest_msg),
                    ) {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    fn parse_position_channel<S: AccountEventSink>(&self, json_value: &Value, tx: &S) -> usize {
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
            if tx.emit_with_dedup_key(
                position_event.to_bytes(),
                position_dedup_key(BasicAccountScope::BybitUnified, &position_msg),
            ) {
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
                if tx.emit_with_dedup_key(
                    pnl_event.to_bytes(),
                    unrealized_pnl_dedup_key(BasicAccountScope::BybitUnified, &pnl_msg),
                ) {
                    count += 1;
                }
            }
        }

        count
    }

    fn parse_order_channel<S: AccountEventSink>(&self, json_value: &Value, tx: &S) -> usize {
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
            // - PARTIALLY_FILLED / FILLED => TradeUpdate supplement
            // - other Bybit-only transitional states are logged and ignored
            match order_codes::bybit_order_topic_kind(order_status_text) {
                BybitOrderTopicKind::OrderUpdate => {
                    let Some(msg) = self.parse_order_like(order_obj, top_timestamp, &raw_json)
                    else {
                        continue;
                    };
                    let event = BasicAccountEventMsg::create(
                        BasicAccountEventType::OrderUpdate,
                        BasicAccountScope::BybitUnified,
                        msg.to_bytes(),
                    );
                    if tx.emit_with_dedup_key(
                        event.to_bytes(),
                        bybit_order_dedup_key(BasicAccountScope::BybitUnified, &msg),
                    ) {
                        count += 1;
                    }
                }
                BybitOrderTopicKind::TradeUpdate => {
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
                    if tx.emit_with_dedup_key(
                        event.to_bytes(),
                        bybit_order_dedup_key(BasicAccountScope::BybitUnified, &msg),
                    ) {
                        count += 1;
                    }
                }
                BybitOrderTopicKind::Ignore => {
                    warn!(
                        "Bybit: ignored order status='{}', payload={}",
                        order_status_text, raw_json
                    );
                }
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
        let execution_type = order_codes::bybit_order_topic_execution_type(&raw_status);
        let raw_execution_type = order_codes::execution_type_label(execution_type).to_string();
        let order_status = order_codes::bybit_order_topic_order_status(&raw_status);

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
            order_codes::bybit_order_topic_kind(raw_status),
            BybitOrderTopicKind::TradeUpdate
        ) {
            return None;
        }

        let order_price = parse_f64_str_or_num(obj.get("price"))
            .or_else(|| parse_f64_str_or_num(obj.get("orderPrice")));
        let avg_price = parse_f64_str_or_num(obj.get("avgPrice"));
        let price = avg_price
            .or(order_price.filter(|p| *p > 0.0))
            .unwrap_or(0.0);

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
        let order_status = order_codes::bybit_status_to_order_status(raw_status);

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
        let is_maker = obj
            .get("isMaker")
            .and_then(|v| v.as_bool())
            .map(|v| if v { 1 } else { 0 })
            .unwrap_or(0);

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
            price,
            quantity,
            cumulative_filled_quantity,
            price,
            String::new(),
            raw_status.to_string(),
            "Trade".to_string(),
        ))
    }

    fn parse_execution_fast_channel<S: AccountEventSink>(
        &self,
        json_value: &Value,
        tx: &S,
    ) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("creationTime"))
            .or_else(|| parse_i64_str_or_num(json_value.get("ts")))
            .unwrap_or(0);
        let raw_json = json_value.to_string();

        for fill_obj in collect_data_objects(json_value) {
            let symbol = fill_obj
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if symbol.is_empty() {
                continue;
            }

            let exec_type = fill_obj
                .get("execType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !exec_type.eq_ignore_ascii_case("Trade") {
                continue;
            }

            let client_order_id_raw = fill_obj
                .get("orderLinkId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            let exec_id_raw = fill_obj
                .get("execId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();

            let Some(client_order_id) = parse_i64_str_or_num(fill_obj.get("orderLinkId")) else {
                warn!(
                    "Bybit: skip execution.fast with non-i64 orderLinkId symbol={} orderLinkId='{}' payload={}",
                    symbol, client_order_id_raw, raw_json
                );
                continue;
            };
            if exec_id_raw.is_empty() {
                warn!(
                    "Bybit: skip execution.fast with empty execId symbol={} payload={}",
                    symbol, raw_json
                );
                continue;
            }

            let event_time = parse_i64_str_or_num(fill_obj.get("execTime"))
                .or_else(|| parse_i64_str_or_num(fill_obj.get("creationTime")))
                .unwrap_or(top_timestamp);
            let side = BybitBasicOrderMsg::side_to_u8(
                fill_obj.get("side").and_then(|v| v.as_str()).unwrap_or(""),
            );
            let last_executed_price =
                parse_f64_str_or_num(fill_obj.get("execPrice")).unwrap_or(0.0);
            let last_executed_quantity =
                parse_f64_str_or_num(fill_obj.get("execQty")).unwrap_or(0.0);
            if last_executed_quantity <= 0.0 {
                continue;
            }

            let is_maker = fill_obj
                .get("isMaker")
                .and_then(parse_boolish)
                .unwrap_or(false);

            let msg = BasicTradeLiteMsg::create(
                detect_order_venue(fill_obj),
                event_time,
                event_time,
                symbol,
                client_order_id,
                &exec_id_raw,
                side,
                is_maker,
                last_executed_price,
                last_executed_quantity,
            );

            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::TradeUpdateLite,
                BasicAccountScope::BybitUnified,
                msg.to_bytes(),
            );
            if tx.emit_with_dedup_key(
                event.to_bytes(),
                trade_lite_dedup_key(BasicAccountScope::BybitUnified, &msg),
            ) {
                count += 1;
            }
        }

        count
    }
}

impl Parser for BybitAccountEventParser {
    fn parse<S: AccountEventSink>(&self, msg: Bytes, tx: &S) -> usize {
        let Some(root) = lazy_json::root_from_bytes(&msg) else {
            return 0;
        };

        if root
            .get("op")
            .and_then(|v| {
                v.as_str()
                    .map(|op| matches!(op, "auth" | "subscribe" | "ping" | "pong"))
            })
            .unwrap_or(false)
        {
            return 0;
        }

        if let Some(success) = root.get("success").and_then(|v| v.as_bool()) {
            if !success {
                let json_str = std::str::from_utf8(msg.as_ref()).unwrap_or("<non-utf8>");
                warn!("Bybit: private ws non-success payload={}", json_str);
                return 0;
            }
        }

        let route = lazy_json::get_string(&root, &["topic"]).unwrap_or_default();

        match route.as_str() {
            "wallet" => self.parse_wallet_channel(&root, tx),
            "execution" => 0,
            "position" | "order" | "execution.fast" => {
                let json_value: Value = match serde_json::from_slice(msg.as_ref()) {
                    Ok(v) => v,
                    Err(_) => return 0,
                };
                match route.as_str() {
                    "position" => self.parse_position_channel(&json_value, tx),
                    "order" => self.parse_order_channel(&json_value, tx),
                    "execution.fast" => self.parse_execution_fast_channel(&json_value, tx),
                    _ => 0,
                }
            }
            _ => {
                let json_str = std::str::from_utf8(msg.as_ref()).unwrap_or("<non-utf8>");
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

fn collect_data_objects(json_value: &Value) -> Vec<&Map<String, Value>> {
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

fn parse_boolish(value: &Value) -> Option<bool> {
    if let Some(v) = value.as_bool() {
        return Some(v);
    }

    value
        .as_str()
        .and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" | "maker" | "m" => Some(true),
            "false" | "0" | "no" | "n" | "taker" | "t" => Some(false),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_event::test_sink::TestAccountEventSink;
    use crate::msg::basic_account_msg::{
        split_basic_account_event, BasicAccountRiskMsg, BasicBalanceMsg, BasicBorrowInterestMsg,
    };

    #[test]
    fn wallet_channel_emits_zero_borrow_when_liability_fields_are_present() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let wallet = Bytes::from_static(
            br#"{
                "topic":"wallet",
                "creationTime":1710000000999,
                "data":[
                    {
                        "updatedTime":"1710000000123",
                        "coin":[
                            {
                                "coin":"USDT",
                                "walletBalance":"1000",
                                "borrowAmount":"0",
                                "accruedInterest":"0"
                            }
                        ]
                    }
                ]
            }"#,
        );

        assert_eq!(parser.parse(wallet, &sink), 2);

        let wrapped_balance = sink.recv().expect("balance event");
        let (event_type, scope, _) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::BybitUnified);

        let wrapped_borrow = sink.recv().expect("zero borrow event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_borrow).expect("wrapped borrow");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        assert_eq!(scope, BasicAccountScope::BybitUnified);
        let msg = BasicBorrowInterestMsg::from_bytes(payload).expect("borrow payload");
        assert_eq!(msg.symbol, "USDT");
        assert_eq!(msg.borrowed, 0.0);
        assert_eq!(msg.interest, 0.0);
    }

    #[test]
    fn wallet_channel_balance_is_gross_wallet() {
        // Bybit UNIFIED 真实场景：借币卖出后 walletBalance 接近 0/为负，borrow 仍挂着。
        // BasicBalanceMsg.wallet 直接输出 walletBalance，净额由 manager 读取时计算。
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let wallet = Bytes::from_static(
            br#"{
                "topic":"wallet",
                "creationTime":1710000000999,
                "data":[
                    {
                        "updatedTime":"1710000000123",
                        "coin":[
                            {
                                "coin":"BNB",
                                "walletBalance":"-0.00035791",
                                "borrowAmount":"24.474621056148061433",
                                "accruedInterest":"0.0000466"
                            }
                        ]
                    }
                ]
            }"#,
        );

        assert_eq!(parser.parse(wallet, &sink), 2);

        let wrapped_balance = sink.recv().expect("balance event");
        let (event_type, _, payload) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        let bal = BasicBalanceMsg::from_bytes(payload).expect("balance payload");
        assert_eq!(bal.symbol, "BNB");
        assert!(
            (bal.wallet + 0.00035791).abs() < 1e-9,
            "wallet = {} 应≈ -0.00035791",
            bal.wallet
        );

        let wrapped_borrow = sink.recv().expect("borrow event");
        let (event_type, _, payload) =
            split_basic_account_event(&wrapped_borrow).expect("wrapped borrow");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        let bi = BasicBorrowInterestMsg::from_bytes(payload).expect("borrow payload");
        assert!((bi.borrowed - 24.474621056148061433).abs() < 1e-9);
        assert!((bi.interest - 0.0000466).abs() < 1e-9);
    }

    #[test]
    fn account_risk_parses_wallet_channel_account_metrics() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let wallet = Bytes::from_static(
            br#"{
                "topic":"wallet",
                "creationTime":1710000000999,
                "data":[
                    {
                        "updatedTime":"1710000000123",
                        "totalEquity":"100123.45",
                        "totalMarginBalance":"99876.50",
                        "totalInitialMargin":"8000.00",
                        "totalMaintenanceMargin":"1500.00",
                        "accountMMRate":"0.0150185459",
                        "accountIMRate":"0.0800989150",
                        "accountLTV":"0.001",
                        "coin":[
                            {
                                "coin":"USDT",
                                "walletBalance":"1000",
                                "borrowAmount":"0",
                                "accruedInterest":"0"
                            }
                        ]
                    }
                ]
            }"#,
        );

        assert_eq!(parser.parse(wallet, &sink), 3);

        let wrapped_risk = sink.recv().expect("risk event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_risk).expect("wrapped risk");
        assert_eq!(event_type, BasicAccountEventType::AccountRisk);
        assert_eq!(scope, BasicAccountScope::BybitUnified);
        let risk = BasicAccountRiskMsg::from_bytes(payload).expect("risk payload");
        assert_eq!(risk.timestamp, 1_710_000_000_123);
        assert!((risk.adj_equity_usd - 99_876.50).abs() < 1e-9);
        assert!((risk.actual_equity_usd - 100_123.45).abs() < 1e-9);
        assert!((risk.maintenance_margin_usd - 1_500.0).abs() < 1e-9);
        assert!((risk.initial_margin_usd - 8_000.0).abs() < 1e-9);
        assert!((risk.margin_ratio - (1.0 / 0.0150185459)).abs() < 1e-9);
        assert!((risk.borrowed_usd - 0.001).abs() < 1e-12);
    }

    #[test]
    fn drops_order_when_order_link_id_is_not_i64() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"abcdef","orderLinkId":"not-i64","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"New","price":"100000","qty":"0.5","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &sink), 0);
        assert!(sink.recv().is_none());
    }

    #[test]
    fn execution_topic_is_ignored() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let execution = Bytes::from_static(
            br#"{"id":"4","topic":"execution","creationTime":1710000000300,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","execType":"Trade","isMaker":true,"execPrice":"99998","execQty":"0.1","orderQty":"0.5","execTime":"1710000000003","feeCurrency":"USDT"}]}"#,
        );

        assert_eq!(parser.parse(execution, &sink), 0);
        assert!(sink.recv().is_none());
    }

    #[test]
    fn execution_fast_topic_emits_trade_update_lite() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let execution = Bytes::from_static(
            br#"{"id":"4","topic":"execution.fast","creationTime":1710000000300,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","execId":"9001","side":"Buy","execType":"Trade","isMaker":true,"execPrice":"99998","execQty":"0.1","execTime":"1710000000003"}]}"#,
        );

        assert_eq!(parser.parse(execution, &sink), 1);
        let wrapped = sink.recv().expect("trade lite event");
        let (event_type, scope, payload) = split_basic_account_event(&wrapped).expect("wrapped");
        assert_eq!(event_type, BasicAccountEventType::TradeUpdateLite);
        assert_eq!(scope, BasicAccountScope::BybitUnified);

        let msg = BasicTradeLiteMsg::from_bytes(&payload).expect("trade lite payload");
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.client_order_id, 12345);
        assert_eq!(msg.trade_id_str(), "9001");
        assert_eq!(msg.side, 1);
        assert_eq!(msg.is_maker, 1);
        assert!((msg.last_executed_price - 99998.0).abs() < 1e-9);
        assert!((msg.last_executed_quantity - 0.1).abs() < 1e-9);
        assert!(sink.recv().is_none());
    }

    #[test]
    fn execution_fast_topic_drops_non_i64_order_link_id() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let execution = Bytes::from_static(
            br#"{"topic":"execution.fast","creationTime":1716800399338,"data":[{"category":"linear","symbol":"ICPUSDT","execId":"3510f361-0add-5c7b-a2e7-9679810944fc","execPrice":"12.015","execQty":"3000","orderId":"443d63fa-b4c3-4297-b7b1-23bca88b04dc","isMaker":false,"orderLinkId":"test-00001","side":"Sell","execTime":"1716800399334","seq":34771365464}]}"#,
        );

        assert_eq!(parser.parse(execution, &sink), 0);
        assert!(sink.recv().is_none());
    }

    #[test]
    fn order_new_emits_order_update_only() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"New","price":"100000","qty":"0.5","cumExecQty":"0","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &sink), 1);
        let msg = sink.recv().expect("one event");
        let (ty, _, body) =
            crate::msg::basic_account_msg::split_basic_account_event(&msg).expect("split");
        assert_eq!(ty, BasicAccountEventType::OrderUpdate);
        let order = BybitBasicOrderMsg::from_bytes(body).expect("decode");
        assert_eq!(order.execution_type, 1);
        assert_eq!(order.order_status, 1);
        assert!(sink.recv().is_none());
    }

    #[test]
    fn order_triggered_is_ignored() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"Triggered","price":"100000","qty":"0.5","cumExecQty":"0","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &sink), 0);
        assert!(sink.recv().is_none());
    }

    #[test]
    fn order_partially_filled_canceled_emits_canceled_order_update() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let order = Bytes::from_static(
            br#"{"id":"3","topic":"order","creationTime":1710000000200,"data":[{"category":"spot","symbol":"BTCUSDT","orderId":"1001","orderLinkId":"12345","side":"Buy","orderType":"Limit","timeInForce":"GTC","orderStatus":"PartiallyFilledCanceled","price":"100000","qty":"0.5","cumExecQty":"0.1","updatedTime":"1710000000002"}]}"#,
        );

        assert_eq!(parser.parse(order, &sink), 1);
        let msg = sink.recv().expect("one event");
        let (ty, _, body) =
            crate::msg::basic_account_msg::split_basic_account_event(&msg).expect("split");
        assert_eq!(ty, BasicAccountEventType::OrderUpdate);
        let order = BybitBasicOrderMsg::from_bytes(body).expect("decode");
        assert_eq!(order.execution_type, 2);
        assert_eq!(order.order_status, 4);
        assert_eq!(order.raw_status, "PartiallyFilledCanceled");
        assert!(sink.recv().is_none());
    }

    #[test]
    fn order_topic_keeps_taker_trade_update_using_avg_price() {
        let parser = BybitAccountEventParser::new();
        let sink = TestAccountEventSink::new();

        let order = Bytes::from_static(
            br#"{
                "id":"5923240c6880ab-c59f-420b-9adb-3639adc9dd90",
                "topic":"order",
                "creationTime":1672364262474,
                "data":[
                    {
                        "symbol":"ETHUSDT",
                        "orderId":"5cf98598-39a7-459e-97bf-76ca765ee020",
                        "side":"Sell",
                        "orderType":"Market",
                        "price":"72.5",
                        "qty":"1",
                        "timeInForce":"IOC",
                        "orderStatus":"Filled",
                        "orderLinkId":"123456",
                        "cumExecQty":"1",
                        "cumExecValue":"75",
                        "avgPrice":"75",
                        "createdTime":"1672364262444",
                        "updatedTime":"1672364262457",
                        "category":"linear",
                        "isMaker":false
                    }
                ]
            }"#,
        );

        assert_eq!(parser.parse(order, &sink), 1);
        let wrapped = sink.recv().expect("order trade supplement");
        let (_, _, payload) = split_basic_account_event(&wrapped).expect("wrapped event");
        let msg = BybitBasicOrderMsg::from_bytes(payload).expect("bybit order payload");

        assert_eq!(
            msg.order_type,
            BybitBasicOrderMsg::order_type_to_u8("Market")
        );
        assert_eq!(msg.execution_type, 5);
        assert_eq!(msg.order_status, 3);
        assert_eq!(msg.is_maker, 0);
        assert!((msg.cumulative_filled_quantity - 1.0).abs() < 1e-12);
        assert!((msg.last_executed_price - 75.0).abs() < 1e-12);
    }
}
