//! Bitget UTA 账户事件解析器（余额 / 持仓 / 订单）

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    BasicPositionMsg, BasicUmUnrealizedMsg,
};
use crate::common::bitget_account_msg::BitgetBasicOrderMsg;
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BitgetAccountEventParser;

impl BitgetAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_account_channel(
        &self,
        json_value: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        let top_timestamp = parse_i64_str_or_num(json_value.get("ts"))
            .or_else(|| parse_i64_str_or_num(json_value.get("timestamp")))
            .unwrap_or(0);

        for account in collect_data_objects(json_value) {
            let timestamp = parse_i64_str_or_num(account.get("uTime"))
                .or_else(|| parse_i64_str_or_num(account.get("updatedTime")))
                .or_else(|| parse_i64_str_or_num(account.get("ts")))
                .unwrap_or(top_timestamp);

            if let Some(coins) = account.get("coin").and_then(|v| v.as_array()) {
                for coin_item in coins {
                    let Some(coin_obj) = coin_item.as_object() else {
                        continue;
                    };
                    count += self.emit_account_coin(coin_obj, timestamp, tx);
                }
            } else {
                count += self.emit_account_coin(account, timestamp, tx);
            }
        }

        count
    }

    fn emit_account_coin(
        &self,
        coin_obj: &Map<String, Value>,
        timestamp: i64,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let coin = coin_obj
            .get("coin")
            .or_else(|| coin_obj.get("marginCoin"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if coin.is_empty() {
            return 0;
        }

        let mut sent = 0;

        let balance = parse_f64_str_or_num(coin_obj.get("available"))
            .or_else(|| parse_f64_str_or_num(coin_obj.get("equity")))
            .or_else(|| parse_f64_str_or_num(coin_obj.get("balance")))
            .unwrap_or(0.0);
        let balance_msg = BasicBalanceMsg::create(timestamp, coin.clone(), balance);
        let payload = balance_msg.to_bytes();
        let event = BasicAccountEventMsg::create(BasicAccountEventType::BalanceUpdate, payload);
        if tx.send(event.to_bytes()).is_ok() {
            sent += 1;
        }

        let borrowed = parse_f64_str_or_num(coin_obj.get("borrowAmount"))
            .or_else(|| parse_f64_str_or_num(coin_obj.get("liability")))
            .unwrap_or(0.0);
        let interest = parse_f64_str_or_num(coin_obj.get("interest"))
            .or_else(|| parse_f64_str_or_num(coin_obj.get("accruedInterest")))
            .unwrap_or(0.0);
        if borrowed > 0.0 || interest > 0.0 {
            let interest_msg = BasicBorrowInterestMsg::create(timestamp, coin, borrowed, interest);
            let payload = interest_msg.to_bytes();
            let event =
                BasicAccountEventMsg::create(BasicAccountEventType::BorrowInterest, payload);
            if tx.send(event.to_bytes()).is_ok() {
                sent += 1;
            }
        }

        sent
    }

    fn parse_positions_channel(
        &self,
        json_value: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        let top_timestamp = parse_i64_str_or_num(json_value.get("ts")).unwrap_or(0);

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

            let side = obj
                .get("posSide")
                .or_else(|| obj.get("holdSide"))
                .and_then(|v| v.as_str())
                .unwrap_or("net");
            let position_side = match side.to_ascii_lowercase().as_str() {
                "long" => 'L',
                "short" => 'S',
                _ => 'N',
            };

            let position_amount = parse_f64_str_or_num(obj.get("size"))
                .or_else(|| parse_f64_str_or_num(obj.get("total")))
                .or_else(|| parse_f64_str_or_num(obj.get("pos")))
                .unwrap_or(0.0) as f32;

            let timestamp = parse_i64_str_or_num(obj.get("updatedTime"))
                .or_else(|| parse_i64_str_or_num(obj.get("uTime")))
                .or_else(|| parse_i64_str_or_num(obj.get("ts")))
                .unwrap_or(top_timestamp);

            let position_msg = BasicPositionMsg::create(
                timestamp,
                inst_id.clone(),
                position_side,
                position_amount,
            );
            let pos_payload = position_msg.to_bytes();
            let pos_event =
                BasicAccountEventMsg::create(BasicAccountEventType::PositionUpdate, pos_payload);
            if tx.send(pos_event.to_bytes()).is_ok() {
                count += 1;
            }

            if let Some(pnl) = parse_f64_str_or_num(obj.get("unrealisedPnl"))
                .or_else(|| parse_f64_str_or_num(obj.get("unrealizedPnl")))
                .or_else(|| parse_f64_str_or_num(obj.get("upl")))
            {
                let pnl_msg = BasicUmUnrealizedMsg::create(timestamp, inst_id, position_side, pnl);
                let pnl_payload = pnl_msg.to_bytes();
                let pnl_event = BasicAccountEventMsg::create(
                    BasicAccountEventType::UnrealizedPnlUpdate,
                    pnl_payload,
                );
                if tx.send(pnl_event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        count
    }

    fn parse_orders_channel(&self, json_value: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let mut count = 0;

        let top_timestamp = parse_i64_str_or_num(json_value.get("ts")).unwrap_or(0);

        for order_obj in collect_data_objects(json_value) {
            let symbol = order_obj
                .get("symbol")
                .or_else(|| order_obj.get("instId"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if symbol.is_empty() {
                continue;
            }

            let order_id = parse_i64_str_or_num(order_obj.get("orderId"))
                .or_else(|| parse_i64_str_or_num(order_obj.get("ordId")))
                .unwrap_or(0);
            let client_order_id = parse_i64_str_or_num(order_obj.get("clientOid"))
                .or_else(|| parse_i64_str_or_num(order_obj.get("clOrdId")))
                .unwrap_or(0);
            let event_time = parse_i64_str_or_num(order_obj.get("updatedTime"))
                .or_else(|| parse_i64_str_or_num(order_obj.get("uTime")))
                .or_else(|| parse_i64_str_or_num(order_obj.get("fillTime")))
                .or_else(|| parse_i64_str_or_num(order_obj.get("createTime")))
                .or_else(|| parse_i64_str_or_num(order_obj.get("cTime")))
                .or_else(|| parse_i64_str_or_num(order_obj.get("ts")))
                .unwrap_or(top_timestamp);

            let side = BitgetBasicOrderMsg::side_to_u8(
                order_obj.get("side").and_then(|v| v.as_str()).unwrap_or(""),
            );
            let order_type = BitgetBasicOrderMsg::order_type_to_u8(
                order_obj
                    .get("orderType")
                    .or_else(|| order_obj.get("ordType"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            );
            let time_in_force = BitgetBasicOrderMsg::time_in_force_to_u8(
                order_obj
                    .get("timeInForce")
                    .or_else(|| order_obj.get("force"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("gtc"),
            );

            let status = order_obj
                .get("orderStatus")
                .or_else(|| order_obj.get("status"))
                .and_then(|v| v.as_str())
                .unwrap_or("live");
            let execution_type = BitgetBasicOrderMsg::status_to_execution_type(status);
            let order_status = BitgetBasicOrderMsg::status_to_order_status(status);

            let is_maker = order_obj
                .get("tradeScope")
                .or_else(|| order_obj.get("liquidity"))
                .and_then(|v| v.as_str())
                .map(|s| {
                    let lower = s.to_ascii_lowercase();
                    if lower == "maker" || lower == "m" {
                        1
                    } else {
                        0
                    }
                })
                .unwrap_or(0);

            let order_price = parse_f64_str_or_num(order_obj.get("price"))
                .or_else(|| parse_f64_str_or_num(order_obj.get("px")))
                .unwrap_or(0.0);
            let avg_price = parse_f64_str_or_num(order_obj.get("avgPrice"))
                .or_else(|| parse_f64_str_or_num(order_obj.get("fillPx")))
                .or_else(|| parse_f64_str_or_num(order_obj.get("fillPrice")));
            let price = if execution_type == 5 {
                avg_price.unwrap_or(order_price)
            } else {
                order_price
            };

            let quantity = parse_f64_str_or_num(order_obj.get("qty"))
                .or_else(|| parse_f64_str_or_num(order_obj.get("sz")))
                .or_else(|| parse_f64_str_or_num(order_obj.get("baseVolume")))
                .unwrap_or(0.0);
            let cumulative_filled_quantity = parse_f64_str_or_num(order_obj.get("cumExecQty"))
                .or_else(|| parse_f64_str_or_num(order_obj.get("accFillSz")))
                .or_else(|| parse_f64_str_or_num(order_obj.get("filledBaseVolume")))
                .unwrap_or(0.0);
            let last_executed_price = if execution_type == 5 {
                avg_price.unwrap_or(order_price)
            } else {
                0.0
            };

            let commission_asset = order_obj
                .get("feeDetail")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.get("feeCoin"))
                .and_then(|v| v.as_str())
                .or_else(|| order_obj.get("feeCcy").and_then(|v| v.as_str()))
                .unwrap_or("")
                .to_string();

            let venue = detect_order_venue(order_obj);

            let msg = BitgetBasicOrderMsg::create(
                venue,
                event_time,
                symbol,
                order_id,
                client_order_id,
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
            );

            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(BasicAccountEventType::OrderUpdate, payload);
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
    }
}

impl Parser for BitgetAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let json_value: Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return 0,
        };

        if let Some(event) = json_value.get("event").and_then(|v| v.as_str()) {
            if matches!(event, "login" | "subscribe") {
                return 0;
            }
            if event == "error" {
                warn!("Bitget: private ws error payload={}", json_str);
                return 0;
            }
        }

        let route = json_value
            .get("arg")
            .and_then(|v| v.as_object())
            .and_then(|arg| {
                arg.get("topic")
                    .and_then(|v| v.as_str())
                    .or_else(|| arg.get("channel").and_then(|v| v.as_str()))
            })
            .or_else(|| json_value.get("topic").and_then(|v| v.as_str()))
            .unwrap_or("");

        match route {
            "account" => self.parse_account_channel(&json_value, tx),
            "position" | "positions" => self.parse_positions_channel(&json_value, tx),
            "order" | "orders" => self.parse_orders_channel(&json_value, tx),
            _ => {
                if !route.is_empty() {
                    debug!("Bitget: ignored route={} payload={}", route, json_str);
                } else {
                    warn!("Bitget: unknown private payload={}", json_str);
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
    if let Some(category) = order_obj.get("category").and_then(|v| v.as_str()) {
        let category = category.to_ascii_lowercase();
        if category.contains("future") || category.contains("swap") || category.contains("perp") {
            return BitgetBasicOrderMsg::VENUE_FUTURES;
        }
    }

    if let Some(inst_type) = order_obj.get("instType").and_then(|v| v.as_str()) {
        let inst_type = inst_type.to_ascii_lowercase();
        if inst_type.contains("future") || inst_type.contains("swap") {
            return BitgetBasicOrderMsg::VENUE_FUTURES;
        }
    }

    BitgetBasicOrderMsg::VENUE_SPOT
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
