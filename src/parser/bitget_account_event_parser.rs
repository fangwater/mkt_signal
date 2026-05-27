//! Bitget UTA 账户事件解析器（余额 / 持仓 / 订单）

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg,
    BasicUmUnrealizedMsg,
};
use crate::common::bitget_account_msg::BitgetBasicOrderMsg;
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BitgetAccountEventParser;

#[derive(Debug, Deserialize)]
struct BitgetAccountChannelRow {
    #[serde(default, rename = "uTime")]
    u_time: String,
    #[serde(default, rename = "updatedTime")]
    updated_time: String,
    #[serde(default)]
    ts: String,
    #[serde(default)]
    coin: Vec<BitgetAccountChannelCoin>,
    #[serde(default, rename = "marginCoin")]
    margin_coin: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    locked: String,
    #[serde(default)]
    equity: String,
    #[serde(default)]
    debt: String,
    #[serde(default)]
    borrow: String,
    #[serde(default)]
    debts: String,
    #[serde(default, rename = "totalEquity")]
    total_equity: String,
    #[serde(default, rename = "accountEquity")]
    account_equity: String,
    #[serde(default, rename = "effEquity")]
    eff_equity: String,
    #[serde(default)]
    mmr: String,
    #[serde(default)]
    imr: String,
    #[serde(default, rename = "mgnRatio")]
    mgn_ratio: String,
    #[serde(default, rename = "totalLiabilities")]
    total_liabilities: String,
    #[serde(default, rename = "notionalUsd")]
    notional_usd: String,
}

#[derive(Debug, Deserialize)]
struct BitgetAccountChannelCoin {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    locked: String,
    #[serde(default)]
    equity: String,
    #[serde(default)]
    debt: String,
    #[serde(default)]
    borrow: String,
    #[serde(default)]
    debts: String,
}

impl Default for BitgetAccountEventParser {
    fn default() -> Self {
        Self::new()
    }
}

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
            let Ok(account_row) =
                serde_json::from_value::<BitgetAccountChannelRow>(Value::Object(account.clone()))
            else {
                continue;
            };
            let timestamp = parse_i64_str(&account_row.u_time)
                .or_else(|| parse_i64_str(&account_row.updated_time))
                .or_else(|| parse_i64_str(&account_row.ts))
                .unwrap_or(top_timestamp);

            if !account_row.coin.is_empty() {
                for coin_item in &account_row.coin {
                    count += self.emit_account_coin(coin_item, timestamp, tx);
                }
            } else if !account_row.margin_coin.is_empty() {
                let coin_item = BitgetAccountChannelCoin {
                    coin: account_row.margin_coin.clone(),
                    balance: account_row.balance.clone(),
                    locked: account_row.locked.clone(),
                    equity: account_row.equity.clone(),
                    debt: account_row.debt.clone(),
                    borrow: account_row.borrow.clone(),
                    debts: account_row.debts.clone(),
                };
                count += self.emit_account_coin(&coin_item, timestamp, tx);
            }

            let has_risk_fields = [
                &account_row.eff_equity,
                &account_row.total_equity,
                &account_row.account_equity,
                &account_row.mmr,
                &account_row.imr,
                &account_row.mgn_ratio,
            ]
            .iter()
            .any(|value| !value.trim().is_empty());
            if has_risk_fields {
                let actual_equity_usd = parse_f64_str(&account_row.total_equity)
                    .or_else(|| parse_f64_str(&account_row.account_equity))
                    .unwrap_or(0.0);
                let adj_equity_usd =
                    parse_f64_str(&account_row.eff_equity).unwrap_or(actual_equity_usd);
                let maintenance_margin_usd = parse_f64_str(&account_row.mmr).unwrap_or(0.0);
                let margin_ratio = if maintenance_margin_usd.abs() > f64::EPSILON {
                    adj_equity_usd / maintenance_margin_usd
                } else {
                    0.0
                };
                let msg = BasicAccountRiskMsg::create(
                    timestamp,
                    adj_equity_usd,
                    actual_equity_usd,
                    maintenance_margin_usd,
                    parse_f64_str(&account_row.imr).unwrap_or(0.0),
                    margin_ratio,
                    parse_f64_str(&account_row.total_liabilities)
                        .unwrap_or(0.0)
                        .abs(),
                    parse_f64_str(&account_row.notional_usd).unwrap_or(0.0),
                );
                let event = BasicAccountEventMsg::create(
                    BasicAccountEventType::AccountRisk,
                    BasicAccountScope::BitgetUnified,
                    msg.to_bytes(),
                );
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        count
    }

    fn emit_account_coin(
        &self,
        coin_obj: &BitgetAccountChannelCoin,
        timestamp: i64,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let coin = coin_obj.coin.clone();
        if coin.is_empty() {
            return 0;
        }

        let mut sent = 0;

        let net_balance = parse_f64_str(&coin_obj.balance)
            .map(|balance| balance + parse_f64_str(&coin_obj.locked).unwrap_or(0.0))
            .or_else(|| parse_f64_str(&coin_obj.equity))
            .unwrap_or(0.0);
        let (has_liability_fields, borrowed, interest) = bitget_coin_debt(coin_obj);
        let wallet = net_balance + borrowed + interest;
        let balance_msg = BasicBalanceMsg::create(timestamp, coin.clone(), wallet);
        let payload = balance_msg.to_bytes();
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::BalanceUpdate,
            BasicAccountScope::BitgetUnified,
            payload,
        );
        if tx.send(event.to_bytes()).is_ok() {
            sent += 1;
        }

        if borrowed > 0.0 || interest > 0.0 || has_liability_fields {
            let interest_msg = BasicBorrowInterestMsg::create(timestamp, coin, borrowed, interest);
            let payload = interest_msg.to_bytes();
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::BorrowInterest,
                BasicAccountScope::BitgetUnified,
                payload,
            );
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
            let pos_event = BasicAccountEventMsg::create(
                BasicAccountEventType::PositionUpdate,
                BasicAccountScope::BitgetUnified,
                pos_payload,
            );
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
                    BasicAccountScope::BitgetUnified,
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
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::OrderUpdate,
                BasicAccountScope::BitgetUnified,
                payload,
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
    }

    /// 解析 UTA `fill` 频道（详细成交，带 feeDetail / orderType）。字段：`tradeId`。
    fn parse_fill_channel(&self, json_value: &Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("ts")).unwrap_or(0);

        for fill_obj in collect_data_objects(json_value) {
            let symbol = fill_obj
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if symbol.is_empty() {
                continue;
            }

            let client_order_id = parse_i64_str_or_num(fill_obj.get("clientOid")).unwrap_or(0);
            if client_order_id <= 0 {
                continue;
            }

            let event_time = parse_i64_str_or_num(fill_obj.get("execTime"))
                .or_else(|| parse_i64_str_or_num(fill_obj.get("updatedTime")))
                .unwrap_or(top_timestamp);
            let trade_id = fill_obj
                .get("tradeId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let side = BitgetBasicOrderMsg::side_to_u8(
                fill_obj.get("side").and_then(|v| v.as_str()).unwrap_or(""),
            );
            let last_executed_quantity =
                parse_f64_str_or_num(fill_obj.get("execQty")).unwrap_or(0.0);
            if last_executed_quantity <= 0.0 {
                continue;
            }

            let last_executed_price =
                parse_f64_str_or_num(fill_obj.get("execPrice")).unwrap_or(0.0);

            let is_maker = fill_obj
                .get("tradeScope")
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

            let venue = detect_order_venue(fill_obj);
            let msg = BasicTradeLiteMsg::create(
                venue,
                event_time,
                event_time,
                symbol,
                client_order_id,
                &trade_id,
                side,
                is_maker != 0,
                last_executed_price,
                last_executed_quantity,
            );

            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::TradeUpdateLite,
                BasicAccountScope::BitgetUnified,
                payload,
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
    }

    /// 解析 UTA `fast-fill` 频道（精简成交，仅 UTA 模式推送）。字段：`execId`。
    /// 参考: https://www.bitget.com/api-doc/uta/websocket/private/Fast-Fill-Channel
    fn parse_fast_fill_channel(
        &self,
        json_value: &Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;
        let top_timestamp = parse_i64_str_or_num(json_value.get("ts")).unwrap_or(0);

        for fill_obj in collect_data_objects(json_value) {
            let symbol = fill_obj
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if symbol.is_empty() {
                continue;
            }

            let client_order_id = parse_i64_str_or_num(fill_obj.get("clientOid")).unwrap_or(0);
            if client_order_id <= 0 {
                continue;
            }

            let exec_id = fill_obj
                .get("execId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if exec_id.is_empty() {
                continue;
            }

            let event_time = parse_i64_str_or_num(fill_obj.get("execTime"))
                .or_else(|| parse_i64_str_or_num(fill_obj.get("updatedTime")))
                .unwrap_or(top_timestamp);

            let side = BitgetBasicOrderMsg::side_to_u8(
                fill_obj.get("side").and_then(|v| v.as_str()).unwrap_or(""),
            );
            let last_executed_quantity =
                parse_f64_str_or_num(fill_obj.get("execQty")).unwrap_or(0.0);
            if last_executed_quantity <= 0.0 {
                continue;
            }

            let last_executed_price =
                parse_f64_str_or_num(fill_obj.get("execPrice")).unwrap_or(0.0);

            let is_maker = fill_obj
                .get("tradeScope")
                .and_then(|v| v.as_str())
                .map(|s| {
                    let lower = s.to_ascii_lowercase();
                    matches!(lower.as_str(), "maker" | "m")
                })
                .unwrap_or(false);

            let venue = detect_order_venue(fill_obj);
            let msg = BasicTradeLiteMsg::create(
                venue,
                event_time,
                event_time,
                symbol,
                client_order_id,
                &exec_id,
                side,
                is_maker,
                last_executed_price,
                last_executed_quantity,
            );

            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::TradeUpdateLite,
                BasicAccountScope::BitgetUnified,
                payload,
            );
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
            "fill" | "fills" => self.parse_fill_channel(&json_value, tx),
            "fast-fill" => self.parse_fast_fill_channel(&json_value, tx),
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

fn collect_data_objects(json_value: &Value) -> Vec<&Map<String, Value>> {
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

fn parse_f64_str(v: &str) -> Option<f64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

fn bitget_coin_debt(coin_obj: &BitgetAccountChannelCoin) -> (bool, f64, f64) {
    let has_liability_fields = !coin_obj.borrow.trim().is_empty()
        || !coin_obj.debt.trim().is_empty()
        || !coin_obj.debts.trim().is_empty();
    let debt_total = parse_f64_str(&coin_obj.debts)
        .or_else(|| parse_f64_str(&coin_obj.debt))
        .unwrap_or(0.0);
    if let Some(borrowed) = parse_f64_str(&coin_obj.borrow) {
        let interest = (debt_total - borrowed).max(0.0);
        (has_liability_fields, borrowed, interest)
    } else {
        (has_liability_fields, debt_total, 0.0)
    }
}

fn parse_i64_str(v: &str) -> Option<i64> {
    let s = v.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        split_basic_account_event, BasicAccountRiskMsg, BasicBalanceMsg, BasicBorrowInterestMsg,
    };
    use crate::common::bitget_account_msg::BitgetBasicOrderMsg;
    use crate::strategy::trade_update::TradeUpdate;

    #[test]
    fn account_channel_emits_zero_borrow_when_liability_fields_are_present() {
        let parser = BitgetAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let account = Bytes::from_static(
            br#"{
                "arg":{"channel":"account","instType":"UTA"},
                "ts":"1710000000999",
                "data":[
                    {
                        "uTime":"1710000000123",
                        "coin":[
                            {"coin":"USDT","balance":"1000","borrow":"0","debts":"0"}
                        ]
                    }
                ]
            }"#,
        );

        let emitted = parser.parse(account, &tx);
        assert_eq!(emitted, 2);

        let wrapped_balance = rx.try_recv().expect("balance event");
        let (event_type, scope, _) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);

        let wrapped_borrow = rx.try_recv().expect("zero borrow event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_borrow).expect("wrapped borrow");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);
        let msg = BasicBorrowInterestMsg::from_bytes(payload).expect("borrow payload");
        assert_eq!(msg.symbol, "USDT");
        assert_eq!(msg.borrowed, 0.0);
        assert_eq!(msg.interest, 0.0);
    }

    #[test]
    fn account_channel_converts_bitget_net_balance_to_gross_wallet() {
        let parser = BitgetAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let account = Bytes::from_static(
            br#"{
                "arg":{"channel":"account","instType":"UTA"},
                "ts":"1710000000999",
                "data":[
                    {
                        "uTime":"1710000000123",
                        "coin":[
                            {"coin":"SOL","equity":"-70.8","balance":"-70.8","debt":"70.8"}
                        ]
                    }
                ]
            }"#,
        );

        let emitted = parser.parse(account, &tx);
        assert_eq!(emitted, 2);

        let wrapped_balance = rx.try_recv().expect("balance event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);
        let balance = BasicBalanceMsg::from_bytes(payload).expect("balance payload");
        assert_eq!(balance.symbol, "SOL");
        assert!(balance.wallet.abs() < 1e-12);

        let wrapped_borrow = rx.try_recv().expect("borrow event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_borrow).expect("wrapped borrow");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);
        let borrow = BasicBorrowInterestMsg::from_bytes(payload).expect("borrow payload");
        assert_eq!(borrow.symbol, "SOL");
        assert!((borrow.borrowed - 70.8).abs() < 1e-12);
        assert_eq!(borrow.interest, 0.0);
        assert!((balance.wallet - borrow.borrowed - borrow.interest + 70.8).abs() < 1e-12);
    }

    #[test]
    fn account_channel_adds_locked_to_balance_wallet() {
        let parser = BitgetAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let account = Bytes::from_static(
            br#"{
                "arg":{"channel":"account","instType":"UTA"},
                "ts":"1710000000999",
                "data":[
                    {
                        "uTime":"1710000000123",
                        "coin":[
                            {"coin":"USDT","equity":"120","balance":"100","locked":"15.5","borrow":"0","debts":"0"}
                        ]
                    }
                ]
            }"#,
        );

        let emitted = parser.parse(account, &tx);
        assert_eq!(emitted, 2);

        let wrapped_balance = rx.try_recv().expect("balance event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);
        let msg = BasicBalanceMsg::from_bytes(payload).expect("balance payload");
        assert_eq!(msg.symbol, "USDT");
        assert!((msg.wallet - 115.5).abs() < 1e-12);
    }

    #[test]
    fn account_risk_parses_account_channel_top_level_metrics() {
        let parser = BitgetAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let account = Bytes::from_static(
            br#"{
                "arg":{"channel":"account","instType":"UTA"},
                "ts":"1710000000999",
                "data":[
                    {
                        "uTime":"1710000000123",
                        "totalEquity":"100123.45",
                        "effEquity":"99876.50",
                        "mmr":"1500.00",
                        "imr":"8000.00",
                        "mgnRatio":"5.23",
                        "totalLiabilities":"100.0",
                        "notionalUsd":"300000.00",
                        "coin":[
                            {"coin":"USDT","balance":"1000","borrow":"0","debts":"0"}
                        ]
                    }
                ]
            }"#,
        );

        let emitted = parser.parse(account, &tx);
        assert_eq!(emitted, 3);

        let _balance = rx.try_recv().expect("balance event");
        let _borrow = rx.try_recv().expect("borrow event");
        let wrapped_risk = rx.try_recv().expect("risk event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped_risk).expect("wrapped risk");
        assert_eq!(event_type, BasicAccountEventType::AccountRisk);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);
        let risk = BasicAccountRiskMsg::from_bytes(payload).expect("risk payload");
        assert_eq!(risk.timestamp, 1_710_000_000_123);
        assert!((risk.adj_equity_usd - 99_876.50).abs() < 1e-9);
        assert!((risk.actual_equity_usd - 100_123.45).abs() < 1e-9);
        assert!((risk.maintenance_margin_usd - 1_500.0).abs() < 1e-9);
        assert!((risk.initial_margin_usd - 8_000.0).abs() < 1e-9);
        assert!((risk.margin_ratio - (99_876.50 / 1_500.0)).abs() < 1e-12);
        assert!((risk.borrowed_usd - 100.0).abs() < 1e-12);
        assert!((risk.notional_usd - 300_000.0).abs() < 1e-9);
    }

    #[test]
    fn fill_channel_emits_trade_update_lite_event() {
        let parser = BitgetAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let fill = Bytes::from_static(
            br#"{
                "arg":{"channel":"fill","instType":"USDT-FUTURES"},
                "ts":"1710000000999",
                "data":[
                    {
                        "category":"USDT-FUTURES",
                        "symbol":"BTCUSDT",
                        "orderId":"998877",
                        "clientOid":"123456",
                        "tradeId":"556677",
                        "side":"buy",
                        "orderType":"limit",
                        "tradeScope":"maker",
                        "execQty":"0.002",
                        "execPrice":"64000.5",
                        "execTime":"1710000000123",
                        "feeDetail":[{"feeCoin":"USDT"}]
                    }
                ]
            }"#,
        );

        let emitted = parser.parse(fill, &tx);
        assert_eq!(emitted, 1);

        let wrapped = rx.try_recv().expect("trade lite event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped).expect("wrapped trade lite");
        assert_eq!(event_type, BasicAccountEventType::TradeUpdateLite);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);

        let msg = BasicTradeLiteMsg::from_bytes(payload).expect("trade lite payload");
        assert_eq!(msg.venue, BitgetBasicOrderMsg::VENUE_FUTURES);
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.client_order_id, 123456);
        assert_eq!(msg.trade_id_str(), "556677");
        assert_eq!(msg.side, 1);
        assert_eq!(msg.is_maker, 1);
        assert!((msg.last_executed_price - 64000.5).abs() < 1e-9);
        assert!((msg.last_executed_quantity - 0.002).abs() < 1e-9);
    }

    #[test]
    fn order_channel_emits_trade_like_update_for_taker_market_fill_using_avg_price() {
        let parser = BitgetAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let order = Bytes::from_static(
            br#"{
                "action":"snapshot",
                "arg":{"instType":"UTA","topic":"order"},
                "ts":1742367838124,
                "data":[
                    {
                        "category":"usdt-futures",
                        "symbol":"BTCUSDT",
                        "orderId":"998877",
                        "clientOid":"123456",
                        "price":"",
                        "qty":"0.001",
                        "orderType":"market",
                        "timeInForce":"gtc",
                        "side":"buy",
                        "cumExecQty":"0.001",
                        "cumExecValue":"83.1315",
                        "avgPrice":"83131.5",
                        "orderStatus":"filled",
                        "feeDetail":[{"feeCoin":"USDT","fee":"0.0332526"}],
                        "createdTime":"1742367838101",
                        "updatedTime":"1742367838115"
                    }
                ]
            }"#,
        );

        let emitted = parser.parse(order, &tx);
        assert_eq!(emitted, 1);

        let wrapped = rx.try_recv().expect("order event");
        let (event_type, scope, payload) =
            split_basic_account_event(&wrapped).expect("wrapped order event");
        assert_eq!(event_type, BasicAccountEventType::OrderUpdate);
        assert_eq!(scope, BasicAccountScope::BitgetUnified);

        let msg = BitgetBasicOrderMsg::from_bytes(payload).expect("bitget order payload");
        assert_eq!(msg.venue, BitgetBasicOrderMsg::VENUE_FUTURES);
        assert_eq!(msg.symbol, "BTCUSDT");
        assert_eq!(msg.order_id, 998877);
        assert_eq!(msg.client_order_id, 123456);
        assert_eq!(msg.order_type, 3);
        assert_eq!(msg.execution_type, 5);
        assert_eq!(msg.order_status, 3);
        assert_eq!(msg.is_maker, 0);
        assert!((msg.cumulative_filled_quantity - 0.001).abs() < 1e-12);
        assert!((msg.price - 83131.5).abs() < 1e-9);
        assert!((msg.last_executed_price - 83131.5).abs() < 1e-9);
        assert!((TradeUpdate::price(&msg) - 83131.5).abs() < 1e-9);
    }
}
