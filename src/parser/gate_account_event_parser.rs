//! Gate.io 账户事件解析器（余额 / 借贷 / 订单）
//!
//! 解析 Gate.io 统一账户的消息：
//! - `unified.asset_detail`: 资产详情 -> BasicBalanceMsg + BasicBorrowInterestMsg
//! - `spot.orders_v2`: 现货订单更新 -> GateBasicOrderMsg
//! - `futures.orders`: 合约订单更新 -> GateBasicOrderMsg
//!
//! ## unified.asset_detail 消息格式:
//! ```json
//! {
//!     "time": 1716796362,
//!     "time_ms": 1716796362915,
//!     "channel": "unified.asset_detail",
//!     "event": "update",
//!     "result": {
//!         "u": 11027732,       // user_id
//!         "t": 1716796364,     // refresh_time (秒)
//!         "dts": {             // 资产详情 map
//!             "BTC": {
//!                 "tl": "0.00",           // total_liab (总借款)
//!                 "b": "1086390.949548"   // balance (余额)
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## spot.orders_v2 消息格式:
//! ```json
//! {
//!     "time": 1694655225,
//!     "time_ms": 1694655225315,
//!     "channel": "spot.orders_v2",
//!     "event": "update",
//!     "result": [{
//!         "id": "399123456",
//!         "text": "t-testtext",  // client_order_id，必须为 i64
//!         "currency_pair": "BTC_USDT",
//!         "type": "limit",
//!         "side": "sell",
//!         "amount": "0.0001",
//!         "price": "26253.3",
//!         "time_in_force": "gtc",
//!         "filled_amount": "812.8",
//!         "avg_deal_price": "0",
//!         "fee_currency": "USDT",
//!         "update_time_ms": "1694655225315",
//!         "event": "put",
//!         "finish_as": "open"
//!     }]
//! }
//! ```
//!
//! ## futures.orders 消息格式:
//! ```json
//! {
//!     "channel": "futures.orders",
//!     "event": "update",
//!     "time": 1541505434,
//!     "time_ms": 1541505434123,
//!     "result": [{
//!         "contract": "BTC_USD",
//!         "id": 4872460,
//!         "text": "123456789",           // client_order_id，必须为 i64
//!         "size": "1",                   // 正=做多, 负=做空
//!         "left": "0",                   // 剩余未成交
//!         "price": "40000.4",
//!         "fill_price": 40000.4,         // 平均成交价
//!         "tif": "gtc",
//!         "status": "finished",          // open / finished
//!         "finish_as": "filled",         // filled/cancelled/liquidated/ioc/auto_deleveraging/reduce_only/position_close/stp/reduce_out
//!         "update_time": 1541505434123   // 已经是 ms
//!     }]
//! }
//! ```

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicBalanceMsg, BasicBorrowInterestMsg,
    GateBasicOrderMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct GateAccountEventParser;

impl GateAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    /// 解析统一账户资产详情
    fn parse_unified_asset_detail(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        // 获取 result
        let Some(result) = json_value.get("result") else {
            return 0;
        };

        // 获取时间戳 (秒 -> 毫秒)
        let timestamp = result
            .get("t")
            .and_then(|v| v.as_i64())
            .map(|t| t * 1000)  // 转换为毫秒
            .unwrap_or_else(|| {
                // 备用: 使用外层的 time_ms
                json_value
                    .get("time_ms")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
            });

        // 获取资产详情 map
        let Some(dts) = result.get("dts").and_then(|v| v.as_object()) else {
            return 0;
        };

        // 遍历每个币种
        for (symbol, details) in dts {
            // 解析余额 (b)
            if let Some(balance_str) = details.get("b").and_then(|v| v.as_str()) {
                if let Ok(balance) = balance_str.parse::<f64>() {
                    let msg = BasicBalanceMsg::create(timestamp, symbol.clone(), balance);
                    let payload = msg.to_bytes();
                    let event = BasicAccountEventMsg::create(BasicAccountEventType::BalanceUpdate, payload);
                    if tx.send(event.to_bytes()).is_ok() {
                        count += 1;
                    }
                }
            }

            // 解析借款 (tl) -> BasicBorrowInterestMsg (interest 设为 0)
            if let Some(total_liab_str) = details.get("tl").and_then(|v| v.as_str()) {
                if let Ok(borrowed) = total_liab_str.parse::<f64>() {
                    // 只有当借款金额 > 0 时才发送消息
                    if borrowed > 0.0 {
                        let msg = BasicBorrowInterestMsg::create(
                            timestamp,
                            symbol.clone(),
                            borrowed,
                            0.0,  // Gate.io 不在此消息中提供利息，设为 0
                        );
                        let payload = msg.to_bytes();
                        let event = BasicAccountEventMsg::create(BasicAccountEventType::BorrowInterest, payload);
                        if tx.send(event.to_bytes()).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }

        count
    }

    /// 解析现货订单更新 (spot.orders_v2)
    fn parse_spot_orders_v2(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        // 获取 result 数组
        let Some(result) = json_value.get("result").and_then(|v| v.as_array()) else {
            return 0;
        };

        for order in result {
            // 解析 client_order_id (text 字段，必须是 i64)
            let text = order.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let client_order_id: i64 = match text.parse() {
                Ok(id) => id,
                Err(_) => {
                    // 打印 JSON 并跳过
                    warn!("Gate: spot.orders_v2 text is not i64, dropping: {}", order);
                    continue;
                }
            };

            // 解析其他字段
            let order_id: i64 = order
                .get("id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let symbol = order
                .get("currency_pair")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let side = GateBasicOrderMsg::side_to_u8(
                order.get("side").and_then(|v| v.as_str()).unwrap_or("")
            );

            let order_type = GateBasicOrderMsg::order_type_to_u8(
                order.get("type").and_then(|v| v.as_str()).unwrap_or("")
            );

            let time_in_force = GateBasicOrderMsg::time_in_force_to_u8(
                order.get("time_in_force").and_then(|v| v.as_str()).unwrap_or("")
            );

            let event = order.get("event").and_then(|v| v.as_str()).unwrap_or("");
            let finish_as = order.get("finish_as").and_then(|v| v.as_str()).unwrap_or("");
            let (execution_type, order_status) =
                GateBasicOrderMsg::event_to_execution_and_status(event, finish_as);

            let price: f64 = order
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let quantity: f64 = order
                .get("amount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let cumulative_filled_quantity: f64 = order
                .get("filled_amount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let commission_asset = order
                .get("fee_currency")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let event_time: i64 = order
                .get("update_time_ms")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            // 创建消息
            let msg = GateBasicOrderMsg::create(
                GateBasicOrderMsg::VENUE_SPOT,
                event_time,
                symbol,
                order_id,
                client_order_id,
                side,
                order_type,
                time_in_force,
                execution_type,
                order_status,
                price,
                quantity,
                cumulative_filled_quantity,
                commission_asset,
            );

            let payload = msg.to_bytes();
            let event_msg = BasicAccountEventMsg::create(BasicAccountEventType::OrderUpdate, payload);
            if tx.send(event_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
    }

    /// 解析合约订单更新 (futures.orders)
    ///
    /// 字段映射：
    /// - contract -> symbol
    /// - size -> quantity (正=做多, 负=做空)
    /// - size - left -> cumulative_filled_quantity
    /// - tif -> time_in_force
    /// - update_time -> event_time (已经是 ms)
    /// - status -> 用于判断执行类型 (open/finished)
    fn parse_futures_orders(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        // 获取 result 数组
        let Some(result) = json_value.get("result").and_then(|v| v.as_array()) else {
            return 0;
        };

        for order in result {
            // 解析 client_order_id (text 字段，必须是 i64)
            let text = order.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let client_order_id: i64 = match text.parse() {
                Ok(id) => id,
                Err(_) => {
                    // 打印 JSON 并跳过
                    warn!("Gate: futures.orders text is not i64, dropping: {}", order);
                    continue;
                }
            };

            // 解析 order_id
            let order_id: i64 = order
                .get("id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // 解析 symbol (contract)
            let symbol = order
                .get("contract")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // 解析 side: futures 没有直接的 side 字段，需要根据 size 正负判断
            // size > 0 为 buy (做多), size < 0 为 sell (做空)
            let size_str = order.get("size").and_then(|v| v.as_str()).unwrap_or("0");
            let size: i64 = size_str.parse().unwrap_or(0);
            let side: u8 = if size >= 0 { 1 } else { 2 }; // 1=Buy, 2=Sell
            let quantity = size.abs() as f64;

            // 解析 left (剩余未成交数量)
            let left_str = order.get("left").and_then(|v| v.as_str()).unwrap_or("0");
            let left: i64 = left_str.parse().unwrap_or(0);
            let cumulative_filled_quantity = (size.abs() - left.abs()) as f64;

            // 解析 order_type: futures 默认是 limit
            // 可以通过 price 是否为 0 判断 market 单
            let price: f64 = order
                .get("price")
                .and_then(|v| v.as_str().or_else(|| v.as_f64().map(|f| f.to_string()).as_deref().map(|_| "")))
                .and_then(|s| s.parse().ok())
                .or_else(|| order.get("price").and_then(|v| v.as_f64()))
                .unwrap_or(0.0);
            let order_type: u8 = if price == 0.0 { 3 } else { 1 }; // 3=Market, 1=Limit

            // 解析 time_in_force
            let time_in_force = GateBasicOrderMsg::time_in_force_to_u8(
                order.get("tif").and_then(|v| v.as_str()).unwrap_or("gtc")
            );

            // 解析 status 和 finish_as
            let status = order.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let finish_as = order.get("finish_as").and_then(|v| v.as_str()).unwrap_or("");
            let (execution_type, order_status) =
                GateBasicOrderMsg::event_to_execution_and_status(status, finish_as);

            // 解析 event_time (update_time 已经是 ms)
            let event_time: i64 = order
                .get("update_time")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            // 手续费币种: futures 合约的手续费币种通常是 USDT
            // Gate 合约不在订单消息中提供手续费币种，这里留空
            let commission_asset = String::new();

            // 创建消息
            let msg = GateBasicOrderMsg::create(
                GateBasicOrderMsg::VENUE_FUTURES,
                event_time,
                symbol,
                order_id,
                client_order_id,
                side,
                order_type,
                time_in_force,
                execution_type,
                order_status,
                price,
                quantity,
                cumulative_filled_quantity,
                commission_asset,
            );

            let payload = msg.to_bytes();
            let event_msg = BasicAccountEventMsg::create(BasicAccountEventType::OrderUpdate, payload);
            if tx.send(event_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }

        count
    }
}

impl Parser for GateAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let json_value: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return 0,
        };

        // 获取频道
        let channel = json_value
            .get("channel")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // 获取事件类型
        let event = json_value
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match channel {
            "unified.asset_detail" => {
                if event == "update" {
                    self.parse_unified_asset_detail(&json_value, tx)
                } else {
                    debug!("Gate: unified.asset_detail event={} (ignored)", event);
                    0
                }
            }
            "spot.orders_v2" => {
                if event == "update" {
                    self.parse_spot_orders_v2(&json_value, tx)
                } else {
                    debug!("Gate: spot.orders_v2 event={} (ignored)", event);
                    0
                }
            }
            "futures.orders" => {
                if event == "update" {
                    self.parse_futures_orders(&json_value, tx)
                } else {
                    debug!("Gate: futures.orders event={} (ignored)", event);
                    0
                }
            }
            "unified.pong" | "spot.pong" | "futures.pong" => {
                // pong 响应，忽略
                0
            }
            _ => {
                if json_value.get("event").is_some() {
                    debug!("Gate: event message: {}", json_str);
                } else if !channel.is_empty() {
                    warn!("Gate: Unknown channel: {}", channel);
                }
                0
            }
        }
    }
}

