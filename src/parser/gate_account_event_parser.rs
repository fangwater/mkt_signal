//! Gate.io 账户事件解析器（余额 / 借贷 / 订单）
//!
//! 解析 Gate.io 统一账户的消息：
//! - `unified.asset_detail`: 资产详情 -> BasicBalanceMsg + BasicBorrowInterestMsg
//! - `spot.orders`/`spot.orders_v2`: 现货订单更新 -> GateBasicOrderMsg
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
//!                 "b": "1086390.949548",  // raw balance (原始余额)
//!                 "e": "1086389.949548"   // equity (统一账户净资产数量)
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## spot.orders_v2 / spot.orders 消息格式:
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
//!         "size": "1",                   // 合约张数(contracts)，正=做多, 负=做空
//!         "left": "0",                   // 剩余未成交张数(contracts)
//!         "price": "40000.4",
//!         "fill_price": 40000.4,         // 平均成交价
//!         "tif": "gtc",
//!         "status": "finished",          // open / finished
//!         "finish_as": "filled",         // filled/cancelled/liquidated/ioc/auto_deleveraging/reduce_only/position_close/stp/reduce_out
//!         "update_time": 1541505434123   // 已经是 ms
//!     }]
//! }
//! ```
//!
//! ## 现货/合约字段差异（便于后续统一）
//! - 现货 spot.orders_v2 独有字段：
//!   - `currency_pair`（交易对）
//!   - `type`（订单类型）
//!   - `side`（方向）
//!   - `amount`（委托数量，现货为币数量）
//!   - `time_in_force`
//!   - `filled_amount`（累计成交量）
//!   - `avg_deal_price`（均价）
//!   - `fee_currency`（手续费币种）
//!   - `update_time_ms`（字符串 ms）
//!   - `event`（put/update/finish）
//! - 合约 futures.orders 独有字段：
//!   - `contract`（交易对/合约名）
//!   - `size`（合约张数 contracts，正负表示方向；与现货 amount 语义不同）
//!   - `left`（剩余未成交 contracts）
//!   - `fill_price`（均价）
//!   - `tif`
//!   - `status`（open/finished）
//!   - `update_time`（i64 ms）
//! - 两边共有/相近字段：
//!   - `id`（order_id）
//!   - `text`（client_order_id，建议使用纯数字或以数字结尾，如 `t-123456`）
//!   - `price`（委托价；市价单常为 0）
//!   - `finish_as`（终态原因，用于映射 execution_type / order_status）

use crate::common::basic_account_msg::{
    BasicAccountEventMsg, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg,
    BasicUmUnrealizedMsg, GateBasicOrderMsg,
};
use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, warn};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct GateAccountEventParser;

#[derive(Debug, Clone, Copy)]
pub struct GateParseReport {
    pub emitted: usize,
    pub complete: bool,
}

impl GateParseReport {
    fn complete(emitted: usize) -> Self {
        Self {
            emitted,
            complete: true,
        }
    }

    fn incomplete(emitted: usize) -> Self {
        Self {
            emitted,
            complete: false,
        }
    }
}

impl Default for GateAccountEventParser {
    fn default() -> Self {
        Self::new()
    }
}

impl GateAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_gate_client_order_id(text: &str) -> Option<i64> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        if let Ok(id) = text.parse::<i64>() {
            return Some(id);
        }
        if let Some(rest) = text.strip_prefix("t-") {
            if let Ok(id) = rest.parse::<i64>() {
                return Some(id);
            }
        }
        None
    }

    /// 解析统一账户资产详情
    fn parse_unified_asset_detail(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let mut count = 0;
        let mut incomplete = false;

        // 获取 result
        let Some(result) = json_value.get("result") else {
            warn!("Gate: unified.asset_detail missing result");
            return GateParseReport::incomplete(0);
        };

        let Some(refresh_time_s) = parse_i64_str_or_num(result.get("t")) else {
            warn!("Gate: unified.asset_detail missing timestamp");
            return GateParseReport::incomplete(0);
        };
        let timestamp = refresh_time_s.saturating_mul(1_000);

        // 获取资产详情 map
        let Some(dts) = result.get("dts").and_then(|v| v.as_object()) else {
            warn!("Gate: unified.asset_detail missing dts");
            return GateParseReport::incomplete(0);
        };

        if dts.is_empty() {
            return GateParseReport::complete(0);
        }

        // 遍历每个币种
        for (symbol, details) in dts {
            let symbol = symbol.trim();
            if symbol.is_empty() {
                warn!("Gate: unified.asset_detail empty symbol");
                incomplete = true;
                continue;
            }

            let total_liab = match details.get("tl") {
                Some(raw_total_liab) => match parse_f64_str_or_num(Some(raw_total_liab)) {
                    Some(v) => Some(v),
                    None => {
                        warn!(
                            "Gate: unified.asset_detail invalid tl for symbol={}",
                            symbol
                        );
                        incomplete = true;
                        None
                    }
                },
                None => None,
            };

            // 下游以 gross wallet - borrowed - interest 计算净头寸；Gate unified 的
            // raw balance(b) 不是权威净值口径，必须用 e + tl 生成 gross wallet。
            let Some(equity) = parse_f64_str_or_num(details.get("e")) else {
                warn!(
                    "Gate: unified.asset_detail missing/invalid equity for symbol={}",
                    symbol
                );
                incomplete = true;
                continue;
            };
            let wallet = equity + total_liab.unwrap_or(0.0);

            let msg = BasicBalanceMsg::create(timestamp, symbol.to_string(), wallet);
            let payload = msg.to_bytes();
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::BalanceUpdate,
                BasicAccountScope::GateUnified,
                payload,
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }

            // 解析借款 (tl) -> BasicBorrowInterestMsg (interest 设为 0)。字段存在时即使为 0
            // 也发送，用来清理 manager 中已归零的旧负债。
            if let Some(borrowed) = total_liab {
                let msg = BasicBorrowInterestMsg::create(
                    timestamp,
                    symbol.to_string(),
                    borrowed,
                    0.0, // Gate.io 不在此消息中提供利息，设为 0
                );
                let payload = msg.to_bytes();
                let event = BasicAccountEventMsg::create(
                    BasicAccountEventType::BorrowInterest,
                    BasicAccountScope::GateUnified,
                    payload,
                );
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        if incomplete {
            GateParseReport::incomplete(count)
        } else {
            GateParseReport::complete(count)
        }
    }

    /// 解析 unified.assets 账户级聚合风险。
    fn parse_unified_assets(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let Some(raw_result) = json_value.get("result") else {
            warn!("Gate: unified.assets missing result");
            return GateParseReport::incomplete(0);
        };

        let result = raw_result
            .as_array()
            .and_then(|arr| arr.first())
            .unwrap_or(raw_result);
        let timestamp_s = parse_i64_str_or_num(result.get("t"))
            .or_else(|| parse_i64_str_or_num(json_value.get("time")))
            .unwrap_or(0);
        let timestamp = timestamp_s.saturating_mul(1_000);

        let initial_rate_pct = parse_f64_str_or_num(result.get("r")).unwrap_or(0.0);
        let maintenance_rate_pct = parse_f64_str_or_num(result.get("R")).unwrap_or(0.0);
        let margin_balance = parse_f64_str_or_num(result.get("b")).unwrap_or(0.0).abs();
        let equity = parse_f64_str_or_num(result.get("e")).unwrap_or(0.0);
        let liabilities = parse_f64_str_or_num(result.get("l")).unwrap_or(0.0).abs();

        let margin_ratio = maintenance_rate_pct / 100.0;
        let maintenance_margin_usd = if maintenance_rate_pct.abs() > f64::EPSILON {
            margin_balance / (maintenance_rate_pct / 100.0)
        } else {
            0.0
        };
        let initial_margin_usd = if initial_rate_pct.abs() > f64::EPSILON {
            margin_balance / (initial_rate_pct / 100.0)
        } else {
            0.0
        };

        let msg = BasicAccountRiskMsg::create(
            timestamp,
            equity,
            equity,
            maintenance_margin_usd,
            initial_margin_usd,
            margin_ratio,
            liabilities,
            0.0,
        );
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::AccountRisk,
            BasicAccountScope::GateUnified,
            msg.to_bytes(),
        );
        if tx.send(event.to_bytes()).is_ok() {
            GateParseReport::complete(1)
        } else {
            GateParseReport::incomplete(0)
        }
    }

    /// 解析现货订单更新 (spot.orders_v2 / spot.orders)
    fn parse_spot_orders_v2(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let mut count = 0;
        let mut incomplete = false;

        // 获取 result 数组
        let Some(result) = json_value.get("result").and_then(|v| v.as_array()) else {
            warn!("Gate: spot.orders missing result array");
            return GateParseReport::incomplete(0);
        };

        if result.is_empty() {
            return GateParseReport::complete(0);
        }

        for order in result {
            // 解析 client_order_id (text 字段，必须是 i64)
            let text = order.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let client_order_id = match Self::parse_gate_client_order_id(text) {
                Some(id) => id,
                None => {
                    warn!("Gate: spot.orders_v2 text is not i64, dropping: {}", order);
                    incomplete = true;
                    continue;
                }
            };

            // 解析其他字段（兼容 string/number）
            let Some(order_id) = parse_i64_str_or_num(order.get("id")) else {
                warn!(
                    "Gate: spot.orders_v2 missing/invalid id, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            let symbol = order
                .get("currency_pair")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if symbol.is_empty() {
                warn!(
                    "Gate: spot.orders_v2 missing currency_pair, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            }

            let Some(side) = parse_gate_side(order.get("side").and_then(|v| v.as_str())) else {
                warn!("Gate: spot.orders_v2 invalid side, dropping: {}", order);
                incomplete = true;
                continue;
            };

            let Some(order_type) =
                parse_gate_order_type(order.get("type").and_then(|v| v.as_str()))
            else {
                warn!("Gate: spot.orders_v2 invalid type, dropping: {}", order);
                incomplete = true;
                continue;
            };

            let Some(time_in_force) =
                parse_gate_time_in_force(order.get("time_in_force").and_then(|v| v.as_str()))
            else {
                warn!(
                    "Gate: spot.orders_v2 invalid time_in_force, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            // spot: maker/taker 推断
            // - post-only(poc) 一定是 maker
            // - 其余：若 update_time_ms > create_time_ms，则认为经历过挂单生命周期，按 maker 处理
            let time_in_force_raw = order
                .get("time_in_force")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let create_time_ms = parse_timestamp_ms_or_seconds(order.get("create_time_ms"))
                .or_else(|| parse_timestamp_ms_or_seconds(order.get("create_time")));
            let update_time_ms = parse_timestamp_ms_or_seconds(order.get("update_time_ms"));
            let is_maker: u8 = if time_in_force_raw == "poc" {
                1
            } else if matches!((create_time_ms, update_time_ms), (Some(create_ts), Some(update_ts)) if update_ts > create_ts)
            {
                1
            } else {
                0
            };

            let Some(event) = order.get("event").and_then(|v| v.as_str()) else {
                warn!("Gate: spot.orders_v2 missing event, dropping: {}", order);
                incomplete = true;
                continue;
            };
            let Some(finish_as) = order.get("finish_as").and_then(|v| v.as_str()) else {
                warn!(
                    "Gate: spot.orders_v2 missing finish_as, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            let Some((execution_type, order_status)) = classify_gate_spot_event(event, finish_as)
            else {
                warn!(
                    "Gate: spot.orders_v2 unsupported event/finish_as, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            let Some(price) = parse_f64_str_or_num(order.get("price")) else {
                warn!(
                    "Gate: spot.orders_v2 missing/invalid price, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            let Some(quantity) = parse_f64_str_or_num(order.get("amount")) else {
                warn!(
                    "Gate: spot.orders_v2 missing/invalid amount, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            if quantity <= 0.0 {
                warn!(
                    "Gate: spot.orders_v2 non-positive amount, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            }

            let Some(cumulative_filled_quantity) = parse_f64_str_or_num(order.get("filled_amount"))
            else {
                warn!(
                    "Gate: spot.orders_v2 missing/invalid filled_amount, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            // spot: 根据状态语义选择成交价/委托价
            // - trade update（event=update 或 finish_as=filled）取 avg_deal_price
            // - 其余 order update 取 price
            let Some(fill_price) = parse_f64_str_or_num(order.get("avg_deal_price")) else {
                warn!(
                    "Gate: spot.orders_v2 missing/invalid avg_deal_price, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            let last_executed_price =
                select_gate_price_by_update_kind(event, finish_as, fill_price, price);
            if execution_type == 5 && last_executed_price <= 0.0 {
                warn!(
                    "Gate: spot.orders_v2 trade update missing fill price, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            }

            let commission_asset = order
                .get("fee_currency")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let Some(event_time) = parse_timestamp_ms_or_seconds(order.get("update_time_ms"))
            else {
                warn!(
                    "Gate: spot.orders_v2 missing update_time, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

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
                is_maker,
                price,
                quantity,
                cumulative_filled_quantity,
                last_executed_price,
                commission_asset,
            );

            let payload = msg.to_bytes();
            let event_msg = BasicAccountEventMsg::create(
                BasicAccountEventType::OrderUpdate,
                BasicAccountScope::GateUnified,
                payload,
            );
            if tx.send(event_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }

        if incomplete {
            GateParseReport::incomplete(count)
        } else {
            GateParseReport::complete(count)
        }
    }

    /// 解析合约订单更新 (futures.orders)
    ///
    /// 字段映射：
    /// - contract -> symbol
    /// - size -> quantity（contracts，正=做多, 负=做空）
    /// - size - left -> cumulative_filled_quantity（contracts）
    /// - tif -> time_in_force
    /// - update_time -> event_time (已经是 ms)
    /// - status -> 用于判断执行类型 (open/finished)
    ///
    /// 注意：这里的 quantity/cumulative_filled_quantity 保持交易所 contracts 口径；
    /// 策略层统一通过 `MonitorChannel::qty_to_base(...)` 转为 base qty 口径。
    fn parse_futures_orders(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let mut count = 0;
        let mut incomplete = false;

        // 获取 result 数组
        let Some(result) = json_value.get("result").and_then(|v| v.as_array()) else {
            warn!("Gate: futures.orders missing result array");
            return GateParseReport::incomplete(0);
        };

        if result.is_empty() {
            return GateParseReport::complete(0);
        }

        for order in result {
            // 解析 client_order_id (text 字段，必须是 i64)
            let text = order.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let client_order_id = match Self::parse_gate_client_order_id(text) {
                Some(id) => id,
                None => {
                    warn!("Gate: futures.orders text is not i64, dropping: {}", order);
                    incomplete = true;
                    continue;
                }
            };

            // 解析 order_id（兼容 string/number）
            let Some(order_id) = parse_i64_str_or_num(order.get("id")) else {
                warn!(
                    "Gate: futures.orders missing/invalid id, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            // 解析 symbol (contract)
            let symbol = order
                .get("contract")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if symbol.is_empty() {
                warn!("Gate: futures.orders missing contract, dropping: {}", order);
                incomplete = true;
                continue;
            }

            // 解析 side: futures 没有直接的 side 字段，需要根据 size(contracts) 正负判断
            // size > 0 为 buy (做多), size < 0 为 sell (做空)
            let Some(size) = parse_i64_str_or_num(order.get("size")) else {
                warn!(
                    "Gate: futures.orders missing/invalid size, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            if size == 0 {
                warn!("Gate: futures.orders zero size, dropping: {}", order);
                incomplete = true;
                continue;
            }
            let side: u8 = if size >= 0 { 1 } else { 2 }; // 1=Buy, 2=Sell
            let quantity = size.abs() as f64; // contracts

            // 解析 left (剩余未成交 contracts)
            let Some(left) = parse_i64_str_or_num(order.get("left")) else {
                warn!(
                    "Gate: futures.orders missing/invalid left, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            let cumulative_filled_quantity = (size.abs() - left.abs()) as f64; // contracts

            // 解析 order_type: futures 默认是 limit
            // 可以通过 price 是否为 0 判断 market 单
            let Some(price) = parse_f64_str_or_num(order.get("price")) else {
                warn!(
                    "Gate: futures.orders missing/invalid price, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            let order_type: u8 = if price == 0.0 { 3 } else { 1 }; // 3=Market, 1=Limit

            // futures: 根据状态语义选择成交价/委托价
            // - trade update（finish_as=_update 或 filled）取 fill_price
            // - 其余 order update 取 price
            let Some(fill_price) = parse_f64_str_or_num(order.get("fill_price")) else {
                warn!(
                    "Gate: futures.orders missing/invalid fill_price, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            // 解析 event_time (兼容 string/number；兼容秒/ms)
            let Some(event_time) = parse_timestamp_ms_or_seconds(order.get("update_time")) else {
                warn!(
                    "Gate: futures.orders missing update_time, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };

            // 解析 time_in_force
            let Some(time_in_force) =
                parse_gate_time_in_force(order.get("tif").and_then(|v| v.as_str()))
            else {
                warn!("Gate: futures.orders invalid tif, dropping: {}", order);
                incomplete = true;
                continue;
            };

            // futures: maker/taker 推断
            // - post-only(poc) 一定是 maker
            // - 其余：若 update_time_ms > create_time_ms，则认为经历过挂单生命周期，按 maker 处理
            let tif_raw = order
                .get("tif")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let create_time_ms = parse_timestamp_ms_or_seconds(order.get("create_time_ms"))
                .or_else(|| parse_timestamp_ms_or_seconds(order.get("create_time")));
            let is_maker: u8 = if tif_raw == "poc" {
                1
            } else if matches!(create_time_ms, Some(create_ts) if event_time > create_ts) {
                1
            } else {
                0
            };

            // 解析 status 和 finish_as
            let Some(status) = order.get("status").and_then(|v| v.as_str()) else {
                warn!("Gate: futures.orders missing status, dropping: {}", order);
                incomplete = true;
                continue;
            };
            let Some(finish_as) = order.get("finish_as").and_then(|v| v.as_str()) else {
                warn!(
                    "Gate: futures.orders missing finish_as, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            };
            let Some((execution_type, order_status)) =
                classify_gate_futures_finish_as(status, finish_as)
            else {
                warn!(
                    "Gate: futures.orders unknown status/finish_as, raw={}",
                    order
                );
                incomplete = true;
                continue;
            };

            let last_executed_price =
                select_gate_futures_price_by_finish_as(finish_as, fill_price, price);
            if execution_type == 5 && last_executed_price <= 0.0 {
                warn!(
                    "Gate: futures.orders trade update missing fill price, dropping: {}",
                    order
                );
                incomplete = true;
                continue;
            }

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
                is_maker,
                price,
                quantity,
                cumulative_filled_quantity,
                last_executed_price,
                commission_asset,
            );

            let payload = msg.to_bytes();
            let event_msg = BasicAccountEventMsg::create(
                BasicAccountEventType::OrderUpdate,
                BasicAccountScope::GateUnified,
                payload,
            );
            if tx.send(event_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }

        if incomplete {
            GateParseReport::incomplete(count)
        } else {
            GateParseReport::complete(count)
        }
    }

    fn parse_futures_positions(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let mut count = 0;
        let mut incomplete = false;
        let Some(result) = json_value.get("result").and_then(|v| v.as_array()) else {
            warn!("Gate: futures.positions missing result array");
            return GateParseReport::incomplete(0);
        };

        if result.is_empty() {
            return GateParseReport::complete(0);
        }

        for row in result {
            let inst_id = row
                .get("contract")
                .or_else(|| row.get("symbol"))
                .or_else(|| row.get("inst_id"))
                .or_else(|| row.get("instId"))
                .or_else(|| row.get("currency_pair"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();
            let inst_id = normalize_symbol_for_internal(inst_id);
            if inst_id.is_empty() {
                warn!(
                    "Gate: futures.positions missing contract/symbol, dropping: {}",
                    row
                );
                incomplete = true;
                continue;
            }

            let Some(timestamp) = parse_timestamp_ms_or_seconds(row.get("time_ms")) else {
                warn!("Gate: futures.positions missing time_ms, dropping: {}", row);
                incomplete = true;
                continue;
            };

            let Some(size) = parse_f64_str_or_num(
                row.get("size")
                    .or_else(|| row.get("position"))
                    .or_else(|| row.get("pos"))
                    .or_else(|| row.get("qty"))
                    .or_else(|| row.get("amount"))
                    .or_else(|| row.get("position_size"))
                    .or_else(|| row.get("positionSize")),
            ) else {
                warn!(
                    "Gate: futures.positions missing/invalid size, dropping: {}",
                    row
                );
                incomplete = true;
                continue;
            };

            let side_text = row
                .get("side")
                .or_else(|| row.get("position_side"))
                .or_else(|| row.get("pos_side"))
                .or_else(|| row.get("direction"))
                .or_else(|| row.get("posSide"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let position_side = match side_text.trim().to_ascii_lowercase().as_str() {
                "buy" | "long" => 'L',
                "sell" | "short" => 'S',
                "net" | "both" => 'N',
                _ => {
                    if size > 0.0 {
                        'L'
                    } else if size < 0.0 {
                        'S'
                    } else {
                        'N'
                    }
                }
            };
            let position_amount = match position_side {
                'N' => size,
                _ => size.abs(),
            } as f32;

            let position_msg = BasicPositionMsg::create(
                timestamp,
                inst_id.clone(),
                position_side,
                position_amount,
            );
            let position_event = BasicAccountEventMsg::create(
                BasicAccountEventType::PositionUpdate,
                BasicAccountScope::GateUnified,
                position_msg.to_bytes(),
            );
            if tx.send(position_event.to_bytes()).is_ok() {
                count += 1;
            }

            if let Some(pnl) = parse_f64_str_or_num(
                row.get("unrealised_pnl")
                    .or_else(|| row.get("unrealized_pnl"))
                    .or_else(|| row.get("unrealisedPnl"))
                    .or_else(|| row.get("unrealizedPnl"))
                    .or_else(|| row.get("upl")),
            ) {
                let pnl_msg = BasicUmUnrealizedMsg::create(timestamp, inst_id, position_side, pnl);
                let pnl_event = BasicAccountEventMsg::create(
                    BasicAccountEventType::UnrealizedPnlUpdate,
                    BasicAccountScope::GateUnified,
                    pnl_msg.to_bytes(),
                );
                if tx.send(pnl_event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        if incomplete {
            GateParseReport::incomplete(count)
        } else {
            GateParseReport::complete(count)
        }
    }

    /// 解析合约私有成交 (futures.usertrades)
    ///
    /// 字段映射：
    /// - outer `time_ms` -> event_time
    /// - `create_time_ms` / `create_time` -> trade_time
    /// - `contract` -> symbol
    /// - `order_id` -> order_id
    /// - `text` -> client_order_id（必须可解析为 i64 或 `t-<i64>`，否则丢弃）
    /// - `id` -> trade_id
    /// - `size` 绝对值 -> last_executed_quantity（contracts）
    /// - `size` 正负 -> side
    /// - `role` -> is_maker
    /// - `price` -> last_executed_price
    fn parse_futures_usertrades(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let mut count = 0;
        let mut incomplete = false;
        let Some(result) = json_value.get("result").and_then(|v| v.as_array()) else {
            warn!("Gate: futures.usertrades missing result array");
            return GateParseReport::incomplete(0);
        };

        let outer_event_time = parse_timestamp_ms_or_seconds(json_value.get("time_ms"));

        if result.is_empty() {
            return GateParseReport::complete(0);
        }

        for trade in result {
            let symbol = trade
                .get("contract")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if symbol.is_empty() {
                warn!(
                    "Gate: futures.usertrades missing contract, dropping: {}",
                    trade
                );
                incomplete = true;
                continue;
            }

            let Some(raw_size) = parse_f64_str_or_num(trade.get("size")) else {
                warn!(
                    "Gate: futures.usertrades missing/invalid size, dropping: {}",
                    trade
                );
                incomplete = true;
                continue;
            };
            let last_executed_quantity = raw_size.abs();
            if last_executed_quantity <= 0.0 {
                warn!("Gate: futures.usertrades zero size, dropping: {}", trade);
                incomplete = true;
                continue;
            }

            let text = trade.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let client_order_id = match Self::parse_gate_client_order_id(text) {
                Some(id) => id,
                None => {
                    warn!(
                        "Gate: futures.usertrades text is not i64/t-i64, dropping: {}",
                        trade
                    );
                    incomplete = true;
                    continue;
                }
            };

            let Some(trade_time) = parse_timestamp_ms_or_seconds(trade.get("create_time_ms"))
                .or_else(|| parse_timestamp_ms_or_seconds(trade.get("create_time")))
            else {
                warn!(
                    "Gate: futures.usertrades missing trade timestamp, dropping: {}",
                    trade
                );
                incomplete = true;
                continue;
            };
            let Some(event_time) = outer_event_time else {
                warn!(
                    "Gate: futures.usertrades missing outer event timestamp, dropping: {}",
                    trade
                );
                incomplete = true;
                continue;
            };

            let side = if raw_size >= 0.0 { 1 } else { 2 };
            let trade_id_raw = trade.get("id");
            let trade_id = match trade_id_raw.and_then(|v| v.as_str().map(|s| s.to_string())) {
                Some(s) if !s.is_empty() => s,
                _ => match trade_id_raw.and_then(|v| v.as_i64()) {
                    Some(n) => n.to_string(),
                    None => {
                        warn!(
                            "Gate: futures.usertrades missing/invalid id, dropping: {}",
                            trade
                        );
                        incomplete = true;
                        continue;
                    }
                },
            };
            let Some(last_executed_price) = parse_f64_str_or_num(trade.get("price")) else {
                warn!(
                    "Gate: futures.usertrades missing/invalid price, dropping: {}",
                    trade
                );
                incomplete = true;
                continue;
            };
            if last_executed_price <= 0.0 {
                warn!(
                    "Gate: futures.usertrades non-positive price, dropping: {}",
                    trade
                );
                incomplete = true;
                continue;
            }
            let Some(is_maker) = parse_gate_role(trade.get("role").and_then(|v| v.as_str())) else {
                warn!("Gate: futures.usertrades invalid role, dropping: {}", trade);
                incomplete = true;
                continue;
            };

            let msg = BasicTradeLiteMsg::create(
                GateBasicOrderMsg::VENUE_FUTURES,
                event_time,
                trade_time,
                symbol,
                client_order_id,
                &trade_id,
                side,
                is_maker,
                last_executed_price,
                last_executed_quantity,
            );

            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::TradeUpdateLite,
                BasicAccountScope::GateUnified,
                msg.to_bytes(),
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }

        if incomplete {
            GateParseReport::incomplete(count)
        } else {
            GateParseReport::complete(count)
        }
    }

    pub fn parse_with_report(
        &self,
        msg: Bytes,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> GateParseReport {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return GateParseReport::incomplete(0),
        };

        let json_value: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return GateParseReport::incomplete(0),
        };

        let channel = json_value
            .get("channel")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let event = json_value
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match channel {
            "unified.asset_detail" => {
                if event == "update" {
                    self.parse_unified_asset_detail(&json_value, tx)
                } else {
                    GateParseReport::incomplete(0)
                }
            }
            "unified.assets" => {
                if event == "update" {
                    self.parse_unified_assets(&json_value, tx)
                } else {
                    GateParseReport::incomplete(0)
                }
            }
            "spot.orders_v2" | "spot.orders" => {
                if event == "update" {
                    self.parse_spot_orders_v2(&json_value, tx)
                } else {
                    GateParseReport::incomplete(0)
                }
            }
            "futures.orders" => {
                if event == "update" {
                    self.parse_futures_orders(&json_value, tx)
                } else {
                    GateParseReport::incomplete(0)
                }
            }
            "futures.usertrades" => {
                if event == "update" {
                    self.parse_futures_usertrades(&json_value, tx)
                } else {
                    GateParseReport::incomplete(0)
                }
            }
            "futures.positions" => {
                if event == "update" {
                    self.parse_futures_positions(&json_value, tx)
                } else {
                    GateParseReport::incomplete(0)
                }
            }
            "unified.pong" | "spot.pong" | "futures.pong" => GateParseReport::complete(0),
            _ => {
                if json_value.get("event").is_some() {
                    debug!(
                        "Gate: unsupported event channel={} event={}",
                        channel, event
                    );
                } else if !channel.is_empty() {
                    warn!("Gate: Unknown channel: {}", channel);
                }
                GateParseReport::incomplete(0)
            }
        }
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

fn parse_timestamp_ms_or_seconds(v: Option<&Value>) -> Option<i64> {
    let ts = parse_i64_str_or_num(v)?;
    if ts >= 1_000_000_000_000 {
        Some(ts)
    } else if ts >= 1_000_000_000 {
        Some(ts.saturating_mul(1_000))
    } else {
        None
    }
}

fn parse_gate_side(raw: Option<&str>) -> Option<u8> {
    match raw?.trim().to_ascii_lowercase().as_str() {
        "buy" => Some(1),
        "sell" => Some(2),
        _ => None,
    }
}

fn parse_gate_order_type(raw: Option<&str>) -> Option<u8> {
    match raw?.trim().to_ascii_lowercase().as_str() {
        "limit" => Some(1),
        "limit_borrow" | "limit_repay" | "limit_borrow_repay" => Some(1),
        "market" => Some(3),
        "market_borrow" | "market_repay" | "market_borrow_repay" => Some(3),
        _ => None,
    }
}

fn parse_gate_time_in_force(raw: Option<&str>) -> Option<u8> {
    match raw?.trim().to_ascii_lowercase().as_str() {
        "gtc" => Some(0),
        "ioc" => Some(1),
        "fok" => Some(2),
        "poc" => Some(3),
        _ => None,
    }
}

fn parse_gate_role(raw: Option<&str>) -> Option<bool> {
    match raw?.trim().to_ascii_lowercase().as_str() {
        "maker" => Some(true),
        "taker" => Some(false),
        _ => None,
    }
}

fn classify_gate_spot_event(event: &str, finish_as: &str) -> Option<(u8, u8)> {
    let event = event.trim().to_ascii_lowercase();
    let finish_as = finish_as.trim();
    if finish_as.is_empty() {
        return None;
    }
    match event.as_str() {
        "put" | "update" | "finish" => Some(GateBasicOrderMsg::event_to_execution_and_status(
            &event, finish_as,
        )),
        _ => None,
    }
}

fn select_gate_price_by_update_kind(
    event_or_status: &str,
    finish_as: &str,
    fill_price: f64,
    order_price: f64,
) -> f64 {
    let event_or_status = event_or_status.trim().to_ascii_lowercase();
    let finish_as = finish_as.trim().to_ascii_lowercase();

    // Gate 语义：
    // - trade update: futures 的 finish_as="_update"/"filled"；spot 的 event="update"
    // - order update: 其余状态
    let use_fill_price =
        event_or_status == "update" || matches!(finish_as.as_str(), "_update" | "filled");
    let selected = if use_fill_price {
        fill_price
    } else {
        order_price
    };

    if selected.is_finite() && selected > 0.0 {
        selected
    } else {
        0.0
    }
}

fn classify_gate_futures_finish_as(status: &str, finish_as: &str) -> Option<(u8, u8)> {
    let status = status.trim().to_ascii_lowercase();
    let finish_as = finish_as.trim().to_ascii_lowercase();

    match (status.as_str(), finish_as.as_str()) {
        ("open", "_new") => Some((1, 1)),
        ("open", "_update") => Some((5, 2)),
        ("finished", "filled") => Some((5, 3)),
        ("finished", "ioc") => Some((5, 3)),
        ("finished", "cancelled") | ("finished", "canceled") => Some((2, 4)),
        ("finished", "liquidated") => Some((2, 4)),
        ("finished", "auto_deleveraging") => Some((2, 4)),
        ("finished", "reduce_only") => Some((2, 4)),
        ("finished", "position_close") => Some((2, 4)),
        ("finished", "stp") => Some((7, 4)),
        ("finished", "reduce_out") => Some((2, 4)),
        // Gate POC/PostOnly 条件不满足时会以 finished+poc 推送，等价于未成交撤单；
        // hedge arb 依赖这个 Canceled order update 触发撤单后重报。
        ("finished", "poc") => Some((2, 4)),
        _ => None,
    }
}

fn select_gate_futures_price_by_finish_as(
    finish_as: &str,
    fill_price: f64,
    order_price: f64,
) -> f64 {
    let finish_as = finish_as.trim().to_ascii_lowercase();
    let selected = match finish_as.as_str() {
        "_update" | "filled" | "ioc" => fill_price,
        "_new" | "cancelled" | "canceled" | "liquidated" | "auto_deleveraging" | "reduce_only"
        | "position_close" | "stp" | "reduce_out" | "poc" => order_price,
        _ => 0.0,
    };

    if selected.is_finite() && selected > 0.0 {
        selected
    } else {
        0.0
    }
}

impl Parser for GateAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        self.parse_with_report(msg, tx).emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::{
        split_basic_account_event, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
        BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicUmUnrealizedMsg,
        GateBasicOrderMsg,
    };
    use crate::common::exchange::Exchange;
    use crate::pre_trade::usdt_balance_manager::UsdtBalanceManager;

    #[test]
    fn unified_asset_detail_prefers_equity_plus_liability_over_raw_balance() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1775532902,
                "time_ms": 1775532902286,
                "channel": "unified.asset_detail",
                "event": "update",
                "result": {
                    "u": 46586604,
                    "t": 1775532902,
                    "dts": {
                        "USDT": {
                            "a": "-53.2047041",
                            "f": "0.00",
                            "e": "-1029.02217879",
                            "tl": "3112.250826",
                            "b": "-53.2047041"
                        }
                    }
                }
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 2);

        let wrapped_balance = rx.try_recv().expect("balance event");
        let (event_type, scope, body) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::GateUnified);
        let balance = BasicBalanceMsg::from_bytes(body).expect("balance body");
        assert_eq!(balance.symbol, "USDT");
        assert!((balance.wallet - 2_083.22864721).abs() < 1e-9);

        let wrapped_borrow = rx.try_recv().expect("borrow event");
        let (event_type, scope, body) =
            split_basic_account_event(&wrapped_borrow).expect("wrapped borrow");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        assert_eq!(scope, BasicAccountScope::GateUnified);
        let borrow = BasicBorrowInterestMsg::from_bytes(body).expect("borrow body");
        assert_eq!(borrow.symbol, "USDT");
        assert!((borrow.borrowed - 3112.250826).abs() < 1e-12);
    }

    #[test]
    fn unified_asset_detail_requires_equity_and_ignores_raw_balance() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1775532902,
                "time_ms": 1775532902286,
                "channel": "unified.asset_detail",
                "event": "update",
                "result": {
                    "u": 46586604,
                    "t": 1775532902,
                    "dts": {
                        "USDT": {
                            "tl": "10",
                            "b": "999"
                        }
                    }
                }
            }"#,
        );

        let report = parser.parse_with_report(payload, &tx);
        assert_eq!(report.emitted, 0);
        assert!(!report.complete);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn gate_usdt_asset_detail_net_position_matches_equity() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1775532902,
                "time_ms": 1775532902286,
                "channel": "unified.asset_detail",
                "event": "update",
                "result": {
                    "u": 46586604,
                    "t": 1775532902,
                    "dts": {
                        "USDT": {
                            "e": "102126.004737962706",
                            "tl": "1509.291784781151",
                            "b": "103635.296522743857"
                        }
                    }
                }
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 2);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Gate);
        let wrapped_balance = rx.try_recv().expect("balance event");
        let (event_type, _, body) =
            split_basic_account_event(&wrapped_balance).expect("wrapped balance");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        let balance = BasicBalanceMsg::from_bytes(body).expect("balance body");
        usdt_mgr.apply_balance(&balance);

        let wrapped_borrow = rx.try_recv().expect("borrow event");
        let (event_type, _, body) =
            split_basic_account_event(&wrapped_borrow).expect("wrapped borrow");
        assert_eq!(event_type, BasicAccountEventType::BorrowInterest);
        let borrow = BasicBorrowInterestMsg::from_bytes(body).expect("borrow body");
        usdt_mgr.apply_borrow_interest(&borrow);

        assert!((balance.wallet - 103_635.296522743857).abs() < 1e-9);
        assert!((usdt_mgr.net_usdt_position() - 102_126.004737962706).abs() < 1e-9);
    }

    #[test]
    fn account_risk_parses_unified_assets_rates() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1700625194,
                "channel": "unified.assets",
                "event": "update",
                "result": {
                    "u": 9008,
                    "t": 1700625194,
                    "r": "20.10",
                    "R": "18.56",
                    "b": "-675222.27",
                    "e": "-617985.29",
                    "l": "1293939.74",
                    "T": "675222.27",
                    "a": "-1432719.62"
                }
            }"#,
        );

        let report = parser.parse_with_report(payload, &tx);
        assert_eq!(report.emitted, 1);
        assert!(report.complete);

        let wrapped = rx.try_recv().expect("risk event");
        let (event_type, scope, body) = split_basic_account_event(&wrapped).expect("wrapped risk");
        assert_eq!(event_type, BasicAccountEventType::AccountRisk);
        assert_eq!(scope, BasicAccountScope::GateUnified);
        let risk = BasicAccountRiskMsg::from_bytes(body).expect("risk body");
        assert_eq!(risk.timestamp, 1_700_625_194_000);
        assert!((risk.adj_equity_usd + 617_985.29).abs() < 1e-9);
        assert!((risk.margin_ratio - 0.1856).abs() < 1e-12);
        assert!((risk.maintenance_margin_usd - (675_222.27 / 0.1856)).abs() < 1e-6);
        assert!((risk.initial_margin_usd - (675_222.27 / 0.2010)).abs() < 1e-6);
        assert!((risk.borrowed_usd - 1_293_939.74).abs() < 1e-9);
    }

    #[test]
    fn futures_positions_emits_position_and_upl() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1716796362,
                "time_ms": 1716796362915,
                "channel": "futures.positions",
                "event": "update",
                "result": [{
                    "contract": "BTC_USDT",
                    "size": "-2",
                    "unrealised_pnl": "-0.25",
                    "time_ms": 1716796362915
                }]
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 2);

        let wrapped_position = rx.try_recv().expect("position event");
        let (event_type, scope, body) =
            split_basic_account_event(&wrapped_position).expect("wrapped position");
        assert_eq!(event_type, BasicAccountEventType::PositionUpdate);
        assert_eq!(scope, BasicAccountScope::GateUnified);
        let position = BasicPositionMsg::from_bytes(body).expect("position body");
        assert_eq!(position.inst_id, "BTCUSDT");
        assert_eq!(position.position_side, 'S');
        assert!((position.position_amount - 2.0).abs() < 1e-6);

        let wrapped_pnl = rx.try_recv().expect("upl event");
        let (event_type, scope, body) =
            split_basic_account_event(&wrapped_pnl).expect("wrapped pnl");
        assert_eq!(event_type, BasicAccountEventType::UnrealizedPnlUpdate);
        assert_eq!(scope, BasicAccountScope::GateUnified);
        let pnl = BasicUmUnrealizedMsg::from_bytes(body).expect("pnl body");
        assert_eq!(pnl.inst_id, "BTCUSDT");
        assert_eq!(pnl.position_side, 'S');
        assert!((pnl.unrealized_pnl + 0.25).abs() < 1e-12);
    }

    #[test]
    fn futures_positions_missing_time_ms_is_incomplete() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "channel": "futures.positions",
                "event": "update",
                "result": [{
                    "contract": "BTC_USDT",
                    "size": "-2",
                    "unrealised_pnl": "-0.25",
                    "time": 1716796362
                }]
            }"#,
        );

        let report = parser.parse_with_report(payload, &tx);
        assert_eq!(report.emitted, 0);
        assert!(!report.complete);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn gate_futures_finish_as_classification_is_strict() {
        let parser = GateAccountEventParser::new();

        let mk_payload = |finish_as: &str, status: &str| {
            Bytes::from(
                format!(
                    r#"{{
                        "channel":"futures.orders",
                        "event":"update",
                        "time":1541505434,
                        "time_ms":1541505434123,
                        "result":[{{
                            "contract":"BTC_USDT",
                            "create_time":1628736847,
                            "create_time_ms":1628736847325,
                            "fill_price":40000.4,
                            "finish_as":"{finish_as}",
                            "finish_time":1628736848,
                            "finish_time_ms":1628736848321,
                            "iceberg":"0",
                            "id":4872460,
                            "is_close":false,
                            "is_liq":false,
                            "is_reduce_only":false,
                            "left":"0",
                            "mkfr":-0.00025,
                            "price":40000.4,
                            "refr":0,
                            "refu":0,
                            "size":"1",
                            "status":"{status}",
                            "text":"123456789",
                            "tif":"gtc",
                            "tkfr":0.0005,
                            "user":"110xxxxx",
                            "update_id":1,
                            "update_time":1541505434123
                        }}]
                    }}"#
                )
                .into_bytes(),
            )
        };

        let cases = [
            ("_new", "open", 1_u8, 1_u8),
            ("_update", "open", 5_u8, 2_u8),
            ("filled", "finished", 5_u8, 3_u8),
            ("ioc", "finished", 5_u8, 3_u8),
            ("cancelled", "finished", 2_u8, 4_u8),
            ("poc", "finished", 2_u8, 4_u8),
        ];

        for (finish_as, status, exec, ord_status) in cases {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let count = parser.parse(mk_payload(finish_as, status), &tx);
            assert_eq!(count, 1, "finish_as={finish_as}");

            let wrapped = rx.try_recv().expect("order event");
            let (event_type, scope, body) =
                split_basic_account_event(&wrapped).expect("wrapped order");
            assert_eq!(event_type, BasicAccountEventType::OrderUpdate);
            assert_eq!(scope, BasicAccountScope::GateUnified);
            let msg = GateBasicOrderMsg::from_bytes(body).expect("gate order body");
            assert_eq!(msg.execution_type, exec, "finish_as={finish_as}");
            assert_eq!(msg.order_status, ord_status, "finish_as={finish_as}");
        }
    }

    #[test]
    fn gate_futures_unknown_finish_as_is_dropped() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "channel":"futures.orders",
                "event":"update",
                "time":1541505434,
                "time_ms":1541505434123,
                "result":[{
                    "contract":"BTC_USDT",
                    "create_time":1628736847,
                    "create_time_ms":1628736847325,
                    "fill_price":40000.4,
                    "finish_as":"mystery_state",
                    "id":4872460,
                    "left":"0",
                    "price":40000.4,
                    "size":"1",
                    "status":"finished",
                    "text":"123456789",
                    "tif":"gtc",
                    "update_time":1541505434123
                }]
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn gate_futures_orders_accept_numeric_size_and_left() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time":1776921360,
                "time_ms":1776921360551,
                "channel":"futures.orders",
                "event":"update",
                "result":[{
                    "create_time":1776921350,
                    "create_time_ms":1776921350009,
                    "fill_price":85.91,
                    "finish_as":"filled",
                    "id":168322043570393737,
                    "left":0,
                    "price":85.91,
                    "size":1,
                    "status":"finished",
                    "text":"t-162578788624891905",
                    "tif":"poc",
                    "contract":"SOL_USDT",
                    "update_time":1776921360550,
                    "role":"maker"
                }]
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 1);

        let wrapped = rx.try_recv().expect("order event");
        let (event_type, scope, body) = split_basic_account_event(&wrapped).expect("wrapped order");
        assert_eq!(event_type, BasicAccountEventType::OrderUpdate);
        assert_eq!(scope, BasicAccountScope::GateUnified);

        let msg = GateBasicOrderMsg::from_bytes(body).expect("gate order body");
        assert_eq!(msg.symbol, "SOL_USDT");
        assert_eq!(msg.execution_type, 5);
        assert_eq!(msg.order_status, 3);
        assert_eq!(msg.quantity, 1.0);
        assert_eq!(msg.cumulative_filled_quantity, 1.0);
        assert_eq!(msg.is_maker, 1);
        assert_eq!(msg.event_time, 1_776_921_360_550);
        assert!((msg.last_executed_price - 85.91).abs() < 1e-12);
    }

    #[test]
    fn gate_futures_orders_missing_update_time_is_incomplete() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "channel":"futures.orders",
                "event":"update",
                "result":[{
                    "contract":"BTC_USDT",
                    "id":4872460,
                    "text":"123456789",
                    "size":"1",
                    "left":"0",
                    "price":"40000.4",
                    "fill_price":"40000.4",
                    "tif":"gtc",
                    "status":"finished",
                    "finish_as":"filled",
                    "create_time":1628736847
                }]
            }"#,
        );

        let report = parser.parse_with_report(payload, &tx);
        assert_eq!(report.emitted, 0);
        assert!(!report.complete);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn gate_spot_orders_accept_margin_types() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "channel":"spot.orders_v2",
                "event":"update",
                "time":1777821643,
                "time_ms":1777821643188,
                "result":[{
                    "id":"1058143375971",
                    "text":"t-2296188760250908673",
                    "create_time":"1777821642",
                    "update_time":"1777821643",
                    "create_time_ms":"1777821642219",
                    "update_time_ms":"1777821643187",
                    "currency_pair":"BTC_USDT",
                    "type":"limit_borrow_repay",
                    "account":"unified",
                    "side":"sell",
                    "amount":"0.0063",
                    "price":"78756.6",
                    "time_in_force":"poc",
                    "left":"0.0063",
                    "filled_amount":"0",
                    "filled_total":"0",
                    "avg_deal_price":"0",
                    "fee_currency":"USDT",
                    "rebated_fee_currency":"USDT",
                    "user":14481073,
                    "event":"finish",
                    "stp_id":0,
                    "stp_act":"-",
                    "finish_as":"cancelled",
                    "biz_info":"-",
                    "amend_text":"-"
                }]
            }"#,
        );

        let report = parser.parse_with_report(payload, &tx);
        assert_eq!(report.emitted, 1);
        assert!(report.complete);

        let wrapped = rx.try_recv().expect("order event");
        let (event_type, scope, body) = split_basic_account_event(&wrapped).expect("wrapped order");
        assert_eq!(event_type, BasicAccountEventType::OrderUpdate);
        assert_eq!(scope, BasicAccountScope::GateUnified);

        let msg = GateBasicOrderMsg::from_bytes(body).expect("gate order body");
        assert_eq!(msg.symbol, "BTC_USDT");
        assert_eq!(msg.order_type, 1);
        assert_eq!(msg.execution_type, 2);
        assert_eq!(msg.order_status, 4);
        assert_eq!(msg.client_order_id, 2296188760250908673);
    }

    #[test]
    fn futures_usertrades_emits_trade_lite_for_t_prefixed_client_order_id() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1543205083,
                "time_ms": 1543205083123,
                "channel": "futures.usertrades",
                "event": "update",
                "result": [{
                    "id": "3335259",
                    "create_time": 1628736848,
                    "create_time_ms": 1628736848321,
                    "contract": "BTC_USD",
                    "order_id": "4872460",
                    "size": "-2",
                    "price": "40000.4",
                    "role": "maker",
                    "text": "t-123456",
                    "fee": 0.0009290592,
                    "point_fee": 0
                }]
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 1);

        let wrapped_trade = rx.try_recv().expect("trade-lite event");
        let (event_type, scope, body) =
            split_basic_account_event(&wrapped_trade).expect("wrapped trade-lite");
        assert_eq!(event_type, BasicAccountEventType::TradeUpdateLite);
        assert_eq!(scope, BasicAccountScope::GateUnified);

        let trade = BasicTradeLiteMsg::from_bytes(body).expect("trade-lite body");
        assert_eq!(trade.venue, GateBasicOrderMsg::VENUE_FUTURES);
        assert_eq!(trade.event_time, 1_543_205_083_123);
        assert_eq!(trade.trade_time, 1_628_736_848_321);
        assert_eq!(trade.symbol, "BTC_USD");
        assert_eq!(trade.client_order_id, 123_456);
        assert_eq!(trade.trade_id_str(), "3335259");
        assert_eq!(trade.side, 2);
        assert_eq!(trade.is_maker, 1);
        assert!((trade.last_executed_price - 40_000.4).abs() < 1e-9);
        assert!((trade.last_executed_quantity - 2.0).abs() < 1e-9);
    }

    #[test]
    fn futures_usertrades_drops_non_numeric_client_order_id() {
        let parser = GateAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = Bytes::from_static(
            br#"{
                "time": 1543205083,
                "time_ms": 1543205083123,
                "channel": "futures.usertrades",
                "event": "update",
                "result": [{
                    "id": "3335259",
                    "create_time": 1628736848,
                    "create_time_ms": 1628736848321,
                    "contract": "BTC_USD",
                    "order_id": "4872460",
                    "size": "1",
                    "price": "40000.4",
                    "role": "maker",
                    "text": "api"
                }]
            }"#,
        );

        let count = parser.parse(payload, &tx);
        assert_eq!(count, 0);
        assert!(rx.try_recv().is_err());
    }
}
