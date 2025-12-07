//! OKX 账户事件解析器（仅余额与持仓）

use crate::common::okex_account_msg::{OkexAccountEventMsg, OkexBalanceMsg, OkexPositionMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::{debug, info, warn};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct OkexAccountEventParser;

impl OkexAccountEventParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_balance_and_position(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut count = 0;

        // balance
        if let Some(arr) = json_value
            .get("data")
            .and_then(|d| d.get(0))
            .and_then(|d| d.get("balData"))
            .and_then(|v| v.as_array())
        {
            for bal in arr {
                let timestamp = bal
                    .get("uTime")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let balance = bal
                    .get("cashBal")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);

                let msg = OkexBalanceMsg::create(timestamp, balance);
                let payload = msg.to_bytes();
                let event = OkexAccountEventMsg::create(msg.msg_type, payload);
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        // position
        if let Some(arr) = json_value
            .get("data")
            .and_then(|d| d.get(0))
            .and_then(|d| d.get("posData"))
            .and_then(|v| v.as_array())
        {
            for pos in arr {
                let inst_id = match pos.get("instId").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let inst_type = pos.get("instType").and_then(|v| v.as_str()).unwrap_or("");
                let pos_ccy = pos.get("posCcy").and_then(|v| v.as_str()).unwrap_or("");
                let pos_side = pos.get("posSide").and_then(|v| v.as_str()).unwrap_or("net");
                let position_side = match pos_side {
                    "long" => 'L',
                    "short" => 'S',
                    _ => 'N',
                };
                let position_amount = pos
                    .get("pos")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f32>().ok())
                    .unwrap_or(0.0);
                let upl = pos
                    .get("upl")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let timestamp = pos
                    .get("uTime")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let liab = pos
                    .get("liab")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f32>().ok())
                    .unwrap_or(0.0);
                let liab_ccy = pos.get("liabCcy").and_then(|v| v.as_str()).unwrap_or("");
                let interest = pos
                    .get("interest")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f32>().ok())
                    .unwrap_or(0.0);
                let liab_is_usdt = liab_ccy.eq_ignore_ascii_case("usdt");
                if inst_type == "MARGIN" {
                    info!(
                        "OKX margin position: posCcy={} instId={} posSide={} pos={} upl={} liab={} {} interest={} uTime={}",
                        pos_ccy, inst_id, pos_side, position_amount, upl, liab, liab_ccy, interest, timestamp
                    );
                } else {
                    info!(
                        "OKX position: instType={} posCcy={} instId={} posSide={} pos={} upl={} uTime={}",
                        inst_type, pos_ccy, inst_id, pos_side, position_amount, upl, timestamp
                    );
                }
                let msg = if inst_type == "MARGIN" {
                    OkexPositionMsg::create_margin(
                        timestamp,
                        inst_id,
                        position_side,
                        position_amount,
                        upl,
                        liab,
                        liab_is_usdt,
                        interest,
                    )
                } else {
                    OkexPositionMsg::create_swap(
                        timestamp,
                        inst_id,
                        position_side,
                        position_amount,
                        upl,
                    )
                };
                let payload = msg.to_bytes();
                let event = OkexAccountEventMsg::create(msg.msg_type(), payload);
                if tx.send(event.to_bytes()).is_ok() {
                    count += 1;
                }
            }
        }

        count
    }
}

impl Parser for OkexAccountEventParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let json_str = match std::str::from_utf8(&msg) {
            Ok(s) => s,
            Err(_) => return 0,
        };

        let json_value: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return 0,
        };

        let channel = json_value
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match channel {
            "balance_and_position" => self.parse_balance_and_position(&json_value, tx),
            "orders" | "positions" | "account" => {
                debug!("OKX: ignored channel={} payload={}", channel, json_str);
                0
            }
            _ => {
                if json_value.get("event").is_some() {
                    debug!("OKX: event message: {}", json_str);
                } else {
                    warn!("OKX: Unknown channel: {}", channel);
                }
                0
            }
        }
    }
}
