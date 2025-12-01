//! OKX 账户事件消息定义
//!
//! 独立的 OKX 消息格式，不与 Binance 共用结构

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// OKX 账户事件类型
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OkexAccountEventType {
    /// 订单更新
    OrderUpdate = 4001,
    /// 账户余额更新
    BalanceUpdate = 4002,
    /// 持仓更新
    PositionUpdate = 4003,
    /// 错误
    Error = 4999,
}

/// OKX 订单更新消息
///
/// 对应 OKX orders 频道推送
#[derive(Debug, Clone)]
pub struct OkexOrderMsg {
    pub msg_type: OkexAccountEventType,
    /// 产品ID，如 BTC-USDT-SWAP
    pub inst_id: String,
    /// 产品类型: SPOT, MARGIN, SWAP, FUTURES, OPTION
    pub inst_type: String,
    /// 订单ID
    pub ord_id: String,
    /// 客户自定义订单ID
    pub cl_ord_id: String,
    /// 订单状态: live, partially_filled, filled, canceled, mmp_canceled
    pub state: String,
    /// 订单方向: buy, sell
    pub side: String,
    /// 持仓方向: long, short, net
    pub pos_side: String,
    /// 订单类型: market, limit, post_only, fok, ioc, optimal_limit_ioc
    pub ord_type: String,
    /// 委托价格
    pub px: f64,
    /// 委托数量
    pub sz: f64,
    /// 成交均价
    pub avg_px: f64,
    /// 累计成交数量
    pub acc_fill_sz: f64,
    /// 最新成交价格
    pub fill_px: f64,
    /// 最新成交数量
    pub fill_sz: f64,
    /// 最新成交ID
    pub trade_id: String,
    /// 手续费 (负数为扣费，正数为返佣)
    pub fee: f64,
    /// 手续费币种
    pub fee_ccy: String,
    /// 收益
    pub pnl: f64,
    /// 是否只减仓
    pub reduce_only: bool,
    /// 订单创建时间 (毫秒)
    pub c_time: i64,
    /// 订单更新时间 (毫秒)
    pub u_time: i64,
    /// 成交时间 (毫秒)
    pub fill_time: i64,
}

impl OkexOrderMsg {
    pub fn to_bytes(&self) -> Bytes {
        let inst_id_bytes = self.inst_id.as_bytes();
        let inst_type_bytes = self.inst_type.as_bytes();
        let ord_id_bytes = self.ord_id.as_bytes();
        let cl_ord_id_bytes = self.cl_ord_id.as_bytes();
        let state_bytes = self.state.as_bytes();
        let side_bytes = self.side.as_bytes();
        let pos_side_bytes = self.pos_side.as_bytes();
        let ord_type_bytes = self.ord_type.as_bytes();
        let trade_id_bytes = self.trade_id.as_bytes();
        let fee_ccy_bytes = self.fee_ccy.as_bytes();

        let total_size = 4  // msg_type
            + 4 + inst_id_bytes.len()
            + 4 + inst_type_bytes.len()
            + 4 + ord_id_bytes.len()
            + 4 + cl_ord_id_bytes.len()
            + 4 + state_bytes.len()
            + 4 + side_bytes.len()
            + 4 + pos_side_bytes.len()
            + 4 + ord_type_bytes.len()
            + 8 * 6  // px, sz, avg_px, acc_fill_sz, fill_px, fill_sz
            + 4 + trade_id_bytes.len()
            + 8 * 2  // fee, pnl
            + 4 + fee_ccy_bytes.len()
            + 1  // reduce_only
            + 8 * 3; // c_time, u_time, fill_time

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);

        // 字符串字段: 长度 + 内容
        buf.put_u32_le(inst_id_bytes.len() as u32);
        buf.put(inst_id_bytes);

        buf.put_u32_le(inst_type_bytes.len() as u32);
        buf.put(inst_type_bytes);

        buf.put_u32_le(ord_id_bytes.len() as u32);
        buf.put(ord_id_bytes);

        buf.put_u32_le(cl_ord_id_bytes.len() as u32);
        buf.put(cl_ord_id_bytes);

        buf.put_u32_le(state_bytes.len() as u32);
        buf.put(state_bytes);

        buf.put_u32_le(side_bytes.len() as u32);
        buf.put(side_bytes);

        buf.put_u32_le(pos_side_bytes.len() as u32);
        buf.put(pos_side_bytes);

        buf.put_u32_le(ord_type_bytes.len() as u32);
        buf.put(ord_type_bytes);

        // 数值字段
        buf.put_f64_le(self.px);
        buf.put_f64_le(self.sz);
        buf.put_f64_le(self.avg_px);
        buf.put_f64_le(self.acc_fill_sz);
        buf.put_f64_le(self.fill_px);
        buf.put_f64_le(self.fill_sz);

        buf.put_u32_le(trade_id_bytes.len() as u32);
        buf.put(trade_id_bytes);

        buf.put_f64_le(self.fee);
        buf.put_f64_le(self.pnl);

        buf.put_u32_le(fee_ccy_bytes.len() as u32);
        buf.put(fee_ccy_bytes);

        buf.put_u8(if self.reduce_only { 1 } else { 0 });

        buf.put_i64_le(self.c_time);
        buf.put_i64_le(self.u_time);
        buf.put_i64_le(self.fill_time);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut cursor = Bytes::copy_from_slice(data);

        if cursor.remaining() < 4 {
            anyhow::bail!("OkexOrderMsg too short");
        }

        let msg_type_u32 = cursor.get_u32_le();
        if msg_type_u32 != OkexAccountEventType::OrderUpdate as u32 {
            anyhow::bail!("Invalid msg type: {}", msg_type_u32);
        }

        fn read_string(cursor: &mut Bytes) -> Result<String> {
            if cursor.remaining() < 4 {
                anyhow::bail!("Not enough data for string length");
            }
            let len = cursor.get_u32_le() as usize;
            if cursor.remaining() < len {
                anyhow::bail!("Not enough data for string content");
            }
            Ok(String::from_utf8(cursor.copy_to_bytes(len).to_vec())?)
        }

        let inst_id = read_string(&mut cursor)?;
        let inst_type = read_string(&mut cursor)?;
        let ord_id = read_string(&mut cursor)?;
        let cl_ord_id = read_string(&mut cursor)?;
        let state = read_string(&mut cursor)?;
        let side = read_string(&mut cursor)?;
        let pos_side = read_string(&mut cursor)?;
        let ord_type = read_string(&mut cursor)?;

        if cursor.remaining() < 8 * 6 {
            anyhow::bail!("Not enough data for numeric fields");
        }

        let px = cursor.get_f64_le();
        let sz = cursor.get_f64_le();
        let avg_px = cursor.get_f64_le();
        let acc_fill_sz = cursor.get_f64_le();
        let fill_px = cursor.get_f64_le();
        let fill_sz = cursor.get_f64_le();

        let trade_id = read_string(&mut cursor)?;

        if cursor.remaining() < 8 * 2 {
            anyhow::bail!("Not enough data for fee/pnl");
        }
        let fee = cursor.get_f64_le();
        let pnl = cursor.get_f64_le();

        let fee_ccy = read_string(&mut cursor)?;

        if cursor.remaining() < 1 + 8 * 3 {
            anyhow::bail!("Not enough data for flags and timestamps");
        }
        let reduce_only = cursor.get_u8() != 0;
        let c_time = cursor.get_i64_le();
        let u_time = cursor.get_i64_le();
        let fill_time = cursor.get_i64_le();

        Ok(Self {
            msg_type: OkexAccountEventType::OrderUpdate,
            inst_id,
            inst_type,
            ord_id,
            cl_ord_id,
            state,
            side,
            pos_side,
            ord_type,
            px,
            sz,
            avg_px,
            acc_fill_sz,
            fill_px,
            fill_sz,
            trade_id,
            fee,
            fee_ccy,
            pnl,
            reduce_only,
            c_time,
            u_time,
            fill_time,
        })
    }
}

/// OKX 账户事件消息包装
pub struct OkexAccountEventMsg {
    pub msg_type: OkexAccountEventType,
    pub msg_length: u32,
    pub data: Bytes,
}

impl OkexAccountEventMsg {
    pub fn create(msg_type: OkexAccountEventType, data: Bytes) -> Self {
        Self {
            msg_type,
            msg_length: data.len() as u32,
            data,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + self.data.len());
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.msg_length);
        buf.put(self.data.clone());
        buf.freeze()
    }
}

/// 获取事件类型
#[inline]
pub fn get_okex_event_type(data: &[u8]) -> OkexAccountEventType {
    if data.len() < 4 {
        return OkexAccountEventType::Error;
    }
    let event_type_u32 = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    match event_type_u32 {
        4001 => OkexAccountEventType::OrderUpdate,
        4002 => OkexAccountEventType::BalanceUpdate,
        4003 => OkexAccountEventType::PositionUpdate,
        _ => OkexAccountEventType::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_okex_order_msg_roundtrip() {
        let msg = OkexOrderMsg {
            msg_type: OkexAccountEventType::OrderUpdate,
            inst_id: "BTC-USDT-SWAP".to_string(),
            inst_type: "SWAP".to_string(),
            ord_id: "123456789".to_string(),
            cl_ord_id: "client_order_1".to_string(),
            state: "filled".to_string(),
            side: "buy".to_string(),
            pos_side: "long".to_string(),
            ord_type: "limit".to_string(),
            px: 50000.0,
            sz: 1.0,
            avg_px: 49999.0,
            acc_fill_sz: 1.0,
            fill_px: 49999.0,
            fill_sz: 1.0,
            trade_id: "987654321".to_string(),
            fee: -0.5,
            fee_ccy: "USDT".to_string(),
            pnl: 10.5,
            reduce_only: false,
            c_time: 1704876947000,
            u_time: 1704876948000,
            fill_time: 1704876948000,
        };

        let bytes = msg.to_bytes();
        let parsed = OkexOrderMsg::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.inst_id, msg.inst_id);
        assert_eq!(parsed.ord_id, msg.ord_id);
        assert_eq!(parsed.state, msg.state);
        assert_eq!(parsed.px, msg.px);
        assert_eq!(parsed.fill_sz, msg.fill_sz);
        assert_eq!(parsed.fee, msg.fee);
        assert_eq!(parsed.c_time, msg.c_time);
    }
}
