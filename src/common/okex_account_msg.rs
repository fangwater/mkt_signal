//! OKX 账户事件消息定义（独立于 Binance）

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

/// OKX 订单更新消息（暂保留定义，仍用于 WS 订单日志）
#[derive(Debug, Clone)]
pub struct OkexOrderMsg {
    pub msg_type: OkexAccountEventType,
    pub inst_id: String,
    pub inst_type: u8,
    pub ord_id: i64,
    pub cl_ord_id: i64,
    pub state: u8,
    pub side: u8,
    pub ord_type: u8,
    pub cancel_source: u8,
    pub amend_source: u8,
    pub price: f64,
    pub quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub create_time: i64,
    pub update_time: i64,
    pub fill_time: i64,
}

impl OkexOrderMsg {
    /// 将 OKX instType 文本映射为紧凑编码
    pub fn inst_type_to_u8(inst_type: &str) -> u8 {
        match inst_type {
            "MARGIN" => 1,
            "SWAP" => 2,
            "FUTURES" => 3,
            "OPTION" => 4,
            _ => 0,
        }
    }

    /// 反向映射，便于日志展示
    pub fn inst_type_to_str(code: u8) -> &'static str {
        match code {
            1 => "MARGIN",
            2 => "SWAP",
            3 => "FUTURES",
            4 => "OPTION",
            _ => "UNKNOWN",
        }
    }

    /// 将订单状态文本压缩为 u8
    pub fn state_to_u8(state: &str) -> u8 {
        match state {
            "canceled" => 1,
            "live" => 2,
            "partially_filled" => 3,
            "filled" => 4,
            "mmp_canceled" => 5,
            _ => 0,
        }
    }

    /// 状态码还原为字符串，仅用于日志展示
    pub fn state_to_str(code: u8) -> &'static str {
        match code {
            1 => "canceled",
            2 => "live",
            3 => "partially_filled",
            4 => "filled",
            5 => "mmp_canceled",
            _ => "unknown",
        }
    }

    /// 撤单来源码转字符串
    pub fn cancel_source_to_str(code: u8) -> &'static str {
        match code {
            0 => "system",
            1 => "user",
            2 => "pre_reduce",
            3 => "risk_control",
            4 => "borrow_limit",
            6 => "adl_trigger",
            7 => "delivery",
            9 => "funding_fee_insufficient",
            10 => "option_expiry",
            13 => "fok_not_filled",
            14 => "ioc_partial",
            15 => "price_out_of_range",
            17 => "position_filled",
            20 => "countdown_cancel",
            21 => "tp_sl_linked_position_closed",
            22 => "better_same_side_order_auto_cancel",
            23 => "better_existing_order_auto_cancel",
            27 => "slippage_protection",
            31 => "post_only_would_cross",
            32 => "self_trade_protection",
            33 => "too_many_matches",
            36 => "linked_sl_triggered_cancel_tp",
            37 => "linked_sl_cancelled_cancel_tp",
            38 => "mmp_cancelled_by_user",
            39 => "mmp_triggered",
            42 => "chase_distance_exceeded",
            43 => "price_worse_than_index",
            44 => "auto_convert_fail",
            45 => "elp_price_check_failed",
            46 => "delta_reduce_cancel",
            _ => "unknown",
        }
    }

    /// 改单来源码转字符串
    pub fn amend_source_to_str(code: u8) -> &'static str {
        match code {
            1 => "user_amend",
            2 => "user_amend_reduce_only_current",
            3 => "user_order_reduce_only_current",
            4 => "existing_reduce_only",
            5 => "option_follow_px_change",
            _ => "unknown",
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let inst_id_bytes = self.inst_id.as_bytes();
        let total_size = 4  // msg_type
            + 1  // inst_type (u8)
            + 4 + inst_id_bytes.len()
            + 8  // ord_id i64
            + 8  // cl_ord_id i64
            + 1  // state u8
            + 1  // side u8
            + 1  // ord_type u8
            + 1  // cancel_source u8
            + 1  // amend_source u8
            + 8 * 3  // price, quantity, cumulative_filled_quantity
            + 8 * 3; // create_time, update_time, fill_time

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_u8(self.inst_type);

        // 字符串字段: 长度 + 内容
        buf.put_u32_le(inst_id_bytes.len() as u32);
        buf.put(inst_id_bytes);

        buf.put_i64_le(self.ord_id);

        buf.put_i64_le(self.cl_ord_id);

        buf.put_u8(self.state);

        buf.put_u8(self.side);

        buf.put_u8(self.ord_type);

        buf.put_u8(self.cancel_source);

        buf.put_u8(self.amend_source);

        // 数值字段
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);

        buf.put_i64_le(self.create_time);
        buf.put_i64_le(self.update_time);
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

        if cursor.remaining() < 1 {
            anyhow::bail!("Not enough data for inst_type");
        }
        let inst_type = cursor.get_u8();

        let inst_id = {
            if cursor.remaining() < 4 {
                anyhow::bail!("Not enough data for inst_id length");
            }
            let len = cursor.get_u32_le() as usize;
            if cursor.remaining() < len {
                anyhow::bail!("Not enough data for inst_id content");
            }
            String::from_utf8(cursor.copy_to_bytes(len).to_vec())?
        };
        if cursor.remaining() < 8 {
            anyhow::bail!("Not enough data for ord_id");
        }
        let ord_id = cursor.get_i64_le();
        if cursor.remaining() < 8 {
            anyhow::bail!("Not enough data for cl_ord_id");
        }
        let cl_ord_id = cursor.get_i64_le();
        if cursor.remaining() < 1 {
            anyhow::bail!("Not enough data for state");
        }
        let state = cursor.get_u8();
        if cursor.remaining() < 1 {
            anyhow::bail!("Not enough data for side");
        }
        let side = cursor.get_u8();
        if cursor.remaining() < 1 {
            anyhow::bail!("Not enough data for ord_type");
        }
        let ord_type = cursor.get_u8();
        if cursor.remaining() < 1 {
            anyhow::bail!("Not enough data for cancel_source");
        }
        let cancel_source = cursor.get_u8();
        if cursor.remaining() < 1 {
            anyhow::bail!("Not enough data for amend_source");
        }
        let amend_source = cursor.get_u8();

        if cursor.remaining() < 8 * 3 {
            anyhow::bail!("Not enough data for numeric fields");
        }

        let price = cursor.get_f64_le();
        let quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();

        if cursor.remaining() < 8 * 3 {
            anyhow::bail!("Not enough data for timestamps");
        }
        let create_time = cursor.get_i64_le();
        let update_time = cursor.get_i64_le();
        let fill_time = cursor.get_i64_le();

        Ok(Self {
            msg_type: OkexAccountEventType::OrderUpdate,
            inst_id,
            inst_type,
            ord_id,
            cl_ord_id,
            state,
            side,
            ord_type,
            cancel_source,
            amend_source,
            price,
            quantity,
            cumulative_filled_quantity,
            create_time,
            update_time,
            fill_time,
        })
    }
}

/// OKX 余额消息（仅一个时间字段）
#[derive(Debug, Clone)]
pub struct OkexBalanceMsg {
    pub msg_type: OkexAccountEventType,
    pub timestamp: i64,
    pub balance: f64,
}

impl OkexBalanceMsg {
    pub fn create(timestamp: i64, balance: f64) -> Self {
        Self {
            msg_type: OkexAccountEventType::BalanceUpdate,
            timestamp,
            balance,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_f64_le(self.balance);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("okex balance msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != OkexAccountEventType::BalanceUpdate as u32 {
            anyhow::bail!("invalid okex balance msg type: {}", msg_type);
        }

        let timestamp = cursor.get_i64_le();
        let balance = cursor.get_f64_le();

        Ok(Self {
            msg_type: OkexAccountEventType::BalanceUpdate,
            timestamp,
            balance,
        })
    }
}

/// OKX 持仓消息（仅一个时间字段）
#[derive(Debug, Clone)]
pub struct OkexPositionMsg {
    pub msg_type: OkexAccountEventType,
    pub timestamp: i64,
    pub inst_id_length: u32,
    pub position_side: char,
    pub padding: [u8; 3],
    pub inst_id: String,
    pub position_amount: f64,
}

impl OkexPositionMsg {
    pub fn create(
        timestamp: i64,
        inst_id: String,
        position_side: char,
        position_amount: f64,
    ) -> Self {
        Self {
            msg_type: OkexAccountEventType::PositionUpdate,
            timestamp,
            inst_id_length: inst_id.len() as u32,
            position_side,
            padding: [0u8; 3],
            inst_id,
            position_amount,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 4 + 1 + 3 + self.inst_id_length as usize + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_u32_le(self.inst_id_length);
        buf.put_u8(self.position_side as u8);
        buf.put(&self.padding[..]);
        buf.put(self.inst_id.as_bytes());
        buf.put_f64_le(self.position_amount);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 4 + 1 + 3 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("okex position msg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != OkexAccountEventType::PositionUpdate as u32 {
            anyhow::bail!("invalid okex position msg type: {}", msg_type);
        }
        let timestamp = cursor.get_i64_le();
        let inst_id_length = cursor.get_u32_le();
        let position_side = cursor.get_u8() as char;
        let mut padding = [0u8; 3];
        cursor.copy_to_slice(&mut padding);
        if cursor.remaining() < inst_id_length as usize + 8 {
            anyhow::bail!("okex position msg truncated");
        }
        let inst_id = String::from_utf8(cursor.copy_to_bytes(inst_id_length as usize).to_vec())?;
        let position_amount = cursor.get_f64_le();

        Ok(Self {
            msg_type: OkexAccountEventType::PositionUpdate,
            timestamp,
            inst_id_length,
            position_side,
            padding,
            inst_id,
            position_amount,
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
