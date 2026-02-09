use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::common::basic_account_msg::BasicAccountEventType;

/// Bitget 订单更新消息（紧凑版，结构参考 GateBasicOrderMsg）
#[derive(Debug, Clone)]
pub struct BitgetBasicOrderMsg {
    pub msg_type: BasicAccountEventType,
    /// 6=BitgetMargin(SPOT), 7=BitgetFutures
    pub venue: u8,
    /// 毫秒级时间戳
    pub event_time: i64,
    pub symbol_length: u32,
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: i64,
    /// Side: 1=Buy, 2=Sell
    pub side: u8,
    /// OrderType: 1=Limit, 3=Market
    pub order_type: u8,
    /// TimeInForce: 0=GTC, 1=IOC, 2=FOK, 3=GTX/PostOnly
    pub time_in_force: u8,
    /// ExecutionType: 1=New, 2=Canceled, 5=Trade, 6=Expired
    pub execution_type: u8,
    /// OrderStatus: 1=New, 2=PartiallyFilled, 3=Filled, 4=Canceled, 5=Expired
    pub order_status: u8,
    /// 是否 maker：1=maker, 0=taker/unknown
    pub is_maker: u8,
    pub price: f64,
    pub quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub commission_asset_length: u32,
    pub commission_asset: String,
}

impl BitgetBasicOrderMsg {
    /// Bitget 现货/杠杆（对应 TradingVenue::BitgetMargin = 6）
    pub const VENUE_SPOT: u8 = 6;
    /// Bitget 合约（对应 TradingVenue::BitgetFutures = 7）
    pub const VENUE_FUTURES: u8 = 7;

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        venue: u8,
        event_time: i64,
        symbol: String,
        order_id: i64,
        client_order_id: i64,
        side: u8,
        order_type: u8,
        time_in_force: u8,
        execution_type: u8,
        order_status: u8,
        is_maker: u8,
        price: f64,
        quantity: f64,
        cumulative_filled_quantity: f64,
        last_executed_price: f64,
        commission_asset: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            venue,
            event_time,
            symbol_length: symbol.len() as u32,
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
            commission_asset_length: commission_asset.len() as u32,
            commission_asset,
        }
    }

    pub fn side_to_u8(side: &str) -> u8 {
        match side.to_ascii_lowercase().as_str() {
            "buy" => 1,
            "sell" => 2,
            _ => 0,
        }
    }

    pub fn order_type_to_u8(order_type: &str) -> u8 {
        match order_type.to_ascii_lowercase().as_str() {
            "limit" | "post_only" | "post-only" => 1,
            "market" => 3,
            _ => 0,
        }
    }

    pub fn time_in_force_to_u8(tif: &str) -> u8 {
        match tif.to_ascii_lowercase().as_str() {
            "gtc" => 0,
            "ioc" => 1,
            "fok" => 2,
            "post_only" | "post-only" | "gtx" => 3,
            _ => 0,
        }
    }

    pub fn status_to_order_status(status: &str) -> u8 {
        match status.to_ascii_lowercase().as_str() {
            "new" | "live" | "init" => 1,
            "partially_filled" | "partially-filled" | "partial-fill" => 2,
            "filled" | "full-fill" => 3,
            "cancelled" | "canceled" => 4,
            "expired" | "rejected" | "rejected_maker" => 5,
            _ => 1,
        }
    }

    pub fn status_to_execution_type(status: &str) -> u8 {
        match status.to_ascii_lowercase().as_str() {
            "new" | "live" | "init" => 1,
            "partially_filled" | "partially-filled" | "partial-fill" => 5,
            "filled" | "full-fill" => 5,
            "cancelled" | "canceled" => 2,
            "expired" => 6,
            "rejected" | "rejected_maker" => 8,
            _ => 1,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 1
            + 8
            + 4
            + self.symbol_length as usize
            + 8 * 2
            + 6
            + 8 * 4
            + 4
            + self.commission_asset_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u8(self.venue);
        buf.put_i64_le(self.event_time);

        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());

        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.client_order_id);

        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        buf.put_u8(self.time_in_force);
        buf.put_u8(self.execution_type);
        buf.put_u8(self.order_status);
        buf.put_u8(self.is_maker);

        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_f64_le(self.last_executed_price);

        buf.put_u32_le(self.commission_asset_length);
        buf.put(self.commission_asset.as_bytes());

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_FIXED_SIZE: usize = 4 + 1 + 8 + 4 + 8 * 2 + 6 + 8 * 4 + 4;
        if data.len() < MIN_FIXED_SIZE {
            anyhow::bail!("BitgetBasicOrderMsg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::OrderUpdate as u32 {
            anyhow::bail!("invalid BitgetBasicOrderMsg type: {}", msg_type);
        }

        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();

        let symbol_length = cursor.get_u32_le();
        if cursor.remaining() < symbol_length as usize {
            anyhow::bail!("BitgetBasicOrderMsg truncated before symbol");
        }
        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;

        if cursor.remaining() < 8 * 2 + 6 + 8 * 4 + 4 {
            anyhow::bail!("BitgetBasicOrderMsg truncated after symbol");
        }

        let order_id = cursor.get_i64_le();
        let client_order_id = cursor.get_i64_le();

        let side = cursor.get_u8();
        let order_type = cursor.get_u8();
        let time_in_force = cursor.get_u8();
        let execution_type = cursor.get_u8();
        let order_status = cursor.get_u8();
        let is_maker = cursor.get_u8();

        let price = cursor.get_f64_le();
        let quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();
        let last_executed_price = cursor.get_f64_le();

        let commission_asset_length = cursor.get_u32_le();
        if cursor.remaining() < commission_asset_length as usize {
            anyhow::bail!("BitgetBasicOrderMsg truncated before commission_asset");
        }
        let commission_asset = String::from_utf8(
            cursor
                .copy_to_bytes(commission_asset_length as usize)
                .to_vec(),
        )?;

        Ok(Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            venue,
            event_time,
            symbol_length,
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
            commission_asset_length,
            commission_asset,
        })
    }
}
