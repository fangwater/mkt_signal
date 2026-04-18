use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::common::basic_account_msg::BasicAccountEventType;

/// Bybit 订单更新消息（保留字符串 ID，便于 unified 接口兼容 Bybit 的字符串订单号）
#[derive(Debug, Clone)]
pub struct BybitBasicOrderMsg {
    pub msg_type: BasicAccountEventType,
    /// 4=BybitMargin(SPOT), 5=BybitFutures
    pub venue: u8,
    /// 毫秒级事件时间戳
    pub event_time: i64,
    pub symbol: String,
    /// 统一接口要求 i64；若原始字符串无法解析为 i64，则使用稳定哈希映射
    pub order_id: i64,
    pub order_id_str: String,
    /// 统一接口要求 i64；若原始字符串无法解析为 i64，则使用稳定哈希映射
    pub client_order_id: i64,
    pub client_order_id_str: String,
    /// Side: 1=Buy, 2=Sell
    pub side: u8,
    /// OrderType: 1=Limit, 3=Market
    pub order_type: u8,
    /// TimeInForce: 0=GTC, 1=IOC, 2=FOK, 3=GTX/PostOnly
    pub time_in_force: u8,
    /// ExecutionType: 1=New, 2=Canceled, 5=Trade, 6=Expired, 8=Rejected
    pub execution_type: u8,
    /// OrderStatus: 1=New, 2=PartiallyFilled, 3=Filled, 4=Canceled, 5=Expired/Rejected
    pub order_status: u8,
    /// 是否 maker：1=maker, 0=taker/unknown
    pub is_maker: u8,
    pub price: f64,
    pub quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub commission_asset: String,
    pub raw_status: String,
    pub raw_execution_type: String,
}

impl BybitBasicOrderMsg {
    pub const VENUE_SPOT: u8 = 4;
    pub const VENUE_FUTURES: u8 = 5;

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        venue: u8,
        event_time: i64,
        symbol: String,
        order_id: i64,
        order_id_str: String,
        client_order_id: i64,
        client_order_id_str: String,
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
        raw_status: String,
        raw_execution_type: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            venue,
            event_time,
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
            "limit" | "postonly" | "post_only" | "post-only" => 1,
            "market" => 3,
            _ => 0,
        }
    }

    pub fn time_in_force_to_u8(tif: &str) -> u8 {
        match tif.to_ascii_lowercase().as_str() {
            "gtc" => 0,
            "ioc" => 1,
            "fok" => 2,
            "postonly" | "post_only" | "post-only" => 3,
            _ => 0,
        }
    }

    pub fn status_to_order_status(status: &str) -> u8 {
        match status.to_ascii_lowercase().as_str() {
            "new" | "created" | "untriggered" | "triggered" | "active" => 1,
            "partiallyfilled" | "partially_filled" | "partialfill" | "partial-fill" => 2,
            "filled" => 3,
            "cancelled" | "canceled" | "deactivated" => 4,
            "partiallyfilledcanceled" | "rejected" | "expired" => 5,
            _ => 1,
        }
    }

    pub fn status_to_execution_type(status: &str) -> u8 {
        match status.to_ascii_lowercase().as_str() {
            "new" | "created" | "untriggered" | "triggered" | "active" => 1,
            "partiallyfilled" | "partially_filled" | "partialfill" | "partial-fill" => 5,
            "filled" => 5,
            "cancelled" | "canceled" | "deactivated" => 2,
            "expired" => 6,
            "partiallyfilledcanceled" | "rejected" => 8,
            _ => 1,
        }
    }

    pub fn stable_i64_from_str(value: &str) -> i64 {
        if value.is_empty() {
            return 0;
        }
        if let Ok(parsed) = value.parse::<i64>() {
            return parsed;
        }

        // 使用稳定的 FNV-1a 64-bit 哈希，避免 DefaultHasher 的随机种子。
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in value.as_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash & 0x7fff_ffff_ffff_ffff) as i64
    }

    pub fn to_bytes(&self) -> Bytes {
        let symbol_len = self.symbol.len();
        let order_id_str_len = self.order_id_str.len();
        let client_order_id_str_len = self.client_order_id_str.len();
        let commission_asset_len = self.commission_asset.len();
        let raw_status_len = self.raw_status.len();
        let raw_execution_type_len = self.raw_execution_type.len();

        let total_size = 4
            + 1
            + 8
            + 4
            + symbol_len
            + 8
            + 4
            + order_id_str_len
            + 8
            + 4
            + client_order_id_str_len
            + 6
            + 8 * 4
            + 4
            + commission_asset_len
            + 4
            + raw_status_len
            + 4
            + raw_execution_type_len;

        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u8(self.venue);
        buf.put_i64_le(self.event_time);

        put_string(&mut buf, &self.symbol);
        buf.put_i64_le(self.order_id);
        put_string(&mut buf, &self.order_id_str);
        buf.put_i64_le(self.client_order_id);
        put_string(&mut buf, &self.client_order_id_str);

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

        put_string(&mut buf, &self.commission_asset);
        put_string(&mut buf, &self.raw_status);
        put_string(&mut buf, &self.raw_execution_type);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_FIXED_SIZE: usize = 4 + 1 + 8 + 4 + 8 + 4 + 8 + 4 + 6 + 8 * 4 + 4 + 4 + 4;
        if data.len() < MIN_FIXED_SIZE {
            anyhow::bail!("BybitBasicOrderMsg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::OrderUpdate as u32 {
            anyhow::bail!("invalid BybitBasicOrderMsg type: {}", msg_type);
        }

        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();
        let symbol = get_string(&mut cursor, "symbol")?;
        let order_id = cursor.get_i64_le();
        let order_id_str = get_string(&mut cursor, "order_id_str")?;
        let client_order_id = cursor.get_i64_le();
        let client_order_id_str = get_string(&mut cursor, "client_order_id_str")?;

        if cursor.remaining() < 6 + 8 * 4 + 4 + 4 + 4 {
            anyhow::bail!("BybitBasicOrderMsg truncated before numeric fields");
        }

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

        let commission_asset = get_string(&mut cursor, "commission_asset")?;
        let raw_status = get_string(&mut cursor, "raw_status")?;
        let raw_execution_type = get_string(&mut cursor, "raw_execution_type")?;

        Ok(Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            venue,
            event_time,
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
        })
    }
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32_le(value.len() as u32);
    buf.put(value.as_bytes());
}

fn get_string(cursor: &mut Bytes, label: &str) -> Result<String> {
    if cursor.remaining() < 4 {
        anyhow::bail!("BybitBasicOrderMsg truncated before {}", label);
    }
    let len = cursor.get_u32_le() as usize;
    if cursor.remaining() < len {
        anyhow::bail!("BybitBasicOrderMsg truncated reading {}", label);
    }
    Ok(String::from_utf8(cursor.copy_to_bytes(len).to_vec())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_hash_falls_back_for_non_numeric_ids() {
        let a = BybitBasicOrderMsg::stable_i64_from_str("abcdef");
        let b = BybitBasicOrderMsg::stable_i64_from_str("abcdef");
        let c = BybitBasicOrderMsg::stable_i64_from_str("123");

        assert_eq!(a, b);
        assert_eq!(c, 123);
        assert!(a > 0);
    }

    #[test]
    fn round_trip_bytes() {
        let msg = BybitBasicOrderMsg::create(
            BybitBasicOrderMsg::VENUE_FUTURES,
            1_700_000_000_000,
            "BTCUSDT".to_string(),
            11,
            "oid-1".to_string(),
            22,
            "clid-1".to_string(),
            1,
            1,
            0,
            5,
            2,
            1,
            100_000.0,
            0.5,
            0.1,
            99_999.5,
            "USDT".to_string(),
            "PartiallyFilled".to_string(),
            "Trade".to_string(),
        );

        let decoded = BybitBasicOrderMsg::from_bytes(&msg.to_bytes()).expect("decode");
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.order_id_str, "oid-1");
        assert_eq!(decoded.client_order_id_str, "clid-1");
        assert_eq!(decoded.execution_type, 5);
        assert_eq!(decoded.order_status, 2);
        assert!((decoded.last_executed_price - 99_999.5).abs() < 1e-9);
    }
}
