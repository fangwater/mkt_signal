use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::signal::trade_signal::SignalType;

pub const PRE_TRADE_SIGNAL_RECORD_CHANNEL: &str = "pre_trade_signal_record";

/// 通用的信号记录消息，用于将策略创建成功后的信号上下文推送到 Iceoryx。
#[derive(Debug, Clone)] 
pub struct SignalRecordMessage {
    pub strategy_id: i32,
    pub signal_type: SignalType,
    pub context: Vec<u8>,
    pub timestamp_us: i64,
}

impl SignalRecordMessage {
    pub fn new(
        strategy_id: i32,
        signal_type: SignalType,
        context: Vec<u8>,
        timestamp_us: i64,
    ) -> Self {
        Self {
            strategy_id,
            signal_type,
            context,
            timestamp_us,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + 4 + 4 + self.context.len() + 8);
        buf.put_i32_le(self.strategy_id);
        buf.put_u32_le(self.signal_type.clone() as u32);
        buf.put_u32_le(self.context.len() as u32);
        buf.extend_from_slice(&self.context);
        buf.put_i64_le(self.timestamp_us);
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self> {
        if bytes.remaining() < 12 {
            return Err(anyhow!("signal record payload too short"));
        }
        let strategy_id = bytes.get_i32_le();
        let signal_type_raw = bytes.get_u32_le();
        let signal_type = SignalType::from_u32(signal_type_raw)
            .ok_or_else(|| anyhow!("unknown signal type in record: {}", signal_type_raw))?;
        let ctx_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < ctx_len {
            return Err(anyhow!(
                "signal record context length mismatch: need {}, got {}",
                ctx_len,
                bytes.remaining()
            ));
        }
        let context_bytes = bytes.copy_to_bytes(ctx_len);
        let context = context_bytes.to_vec();
        let timestamp_us = if bytes.remaining() >= 8 {
            bytes.get_i64_le()
        } else {
            0
        };
        Ok(Self {
            strategy_id,
            signal_type,
            context,
            timestamp_us,
        })
    }
}
