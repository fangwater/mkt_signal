use bytes::{BufMut, Bytes, BytesMut};

pub const TRADE_SIGNAL_HEADER_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    ArbOpen = 1,          // 套利开仓信号
    ArbCancel = 3,        // 套利撤单信号
    ArbClose = 4,         // 套利平仓信号，和开仓信号类似，区别是如果对应方向头寸为0就不执行
    MMOpen = 5,           // 做市开仓信号
    MMCancel = 6,         // 做市撤单信号
    MMHedge = 7,          // 做市对冲信号
    MMCancelTrigger = 8,  // 做市撤单触发信号
    ArbCancelTrigger = 9, // 套利撤单触发信号
    ArbHedge = 10,        // 套利对冲回包信号
    MMOpenBatch = 11,     // 做市批量开仓信号
}

impl SignalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalType::ArbOpen => "ArbOpen",
            SignalType::ArbCancel => "ArbCancel",
            SignalType::ArbClose => "ArbClose",
            SignalType::MMOpen => "MMOpen",
            SignalType::MMCancel => "MMCancel",
            SignalType::MMHedge => "MMHedge",
            SignalType::MMCancelTrigger => "MMCancelTrigger",
            SignalType::ArbCancelTrigger => "ArbCancelTrigger",
            SignalType::ArbHedge => "ArbHedge",
            SignalType::MMOpenBatch => "MMOpenBatch",
        }
    }

    /// 从u32转换为SignalType
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(SignalType::ArbOpen),
            3 => Some(SignalType::ArbCancel),
            4 => Some(SignalType::ArbClose),
            5 => Some(SignalType::MMOpen),
            6 => Some(SignalType::MMCancel),
            7 => Some(SignalType::MMHedge),
            8 => Some(SignalType::MMCancelTrigger),
            9 => Some(SignalType::ArbCancelTrigger),
            10 => Some(SignalType::ArbHedge),
            11 => Some(SignalType::MMOpenBatch),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub signal_type: SignalType, //信号种类
    pub generation_time: i64,    //信号的产生时间
    pub handle_time: f64,        //信号被pre-process处理的时间
    pub context: Bytes,          //信号的具体内容，信号上下文
}

#[derive(Debug, Clone, Copy)]
pub struct TradeSignalView<'a> {
    pub signal_type: SignalType,
    pub generation_time: i64,
    pub handle_time: f64,
    pub context: &'a [u8],
}

impl TradeSignal {
    /// 创建一个新的交易信号
    pub fn create(
        signal_type: SignalType,
        generation_time: i64,
        handle_time: f64,
        context: Bytes,
    ) -> Self {
        Self {
            signal_type,
            generation_time,
            handle_time,
            context,
        }
    }

    /// 将交易信号转换为字节数组
    /// 格式: signal_type(4) + generation_time(8) + handle_time(8) + context_length(4) + context
    pub fn to_bytes(&self) -> Bytes {
        let context_length = self.context.len() as u32;
        // 计算总大小: signal_type(4) + generation_time(8) + handle_time(8) + context_length(4) + context
        let total_size = TRADE_SIGNAL_HEADER_LEN + context_length as usize;
        let mut buf = BytesMut::with_capacity(total_size);

        // 写入信号类型
        buf.put_u32_le(self.signal_type.clone() as u32);

        // 写入生成时间
        buf.put_i64_le(self.generation_time);

        // 写入处理时间
        buf.put_f64_le(self.handle_time);

        // 写入context长度
        buf.put_u32_le(context_length);

        // 写入context内容
        buf.put(self.context.clone());

        buf.freeze()
    }

    pub fn write_parts_to_slice(
        signal_type: SignalType,
        generation_time: i64,
        handle_time: f64,
        context: &[u8],
        out: &mut [u8],
    ) -> Result<usize, String> {
        let context_length = u32::try_from(context.len())
            .map_err(|_| format!("context too large: {} bytes", context.len()))?;
        let total_size = TRADE_SIGNAL_HEADER_LEN
            .checked_add(context.len())
            .ok_or_else(|| "trade signal size overflow".to_string())?;
        if out.len() < total_size {
            return Err(format!(
                "trade signal output too small: need {} bytes, got {}",
                total_size,
                out.len()
            ));
        }

        out[0..4].copy_from_slice(&(signal_type as u32).to_le_bytes());
        out[4..12].copy_from_slice(&generation_time.to_le_bytes());
        out[12..20].copy_from_slice(&handle_time.to_le_bytes());
        out[20..24].copy_from_slice(&context_length.to_le_bytes());
        out[TRADE_SIGNAL_HEADER_LEN..total_size].copy_from_slice(context);
        Ok(total_size)
    }

    /// 从字节数组解析交易信号
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 24 {
            return Err("数据长度不足，最少需要24字节".to_string());
        }

        // 解析信号类型 (offset 0)
        let signal_type_u32 = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let signal_type = SignalType::from_u32(signal_type_u32)
            .ok_or_else(|| format!("未知的信号类型: {}", signal_type_u32))?;

        // 解析生成时间 (offset 4)
        let generation_time = i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);

        // 解析处理时间 (offset 12)
        let handle_time = f64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]);

        // 解析context长度 (offset 20)
        let context_length = u32::from_le_bytes([data[20], data[21], data[22], data[23]]) as usize;

        // 检查剩余数据长度
        if data.len() < 24 + context_length {
            return Err(format!(
                "数据长度不足，期望{}字节，实际{}字节",
                24 + context_length,
                data.len()
            ));
        }

        // 解析context内容 (offset 24)
        let context = Bytes::copy_from_slice(&data[24..24 + context_length]);

        Ok(Self {
            signal_type,
            generation_time,
            handle_time,
            context,
        })
    }

    /// 从Bytes解析交易信号
    pub fn from_bytes_ref(data: &Bytes) -> Result<Self, String> {
        Self::from_bytes(data)
    }

    /// 获取信号类型（零拷贝）
    #[inline]
    pub fn get_signal_type(data: &[u8]) -> Option<SignalType> {
        if data.len() < 4 {
            return None;
        }
        let signal_type_u32 = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        SignalType::from_u32(signal_type_u32)
    }

    /// 获取生成时间（零拷贝）
    #[inline]
    pub fn get_generation_time(data: &[u8]) -> Option<i64> {
        if data.len() < 12 {
            return None;
        }
        Some(i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]))
    }

    /// 获取处理时间（零拷贝）
    #[inline]
    pub fn get_handle_time(data: &[u8]) -> Option<f64> {
        if data.len() < 20 {
            return None;
        }
        Some(f64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]))
    }

    /// 获取context长度（零拷贝）
    #[inline]
    pub fn encoded_len(data: &[u8]) -> Option<usize> {
        let context_length = Self::get_context_length(data)? as usize;
        let total_len = TRADE_SIGNAL_HEADER_LEN.checked_add(context_length)?;
        (data.len() >= total_len).then_some(total_len)
    }

    pub fn get_context_length(data: &[u8]) -> Option<u32> {
        if data.len() < 24 {
            return None;
        }
        Some(u32::from_le_bytes([data[20], data[21], data[22], data[23]]))
    }
}

impl<'a> TradeSignalView<'a> {
    pub fn from_bytes(data: &'a [u8]) -> Result<Self, String> {
        if data.len() < 24 {
            return Err("数据长度不足，最少需要24字节".to_string());
        }

        let signal_type_u32 = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let signal_type = SignalType::from_u32(signal_type_u32)
            .ok_or_else(|| format!("未知的信号类型: {}", signal_type_u32))?;
        let generation_time = i64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let handle_time = f64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]);
        let context_length = u32::from_le_bytes([data[20], data[21], data[22], data[23]]) as usize;
        if data.len() < 24 + context_length {
            return Err(format!(
                "数据长度不足，期望{}字节，实际{}字节",
                24 + context_length,
                data.len()
            ));
        }

        Ok(Self {
            signal_type,
            generation_time,
            handle_time,
            context: &data[24..24 + context_length],
        })
    }

    pub fn from_exact_bytes(data: &'a [u8]) -> Result<Self, String> {
        let signal = Self::from_bytes(data)?;
        let expected_len = TRADE_SIGNAL_HEADER_LEN + signal.context.len();
        if expected_len != data.len() {
            return Err(format!(
                "信号长度不匹配，期望{}字节，实际{}字节",
                expected_len,
                data.len()
            ));
        }
        Ok(signal)
    }

    pub fn to_owned_signal(self) -> TradeSignal {
        TradeSignal {
            signal_type: self.signal_type,
            generation_time: self.generation_time,
            handle_time: self.handle_time,
            context: Bytes::copy_from_slice(self.context),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_signal_view_borrows_context_and_matches_owned_parse() {
        let signal = TradeSignal::create(
            SignalType::ArbOpen,
            123456,
            7.5,
            Bytes::from_static(b"ctx-bytes"),
        );
        let bytes = signal.to_bytes();

        let view = TradeSignalView::from_bytes(bytes.as_ref()).expect("view parse");
        let owned = TradeSignal::from_bytes(bytes.as_ref()).expect("owned parse");

        assert_eq!(view.signal_type, owned.signal_type);
        assert_eq!(view.generation_time, owned.generation_time);
        assert_eq!(view.handle_time, owned.handle_time);
        assert_eq!(view.context, owned.context.as_ref());
        assert_eq!(view.to_owned_signal().context.as_ref(), b"ctx-bytes");
    }

    #[test]
    fn trade_signal_view_exact_parse_rejects_trailing_bytes() {
        let signal = TradeSignal::create(
            SignalType::ArbOpen,
            123456,
            7.5,
            Bytes::from_static(b"ctx-bytes"),
        );
        let mut bytes = signal.to_bytes().to_vec();
        bytes.push(0);

        assert!(TradeSignalView::from_bytes(&bytes).is_ok());
        assert!(TradeSignalView::from_exact_bytes(&bytes).is_err());
    }

    #[test]
    fn write_parts_to_slice_matches_to_bytes() {
        let signal = TradeSignal::create(
            SignalType::ArbOpen,
            123456,
            7.5,
            Bytes::from_static(b"ctx-bytes"),
        );
        let expected = signal.to_bytes();
        let mut out = [0u8; 128];
        let written = TradeSignal::write_parts_to_slice(
            SignalType::ArbOpen,
            123456,
            7.5,
            b"ctx-bytes",
            &mut out,
        )
        .expect("write signal");

        assert_eq!(written, expected.len());
        assert_eq!(&out[..written], expected.as_ref());
    }
}
