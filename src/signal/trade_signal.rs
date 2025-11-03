use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
pub enum SignalType {
    BinSingleForwardArbOpenMT = 1,  // 币安单所正向套利触发开仓（MT 流程）
    BinSingleForwardArbOpenMM = 2,  // 币安单所正向套利触发开仓（MM 流程）
    BinSingleForwardArbHedgeMT = 3, // 币安单所正向套利触发对冲（MT 流程）
    BinSingleForwardArbHedgeMM = 4, // 币安单所正向套利触发对冲（MM 流程）
    BinSingleForwardArbCancelMT = 5, // MT 撤单
    BinSingleForwardArbCancelMM = 6, // MM 撤单
                                    // 未来可以添加更多信号类型:
                                    // BinSingleReverseArb,  // 币安单所反向套利
                                    // CrossExchangeArb,     // 跨交易所套利
                                    // TriangularArb,        // 三角套利
                                    // StatisticalArb,       // 统计套利
                                    // etc...
}

impl SignalType {
    /// 从u32转换为SignalType
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(SignalType::BinSingleForwardArbOpenMT),
            2 => Some(SignalType::BinSingleForwardArbOpenMM),
            3 => Some(SignalType::BinSingleForwardArbHedgeMT),
            4 => Some(SignalType::BinSingleForwardArbHedgeMM),
            5 => Some(SignalType::BinSingleForwardArbCancelMT),
            6 => Some(SignalType::BinSingleForwardArbCancelMM),
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
        let total_size = 4 + 8 + 8 + 4 + context_length as usize;
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
    pub fn get_context_length(data: &[u8]) -> Option<u32> {
        if data.len() < 24 {
            return None;
        }
        Some(u32::from_le_bytes([data[20], data[21], data[22], data[23]]))
    }
}
