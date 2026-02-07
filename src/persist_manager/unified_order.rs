/// 统一订单格式（仅结构定义，不含持久化接线逻辑）。
///
/// 设计约束：
/// - 不使用 `String`，所有可变文本统一使用二进制字节表示。
/// - 枚举类字段统一使用 `u8` 紧凑编码，编码值与现有模块保持一致：
///   - `venue` 对齐 `TradingVenue::to_u8()`
///   - `ttype` 对齐 `OrderType` 的二进制编码
///   - `side` 对齐 `Side::to_u8()`
///   - `status` 对齐 `OrderStatus::to_u8()`
/// - `from_key` 采用 `u32 + bytes` 形式，放在结构尾部，满足不定长扩展需求。
#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedOrderRecord {
    /// 交易标的字节长度（`symbol` 的长度，单位：byte）。
    pub symbol_len: u16,
    /// 交易标的字节内容（例如 `BTCUSDT` 的 UTF-8 bytes）。
    pub symbol: Vec<u8>,

    /// 订单创建时间戳（订单首次生成时间）。
    pub create_ts: i64,
    /// 订单最近一次状态变更时间戳。
    pub update_ts: i64,
    /// 触发该订单的信号时间戳。
    pub signal_ts: i64,

    /// 客户端自定义订单 ID（幂等与追踪）。
    pub client_order_id: i64,

    /// 交易所/交易场所（`u8`，对齐 `TradingVenue` 编码）。
    pub venue: u8,
    /// 订单类型（`u8`，对齐 `OrderType` 编码）。
    pub ttype: u8,
    /// 买卖方向（`u8`，对齐 `Side` 编码）。
    pub side: u8,

    /// 下单价格。
    pub price: f64,
    /// 相对参考价的偏移量。
    pub price_offset: f64,
    /// 初始下单数量。
    pub amount_init: f64,
    /// 本次更新对应的数量（如增减仓/成交增量）。
    pub amount_update: f64,

    /// 订单状态（`u8`，对齐 `OrderStatus` 编码）。
    pub status: u8,

    /// `from_key` 字节长度（单位：byte）。
    pub from_key_len: u32,
    /// 来源规则标识（不限制长度，原始二进制 bytes）。
    pub from_key: Vec<u8>,
}

impl UnifiedOrderRecord {
    /// 根据 `symbol` 与 `from_key` 自动回填长度字段。
    pub fn refresh_lengths(&mut self) {
        self.symbol_len = self.symbol.len() as u16;
        self.from_key_len = self.from_key.len() as u32;
    }

    /// 检查长度字段是否与实际 bytes 一致。
    pub fn length_fields_consistent(&self) -> bool {
        self.symbol_len as usize == self.symbol.len()
            && self.from_key_len as usize == self.from_key.len()
    }
}
