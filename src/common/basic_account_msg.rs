//! Basic 账户事件消息定义（独立于交易所）

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Basic 账户事件类型
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasicAccountEventType {
    /// 订单更新
    OrderUpdate = 4001,
    /// 账户余额更新
    BalanceUpdate = 4002,
    /// 持仓更新
    PositionUpdate = 4003,
    /// 借贷利息更新
    BorrowInterest = 4004,
    /// 合约未实现盈亏更新
    UnrealizedPnlUpdate = 4005,
    /// 错误
    Error = 4999,
}

/// Basic 账户事件来源范围。
///
/// 说明：
/// - 只有 Binance 需要区分 standard spot / standard um / unified
/// - 其他交易所当前统一使用 unified 口径，便于 pre_trade 路由与理解
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BasicAccountScope {
    Unknown = 0,
    BinanceUnified = 1,
    BinanceStdSpot = 2,
    BinanceStdUm = 3,
    OkexUnified = 10,
    GateUnified = 11,
    BitgetUnified = 12,
    BybitUnified = 13,
}

impl BasicAccountScope {
    pub fn from_u32(value: u32) -> Self {
        match value {
            1 => Self::BinanceUnified,
            2 => Self::BinanceStdSpot,
            3 => Self::BinanceStdUm,
            10 => Self::OkexUnified,
            11 => Self::GateUnified,
            12 => Self::BitgetUnified,
            13 => Self::BybitUnified,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::BinanceUnified => "binance_unified",
            Self::BinanceStdSpot => "binance_std_spot",
            Self::BinanceStdUm => "binance_std_um",
            Self::OkexUnified => "okex_unified",
            Self::GateUnified => "gate_unified",
            Self::BitgetUnified => "bitget_unified",
            Self::BybitUnified => "bybit_unified",
        }
    }
}

/// Basic 订单更新消息（仍用于 OKX WS 订单日志）
///
/// 数量字段口径：
/// - spot/margin：`quantity` / `cumulative_filled_quantity` 为 base qty
/// - futures(swap)：`quantity` / `cumulative_filled_quantity` 为 contracts
///
/// 策略层统一通过 `MonitorChannel::qty_to_base(...)` 转换成 base qty 做风控与策略计算。
#[derive(Debug, Clone)]
pub struct OkexOrderMsg {
    pub msg_type: BasicAccountEventType,
    pub inst_id: String,
    pub inst_type: u8,
    pub ord_id: i64,
    pub cl_ord_id: i64,
    pub trade_id: i64,
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
    /// 将 instType 文本映射为紧凑编码
    pub fn inst_type_to_u8(inst_type: &str) -> u8 {
        match inst_type {
            "SPOT" => 0,
            "MARGIN" => 1,
            "SWAP" => 2,
            "FUTURES" => 3,
            "OPTION" => 4,
            _ => u8::MAX,
        }
    }

    /// 反向映射，便于日志展示
    pub fn inst_type_to_str(code: u8) -> &'static str {
        match code {
            0 => "SPOT",
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
            + 8  // trade_id i64
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

        buf.put_i64_le(self.trade_id);

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
        if msg_type_u32 != BasicAccountEventType::OrderUpdate as u32 {
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
        if cursor.remaining() < 8 {
            anyhow::bail!("Not enough data for trade_id");
        }
        let trade_id = cursor.get_i64_le();
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
            msg_type: BasicAccountEventType::OrderUpdate,
            inst_id,
            inst_type,
            ord_id,
            cl_ord_id,
            trade_id,
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

/// Binance basic 订单更新消息（统一 margin / UM 的 schema）
///
/// - 外层仍为 `BasicAccountEventMsg(msg_type=OrderUpdate, payload=...)`
/// - payload 自身也以 `BasicAccountEventType::OrderUpdate` 开头（与 OkexOrderMsg / BasicBalanceMsg 一致）
/// - `venue` 用于区分 margin / UM（下游通过 trait 使用，不再依赖 payload 内层 AccountEventType）
#[derive(Debug, Clone)]
pub struct BinanceBasicOrderMsg {
    pub msg_type: BasicAccountEventType,
    /// 1=margin, 2=um
    pub venue: u8,
    pub event_time: i64,
    pub trade_time: i64,
    pub symbol_length: u32,
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: i64,
    pub trade_id: i64,
    /// Side::to_u8()
    pub side: u8,
    /// OrderType::to_u8()
    pub order_type: u8,
    /// TimeInForce::to_u8()
    pub time_in_force: u8,
    /// 1..=7: ExecutionType
    pub execution_type: u8,
    /// 1..=6: OrderStatus
    pub order_status: u8,
    pub is_maker: u8,
    pub price: f64,
    pub quantity: f64,
    pub last_executed_quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub average_price: f64,
    pub commission: f64,
    pub realized_pnl: f64,
    pub commission_asset_length: u32,
    pub commission_asset: String,
}

impl BinanceBasicOrderMsg {
    pub const VENUE_MARGIN: u8 = 1;
    pub const VENUE_UM: u8 = 2;

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        venue: u8,
        event_time: i64,
        trade_time: i64,
        symbol: String,
        order_id: i64,
        client_order_id: i64,
        trade_id: i64,
        side: u8,
        order_type: u8,
        time_in_force: u8,
        execution_type: u8,
        order_status: u8,
        is_maker: bool,
        price: f64,
        quantity: f64,
        last_executed_quantity: f64,
        cumulative_filled_quantity: f64,
        last_executed_price: f64,
        average_price: f64,
        commission: f64,
        realized_pnl: f64,
        commission_asset: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            venue,
            event_time,
            trade_time,
            symbol_length: symbol.len() as u32,
            symbol,
            order_id,
            client_order_id,
            trade_id,
            side,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            is_maker: if is_maker { 1 } else { 0 },
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            average_price,
            commission,
            realized_pnl,
            commission_asset_length: commission_asset.len() as u32,
            commission_asset,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 1
            + 8
            + 8
            + 4
            + self.symbol_length as usize
            + 8 * 3
            + 1 * 6
            + 8 * 8
            + 4
            + self.commission_asset_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u8(self.venue);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.trade_time);

        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());

        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.client_order_id);
        buf.put_i64_le(self.trade_id);

        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        buf.put_u8(self.time_in_force);
        buf.put_u8(self.execution_type);
        buf.put_u8(self.order_status);
        buf.put_u8(self.is_maker);

        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.last_executed_quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_f64_le(self.last_executed_price);
        buf.put_f64_le(self.average_price);
        buf.put_f64_le(self.commission);
        buf.put_f64_le(self.realized_pnl);

        buf.put_u32_le(self.commission_asset_length);
        buf.put(self.commission_asset.as_bytes());

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        // u32 msg_type + u8 venue + i64 + i64 + u32 symbol_len + (symbol bytes) + 3*i64
        // + 6*u8 + 8*f64 + u32 comm_asset_len + (comm_asset bytes)
        const MIN_FIXED_SIZE: usize = 4 + 1 + 8 + 8 + 4 + 8 * 3 + 1 * 6 + 8 * 8 + 4;
        if data.len() < MIN_FIXED_SIZE {
            anyhow::bail!("BinanceBasicOrderMsg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::OrderUpdate as u32 {
            anyhow::bail!("invalid BinanceBasicOrderMsg type: {}", msg_type);
        }

        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();
        let trade_time = cursor.get_i64_le();

        let symbol_length = cursor.get_u32_le();
        if cursor.remaining() < symbol_length as usize {
            anyhow::bail!("BinanceBasicOrderMsg truncated before symbol");
        }
        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;

        if cursor.remaining() < 8 * 3 + 1 * 6 + 8 * 8 + 4 {
            anyhow::bail!("BinanceBasicOrderMsg truncated after symbol");
        }

        let order_id = cursor.get_i64_le();
        let client_order_id = cursor.get_i64_le();
        let trade_id = cursor.get_i64_le();

        let side = cursor.get_u8();
        let order_type = cursor.get_u8();
        let time_in_force = cursor.get_u8();
        let execution_type = cursor.get_u8();
        let order_status = cursor.get_u8();
        let is_maker = cursor.get_u8();

        let price = cursor.get_f64_le();
        let quantity = cursor.get_f64_le();
        let last_executed_quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();
        let last_executed_price = cursor.get_f64_le();
        let average_price = cursor.get_f64_le();
        let commission = cursor.get_f64_le();
        let realized_pnl = cursor.get_f64_le();

        let commission_asset_length = cursor.get_u32_le();
        if cursor.remaining() < commission_asset_length as usize {
            anyhow::bail!("BinanceBasicOrderMsg truncated before commission_asset");
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
            trade_time,
            symbol_length,
            symbol,
            order_id,
            client_order_id,
            trade_id,
            side,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            is_maker,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            average_price,
            commission,
            realized_pnl,
            commission_asset_length,
            commission_asset,
        })
    }
}

/// Gate.io 订单更新消息（精简版，不包含 Gate 不提供的字段）
///
/// 字段映射：
/// - `venue`: 8=GateMargin(spot), 9=GateFutures
/// - `event_time`: update_time_ms
/// - `symbol`: currency_pair
/// - `order_id`: id (parse to i64)
/// - `client_order_id`: text (必须能 parse 为 i64，否则 drop)
/// - `side`: "buy"=1, "sell"=2
/// - `order_type`: "limit"=1, "market"=3
/// - `time_in_force`: "gtc"=0, "ioc"=1, "fok"=2, "poc"=3(GTX/PostOnly)
/// - `execution_type`: 1(New), 2(Canceled), 5(Trade), 6(Expired), 7(TradePrevention)
/// - `order_status`: 1(New), 2(PartiallyFilled), 3(Filled), 4(Canceled), 5(Expired)
/// - `last_executed_price`:
///   - spot.orders_v2: avg_deal_price（成交价/均价口径，Gate 仅提供此字段）
///   - futures.orders: fill_price（成交价口径）
#[derive(Debug, Clone)]
pub struct GateBasicOrderMsg {
    pub msg_type: BasicAccountEventType,
    /// 8=GateMargin(spot), 9=GateFutures
    pub venue: u8,
    pub event_time: i64,
    pub symbol_length: u32,
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: i64,
    /// Side: 1=Buy, 2=Sell
    pub side: u8,
    /// OrderType: 1=Limit, 3=Market
    pub order_type: u8,
    /// TimeInForce: 0=GTC, 1=IOC, 2=FOK, 3=GTX(poc/PostOnly)
    pub time_in_force: u8,
    /// ExecutionType: 1=New, 2=Canceled, 5=Trade, 6=Expired
    pub execution_type: u8,
    /// OrderStatus: 1=New, 2=PartiallyFilled, 3=Filled, 4=Canceled
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

impl GateBasicOrderMsg {
    /// Gate.io 现货 (对应 TradingVenue::GateMargin = 8)
    pub const VENUE_SPOT: u8 = 8;
    /// Gate.io 合约 (对应 TradingVenue::GateFutures = 9)
    pub const VENUE_FUTURES: u8 = 9;

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

    /// Gate.io side 字符串转 u8
    pub fn side_to_u8(side: &str) -> u8 {
        match side.to_lowercase().as_str() {
            "buy" => 1,
            "sell" => 2,
            _ => 0,
        }
    }

    /// Gate.io order type 字符串转 u8
    pub fn order_type_to_u8(order_type: &str) -> u8 {
        match order_type.to_lowercase().as_str() {
            "limit" => 1,
            "market" => 3,
            _ => 0,
        }
    }

    /// Gate.io time_in_force 字符串转 u8
    /// - gtc: Good Till Cancel (0)
    /// - ioc: Immediate Or Cancel (1)
    /// - fok: Fill Or Kill (2)
    /// - poc: Pending Or Cancelled / PostOnly (3) - 被动委托，只挂单不吃单
    pub fn time_in_force_to_u8(tif: &str) -> u8 {
        match tif.to_lowercase().as_str() {
            "gtc" => 0,
            "ioc" => 1,
            "fok" => 2,
            "poc" => 3, // Pending Or Cancelled (PostOnly/GTX) - 被动委托
            _ => 0,
        }
    }

    /// Gate.io event + finish_as 转 (execution_type, order_status)
    ///
    /// event (spot.orders_v2): "put" / "update" / "finish"
    /// status (futures.orders): "open" / "finished"
    ///
    /// finish_as 可能的值:
    /// - filled：全部成交
    /// - cancelled：手动取消
    /// - liquidated：因清算而取消
    /// - ioc：IOC 立即完成
    /// - auto_deleveraging：ADL 完成
    /// - reduce_only：因减仓设置而增仓而取消
    /// - position_close：因平仓而取消
    /// - stp：自成交限制而被撤销
    /// - _new：新建 (futures 特有)
    /// - _update：成交或部分成交 (futures 特有)
    /// - reduce_out: 只减仓被排除的不容易成交的挂单
    pub fn event_to_execution_and_status(event: &str, finish_as: &str) -> (u8, u8) {
        let event_lower = event.to_lowercase();
        let finish_as_lower = finish_as.to_lowercase();

        match event_lower.as_str() {
            // spot.orders_v2 的事件
            "put" => (1, 1),    // New, New
            "update" => (5, 2), // Trade, PartiallyFilled

            // futures.orders 的 status
            "open" => {
                // futures 订单还在挂单中
                match finish_as_lower.as_str() {
                    "_new" => (1, 1),    // New, New
                    "_update" => (5, 2), // Trade, PartiallyFilled
                    _ => (1, 1),         // 默认 New
                }
            }

            // 共用: spot 的 "finish" 或 futures 的 "finished"
            "finish" | "finished" => {
                match finish_as_lower.as_str() {
                    // 成交完成
                    "filled" => (5, 3), // Trade, Filled

                    // 各种取消原因 -> Canceled
                    "cancelled" | "canceled" => (2, 4), // Canceled, Canceled
                    "liquidated" => (2, 4),             // 清算取消
                    "reduce_only" => (2, 4),            // 减仓设置取消
                    "position_close" => (2, 4),         // 平仓取消
                    "reduce_out" => (2, 4),             // 只减仓排除
                    "stp" => (7, 4),                    // TradePrevention, Canceled

                    // IOC/ADL 特殊完成
                    "ioc" => (5, 3),               // IOC 立即完成视为 Trade, Filled
                    "auto_deleveraging" => (5, 3), // ADL 完成视为 Trade, Filled

                    // 其他未知情况
                    _ => (6, 5), // Expired, Expired
                }
            }

            _ => (1, 1), // 默认 New
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4  // msg_type
            + 1  // venue
            + 8  // event_time
            + 4 + self.symbol_length as usize  // symbol
            + 8 * 2  // order_id, client_order_id
            + 1 * 6  // side, order_type, time_in_force, execution_type, order_status, is_maker
            + 8 * 4  // price, quantity, cumulative_filled_quantity, last_executed_price
            + 4 + self.commission_asset_length as usize; // commission_asset

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
        const MIN_FIXED_SIZE: usize = 4 + 1 + 8 + 4 + 8 * 2 + 1 * 6 + 8 * 4 + 4;
        if data.len() < MIN_FIXED_SIZE {
            anyhow::bail!("GateBasicOrderMsg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::OrderUpdate as u32 {
            anyhow::bail!("invalid GateBasicOrderMsg type: {}", msg_type);
        }

        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();

        let symbol_length = cursor.get_u32_le();
        if cursor.remaining() < symbol_length as usize {
            anyhow::bail!("GateBasicOrderMsg truncated before symbol");
        }
        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;

        if cursor.remaining() < 8 * 2 + 1 * 6 + 8 * 4 + 4 {
            anyhow::bail!("GateBasicOrderMsg truncated after symbol");
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
            anyhow::bail!("GateBasicOrderMsg truncated before commission_asset");
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

/// Basic 余额消息（仅一个时间字段）
#[derive(Debug, Clone)]
pub struct BasicBalanceMsg {
    pub msg_type: BasicAccountEventType,
    pub timestamp: i64,
    pub symbol_length: u32,
    pub symbol: String,
    pub balance: f64,
}

impl BasicBalanceMsg {
    pub fn create(timestamp: i64, symbol: String, balance: f64) -> Self {
        Self {
            msg_type: BasicAccountEventType::BalanceUpdate,
            timestamp,
            symbol_length: symbol.len() as u32,
            symbol,
            balance,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 4 + self.symbol_length as usize + 8;
        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_u32_le(self.symbol_length);
        buf.put(self.symbol.as_bytes());
        buf.put_f64_le(self.balance);

        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 4 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("okex balance msg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::BalanceUpdate as u32 {
            anyhow::bail!("invalid okex balance msg type: {}", msg_type);
        }

        let timestamp = cursor.get_i64_le();
        let symbol_length = cursor.get_u32_le();
        if cursor.remaining() < symbol_length as usize + 8 {
            anyhow::bail!("invalid okex balance msg length");
        }
        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;
        let balance = cursor.get_f64_le();

        Ok(Self {
            msg_type: BasicAccountEventType::BalanceUpdate,
            timestamp,
            symbol_length,
            symbol,
            balance,
        })
    }
}

/// Basic 持仓消息（统一结构）
#[derive(Debug, Clone)]
pub struct BasicPositionMsg {
    pub msg_type: BasicAccountEventType,
    pub timestamp: i64,
    pub inst_id_length: u32,
    pub position_side: char,
    pub padding: [u8; 3],
    pub inst_id: String,
    pub position_amount: f32,
}

/// Basic 合约未实现盈亏消息（USDT 计价）
#[derive(Debug, Clone)]
pub struct BasicUmUnrealizedMsg {
    pub msg_type: BasicAccountEventType,
    pub timestamp: i64,
    pub inst_id_length: u32,
    pub position_side: char,
    pub padding: [u8; 3],
    pub inst_id: String,
    pub unrealized_pnl: f64,
}

/// Basic 借贷利息消息（REST 拉取 OKX GET /api/v5/account/interest-accrued）
#[derive(Debug, Clone)]
pub struct BasicBorrowInterestMsg {
    pub msg_type: BasicAccountEventType,
    pub timestamp: i64,
    pub symbol_length: u32,
    pub padding: [u8; 4],
    pub symbol: String,
    pub borrowed: f64,
    pub interest: f64,
}

impl BasicPositionMsg {
    pub fn create(
        timestamp: i64,
        inst_id: String,
        position_side: char,
        position_amount: f32,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::PositionUpdate,
            timestamp,
            inst_id_length: inst_id.len() as u32,
            position_side,
            padding: [0u8; 3],
            inst_id,
            position_amount,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 4 + 1 + 3 + self.inst_id_length as usize + 4;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_u32_le(self.inst_id_length);
        buf.put_u8(self.position_side as u8);
        buf.put(&self.padding[..]);
        buf.put(self.inst_id.as_bytes());
        buf.put_f32_le(self.position_amount);
        buf.freeze()
    }

    pub fn msg_type(&self) -> BasicAccountEventType {
        BasicAccountEventType::PositionUpdate
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn inst_id(&self) -> &str {
        self.inst_id.as_str()
    }

    pub fn position_side(&self) -> char {
        self.position_side
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 4 + 1 + 3 + 4;
        if data.len() < MIN_SIZE {
            anyhow::bail!("okex position msg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::PositionUpdate as u32 {
            anyhow::bail!("invalid okex position msg type: {}", msg_type);
        }
        let timestamp = cursor.get_i64_le();
        let inst_id_length = cursor.get_u32_le();
        let position_side = cursor.get_u8() as char;
        let mut padding = [0u8; 3];
        cursor.copy_to_slice(&mut padding);
        if cursor.remaining() < inst_id_length as usize + 4 {
            anyhow::bail!("okex position msg truncated");
        }
        let inst_id = String::from_utf8(cursor.copy_to_bytes(inst_id_length as usize).to_vec())?;
        let position_amount = cursor.get_f32_le();
        Ok(Self {
            msg_type: BasicAccountEventType::PositionUpdate,
            timestamp,
            inst_id_length,
            position_side,
            padding,
            inst_id,
            position_amount,
        })
    }
}

impl BasicUmUnrealizedMsg {
    pub fn create(
        timestamp: i64,
        inst_id: String,
        position_side: char,
        unrealized_pnl: f64,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::UnrealizedPnlUpdate,
            timestamp,
            inst_id_length: inst_id.len() as u32,
            position_side,
            padding: [0u8; 3],
            inst_id,
            unrealized_pnl,
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
        buf.put_f64_le(self.unrealized_pnl);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 4 + 1 + 3 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("unrealized pnl msg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::UnrealizedPnlUpdate as u32 {
            anyhow::bail!("invalid unrealized pnl msg type: {}", msg_type);
        }
        let timestamp = cursor.get_i64_le();
        let inst_id_length = cursor.get_u32_le();
        let position_side = cursor.get_u8() as char;
        let mut padding = [0u8; 3];
        cursor.copy_to_slice(&mut padding);
        if cursor.remaining() < inst_id_length as usize + 8 {
            anyhow::bail!("unrealized pnl msg truncated");
        }
        let inst_id = String::from_utf8(cursor.copy_to_bytes(inst_id_length as usize).to_vec())?;
        let unrealized_pnl = cursor.get_f64_le();
        Ok(Self {
            msg_type: BasicAccountEventType::UnrealizedPnlUpdate,
            timestamp,
            inst_id_length,
            position_side,
            padding,
            inst_id,
            unrealized_pnl,
        })
    }
}

impl BasicBorrowInterestMsg {
    pub fn create(timestamp: i64, symbol: String, borrowed: f64, interest: f64) -> Self {
        Self {
            msg_type: BasicAccountEventType::BorrowInterest,
            timestamp,
            symbol_length: symbol.len() as u32,
            padding: [0u8; 4],
            symbol,
            borrowed,
            interest,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 4 + 4 + self.symbol_length as usize + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_u32_le(self.symbol_length);
        buf.put(&self.padding[..]);
        buf.put(self.symbol.as_bytes());
        buf.put_f64_le(self.borrowed);
        buf.put_f64_le(self.interest);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 4 + 4 + 8 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("borrow interest msg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::BorrowInterest as u32 {
            anyhow::bail!("invalid borrow interest msg type: {}", msg_type);
        }
        let timestamp = cursor.get_i64_le();
        let symbol_length = cursor.get_u32_le();
        let mut padding = [0u8; 4];
        cursor.copy_to_slice(&mut padding);
        if cursor.remaining() < symbol_length as usize + 8 + 8 {
            anyhow::bail!("borrow interest msg truncated");
        }
        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_length as usize).to_vec())?;
        let borrowed = cursor.get_f64_le();
        let interest = cursor.get_f64_le();

        Ok(Self {
            msg_type: BasicAccountEventType::BorrowInterest,
            timestamp,
            symbol_length,
            padding,
            symbol,
            borrowed,
            interest,
        })
    }
}

/// Basic 账户事件消息包装
pub struct BasicAccountEventMsg {
    pub msg_type: BasicAccountEventType,
    pub account_scope: BasicAccountScope,
    pub msg_length: u32,
    pub data: Bytes,
}

impl BasicAccountEventMsg {
    pub fn create(
        msg_type: BasicAccountEventType,
        account_scope: BasicAccountScope,
        data: Bytes,
    ) -> Self {
        Self {
            msg_type,
            account_scope,
            msg_length: data.len() as u32,
            data,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(BASIC_ACCOUNT_EVENT_HEADER_LEN + self.data.len());
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.account_scope as u32);
        buf.put_u32_le(self.msg_length);
        buf.put(self.data.clone());
        buf.freeze()
    }
}

pub const BASIC_ACCOUNT_EVENT_HEADER_LEN: usize = 12;

/// 解析 basic account event 头与 payload。
#[inline]
pub fn split_basic_account_event(
    data: &[u8],
) -> Option<(BasicAccountEventType, BasicAccountScope, &[u8])> {
    if data.len() < BASIC_ACCOUNT_EVENT_HEADER_LEN {
        return None;
    }

    let event_type = get_basic_event_type(data);
    if matches!(event_type, BasicAccountEventType::Error) {
        return None;
    }

    let account_scope = get_basic_account_scope(data);
    let payload_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let total_len = BASIC_ACCOUNT_EVENT_HEADER_LEN + payload_len;
    if data.len() < total_len {
        return None;
    }

    Some((
        event_type,
        account_scope,
        &data[BASIC_ACCOUNT_EVENT_HEADER_LEN..total_len],
    ))
}

/// 获取基础事件类型
#[inline]
pub fn get_basic_event_type(data: &[u8]) -> BasicAccountEventType {
    if data.len() < 4 {
        return BasicAccountEventType::Error;
    }
    let event_type_u32 = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    match event_type_u32 {
        4001 => BasicAccountEventType::OrderUpdate,
        4002 => BasicAccountEventType::BalanceUpdate,
        4003 => BasicAccountEventType::PositionUpdate,
        4004 => BasicAccountEventType::BorrowInterest,
        4005 => BasicAccountEventType::UnrealizedPnlUpdate,
        _ => BasicAccountEventType::Error,
    }
}

/// 获取账户来源范围
#[inline]
pub fn get_basic_account_scope(data: &[u8]) -> BasicAccountScope {
    if data.len() < 8 {
        return BasicAccountScope::Unknown;
    }
    let scope_u32 = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    BasicAccountScope::from_u32(scope_u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_account_event_prefix_fields_round_trip() {
        let payload = BasicBalanceMsg::create(123, "USDT".to_string(), 456.0).to_bytes();
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::BalanceUpdate,
            BasicAccountScope::BinanceStdSpot,
            payload.clone(),
        )
        .to_bytes();

        let (event_type, scope, body) =
            split_basic_account_event(&event).expect("should parse wrapped basic account event");
        assert_eq!(event_type, BasicAccountEventType::BalanceUpdate);
        assert_eq!(scope, BasicAccountScope::BinanceStdSpot);
        assert_eq!(body, payload.as_ref());
    }
}
