use bytes::{BufMut, Bytes, BytesMut};
use crate::common::exchange::Exchange;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TradeResponseType {
    BinanceNewUMOrder = 3001,  // 币安UM合约下单
    BinanceNewUMConditionalOrder = 3002,  // 币安UM条件单下单
    BinanceNewMarginOrder = 3003,  // 币安现货杠杆下单
    BinanceCancelUMOrder = 3004,  // 币安UM合约撤单
    ErrorResponse = 3999,
}

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TradeRequestType {
    BinanceNewUMOrder = 4001,  // 币安UM合约下单请求
    BinanceNewUMConditionalOrder = 4002,  // 币安UM条件单下单请求
    BinanceNewMarginOrder = 4003,  // 币安现货杠杆下单请求
    BinanceCancelUMOrder = 4004,  // 币安UM合约撤单请求
}

// 交易请求的公共头部
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct TradeRequestHeader {
    pub msg_type: u32,              // 请求类型
    pub params_length: u32,          // 额外参数的长度
    pub create_time: i64,           // 构造请求的时间戳
    pub client_order_id: i64,        // 客户端订单ID
}

// 币安UM合约下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,  // 额外的请求参数（JSON或其他格式）
}

impl BinanceNewUMOrderRequest {
    pub fn create(
        create_time: i64,
        client_order_id: i64,
        params: Bytes,
    ) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceNewUMOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self {
            header,
            params,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8
            + self.params.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put(self.params.clone());

        buf.freeze()
    }
}

// 交易响应的公共头部
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct TradeResponseHeader {
    pub msg_type: TradeResponseType,
    pub local_recv_time: i64,       // 本地接收时间
    pub client_order_id: i64,        // 客户端订单ID
    pub exchange: u32,               // 交易所类型
    pub body_length: u32,            // 响应体数据长度
}

// 币安UM合约下单响应体
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMOrderResponseBody {
    pub order_id: i64,
    pub update_time: i64,
    pub good_till_date: i64,
    pub symbol_length: u32,
    pub padding: [u8; 4],
    pub symbol: String,
    pub side: char,  // 'B' for BUY, 'S' for SELL
    pub position_side: char,  // 'L' for LONG, 'S' for SHORT, 'B' for BOTH
    pub reduce_only: bool,
    pub padding2: [u8; 5],
    pub cum_qty: f64,
    pub cum_quote: f64,
    pub executed_qty: f64,
    pub avg_price: f64,
    pub orig_qty: f64,
    pub price: f64,
    pub order_type_length: u32,
    pub time_in_force_length: u32,
    pub status_length: u32,
    pub self_trade_prevention_mode_length: u32,
    pub price_match_length: u32,
    pub padding3: [u8; 4],
    pub order_type: String,
    pub time_in_force: String,
    pub status: String,
    pub self_trade_prevention_mode: String,
    pub price_match: String,
}

impl BinanceNewUMOrderResponseBody {
    pub fn to_bytes(&self) -> Bytes {
        let total_size = 8 + 8 + 8 + 4 + 4
            + self.symbol_length as usize
            + 1 + 1 + 1 + 5
            + 8 * 6
            + 4 + 4 + 4 + 4 + 4 + 4
            + self.order_type_length as usize
            + self.time_in_force_length as usize
            + self.status_length as usize
            + self.self_trade_prevention_mode_length as usize
            + self.price_match_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.update_time);
        buf.put_i64_le(self.good_till_date);
        buf.put_u32_le(self.symbol_length);
        buf.put(&self.padding[..]);
        buf.put(self.symbol.as_bytes());

        buf.put_u8(self.side as u8);
        buf.put_u8(self.position_side as u8);
        buf.put_u8(if self.reduce_only { 1 } else { 0 });
        buf.put(&self.padding2[..]);

        buf.put_f64_le(self.cum_qty);
        buf.put_f64_le(self.cum_quote);
        buf.put_f64_le(self.executed_qty);
        buf.put_f64_le(self.avg_price);
        buf.put_f64_le(self.orig_qty);
        buf.put_f64_le(self.price);

        buf.put_u32_le(self.order_type_length);
        buf.put_u32_le(self.time_in_force_length);
        buf.put_u32_le(self.status_length);
        buf.put_u32_le(self.self_trade_prevention_mode_length);
        buf.put_u32_le(self.price_match_length);
        buf.put(&self.padding3[..]);

        buf.put(self.order_type.as_bytes());
        buf.put(self.time_in_force.as_bytes());
        buf.put(self.status.as_bytes());
        buf.put(self.self_trade_prevention_mode.as_bytes());
        buf.put(self.price_match.as_bytes());

        buf.freeze()
    }
}

// 币安UM合约下单响应
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMOrderResponse {
    pub header: TradeResponseHeader,
    pub body: BinanceNewUMOrderResponseBody,
}

impl BinanceNewUMOrderResponse {
    pub fn create(
        local_recv_time: i64,
        client_order_id: i64,
        symbol: String,
        order_id: i64,
        update_time: i64,
        good_till_date: i64,
        side: char,
        position_side: char,
        reduce_only: bool,
        cum_qty: f64,
        cum_quote: f64,
        executed_qty: f64,
        avg_price: f64,
        orig_qty: f64,
        price: f64,
        order_type: String,
        time_in_force: String,
        status: String,
        self_trade_prevention_mode: String,
        price_match: String,
    ) -> Self {
        let body = BinanceNewUMOrderResponseBody {
            order_id,
            update_time,
            good_till_date,
            symbol_length: symbol.len() as u32,
            padding: [0u8; 4],
            symbol,
            side,
            position_side,
            reduce_only,
            padding2: [0u8; 5],
            cum_qty,
            cum_quote,
            executed_qty,
            avg_price,
            orig_qty,
            price,
            order_type_length: order_type.len() as u32,
            time_in_force_length: time_in_force.len() as u32,
            status_length: status.len() as u32,
            self_trade_prevention_mode_length: self_trade_prevention_mode.len() as u32,
            price_match_length: price_match.len() as u32,
            padding3: [0u8; 4],
            order_type,
            time_in_force,
            status,
            self_trade_prevention_mode,
            price_match,
        };

        let body_bytes = body.to_bytes();

        let header = TradeResponseHeader {
            msg_type: TradeResponseType::BinanceNewUMOrder,
            local_recv_time,
            client_order_id,
            exchange: Exchange::BinanceFutures as u32,
            body_length: body_bytes.len() as u32,
        };

        Self {
            header,
            body,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let body_bytes = self.body.to_bytes();
        let total_size = 4 + 8 + 8 + 4 + 4
            + body_bytes.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type as u32);
        buf.put_i64_le(self.header.local_recv_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put_u32_le(self.header.exchange);
        buf.put_u32_le(self.header.body_length);
        buf.put(body_bytes);

        buf.freeze()
    }
}

// 币安UM条件单下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMConditionalOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,  // 额外的请求参数（JSON或其他格式）
}

impl BinanceNewUMConditionalOrderRequest {
    pub fn create(
        create_time: i64,
        client_order_id: i64,
        params: Bytes,
    ) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceNewUMConditionalOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self {
            header,
            params,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8
            + self.params.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put(self.params.clone());

        buf.freeze()
    }
}

// 币安UM条件单下单响应体
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMConditionalOrderResponseBody {
    pub strategy_id: i64,
    pub update_time: i64,
    pub strategy_type_length: u32,
    pub strategy_status_length: u32,
    pub symbol_length: u32,
    pub side_length: u32,
    pub position_side_length: u32,
    pub padding: [u8; 4],
    pub strategy_type: String,
    pub strategy_status: String,
    pub symbol: String,
    pub side: String,
    pub position_side: String,
    pub stop_price: f64,
    pub price_protect: bool,
    pub padding2: [u8; 7],
}

impl BinanceNewUMConditionalOrderResponseBody {
    pub fn to_bytes(&self) -> Bytes {
        let total_size = 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4
            + self.strategy_type_length as usize
            + self.strategy_status_length as usize
            + self.symbol_length as usize
            + self.side_length as usize
            + self.position_side_length as usize
            + 8 + 1 + 7;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_i64_le(self.strategy_id);
        buf.put_i64_le(self.update_time);
        buf.put_u32_le(self.strategy_type_length);
        buf.put_u32_le(self.strategy_status_length);
        buf.put_u32_le(self.symbol_length);
        buf.put_u32_le(self.side_length);
        buf.put_u32_le(self.position_side_length);
        buf.put(&self.padding[..]);

        buf.put(self.strategy_type.as_bytes());
        buf.put(self.strategy_status.as_bytes());
        buf.put(self.symbol.as_bytes());
        buf.put(self.side.as_bytes());
        buf.put(self.position_side.as_bytes());

        buf.put_f64_le(self.stop_price);
        buf.put_u8(if self.price_protect { 1 } else { 0 });
        buf.put(&self.padding2[..]);

        buf.freeze()
    }
}

// 币安UM条件单下单响应
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewUMConditionalOrderResponse {
    pub header: TradeResponseHeader,
    pub body: BinanceNewUMConditionalOrderResponseBody,
}

impl BinanceNewUMConditionalOrderResponse {
    pub fn create(
        local_recv_time: i64,
        client_order_id: i64,
        strategy_id: i64,
        update_time: i64,
        strategy_type: String,
        strategy_status: String,
        symbol: String,
        side: String,
        position_side: String,
        stop_price: f64,
        price_protect: bool,
    ) -> Self {
        let body = BinanceNewUMConditionalOrderResponseBody {
            strategy_id,
            update_time,
            strategy_type_length: strategy_type.len() as u32,
            strategy_status_length: strategy_status.len() as u32,
            symbol_length: symbol.len() as u32,
            side_length: side.len() as u32,
            position_side_length: position_side.len() as u32,
            padding: [0u8; 4],
            strategy_type,
            strategy_status,
            symbol,
            side,
            position_side,
            stop_price,
            price_protect,
            padding2: [0u8; 7],
        };

        let body_bytes = body.to_bytes();

        let header = TradeResponseHeader {
            msg_type: TradeResponseType::BinanceNewUMConditionalOrder,
            local_recv_time,
            client_order_id,
            exchange: Exchange::BinanceFutures as u32,
            body_length: body_bytes.len() as u32,
        };

        Self {
            header,
            body,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let body_bytes = self.body.to_bytes();
        let total_size = 4 + 8 + 8 + 4 + 4
            + body_bytes.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type as u32);
        buf.put_i64_le(self.header.local_recv_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put_u32_le(self.header.exchange);
        buf.put_u32_le(self.header.body_length);
        buf.put(body_bytes);

        buf.freeze()
    }
}

// 币安现货杠杆下单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewMarginOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,  // 额外的请求参数（JSON或其他格式）
}

impl BinanceNewMarginOrderRequest {
    pub fn create(
        create_time: i64,
        client_order_id: i64,
        params: Bytes,
    ) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceNewMarginOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self {
            header,
            params,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8
            + self.params.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put(self.params.clone());

        buf.freeze()
    }
}

// 币安现货杠杆下单响应体
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewMarginOrderResponseBody {
    pub order_id: i64,
    pub transact_time: i64,
    pub symbol_length: u32,
    pub status_length: u32,
    pub order_type_length: u32,
    pub time_in_force_length: u32,
    pub side_length: u32,
    pub margin_buy_borrow_asset_length: u32,
    pub symbol: String,
    pub status: String,
    pub order_type: String,
    pub time_in_force: String,
    pub side: String,
    pub price: f64,
    pub orig_qty: f64,
    pub executed_qty: f64,
    pub cummulative_quote_qty: f64,
    pub margin_buy_borrow_amount: f64,
    pub margin_buy_borrow_asset: String,
}

impl BinanceNewMarginOrderResponseBody {
    pub fn to_bytes(&self) -> Bytes {
        let total_size = 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4
            + self.symbol_length as usize
            + self.status_length as usize
            + self.order_type_length as usize
            + self.time_in_force_length as usize
            + self.side_length as usize
            + 8 * 5
            + self.margin_buy_borrow_asset_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.transact_time);
        buf.put_u32_le(self.symbol_length);
        buf.put_u32_le(self.status_length);
        buf.put_u32_le(self.order_type_length);
        buf.put_u32_le(self.time_in_force_length);
        buf.put_u32_le(self.side_length);
        buf.put_u32_le(self.margin_buy_borrow_asset_length);

        buf.put(self.symbol.as_bytes());
        buf.put(self.status.as_bytes());
        buf.put(self.order_type.as_bytes());
        buf.put(self.time_in_force.as_bytes());
        buf.put(self.side.as_bytes());

        buf.put_f64_le(self.price);
        buf.put_f64_le(self.orig_qty);
        buf.put_f64_le(self.executed_qty);
        buf.put_f64_le(self.cummulative_quote_qty);
        buf.put_f64_le(self.margin_buy_borrow_amount);
        buf.put(self.margin_buy_borrow_asset.as_bytes());

        buf.freeze()
    }
}

// 币安现货杠杆下单响应
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceNewMarginOrderResponse {
    pub header: TradeResponseHeader,
    pub body: BinanceNewMarginOrderResponseBody,
}

impl BinanceNewMarginOrderResponse {
    pub fn create(
        local_recv_time: i64,
        client_order_id: i64,
        symbol: String,
        order_id: i64,
        transact_time: i64,
        status: String,
        order_type: String,
        time_in_force: String,
        side: String,
        price: f64,
        orig_qty: f64,
        executed_qty: f64,
        cummulative_quote_qty: f64,
        margin_buy_borrow_amount: f64,
        margin_buy_borrow_asset: String,
    ) -> Self {
        let body = BinanceNewMarginOrderResponseBody {
            order_id,
            transact_time,
            symbol_length: symbol.len() as u32,
            status_length: status.len() as u32,
            order_type_length: order_type.len() as u32,
            time_in_force_length: time_in_force.len() as u32,
            side_length: side.len() as u32,
            margin_buy_borrow_asset_length: margin_buy_borrow_asset.len() as u32,
            symbol,
            status,
            order_type,
            time_in_force,
            side,
            price,
            orig_qty,
            executed_qty,
            cummulative_quote_qty,
            margin_buy_borrow_amount,
            margin_buy_borrow_asset,
        };

        let body_bytes = body.to_bytes();

        let header = TradeResponseHeader {
            msg_type: TradeResponseType::BinanceNewMarginOrder,
            local_recv_time,
            client_order_id,
            exchange: Exchange::Binance as u32,
            body_length: body_bytes.len() as u32,
        };

        Self {
            header,
            body,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let body_bytes = self.body.to_bytes();
        let total_size = 4 + 8 + 8 + 4 + 4
            + body_bytes.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type as u32);
        buf.put_i64_le(self.header.local_recv_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put_u32_le(self.header.exchange);
        buf.put_u32_le(self.header.body_length);
        buf.put(body_bytes);

        buf.freeze()
    }
}

// 币安UM合约撤单请求
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelUMOrderRequest {
    pub header: TradeRequestHeader,
    pub params: Bytes,  // 额外的请求参数（JSON或其他格式）
}

impl BinanceCancelUMOrderRequest {
    pub fn create(
        create_time: i64,
        client_order_id: i64,
        params: Bytes,
    ) -> Self {
        let header = TradeRequestHeader {
            msg_type: TradeRequestType::BinanceCancelUMOrder as u32,
            params_length: params.len() as u32,
            create_time,
            client_order_id,
        };

        Self {
            header,
            params,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8
            + self.params.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put(self.params.clone());

        buf.freeze()
    }
}

// 币安UM合约撤单响应体
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelUMOrderResponseBody {
    pub order_id: i64,
    pub update_time: i64,
    pub good_till_date: i64,
    pub symbol_length: u32,
    pub padding: [u8; 4],
    pub symbol: String,
    pub side: char,  // 'B' for BUY, 'S' for SELL
    pub position_side: char,  // 'L' for LONG, 'S' for SHORT, 'B' for BOTH
    pub reduce_only: bool,
    pub padding2: [u8; 5],
    pub cum_qty: f64,
    pub cum_quote: f64,
    pub executed_qty: f64,
    pub avg_price: f64,
    pub orig_qty: f64,
    pub price: f64,
    pub order_type_length: u32,
    pub time_in_force_length: u32,
    pub status_length: u32,
    pub self_trade_prevention_mode_length: u32,
    pub price_match_length: u32,
    pub padding3: [u8; 4],
    pub order_type: String,
    pub time_in_force: String,
    pub status: String,
    pub self_trade_prevention_mode: String,
    pub price_match: String,
}

impl BinanceCancelUMOrderResponseBody {
    pub fn to_bytes(&self) -> Bytes {
        let total_size = 8 + 8 + 8 + 4 + 4
            + self.symbol_length as usize
            + 1 + 1 + 1 + 5
            + 8 * 6
            + 4 + 4 + 4 + 4 + 4 + 4
            + self.order_type_length as usize
            + self.time_in_force_length as usize
            + self.status_length as usize
            + self.self_trade_prevention_mode_length as usize
            + self.price_match_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.update_time);
        buf.put_i64_le(self.good_till_date);
        buf.put_u32_le(self.symbol_length);
        buf.put(&self.padding[..]);
        buf.put(self.symbol.as_bytes());

        buf.put_u8(self.side as u8);
        buf.put_u8(self.position_side as u8);
        buf.put_u8(if self.reduce_only { 1 } else { 0 });
        buf.put(&self.padding2[..]);

        buf.put_f64_le(self.cum_qty);
        buf.put_f64_le(self.cum_quote);
        buf.put_f64_le(self.executed_qty);
        buf.put_f64_le(self.avg_price);
        buf.put_f64_le(self.orig_qty);
        buf.put_f64_le(self.price);

        buf.put_u32_le(self.order_type_length);
        buf.put_u32_le(self.time_in_force_length);
        buf.put_u32_le(self.status_length);
        buf.put_u32_le(self.self_trade_prevention_mode_length);
        buf.put_u32_le(self.price_match_length);
        buf.put(&self.padding3[..]);

        buf.put(self.order_type.as_bytes());
        buf.put(self.time_in_force.as_bytes());
        buf.put(self.status.as_bytes());
        buf.put(self.self_trade_prevention_mode.as_bytes());
        buf.put(self.price_match.as_bytes());

        buf.freeze()
    }
}

// 币安UM合约撤单响应
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct BinanceCancelUMOrderResponse {
    pub header: TradeResponseHeader,
    pub body: BinanceCancelUMOrderResponseBody,
}

impl BinanceCancelUMOrderResponse {
    pub fn create(
        local_recv_time: i64,
        client_order_id: i64,
        symbol: String,
        order_id: i64,
        update_time: i64,
        good_till_date: i64,
        side: char,
        position_side: char,
        reduce_only: bool,
        cum_qty: f64,
        cum_quote: f64,
        executed_qty: f64,
        avg_price: f64,
        orig_qty: f64,
        price: f64,
        order_type: String,
        time_in_force: String,
        status: String,
        self_trade_prevention_mode: String,
        price_match: String,
    ) -> Self {
        let body = BinanceCancelUMOrderResponseBody {
            order_id,
            update_time,
            good_till_date,
            symbol_length: symbol.len() as u32,
            padding: [0u8; 4],
            symbol,
            side,
            position_side,
            reduce_only,
            padding2: [0u8; 5],
            cum_qty,
            cum_quote,
            executed_qty,
            avg_price,
            orig_qty,
            price,
            order_type_length: order_type.len() as u32,
            time_in_force_length: time_in_force.len() as u32,
            status_length: status.len() as u32,
            self_trade_prevention_mode_length: self_trade_prevention_mode.len() as u32,
            price_match_length: price_match.len() as u32,
            padding3: [0u8; 4],
            order_type,
            time_in_force,
            status,
            self_trade_prevention_mode,
            price_match,
        };

        let body_bytes = body.to_bytes();

        let header = TradeResponseHeader {
            msg_type: TradeResponseType::BinanceCancelUMOrder,
            local_recv_time,
            client_order_id,
            exchange: Exchange::BinanceFutures as u32,
            body_length: body_bytes.len() as u32,
        };

        Self {
            header,
            body,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let body_bytes = self.body.to_bytes();
        let total_size = 4 + 8 + 8 + 4 + 4
            + body_bytes.len();

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.header.msg_type as u32);
        buf.put_i64_le(self.header.local_recv_time);
        buf.put_i64_le(self.header.client_order_id);
        buf.put_u32_le(self.header.exchange);
        buf.put_u32_le(self.header.body_length);
        buf.put(body_bytes);

        buf.freeze()
    }
}

// 错误响应
#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct ErrorResponseMsg {
    pub msg_type: TradeResponseType,
    pub local_recv_time: i64,
    pub client_order_id: i64,
    pub exchange: u32,
    pub error_code: i32,
    pub error_msg_length: u32,
    pub padding: [u8; 4],
    pub error_msg: String,
}

impl ErrorResponseMsg {
    pub fn create(
        local_recv_time: i64,
        client_order_id: i64,
        exchange: Exchange,
        error_code: i32,
        error_msg: String,
    ) -> Self {
        Self {
            msg_type: TradeResponseType::ErrorResponse,
            local_recv_time,
            client_order_id,
            exchange: exchange as u32,
            error_code,
            error_msg_length: error_msg.len() as u32,
            padding: [0u8; 4],
            error_msg,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 8 + 8 + 4 + 4 + 4 + 4
            + self.error_msg_length as usize;

        let mut buf = BytesMut::with_capacity(total_size);

        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.local_recv_time);
        buf.put_i64_le(self.client_order_id);
        buf.put_u32_le(self.exchange);
        buf.put_i32_le(self.error_code);
        buf.put_u32_le(self.error_msg_length);
        buf.put(&self.padding[..]);
        buf.put(self.error_msg.as_bytes());

        buf.freeze()
    }
}