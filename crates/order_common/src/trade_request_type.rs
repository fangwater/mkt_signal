use std::convert::TryFrom;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TradeRequestType {
    BinanceNewUMOrder = 4001,                   // 币安UM合约下单请求
    BinanceNewUMConditionalOrder = 4002,        // 币安UM条件单下单请求
    BinanceNewMarginOrder = 4003,               // 币安现货杠杆下单请求
    BinanceCancelUMOrder = 4004,                // 币安UM合约撤单请求
    BinanceCancelAllUMOrders = 4005,            // 币安UM合约撤销全部订单请求
    BinanceCancelUMConditionalOrder = 4006,     // 币安UM条件单撤单请求
    BinanceCancelAllUMConditionalOrders = 4007, // 币安UM条件单撤销全部订单请求
    BinanceCancelMarginOrder = 4008,            // 币安杠杆账户撤单请求
    BinanceModifyUMOrder = 4009,                // 币安UM合约修改订单请求
    BinanceUMSetLeverage = 4012,                // 币安UM设置杠杆
    BinanceWsNewUMOrder = 4013,                 // 币安UM WebSocket 下单请求
    BinanceWsCancelUMOrder = 4014,              // 币安UM WebSocket 撤单请求
    BinanceWsNewMarginOrder = 4015,             // 币安现货(标准账户) WebSocket 下单请求
    BinanceWsCancelMarginOrder = 4016,          // 币安现货(标准账户) WebSocket 撤单请求
    BinanceUniversalTransfer = 4017,            // 币安万向划转请求
    OkexNewMarginOrder = 5001,                  // Okex 下单（现货/杠杆）
    OkexNewUMOrder = 5002,                      // Okex 下单（合约/UM风格）
    OkexCancelMarginOrder = 5003,               // Okex 撤单（现货/杠杆）
    OkexCancelUMOrder = 5004,                   // Okex 撤单（合约/UM风格）
    GateUnifiedNewOrder = 5201,                 // Gate 统一账户下单请求
    GateUnifiedCancelOrder = 5202,              // Gate 统一账户撤单请求
    GateFuturesNewOrder = 5203,                 // Gate U 本位合约下单请求
    GateFuturesCancelOrder = 5204,              // Gate U 本位合约撤单请求
    BybitNewMarginOrder = 5301,                 // Bybit 统一账户现货杠杆下单请求
    BybitNewUMOrder = 5302,                     // Bybit 统一账户 U 本位下单请求
    BybitCancelMarginOrder = 5303,              // Bybit 统一账户现货杠杆撤单请求
    BybitCancelUMOrder = 5304,                  // Bybit 统一账户 U 本位撤单请求
    BitgetNewMarginOrder = 5401,                // Bitget 统一账户现货下单请求
    BitgetNewUMOrder = 5402,                    // Bitget 统一账户 U 本位下单请求
    BitgetCancelMarginOrder = 5403,             // Bitget 统一账户现货撤单请求
    BitgetCancelUMOrder = 5404,                 // Bitget 统一账户 U 本位撤单请求
}

impl TryFrom<u32> for TradeRequestType {
    type Error = ();
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            4001 => Ok(TradeRequestType::BinanceNewUMOrder),
            4002 => Ok(TradeRequestType::BinanceNewUMConditionalOrder),
            4003 => Ok(TradeRequestType::BinanceNewMarginOrder),
            4004 => Ok(TradeRequestType::BinanceCancelUMOrder),
            4005 => Ok(TradeRequestType::BinanceCancelAllUMOrders),
            4006 => Ok(TradeRequestType::BinanceCancelUMConditionalOrder),
            4007 => Ok(TradeRequestType::BinanceCancelAllUMConditionalOrders),
            4008 => Ok(TradeRequestType::BinanceCancelMarginOrder),
            4009 => Ok(TradeRequestType::BinanceModifyUMOrder),
            4012 => Ok(TradeRequestType::BinanceUMSetLeverage),
            4013 => Ok(TradeRequestType::BinanceWsNewUMOrder),
            4014 => Ok(TradeRequestType::BinanceWsCancelUMOrder),
            4015 => Ok(TradeRequestType::BinanceWsNewMarginOrder),
            4016 => Ok(TradeRequestType::BinanceWsCancelMarginOrder),
            4017 => Ok(TradeRequestType::BinanceUniversalTransfer),
            5001 => Ok(TradeRequestType::OkexNewMarginOrder),
            5002 => Ok(TradeRequestType::OkexNewUMOrder),
            5003 => Ok(TradeRequestType::OkexCancelMarginOrder),
            5004 => Ok(TradeRequestType::OkexCancelUMOrder),
            5201 => Ok(TradeRequestType::GateUnifiedNewOrder),
            5202 => Ok(TradeRequestType::GateUnifiedCancelOrder),
            5203 => Ok(TradeRequestType::GateFuturesNewOrder),
            5204 => Ok(TradeRequestType::GateFuturesCancelOrder),
            5301 => Ok(TradeRequestType::BybitNewMarginOrder),
            5302 => Ok(TradeRequestType::BybitNewUMOrder),
            5303 => Ok(TradeRequestType::BybitCancelMarginOrder),
            5304 => Ok(TradeRequestType::BybitCancelUMOrder),
            5401 => Ok(TradeRequestType::BitgetNewMarginOrder),
            5402 => Ok(TradeRequestType::BitgetNewUMOrder),
            5403 => Ok(TradeRequestType::BitgetCancelMarginOrder),
            5404 => Ok(TradeRequestType::BitgetCancelUMOrder),
            _ => Err(()),
        }
    }
}

impl TradeRequestType {
    pub fn is_new_order(self) -> bool {
        matches!(
            self,
            TradeRequestType::BinanceNewUMOrder
                | TradeRequestType::BinanceNewUMConditionalOrder
                | TradeRequestType::BinanceNewMarginOrder
                | TradeRequestType::BinanceWsNewUMOrder
                | TradeRequestType::BinanceWsNewMarginOrder
                | TradeRequestType::OkexNewMarginOrder
                | TradeRequestType::OkexNewUMOrder
                | TradeRequestType::GateUnifiedNewOrder
                | TradeRequestType::GateFuturesNewOrder
                | TradeRequestType::BybitNewMarginOrder
                | TradeRequestType::BybitNewUMOrder
                | TradeRequestType::BitgetNewMarginOrder
                | TradeRequestType::BitgetNewUMOrder
        )
    }
}
