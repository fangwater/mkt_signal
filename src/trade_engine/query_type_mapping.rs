use super::query_request::QueryRequestType;

pub struct QueryTypeMapping;

impl QueryTypeMapping {
    pub fn is_binance_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::BinanceMarginQuery
                | QueryRequestType::BinanceUMQuery
                | QueryRequestType::BinanceWsMarginQuery
                | QueryRequestType::BinancePmBalanceSnapshot
                | QueryRequestType::BinanceUmAccountSnapshot
                | QueryRequestType::BinanceUmBalanceSnapshotStd
                | QueryRequestType::BinanceUmAccountSnapshotStd
                | QueryRequestType::BinanceSpotAccountSnapshotStd
        )
    }

    pub fn is_okex_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::OkexMarginQuery
                | QueryRequestType::OkexUMQuery
                | QueryRequestType::OkexAccountBalanceSnapshot
                | QueryRequestType::OkexPositionsSnapshot
        )
    }

    pub fn is_gate_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::GateUnifiedBalanceSnapshot
                | QueryRequestType::GateUnifiedPositionsSnapshot
        )
    }

    pub fn is_bybit_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::BybitMarginQuery
                | QueryRequestType::BybitUMQuery
                | QueryRequestType::BybitAccountBalanceSnapshot
                | QueryRequestType::BybitPositionsSnapshot
        )
    }

    pub fn is_bitget_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::BitgetMarginQuery
                | QueryRequestType::BitgetUMQuery
                | QueryRequestType::BitgetAccountBalanceSnapshot
                | QueryRequestType::BitgetPositionsSnapshot
        )
    }

    pub fn get_endpoint(request_type: QueryRequestType) -> &'static str {
        match request_type {
            QueryRequestType::BinanceMarginQuery => "/papi/v1/margin/order",
            QueryRequestType::BinanceUMQuery => "/papi/v1/um/order",
            QueryRequestType::BinanceWsUMQuery | QueryRequestType::BinanceWsMarginQuery => {
                unreachable!("Binance ws queries run via websocket; REST mapping not used")
            }
            QueryRequestType::BinancePmBalanceSnapshot => "/papi/v1/balance",
            QueryRequestType::BinanceUmAccountSnapshot => "/papi/v1/um/account",
            QueryRequestType::BinanceUmBalanceSnapshotStd => "/fapi/v2/balance",
            QueryRequestType::BinanceUmAccountSnapshotStd => "/fapi/v2/account",
            QueryRequestType::BinanceSpotAccountSnapshotStd => "/api/v3/account",
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => {
                "/api/v5/trade/order"
            }
            QueryRequestType::OkexAccountBalanceSnapshot => "/api/v5/account/balance",
            QueryRequestType::OkexPositionsSnapshot => "/api/v5/account/positions",
            QueryRequestType::GateUnifiedBalanceSnapshot => "/api/v4/unified/accounts",
            QueryRequestType::GateUnifiedPositionsSnapshot => "/api/v4/futures/usdt/positions",
            QueryRequestType::GateUnifiedOrderQuery | QueryRequestType::GateFuturesOrderQuery => {
                unreachable!("Gate order queries run via websocket; REST mapping not used")
            }
            QueryRequestType::BybitMarginQuery | QueryRequestType::BybitUMQuery => {
                "/v5/order/realtime"
            }
            QueryRequestType::BybitAccountBalanceSnapshot => "/v5/account/wallet-balance",
            QueryRequestType::BybitPositionsSnapshot => "/v5/position/list",
            QueryRequestType::BitgetMarginQuery | QueryRequestType::BitgetUMQuery => {
                "/api/v3/trade/order-info"
            }
            QueryRequestType::BitgetAccountBalanceSnapshot => "/api/v3/account/assets",
            QueryRequestType::BitgetPositionsSnapshot => "/api/v3/position/current-position",
        }
    }

    pub fn get_method(request_type: QueryRequestType) -> &'static str {
        match request_type {
            QueryRequestType::BinanceMarginQuery
            | QueryRequestType::BinanceUMQuery
            | QueryRequestType::BinancePmBalanceSnapshot
            | QueryRequestType::BinanceUmAccountSnapshot
            | QueryRequestType::BinanceUmBalanceSnapshotStd
            | QueryRequestType::BinanceUmAccountSnapshotStd
            | QueryRequestType::BinanceSpotAccountSnapshotStd
            | QueryRequestType::OkexMarginQuery
            | QueryRequestType::OkexUMQuery
            | QueryRequestType::OkexAccountBalanceSnapshot
            | QueryRequestType::OkexPositionsSnapshot
            | QueryRequestType::GateUnifiedBalanceSnapshot
            | QueryRequestType::GateUnifiedPositionsSnapshot
            | QueryRequestType::BybitMarginQuery
            | QueryRequestType::BybitUMQuery
            | QueryRequestType::BybitAccountBalanceSnapshot
            | QueryRequestType::BybitPositionsSnapshot
            | QueryRequestType::BitgetMarginQuery
            | QueryRequestType::BitgetUMQuery
            | QueryRequestType::BitgetAccountBalanceSnapshot
            | QueryRequestType::BitgetPositionsSnapshot => "GET",
            QueryRequestType::BinanceWsUMQuery | QueryRequestType::BinanceWsMarginQuery => {
                unreachable!("Binance ws queries run via websocket; REST mapping not used")
            }
            QueryRequestType::GateUnifiedOrderQuery | QueryRequestType::GateFuturesOrderQuery => {
                unreachable!("Gate order queries run via websocket; REST mapping not used")
            }
        }
    }

    pub fn get_weight(request_type: QueryRequestType) -> u32 {
        match request_type {
            QueryRequestType::BinanceMarginQuery => 1,
            QueryRequestType::BinanceUMQuery => 1,
            QueryRequestType::BinanceWsUMQuery | QueryRequestType::BinanceWsMarginQuery => {
                unreachable!("Binance ws queries run via websocket; REST mapping not used")
            }
            QueryRequestType::BinancePmBalanceSnapshot => 1,
            QueryRequestType::BinanceUmAccountSnapshot => 1,
            QueryRequestType::BinanceUmBalanceSnapshotStd => 5,
            QueryRequestType::BinanceUmAccountSnapshotStd => 5,
            QueryRequestType::BinanceSpotAccountSnapshotStd => 20,
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => 1,
            QueryRequestType::OkexAccountBalanceSnapshot => 1,
            QueryRequestType::OkexPositionsSnapshot => 1,
            QueryRequestType::GateUnifiedBalanceSnapshot => 1,
            QueryRequestType::GateUnifiedPositionsSnapshot => 1,
            QueryRequestType::BybitMarginQuery => 1,
            QueryRequestType::BybitUMQuery => 1,
            QueryRequestType::BybitAccountBalanceSnapshot => 1,
            QueryRequestType::BybitPositionsSnapshot => 1,
            QueryRequestType::BitgetMarginQuery
            | QueryRequestType::BitgetUMQuery
            | QueryRequestType::BitgetAccountBalanceSnapshot
            | QueryRequestType::BitgetPositionsSnapshot => 1,
            QueryRequestType::GateUnifiedOrderQuery | QueryRequestType::GateFuturesOrderQuery => {
                unreachable!("Gate order queries run via websocket; REST mapping not used")
            }
        }
    }
}
