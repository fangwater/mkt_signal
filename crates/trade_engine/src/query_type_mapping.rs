use super::config::RestConstants;
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
                | QueryRequestType::BinancePmAccountSnapshot
        )
    }

    pub fn is_okex_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::OkexMarginQuery
                | QueryRequestType::OkexUMQuery
                | QueryRequestType::OkexAccountBalanceSnapshot
                | QueryRequestType::OkexPositionsSnapshot
                | QueryRequestType::OkexUsdtAvailableSnapshot
                | QueryRequestType::OkexUsdtMaxLoan
        )
    }

    pub fn is_gate_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::GateUnifiedBalanceSnapshot
                | QueryRequestType::GateUnifiedPositionsSnapshot
                | QueryRequestType::GateUnifiedUsdtAvailableSnapshot
                | QueryRequestType::GateUnifiedUsdtMaxBorrowable
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
                | QueryRequestType::BitgetUsdtAvailableSnapshot
                | QueryRequestType::BitgetUsdtMaxTransferable
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
            QueryRequestType::BinancePmAccountSnapshot => "/papi/v1/account",
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => {
                "/api/v5/trade/order"
            }
            QueryRequestType::OkexAccountBalanceSnapshot => "/api/v5/account/balance",
            QueryRequestType::OkexPositionsSnapshot => "/api/v5/account/positions",
            QueryRequestType::OkexUsdtAvailableSnapshot => "/api/v5/account/balance",
            QueryRequestType::OkexUsdtMaxLoan => "/api/v5/account/max-loan",
            QueryRequestType::GateUnifiedBalanceSnapshot => "/api/v4/unified/accounts",
            QueryRequestType::GateUnifiedPositionsSnapshot => "/api/v4/futures/usdt/positions",
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot => "/api/v4/unified/accounts",
            QueryRequestType::GateUnifiedUsdtMaxBorrowable => "/api/v4/unified/borrowable",
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
            QueryRequestType::BitgetUsdtAvailableSnapshot => "/api/v3/account/assets",
            QueryRequestType::BitgetUsdtMaxTransferable => "/api/v3/account/max-transferable",
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
            | QueryRequestType::BinancePmAccountSnapshot
            | QueryRequestType::OkexMarginQuery
            | QueryRequestType::OkexUMQuery
            | QueryRequestType::OkexAccountBalanceSnapshot
            | QueryRequestType::OkexPositionsSnapshot
            | QueryRequestType::OkexUsdtAvailableSnapshot
            | QueryRequestType::OkexUsdtMaxLoan
            | QueryRequestType::GateUnifiedBalanceSnapshot
            | QueryRequestType::GateUnifiedPositionsSnapshot
            | QueryRequestType::GateUnifiedUsdtAvailableSnapshot
            | QueryRequestType::GateUnifiedUsdtMaxBorrowable
            | QueryRequestType::BybitMarginQuery
            | QueryRequestType::BybitUMQuery
            | QueryRequestType::BybitAccountBalanceSnapshot
            | QueryRequestType::BybitPositionsSnapshot
            | QueryRequestType::BitgetMarginQuery
            | QueryRequestType::BitgetUMQuery
            | QueryRequestType::BitgetAccountBalanceSnapshot
            | QueryRequestType::BitgetPositionsSnapshot
            | QueryRequestType::BitgetUsdtAvailableSnapshot
            | QueryRequestType::BitgetUsdtMaxTransferable => "GET",
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
            QueryRequestType::BinancePmAccountSnapshot => 20,
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => 1,
            QueryRequestType::OkexAccountBalanceSnapshot
            | QueryRequestType::OkexPositionsSnapshot
            | QueryRequestType::OkexUsdtAvailableSnapshot
            | QueryRequestType::OkexUsdtMaxLoan => 1,
            QueryRequestType::GateUnifiedBalanceSnapshot => 1,
            QueryRequestType::GateUnifiedPositionsSnapshot => 1,
            QueryRequestType::GateUnifiedUsdtAvailableSnapshot => 1,
            QueryRequestType::GateUnifiedUsdtMaxBorrowable => 1,
            QueryRequestType::BybitMarginQuery => 1,
            QueryRequestType::BybitUMQuery => 1,
            QueryRequestType::BybitAccountBalanceSnapshot => 1,
            QueryRequestType::BybitPositionsSnapshot => 1,
            QueryRequestType::BitgetMarginQuery
            | QueryRequestType::BitgetUMQuery
            | QueryRequestType::BitgetAccountBalanceSnapshot
            | QueryRequestType::BitgetPositionsSnapshot
            | QueryRequestType::BitgetUsdtAvailableSnapshot
            | QueryRequestType::BitgetUsdtMaxTransferable => 1,
            QueryRequestType::GateUnifiedOrderQuery | QueryRequestType::GateFuturesOrderQuery => {
                unreachable!("Gate order queries run via websocket; REST mapping not used")
            }
        }
    }

    pub fn is_binance_snapshot(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::BinancePmBalanceSnapshot
                | QueryRequestType::BinanceUmAccountSnapshot
                | QueryRequestType::BinanceUmBalanceSnapshotStd
                | QueryRequestType::BinanceUmAccountSnapshotStd
                | QueryRequestType::BinanceSpotAccountSnapshotStd
                | QueryRequestType::BinancePmAccountSnapshot
        )
    }

    pub fn recv_window_ms(request_type: QueryRequestType) -> Option<u64> {
        Self::is_binance_snapshot(request_type).then_some(RestConstants::SNAPSHOT_RECV_WINDOW_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::QueryTypeMapping;
    use crate::config::RestConstants;
    use crate::query_request::QueryRequestType;

    #[test]
    fn binance_snapshots_use_specialized_recv_window() {
        for req_type in [
            QueryRequestType::BinancePmBalanceSnapshot,
            QueryRequestType::BinancePmAccountSnapshot,
            QueryRequestType::BinanceUmAccountSnapshot,
            QueryRequestType::BinanceUmBalanceSnapshotStd,
            QueryRequestType::BinanceUmAccountSnapshotStd,
            QueryRequestType::BinanceSpotAccountSnapshotStd,
        ] {
            assert!(QueryTypeMapping::is_binance_snapshot(req_type));
            assert_eq!(
                QueryTypeMapping::recv_window_ms(req_type),
                Some(RestConstants::SNAPSHOT_RECV_WINDOW_MS)
            );
        }
    }

    #[test]
    fn binance_order_queries_keep_default_recv_window() {
        for req_type in [
            QueryRequestType::BinanceMarginQuery,
            QueryRequestType::BinanceUMQuery,
        ] {
            assert!(!QueryTypeMapping::is_binance_snapshot(req_type));
            assert_eq!(QueryTypeMapping::recv_window_ms(req_type), None);
        }
    }
}
