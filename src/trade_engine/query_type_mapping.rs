use super::query_request::QueryRequestType;

pub struct QueryTypeMapping;

impl QueryTypeMapping {
    pub fn is_binance_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::BinanceMarginQuery
                | QueryRequestType::BinanceUMQuery
                | QueryRequestType::BinancePmBalanceSnapshot
                | QueryRequestType::BinanceUmAccountSnapshot
        )
    }

    pub fn is_okex_rest(request_type: QueryRequestType) -> bool {
        matches!(
            request_type,
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery
        )
    }

    pub fn get_endpoint(request_type: QueryRequestType) -> &'static str {
        match request_type {
            QueryRequestType::BinanceMarginQuery => "/papi/v1/margin/order",
            QueryRequestType::BinanceUMQuery => "/papi/v1/um/order",
            QueryRequestType::BinancePmBalanceSnapshot => "/papi/v1/balance",
            QueryRequestType::BinanceUmAccountSnapshot => "/papi/v1/um/account",
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => "/api/v5/trade/order",
        }
    }

    pub fn get_method(request_type: QueryRequestType) -> &'static str {
        match request_type {
            QueryRequestType::BinanceMarginQuery
            | QueryRequestType::BinanceUMQuery
            | QueryRequestType::BinancePmBalanceSnapshot
            | QueryRequestType::BinanceUmAccountSnapshot
            | QueryRequestType::OkexMarginQuery
            | QueryRequestType::OkexUMQuery => "GET",
        }
    }

    pub fn get_weight(request_type: QueryRequestType) -> u32 {
        match request_type {
            QueryRequestType::BinanceMarginQuery => 1,
            QueryRequestType::BinanceUMQuery => 1,
            QueryRequestType::BinancePmBalanceSnapshot => 1,
            QueryRequestType::BinanceUmAccountSnapshot => 1,
            QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => 1,
        }
    }
}
