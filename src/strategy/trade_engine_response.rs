/// TradeEngineResponse trait 提供 trade engine 返回结果的通用访问接口
pub trait TradeEngineResponse {
    fn status(&self) -> u16;
    fn req_type(&self) -> u32;
    fn exchange(&self) -> u32;
    fn client_order_id(&self) -> i64;
    fn body(&self) -> &str;
    fn ip_weight(&self) -> u32;
    fn order_count(&self) -> u32;

    fn is_success(&self) -> bool {
        self.status() == 200
    }
}

/// trade engine 返回的通用消息
#[derive(Debug, Clone)]
pub struct TradeEngineResponseMessage {
    status: u16,
    req_type: u32,
    exchange: u32,
    client_order_id: i64,
    body: String,
    ip_weight: u32,
    order_count: u32,
}

impl TradeEngineResponseMessage {
    pub fn new(
        status: u16,
        req_type: u32,
        exchange: u32,
        client_order_id: i64,
        body: String,
        ip_weight: u32,
        order_count: u32,
    ) -> Self {
        Self {
            status,
            req_type,
            exchange,
            client_order_id,
            body,
            ip_weight,
            order_count,
        }
    }
}

impl TradeEngineResponse for TradeEngineResponseMessage {
    fn status(&self) -> u16 {
        self.status
    }

    fn req_type(&self) -> u32 {
        self.req_type
    }

    fn exchange(&self) -> u32 {
        self.exchange
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn body(&self) -> &str {
        &self.body
    }

    fn ip_weight(&self) -> u32 {
        self.ip_weight
    }

    fn order_count(&self) -> u32 {
        self.order_count
    }
}
