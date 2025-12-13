use anyhow::Result;

pub trait QueryEngineResponse {
    fn status(&self) -> u16;
    fn req_type(&self) -> u32;
    fn exchange(&self) -> u32;
    fn client_query_id(&self) -> i64;
    fn body(&self) -> &str;
    fn ip_used_weight_1m(&self) -> u32;
    fn query_count_1m(&self) -> u32;
}

#[derive(Debug, Clone)]
pub struct QueryEngineResponseMessage {
    status: u16,
    req_type: u32,
    exchange: u32,
    client_query_id: i64,
    body: String,
    ip_used_weight_1m: u32,
    query_count_1m: u32,
}

impl QueryEngineResponseMessage {
    pub fn new(
        status: u16,
        req_type: u32,
        exchange: u32,
        client_query_id: i64,
        body: String,
        ip_used_weight_1m: u32,
        query_count_1m: u32,
    ) -> Self {
        Self {
            status,
            req_type,
            exchange,
            client_query_id,
            body,
            ip_used_weight_1m,
            query_count_1m,
        }
    }

    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        const HEADER_LEN: usize = 40;
        if payload.len() < HEADER_LEN {
            anyhow::bail!("payload too short: {} < {}", payload.len(), HEADER_LEN);
        }

        let req_type = u32::from_le_bytes(payload[0..4].try_into()?);
        let client_query_id = i64::from_le_bytes(payload[12..20].try_into()?);
        let exchange = u32::from_le_bytes(payload[20..24].try_into()?);
        let status = u16::from_le_bytes(payload[24..26].try_into()?);
        let ip_used_weight_1m = u32::from_le_bytes(payload[28..32].try_into()?);
        let query_count_1m = u32::from_le_bytes(payload[32..36].try_into()?);
        let body_len = u32::from_le_bytes(payload[36..40].try_into()?) as usize;

        let available_body = payload.len().saturating_sub(HEADER_LEN);
        let actual_body_len = body_len.min(available_body);
        let body = if actual_body_len > 0 {
            String::from_utf8_lossy(&payload[HEADER_LEN..HEADER_LEN + actual_body_len]).to_string()
        } else {
            String::new()
        };

        Ok(Self::new(
            status,
            req_type,
            exchange,
            client_query_id,
            body,
            ip_used_weight_1m,
            query_count_1m,
        ))
    }
}

impl QueryEngineResponse for QueryEngineResponseMessage {
    fn status(&self) -> u16 {
        self.status
    }

    fn req_type(&self) -> u32 {
        self.req_type
    }

    fn exchange(&self) -> u32 {
        self.exchange
    }

    fn client_query_id(&self) -> i64 {
        self.client_query_id
    }

    fn body(&self) -> &str {
        &self.body
    }

    fn ip_used_weight_1m(&self) -> u32 {
        self.ip_used_weight_1m
    }

    fn query_count_1m(&self) -> u32 {
        self.query_count_1m
    }
}

