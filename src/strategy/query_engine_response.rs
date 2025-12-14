use anyhow::Result;
use bytes::Bytes;

pub trait QueryEngineResponse {
    fn req_type(&self) -> u32;
    fn client_query_id(&self) -> i64;
    fn body_bytes(&self) -> &Bytes;
}

#[derive(Debug, Clone)]
pub struct QueryEngineResponseMessage {
    req_type: u32,
    client_query_id: i64,
    body: Bytes,
}

impl QueryEngineResponseMessage {
    pub fn new(req_type: u32, client_query_id: i64, body: Bytes) -> Self {
        Self {
            req_type,
            client_query_id,
            body,
        }
    }

    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        const HEADER_LEN: usize = 4 + 8;
        if payload.len() < HEADER_LEN {
            anyhow::bail!("payload too short: {} < {}", payload.len(), HEADER_LEN);
        }

        let req_type = u32::from_le_bytes(payload[0..4].try_into()?);
        let client_query_id = i64::from_le_bytes(payload[4..12].try_into()?);

        // Body may be binary and can legitimately contain 0, so do NOT trim here.
        let body = Bytes::copy_from_slice(&payload[HEADER_LEN..]);
        Ok(Self::new(req_type, client_query_id, body))
    }

    /// For text responses, trims trailing 0 padding.
    pub fn body_text_lossy_trimmed(&self) -> String {
        let bytes = self.body.as_ref();
        let actual_len = bytes
            .iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1)
            .unwrap_or(0);
        if actual_len == 0 {
            return String::new();
        }
        String::from_utf8_lossy(&bytes[..actual_len]).to_string()
    }
}

impl QueryEngineResponse for QueryEngineResponseMessage {
    fn req_type(&self) -> u32 {
        self.req_type
    }

    fn client_query_id(&self) -> i64 {
        self.client_query_id
    }

    fn body_bytes(&self) -> &Bytes {
        &self.body
    }
}
