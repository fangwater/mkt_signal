use bytes::{BufMut, Bytes, BytesMut};
use log::debug;
use std::convert::TryFrom;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum QueryRequestType {
    BinanceMarginQuery = 6001,
    BinanceUMQuery = 6002,
    OkexMarginQuery = 7001,
    OkexUMQuery = 7002,
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct QueryRequestHeader {
    pub msg_type: u32,
    pub params_length: u32,
    pub create_time: i64,
    pub client_query_id: i64,
}

#[derive(Debug, Clone)]
pub struct QueryRequestMsg {
    pub req_type: QueryRequestType,
    pub create_time: i64,
    pub client_query_id: i64,
    pub params: Bytes,
}

impl TryFrom<u32> for QueryRequestType {
    type Error = ();
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            6001 => Ok(QueryRequestType::BinanceMarginQuery),
            6002 => Ok(QueryRequestType::BinanceUMQuery),
            7001 => Ok(QueryRequestType::OkexMarginQuery),
            7002 => Ok(QueryRequestType::OkexUMQuery),
            _ => Err(()),
        }
    }
}

impl QueryRequestMsg {
    /// Layout (little-endian):
    ///   u32 msg_type, u32 params_length, i64 create_time, i64 client_query_id, [params_length] bytes
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < 4 + 4 + 8 + 8 {
            debug!("QueryRequestMsg::parse buffer too short: {}", buf.len());
            return None;
        }
        let msg_type = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let params_len = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
        let create_time = i64::from_le_bytes(buf[8..16].try_into().ok()?);
        let client_query_id = i64::from_le_bytes(buf[16..24].try_into().ok()?);
        if buf.len() < 24 + params_len {
            debug!(
                "QueryRequestMsg::parse invalid params_len: total={}, params_len={}",
                buf.len(),
                params_len
            );
            return None;
        }
        let req_type = QueryRequestType::try_from(msg_type).ok()?;
        let params = Bytes::copy_from_slice(&buf[24..24 + params_len]);
        Some(Self {
            req_type,
            create_time,
            client_query_id,
            params,
        })
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct GenericQueryRequest {
    pub header: QueryRequestHeader,
    pub params: Bytes,
}

impl GenericQueryRequest {
    pub fn create(
        req_type: QueryRequestType,
        create_time: i64,
        client_query_id: i64,
        params: Bytes,
    ) -> Self {
        let header = QueryRequestHeader {
            msg_type: req_type as u32,
            params_length: params.len() as u32,
            create_time,
            client_query_id,
        };
        Self { header, params }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4 + 4 + 8 + 8 + self.params.len();
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.header.msg_type);
        buf.put_u32_le(self.header.params_length);
        buf.put_i64_le(self.header.create_time);
        buf.put_i64_le(self.header.client_query_id);
        buf.put(self.params.clone());
        buf.freeze()
    }
}

