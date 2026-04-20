use bytes::{BufMut, Bytes, BytesMut};
use log::debug;
use std::convert::TryFrom;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum QueryRequestType {
    BinanceMarginQuery = 6001,
    BinanceUMQuery = 6002,
    BinanceWsUMQuery = 6003,
    BinanceWsMarginQuery = 6004,
    BinancePmBalanceSnapshot = 6101,
    BinanceUmAccountSnapshot = 6102,
    BinanceUmBalanceSnapshotStd = 6103,
    BinanceUmAccountSnapshotStd = 6104,
    BinanceSpotAccountSnapshotStd = 6105,
    OkexMarginQuery = 7001,
    OkexUMQuery = 7002,
    OkexAccountBalanceSnapshot = 7101,
    OkexPositionsSnapshot = 7102,
    GateUnifiedOrderQuery = 8001,
    GateFuturesOrderQuery = 8002,
    GateUnifiedBalanceSnapshot = 8101,
    GateUnifiedPositionsSnapshot = 8102,
    BybitMarginQuery = 9001,
    BybitUMQuery = 9002,
    BybitAccountBalanceSnapshot = 9101,
    BybitPositionsSnapshot = 9102,
    BitgetMarginQuery = 9201,
    BitgetUMQuery = 9202,
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
            6003 => Ok(QueryRequestType::BinanceWsUMQuery),
            6004 => Ok(QueryRequestType::BinanceWsMarginQuery),
            6101 => Ok(QueryRequestType::BinancePmBalanceSnapshot),
            6102 => Ok(QueryRequestType::BinanceUmAccountSnapshot),
            6103 => Ok(QueryRequestType::BinanceUmBalanceSnapshotStd),
            6104 => Ok(QueryRequestType::BinanceUmAccountSnapshotStd),
            6105 => Ok(QueryRequestType::BinanceSpotAccountSnapshotStd),
            7001 => Ok(QueryRequestType::OkexMarginQuery),
            7002 => Ok(QueryRequestType::OkexUMQuery),
            7101 => Ok(QueryRequestType::OkexAccountBalanceSnapshot),
            7102 => Ok(QueryRequestType::OkexPositionsSnapshot),
            8001 => Ok(QueryRequestType::GateUnifiedOrderQuery),
            8002 => Ok(QueryRequestType::GateFuturesOrderQuery),
            8101 => Ok(QueryRequestType::GateUnifiedBalanceSnapshot),
            8102 => Ok(QueryRequestType::GateUnifiedPositionsSnapshot),
            9001 => Ok(QueryRequestType::BybitMarginQuery),
            9002 => Ok(QueryRequestType::BybitUMQuery),
            9101 => Ok(QueryRequestType::BybitAccountBalanceSnapshot),
            9102 => Ok(QueryRequestType::BybitPositionsSnapshot),
            9201 => Ok(QueryRequestType::BitgetMarginQuery),
            9202 => Ok(QueryRequestType::BitgetUMQuery),
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
