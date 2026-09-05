use bytes::{BufMut, Bytes, BytesMut};
use log::debug;
use mkt_parsers::msg::hyperliquid_account_msg::HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN;
use std::convert::TryFrom;

pub const SNAPSHOT_COMPLETE_MARKER: &[u8] = b"SNAPSHOT_COMPLETE";
pub const SNAPSHOT_BEGIN_MARKER: &[u8] = b"SNAPSHOT_BEGIN";

pub fn snapshot_begin_body(account_scope: u32) -> Bytes {
    let mut body = BytesMut::with_capacity(SNAPSHOT_BEGIN_MARKER.len() + 4);
    body.put_slice(SNAPSHOT_BEGIN_MARKER);
    body.put_u32_le(account_scope);
    body.freeze()
}

pub fn snapshot_begin_scope(body: &[u8]) -> Option<u32> {
    let scope_start = SNAPSHOT_BEGIN_MARKER.len();
    let scope_end = scope_start.checked_add(4)?;
    if body.len() < scope_end || !body.starts_with(SNAPSHOT_BEGIN_MARKER) {
        return None;
    }
    if body[scope_end..].iter().any(|byte| *byte != 0) {
        return None;
    }
    Some(u32::from_le_bytes(
        body[scope_start..scope_end].try_into().ok()?,
    ))
}

pub fn is_snapshot_complete_body(body: &[u8]) -> bool {
    body.starts_with(SNAPSHOT_COMPLETE_MARKER)
        && body[SNAPSHOT_COMPLETE_MARKER.len()..]
            .iter()
            .all(|byte| *byte == 0)
}

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum QueryRequestType {
    BinanceMarginQuery = 6001,
    BinanceUMQuery = 6002,
    BinanceWsUMQuery = 6003,
    BinanceWsMarginQuery = 6004,
    BinanceCmQuery = 6005,
    BinancePmCmQuery = 6006,
    BinancePmBalanceSnapshot = 6101,
    BinanceUmAccountSnapshot = 6102,
    BinanceUmBalanceSnapshotStd = 6103,
    BinanceUmAccountSnapshotStd = 6104,
    BinanceSpotAccountSnapshotStd = 6105,
    BinancePmAccountSnapshot = 6106,
    BinanceCmBalanceSnapshotStd = 6107,
    BinanceCmAccountSnapshotStd = 6108,
    BinancePmCmAccountSnapshot = 6109,
    OkexMarginQuery = 7001,
    OkexUMQuery = 7002,
    OkexAccountBalanceSnapshot = 7101,
    OkexPositionsSnapshot = 7102,
    OkexUsdtAvailableSnapshot = 7103,
    OkexUsdtMaxLoan = 7104,
    GateUnifiedOrderQuery = 8001,
    GateFuturesOrderQuery = 8002,
    GateUnifiedBalanceSnapshot = 8101,
    GateUnifiedPositionsSnapshot = 8102,
    GateUnifiedUsdtAvailableSnapshot = 8103,
    GateUnifiedUsdtMaxBorrowable = 8104,
    BybitMarginQuery = 9001,
    BybitUMQuery = 9002,
    BybitAccountBalanceSnapshot = 9101,
    BybitPositionsSnapshot = 9102,
    BitgetMarginQuery = 9201,
    BitgetUMQuery = 9202,
    BitgetAccountBalanceSnapshot = 9203,
    BitgetPositionsSnapshot = 9204,
    BitgetUsdtAvailableSnapshot = 9205,
    BitgetUsdtMaxTransferable = 9206,
    BitgetCoinFuturesQuery = 9207,
    BitgetCoinPositionsSnapshot = 9208,
    HyperliquidMarginQuery = 9301,
    HyperliquidUMQuery = 9302,
    HyperliquidClearinghouseSnapshot = 9401,
    HyperliquidSpotStateSnapshot = 9402,
    HyperliquidUserAbstraction = 9403,
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
            6005 => Ok(QueryRequestType::BinanceCmQuery),
            6006 => Ok(QueryRequestType::BinancePmCmQuery),
            6101 => Ok(QueryRequestType::BinancePmBalanceSnapshot),
            6102 => Ok(QueryRequestType::BinanceUmAccountSnapshot),
            6103 => Ok(QueryRequestType::BinanceUmBalanceSnapshotStd),
            6104 => Ok(QueryRequestType::BinanceUmAccountSnapshotStd),
            6105 => Ok(QueryRequestType::BinanceSpotAccountSnapshotStd),
            6106 => Ok(QueryRequestType::BinancePmAccountSnapshot),
            6107 => Ok(QueryRequestType::BinanceCmBalanceSnapshotStd),
            6108 => Ok(QueryRequestType::BinanceCmAccountSnapshotStd),
            6109 => Ok(QueryRequestType::BinancePmCmAccountSnapshot),
            7001 => Ok(QueryRequestType::OkexMarginQuery),
            7002 => Ok(QueryRequestType::OkexUMQuery),
            7101 => Ok(QueryRequestType::OkexAccountBalanceSnapshot),
            7102 => Ok(QueryRequestType::OkexPositionsSnapshot),
            7103 => Ok(QueryRequestType::OkexUsdtAvailableSnapshot),
            7104 => Ok(QueryRequestType::OkexUsdtMaxLoan),
            8001 => Ok(QueryRequestType::GateUnifiedOrderQuery),
            8002 => Ok(QueryRequestType::GateFuturesOrderQuery),
            8101 => Ok(QueryRequestType::GateUnifiedBalanceSnapshot),
            8102 => Ok(QueryRequestType::GateUnifiedPositionsSnapshot),
            8103 => Ok(QueryRequestType::GateUnifiedUsdtAvailableSnapshot),
            8104 => Ok(QueryRequestType::GateUnifiedUsdtMaxBorrowable),
            9001 => Ok(QueryRequestType::BybitMarginQuery),
            9002 => Ok(QueryRequestType::BybitUMQuery),
            9101 => Ok(QueryRequestType::BybitAccountBalanceSnapshot),
            9102 => Ok(QueryRequestType::BybitPositionsSnapshot),
            9201 => Ok(QueryRequestType::BitgetMarginQuery),
            9202 => Ok(QueryRequestType::BitgetUMQuery),
            9203 => Ok(QueryRequestType::BitgetAccountBalanceSnapshot),
            9204 => Ok(QueryRequestType::BitgetPositionsSnapshot),
            9205 => Ok(QueryRequestType::BitgetUsdtAvailableSnapshot),
            9206 => Ok(QueryRequestType::BitgetUsdtMaxTransferable),
            9207 => Ok(QueryRequestType::BitgetCoinFuturesQuery),
            9208 => Ok(QueryRequestType::BitgetCoinPositionsSnapshot),
            9301 => Ok(QueryRequestType::HyperliquidMarginQuery),
            9302 => Ok(QueryRequestType::HyperliquidUMQuery),
            9401 => Ok(QueryRequestType::HyperliquidClearinghouseSnapshot),
            9402 => Ok(QueryRequestType::HyperliquidSpotStateSnapshot),
            9403 => Ok(QueryRequestType::HyperliquidUserAbstraction),
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

/// Account-bound payload for every Hyperliquid info request crossing IPC.
/// The body remains request-specific JSON (or empty for account snapshots).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidQueryParams {
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub body: Bytes,
}

impl HyperliquidQueryParams {
    pub fn create(account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN], body: Bytes) -> Self {
        Self { account_hash, body }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf =
            BytesMut::with_capacity(HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + self.body.len());
        buf.put_slice(&self.account_hash);
        buf.put_slice(&self.body);
        buf.freeze()
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() < HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN {
            return None;
        }
        let (account_hash, body) = raw.split_at(HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN);
        Some(Self {
            account_hash: account_hash.try_into().ok()?,
            body: Bytes::copy_from_slice(body),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_complete_marker_allows_ipc_zero_padding() {
        let mut body = SNAPSHOT_COMPLETE_MARKER.to_vec();
        body.resize(64, 0);
        assert!(is_snapshot_complete_body(&body));
        body[63] = 1;
        assert!(!is_snapshot_complete_body(&body));
    }

    #[test]
    fn hyperliquid_query_params_roundtrip_account_identity_and_body() {
        let params = HyperliquidQueryParams::create(
            [9; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            Bytes::from_static(b"{\"oid\":123}"),
        );
        let decoded = HyperliquidQueryParams::from_bytes(&params.to_bytes()).unwrap();
        assert_eq!(decoded, params);
        assert!(HyperliquidQueryParams::from_bytes(&[0; 31]).is_none());
    }

    #[test]
    fn snapshot_begin_marker_retains_scope_through_ipc_zero_padding() {
        let mut body = snapshot_begin_body(17).to_vec();
        body.resize(64, 0);
        assert_eq!(snapshot_begin_scope(&body), Some(17));
        body[63] = 1;
        assert_eq!(snapshot_begin_scope(&body), None);
    }

    #[test]
    fn hyperliquid_query_types_round_trip_through_ipc_header() {
        for req_type in [
            QueryRequestType::HyperliquidMarginQuery,
            QueryRequestType::HyperliquidUMQuery,
            QueryRequestType::HyperliquidClearinghouseSnapshot,
            QueryRequestType::HyperliquidSpotStateSnapshot,
            QueryRequestType::HyperliquidUserAbstraction,
        ] {
            let request = GenericQueryRequest::create(req_type, 123, 456, Bytes::new());
            let parsed = QueryRequestMsg::parse(&request.to_bytes()).expect("parse query request");
            assert_eq!(parsed.req_type, req_type);
            assert_eq!(parsed.create_time, 123);
            assert_eq!(parsed.client_query_id, 456);
        }
    }
}
