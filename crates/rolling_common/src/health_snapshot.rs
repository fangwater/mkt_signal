//! Per-connection trade-engine TCP health snapshot.
//!
//! One fixed-size message is published per websocket connection every second.
//! Health decisions use kernel `TCP_INFO`; the retransmission ratio is
//! informational only. Any positive retransmission delta is an anomaly.

use std::net::{IpAddr, SocketAddr};

pub const HEALTH_SNAPSHOT_MSG_TYPE: u32 = 7200;
pub const HEALTH_SNAPSHOT_SCHEMA_VER: u32 = 1;
pub const HEALTH_SNAPSHOT_PAYLOAD_LEN: usize = std::mem::size_of::<HealthSnapshotMsg>();

pub const HEALTH_MARKET_UNKNOWN: u8 = 0;
pub const HEALTH_MARKET_SPOT: u8 = 1;
pub const HEALTH_MARKET_FUTURES: u8 = 2;

pub const HEALTH_STATE_DISCONNECTED: u8 = 0;
pub const HEALTH_STATE_HEALTHY: u8 = 1;
pub const HEALTH_STATE_PAUSED: u8 = 2;
pub const HEALTH_STATE_DRAINING: u8 = 3;
pub const HEALTH_STATE_RECONNECTING: u8 = 4;
pub const HEALTH_STATE_PROTECTED: u8 = 5;

pub const HEALTH_FLAG_CONNECTED: u16 = 1 << 0;
pub const HEALTH_FLAG_ROUTE_PAUSED: u16 = 1 << 1;
pub const HEALTH_FLAG_RECONNECT_PENDING: u16 = 1 << 2;
pub const HEALTH_FLAG_RECONNECTING: u16 = 1 << 3;
pub const HEALTH_FLAG_TCP_LOSS_ACT: u16 = 1 << 4;
pub const HEALTH_FLAG_LAST_ROUTE_PROTECTED: u16 = 1 << 5;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HealthSnapshotMsg {
    pub msg_type: u32,
    pub schema_ver: u32,
    pub venue_id: u32,
    pub endpoint_id: u32,
    pub snapshot_time_us: i64,
    pub sample_interval_ms: u32,
    pub window_ms: u32,
    pub group_id: u32,
    pub market_id: u8,
    pub state: u8,
    pub flags: u16,
    pub local_ip: [u8; 16],
    pub remote_ip: [u8; 16],
    pub remote_port: u16,
    pub _pad0: [u8; 6],
    pub window_data_segs_out: u64,
    pub window_retrans: u64,
    pub total_retrans: u32,
    pub rtt_us: u32,
    pub rttvar_us: u32,
    pub last_retrans_age_ms: u32,
    pub pending: u32,
    pub inflight: u32,
    pub query_pending: u32,
    pub query_inflight: u32,
    pub _reserved: [u8; 384],
}

impl HealthSnapshotMsg {
    pub fn new(venue_id: u32, endpoint_id: u32, snapshot_time_us: i64) -> Self {
        Self {
            msg_type: HEALTH_SNAPSHOT_MSG_TYPE,
            schema_ver: HEALTH_SNAPSHOT_SCHEMA_VER,
            venue_id,
            endpoint_id,
            snapshot_time_us,
            sample_interval_ms: 0,
            window_ms: 0,
            group_id: u32::MAX,
            market_id: HEALTH_MARKET_UNKNOWN,
            state: HEALTH_STATE_DISCONNECTED,
            flags: 0,
            local_ip: [0; 16],
            remote_ip: [0; 16],
            remote_port: 0,
            _pad0: [0; 6],
            window_data_segs_out: 0,
            window_retrans: 0,
            total_retrans: 0,
            rtt_us: 0,
            rttvar_us: 0,
            last_retrans_age_ms: u32::MAX,
            pending: 0,
            inflight: 0,
            query_pending: 0,
            query_inflight: 0,
            _reserved: [0; 384],
        }
    }

    pub fn set_local_ip(&mut self, ip: IpAddr) {
        self.local_ip = ip_bytes(ip);
    }

    pub fn set_remote_addr(&mut self, addr: Option<SocketAddr>) {
        if let Some(addr) = addr {
            self.remote_ip = ip_bytes(addr.ip());
            self.remote_port = addr.port();
        } else {
            self.remote_ip = [0; 16];
            self.remote_port = 0;
        }
    }

    pub fn into_bytes(self) -> [u8; HEALTH_SNAPSHOT_PAYLOAD_LEN] {
        // SAFETY: repr(C), all padding is explicit and initialized, and the
        // destination has exactly the same size.
        unsafe { std::mem::transmute(self) }
    }
}

pub fn ip_bytes(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(ip) => ip.to_ipv6_mapped().octets(),
        IpAddr::V6(ip) => ip.octets(),
    }
}

pub fn ip_from_bytes(bytes: [u8; 16]) -> IpAddr {
    let ip = std::net::Ipv6Addr::from(bytes);
    ip.to_ipv4_mapped()
        .map(IpAddr::V4)
        .unwrap_or(IpAddr::V6(ip))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_512_bytes() {
        assert_eq!(HEALTH_SNAPSHOT_PAYLOAD_LEN, 512);
    }

    #[test]
    fn ip_roundtrip_supports_v4_and_v6() {
        for ip in ["10.0.0.1".parse().unwrap(), "2001:db8::1".parse().unwrap()] {
            assert_eq!(ip_from_bytes(ip_bytes(ip)), ip);
        }
    }

    #[test]
    fn header_roundtrip() {
        let msg = HealthSnapshotMsg::new(4, 7, 123);
        let bytes = msg.into_bytes();
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 7200);
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), 7);
    }
}
