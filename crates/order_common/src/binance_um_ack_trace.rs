use std::net::IpAddr;

pub const BINANCE_UM_NEW_ACK_TRACE_MSG_TYPE: u32 = 7200;
pub const BINANCE_UM_NEW_ACK_TRACE_SCHEMA_VER: u32 = 1;
pub const BINANCE_UM_NEW_ACK_TRACE_SERVICE: &str = "te_pubs/binance/um_new_ack_trace";
pub const BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN: usize =
    std::mem::size_of::<BinanceUmNewAckTraceMsg>();

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinanceUmNewAckTraceMsg {
    pub msg_type: u32,
    pub schema_ver: u32,
    pub endpoint_id: u32,
    pub route_group_id: u32,
    pub client_order_id: i64,
    pub transport_id: i64,
    pub order_create_time_us: i64,
    pub ws_send_start_time_us: i64,
    pub ws_send_done_time_us: i64,
    pub ack_recv_time_us: i64,
    pub rtt_us: i64,
    pub local_ip: [u8; 16],
    pub remote_ip: [u8; 16],
}

impl BinanceUmNewAckTraceMsg {
    pub fn new(
        endpoint_id: u32,
        route_group_id: u32,
        client_order_id: i64,
        transport_id: i64,
        order_create_time_us: i64,
        ws_send_start_time_us: i64,
        ws_send_done_time_us: i64,
        ack_recv_time_us: i64,
        rtt_us: i64,
        local_ip: IpAddr,
        remote_ip: Option<IpAddr>,
    ) -> Self {
        Self {
            msg_type: BINANCE_UM_NEW_ACK_TRACE_MSG_TYPE,
            schema_ver: BINANCE_UM_NEW_ACK_TRACE_SCHEMA_VER,
            endpoint_id,
            route_group_id,
            client_order_id,
            transport_id,
            order_create_time_us,
            ws_send_start_time_us,
            ws_send_done_time_us,
            ack_recv_time_us,
            rtt_us,
            local_ip: ip_to_bytes(local_ip),
            remote_ip: remote_ip.map(ip_to_bytes).unwrap_or([0; 16]),
        }
    }

    pub fn into_bytes(self) -> [u8; BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN] {
        // SAFETY: `BinanceUmNewAckTraceMsg` is `#[repr(C)]` and all fields are
        // initialized. The payload length is exactly `size_of::<Self>()`.
        unsafe { std::mem::transmute(self) }
    }

    pub fn from_bytes(raw: &[u8]) -> Option<Self> {
        if raw.len() != BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN {
            return None;
        }
        let mut out = Self {
            msg_type: 0,
            schema_ver: 0,
            endpoint_id: 0,
            route_group_id: 0,
            client_order_id: 0,
            transport_id: 0,
            order_create_time_us: 0,
            ws_send_start_time_us: 0,
            ws_send_done_time_us: 0,
            ack_recv_time_us: 0,
            rtt_us: 0,
            local_ip: [0; 16],
            remote_ip: [0; 16],
        };
        let dst = unsafe {
            std::slice::from_raw_parts_mut(
                (&mut out as *mut Self).cast::<u8>(),
                BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN,
            )
        };
        dst.copy_from_slice(raw);
        (out.msg_type == BINANCE_UM_NEW_ACK_TRACE_MSG_TYPE
            && out.schema_ver == BINANCE_UM_NEW_ACK_TRACE_SCHEMA_VER)
            .then_some(out)
    }
}

fn ip_to_bytes(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(ip) => ip.to_ipv6_mapped().octets(),
        IpAddr::V6(ip) => ip.octets(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_size_is_stable() {
        assert_eq!(BINANCE_UM_NEW_ACK_TRACE_PAYLOAD_LEN, 104);
    }

    #[test]
    fn roundtrip_payload() {
        let msg = BinanceUmNewAckTraceMsg::new(
            7,
            3,
            1001,
            2002,
            10,
            18,
            20,
            35,
            15,
            "172.31.33.133".parse().unwrap(),
            Some("13.112.240.202".parse().unwrap()),
        );
        let parsed = BinanceUmNewAckTraceMsg::from_bytes(&msg.into_bytes()).unwrap();
        assert_eq!(parsed, msg);
        assert_eq!(
            parsed.local_ip,
            ip_to_bytes("172.31.33.133".parse().unwrap())
        );
        assert_eq!(
            parsed.remote_ip,
            ip_to_bytes("13.112.240.202".parse().unwrap())
        );
    }
}
