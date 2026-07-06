use log::{debug, warn};
use tokio::net::TcpStream;

pub const DEFAULT_WS_BUSY_POLL_US: u32 = 8;

/// 从内核 `TCP_INFO` 读取的连接级健康快照。
/// `total_retrans`/`segs_out` 用于算重传率（路径丢包的直接指标，
/// 是 spot/UM 共享的 TCP 层信号）；`rtt_us`/`rttvar_us` 供参考。
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpRetransSnapshot {
    pub total_retrans: u32,
    pub segs_out: u32,
    pub data_segs_out: u32,
    pub rtt_us: u32,
    pub rttvar_us: u32,
}

/// 通过 `getsockopt(TCP_INFO)` 读取一条 TCP 连接的重传/RTT 快照。
/// 纯用户态、无需任何 capability。非 Linux 或读取失败返回 `None`。
///
/// libc 的 `tcp_info` 结构冻结在较老 ABI、缺 `tcpi_segs_out`/`tcpi_data_segs_out`，
/// 故直接读原始字节按偏移取字段。内核 `struct tcp_info` 只在末尾追加、字段偏移稳定，
/// 因此这些偏移在 x86_64 Linux 上可靠；越界字段回退 0。
#[cfg(target_os = "linux")]
pub fn read_tcp_retrans_snapshot(fd: std::os::fd::RawFd) -> Option<TcpRetransSnapshot> {
    // 偏移来自 linux/tcp.h 的 struct tcp_info（x86_64 布局）。
    const OFF_RTT: usize = 68;
    const OFF_RTTVAR: usize = 72;
    const OFF_TOTAL_RETRANS: usize = 100;
    const OFF_SEGS_OUT: usize = 136;
    const OFF_DATA_SEGS_OUT: usize = 156;

    let mut buf = [0u8; 256];
    let mut len = buf.len() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            buf.as_mut_ptr() as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return None;
    }
    let len = len as usize;
    let u32_at = |off: usize| -> u32 {
        match buf.get(off..off + 4) {
            Some(slice) if off + 4 <= len => u32::from_ne_bytes(slice.try_into().unwrap()),
            _ => 0,
        }
    };
    Some(TcpRetransSnapshot {
        total_retrans: u32_at(OFF_TOTAL_RETRANS),
        segs_out: u32_at(OFF_SEGS_OUT),
        data_segs_out: u32_at(OFF_DATA_SEGS_OUT),
        rtt_us: u32_at(OFF_RTT),
        rttvar_us: u32_at(OFF_RTTVAR),
    })
}

#[cfg(not(target_os = "linux"))]
pub fn read_tcp_retrans_snapshot(_fd: std::os::fd::RawFd) -> Option<TcpRetransSnapshot> {
    None
}

#[cfg(all(test, target_os = "linux"))]
mod tcp_info_tests {
    use super::read_tcp_retrans_snapshot;

    // 通过 loopback 连接发点数据，验证 TCP_INFO 字段偏移不是垃圾：
    // data_segs_out 至少发过一段、rtt_us 非 0。
    #[test]
    fn tcp_info_snapshot_reads_sane_fields() {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::os::fd::AsRawFd;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let mut client = TcpStream::connect(addr).expect("connect");
        let (mut server, _) = listener.accept().expect("accept");
        client.write_all(&[0u8; 8192]).expect("write");
        client.flush().expect("flush");
        let mut buf = [0u8; 8192];
        let _ = server.read(&mut buf);

        let snap = read_tcp_retrans_snapshot(client.as_raw_fd()).expect("snapshot");
        assert!(
            snap.data_segs_out >= 1,
            "data_segs_out={}",
            snap.data_segs_out
        );
        assert!(snap.rtt_us > 0, "rtt_us={}", snap.rtt_us);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TcpSocketTuning {
    pub nodelay: bool,
    pub quickack: bool,
    pub user_timeout_ms: Option<u32>,
    pub busy_poll_us: Option<u32>,
}

impl Default for TcpSocketTuning {
    fn default() -> Self {
        Self {
            nodelay: true,
            quickack: true,
            user_timeout_ms: None,
            busy_poll_us: None,
        }
    }
}

pub fn ipc_fast_poll_enabled() -> bool {
    for name in ["enable_ipc_fast_poll", "ENABLE_IPC_FAST_POLL"] {
        let Ok(raw) = std::env::var(name) else {
            continue;
        };
        match parse_bool_env(&raw) {
            Some(enabled) => return enabled,
            None => warn!(
                "invalid {}='{}', treating IPC-gated socket busy poll as disabled",
                name, raw
            ),
        }
    }
    false
}

fn parse_bool_env(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

pub fn tune_tcp_stream(stream: &TcpStream, label: &str, tuning: TcpSocketTuning) {
    if tuning.nodelay {
        if let Err(err) = stream.set_nodelay(true) {
            warn!("{} TCP_NODELAY failed: {}", label, err);
        }
    }

    #[cfg(target_os = "linux")]
    tune_tcp_stream_linux(stream, label, tuning);

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (stream, label, tuning);
    }
}

#[cfg(target_os = "linux")]
fn tune_tcp_stream_linux(stream: &TcpStream, label: &str, tuning: TcpSocketTuning) {
    use std::os::fd::AsRawFd;

    let fd = stream.as_raw_fd();
    if tuning.quickack {
        set_i32_sockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_QUICKACK,
            1,
            label,
            "TCP_QUICKACK",
        );
    }
    if let Some(ms) = tuning.user_timeout_ms {
        set_i32_sockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_USER_TIMEOUT,
            ms,
            label,
            "TCP_USER_TIMEOUT",
        );
    }
    if let Some(us) = tuning.busy_poll_us {
        set_i32_sockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            us,
            label,
            "SO_BUSY_POLL",
        );
        debug!("{} SO_BUSY_POLL={}us", label, us);
    }
}

#[cfg(target_os = "linux")]
fn set_i32_sockopt(
    fd: std::os::fd::RawFd,
    level: libc::c_int,
    optname: libc::c_int,
    value: u32,
    label: &str,
    opt_label: &str,
) {
    let value = value.min(libc::c_int::MAX as u32) as libc::c_int;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            optname,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if rc != 0 {
        warn!(
            "{} {}={} failed: {}",
            label,
            opt_label,
            value,
            std::io::Error::last_os_error()
        );
    }
}
