use log::{debug, warn};
use tokio::net::TcpStream;

pub const DEFAULT_WS_BUSY_POLL_US: u32 = 8;

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
