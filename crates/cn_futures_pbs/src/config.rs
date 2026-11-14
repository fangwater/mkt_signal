use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub service_root: String,
    pub zmq: Option<Zmq>,
    pub ctp_config: Option<PathBuf>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Zmq {
    pub access_scope: String,
    pub endpoint: String,
    pub receive_hwm: i32,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let config: Self =
            toml::from_str(&std::fs::read_to_string(path).context("read cn-futures-pbs config")?)
                .context("parse cn-futures-pbs config")?;
        crate::ipc::validate_root(&config.service_root)?;
        Ok(config)
    }
}
impl Zmq {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.access_scope == "company_intranet",
            "--zmq currently implements the company_intranet Tonglian relay only"
        );
        let endpoint = self
            .endpoint
            .strip_prefix("tcp://")
            .context("company ZMQ requires explicit TCP endpoint")?
            .parse::<SocketAddrV4>()
            .context("company ZMQ requires an IPv4 literal and port")?;
        let ip: Ipv4Addr = *endpoint.ip();
        ensure!(
            (ip.is_private() || ip.is_loopback()) && endpoint.port() > 0 && self.receive_hwm > 0,
            "company relay requires private IPv4, positive port and receive_hwm"
        );
        Ok(())
    }
}
