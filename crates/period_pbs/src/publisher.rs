use anyhow::{Context, Result};

use crate::config::ZmqConfig;

pub struct PeriodPublisher {
    _context: zmq::Context,
    socket: zmq::Socket,
    send_flags: i32,
}

impl PeriodPublisher {
    pub fn bind(config: &ZmqConfig) -> Result<Self> {
        let context = zmq::Context::new();
        let socket = context
            .socket(zmq::PUB)
            .context("create period_pbs ZMQ PUB socket")?;
        socket
            .set_sndhwm(config.sndhwm)
            .context("set period_pbs ZMQ PUB sndhwm")?;
        socket
            .set_linger(config.linger_ms)
            .context("set period_pbs ZMQ PUB linger")?;
        socket
            .bind(config.bind.trim())
            .with_context(|| format!("bind period_pbs ZMQ PUB on {}", config.bind.trim()))?;

        Ok(Self {
            _context: context,
            socket,
            send_flags: if config.send_dontwait {
                zmq::DONTWAIT
            } else {
                0
            },
        })
    }

    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
        self.socket
            .send_multipart([topic.as_bytes(), payload], self.send_flags)
            .with_context(|| format!("publish period_pbs payload topic={topic}"))
    }
}
