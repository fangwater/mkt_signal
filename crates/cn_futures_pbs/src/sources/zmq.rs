use crate::msg::{MarketDataMsg, ParseError};
use anyhow::{Context, Result};
use cn_futures_core::mdl;

pub use cn_futures_core::mdl::TOPICS;

/// Bare fixed-field MDL CSV parser (cn_futures_core::mdl): borrowed bytes,
/// stack slots, no heap or JSON. Only the IPC struct fill happens here.
pub fn parse(frame: &[u8], received_us: u64) -> Result<MarketDataMsg, ParseError> {
    let q = mdl::parse_frame(frame)?;
    let mut msg = MarketDataMsg {
        received_us,
        ..MarketDataMsg::default()
    };
    msg.set_symbol(q.symbol)?;
    msg.set_venue(q.venue)?;
    msg.trading_day = q.trading_day;
    msg.action_day = q.action_day;
    msg.exchange_ts_us = q.exchange_ts_us;
    msg.source_sequence = q.source_sequence;
    msg.last_price = q.last_price;
    msg.volume = q.volume;
    msg.turnover = q.turnover;
    msg.open_interest = q.open_interest;
    for level in 0..5 {
        msg.bid[level].price = q.bid[level].price;
        msg.bid[level].quantity = q.bid[level].quantity;
        msg.ask[level].price = q.ask[level].price;
        msg.ask[level].quantity = q.ask[level].quantity;
    }
    Ok(msg)
}

pub struct ZmqSource {
    socket: zmq::Socket,
    monitor: zmq::Socket,
    message: zmq::Message,
    generation: u64,
    connected: bool,
    invalid: u64,
    _context: zmq::Context,
}
impl ZmqSource {
    pub fn new(endpoint: &str, hwm: i32) -> Result<Self> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SUB)?;
        socket.set_linger(0)?;
        socket.set_rcvhwm(hwm)?;
        socket.set_maxmsgsize(1_048_576)?;
        socket.monitor(
            "inproc://cn-futures-monitor",
            zmq::SocketEvent::CONNECTED as i32 | zmq::SocketEvent::DISCONNECTED as i32,
        )?;
        let monitor = context.socket(zmq::PAIR)?;
        monitor.set_linger(0)?;
        monitor.connect("inproc://cn-futures-monitor")?;
        for topic in TOPICS {
            socket.set_subscribe(format!("{topic} || ").as_bytes())?;
        }
        socket
            .connect(endpoint)
            .context("connect company-intranet Tonglian ZMQ source")?;
        Ok(Self {
            socket,
            monitor,
            message: zmq::Message::new(),
            generation: 0,
            connected: false,
            invalid: 0,
            _context: context,
        })
    }
    fn reset(&mut self) -> MarketDataMsg {
        self.generation = self.generation.saturating_add(1);
        MarketDataMsg::reset(self.generation)
    }
}
impl super::Source for ZmqSource {
    fn poll(&mut self) -> Result<Option<MarketDataMsg>> {
        match self.monitor.recv_msg(zmq::DONTWAIT) {
            Ok(event) => {
                anyhow::ensure!(
                    event.len() == 6 && self.monitor.get_rcvmore()?,
                    "invalid ZMQ monitor event"
                );
                let _address = self.monitor.recv_msg(0)?;
                self.connected = u16::from_ne_bytes([event[0], event[1]])
                    == zmq::SocketEvent::CONNECTED.to_raw();
                return Ok(Some(self.reset()));
            }
            Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => {}
            Err(e) => return Err(e.into()),
        }
        match self.socket.recv(&mut self.message, zmq::DONTWAIT) {
            Ok(()) => {}
            Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        if self.socket.get_rcvmore()? {
            while self.socket.get_rcvmore()? {
                self.socket.recv(&mut self.message, 0)?;
            }
            self.invalid = self.invalid.saturating_add(1);
            eprintln!(
                "cn-futures-pbs: rejected multipart input; invalid={}",
                self.invalid
            );
            return Ok(Some(self.reset()));
        }
        // Socket buffers may still contain old frames after a disconnect.
        // Keep the stream invalid until the transport reports a new connection.
        if !self.connected {
            return Ok(None);
        }
        match parse(&self.message, crate::now_us()) {
            Ok(mut msg) => {
                msg.source_generation = self.generation;
                Ok(Some(msg))
            }
            Err(e) => {
                self.invalid = self.invalid.saturating_add(1);
                if self.invalid == 1 || self.invalid.is_multiple_of(1024) {
                    eprintln!("cn-futures-pbs: {e}; invalid={}", self.invalid);
                }
                Ok(Some(self.reset()))
            }
        }
    }
}
