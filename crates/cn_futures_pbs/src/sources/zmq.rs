use crate::msg::{day, number, timestamp, unsigned, MarketDataMsg, ParseError};
use anyhow::{Context, Result};

pub const TOPICS: [&str; 6] = [
    "mdl.21.1.*",
    "mdl.22.1.*",
    "mdl.22.3.*",
    "mdl.23.1.*",
    "mdl.24.1.*",
    "mdl.26.1.*",
];

/// Bare fixed-field MDL CSV parser: borrowed bytes, stack slots, no heap or JSON.
pub fn parse(frame: &[u8], received_us: u64) -> Result<MarketDataMsg, ParseError> {
    if frame.get(10..14) != Some(b" || ") {
        return Err(ParseError("invalid single-frame delimiter"));
    }
    // count, symbol, action date, last/volume/turnover/OI, bid start, ask start, stride.
    let (venue, count, symbol, action, last, volume, turnover, oi, bid, ask, stride) =
        match &frame[..10] {
            b"mdl.21.1.*" => ("CFFEX", 43, 3, 0, 4, 8, 9, 10, 19, 29, 2),
            b"mdl.22.1.*" => ("SHFE", 43, 0, 2, 4, 11, 12, 13, 21, 31, 2),
            b"mdl.22.3.*" => ("INE", 43, 0, 2, 4, 11, 12, 13, 21, 31, 2),
            b"mdl.23.1.*" => ("CZCE", 59, 3, 0, 4, 8, 9, 10, 23, 38, 3),
            b"mdl.24.1.*" => ("DCE", 59, 3, 0, 4, 9, 10, 11, 23, 38, 3),
            b"mdl.26.1.*" => ("GFEX", 59, 3, 0, 4, 9, 10, 11, 23, 38, 3),
            _ => return Err(ParseError("not a supported futures L2 topic")),
        };
    let mut fields: [&[u8]; 59] = [b""; 59];
    let mut len = 0;
    for (index, value) in frame[14..].split(|b| *b == b',').enumerate() {
        if index < 59 {
            fields[index] = value;
        }
        len = index + 1;
    }
    if len < count {
        return Err(ParseError("truncated futures quote"));
    }
    let mut msg = MarketDataMsg {
        received_us,
        ..MarketDataMsg::default()
    };
    msg.set_symbol(fields[symbol])?;
    msg.set_venue(venue)?;
    msg.trading_day = day(fields[1])?;
    msg.action_day = day(fields[action])?;
    msg.exchange_ts_us = timestamp(msg.action_day, fields[if symbol == 0 { 3 } else { 2 }], 0)?;
    msg.source_sequence = unsigned(fields[count - 1])?;
    msg.last_price = number(fields[last])?;
    msg.volume = unsigned(fields[volume])?;
    msg.turnover = number(fields[turnover])?;
    msg.open_interest = number(fields[oi])?;
    for level in 0..5 {
        msg.bid[level].price = number(fields[bid + level * stride])?;
        msg.bid[level].quantity = unsigned(fields[bid + level * stride + 1])?;
        msg.ask[level].price = number(fields[ask + level * stride])?;
        msg.ask[level].quantity = unsigned(fields[ask + level * stride + 1])?;
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
