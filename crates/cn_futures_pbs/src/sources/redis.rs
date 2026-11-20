use super::{zmq, Source};
use crate::msg::{MarketDataMsg, ParseError};
use anyhow::{bail, ensure, Result};
use redis::{Parser, Value};
use std::{
    io::Write,
    net::{SocketAddrV4, TcpStream},
    time::{Duration, Instant},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const PONG_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PAYLOAD: usize = 1_048_576;
const DELIMITER: &[u8] = b" || ";

/// Literal channels on the company `data` feeder (port 9380). The Redis guide
/// lists their archived names as `mdl_<type>_<message>_0.csv`.
/// The asterisk is part of the channel name.
pub const DATA_TOPICS: [&str; 49] = [
    // Stock Connect turnover, northbound daily quota, and SH/SZ southbound quota.
    "mdl.11.5.*",
    "mdl.11.6.*",
    "mdl.4.15.*",
    "mdl.6.40.*",
    // Shanghai/Shenzhen stock and index bars, single- and multi-minute.
    "mdl.13.1.*",
    "mdl.13.2.*",
    "mdl.13.9.*",
    "mdl.13.10.*",
    "mdl.13.16.*",
    "mdl.13.17.*",
    "mdl.13.24.*",
    "mdl.13.25.*",
    // Futures bars. SHFE one-minute and multi-minute use channels 5 and 20.
    // Where one channel carries multiple intervals, keep the vendor's Unit.
    "mdl.13.4.*",
    "mdl.13.5.*",
    "mdl.13.6.*",
    "mdl.13.7.*",
    "mdl.13.19.*",
    "mdl.13.20.*",
    "mdl.13.21.*",
    "mdl.13.22.*",
    "mdl.13.57.*",
    "mdl.13.99.*",
    // Options bars. Codes here are MDL message IDs, not exchange IDs.
    "mdl.13.8.*",
    "mdl.13.23.*",
    "mdl.13.40.*",
    "mdl.13.41.*",
    "mdl.13.42.*",
    "mdl.13.43.*",
    "mdl.13.61.*",
    "mdl.13.62.*",
    "mdl.13.78.*",
    "mdl.13.79.*",
    "mdl.13.94.*",
    "mdl.13.100.*",
    // Shenzhen and Shanghai bond bars also appear on the data feeder.
    "mdl.13.96.*",
    "mdl.13.97.*",
    // Futures L2, including spreads, volume by price and best-price queues.
    "mdl.21.1.*",
    "mdl.22.1.*",
    "mdl.22.3.*",
    "mdl.23.1.*",
    "mdl.23.5.*",
    "mdl.24.1.*",
    "mdl.24.3.*",
    "mdl.24.5.*",
    "mdl.24.7.*",
    "mdl.26.1.*",
    "mdl.26.3.*",
    "mdl.26.5.*",
    "mdl.26.7.*",
];

pub struct Message {
    pub topic: String,
    pub payload: Vec<u8>,
}

pub enum Event {
    Reset,
    Message(Message),
}

struct Subscription {
    socket: TcpStream,
    parser: Parser,
    topics: &'static [&'static str],
    acknowledged: usize,
    deadline: Instant,
    next_ping: Instant,
    pong_deadline: Option<Instant>,
}

impl Subscription {
    fn connect(endpoint: SocketAddrV4, topics: &'static [&'static str]) -> Result<Self> {
        let mut socket = TcpStream::connect_timeout(&endpoint.into(), CONNECT_TIMEOUT)?;
        socket.set_read_timeout(Some(READ_TIMEOUT))?;
        socket.set_write_timeout(Some(CONNECT_TIMEOUT))?;
        // This feeder implements exact SUBSCRIBE over RESP2. Send one command
        // per topic and consume acknowledgments alongside interleaved messages.
        for topic in topics {
            socket.write_all(&redis::cmd("SUBSCRIBE").arg(topic).get_packed_command())?;
        }
        let now = Instant::now();
        Ok(Self {
            socket,
            parser: Parser::new(),
            topics,
            acknowledged: 0,
            deadline: now + CONNECT_TIMEOUT,
            next_ping: now,
            pong_deadline: None,
        })
    }

    fn receive(&mut self) -> Result<Option<Message>> {
        let now = Instant::now();
        ensure!(
            self.acknowledged == self.topics.len() || now < self.deadline,
            "missing Redis subscription acknowledgments"
        );
        ensure!(
            self.pong_deadline.is_none_or(|deadline| now < deadline),
            "Redis subscription PING timed out"
        );
        // The company feeder answers PING on this subscribed socket. A
        // separate socket could be healthy while this subscription is stuck.
        if self.acknowledged == self.topics.len()
            && self.pong_deadline.is_none()
            && now >= self.next_ping
        {
            self.socket.write_all(b"*1\r\n$4\r\nPING\r\n")?;
            self.pong_deadline = Some(Instant::now() + PONG_TIMEOUT);
        }
        // Keep the same parser after an idle timeout: TCP can split a RESP
        // message across reads, and the decoder retains its partial frame.
        let value = match self.parser.parse_value(&mut self.socket) {
            Ok(value) => value,
            Err(error) if error.is_timeout() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if matches!(&value, Value::SimpleString(reply) if reply == "PONG") {
            self.pong_deadline = None;
            self.next_ping = Instant::now() + PING_INTERVAL;
            return Ok(None);
        }
        let Value::Array(fields) = value else {
            bail!("unexpected Redis subscription response");
        };
        let Ok([Value::BulkString(kind), Value::BulkString(topic), value]) =
            <[Value; 3]>::try_from(fields)
        else {
            bail!("unexpected Redis Pub/Sub message");
        };
        match (kind.as_slice(), value) {
            (b"subscribe", Value::Int(count)) => {
                ensure!(
                    self.topics
                        .get(self.acknowledged)
                        .map(|name| name.as_bytes())
                        == Some(topic.as_slice())
                        && count == self.acknowledged as i64 + 1,
                    "Redis acknowledged a different subscription"
                );
                self.acknowledged += 1;
                if self.acknowledged == self.topics.len() {
                    eprintln!(
                        "cn-futures-pbs: Redis confirmed {} subscriptions",
                        self.acknowledged
                    );
                }
                Ok(None)
            }
            (b"message", Value::BulkString(payload)) => {
                let topic = String::from_utf8(topic)?;
                ensure!(
                    payload.len() <= MAX_PAYLOAD && self.topics.contains(&topic.as_str()),
                    "invalid Redis message channel or size"
                );
                Ok(Some(Message { topic, payload }))
            }
            _ => bail!("unexpected Redis Pub/Sub message"),
        }
    }
}

impl Message {
    /// Redis supplies the topic separately. Expose the same frame layout as
    /// the ZMQ relay while keeping the vendor CSV byte-for-byte.
    pub fn mdl_frame<'a>(&'a self, buffer: &'a mut Vec<u8>) -> Result<&'a [u8], ParseError> {
        let topic = self.topic.as_bytes();
        if self.payload.get(topic.len()..topic.len() + DELIMITER.len()) == Some(DELIMITER) {
            if !self.payload.starts_with(topic) {
                return Err(ParseError("Redis channel and MDL envelope differ"));
            }
            return Ok(&self.payload);
        }
        buffer.clear();
        buffer.extend_from_slice(topic);
        buffer.extend_from_slice(DELIMITER);
        buffer.extend_from_slice(&self.payload);
        Ok(buffer)
    }
}

/// A reconnect starts a new continuity segment. Pub/Sub does not replay any
/// quotes that elapsed while the socket was disconnected.
pub struct RedisFeed {
    endpoint: SocketAddrV4,
    topics: &'static [&'static str],
    subscription: Option<Subscription>,
    next_connect: Instant,
}

impl RedisFeed {
    pub fn new(endpoint: SocketAddrV4, topics: &'static [&'static str]) -> Self {
        Self {
            endpoint,
            topics,
            subscription: None,
            next_connect: Instant::now(),
        }
    }

    pub fn poll(&mut self) -> Option<Event> {
        let Some(subscription) = self.subscription.as_mut() else {
            let delay = self.next_connect.saturating_duration_since(Instant::now());
            if !delay.is_zero() {
                std::thread::sleep(delay.min(READ_TIMEOUT));
                return None;
            }
            match Subscription::connect(self.endpoint, self.topics) {
                Ok(subscription) => self.subscription = Some(subscription),
                Err(error) => {
                    eprintln!("cn-futures-pbs: Redis feeder connect failed: {error:#}");
                    self.next_connect = Instant::now() + RECONNECT_DELAY;
                    return None;
                }
            }
            return Some(Event::Reset);
        };
        match subscription.receive() {
            Ok(message) => message.map(Event::Message),
            Err(error) => {
                eprintln!("cn-futures-pbs: Redis feeder disconnected: {error:#}");
                self.subscription = None;
                self.next_connect = Instant::now() + RECONNECT_DELAY;
                Some(Event::Reset)
            }
        }
    }
}

/// The six futures books are the subset that maps to MarketDataMsg.
pub struct RedisSource {
    feed: RedisFeed,
    generation: u64,
    frame: Vec<u8>,
}

impl RedisSource {
    pub fn new(endpoint: SocketAddrV4) -> Self {
        Self {
            feed: RedisFeed::new(endpoint, &zmq::TOPICS),
            generation: 0,
            frame: Vec::new(),
        }
    }

    fn reset(&mut self) -> MarketDataMsg {
        self.generation = self.generation.saturating_add(1);
        MarketDataMsg::reset(self.generation)
    }
}

impl Source for RedisSource {
    fn poll(&mut self) -> Result<Option<MarketDataMsg>> {
        let Some(event) = self.feed.poll() else {
            return Ok(None);
        };
        let Event::Message(message) = event else {
            return Ok(Some(self.reset()));
        };
        let quote = message.mdl_frame(&mut self.frame).and_then(|frame| {
            // Redis preserves the source CSV line ending. The ZeroMQ
            // relay strips it, and the shared parser expects a bare SeqNo.
            let row = frame
                .strip_suffix(b"\r\n")
                .or_else(|| frame.strip_suffix(b"\n"))
                .unwrap_or(frame);
            zmq::parse(row, crate::now_us())
        });
        match quote {
            Ok(mut quote) => {
                quote.source_generation = self.generation;
                Ok(Some(quote))
            }
            Err(error) => {
                eprintln!("cn-futures-pbs: Redis futures quote rejected: {error}");
                Ok(Some(self.reset()))
            }
        }
    }
}
