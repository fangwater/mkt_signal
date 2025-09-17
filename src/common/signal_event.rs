use anyhow::{ensure, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

pub const SIGNAL_EVENT_MAGIC: u32 = 0x5349474e; // 'SIGN'
pub const SIGNAL_EVENT_VERSION: u16 = 1;
pub const SIGNAL_EVENT_HEADER_SIZE: usize = 36;
pub const SIGNAL_EVENT_MAX_FRAME: usize = 1024;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalEventType {
    TradeSignal = 1,
}

impl SignalEventType {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(Self::TradeSignal),
            _ => None,
        }
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalEventHeader {
    pub magic: u32,
    pub version: u16,
    pub event_type: u16,
    pub payload_len: u32,
    pub event_ts_ms: i64,
    pub publish_ts_ms: i64,
    pub reserved0: u32,
    pub reserved1: u32,
}

impl SignalEventHeader {
    pub const SIZE: usize = SIGNAL_EVENT_HEADER_SIZE;

    pub fn new(event_type: SignalEventType, event_ts_ms: i64, payload_len: usize) -> Result<Self> {
        ensure!(
            payload_len <= u32::MAX as usize,
            "payload太大: {}",
            payload_len
        );
        Ok(Self {
            magic: SIGNAL_EVENT_MAGIC,
            version: SIGNAL_EVENT_VERSION,
            event_type: event_type.as_u16(),
            payload_len: payload_len as u32,
            event_ts_ms,
            publish_ts_ms: Utc::now().timestamp_millis(),
            reserved0: 0,
            reserved1: 0,
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() >= Self::SIZE, "信号头长度不足: {}", bytes.len());
        let magic = u32::from_le_bytes(bytes[0..4].try_into()?);
        let version = u16::from_le_bytes(bytes[4..6].try_into()?);
        let event_type = u16::from_le_bytes(bytes[6..8].try_into()?);
        let payload_len = u32::from_le_bytes(bytes[8..12].try_into()?);
        let event_ts_ms = i64::from_le_bytes(bytes[12..20].try_into()?);
        let publish_ts_ms = i64::from_le_bytes(bytes[20..28].try_into()?);
        let reserved0 = u32::from_le_bytes(bytes[28..32].try_into()?);
        let reserved1 = u32::from_le_bytes(bytes[32..36].try_into()?);
        let header = Self {
            magic,
            version,
            event_type,
            payload_len,
            event_ts_ms,
            publish_ts_ms,
            reserved0,
            reserved1,
        };
        header.validate()?;
        Ok(header)
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.event_type.to_le_bytes());
        buf[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[12..20].copy_from_slice(&self.event_ts_ms.to_le_bytes());
        buf[20..28].copy_from_slice(&self.publish_ts_ms.to_le_bytes());
        buf[28..32].copy_from_slice(&self.reserved0.to_le_bytes());
        buf[32..36].copy_from_slice(&self.reserved1.to_le_bytes());
        buf
    }

    pub fn event_type(&self) -> Option<SignalEventType> {
        SignalEventType::from_raw(self.event_type)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.magic == SIGNAL_EVENT_MAGIC,
            "信号魔数错误: {:#x}",
            self.magic
        );
        ensure!(
            self.version == SIGNAL_EVENT_VERSION,
            "信号版本不支持: {}",
            self.version
        );
        Ok(())
    }
}

pub struct SignalEvent<'a> {
    pub header: SignalEventHeader,
    pub payload: &'a [u8],
}

impl<'a> SignalEvent<'a> {
    pub fn parse(frame: &'a [u8]) -> Result<Self> {
        ensure!(
            frame.len() >= SignalEventHeader::SIZE,
            "信号帧长度不足: {}",
            frame.len()
        );
        let header = SignalEventHeader::from_bytes(frame)?;
        let total = SignalEventHeader::SIZE + header.payload_len as usize;
        ensure!(
            frame.len() >= total,
            "信号帧长度不匹配: frame={}, header={}",
            frame.len(),
            total
        );
        let payload =
            &frame[SignalEventHeader::SIZE..SignalEventHeader::SIZE + header.payload_len as usize];
        Ok(Self { header, payload })
    }
}

pub fn encode_frame(
    event_type: SignalEventType,
    event_ts_ms: i64,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let header = SignalEventHeader::new(event_type, event_ts_ms, payload.len())?;
    let total = SignalEventHeader::SIZE + payload.len();
    ensure!(
        total <= SIGNAL_EVENT_MAX_FRAME,
        "信号帧超出最大长度: {} > {}",
        total,
        SIGNAL_EVENT_MAX_FRAME
    );
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignalData {
    pub symbol: String,
    pub exchange: String,
    pub event_type: String,
    pub signal_type: String,
    pub spread_deviation: f64,
    pub spread_threshold: f64,
    pub funding_rate: f64,
    pub predicted_rate: f64,
    pub loan_rate: f64,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
    pub timestamp: i64,
}

impl TradeSignalData {
    pub fn event_ts_ms(&self) -> i64 {
        self.timestamp
    }
}

pub fn encode_trade_signal(data: &TradeSignalData) -> Result<Vec<u8>> {
    let payload = bincode::serialize(data)?;
    encode_frame(SignalEventType::TradeSignal, data.event_ts_ms(), &payload)
}

pub fn decode_trade_signal(event: &SignalEvent<'_>) -> Result<TradeSignalData> {
    ensure!(
        matches!(
            event.header.event_type(),
            Some(SignalEventType::TradeSignal)
        ),
        "事件类型不匹配: {}",
        event.header.event_type
    );
    let data: TradeSignalData = bincode::deserialize(event.payload)?;
    Ok(data)
}
