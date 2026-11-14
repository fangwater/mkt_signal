use anyhow::{bail, ensure, Result};
use serde::{de::Error, Deserialize, Deserializer};

pub const MESSAGE_TYPE: u32 = 1040;
pub const HEADER_BYTES: usize = 144;
pub const EVENT_BYTES: usize = 96;
pub const MAX_CHUNK_BYTES: usize = 65_536;
pub const DECIMAL_SCALE: u64 = 100_000_000;
pub const HAS_USER: u16 = 1;
pub const HAS_CLOID: u16 = 2;
pub const HAS_ANCHOR: u16 = 4;
pub const REDUCE_ONLY: u16 = 8;
pub const IS_TRIGGER: u16 = 16;
pub const POSITION_TPSL: u16 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Coin {
    bytes: [u8; 64],
    len: u8,
}
impl Coin {
    pub fn new(value: &str) -> Result<Self> {
        ensure!(
            !value.is_empty() && value.len() <= 64,
            "invalid coin length"
        );
        ensure!(
            value
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b'"' && b != b'\\'),
            "invalid wire coin"
        );
        let mut result = Self {
            bytes: [0; 64],
            len: value.len() as u8,
        };
        result.bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(result)
    }
    pub fn as_str(&self) -> &str {
        // Coin is private and constructed only from validated ASCII.
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap()
    }
}
impl<'de> Deserialize<'de> for Coin {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        Self::new(<&str>::deserialize(de)?).map_err(D::Error::custom)
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Snapshot = 1,
    Update = 2,
    Reset = 3,
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum Side {
    #[default]
    Unknown = 0,
    Bid = 1,
    Ask = 2,
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Op {
    #[default]
    Snapshot = 1,
    New = 2,
    Update = 3,
    Remove = 4,
}

/// Integer e8 prices/sizes; never round through f64. Array order is meaningful.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Event {
    pub oid: u64,
    pub price: u64,
    pub size: u64,
    pub original_size: u64,
    pub insert_before: u64,
    pub order_time_us: u64,
    pub user: [u8; 20],
    pub cloid: [u8; 16],
    pub flags: u16,
    pub side: Side,
    pub op: Op,
    pub status_time_us: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub epoch: [u8; 16],
    pub sequence: u64,
    pub height: u64,
    pub previous_height: u64,
    pub event_time_us: u64,
    pub receipt_time_us: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub kind: Kind,
    pub coin: Coin,
}

#[inline]
fn put64(out: &mut [u8], offset: usize, value: u64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
#[inline]
fn u64at(raw: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(raw[offset..offset + 8].try_into().unwrap())
}
#[inline]
fn u32at(raw: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap())
}

impl Event {
    #[inline]
    pub fn encode(&self, out: &mut [u8]) {
        put64(out, 0, self.oid);
        put64(out, 8, self.price);
        put64(out, 16, self.size);
        put64(out, 24, self.original_size);
        put64(out, 32, self.insert_before);
        put64(out, 40, self.order_time_us);
        out[48..68].copy_from_slice(&self.user);
        out[68..84].copy_from_slice(&self.cloid);
        out[84..86].copy_from_slice(&self.flags.to_le_bytes());
        out[86] = self.side as u8;
        out[87] = self.op as u8;
        put64(out, 88, self.status_time_us);
    }
    pub fn decode(raw: &[u8]) -> Result<Self> {
        ensure!(raw.len() == EVENT_BYTES, "invalid event length");
        let side = match raw[86] {
            0 => Side::Unknown,
            1 => Side::Bid,
            2 => Side::Ask,
            _ => bail!("invalid side"),
        };
        let op = match raw[87] {
            1 => Op::Snapshot,
            2 => Op::New,
            3 => Op::Update,
            4 => Op::Remove,
            _ => bail!("invalid operation"),
        };
        let flags = u16::from_le_bytes(raw[84..86].try_into().unwrap());
        ensure!(flags & !63 == 0, "unknown event flags");
        let event = Self {
            oid: u64at(raw, 0),
            price: u64at(raw, 8),
            size: u64at(raw, 16),
            original_size: u64at(raw, 24),
            insert_before: u64at(raw, 32),
            order_time_us: u64at(raw, 40),
            user: raw[48..68].try_into().unwrap(),
            cloid: raw[68..84].try_into().unwrap(),
            flags,
            side,
            op,
            status_time_us: u64at(raw, 88),
        };
        ensure!(
            event.oid > 0 && event.price > 0,
            "invalid order identity/price"
        );
        ensure!(
            !matches!(op, Op::Snapshot | Op::New) || (side != Side::Unknown && event.size > 0),
            "invalid resting order"
        );
        ensure!(
            flags & HAS_ANCHOR == 0
                || (op == Op::New && event.insert_before > 0 && event.insert_before != event.oid),
            "invalid priority anchor"
        );
        Ok(event)
    }
}
impl Header {
    /// Initialize exactly the emitted prefix of a transport-owned loan. No
    /// reference to uninitialized u8 memory is ever constructed.
    pub fn encode_uninit(
        &self,
        events: &[Event],
        out: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<usize> {
        let len = HEADER_BYTES + events.len() * EVENT_BYTES;
        ensure!(
            out.len() >= len && len <= MAX_CHUNK_BYTES,
            "chunk exceeds capacity"
        );
        let mut prefix = [0; HEADER_BYTES];
        self.encode(&[], &mut prefix)?;
        prefix[4..8].copy_from_slice(&(len as u32).to_le_bytes());
        prefix[72..76].copy_from_slice(&(events.len() as u32).to_le_bytes());
        fn initialize(dst: &mut [std::mem::MaybeUninit<u8>], src: &[u8]) {
            for (dst, &value) in dst.iter_mut().zip(src) {
                dst.write(value);
            }
        }
        initialize(&mut out[..HEADER_BYTES], &prefix);
        let mut record = [0; EVENT_BYTES];
        for (event, dst) in events
            .iter()
            .zip(out[HEADER_BYTES..len].as_chunks_mut::<EVENT_BYTES>().0)
        {
            event.encode(&mut record);
            initialize(dst, &record);
        }
        Ok(len)
    }
    pub fn encode(&self, events: &[Event], out: &mut [u8]) -> Result<usize> {
        let len = HEADER_BYTES + events.len() * EVENT_BYTES;
        ensure!(
            out.len() >= len && len <= MAX_CHUNK_BYTES,
            "chunk exceeds capacity"
        );
        ensure!(
            self.chunk_count > 0 && self.chunk_index < self.chunk_count,
            "invalid chunk boundary"
        );
        let out = &mut out[..len];
        out[..4].copy_from_slice(&MESSAGE_TYPE.to_le_bytes());
        out[4..8].copy_from_slice(&(len as u32).to_le_bytes());
        out[8..24].copy_from_slice(&self.epoch);
        put64(out, 24, self.sequence);
        put64(out, 32, self.height);
        put64(out, 40, self.previous_height);
        put64(out, 48, self.event_time_us);
        put64(out, 56, self.receipt_time_us);
        out[64..68].copy_from_slice(&self.chunk_index.to_le_bytes());
        out[68..72].copy_from_slice(&self.chunk_count.to_le_bytes());
        out[72..76].copy_from_slice(&(events.len() as u32).to_le_bytes());
        out[76] = self.kind as u8;
        out[77] = 0;
        out[78..80].copy_from_slice(&(self.coin.len as u16).to_le_bytes());
        out[80..144].copy_from_slice(&self.coin.bytes);
        for (event, dst) in events
            .iter()
            .zip(out[HEADER_BYTES..].as_chunks_mut::<EVENT_BYTES>().0)
        {
            event.encode(dst);
        }
        Ok(len)
    }
    pub fn decode(raw: &[u8]) -> Result<(Self, &[u8])> {
        ensure!(
            raw.len() >= HEADER_BYTES && raw.len() <= MAX_CHUNK_BYTES,
            "invalid chunk length"
        );
        ensure!(
            u32at(raw, 0) == MESSAGE_TYPE && u32at(raw, 4) as usize == raw.len(),
            "invalid L4 message header"
        );
        let count = u32at(raw, 72) as usize;
        ensure!(
            raw.len() == HEADER_BYTES + count * EVENT_BYTES,
            "invalid record count"
        );
        let kind = match raw[76] {
            1 => Kind::Snapshot,
            2 => Kind::Update,
            3 => Kind::Reset,
            _ => bail!("invalid batch kind"),
        };
        ensure!(raw[77] == 0, "unknown header flags");
        let len = u16::from_le_bytes(raw[78..80].try_into().unwrap()) as usize;
        ensure!(
            len > 0 && len <= 64 && raw[80 + len..144].iter().all(|v| *v == 0),
            "invalid coin encoding"
        );
        let coin = Coin::new(std::str::from_utf8(&raw[80..80 + len])?)?;
        let header = Self {
            epoch: raw[8..24].try_into().unwrap(),
            sequence: u64at(raw, 24),
            height: u64at(raw, 32),
            previous_height: u64at(raw, 40),
            event_time_us: u64at(raw, 48),
            receipt_time_us: u64at(raw, 56),
            chunk_index: u32at(raw, 64),
            chunk_count: u32at(raw, 68),
            kind,
            coin,
        };
        ensure!(
            header.chunk_count > 0 && header.chunk_index < header.chunk_count,
            "invalid chunk boundary"
        );
        ensure!(
            kind != Kind::Reset
                || (count == 0 && header.chunk_count == 1 && header.chunk_index == 0),
            "invalid reset"
        );
        Ok((header, &raw[HEADER_BYTES..]))
    }
}

/// Node order-book values use e8. Reject unsupported precision/overflow instead of rounding.
pub fn decimal(value: &str) -> Result<u64> {
    ensure!(!value.is_empty(), "empty decimal");
    let mut whole = 0_u64;
    let mut fraction = 0_u64;
    let mut digits = 0_u32;
    let mut dot = false;
    let mut saw_digit = false;
    for byte in value.bytes() {
        if byte == b'.' && !dot && saw_digit {
            dot = true;
            continue;
        }
        ensure!(byte.is_ascii_digit(), "invalid decimal");
        saw_digit = true;
        let digit = (byte - b'0') as u64;
        if dot {
            digits += 1;
            if digits <= 8 {
                fraction = fraction * 10 + digit;
            } else {
                ensure!(digit == 0, "decimal exceeds e8 precision");
            }
        } else {
            whole = whole
                .checked_mul(10)
                .and_then(|n| n.checked_add(digit))
                .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
        }
    }
    ensure!(saw_digit && (!dot || digits > 0), "invalid decimal");
    let fraction = fraction * 10_u64.pow(8 - digits.min(8));
    whole
        .checked_mul(DECIMAL_SCALE)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| anyhow::anyhow!("decimal overflow"))
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Fixed(pub u64);
impl<'de> Deserialize<'de> for Fixed {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        decimal(<&str>::deserialize(de)?)
            .map(Self)
            .map_err(D::Error::custom)
    }
}
pub(crate) fn hex<const N: usize>(s: &str) -> Result<[u8; N]> {
    ensure!(
        s.len() == 2 + N * 2 && s.starts_with("0x"),
        "invalid hexadecimal identity"
    );
    fn nibble(b: u8) -> Result<u8> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => bail!("invalid hex digit"),
        }
    }
    let mut out = [0; N];
    for (dst, pair) in out.iter_mut().zip(s.as_bytes()[2..].as_chunks::<2>().0) {
        *dst = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    Ok(out)
}
