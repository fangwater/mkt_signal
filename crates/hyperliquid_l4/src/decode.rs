use crate::wire::{
    hex, Coin, Event, Fixed, Kind, Op, Side, HAS_ANCHOR, HAS_CLOID, HAS_USER, IS_TRIGGER,
    POSITION_TPSL, REDUCE_ONLY,
};
use ahash::AHashMap;
use anyhow::{bail, ensure, Context, Result};
use serde::de::{DeserializeSeed, Error, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

/// The only borrowed output is a slice into Decoder's reusable event arena.
pub struct Batch<'a> {
    pub coin: Coin,
    pub kind: Kind,
    pub height: u64,
    pub event_time_us: u64,
    pub events: &'a [Event],
}
pub enum Decoded<'a> {
    Batch(Batch<'a>),
    Ack(Coin),
    Pong,
}

enum Meta {
    Snapshot { coin: Coin, time: u64, height: u64 },
    Updates { time: u64, height: u64 },
    Ack(Coin),
    Pong,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawOrder<'a> {
    #[serde(borrow)]
    user: Option<&'a str>,
    coin: Coin,
    #[serde(borrow)]
    side: &'a str,
    #[serde(borrow)]
    limit_px: &'a str,
    #[serde(borrow)]
    sz: &'a str,
    oid: u64,
    timestamp: u64,
    is_trigger: bool,
    is_position_tpsl: bool,
    reduce_only: bool,
    #[serde(borrow)]
    tif: Option<&'a str>,
    #[serde(borrow)]
    cloid: Option<&'a str>,
}
#[derive(Clone, Copy)]
struct Order {
    coin: Coin,
    event: Event,
}
impl<'de> Deserialize<'de> for Order {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        let raw = RawOrder::deserialize(de)?;
        Self::from_raw(raw).map_err(D::Error::custom)
    }
}
impl Order {
    fn from_raw(raw: RawOrder<'_>) -> Result<Self> {
        let price = crate::wire::decimal(raw.limit_px)?;
        let size = crate::wire::decimal(raw.sz)?;
        ensure!(raw.oid > 0 && price > 0, "invalid order id/price");
        let side = match raw.side {
            "B" => Side::Bid,
            "A" => Side::Ask,
            _ => bail!("invalid order side"),
        };
        let mut flags = 0;
        let user = if let Some(s) = raw.user {
            flags |= HAS_USER;
            hex(s)?
        } else {
            [0; 20]
        };
        let cloid = if let Some(s) = raw.cloid {
            flags |= HAS_CLOID;
            hex(s)?
        } else {
            [0; 16]
        };
        if raw.is_trigger {
            flags |= IS_TRIGGER;
        }
        if raw.is_position_tpsl {
            flags |= POSITION_TPSL;
        }
        if raw.reduce_only {
            flags |= REDUCE_ONLY;
        }
        Ok(Self {
            coin: raw.coin,
            event: Event {
                oid: raw.oid,
                price,
                size,
                order_time_us: ms_to_us(raw.timestamp)?,
                side,
                flags,
                user,
                cloid,
                ..Event::default()
            },
        })
    }
}
#[derive(Clone, Copy)]
struct Status {
    coin: Coin,
    opening: Option<Event>,
}
impl<'de> Deserialize<'de> for Status {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw<'a> {
            #[serde(borrow)]
            time: &'a str,
            #[serde(borrow)]
            user: &'a str,
            #[serde(borrow)]
            status: &'a str,
            #[serde(borrow)]
            order: RawOrder<'a>,
        }
        let raw = Raw::deserialize(de)?;
        let parsed = (|| -> Result<Self> {
            let coin = raw.order.coin;
            let trigger = raw.order.is_trigger;
            let opens = (raw.status == "open" && !trigger && raw.order.tif != Some("Ioc"))
                || (trigger && raw.status == "triggered");
            // Match the node server: only opening/triggered statuses provide
            // book metadata. Rejections/terminal rows can contain zero price,
            // zero remaining size or no assigned oid; they are not book orders.
            if !opens {
                return Ok(Self {
                    coin,
                    opening: None,
                });
            }
            let mut order = Order::from_raw(raw.order)?;
            let user = hex(raw.user)?;
            ensure!(
                order.event.flags & HAS_USER == 0 || order.event.user == user,
                "conflicting order user"
            );
            order.event.user = user;
            order.event.flags |= HAS_USER;
            let time = chrono::NaiveDateTime::parse_from_str(raw.time, "%Y-%m-%dT%H:%M:%S%.f")?
                .and_utc()
                .timestamp_micros();
            ensure!(time > 0, "invalid order status time");
            order.event.status_time_us = time as u64;
            if trigger && raw.status == "triggered" {
                order.event.flags &= !IS_TRIGGER;
                order.event.order_time_us = time as u64;
            }
            Ok(Self {
                coin,
                opening: Some(order.event),
            })
        })();
        parsed.map_err(D::Error::custom)
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum RawDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: Fixed,
        #[serde(default)]
        insert_before: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: Fixed,
        new_sz: Fixed,
    },
    Remove,
}
#[derive(Clone, Copy)]
struct Diff {
    coin: Coin,
    event: Event,
}
impl<'de> Deserialize<'de> for Diff {
    fn deserialize<D: Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw<'a> {
            #[serde(borrow)]
            user: &'a str,
            oid: u64,
            px: Fixed,
            coin: Coin,
            raw_book_diff: RawDiff,
        }
        let raw = Raw::deserialize(de)?;
        let parsed = (|| -> Result<Self> {
            ensure!(raw.oid > 0 && raw.px.0 > 0, "invalid order diff id/price");
            let mut event = Event {
                oid: raw.oid,
                price: raw.px.0,
                user: hex(raw.user)?,
                flags: HAS_USER,
                ..Event::default()
            };
            match raw.raw_book_diff {
                RawDiff::New { sz, insert_before } => {
                    ensure!(sz.0 > 0, "empty new order");
                    event.op = Op::New;
                    event.size = sz.0;
                    if let Some(anchor) = insert_before {
                        ensure!(anchor > 0 && anchor != raw.oid, "invalid insertBefore");
                        event.flags |= HAS_ANCHOR;
                        event.insert_before = anchor;
                    }
                }
                RawDiff::Update { orig_sz, new_sz } => {
                    event.op = Op::Update;
                    event.original_size = orig_sz.0;
                    event.size = new_sz.0;
                }
                RawDiff::Remove => {
                    event.op = Op::Remove;
                }
            }
            Ok(Self {
                coin: raw.coin,
                event,
            })
        })();
        parsed.map_err(D::Error::custom)
    }
}

/// A bounded, capacity-reusing sequence visitor. No per-record heap objects.
struct Reuse<'a, T> {
    out: &'a mut Vec<T>,
    limit: usize,
}
impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for Reuse<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<(), D::Error> {
        de.deserialize_seq(self)
    }
}
impl<'de, T: Deserialize<'de>> Visitor<'de> for Reuse<'_, T> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a bounded L4 record array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while let Some(item) = seq.next_element()? {
            if self.out.len() >= self.limit {
                return Err(A::Error::custom("L4 record limit exceeded"));
            }
            self.out.push(item);
        }
        Ok(())
    }
}
struct SnapshotSide<'a> {
    events: &'a mut Vec<Event>,
    seen: &'a mut AHashMap<u64, usize>,
    coin: &'a mut Option<Coin>,
    side: Side,
    limit: usize,
}
impl<'de> DeserializeSeed<'de> for SnapshotSide<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<(), D::Error> {
        de.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for SnapshotSide<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a snapshot side")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while let Some(order) = seq.next_element::<Order>()? {
            if self.events.len() >= self.limit {
                return Err(A::Error::custom("L4 record limit exceeded"));
            }
            if order.event.size == 0 {
                return Err(A::Error::custom("empty resting snapshot order"));
            }
            if self.coin.is_some_and(|coin| order.coin != coin) || order.event.side != self.side {
                return Err(A::Error::custom("snapshot coin/side mismatch"));
            }
            *self.coin = Some(order.coin);
            if self
                .seen
                .insert(order.event.oid, self.events.len())
                .is_some()
            {
                return Err(A::Error::custom("duplicate snapshot oid"));
            }
            self.events.push(order.event);
        }
        Ok(())
    }
}
fn ms_to_us(ms: u64) -> Result<u64> {
    ensure!(ms > 0, "invalid venue timestamp");
    ms.checked_mul(1_000).context("timestamp overflow")
}

// Single-pass seed visitors: deserialize directly into reusable typed arenas.
// No JSON DOM, raw-value copies, repeated scans, or dependency on field order.
struct EnvelopeSeed<'a>(&'a mut Decoder);
impl<'de> DeserializeSeed<'de> for EnvelopeSeed<'_> {
    type Value = Meta;
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<Meta, D::Error> {
        de.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for EnvelopeSeed<'_> {
    type Value = Meta;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an L4 WebSocket envelope")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Meta, A::Error> {
        let mut channel = None;
        let mut data = None;
        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "channel" => {
                    if channel.is_some() {
                        return Err(A::Error::duplicate_field("channel"));
                    }
                    channel = Some(map.next_value::<&str>()?);
                }
                "data" => {
                    if data.is_some() {
                        return Err(A::Error::duplicate_field("data"));
                    }
                    data = Some(map.next_value_seed(DataSeed(self.0))?);
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let channel = channel.ok_or_else(|| A::Error::missing_field("channel"))?;
        let data = data.unwrap_or(Meta::Pong);
        match (channel, &data) {
            ("l4Book", Meta::Snapshot { .. } | Meta::Updates { .. })
            | ("subscriptionResponse", Meta::Ack(_))
            | ("pong", Meta::Pong) => Ok(data),
            _ => Err(A::Error::custom(
                "unexpected L4 channel/payload combination",
            )),
        }
    }
}
struct DataSeed<'a>(&'a mut Decoder);
impl<'de> DeserializeSeed<'de> for DataSeed<'_> {
    type Value = Meta;
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<Meta, D::Error> {
        de.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for DataSeed<'_> {
    type Value = Meta;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an L4 snapshot, update, or ACK")
    }
    fn visit_str<E: Error>(self, value: &str) -> std::result::Result<Meta, E> {
        Err(E::custom(format!("node L4 error: {value}")))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Meta, A::Error> {
        let Some(first) = map.next_key::<&str>()? else {
            return Ok(Meta::Pong);
        };
        if first == "Snapshot" || first == "Updates" {
            let meta = if first == "Snapshot" {
                map.next_value_seed(SnapshotSeed(self.0))?
            } else {
                map.next_value_seed(UpdateSeed(self.0))?
            };
            if map.next_key::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom("multiple L4 payload variants"));
            }
            return Ok(meta);
        }
        #[derive(Deserialize)]
        struct Sub<'a> {
            #[serde(borrow, rename = "type")]
            kind: &'a str,
            coin: Coin,
        }
        let mut method = None;
        let mut coin = None;
        let mut key = Some(first);
        while let Some(field) = key {
            match field {
                "method" => {
                    if method.is_some() {
                        return Err(A::Error::duplicate_field("method"));
                    }
                    method = Some(map.next_value::<&str>()?);
                }
                "subscription" => {
                    if coin.is_some() {
                        return Err(A::Error::duplicate_field("subscription"));
                    }
                    let sub: Sub<'_> = map.next_value()?;
                    if sub.kind != "l4Book" {
                        return Err(A::Error::custom("unexpected ACK subscription type"));
                    }
                    coin = Some(sub.coin);
                }
                _ => return Err(A::Error::custom("unknown L4 data payload")),
            }
            key = map.next_key()?;
        }
        if method != Some("subscribe") {
            return Err(A::Error::custom("unexpected L4 ACK method"));
        }
        Ok(Meta::Ack(
            coin.ok_or_else(|| A::Error::missing_field("subscription"))?,
        ))
    }
}
struct LevelsSeed<'a> {
    decoder: &'a mut Decoder,
    coin: &'a mut Option<Coin>,
}
impl<'de> DeserializeSeed<'de> for LevelsSeed<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<(), D::Error> {
        de.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for LevelsSeed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("exactly two snapshot sides")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        for side in [Side::Bid, Side::Ask] {
            let present = seq.next_element_seed(SnapshotSide {
                events: &mut self.decoder.events,
                seen: &mut self.decoder.opening,
                coin: self.coin,
                side,
                limit: self.decoder.limit,
            })?;
            if present.is_none() {
                return Err(A::Error::custom("missing snapshot side"));
            }
        }
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("extra snapshot side"));
        }
        Ok(())
    }
}
struct SnapshotSeed<'a>(&'a mut Decoder);
impl<'de> DeserializeSeed<'de> for SnapshotSeed<'_> {
    type Value = Meta;
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<Meta, D::Error> {
        de.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for SnapshotSeed<'_> {
    type Value = Meta;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an L4 snapshot")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Meta, A::Error> {
        let (mut coin, mut inferred, mut time, mut height) = (None, None, None, None);
        let mut levels = false;
        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "coin" => {
                    if coin.is_some() {
                        return Err(A::Error::duplicate_field("coin"));
                    }
                    coin = Some(map.next_value::<Coin>()?);
                }
                "time" => {
                    if time.is_some() {
                        return Err(A::Error::duplicate_field("time"));
                    }
                    time = Some(map.next_value::<u64>()?);
                }
                "height" => {
                    if height.is_some() {
                        return Err(A::Error::duplicate_field("height"));
                    }
                    height = Some(map.next_value::<u64>()?);
                }
                "levels" => {
                    if levels {
                        return Err(A::Error::duplicate_field("levels"));
                    }
                    levels = true;
                    map.next_value_seed(LevelsSeed {
                        decoder: self.0,
                        coin: &mut inferred,
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let coin = coin.ok_or_else(|| A::Error::missing_field("coin"))?;
        if !levels {
            return Err(A::Error::missing_field("levels"));
        }
        if inferred.is_some_and(|value| value != coin) {
            return Err(A::Error::custom("snapshot header coin mismatch"));
        }
        Ok(Meta::Snapshot {
            coin,
            time: time.ok_or_else(|| A::Error::missing_field("time"))?,
            height: height.ok_or_else(|| A::Error::missing_field("height"))?,
        })
    }
}
struct UpdateSeed<'a>(&'a mut Decoder);
impl<'de> DeserializeSeed<'de> for UpdateSeed<'_> {
    type Value = Meta;
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<Meta, D::Error> {
        de.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for UpdateSeed<'_> {
    type Value = Meta;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an L4 block update")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Meta, A::Error> {
        let (mut time, mut height) = (None, None);
        let (mut statuses, mut diffs) = (false, false);
        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "time" => {
                    if time.is_some() {
                        return Err(A::Error::duplicate_field("time"));
                    }
                    time = Some(map.next_value::<u64>()?);
                }
                "height" => {
                    if height.is_some() {
                        return Err(A::Error::duplicate_field("height"));
                    }
                    height = Some(map.next_value::<u64>()?);
                }
                "order_statuses" => {
                    if statuses {
                        return Err(A::Error::duplicate_field("order_statuses"));
                    }
                    statuses = true;
                    map.next_value_seed(Reuse {
                        out: &mut self.0.statuses,
                        limit: self.0.limit,
                    })?;
                }
                "book_diffs" => {
                    if diffs {
                        return Err(A::Error::duplicate_field("book_diffs"));
                    }
                    diffs = true;
                    map.next_value_seed(Reuse {
                        out: &mut self.0.diffs,
                        limit: self.0.limit,
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !statuses || !diffs {
            return Err(A::Error::custom("incomplete L4 update arrays"));
        }
        Ok(Meta::Updates {
            time: time.ok_or_else(|| A::Error::missing_field("time"))?,
            height: height.ok_or_else(|| A::Error::missing_field("height"))?,
        })
    }
}

/// Reusable SIMD JSON decoder, bounded before publication. A failed parse emits nothing.
pub struct Decoder {
    statuses: Vec<Status>,
    diffs: Vec<Diff>,
    events: Vec<Event>,
    opening: AHashMap<u64, usize>,
    limit: usize,
}
impl Decoder {
    pub fn new(reserve: usize, limit: usize) -> Result<Self> {
        ensure!(limit > 0 && reserve <= limit, "invalid L4 decoder capacity");
        Ok(Self {
            statuses: Vec::with_capacity(reserve),
            diffs: Vec::with_capacity(reserve),
            events: Vec::with_capacity(reserve),
            opening: AHashMap::with_capacity(reserve),
            limit,
        })
    }
    pub fn decode<'a>(&'a mut self, raw: &[u8]) -> Result<Decoded<'a>> {
        self.statuses.clear();
        self.diffs.clear();
        self.events.clear();
        self.opening.clear();
        let mut deserializer = sonic_rs::Deserializer::from_slice(raw);
        let meta = EnvelopeSeed(self)
            .deserialize(&mut deserializer)
            .context("L4 frame")?;
        deserializer.end()?;
        let (coin, kind, height, event_time_us) = match meta {
            Meta::Ack(coin) => return Ok(Decoded::Ack(coin)),
            Meta::Pong => return Ok(Decoded::Pong),
            Meta::Snapshot { coin, time, height } => {
                (coin, Kind::Snapshot, height, ms_to_us(time)?)
            }
            Meta::Updates { time, height } => {
                // Upstream Updates has NO top-level coin. It emits one nonempty coin group.
                let coin = self
                    .diffs
                    .first()
                    .map(|v| v.coin)
                    .or_else(|| self.statuses.first().map(|v| v.coin))
                    .context("empty L4 Updates has no coin identity")?;
                for (index, status) in self.statuses.iter().enumerate() {
                    ensure!(status.coin == coin, "mixed status coins");
                    if let Some(order) = status.opening {
                        ensure!(
                            self.opening.insert(order.oid, index).is_none(),
                            "conflicting opening statuses"
                        );
                    }
                }
                for diff in &self.diffs {
                    ensure!(diff.coin == coin, "mixed diff coins");
                    let mut event = diff.event;
                    if event.op == Op::New {
                        let index = self
                            .opening
                            .remove(&event.oid)
                            .context("new order missing opening status")?;
                        let order = self.statuses[index]
                            .opening
                            .context("opening status metadata missing")?;
                        ensure!(
                            order.price == event.price && order.user == event.user,
                            "new order/status mismatch"
                        );
                        let mut order = order;
                        order.op = Op::New;
                        order.size = event.size;
                        order.insert_before = event.insert_before;
                        order.flags |= event.flags & HAS_ANCHOR;
                        event = order;
                    }
                    self.events.push(event);
                }
                (coin, Kind::Update, height, ms_to_us(time)?)
            }
        };
        ensure!(height > 0, "invalid block height");
        Ok(Decoded::Batch(Batch {
            coin,
            kind,
            height,
            event_time_us,
            events: &self.events,
        }))
    }
}
