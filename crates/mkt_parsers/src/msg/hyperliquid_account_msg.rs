use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};

use super::basic_account_msg::{
    trade_id_as_str, trade_id_bytes_from_str, BasicAccountEventType, TRADE_ID_LEN,
};

pub const HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN: usize = 32;
pub const HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN: usize = 128;
pub const HYPERLIQUID_FACT_REPLAY_REQUEST_MAGIC: u32 = 0x4852_4651;
pub const HYPERLIQUID_FACT_REPLAY_REQUEST_SERVICE: &str =
    "account_pubs/hyperliquid_fact_replay_requests";

/// Stable SHA-256 identity for a Hyperliquid network and normalized account address.
///
/// The input is normalized here as well so the producer and consumer cannot
/// accidentally hash different case or whitespace representations.
pub fn hyperliquid_account_identity_hash(
    address: &str,
    testnet: bool,
) -> Result<[u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]> {
    let normalized = address.trim().to_ascii_lowercase();
    let hex = normalized
        .strip_prefix("0x")
        .ok_or_else(|| anyhow::anyhow!("Hyperliquid account address must start with 0x"))?;
    if hex.len() != 40 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("Hyperliquid account address must contain exactly 40 hex digits");
    }
    let mut raw = [0_u8; 20];
    for (index, output) in raw.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&hex[offset..offset + 2], 16)?;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"mkt_signal/hyperliquid/account_identity");
    hasher.update([u8::from(testnet)]);
    hasher.update(raw);
    Ok(hasher.finalize().into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidFactIdentity {
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    /// Unique producer process epoch.
    pub monitor_id: u64,
    /// Strictly monotonic within `monitor_id`, starting at one.
    pub fact_seq: u64,
}

/// Hyperliquid order lifecycle update.
///
/// Fills are published separately as `HyperliquidBasicFillMsg`; this message only
/// carries the order state from the `orderUpdates` subscription.
#[derive(Debug, Clone, PartialEq)]
pub struct HyperliquidBasicOrderMsg {
    pub msg_type: BasicAccountEventType,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub fact_seq: u64,
    /// `TradingVenue` discriminant: 12=spot, 13=perpetual.
    pub venue: u8,
    /// Exchange status timestamp in milliseconds.
    pub event_time: i64,
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: i64,
    pub cloid: String,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub execution_type: u8,
    pub order_status: u8,
    pub price: f64,
    pub quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub raw_status: String,
}

impl HyperliquidBasicOrderMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        venue: u8,
        event_time: i64,
        symbol: String,
        order_id: i64,
        client_order_id: i64,
        cloid: String,
        side: u8,
        order_type: u8,
        time_in_force: u8,
        execution_type: u8,
        order_status: u8,
        price: f64,
        quantity: f64,
        cumulative_filled_quantity: f64,
        raw_status: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            fact_seq: 0,
            venue,
            event_time,
            symbol,
            order_id,
            client_order_id,
            cloid,
            side,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            price,
            quantity,
            cumulative_filled_quantity,
            raw_status,
        }
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.account_hash = identity.account_hash;
        self.monitor_id = identity.monitor_id;
        self.fact_seq = identity.fact_seq;
        self
    }

    pub fn fact_identity(&self) -> HyperliquidFactIdentity {
        HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq: self.fact_seq,
        }
    }

    /// Stable venue lifecycle identity, independent from the monitor epoch.
    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/order_lifecycle");
        hasher.update(self.account_hash);
        hasher.update([self.venue]);
        hasher.update(self.order_id.to_be_bytes());
        hasher.update(self.event_time.to_be_bytes());
        update_digest_field(&mut hasher, self.symbol.as_bytes());
        update_digest_field(&mut hasher, self.raw_status.as_bytes());
        finish_digest(hasher)
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 1
            + 8
            + encoded_string_len(&self.symbol)
            + 8
            + 8
            + encoded_string_len(&self.cloid)
            + 5
            + 8 * 3
            + encoded_string_len(&self.raw_status);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.fact_seq);
        buf.put_u8(self.venue);
        buf.put_i64_le(self.event_time);
        put_string(&mut buf, &self.symbol);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.client_order_id);
        put_string(&mut buf, &self.cloid);
        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        buf.put_u8(self.time_in_force);
        buf.put_u8(self.execution_type);
        buf.put_u8(self.order_status);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        put_string(&mut buf, &self.raw_status);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 1
            + 8
            + 4
            + 8
            + 8
            + 4
            + 5
            + 8 * 3
            + 4;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidBasicOrderMsg too short: {}", data.len());
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::OrderUpdate as u32 {
            anyhow::bail!("invalid HyperliquidBasicOrderMsg type: {msg_type}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        let monitor_id = cursor.get_u64_le();
        let fact_seq = cursor.get_u64_le();
        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();
        let symbol = get_string(&mut cursor, "symbol")?;
        if cursor.remaining() < 8 + 8 + 4 {
            anyhow::bail!("HyperliquidBasicOrderMsg truncated before identifiers");
        }
        let order_id = cursor.get_i64_le();
        let client_order_id = cursor.get_i64_le();
        let cloid = get_string(&mut cursor, "cloid")?;
        if cursor.remaining() < 5 + 8 * 3 + 4 {
            anyhow::bail!("HyperliquidBasicOrderMsg truncated before order fields");
        }
        let side = cursor.get_u8();
        let order_type = cursor.get_u8();
        let time_in_force = cursor.get_u8();
        let execution_type = cursor.get_u8();
        let order_status = cursor.get_u8();
        let price = cursor.get_f64_le();
        let quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();
        let raw_status = get_string(&mut cursor, "raw_status")?;

        Ok(Self {
            msg_type: BasicAccountEventType::OrderUpdate,
            account_hash,
            monitor_id,
            fact_seq,
            venue,
            event_time,
            symbol,
            order_id,
            client_order_id,
            cloid,
            side,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            price,
            quantity,
            cumulative_filled_quantity,
            raw_status,
        })
    }
}

fn encoded_string_len(value: &str) -> usize {
    4 + value.len()
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32_le(value.len() as u32);
    buf.put(value.as_bytes());
}

fn get_string(cursor: &mut Bytes, field: &str) -> Result<String> {
    if cursor.remaining() < 4 {
        anyhow::bail!("Hyperliquid message truncated before {field}");
    }
    let len = cursor.get_u32_le() as usize;
    if cursor.remaining() < len {
        anyhow::bail!("Hyperliquid message truncated reading {field}");
    }
    Ok(String::from_utf8(cursor.copy_to_bytes(len).to_vec())?)
}

fn encoded_optional_string_len(value: &Option<String>) -> usize {
    1 + value.as_deref().map(encoded_string_len).unwrap_or(0)
}

fn put_optional_string(buf: &mut BytesMut, value: &Option<String>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            put_string(buf, value);
        }
        None => buf.put_u8(0),
    }
}

fn get_optional_string(cursor: &mut Bytes, field: &str) -> Result<Option<String>> {
    if !cursor.has_remaining() {
        anyhow::bail!("Hyperliquid message truncated before optional {field}");
    }
    match cursor.get_u8() {
        0 => Ok(None),
        1 => Ok(Some(get_string(cursor, field)?)),
        flag => anyhow::bail!("Hyperliquid message has invalid optional {field} flag: {flag}"),
    }
}

fn put_optional_i64(buf: &mut BytesMut, value: Option<i64>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            buf.put_i64_le(value);
        }
        None => buf.put_u8(0),
    }
}

fn get_optional_i64(cursor: &mut Bytes, field: &str) -> Result<Option<i64>> {
    if !cursor.has_remaining() {
        anyhow::bail!("Hyperliquid message truncated before optional {field}");
    }
    match cursor.get_u8() {
        0 => Ok(None),
        1 if cursor.remaining() >= 8 => Ok(Some(cursor.get_i64_le())),
        1 => anyhow::bail!("Hyperliquid message truncated reading optional {field}"),
        flag => anyhow::bail!("Hyperliquid message has invalid optional {field} flag: {flag}"),
    }
}

fn put_optional_bool(buf: &mut BytesMut, value: Option<bool>) {
    match value {
        Some(false) => buf.put_u8(1),
        Some(true) => buf.put_u8(2),
        None => buf.put_u8(0),
    }
}

fn get_optional_bool(cursor: &mut Bytes, field: &str) -> Result<Option<bool>> {
    if !cursor.has_remaining() {
        anyhow::bail!("Hyperliquid message truncated before optional {field}");
    }
    match cursor.get_u8() {
        0 => Ok(None),
        1 => Ok(Some(false)),
        2 => Ok(Some(true)),
        flag => anyhow::bail!("Hyperliquid message has invalid optional {field} flag: {flag}"),
    }
}

fn get_bool(cursor: &mut Bytes, field: &str) -> Result<bool> {
    if !cursor.has_remaining() {
        anyhow::bail!("Hyperliquid message truncated before {field}");
    }
    match cursor.get_u8() {
        0 => Ok(false),
        1 => Ok(true),
        flag => anyhow::bail!("Hyperliquid message has invalid {field} flag: {flag}"),
    }
}

fn update_digest_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn put_digest_optional_bool(hasher: &mut Sha256, value: Option<bool>) {
    hasher.update([match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }]);
}

fn finish_digest(hasher: Sha256) -> [u8; 32] {
    hasher.finalize().into()
}

/// A factual Hyperliquid fill. Unlike `BasicTradeLiteMsg`, this preserves the
/// venue oid and raw transaction hash so a manual/external fill can be audited.
#[derive(Debug, Clone, PartialEq)]
pub struct HyperliquidBasicFillMsg {
    pub msg_type: BasicAccountEventType,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub fact_seq: u64,
    pub venue: u8,
    /// Exchange timestamps in milliseconds.
    pub event_time: i64,
    pub trade_time: i64,
    pub symbol: String,
    pub order_id: i64,
    /// Zero when no internal cloid can be attributed.
    pub client_order_id: i64,
    pub cloid: String,
    pub trade_id: [u8; TRADE_ID_LEN],
    pub venue_trade_id: i64,
    pub transaction_hash: String,
    /// Empty for normal fills; otherwise the documented liquidation method.
    pub liquidation_method: String,
    pub side: u8,
    pub is_maker: u8,
    pub price: f64,
    pub last_filled_quantity: f64,
    pub cumulative_filled_quantity: f64,
    /// Existing `OrderStatus` code; zero means the original order size is unknown.
    pub order_status: u8,
    /// The exact venue `coin`; `None` is reserved for synthetic/test messages.
    pub wire_coin: Option<String>,
    /// Venue-native decimal strings. `None` means the source payload omitted the field.
    pub start_position: Option<String>,
    pub dir: Option<String>,
    pub closed_pnl: Option<String>,
    pub fee: Option<String>,
    pub fee_token: Option<String>,
    pub builder_fee: Option<String>,
    /// Present when the fill belongs to a venue TWAP execution.
    pub twap_id: Option<i64>,
    pub liquidated_user: Option<String>,
    pub liquidation_mark_price: Option<String>,
}

/// A factual Hyperliquid funding payment. Decimal strings remain byte-for-byte
/// equivalent to the venue fields so persistence can retain exact accounting evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidFundingMsg {
    pub msg_type: BasicAccountEventType,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub fact_seq: u64,
    /// Exchange timestamp in milliseconds.
    pub event_time: i64,
    pub coin: String,
    pub usdc: String,
    pub szi: String,
    pub funding_rate: String,
    /// Available on the HTTP `userFunding` row; websocket rows may omit it.
    pub transaction_hash: Option<String>,
}

/// A factual Hyperliquid non-funding ledger update. `delta_json` is the complete
/// venue delta object, including fields unknown to this build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidLedgerMsg {
    pub msg_type: BasicAccountEventType,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub fact_seq: u64,
    /// Exchange timestamp in milliseconds.
    pub event_time: i64,
    pub transaction_hash: String,
    pub delta_type: String,
    pub delta_json: String,
}

/// Association between one factual venue fill and its parent Hyperliquid TWAP.
/// The fill itself remains on `HyperliquidFill`; this record prevents the
/// `userTwapSliceFills` mirror from creating a second strategy execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidTwapSliceFillMsg {
    pub msg_type: BasicAccountEventType,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub fact_seq: u64,
    pub venue: u8,
    /// Venue fill timestamp in milliseconds.
    pub event_time: i64,
    pub wire_coin: String,
    pub symbol: String,
    pub order_id: i64,
    pub venue_trade_id: i64,
    pub transaction_hash: String,
    pub twap_id: i64,
}

/// One factual row from `userTwapHistory` / Info `twapHistory`.
/// Decimal strings are retained byte-for-byte from the venue payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidTwapHistoryMsg {
    pub msg_type: BasicAccountEventType,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub fact_seq: u64,
    /// Venue history row time in seconds (kept exactly as returned).
    pub event_time: i64,
    pub twap_id: Option<i64>,
    pub user: String,
    pub coin: String,
    pub side: String,
    pub size: String,
    pub executed_size: String,
    pub executed_notional: String,
    pub minutes: i64,
    pub reduce_only: bool,
    pub randomize: bool,
    /// TWAP start timestamp in milliseconds.
    pub timestamp: i64,
    pub stop_price: Option<String>,
    pub trigger_price: Option<String>,
    pub trigger_above: Option<bool>,
    pub status: String,
    pub description: Option<String>,
}

/// One complete venue-native row from a Hyperliquid `spotState` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidSpotBalanceMsg {
    pub msg_type: BasicAccountEventType,
    /// Local snapshot application timestamp in milliseconds; `spotState` has no row timestamp.
    pub timestamp: i64,
    pub token: i64,
    pub coin: String,
    pub total: String,
    pub hold: String,
    pub entry_ntl: String,
}

/// One complete venue-native summary row from a Hyperliquid perpetual DEX
/// account-state snapshot. Decimal strings remain byte-for-byte equivalent to
/// the exchange payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidPerpDexStateMsg {
    pub msg_type: BasicAccountEventType,
    /// Local snapshot application timestamp in milliseconds.
    pub timestamp: i64,
    /// Empty for the validator-operated default DEX.
    pub dex: String,
    pub collateral_token: i64,
    pub margin_account_value: String,
    pub margin_total_ntl_pos: String,
    pub margin_total_raw_usd: String,
    pub margin_total_margin_used: String,
    pub cross_account_value: String,
    pub cross_total_ntl_pos: String,
    pub cross_total_raw_usd: String,
    pub cross_total_margin_used: String,
    pub cross_maintenance_margin_used: String,
    pub withdrawable: String,
}

impl HyperliquidFundingMsg {
    pub fn create(
        event_time: i64,
        coin: String,
        usdc: String,
        szi: String,
        funding_rate: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidFunding,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            fact_seq: 0,
            event_time,
            coin,
            usdc,
            szi,
            funding_rate,
            transaction_hash: None,
        }
    }

    pub fn with_transaction_hash(mut self, transaction_hash: Option<String>) -> Self {
        self.transaction_hash = transaction_hash;
        self
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.account_hash = identity.account_hash;
        self.monitor_id = identity.monitor_id;
        self.fact_seq = identity.fact_seq;
        self
    }

    pub fn fact_identity(&self) -> HyperliquidFactIdentity {
        HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq: self.fact_seq,
        }
    }

    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/funding");
        hasher.update(self.account_hash);
        hasher.update(self.event_time.to_be_bytes());
        update_digest_field(&mut hasher, self.coin.as_bytes());
        update_digest_field(&mut hasher, self.usdc.as_bytes());
        update_digest_field(&mut hasher, self.szi.as_bytes());
        update_digest_field(&mut hasher, self.funding_rate.as_bytes());
        finish_digest(hasher)
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 8
            + encoded_string_len(&self.coin)
            + encoded_string_len(&self.usdc)
            + encoded_string_len(&self.szi)
            + encoded_string_len(&self.funding_rate)
            + encoded_optional_string_len(&self.transaction_hash);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.fact_seq);
        buf.put_i64_le(self.event_time);
        put_string(&mut buf, &self.coin);
        put_string(&mut buf, &self.usdc);
        put_string(&mut buf, &self.szi);
        put_string(&mut buf, &self.funding_rate);
        put_optional_string(&mut buf, &self.transaction_hash);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8 + 8 + 4 * 4 + 1;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidFundingMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidFunding as u32 {
            anyhow::bail!("invalid HyperliquidFundingMsg type: {msg_type}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        let monitor_id = cursor.get_u64_le();
        let fact_seq = cursor.get_u64_le();
        let event_time = cursor.get_i64_le();
        let coin = get_string(&mut cursor, "funding coin")?;
        let usdc = get_string(&mut cursor, "funding usdc")?;
        let szi = get_string(&mut cursor, "funding szi")?;
        let funding_rate = get_string(&mut cursor, "funding rate")?;
        let transaction_hash = get_optional_string(&mut cursor, "funding transaction hash")?;
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidFundingMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidFunding,
            account_hash,
            monitor_id,
            fact_seq,
            event_time,
            coin,
            usdc,
            szi,
            funding_rate,
            transaction_hash,
        })
    }
}

impl HyperliquidLedgerMsg {
    pub fn create(
        event_time: i64,
        transaction_hash: String,
        delta_type: String,
        delta_json: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidLedger,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            fact_seq: 0,
            event_time,
            transaction_hash,
            delta_type,
            delta_json,
        }
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.account_hash = identity.account_hash;
        self.monitor_id = identity.monitor_id;
        self.fact_seq = identity.fact_seq;
        self
    }

    pub fn fact_identity(&self) -> HyperliquidFactIdentity {
        HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq: self.fact_seq,
        }
    }

    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/non_funding_ledger");
        hasher.update(self.account_hash);
        hasher.update(self.event_time.to_be_bytes());
        update_digest_field(&mut hasher, self.transaction_hash.as_bytes());
        update_digest_field(&mut hasher, self.delta_type.as_bytes());
        update_digest_field(&mut hasher, self.delta_json.as_bytes());
        finish_digest(hasher)
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 8
            + encoded_string_len(&self.transaction_hash)
            + encoded_string_len(&self.delta_type)
            + encoded_string_len(&self.delta_json);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.fact_seq);
        buf.put_i64_le(self.event_time);
        put_string(&mut buf, &self.transaction_hash);
        put_string(&mut buf, &self.delta_type);
        put_string(&mut buf, &self.delta_json);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8 + 8 + 4 * 3;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidLedgerMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidLedger as u32 {
            anyhow::bail!("invalid HyperliquidLedgerMsg type: {msg_type}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        let monitor_id = cursor.get_u64_le();
        let fact_seq = cursor.get_u64_le();
        let event_time = cursor.get_i64_le();
        let transaction_hash = get_string(&mut cursor, "ledger transaction hash")?;
        let delta_type = get_string(&mut cursor, "ledger delta type")?;
        let delta_json = get_string(&mut cursor, "ledger delta JSON")?;
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidLedgerMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidLedger,
            account_hash,
            monitor_id,
            fact_seq,
            event_time,
            transaction_hash,
            delta_type,
            delta_json,
        })
    }
}

impl HyperliquidTwapSliceFillMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        venue: u8,
        event_time: i64,
        wire_coin: String,
        symbol: String,
        order_id: i64,
        venue_trade_id: i64,
        transaction_hash: String,
        twap_id: i64,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidTwapSliceFill,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            fact_seq: 0,
            venue,
            event_time,
            wire_coin,
            symbol,
            order_id,
            venue_trade_id,
            transaction_hash,
            twap_id,
        }
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.account_hash = identity.account_hash;
        self.monitor_id = identity.monitor_id;
        self.fact_seq = identity.fact_seq;
        self
    }

    pub fn fact_identity(&self) -> HyperliquidFactIdentity {
        HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq: self.fact_seq,
        }
    }

    /// Uses the venue's globally unique fill tuple `(time, coin, tid)` and is
    /// deliberately independent from the producer epoch and parent id.
    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/twap_slice_fill");
        hasher.update(self.account_hash);
        hasher.update([self.venue]);
        hasher.update(self.event_time.to_be_bytes());
        update_digest_field(&mut hasher, self.wire_coin.as_bytes());
        hasher.update(self.venue_trade_id.to_be_bytes());
        finish_digest(hasher)
    }

    pub fn content_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/twap_slice_fill/content");
        hasher.update(self.account_hash);
        hasher.update([self.venue]);
        hasher.update(self.event_time.to_be_bytes());
        update_digest_field(&mut hasher, self.wire_coin.as_bytes());
        update_digest_field(&mut hasher, self.symbol.as_bytes());
        hasher.update(self.order_id.to_be_bytes());
        hasher.update(self.venue_trade_id.to_be_bytes());
        update_digest_field(&mut hasher, self.transaction_hash.as_bytes());
        hasher.update(self.twap_id.to_be_bytes());
        finish_digest(hasher)
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 1
            + 8
            + encoded_string_len(&self.wire_coin)
            + encoded_string_len(&self.symbol)
            + 8
            + 8
            + encoded_string_len(&self.transaction_hash)
            + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.fact_seq);
        buf.put_u8(self.venue);
        buf.put_i64_le(self.event_time);
        put_string(&mut buf, &self.wire_coin);
        put_string(&mut buf, &self.symbol);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.venue_trade_id);
        put_string(&mut buf, &self.transaction_hash);
        buf.put_i64_le(self.twap_id);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize =
            4 + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8 + 1 + 8 + 4 + 4 + 8 + 8 + 4 + 8;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidTwapSliceFillMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidTwapSliceFill as u32 {
            anyhow::bail!("invalid HyperliquidTwapSliceFillMsg type: {msg_type}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        let monitor_id = cursor.get_u64_le();
        let fact_seq = cursor.get_u64_le();
        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();
        let wire_coin = get_string(&mut cursor, "TWAP slice wire coin")?;
        let symbol = get_string(&mut cursor, "TWAP slice symbol")?;
        if cursor.remaining() < 8 + 8 + 4 + 8 {
            anyhow::bail!("HyperliquidTwapSliceFillMsg truncated before fill identity");
        }
        let order_id = cursor.get_i64_le();
        let venue_trade_id = cursor.get_i64_le();
        let transaction_hash = get_string(&mut cursor, "TWAP slice transaction hash")?;
        if cursor.remaining() < 8 {
            anyhow::bail!("HyperliquidTwapSliceFillMsg truncated before TWAP id");
        }
        let twap_id = cursor.get_i64_le();
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidTwapSliceFillMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidTwapSliceFill,
            account_hash,
            monitor_id,
            fact_seq,
            venue,
            event_time,
            wire_coin,
            symbol,
            order_id,
            venue_trade_id,
            transaction_hash,
            twap_id,
        })
    }
}

impl HyperliquidTwapHistoryMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        event_time: i64,
        twap_id: Option<i64>,
        user: String,
        coin: String,
        side: String,
        size: String,
        executed_size: String,
        executed_notional: String,
        minutes: i64,
        reduce_only: bool,
        randomize: bool,
        timestamp: i64,
        stop_price: Option<String>,
        trigger_price: Option<String>,
        trigger_above: Option<bool>,
        status: String,
        description: Option<String>,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidTwapHistory,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            fact_seq: 0,
            event_time,
            twap_id,
            user,
            coin,
            side,
            size,
            executed_size,
            executed_notional,
            minutes,
            reduce_only,
            randomize,
            timestamp,
            stop_price,
            trigger_price,
            trigger_above,
            status,
            description,
        }
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.account_hash = identity.account_hash;
        self.monitor_id = identity.monitor_id;
        self.fact_seq = identity.fact_seq;
        self
    }

    pub fn fact_identity(&self) -> HyperliquidFactIdentity {
        HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq: self.fact_seq,
        }
    }

    /// A venue history row is identified by parent id (when present), its row
    /// time, the original TWAP start, user/coin, and lifecycle status.
    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/twap_history");
        hasher.update(self.account_hash);
        hasher.update(self.event_time.to_be_bytes());
        match self.twap_id {
            Some(value) => {
                hasher.update([1]);
                hasher.update(value.to_be_bytes());
            }
            None => hasher.update([0]),
        }
        hasher.update(self.timestamp.to_be_bytes());
        update_digest_field(&mut hasher, self.user.as_bytes());
        update_digest_field(&mut hasher, self.coin.as_bytes());
        update_digest_field(&mut hasher, self.status.as_bytes());
        finish_digest(hasher)
    }

    pub fn content_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/twap_history/content");
        hasher.update(self.account_hash);
        hasher.update(self.event_time.to_be_bytes());
        match self.twap_id {
            Some(value) => {
                hasher.update([1]);
                hasher.update(value.to_be_bytes());
            }
            None => hasher.update([0]),
        }
        for value in [
            self.user.as_str(),
            self.coin.as_str(),
            self.side.as_str(),
            self.size.as_str(),
            self.executed_size.as_str(),
            self.executed_notional.as_str(),
        ] {
            update_digest_field(&mut hasher, value.as_bytes());
        }
        hasher.update(self.minutes.to_be_bytes());
        hasher.update([u8::from(self.reduce_only), u8::from(self.randomize)]);
        hasher.update(self.timestamp.to_be_bytes());
        for value in [
            self.stop_price.as_deref(),
            self.trigger_price.as_deref(),
            self.description.as_deref(),
        ] {
            match value {
                Some(value) => {
                    hasher.update([1]);
                    update_digest_field(&mut hasher, value.as_bytes());
                }
                None => hasher.update([0]),
            }
        }
        put_digest_optional_bool(&mut hasher, self.trigger_above);
        update_digest_field(&mut hasher, self.status.as_bytes());
        finish_digest(hasher)
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 8
            + 1
            + self.twap_id.map(|_| 8).unwrap_or(0)
            + encoded_string_len(&self.user)
            + encoded_string_len(&self.coin)
            + encoded_string_len(&self.side)
            + encoded_string_len(&self.size)
            + encoded_string_len(&self.executed_size)
            + encoded_string_len(&self.executed_notional)
            + 8
            + 1
            + 1
            + 8
            + encoded_optional_string_len(&self.stop_price)
            + encoded_optional_string_len(&self.trigger_price)
            + 1
            + encoded_string_len(&self.status)
            + encoded_optional_string_len(&self.description);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.fact_seq);
        buf.put_i64_le(self.event_time);
        put_optional_i64(&mut buf, self.twap_id);
        put_string(&mut buf, &self.user);
        put_string(&mut buf, &self.coin);
        put_string(&mut buf, &self.side);
        put_string(&mut buf, &self.size);
        put_string(&mut buf, &self.executed_size);
        put_string(&mut buf, &self.executed_notional);
        buf.put_i64_le(self.minutes);
        buf.put_u8(u8::from(self.reduce_only));
        buf.put_u8(u8::from(self.randomize));
        buf.put_i64_le(self.timestamp);
        put_optional_string(&mut buf, &self.stop_price);
        put_optional_string(&mut buf, &self.trigger_price);
        put_optional_bool(&mut buf, self.trigger_above);
        put_string(&mut buf, &self.status);
        put_optional_string(&mut buf, &self.description);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 8
            + 1
            + 4 * 6
            + 8
            + 1
            + 1
            + 8
            + 1
            + 1
            + 1
            + 4
            + 1;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidTwapHistoryMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidTwapHistory as u32 {
            anyhow::bail!("invalid HyperliquidTwapHistoryMsg type: {msg_type}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        let monitor_id = cursor.get_u64_le();
        let fact_seq = cursor.get_u64_le();
        let event_time = cursor.get_i64_le();
        let twap_id = get_optional_i64(&mut cursor, "TWAP id")?;
        let user = get_string(&mut cursor, "TWAP user")?;
        let coin = get_string(&mut cursor, "TWAP coin")?;
        let side = get_string(&mut cursor, "TWAP side")?;
        let size = get_string(&mut cursor, "TWAP size")?;
        let executed_size = get_string(&mut cursor, "TWAP executed size")?;
        let executed_notional = get_string(&mut cursor, "TWAP executed notional")?;
        if cursor.remaining() < 8 + 1 + 1 + 8 {
            anyhow::bail!("HyperliquidTwapHistoryMsg truncated before TWAP state fields");
        }
        let minutes = cursor.get_i64_le();
        let reduce_only = get_bool(&mut cursor, "TWAP reduce-only")?;
        let randomize = get_bool(&mut cursor, "TWAP randomize")?;
        let timestamp = cursor.get_i64_le();
        let stop_price = get_optional_string(&mut cursor, "TWAP stop price")?;
        let trigger_price = get_optional_string(&mut cursor, "TWAP trigger price")?;
        let trigger_above = get_optional_bool(&mut cursor, "TWAP trigger direction")?;
        let status = get_string(&mut cursor, "TWAP status")?;
        let description = get_optional_string(&mut cursor, "TWAP status description")?;
        if trigger_price.is_some() != trigger_above.is_some() {
            anyhow::bail!("HyperliquidTwapHistoryMsg has a partial trigger");
        }
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidTwapHistoryMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidTwapHistory,
            account_hash,
            monitor_id,
            fact_seq,
            event_time,
            twap_id,
            user,
            coin,
            side,
            size,
            executed_size,
            executed_notional,
            minutes,
            reduce_only,
            randomize,
            timestamp,
            stop_price,
            trigger_price,
            trigger_above,
            status,
            description,
        })
    }
}

impl HyperliquidSpotBalanceMsg {
    pub fn create(
        timestamp: i64,
        token: i64,
        coin: String,
        total: String,
        hold: String,
        entry_ntl: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidSpotBalance,
            timestamp,
            token,
            coin,
            total,
            hold,
            entry_ntl,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8
            + 8
            + encoded_string_len(&self.coin)
            + encoded_string_len(&self.total)
            + encoded_string_len(&self.hold)
            + encoded_string_len(&self.entry_ntl);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_i64_le(self.token);
        put_string(&mut buf, &self.coin);
        put_string(&mut buf, &self.total);
        put_string(&mut buf, &self.hold);
        put_string(&mut buf, &self.entry_ntl);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 + 4 * 4;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidSpotBalanceMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidSpotBalance as u32 {
            anyhow::bail!("invalid HyperliquidSpotBalanceMsg type: {msg_type}");
        }
        let timestamp = cursor.get_i64_le();
        let token = cursor.get_i64_le();
        let coin = get_string(&mut cursor, "spot balance coin")?;
        let total = get_string(&mut cursor, "spot balance total")?;
        let hold = get_string(&mut cursor, "spot balance hold")?;
        let entry_ntl = get_string(&mut cursor, "spot balance entry notional")?;
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidSpotBalanceMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidSpotBalance,
            timestamp,
            token,
            coin,
            total,
            hold,
            entry_ntl,
        })
    }
}

impl HyperliquidPerpDexStateMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        timestamp: i64,
        dex: String,
        collateral_token: i64,
        margin_account_value: String,
        margin_total_ntl_pos: String,
        margin_total_raw_usd: String,
        margin_total_margin_used: String,
        cross_account_value: String,
        cross_total_ntl_pos: String,
        cross_total_raw_usd: String,
        cross_total_margin_used: String,
        cross_maintenance_margin_used: String,
        withdrawable: String,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidPerpDexState,
            timestamp,
            dex,
            collateral_token,
            margin_account_value,
            margin_total_ntl_pos,
            margin_total_raw_usd,
            margin_total_margin_used,
            cross_account_value,
            cross_total_ntl_pos,
            cross_total_raw_usd,
            cross_total_margin_used,
            cross_maintenance_margin_used,
            withdrawable,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + 8
            + encoded_string_len(&self.dex)
            + 8
            + encoded_string_len(&self.margin_account_value)
            + encoded_string_len(&self.margin_total_ntl_pos)
            + encoded_string_len(&self.margin_total_raw_usd)
            + encoded_string_len(&self.margin_total_margin_used)
            + encoded_string_len(&self.cross_account_value)
            + encoded_string_len(&self.cross_total_ntl_pos)
            + encoded_string_len(&self.cross_total_raw_usd)
            + encoded_string_len(&self.cross_total_margin_used)
            + encoded_string_len(&self.cross_maintenance_margin_used)
            + encoded_string_len(&self.withdrawable);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        put_string(&mut buf, &self.dex);
        buf.put_i64_le(self.collateral_token);
        put_string(&mut buf, &self.margin_account_value);
        put_string(&mut buf, &self.margin_total_ntl_pos);
        put_string(&mut buf, &self.margin_total_raw_usd);
        put_string(&mut buf, &self.margin_total_margin_used);
        put_string(&mut buf, &self.cross_account_value);
        put_string(&mut buf, &self.cross_total_ntl_pos);
        put_string(&mut buf, &self.cross_total_raw_usd);
        put_string(&mut buf, &self.cross_total_margin_used);
        put_string(&mut buf, &self.cross_maintenance_margin_used);
        put_string(&mut buf, &self.withdrawable);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 4 + 8 + 4 * 10;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidPerpDexStateMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidPerpDexState as u32 {
            anyhow::bail!("invalid HyperliquidPerpDexStateMsg type: {msg_type}");
        }
        let timestamp = cursor.get_i64_le();
        let dex = get_string(&mut cursor, "perp dex")?;
        if cursor.remaining() < 8 + 4 * 10 {
            anyhow::bail!("HyperliquidPerpDexStateMsg truncated before collateral token");
        }
        let collateral_token = cursor.get_i64_le();
        let margin_account_value = get_string(&mut cursor, "margin account value")?;
        let margin_total_ntl_pos = get_string(&mut cursor, "margin total notional")?;
        let margin_total_raw_usd = get_string(&mut cursor, "margin total raw USD")?;
        let margin_total_margin_used = get_string(&mut cursor, "margin total margin used")?;
        let cross_account_value = get_string(&mut cursor, "cross account value")?;
        let cross_total_ntl_pos = get_string(&mut cursor, "cross total notional")?;
        let cross_total_raw_usd = get_string(&mut cursor, "cross total raw USD")?;
        let cross_total_margin_used = get_string(&mut cursor, "cross total margin used")?;
        let cross_maintenance_margin_used =
            get_string(&mut cursor, "cross maintenance margin used")?;
        let withdrawable = get_string(&mut cursor, "withdrawable")?;
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidPerpDexStateMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidPerpDexState,
            timestamp,
            dex,
            collateral_token,
            margin_account_value,
            margin_total_ntl_pos,
            margin_total_raw_usd,
            margin_total_margin_used,
            cross_account_value,
            cross_total_ntl_pos,
            cross_total_raw_usd,
            cross_total_margin_used,
            cross_maintenance_margin_used,
            withdrawable,
        })
    }
}

/// Lifecycle phase for an atomic Hyperliquid account-state snapshot batch.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidSnapshotPhase {
    Invalidate = 1,
    Begin = 2,
    Complete = 3,
}

impl HyperliquidSnapshotPhase {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Invalidate),
            2 => Some(Self::Begin),
            3 => Some(Self::Complete),
            _ => None,
        }
    }
}

/// Identifies the redundant private websocket path that supplied a snapshot.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidSnapshotPath {
    Primary = 1,
    Secondary = 2,
}

/// Lifecycle phase for one consumer-correlated order/fill replay transaction.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidFactReplayPhase {
    Begin = 1,
    Complete = 2,
    Gap = 3,
}

impl HyperliquidFactReplayPhase {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Begin),
            2 => Some(Self::Complete),
            3 => Some(Self::Gap),
            _ => None,
        }
    }
}

/// Reverse IPC request from one consumer to the factual replay producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidFactReplayRequestMsg {
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub consumer_id: u64,
    pub request_id: u64,
    pub last_monitor_id: u64,
    pub last_fact_seq: u64,
}

impl HyperliquidFactReplayRequestMsg {
    pub const ENCODED_LEN: usize = 4 + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8 + 8 + 8;

    pub fn to_ipc_payload(&self) -> [u8; HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN] {
        let mut payload = [0_u8; HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN];
        let mut buf = BytesMut::with_capacity(Self::ENCODED_LEN);
        buf.put_u32_le(HYPERLIQUID_FACT_REPLAY_REQUEST_MAGIC);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.consumer_id);
        buf.put_u64_le(self.request_id);
        buf.put_u64_le(self.last_monitor_id);
        buf.put_u64_le(self.last_fact_seq);
        payload[..Self::ENCODED_LEN].copy_from_slice(&buf);
        payload
    }

    pub fn from_ipc_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < Self::ENCODED_LEN {
            anyhow::bail!(
                "Hyperliquid fact replay request too short: {}",
                payload.len()
            );
        }
        let mut cursor = Bytes::copy_from_slice(&payload[..Self::ENCODED_LEN]);
        let magic = cursor.get_u32_le();
        if magic != HYPERLIQUID_FACT_REPLAY_REQUEST_MAGIC {
            anyhow::bail!("invalid Hyperliquid fact replay request magic: {magic:#x}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        Ok(Self {
            account_hash,
            consumer_id: cursor.get_u64_le(),
            request_id: cursor.get_u64_le(),
            last_monitor_id: cursor.get_u64_le(),
            last_fact_seq: cursor.get_u64_le(),
        })
    }
}

/// Producer response on the regular account event stream. Replay payloads are
/// the same stamped order/fill facts used by the live stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidFactReplayControlMsg {
    pub msg_type: BasicAccountEventType,
    pub phase: u8,
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    pub monitor_id: u64,
    pub consumer_id: u64,
    pub request_id: u64,
    /// Inclusive replay range. `first_seq > last_seq` denotes an empty replay.
    pub first_seq: u64,
    pub last_seq: u64,
    /// Producer head captured when this bounded replay transaction began.
    pub head_seq: u64,
}

impl HyperliquidFactReplayControlMsg {
    pub const ENCODED_LEN: usize =
        4 + 1 + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8 + 8 + 8 + 8 + 8;

    pub fn create(
        phase: HyperliquidFactReplayPhase,
        account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
        monitor_id: u64,
        consumer_id: u64,
        request_id: u64,
        first_seq: u64,
        last_seq: u64,
        head_seq: u64,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidFactReplayControl,
            phase: phase as u8,
            account_hash,
            monitor_id,
            consumer_id,
            request_id,
            first_seq,
            last_seq,
            head_seq,
        }
    }

    pub fn phase(&self) -> Option<HyperliquidFactReplayPhase> {
        HyperliquidFactReplayPhase::from_u8(self.phase)
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::ENCODED_LEN);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u8(self.phase);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.consumer_id);
        buf.put_u64_le(self.request_id);
        buf.put_u64_le(self.first_seq);
        buf.put_u64_le(self.last_seq);
        buf.put_u64_le(self.head_seq);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() != Self::ENCODED_LEN {
            anyhow::bail!(
                "HyperliquidFactReplayControlMsg invalid length: expected={} actual={}",
                Self::ENCODED_LEN,
                data.len()
            );
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidFactReplayControl as u32 {
            anyhow::bail!("invalid Hyperliquid fact replay control type: {msg_type}");
        }
        let phase = cursor.get_u8();
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidFactReplayControl,
            phase,
            account_hash,
            monitor_id: cursor.get_u64_le(),
            consumer_id: cursor.get_u64_le(),
            request_id: cursor.get_u64_le(),
            first_seq: cursor.get_u64_le(),
            last_seq: cursor.get_u64_le(),
            head_seq: cursor.get_u64_le(),
        })
    }
}

impl HyperliquidSnapshotPath {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Primary),
            2 => Some(Self::Secondary),
            _ => None,
        }
    }
}

/// Atomic lifecycle control for one complete Hyperliquid account snapshot.
/// The surrounding `BasicAccountEventMsg` carries the mode-specific scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidSnapshotCompleteMsg {
    pub msg_type: BasicAccountEventType,
    pub phase: u8,
    pub path: u8,
    /// `TradingVenue` discriminant: 12=spot, 13=perpetual.
    pub venue: u8,
    /// Stable identity of the normalized account whose rows surround this control.
    pub account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    /// Unique for one account-monitor process lifetime.
    pub monitor_id: u64,
    /// Monotonic websocket connection generation within that process/path.
    pub generation: u64,
    /// Monotonic snapshot batch within the connection generation/path.
    pub batch_id: u64,
    /// Local snapshot application timestamp in milliseconds.
    pub timestamp: i64,
    /// Consumer must revoke this completion after this Unix timestamp (ms).
    pub valid_until: i64,
}

impl HyperliquidSnapshotCompleteMsg {
    pub const ENCODED_LEN: usize =
        4 + 1 + 1 + 1 + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8 + 8 + 8 + 8;

    /// Compatibility-free constructor used by snapshot processors. The live
    /// account monitor replaces this marker with a path-correlated control.
    pub fn create(venue: u8, timestamp: i64) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidSnapshotComplete,
            phase: HyperliquidSnapshotPhase::Complete as u8,
            path: 0,
            venue,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            generation: 0,
            batch_id: 0,
            timestamp,
            valid_until: timestamp,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_control(
        phase: HyperliquidSnapshotPhase,
        path: HyperliquidSnapshotPath,
        venue: u8,
        monitor_id: u64,
        generation: u64,
        batch_id: u64,
        timestamp: i64,
        valid_until: i64,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidSnapshotComplete,
            phase: phase as u8,
            path: path as u8,
            venue,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id,
            generation,
            batch_id,
            timestamp,
            valid_until,
        }
    }

    pub fn with_account_hash(
        mut self,
        account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    ) -> Self {
        self.account_hash = account_hash;
        self
    }

    pub fn phase(&self) -> Option<HyperliquidSnapshotPhase> {
        HyperliquidSnapshotPhase::from_u8(self.phase)
    }

    pub fn path(&self) -> Option<HyperliquidSnapshotPath> {
        HyperliquidSnapshotPath::from_u8(self.path)
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::ENCODED_LEN);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u8(self.phase);
        buf.put_u8(self.path);
        buf.put_u8(self.venue);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.generation);
        buf.put_u64_le(self.batch_id);
        buf.put_i64_le(self.timestamp);
        buf.put_i64_le(self.valid_until);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() != Self::ENCODED_LEN {
            anyhow::bail!(
                "HyperliquidSnapshotCompleteMsg invalid length: expected={} actual={}",
                Self::ENCODED_LEN,
                data.len()
            );
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidSnapshotComplete as u32 {
            anyhow::bail!("invalid HyperliquidSnapshotCompleteMsg type: {msg_type}");
        }
        let phase = cursor.get_u8();
        let path = cursor.get_u8();
        let venue = cursor.get_u8();
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidSnapshotComplete,
            phase,
            path,
            venue,
            account_hash,
            monitor_id: cursor.get_u64_le(),
            generation: cursor.get_u64_le(),
            batch_id: cursor.get_u64_le(),
            timestamp: cursor.get_i64_le(),
            valid_until: cursor.get_i64_le(),
        })
    }
}

impl HyperliquidBasicFillMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        venue: u8,
        event_time: i64,
        trade_time: i64,
        symbol: String,
        order_id: i64,
        client_order_id: i64,
        cloid: String,
        trade_id: &str,
        venue_trade_id: i64,
        transaction_hash: String,
        liquidation_method: String,
        side: u8,
        is_maker: bool,
        price: f64,
        last_filled_quantity: f64,
        cumulative_filled_quantity: f64,
        order_status: Option<u8>,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::HyperliquidFill,
            account_hash: [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 0,
            fact_seq: 0,
            venue,
            event_time,
            trade_time,
            symbol,
            order_id,
            client_order_id,
            cloid,
            trade_id: trade_id_bytes_from_str(trade_id),
            venue_trade_id,
            transaction_hash,
            liquidation_method,
            side,
            is_maker: u8::from(is_maker),
            price,
            last_filled_quantity,
            cumulative_filled_quantity,
            order_status: order_status.unwrap_or(0),
            wire_coin: None,
            start_position: None,
            dir: None,
            closed_pnl: None,
            fee: None,
            fee_token: None,
            builder_fee: None,
            twap_id: None,
            liquidated_user: None,
            liquidation_mark_price: None,
        }
    }

    pub fn with_venue_audit_fields(
        mut self,
        wire_coin: Option<String>,
        start_position: Option<String>,
        dir: Option<String>,
        closed_pnl: Option<String>,
        fee: Option<String>,
        fee_token: Option<String>,
        builder_fee: Option<String>,
        twap_id: Option<i64>,
        liquidated_user: Option<String>,
        liquidation_mark_price: Option<String>,
    ) -> Self {
        self.wire_coin = wire_coin;
        self.start_position = start_position;
        self.dir = dir;
        self.closed_pnl = closed_pnl;
        self.fee = fee;
        self.fee_token = fee_token;
        self.builder_fee = builder_fee;
        self.twap_id = twap_id;
        self.liquidated_user = liquidated_user;
        self.liquidation_mark_price = liquidation_mark_price;
        self
    }

    pub fn with_fact_identity(mut self, identity: HyperliquidFactIdentity) -> Self {
        self.account_hash = identity.account_hash;
        self.monitor_id = identity.monitor_id;
        self.fact_seq = identity.fact_seq;
        self
    }

    pub fn fact_identity(&self) -> HyperliquidFactIdentity {
        HyperliquidFactIdentity {
            account_hash: self.account_hash,
            monitor_id: self.monitor_id,
            fact_seq: self.fact_seq,
        }
    }

    pub fn trade_id_str(&self) -> &str {
        trade_id_as_str(&self.trade_id)
    }

    pub fn stable_venue_key(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"mkt_signal/hyperliquid/fill");
        hasher.update(self.account_hash);
        hasher.update([self.venue]);
        hasher.update(self.trade_time.to_be_bytes());
        update_digest_field(
            &mut hasher,
            self.wire_coin.as_deref().unwrap_or(&self.symbol).as_bytes(),
        );
        hasher.update(self.venue_trade_id.to_be_bytes());
        finish_digest(hasher)
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 1
            + 8
            + 8
            + encoded_string_len(&self.symbol)
            + 8
            + 8
            + encoded_string_len(&self.cloid)
            + TRADE_ID_LEN
            + 8
            + encoded_string_len(&self.transaction_hash)
            + encoded_string_len(&self.liquidation_method)
            + 1
            + 1
            + 8
            + 8
            + 8
            + 1
            + encoded_optional_string_len(&self.wire_coin)
            + encoded_optional_string_len(&self.start_position)
            + encoded_optional_string_len(&self.dir)
            + encoded_optional_string_len(&self.closed_pnl)
            + encoded_optional_string_len(&self.fee)
            + encoded_optional_string_len(&self.fee_token)
            + encoded_optional_string_len(&self.builder_fee)
            + 1
            + self.twap_id.map(|_| 8).unwrap_or(0)
            + encoded_optional_string_len(&self.liquidated_user)
            + encoded_optional_string_len(&self.liquidation_mark_price);
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32_le(self.msg_type as u32);
        buf.put(&self.account_hash[..]);
        buf.put_u64_le(self.monitor_id);
        buf.put_u64_le(self.fact_seq);
        buf.put_u8(self.venue);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.trade_time);
        put_string(&mut buf, &self.symbol);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.client_order_id);
        put_string(&mut buf, &self.cloid);
        buf.put(&self.trade_id[..]);
        buf.put_i64_le(self.venue_trade_id);
        put_string(&mut buf, &self.transaction_hash);
        put_string(&mut buf, &self.liquidation_method);
        buf.put_u8(self.side);
        buf.put_u8(self.is_maker);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.last_filled_quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_u8(self.order_status);
        put_optional_string(&mut buf, &self.wire_coin);
        put_optional_string(&mut buf, &self.start_position);
        put_optional_string(&mut buf, &self.dir);
        put_optional_string(&mut buf, &self.closed_pnl);
        put_optional_string(&mut buf, &self.fee);
        put_optional_string(&mut buf, &self.fee_token);
        put_optional_string(&mut buf, &self.builder_fee);
        match self.twap_id {
            Some(twap_id) => {
                buf.put_u8(1);
                buf.put_i64_le(twap_id);
            }
            None => buf.put_u8(0),
        }
        put_optional_string(&mut buf, &self.liquidated_user);
        put_optional_string(&mut buf, &self.liquidation_mark_price);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4
            + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN
            + 8
            + 8
            + 1
            + 8
            + 8
            + 4
            + 8
            + 8
            + 4
            + TRADE_ID_LEN
            + 8
            + 4
            + 4
            + 1
            + 1
            + 8
            + 8
            + 8
            + 1
            + 10;
        if data.len() < MIN_SIZE {
            anyhow::bail!("HyperliquidBasicFillMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::HyperliquidFill as u32 {
            anyhow::bail!("invalid HyperliquidBasicFillMsg type: {msg_type}");
        }
        let mut account_hash = [0; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        cursor.copy_to_slice(&mut account_hash);
        let monitor_id = cursor.get_u64_le();
        let fact_seq = cursor.get_u64_le();
        let venue = cursor.get_u8();
        let event_time = cursor.get_i64_le();
        let trade_time = cursor.get_i64_le();
        let symbol = get_string(&mut cursor, "fill symbol")?;
        if cursor.remaining() < 8 + 8 + 4 {
            anyhow::bail!("HyperliquidBasicFillMsg truncated before identifiers");
        }
        let order_id = cursor.get_i64_le();
        let client_order_id = cursor.get_i64_le();
        let cloid = get_string(&mut cursor, "fill cloid")?;
        if cursor.remaining() < TRADE_ID_LEN + 8 + 4 {
            anyhow::bail!("HyperliquidBasicFillMsg truncated before trade identity");
        }
        let mut trade_id = [0; TRADE_ID_LEN];
        cursor.copy_to_slice(&mut trade_id);
        let venue_trade_id = cursor.get_i64_le();
        let transaction_hash = get_string(&mut cursor, "transaction_hash")?;
        let liquidation_method = get_string(&mut cursor, "liquidation_method")?;
        if cursor.remaining() < 1 + 1 + 8 + 8 + 8 + 1 {
            anyhow::bail!("HyperliquidBasicFillMsg truncated before fill fields");
        }
        let side = cursor.get_u8();
        let is_maker = cursor.get_u8();
        let price = cursor.get_f64_le();
        let last_filled_quantity = cursor.get_f64_le();
        let cumulative_filled_quantity = cursor.get_f64_le();
        let order_status = cursor.get_u8();
        let wire_coin = get_optional_string(&mut cursor, "fill wire coin")?;
        let start_position = get_optional_string(&mut cursor, "fill start position")?;
        let dir = get_optional_string(&mut cursor, "fill direction")?;
        let closed_pnl = get_optional_string(&mut cursor, "fill closed PnL")?;
        let fee = get_optional_string(&mut cursor, "fill fee")?;
        let fee_token = get_optional_string(&mut cursor, "fill fee token")?;
        let builder_fee = get_optional_string(&mut cursor, "fill builder fee")?;
        if !cursor.has_remaining() {
            anyhow::bail!("HyperliquidBasicFillMsg truncated before optional TWAP id");
        }
        let twap_id = match cursor.get_u8() {
            0 => None,
            1 if cursor.remaining() >= 8 => Some(cursor.get_i64_le()),
            1 => anyhow::bail!("HyperliquidBasicFillMsg truncated reading TWAP id"),
            flag => {
                anyhow::bail!("HyperliquidBasicFillMsg has invalid optional TWAP id flag: {flag}")
            }
        };
        let liquidated_user = get_optional_string(&mut cursor, "fill liquidated user")?;
        let liquidation_mark_price =
            get_optional_string(&mut cursor, "fill liquidation mark price")?;
        if cursor.has_remaining() {
            anyhow::bail!(
                "HyperliquidBasicFillMsg has {} trailing bytes",
                cursor.remaining()
            );
        }
        Ok(Self {
            msg_type: BasicAccountEventType::HyperliquidFill,
            account_hash,
            monitor_id,
            fact_seq,
            venue,
            event_time,
            trade_time,
            symbol,
            order_id,
            client_order_id,
            cloid,
            trade_id,
            venue_trade_id,
            transaction_hash,
            liquidation_method,
            side,
            is_maker,
            price,
            last_filled_quantity,
            cumulative_filled_quantity,
            order_status,
            wire_coin,
            start_position,
            dir,
            closed_pnl,
            fee,
            fee_token,
            builder_fee,
            twap_id,
            liquidated_user,
            liquidation_mark_price,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        hyperliquid_account_identity_hash, HyperliquidBasicOrderMsg, HyperliquidFactIdentity,
        HyperliquidFactReplayControlMsg, HyperliquidFactReplayPhase,
        HyperliquidFactReplayRequestMsg, HyperliquidFundingMsg, HyperliquidLedgerMsg,
        HyperliquidPerpDexStateMsg, HyperliquidSnapshotCompleteMsg, HyperliquidSnapshotPath,
        HyperliquidSnapshotPhase, HyperliquidSpotBalanceMsg, HyperliquidTwapHistoryMsg,
        HyperliquidTwapSliceFillMsg, HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN,
    };

    #[test]
    fn order_message_roundtrips() {
        let msg = HyperliquidBasicOrderMsg::create(
            13,
            1_725_000_000_123,
            "BTCUSDC".to_string(),
            987,
            42,
            "0x0000000000000000000000000000002a".to_string(),
            1,
            1,
            0,
            1,
            2,
            63_000.5,
            0.25,
            0.1,
            "open".to_string(),
        )
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [7; 32],
            monitor_id: 91,
            fact_seq: 17,
        });
        let decoded = HyperliquidBasicOrderMsg::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
        let stable_key = decoded.stable_venue_key();
        assert_eq!(
            stable_key,
            decoded
                .clone()
                .with_fact_identity(HyperliquidFactIdentity {
                    account_hash: [7; 32],
                    monitor_id: 92,
                    fact_seq: 999,
                })
                .stable_venue_key()
        );
    }

    #[test]
    fn rejects_truncated_message() {
        assert!(HyperliquidBasicOrderMsg::from_bytes(&[0; 12]).is_err());
    }

    #[test]
    fn fill_message_roundtrips_external_identity() {
        let msg = super::HyperliquidBasicFillMsg::create(
            13,
            1_725_000_000_123,
            1_725_000_000_123,
            "BTCUSDC".to_string(),
            987,
            0,
            String::new(),
            "hl:0123456789abcdef0123456789abcdef",
            1234,
            "0xabc".to_string(),
            "market".to_string(),
            2,
            true,
            63_000.5,
            0.25,
            0.75,
            Some(2),
        )
        .with_venue_audit_fields(
            Some("BTC".to_string()),
            Some("1.250000".to_string()),
            Some("Close Long".to_string()),
            Some("12.5000".to_string()),
            Some("0.015".to_string()),
            Some("USDC".to_string()),
            Some("0.002".to_string()),
            Some(77),
            Some("0x1111111111111111111111111111111111111111".to_string()),
            Some("62999.75".to_string()),
        )
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [8; 32],
            monitor_id: 92,
            fact_seq: 18,
        });
        let decoded = super::HyperliquidBasicFillMsg::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(
            decoded.trade_id_str(),
            "hl:0123456789abcdef0123456789abcdef"
        );
        assert_eq!(decoded.last_filled_quantity, 0.25);
        assert_eq!(decoded.cumulative_filled_quantity, 0.75);
        assert_eq!(decoded.order_status, 2);
        assert_eq!(decoded.wire_coin.as_deref(), Some("BTC"));
        assert_eq!(decoded.start_position.as_deref(), Some("1.250000"));
        assert_eq!(decoded.dir.as_deref(), Some("Close Long"));
        assert_eq!(decoded.closed_pnl.as_deref(), Some("12.5000"));
        assert_eq!(decoded.fee.as_deref(), Some("0.015"));
        assert_eq!(decoded.fee_token.as_deref(), Some("USDC"));
        assert_eq!(decoded.builder_fee.as_deref(), Some("0.002"));
        assert_eq!(decoded.twap_id, Some(77));
        assert_eq!(
            decoded.liquidated_user.as_deref(),
            Some("0x1111111111111111111111111111111111111111")
        );
        assert_eq!(decoded.liquidation_mark_price.as_deref(), Some("62999.75"));

        let stable_key = decoded.stable_venue_key();
        let next_epoch = decoded.clone().with_fact_identity(HyperliquidFactIdentity {
            account_hash: decoded.account_hash,
            monitor_id: 999,
            fact_seq: 1,
        });
        assert_eq!(stable_key, next_epoch.stable_venue_key());
        let mut changed_trade = decoded.clone();
        changed_trade.venue_trade_id += 1;
        assert_ne!(stable_key, changed_trade.stable_venue_key());
    }

    #[test]
    fn funding_and_ledger_messages_roundtrip_with_epoch_independent_keys() {
        let funding = HyperliquidFundingMsg::create(
            1_725_000_000_123,
            "xyz:FOO".to_string(),
            "-0.0012300".to_string(),
            "2.500".to_string(),
            "0.0000125".to_string(),
        )
        .with_transaction_hash(Some("0xfunding".to_string()))
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [5; 32],
            monitor_id: 10,
            fact_seq: 11,
        });
        assert_eq!(
            HyperliquidFundingMsg::from_bytes(&funding.to_bytes()).unwrap(),
            funding
        );
        assert_eq!(funding.transaction_hash.as_deref(), Some("0xfunding"));
        let funding_key = funding.stable_venue_key();
        assert_eq!(
            funding_key,
            funding
                .clone()
                .with_fact_identity(HyperliquidFactIdentity {
                    account_hash: funding.account_hash,
                    monitor_id: 12,
                    fact_seq: 1,
                })
                .stable_venue_key()
        );
        let mut changed_funding = funding.clone();
        changed_funding.usdc = "-0.0012301".to_string();
        assert_ne!(funding_key, changed_funding.stable_venue_key());

        let ledger = HyperliquidLedgerMsg::create(
            1_725_000_000_456,
            "0xabc".to_string(),
            "spotTransfer".to_string(),
            r#"{"amount":"1.25","destination":"0xdef","type":"spotTransfer"}"#.to_string(),
        )
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [6; 32],
            monitor_id: 20,
            fact_seq: 21,
        });
        assert_eq!(
            HyperliquidLedgerMsg::from_bytes(&ledger.to_bytes()).unwrap(),
            ledger
        );
        let ledger_key = ledger.stable_venue_key();
        assert_eq!(
            ledger_key,
            ledger
                .clone()
                .with_fact_identity(HyperliquidFactIdentity {
                    account_hash: ledger.account_hash,
                    monitor_id: 22,
                    fact_seq: 1,
                })
                .stable_venue_key()
        );
        let mut changed_ledger = ledger.clone();
        changed_ledger.delta_json.push(' ');
        assert_ne!(ledger_key, changed_ledger.stable_venue_key());
    }

    #[test]
    fn spot_balance_message_roundtrips_exact_venue_decimals() {
        let msg = HyperliquidSpotBalanceMsg::create(
            1_725_000_000_789,
            150,
            "HYPE".to_string(),
            "12.3400".to_string(),
            "0.250".to_string(),
            "101.987600".to_string(),
        );
        assert_eq!(
            HyperliquidSpotBalanceMsg::from_bytes(&msg.to_bytes()).unwrap(),
            msg
        );
    }

    #[test]
    fn perp_dex_state_message_roundtrips_exact_venue_decimals() {
        let msg = HyperliquidPerpDexStateMsg::create(
            1_725_000_000_790,
            "xyz".to_string(),
            2,
            "100.00100".to_string(),
            "20.500".to_string(),
            "79.50100".to_string(),
            "2.0500".to_string(),
            "95.000".to_string(),
            "18.7500".to_string(),
            "76.250".to_string(),
            "1.87500".to_string(),
            "0.937500".to_string(),
            "71.234500".to_string(),
        );
        let bytes = msg.to_bytes();
        assert_eq!(HyperliquidPerpDexStateMsg::from_bytes(&bytes).unwrap(), msg);
        assert!(HyperliquidPerpDexStateMsg::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn twap_slice_association_roundtrips_with_stable_fill_identity() {
        let msg = HyperliquidTwapSliceFillMsg::create(
            13,
            1_725_000_000_123,
            "xyz:FOO".to_string(),
            "XYZFOOUSDH".to_string(),
            71,
            72,
            "0xabc".to_string(),
            73,
        )
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [5; 32],
            monitor_id: 10,
            fact_seq: 11,
        });
        let decoded = HyperliquidTwapSliceFillMsg::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(
            decoded.stable_venue_key(),
            decoded
                .clone()
                .with_fact_identity(HyperliquidFactIdentity {
                    account_hash: [5; 32],
                    monitor_id: 12,
                    fact_seq: 13,
                })
                .stable_venue_key()
        );
    }

    #[test]
    fn twap_history_roundtrips_exact_state_and_optional_trigger() {
        let msg = HyperliquidTwapHistoryMsg::create(
            1_788_587_622,
            Some(2_184_501),
            "0x1111111111111111111111111111111111111111".to_string(),
            "BTC".to_string(),
            "B".to_string(),
            "1.2500".to_string(),
            "0.500".to_string(),
            "32000.1250".to_string(),
            120,
            true,
            false,
            1_788_581_510_182,
            Some("70000.00".to_string()),
            Some("65000.50".to_string()),
            Some(true),
            "waitingForTrigger".to_string(),
            None,
        )
        .with_fact_identity(HyperliquidFactIdentity {
            account_hash: [6; 32],
            monitor_id: 14,
            fact_seq: 15,
        });
        let bytes = msg.to_bytes();
        assert_eq!(HyperliquidTwapHistoryMsg::from_bytes(&bytes).unwrap(), msg);
        assert!(HyperliquidTwapHistoryMsg::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn snapshot_complete_message_roundtrips() {
        let msg = HyperliquidSnapshotCompleteMsg::create(13, 1_725_000_000_123);
        let decoded = HyperliquidSnapshotCompleteMsg::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
        assert!(HyperliquidSnapshotCompleteMsg::from_bytes(&msg.to_bytes()[..12]).is_err());
    }

    #[test]
    fn snapshot_control_identity_roundtrips() {
        let msg = HyperliquidSnapshotCompleteMsg::create_control(
            HyperliquidSnapshotPhase::Begin,
            HyperliquidSnapshotPath::Secondary,
            12,
            77,
            5,
            9,
            1_725_000_000_123,
            1_725_000_060_123,
        )
        .with_account_hash([9; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]);
        let decoded = HyperliquidSnapshotCompleteMsg::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(decoded.phase(), Some(HyperliquidSnapshotPhase::Begin));
        assert_eq!(decoded.path(), Some(HyperliquidSnapshotPath::Secondary));
    }

    #[test]
    fn fact_replay_request_and_controls_roundtrip() {
        let request = HyperliquidFactReplayRequestMsg {
            account_hash: [3; 32],
            consumer_id: 11,
            request_id: 12,
            last_monitor_id: 13,
            last_fact_seq: 14,
        };
        assert_eq!(
            HyperliquidFactReplayRequestMsg::from_ipc_payload(&request.to_ipc_payload()).unwrap(),
            request
        );

        for phase in [
            HyperliquidFactReplayPhase::Begin,
            HyperliquidFactReplayPhase::Complete,
            HyperliquidFactReplayPhase::Gap,
        ] {
            let control =
                HyperliquidFactReplayControlMsg::create(phase, [4; 32], 21, 22, 23, 24, 25, 26);
            let decoded = HyperliquidFactReplayControlMsg::from_bytes(&control.to_bytes()).unwrap();
            assert_eq!(decoded, control);
            assert_eq!(decoded.phase(), Some(phase));
        }
    }

    #[test]
    fn account_identity_hash_normalizes_case_and_rejects_bad_addresses() {
        let lower = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
        let upper = "  0XABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD  ";
        assert_eq!(
            hyperliquid_account_identity_hash(lower, false).unwrap(),
            hyperliquid_account_identity_hash(upper, false).unwrap()
        );
        assert_ne!(
            hyperliquid_account_identity_hash(lower, false).unwrap(),
            hyperliquid_account_identity_hash("0xabcdefabcdefabcdefabcdefabcdefabcdefabce", false,)
                .unwrap()
        );
        assert_ne!(
            hyperliquid_account_identity_hash(lower, false).unwrap(),
            hyperliquid_account_identity_hash(lower, true).unwrap()
        );
        assert!(hyperliquid_account_identity_hash("0x1234", false).is_err());
    }
}
