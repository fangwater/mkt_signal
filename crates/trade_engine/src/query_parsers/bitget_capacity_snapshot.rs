use bytes::Bytes;
use serde::Deserialize;

const WIRE_MAGIC: [u8; 4] = *b"BGC1";
const WIRE_VERSION: u8 = 1;
const WIRE_LEN: usize = 32;
const FLAG_AVAILABLE: u8 = 1 << 0;
const FLAG_MAX_BORROWABLE: u8 = 1 << 1;
const FLAG_MARGIN_RATIO: u8 = 1 << 2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BitgetCapacitySnapshotMsg {
    flags: u8,
    available: f64,
    max_borrowable: f64,
    margin_ratio: f64,
}

impl BitgetCapacitySnapshotMsg {
    pub fn available(available: f64, margin_ratio: f64) -> Self {
        Self {
            flags: FLAG_AVAILABLE | FLAG_MARGIN_RATIO,
            available,
            max_borrowable: 0.0,
            margin_ratio,
        }
    }

    pub fn max_borrowable(max_borrowable: f64) -> Self {
        Self {
            flags: FLAG_MAX_BORROWABLE,
            available: 0.0,
            max_borrowable,
            margin_ratio: 0.0,
        }
    }

    pub fn available_value(self) -> Option<f64> {
        (self.flags & FLAG_AVAILABLE != 0).then_some(self.available)
    }

    pub fn max_borrowable_value(self) -> Option<f64> {
        (self.flags & FLAG_MAX_BORROWABLE != 0).then_some(self.max_borrowable)
    }

    pub fn margin_ratio_value(self) -> Option<f64> {
        (self.flags & FLAG_MARGIN_RATIO != 0).then_some(self.margin_ratio)
    }

    pub fn to_bytes(self) -> Bytes {
        let mut out = [0u8; WIRE_LEN];
        out[..4].copy_from_slice(&WIRE_MAGIC);
        out[4] = WIRE_VERSION;
        out[5] = self.flags;
        out[8..16].copy_from_slice(&self.available.to_le_bytes());
        out[16..24].copy_from_slice(&self.max_borrowable.to_le_bytes());
        out[24..32].copy_from_slice(&self.margin_ratio.to_le_bytes());
        Bytes::copy_from_slice(&out)
    }

    pub fn from_bytes(bytes: &Bytes) -> Option<Self> {
        let bytes = bytes.as_ref();
        if bytes.len() < WIRE_LEN || bytes[..4] != WIRE_MAGIC || bytes[4] != WIRE_VERSION {
            return None;
        }
        let flags = bytes[5];
        if flags & !(FLAG_AVAILABLE | FLAG_MAX_BORROWABLE | FLAG_MARGIN_RATIO) != 0 {
            return None;
        }
        Some(Self {
            flags,
            available: f64::from_le_bytes(bytes[8..16].try_into().ok()?),
            max_borrowable: f64::from_le_bytes(bytes[16..24].try_into().ok()?),
            margin_ratio: f64::from_le_bytes(bytes[24..32].try_into().ok()?),
        })
    }
}

#[derive(Debug, Deserialize)]
struct AssetsResponse {
    #[serde(default)]
    code: String,
    data: Option<AssetsData>,
}

#[derive(Debug, Deserialize)]
struct AssetsData {
    #[serde(default, rename = "accountEquity")]
    account_equity: String,
    #[serde(default, rename = "totalEquity")]
    total_equity: String,
    #[serde(default, rename = "usdtEquity")]
    usdt_equity: String,
    #[serde(default, rename = "effEquity")]
    effective_equity: String,
    #[serde(default)]
    mmr: String,
    #[serde(default)]
    assets: Vec<AssetRow>,
}

#[derive(Debug, Deserialize)]
struct AssetRow {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    available: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    equity: String,
}

#[derive(Debug, Deserialize)]
struct MaxTransferResponse {
    #[serde(default)]
    code: String,
    data: Option<MaxTransferData>,
}

#[derive(Debug, Deserialize)]
struct MaxTransferData {
    #[serde(default, rename = "borrowMaxTransfer")]
    borrow_max_transfer: String,
}

fn response_ok(code: &str) -> bool {
    code == "00000" || code == "0"
}

fn parse_f64(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        value.parse::<f64>().ok()
    }
}

pub fn parse_bitget_usdt_available_snapshot(json: &str) -> Option<BitgetCapacitySnapshotMsg> {
    let response: AssetsResponse = serde_json::from_str(json).ok()?;
    if !response_ok(&response.code) {
        return None;
    }
    let data = response.data?;
    let usdt = data
        .assets
        .iter()
        .find(|row| row.coin.eq_ignore_ascii_case("USDT"))?;
    let available = parse_f64(&usdt.available)
        .or_else(|| parse_f64(&usdt.balance))
        .or_else(|| parse_f64(&usdt.equity))?;
    let actual_equity = parse_f64(&data.total_equity)
        .or_else(|| parse_f64(&data.account_equity))
        .or_else(|| parse_f64(&data.usdt_equity))
        .unwrap_or(0.0);
    let effective_equity = parse_f64(&data.effective_equity).unwrap_or(actual_equity);
    let maintenance_margin = parse_f64(&data.mmr).unwrap_or(0.0);
    let margin_ratio = if maintenance_margin.abs() > f64::EPSILON {
        effective_equity / maintenance_margin
    } else {
        0.0
    };
    Some(BitgetCapacitySnapshotMsg::available(
        available,
        margin_ratio,
    ))
}

pub fn parse_bitget_usdt_max_transferable(json: &str) -> Option<BitgetCapacitySnapshotMsg> {
    let response: MaxTransferResponse = serde_json::from_str(json).ok()?;
    if !response_ok(&response.code) {
        return None;
    }
    let max_borrowable = parse_f64(&response.data?.borrow_max_transfer)?;
    Some(BitgetCapacitySnapshotMsg::max_borrowable(max_borrowable))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_roundtrips_available_snapshot() {
        let json = r#"{"code":"00000","data":{"totalEquity":"10000","effEquity":"9000","mmr":"300","assets":[{"coin":"BTC","available":"1"},{"coin":"USDT","available":"1234.5"}]}}"#;
        let parsed = parse_bitget_usdt_available_snapshot(json).expect("parse available");
        assert_eq!(parsed.available_value(), Some(1234.5));
        assert_eq!(parsed.max_borrowable_value(), None);
        assert_eq!(parsed.margin_ratio_value(), Some(30.0));
        assert_eq!(
            BitgetCapacitySnapshotMsg::from_bytes(&parsed.to_bytes()),
            Some(parsed)
        );
    }

    #[test]
    fn parses_and_roundtrips_max_transferable() {
        let json = r#"{"code":"00000","data":{"coin":"USDT","borrowMaxTransfer":"987.25"}}"#;
        let parsed = parse_bitget_usdt_max_transferable(json).expect("parse max transferable");
        assert_eq!(parsed.available_value(), None);
        assert_eq!(parsed.max_borrowable_value(), Some(987.25));
        assert_eq!(parsed.margin_ratio_value(), None);
        assert_eq!(
            BitgetCapacitySnapshotMsg::from_bytes(&parsed.to_bytes()),
            Some(parsed)
        );
    }
}
