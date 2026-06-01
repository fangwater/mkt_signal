use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::signal::common::{bytes_helper, SignalBytes};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_json::{Map, Value};

const WIRE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TargetPosMode {
    Percent = 1,
    Qty = 2,
    Amount = 3,
}

impl TargetPosMode {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Percent),
            2 => Some(Self::Qty),
            3 => Some(Self::Amount),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Percent => "percent",
            Self::Qty => "qty",
            Self::Amount => "amount",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "percent" | "percentage" | "weight" | "weights" | "ratio" => Some(Self::Percent),
            "qty" | "quantity" | "base_qty" | "base_quantity" => Some(Self::Qty),
            "amount" | "notional" | "usd" | "usdt" | "quote_amount" => Some(Self::Amount),
            _ => None,
        }
    }

    fn value_keys(self) -> &'static [&'static str] {
        match self {
            Self::Percent => &["value", "percent", "percentage", "weight", "ratio"],
            Self::Qty => &["value", "qty", "quantity", "base_qty", "base_quantity"],
            Self::Amount => &["value", "amount", "notional", "usd", "usdt", "quote_amount"],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetPosTarget {
    symbol: [u8; 32],
    value: f64,
}

impl TargetPosTarget {
    pub fn new(symbol: &str, value: f64) -> Result<Self, String> {
        let symbol = normalize_symbol_for_internal(symbol);
        if symbol.is_empty() {
            return Err("target symbol is empty".to_string());
        }
        if symbol.len() > 32 {
            return Err(format!(
                "target symbol '{}' exceeds 32 bytes after normalization",
                symbol
            ));
        }
        if !value.is_finite() {
            return Err(format!("target value for '{}' is not finite", symbol));
        }

        let mut symbol_bytes = [0u8; 32];
        symbol_bytes[..symbol.len()].copy_from_slice(symbol.as_bytes());
        Ok(Self {
            symbol: symbol_bytes,
            value,
        })
    }

    pub fn symbol(&self) -> String {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.symbol[..end]).to_string()
    }

    pub fn value(&self) -> f64 {
        self.value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetPosExecSignal {
    pub version: u8,
    pub mode: TargetPosMode,
    pub generation_time: i64,
    pub targets: Vec<TargetPosTarget>,
}

impl TargetPosExecSignal {
    pub fn new(
        mode: TargetPosMode,
        generation_time: i64,
        targets: Vec<TargetPosTarget>,
    ) -> Result<Self, String> {
        if targets.is_empty() {
            return Err("target position signal must contain at least one target".to_string());
        }
        Ok(Self {
            version: WIRE_VERSION,
            mode,
            generation_time,
            targets,
        })
    }

    pub fn from_json_str(input: &str, generation_time: i64) -> Result<Self, String> {
        let value: Value = serde_json::from_str(input)
            .map_err(|err| format!("failed to parse target position json: {err}"))?;
        Self::from_json_value(&value, generation_time)
    }

    pub fn from_json_value(value: &Value, generation_time: i64) -> Result<Self, String> {
        let obj = value
            .as_object()
            .ok_or_else(|| "target position json root must be an object".to_string())?;

        if let Some(mode_value) = obj.get("mode").or_else(|| obj.get("type")) {
            let mode = parse_mode_value(mode_value)?;
            let targets_value = obj
                .get("targets")
                .or_else(|| obj.get("positions"))
                .ok_or_else(|| "target position json missing targets/positions".to_string())?;
            let targets = parse_targets(mode, targets_value)?;
            return Self::new(mode, generation_time, targets);
        }
        Err("target position json must contain mode/type + targets/positions".to_string())
    }

    pub fn to_json_value(&self) -> Value {
        let targets = self
            .targets
            .iter()
            .map(|target| {
                serde_json::json!({
                    "symbol": target.symbol(),
                    "value": target.value(),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "version": self.version,
            "mode": self.mode.as_str(),
            "generation_time": self.generation_time,
            "targets": targets,
        })
    }
}

impl SignalBytes for TargetPosExecSignal {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.version);
        buf.put_u8(self.mode as u8);
        buf.put_i64_le(self.generation_time);
        buf.put_u32_le(self.targets.len() as u32);
        for target in &self.targets {
            bytes_helper::write_fixed_bytes(&mut buf, &target.symbol);
            buf.put_f64_le(target.value);
        }
        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 14 {
            return Err("not enough bytes for TargetPosExecSignal header".to_string());
        }

        let version = bytes.get_u8();
        if version != WIRE_VERSION {
            return Err(format!(
                "unsupported TargetPosExecSignal version: {version}"
            ));
        }
        let mode_u8 = bytes.get_u8();
        let mode = TargetPosMode::from_u8(mode_u8)
            .ok_or_else(|| format!("unknown TargetPosMode: {mode_u8}"))?;
        let generation_time = bytes.get_i64_le();
        let count = bytes.get_u32_le() as usize;
        let mut targets = Vec::with_capacity(count);
        for _ in 0..count {
            let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
            if bytes.remaining() < 8 {
                return Err("not enough bytes for target value".to_string());
            }
            let value = bytes.get_f64_le();
            let target = TargetPosTarget { symbol, value };
            if target.symbol().is_empty() {
                return Err("target symbol is empty".to_string());
            }
            if !target.value.is_finite() {
                return Err(format!(
                    "target value for '{}' is not finite",
                    target.symbol()
                ));
            }
            targets.push(target);
        }
        if bytes.iter().any(|byte| *byte != 0) {
            return Err("unexpected non-zero trailing bytes for TargetPosExecSignal".to_string());
        }
        Self::new(mode, generation_time, targets)
    }
}

fn parse_mode_value(value: &Value) -> Result<TargetPosMode, String> {
    let raw = value
        .as_str()
        .ok_or_else(|| "mode/type must be a string".to_string())?;
    TargetPosMode::parse(raw).ok_or_else(|| format!("unknown target position mode: {raw}"))
}

fn parse_targets(mode: TargetPosMode, value: &Value) -> Result<Vec<TargetPosTarget>, String> {
    match value {
        Value::Object(map) => parse_targets_map(map),
        Value::Array(items) => parse_targets_array(mode, items),
        _ => Err("targets/positions must be an object map or array".to_string()),
    }
}

fn parse_targets_map(map: &Map<String, Value>) -> Result<Vec<TargetPosTarget>, String> {
    let mut targets = Vec::with_capacity(map.len());
    for (symbol, value) in map {
        let number = value
            .as_f64()
            .ok_or_else(|| format!("target value for '{}' must be a number", symbol))?;
        targets.push(TargetPosTarget::new(symbol, number)?);
    }
    check_duplicate_symbols(&targets)?;
    Ok(targets)
}

fn parse_targets_array(
    mode: TargetPosMode,
    items: &[Value],
) -> Result<Vec<TargetPosTarget>, String> {
    let mut targets = Vec::with_capacity(items.len());
    for item in items {
        let obj = item
            .as_object()
            .ok_or_else(|| "target array item must be an object".to_string())?;
        let symbol = obj
            .get("symbol")
            .and_then(Value::as_str)
            .ok_or_else(|| "target array item missing string symbol".to_string())?;
        let value = mode
            .value_keys()
            .iter()
            .find_map(|key| obj.get(*key).and_then(Value::as_f64))
            .ok_or_else(|| {
                format!(
                    "target array item for '{}' missing numeric value for mode {}",
                    symbol,
                    mode.as_str()
                )
            })?;
        targets.push(TargetPosTarget::new(symbol, value)?);
    }
    check_duplicate_symbols(&targets)?;
    Ok(targets)
}

fn check_duplicate_symbols(targets: &[TargetPosTarget]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for target in targets {
        let symbol = target.symbol();
        if !seen.insert(symbol.clone()) {
            return Err(format!(
                "duplicate target symbol after normalization: {symbol}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find_value(signal: &TargetPosExecSignal, symbol: &str) -> f64 {
        signal
            .targets
            .iter()
            .find(|target| target.symbol() == symbol)
            .map(TargetPosTarget::value)
            .unwrap()
    }

    #[test]
    fn parses_canonical_percent_map() {
        let signal = TargetPosExecSignal::from_json_str(
            r#"{"mode":"percent","targets":{"BTCUSDT":0.5,"eth-usdt-swap":-0.25}}"#,
            123,
        )
        .unwrap();
        assert_eq!(signal.mode, TargetPosMode::Percent);
        assert_eq!(signal.generation_time, 123);
        assert_eq!(find_value(&signal, "BTCUSDT"), 0.5);
        assert_eq!(find_value(&signal, "ETHUSDT"), -0.25);
    }
    #[test]
    fn rejects_top_level_mode_shorthand() {
        let err = TargetPosExecSignal::from_json_str(
            r#"{"percent":{"BTCUSDT":0.5,"ETHUSDT":-0.25}}"#,
            456,
        )
        .unwrap_err();
        assert!(err.contains("mode/type + targets/positions"));
    }

    #[test]
    fn parses_amount_array() {
        let signal = TargetPosExecSignal::from_json_str(
            r#"{"type":"amount","positions":[{"symbol":"BTCUSDT","amount":1000},{"symbol":"ETHUSDT","notional":-500}]}"#,
            789,
        )
        .unwrap();
        assert_eq!(signal.mode, TargetPosMode::Amount);
        assert_eq!(find_value(&signal, "BTCUSDT"), 1000.0);
        assert_eq!(find_value(&signal, "ETHUSDT"), -500.0);
    }

    #[test]
    fn rejects_duplicate_normalized_symbols() {
        let err = TargetPosExecSignal::from_json_str(
            r#"{"mode":"qty","positions":[{"symbol":"BTC-USDT","qty":1},{"symbol":"BTCUSDT","qty":2}]}"#,
            1,
        )
        .unwrap_err();
        assert!(err.contains("duplicate target symbol"));
    }

    #[test]
    fn binary_roundtrip() {
        let signal = TargetPosExecSignal::from_json_str(
            r#"{"mode":"amount","targets":{"BTCUSDT":1000,"ETHUSDT":500}}"#,
            42,
        )
        .unwrap();
        let parsed = TargetPosExecSignal::from_bytes(signal.to_bytes()).unwrap();
        assert_eq!(parsed, signal);
    }

    #[test]
    fn binary_roundtrip_with_ipc_padding() {
        let signal =
            TargetPosExecSignal::from_json_str(r#"{"mode":"qty","targets":{"BTCUSDT":1}}"#, 42)
                .unwrap();
        let payload = signal.to_bytes();
        let mut padded = vec![0u8; 4096];
        padded[..payload.len()].copy_from_slice(&payload);
        let parsed = TargetPosExecSignal::from_bytes(Bytes::from(padded)).unwrap();
        assert_eq!(parsed, signal);
    }
}
