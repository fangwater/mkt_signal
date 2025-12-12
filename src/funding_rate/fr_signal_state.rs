//! Funding rate signal state snapshot for visualization.
//!
//! This module defines a binary-serializable entry that corresponds to one row
//! of the old "three-line table" printed by fr_signal, plus extra spread details.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::funding_rate::common::CompareOp;
use crate::funding_rate::spread_factor::SpreadType;
use crate::signal::common::bytes_helper;

/// Unified tag for FR/Spread/Final signal columns.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalTag {
    None = 0,
    FwdOpen = 1,
    FwdClose = 2,
    BwdOpen = 3,
    BwdClose = 4,
    FwdCancel = 5,
    BwdCancel = 6,
}

impl SignalTag {
    pub fn from_label(label: &str) -> Self {
        match label {
            "FwdOpen" => SignalTag::FwdOpen,
            "FwdClose" => SignalTag::FwdClose,
            "BwdOpen" => SignalTag::BwdOpen,
            "BwdClose" => SignalTag::BwdClose,
            "FwdCancel" => SignalTag::FwdCancel,
            "BwdCancel" => SignalTag::BwdCancel,
            _ => SignalTag::None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOpTag {
    Unknown = 0,
    GreaterThan = 1,
    LessThan = 2,
}

impl From<CompareOp> for CompareOpTag {
    fn from(value: CompareOp) -> Self {
        match value {
            CompareOp::GreaterThan => CompareOpTag::GreaterThan,
            CompareOp::LessThan => CompareOpTag::LessThan,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadTypeTag {
    Unknown = 0,
    BidAsk = 1,
    AskBid = 2,
    SpreadRate = 3,
}

impl From<SpreadType> for SpreadTypeTag {
    fn from(value: SpreadType) -> Self {
        match value {
            SpreadType::BidAsk => SpreadTypeTag::BidAsk,
            SpreadType::AskBid => SpreadTypeTag::AskBid,
            SpreadType::SpreadRate => SpreadTypeTag::SpreadRate,
        }
    }
}

/// One snapshot entry for a symbol.
///
/// Percentage columns are stored as "pct" already (e.g. 0.0005 -> 0.05).
/// Spread values/thresholds are stored as raw rates (not multiplied by 100).
#[derive(Debug, Clone)]
pub struct FrSignalStateEntry {
    pub symbol: [u8; 32],

    pub pred_fr_pct: f64,
    pub fr_ma_pct: f64,
    pub pred_loan_pct: f64,
    pub cur_loan_pct: f64,
    pub fr_plus_pred_loan_pct: f64,
    pub ma_plus_cur_loan_pct: f64,

    pub fr_sig: SignalTag,

    pub spread_sig: SignalTag,
    /// Raw spread values ("上下价差值" + spread_rate)
    pub spread_bidask: f64,
    pub spread_askbid: f64,
    pub spread_rate: f64,
    /// Spread value used for current spread_sig (NaN if none)
    pub spread_used_value: f64,
    /// Threshold used for current spread_sig (NaN if none)
    pub spread_threshold: f64,
    pub spread_compare_op: CompareOpTag,
    pub spread_type: SpreadTypeTag,

    pub final_sig: SignalTag,
}

impl FrSignalStateEntry {
    pub fn new(
        symbol: &str,
        pred_fr_pct: f64,
        fr_ma_pct: f64,
        pred_loan_pct: f64,
        cur_loan_pct: f64,
        fr_plus_pred_loan_pct: f64,
        ma_plus_cur_loan_pct: f64,
        fr_sig: SignalTag,
        spread_sig: SignalTag,
        spread_bidask: f64,
        spread_askbid: f64,
        spread_rate: f64,
        spread_used_value: f64,
        spread_threshold: f64,
        spread_compare_op: CompareOpTag,
        spread_type: SpreadTypeTag,
        final_sig: SignalTag,
    ) -> Self {
        let mut symbol_bytes = [0u8; 32];
        let b = symbol.as_bytes();
        let n = b.len().min(32);
        symbol_bytes[..n].copy_from_slice(&b[..n]);
        Self {
            symbol: symbol_bytes,
            pred_fr_pct,
            fr_ma_pct,
            pred_loan_pct,
            cur_loan_pct,
            fr_plus_pred_loan_pct,
            ma_plus_cur_loan_pct,
            fr_sig,
            spread_sig,
            spread_bidask,
            spread_askbid,
            spread_rate,
            spread_used_value,
            spread_threshold,
            spread_compare_op,
            spread_type,
            final_sig,
        }
    }

    fn write_to_buf(&self, buf: &mut BytesMut) {
        bytes_helper::write_fixed_bytes(buf, &self.symbol);
        buf.put_f64_le(self.pred_fr_pct);
        buf.put_f64_le(self.fr_ma_pct);
        buf.put_f64_le(self.pred_loan_pct);
        buf.put_f64_le(self.cur_loan_pct);
        buf.put_f64_le(self.fr_plus_pred_loan_pct);
        buf.put_f64_le(self.ma_plus_cur_loan_pct);
        buf.put_u8(self.fr_sig as u8);
        buf.put_u8(self.spread_sig as u8);
        buf.put_f64_le(self.spread_bidask);
        buf.put_f64_le(self.spread_askbid);
        buf.put_f64_le(self.spread_rate);
        buf.put_f64_le(self.spread_used_value);
        buf.put_f64_le(self.spread_threshold);
        buf.put_u8(self.spread_compare_op as u8);
        buf.put_u8(self.spread_type as u8);
        buf.put_u8(self.final_sig as u8);
    }

    fn read_from_bytes(bytes: &mut Bytes) -> Result<Self, String> {
        let symbol = bytes_helper::read_fixed_bytes(bytes)?;
        if bytes.remaining() < 8 * 6 + 2 + 8 * 5 + 3 {
            return Err("insufficient bytes for FrSignalStateEntry".into());
        }
        let pred_fr_pct = bytes.get_f64_le();
        let fr_ma_pct = bytes.get_f64_le();
        let pred_loan_pct = bytes.get_f64_le();
        let cur_loan_pct = bytes.get_f64_le();
        let fr_plus_pred_loan_pct = bytes.get_f64_le();
        let ma_plus_cur_loan_pct = bytes.get_f64_le();
        let fr_sig = match bytes.get_u8() {
            1 => SignalTag::FwdOpen,
            2 => SignalTag::FwdClose,
            3 => SignalTag::BwdOpen,
            4 => SignalTag::BwdClose,
            5 => SignalTag::FwdCancel,
            6 => SignalTag::BwdCancel,
            _ => SignalTag::None,
        };
        let spread_sig = match bytes.get_u8() {
            1 => SignalTag::FwdOpen,
            2 => SignalTag::FwdClose,
            3 => SignalTag::BwdOpen,
            4 => SignalTag::BwdClose,
            5 => SignalTag::FwdCancel,
            6 => SignalTag::BwdCancel,
            _ => SignalTag::None,
        };
        let spread_bidask = bytes.get_f64_le();
        let spread_askbid = bytes.get_f64_le();
        let spread_rate = bytes.get_f64_le();
        let spread_used_value = bytes.get_f64_le();
        let spread_threshold = bytes.get_f64_le();
        let spread_compare_op = match bytes.get_u8() {
            1 => CompareOpTag::GreaterThan,
            2 => CompareOpTag::LessThan,
            _ => CompareOpTag::Unknown,
        };
        let spread_type = match bytes.get_u8() {
            1 => SpreadTypeTag::BidAsk,
            2 => SpreadTypeTag::AskBid,
            3 => SpreadTypeTag::SpreadRate,
            _ => SpreadTypeTag::Unknown,
        };
        let final_sig = match bytes.get_u8() {
            1 => SignalTag::FwdOpen,
            2 => SignalTag::FwdClose,
            3 => SignalTag::BwdOpen,
            4 => SignalTag::BwdClose,
            5 => SignalTag::FwdCancel,
            6 => SignalTag::BwdCancel,
            _ => SignalTag::None,
        };
        Ok(Self {
            symbol,
            pred_fr_pct,
            fr_ma_pct,
            pred_loan_pct,
            cur_loan_pct,
            fr_plus_pred_loan_pct,
            ma_plus_cur_loan_pct,
            fr_sig,
            spread_sig,
            spread_bidask,
            spread_askbid,
            spread_rate,
            spread_used_value,
            spread_threshold,
            spread_compare_op,
            spread_type,
            final_sig,
        })
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to_buf(&mut buf);
        buf.freeze()
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: Bytes) -> Result<Self, String> {
        let mut bytes = data;
        Self::read_from_bytes(&mut bytes)
    }
}

/// A batch of entries published periodically.
#[derive(Debug, Clone)]
pub struct FrSignalStateBatch {
    pub ts_us: i64,
    pub entries: Vec<FrSignalStateEntry>,
}

impl FrSignalStateBatch {
    pub fn new(ts_us: i64, entries: Vec<FrSignalStateEntry>) -> Self {
        Self { ts_us, entries }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i64_le(self.ts_us);
        let count: u16 = self.entries.len().min(u16::MAX as usize) as u16;
        buf.put_u16_le(count);
        for e in self.entries.iter().take(count as usize) {
            e.write_to_buf(&mut buf);
        }
        buf.freeze()
    }

    pub fn from_bytes(data: Bytes) -> Result<Self, String> {
        let mut bytes = data;
        if bytes.remaining() < 10 {
            return Err("insufficient bytes for FrSignalStateBatch header".into());
        }
        let ts_us = bytes.get_i64_le();
        let count = bytes.get_u16_le() as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(FrSignalStateEntry::read_from_bytes(&mut bytes)?);
        }
        Ok(Self { ts_us, entries })
    }
}

/// Default IceOryx channel name for publishing signal state.
pub const DEFAULT_STATE_CHANNEL: &str = "fr_signal_state";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_batch_roundtrip_bytes() {
        let e = FrSignalStateEntry::new(
            "BTC-USDT-SWAP",
            0.12,
            0.34,
            0.56,
            0.78,
            0.90,
            1.23,
            SignalTag::FwdOpen,
            SignalTag::BwdCancel,
            0.0001,
            0.0002,
            0.0003,
            0.0002,
            0.00025,
            CompareOpTag::LessThan,
            SpreadTypeTag::BidAsk,
            SignalTag::FwdOpen,
        );
        let batch = FrSignalStateBatch::new(123456789, vec![e.clone()]);
        let bytes = batch.to_bytes();
        let back = FrSignalStateBatch::from_bytes(bytes).expect("roundtrip ok");
        assert_eq!(back.ts_us, 123456789);
        assert_eq!(back.entries.len(), 1);
        let b = &back.entries[0];
        assert_eq!(
            String::from_utf8_lossy(&b.symbol).trim_end_matches('\0'),
            "BTC-USDT-SWAP"
        );
        assert!((b.pred_fr_pct - 0.12).abs() < 1e-12);
        assert!((b.fr_ma_pct - 0.34).abs() < 1e-12);
        assert!((b.pred_loan_pct - 0.56).abs() < 1e-12);
        assert!((b.cur_loan_pct - 0.78).abs() < 1e-12);
        assert_eq!(b.fr_sig, SignalTag::FwdOpen);
        assert_eq!(b.spread_sig, SignalTag::BwdCancel);
        assert_eq!(b.spread_compare_op, CompareOpTag::LessThan);
        assert_eq!(b.spread_type, SpreadTypeTag::BidAsk);
        assert_eq!(b.final_sig, SignalTag::FwdOpen);
    }
}
