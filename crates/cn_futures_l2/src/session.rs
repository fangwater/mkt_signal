//! Shared output-window classification for domestic futures.
//!
//! Replay staging writes every source row. Parquet exporters call this module
//! to exclude opening auctions and the known post-close tail of equity-index
//! futures. Other closing times remain source-driven until product calendars
//! are available.

use chrono::{NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Asia::Shanghai;
use chrono_tz::Tz;

use crate::universe::product_id;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exchange {
    Ccfx,
    Xdce,
    Xgfe,
    Xsge,
    Xsie,
    Xzce,
}

impl Exchange {
    pub fn parse(code: &str) -> Option<Self> {
        match code.trim().to_ascii_lowercase().as_str() {
            "ccfx" => Some(Self::Ccfx),
            "xdce" => Some(Self::Xdce),
            "xgfe" => Some(Self::Xgfe),
            "xsge" => Some(Self::Xsge),
            "xsie" => Some(Self::Xsie),
            "xzce" => Some(Self::Xzce),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ccfx => "ccfx",
            Self::Xdce => "xdce",
            Self::Xgfe => "xgfe",
            Self::Xsge => "xsge",
            Self::Xsie => "xsie",
            Self::Xzce => "xzce",
        }
    }

    pub fn msg_stem(self) -> &'static str {
        match self {
            Self::Ccfx => "mdl_21_1_0.csv",
            Self::Xdce => "mdl_24_1_0.csv",
            Self::Xgfe => "mdl_26_1_0.csv",
            Self::Xsge => "mdl_22_1_0.csv",
            Self::Xsie => "mdl_22_3_0.csv",
            Self::Xzce => "mdl_23_1_0.csv",
        }
    }

    pub fn has_order_queue(self) -> bool {
        matches!(self, Self::Xdce | Self::Xgfe)
    }

    pub fn msg_queue_stem(self) -> Option<&'static str> {
        match self {
            Self::Xdce => Some("mdl_24_7_0.csv"),
            Self::Xgfe => Some("mdl_26_7_0.csv"),
            _ => None,
        }
    }

    pub fn comm_l2_queue_prefix(self) -> Option<&'static str> {
        match self {
            Self::Xdce => Some("futorderq_xdcel2"),
            Self::Xgfe => Some("futorderq_xgfel2"),
            _ => None,
        }
    }
}

fn t(hour: u32, minute: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, minute, 0).expect("valid session clock")
}

fn in_window(clock: NaiveTime, start: NaiveTime, end: NaiveTime) -> bool {
    if start < end {
        start <= clock && clock < end
    } else {
        clock >= start || clock < end
    }
}

pub fn is_auction(clock: NaiveTime, exchange: Exchange) -> bool {
    match exchange {
        Exchange::Ccfx => in_window(clock, t(9, 25), t(9, 30)),
        _ => in_window(clock, t(8, 55), t(9, 0)) || in_window(clock, t(20, 55), t(21, 0)),
    }
}

pub fn shanghai(ts_sec: i64) -> chrono::DateTime<Tz> {
    Utc.timestamp_opt(ts_sec, 0)
        .single()
        .expect("unix second")
        .with_timezone(&Shanghai)
}

pub fn is_auction_ts(ts_sec: i64, exchange: Exchange) -> bool {
    is_auction(shanghai(ts_sec).time(), exchange)
}

fn product_key(contract_id: &str) -> String {
    product_id(contract_id).unwrap_or_else(|| contract_id.trim().to_ascii_uppercase())
}

pub fn is_equity_index_product(contract_id: &str) -> bool {
    matches!(product_key(contract_id).as_str(), "IC" | "IF" | "IH" | "IM")
}

/// Equity-index continuous trading ends at 15:00 Shanghai. The complete
/// 15:00 minute is retained; 15:01 onward is excluded. This deliberately does
/// not apply to CFFEX treasury futures, whose day session continues to 15:15.
pub fn after_equity_index_close(ts_sec: i64, contract_id: &str) -> bool {
    if !is_equity_index_product(contract_id) {
        return false;
    }
    let local = shanghai(ts_sec);
    local.hour() > 15 || (local.hour() == 15 && local.minute() >= 1)
}

/// Return whether derived parquet outputs must omit this timestamp. This is
/// intentionally narrower than a complete session calendar.
pub fn skip_output(ts_sec: i64, exchange: Exchange, contract_id: &str) -> bool {
    is_auction_ts(ts_sec, exchange) || after_equity_index_close(ts_sec, contract_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(hour: u32, minute: u32, second: u32) -> i64 {
        Shanghai
            .with_ymd_and_hms(2024, 1, 2, hour, minute, second)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn opening_auction_windows_are_exchange_specific() {
        assert!(is_auction(t(8, 59), Exchange::Xsge));
        assert!(is_auction(t(20, 59), Exchange::Xsge));
        assert!(!is_auction(t(9, 0), Exchange::Xsge));
        assert!(!is_auction(t(21, 0), Exchange::Xsge));
        assert!(is_auction(t(9, 29), Exchange::Ccfx));
        assert!(!is_auction(t(9, 30), Exchange::Ccfx));
    }

    #[test]
    fn only_equity_indexes_drop_after_the_retained_1500_minute() {
        for product in ["IF", "IH2409", "IC2412", "IM2503"] {
            assert!(!after_equity_index_close(sec(15, 0, 0), product));
            assert!(!after_equity_index_close(sec(15, 0, 59), product));
            assert!(after_equity_index_close(sec(15, 1, 0), product));
        }
        for product in ["T", "TF2409", "TL2412", "TS2503", "rb2410"] {
            assert!(!after_equity_index_close(sec(15, 1, 0), product));
            assert!(!after_equity_index_close(sec(15, 15, 0), product));
        }
    }

    #[test]
    fn output_filter_combines_auction_and_equity_index_close() {
        assert!(skip_output(sec(9, 29, 0), Exchange::Ccfx, "IF2409"));
        assert!(!skip_output(sec(15, 0, 59), Exchange::Ccfx, "IF2409"));
        assert!(skip_output(sec(15, 1, 0), Exchange::Ccfx, "IF2409"));
        assert!(!skip_output(sec(15, 1, 0), Exchange::Ccfx, "T2409"));
    }
}
