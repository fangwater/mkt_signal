//! Exchange-default continuous sessions. Parser does not filter with these.
//!
//! Mirrors `cn_futures.session`. Product-specific calendars are not loaded here.
//! Keep the clocks for a later reader; `process_instrument` writes every source row.

use chrono::NaiveTime;

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

pub fn continuous_segment(clock: NaiveTime, exchange: Exchange) -> Option<&'static str> {
    if is_auction(clock, exchange) {
        return None;
    }
    match exchange {
        Exchange::Ccfx => {
            if in_window(clock, t(9, 30), t(11, 30)) {
                Some("day_am")
            } else if in_window(clock, t(13, 0), t(15, 0)) {
                Some("day_pm")
            } else {
                None
            }
        }
        _ => {
            if in_window(clock, t(21, 0), t(2, 30)) {
                Some("night")
            } else if in_window(clock, t(9, 0), t(10, 15)) {
                Some("day_am1")
            } else if in_window(clock, t(10, 30), t(11, 30)) {
                Some("day_am2")
            } else if in_window(clock, t(13, 30), t(15, 0)) {
                Some("day_pm")
            } else {
                None
            }
        }
    }
}

pub fn skip_output(clock: NaiveTime, exchange: Exchange) -> bool {
    continuous_segment(clock, exchange).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commodity_night_wraps_midnight() {
        let exchange = Exchange::Xsge;
        assert_eq!(continuous_segment(t(21, 0), exchange), Some("night"));
        assert_eq!(continuous_segment(t(0, 0), exchange), Some("night"));
        assert_eq!(continuous_segment(t(2, 29), exchange), Some("night"));
        assert_eq!(continuous_segment(t(2, 30), exchange), None);
        assert!(is_auction(t(20, 59), exchange));
        assert!(!is_auction(t(21, 0), exchange));
        assert!(skip_output(t(15, 0), exchange));
        assert_eq!(continuous_segment(t(9, 0), exchange), Some("day_am1"));
    }

    #[test]
    fn ccfx_has_no_night_and_later_open() {
        assert_eq!(continuous_segment(t(9, 30), Exchange::Ccfx), Some("day_am"));
        assert_eq!(continuous_segment(t(9, 0), Exchange::Ccfx), None);
        assert!(is_auction(t(9, 25), Exchange::Ccfx));
        assert_eq!(continuous_segment(t(21, 0), Exchange::Ccfx), None);
    }
}
