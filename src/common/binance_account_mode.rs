use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceAccountMode {
    Unified,
    Standard,
}

impl BinanceAccountMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unified => "UNIFIED",
            Self::Standard => "STANDARD",
        }
    }
}

static BINANCE_ACCOUNT_MODE: OnceLock<BinanceAccountMode> = OnceLock::new();

fn parse_binance_account_mode(context: &str) -> BinanceAccountMode {
    match std::env::var("BINANCE_ACCOUNT_MODE") {
        Ok(raw) => {
            let v = raw.trim();
            if v == "STANDARD" {
                BinanceAccountMode::Standard
            } else if v == "UNIFIED" {
                BinanceAccountMode::Unified
            } else {
                panic!(
                    "BINANCE_ACCOUNT_MODE must be 'UNIFIED' or 'STANDARD'{}; got '{}'",
                    format_context(context),
                    v
                );
            }
        }
        Err(_) => {
            panic!(
                "BINANCE_ACCOUNT_MODE must be set to 'UNIFIED' or 'STANDARD'{}",
                format_context(context)
            );
        }
    }
}

fn format_context(context: &str) -> String {
    if context.is_empty() {
        String::new()
    } else {
        format!(" (context={})", context)
    }
}

pub fn init_binance_account_mode(context: &str) -> BinanceAccountMode {
    let mode = parse_binance_account_mode(context);
    if let Err(existing) = BINANCE_ACCOUNT_MODE.set(mode) {
        if existing != mode {
            panic!(
                "BINANCE_ACCOUNT_MODE already initialized as '{}' but tried to set '{}'{}",
                existing.as_str(),
                mode.as_str(),
                format_context(context)
            );
        }
    }
    mode
}

pub fn binance_account_mode() -> BinanceAccountMode {
    if let Some(mode) = BINANCE_ACCOUNT_MODE.get() {
        *mode
    } else {
        init_binance_account_mode("binance_account_mode()")
    }
}
