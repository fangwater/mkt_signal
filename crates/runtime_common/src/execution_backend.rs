use crate::exchange::Exchange;
use anyhow::{bail, Context, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecBackend {
    Native,
    Ltp,
}

impl ExecBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Ltp => "ltp",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "native" | "exchange" | "direct" => Ok(Self::Native),
            "ltp" | "rapidx" | "liquidity" | "liquiditytech" => Ok(Self::Ltp),
            _ => bail!("invalid execution backend; expected native or rapidx"),
        }
    }

    pub fn resolve(exchange: Exchange, default: &str, mapping: &str) -> Result<Self> {
        let default = Self::parse(default)?;
        let mut wildcard = None;
        let mut specific = None;
        for entry in mapping.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (key, value) = entry
                .split_once('=')
                .context("backend map requires exchange=backend")?;
            let value = Self::parse(value)?;
            let key = key.trim().to_ascii_lowercase();
            if key == "*" {
                if wildcard.replace(value).is_some() {
                    bail!("duplicate wildcard backend mapping");
                }
            } else {
                if !matches!(
                    key.as_str(),
                    "binance" | "okex" | "bybit" | "bitget" | "gate" | "hyperliquid"
                ) {
                    bail!("unknown exchange in execution backend map");
                }
                if key == exchange.as_str() && specific.replace(value).is_some() {
                    bail!("duplicate exchange backend mapping");
                }
            }
        }
        let backend = specific.or(wildcard).unwrap_or(default);
        if !backend.supports_exchange(exchange) {
            bail!(
                "execution backend {} does not support {}",
                backend.as_str(),
                exchange
            );
        }
        Ok(backend)
    }

    pub fn for_exchange(exchange: Exchange) -> Result<Self> {
        Self::resolve(
            exchange,
            &std::env::var("TRADE_ENGINE_EXEC_BACKEND").unwrap_or_default(),
            &std::env::var("TRADE_ENGINE_EXEC_BACKEND_MAP").unwrap_or_default(),
        )
    }

    pub fn supports_exchange(self, exchange: Exchange) -> bool {
        self == Self::Native || matches!(exchange, Exchange::Binance | Exchange::Okex)
    }
}

pub fn rapidx_portfolio_id() -> Result<String> {
    let value = std::env::var("LTP_PORTFOLIO_ID")
        .context("LTP_PORTFOLIO_ID is required for RapidX account binding")?;
    validate_portfolio_id(&value)?;
    Ok(value)
}

pub fn validate_portfolio_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 64 || !value.bytes().all(|b| b.is_ascii_digit()) {
        bail!("RapidX portfolio ID must contain 1..64 ASCII digits");
    }
    Ok(())
}

pub fn account_stream_slug(exchange: Exchange) -> Result<String> {
    match ExecBackend::for_exchange(exchange)? {
        ExecBackend::Native => Ok(exchange.as_str().to_string()),
        ExecBackend::Ltp => Ok(format!(
            "rapidx_{}_{}",
            exchange.as_str(),
            rapidx_portfolio_id()?
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backend_configuration_fails_closed() {
        assert!(ExecBackend::resolve(Exchange::Binance, "typo", "").is_err());
        assert!(ExecBackend::resolve(Exchange::Binance, "native", "binance=typo").is_err());
        assert!(ExecBackend::resolve(Exchange::Gate, "rapidx", "").is_err());
        assert!(ExecBackend::resolve(Exchange::Binance, "native", "binnance=rapidx").is_err());
    }
    #[test]
    fn specific_source_precedes_wildcard_regardless_of_order() {
        for map in ["*=native,binance=rapidx", "binance=rapidx,*=native"] {
            assert_eq!(
                ExecBackend::resolve(Exchange::Binance, "native", map).unwrap(),
                ExecBackend::Ltp
            );
        }
        assert!(validate_portfolio_id("../account").is_err());
        assert!(validate_portfolio_id("").is_err());
        assert!(validate_portfolio_id("1702884522340000").is_ok());
    }
}
