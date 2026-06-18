use log::{info, warn};
use runtime_common::exchange::Exchange;

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

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "native" | "exchange" | "direct" => Some(Self::Native),
            "ltp" | "rapidx" | "liquidity" | "liquiditytech" => Some(Self::Ltp),
            _ => None,
        }
    }

    pub fn for_exchange(exchange: Exchange) -> Self {
        let default = std::env::var("TRADE_ENGINE_EXEC_BACKEND")
            .ok()
            .and_then(|raw| {
                let parsed = Self::parse(&raw);
                if parsed.is_none() {
                    warn!(
                        "invalid TRADE_ENGINE_EXEC_BACKEND='{}', falling back to native",
                        raw
                    );
                }
                parsed
            })
            .unwrap_or(Self::Native);

        let Some(raw_map) = std::env::var("TRADE_ENGINE_EXEC_BACKEND_MAP")
            .ok()
            .filter(|raw| !raw.trim().is_empty())
        else {
            info!(
                "trade_engine execution backend: exchange={} backend={}",
                exchange,
                default.as_str()
            );
            return default;
        };

        let exchange_key = exchange.as_str();
        for entry in raw_map.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((key, value)) = entry.split_once('=') else {
                warn!(
                    "invalid TRADE_ENGINE_EXEC_BACKEND_MAP entry '{}'; expected exchange=backend",
                    entry
                );
                continue;
            };
            let key = key.trim().to_ascii_lowercase();
            if key != exchange_key && key != "*" {
                continue;
            }
            match Self::parse(value) {
                Some(backend) => {
                    info!(
                        "trade_engine execution backend: exchange={} backend={} source=TRADE_ENGINE_EXEC_BACKEND_MAP",
                        exchange,
                        backend.as_str()
                    );
                    return backend;
                }
                None => warn!(
                    "invalid backend '{}' for exchange '{}' in TRADE_ENGINE_EXEC_BACKEND_MAP",
                    value, key
                ),
            }
        }

        info!(
            "trade_engine execution backend: exchange={} backend={} source=TRADE_ENGINE_EXEC_BACKEND/default",
            exchange,
            default.as_str()
        );
        default
    }

    pub fn supports_exchange(self, exchange: Exchange) -> bool {
        match self {
            Self::Native => true,
            Self::Ltp => matches!(exchange, Exchange::Binance | Exchange::Okex),
        }
    }
}
