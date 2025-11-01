use mkt_signal::signal::channels::{
    SIGNAL_CHANNEL_MM_ARBITRAGE_BACKWARD, SIGNAL_CHANNEL_MM_ARBITRAGE_FORWARD,
};

const PROCESS_DISPLAY_NAME: &str = "Binance Forward Arb 策略 (MM)";
const SIGNAL_CHANNEL_FORWARD: &str = SIGNAL_CHANNEL_MM_ARBITRAGE_FORWARD;
const SIGNAL_CHANNEL_BACKWARD: Option<&str> = Some(SIGNAL_CHANNEL_MM_ARBITRAGE_BACKWARD);
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy_mm";
const DEFAULT_REDIS_HASH_KEY: &str = "binance_arb_price_spread_threshold";

include!("funding_rate_strategy_shared.rs");
