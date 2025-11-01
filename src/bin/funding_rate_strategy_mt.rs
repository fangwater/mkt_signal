use mkt_signal::signal::channels::SIGNAL_CHANNEL_MT_ARBITRAGE;

const PROCESS_DISPLAY_NAME: &str = "Binance Forward Arb 策略 (MT)";
const SIGNAL_CHANNEL_FORWARD: &str = SIGNAL_CHANNEL_MT_ARBITRAGE;
const SIGNAL_CHANNEL_BACKWARD: Option<&str> = None;
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy_mt";
const DEFAULT_REDIS_HASH_KEY: &str = "binance_arb_price_spread_threshold";

include!("funding_rate_strategy_shared.rs");
