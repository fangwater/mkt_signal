use mkt_signal::signal::channels::SIGNAL_CHANNEL_MT_ARBITRAGE;

const PROCESS_DISPLAY_NAME: &str = "Binance Forward Arb 策略 (MT)";
const SIGNAL_CHANNEL_FORWARD: &str = SIGNAL_CHANNEL_MT_ARBITRAGE;
const SIGNAL_CHANNEL_BACKWARD: Option<&str> = None;
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy_mt";
const DEFAULT_REDIS_HASH_KEY: &str = "binance_arb_price_spread_threshold";
const OPEN_SIGNAL_TYPE: mkt_signal::signal::trade_signal::SignalType =
    mkt_signal::signal::trade_signal::SignalType::BinSingleForwardArbOpenMT;
const CANCEL_SIGNAL_TYPE: mkt_signal::signal::trade_signal::SignalType =
    mkt_signal::signal::trade_signal::SignalType::BinSingleForwardArbCancelMT;

include!("funding_rate_strategy_shared.rs");
