use anyhow::Result;
use log::{info, warn};
use mkt_signal::pre_trade::monitor_channel::MonitorChannel;
use mkt_signal::pre_trade::params_load::PreTradeParams;
use mkt_signal::pre_trade::persist_channel::PersistChannel;
use mkt_signal::pre_trade::resample_channel::ResampleChannel;
use mkt_signal::pre_trade::signal_channel::SignalChannel;
use mkt_signal::pre_trade::trade_eng_channel::TradeEngChannel;
use mkt_signal::pre_trade::PreTrade;
use mkt_signal::strategy::StrategyManager;
use std::cell::RefCell;
use std::rc::Rc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    info!("pre_trade starting");
    info!("Required env vars: BINANCE_API_KEY, BINANCE_API_SECRET");
    info!("Optional env vars: REDIS_URL");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 1. 初始化 PreTradeParams（从 Redis 加载参数） 
            info!("Initializing PreTradeParams singleton..."); 
            if let Err(err) = PreTradeParams::instance().load_from_redis(None).await {
                warn!("Failed to load pre-trade params from Redis: {err:#}");
                warn!("Using default parameters");
            } else {
                info!("PreTradeParams loaded successfully"); 
            } 

            // 2. 初始化 StrategyManager
            info!("Initializing StrategyManager...");
            let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new())); 

            // 3. 初始化 MonitorChannel（包含所有账户管理器） 
            info!("Initializing MonitorChannel singleton..."); 
            if let Err(err) = MonitorChannel::init_singleton(strategy_mgr.clone()).await { 
                return Err(err); 
            } 
            info!("MonitorChannel initialized successfully"); 

            // 4. 初始化 SignalChannel
            info!("Initializing SignalChannel singleton...");
            if let Err(err) = SignalChannel::initialize("funding_rate_signal", Some("signal_query")) {
                warn!("Failed to initialize SignalChannel: {err:#}");
            } else {
                info!("SignalChannel initialized on channel: funding_rate_signal");
            } 

            // 5. 初始化 ResampleChannel
            info!("Initializing ResampleChannel singleton...");
            if let Err(err) = ResampleChannel::initialize("pre_trade_positions", "pre_trade_exposure", "pre_trade_risk") {
                warn!("Failed to initialize ResampleChannel: {err:#}");
            } else {
                info!("ResampleChannel initialized successfully");
            } 

            // 6. 初始化 TradeEngChannel 
            info!("Initializing TradeEngChannel singleton..."); 
            if let Err(err) = TradeEngChannel::initialize("order_reqs/binance", "order_resps/binance") {
                warn!("Failed to initialize TradeEngChannel: {err:#}"); 
            } else {
                info!("TradeEngChannel initialized on services: req=order_reqs/binance, resp=order_resps/binance");
            } 

            // 7. 预热 PersistChannel（自动初始化，调用一次即可）
            info!("Initializing PersistChannel singleton...");
            PersistChannel::with(|_ch| {
                info!("PersistChannel initialized successfully"); 
            }); 

            info!("All singletons initialized, starting pre_trade main loop..."); 

            // 8. 运行主循环 
            let pre_trade = PreTrade::new(); 
            pre_trade.run().await 
        })
        .await
}
