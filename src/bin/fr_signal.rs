//! Funding Rate 资金费率套利信号生成器（精简版）
//!
//! 这是基于模块化设计的新实现，只依赖 src/funding_rate/ 模块

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use log::info;
use reqwest::Client;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_publisher::{ResamplePublisher, SignalPublisher};
use mkt_signal::common::min_qty_table::MinQtyTable;
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::mkt_msg::{AskBidSpreadMsg, FundingRateMsg};

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    EngineStats, FundingThresholdEntry, ParamsSnapshot, QtyStepInfo, RateThresholds,
    SymbolState, SymbolThreshold, approx_equal,
};

const PROCESS_NAME: &str = "fr_signal";
const SIGNAL_CHANNEL: &str = "funding_rate_signal";
const FR_RESAMPLE_MSG_CHANNEL: &str = "fr_resample_msg";
const NODE_SUB: &str = "fr_signal_sub";
const DEFAULT_REDIS_KEY: &str = "binance_forward_spread_thresholds";

// 写死的配置常量
const RELOAD_INTERVAL_SECS: u64 = 60;

/// 简化配置结构（写死所有值，只有 Redis 连接信息从环境变量读取）
struct Config {
    redis: RedisSettings,
    redis_key: String,
    reload_interval_secs: u64,
}

impl Config {
    fn load() -> Result<Self> {
        let redis_host = std::env::var("FUNDING_RATE_REDIS_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());
        let redis_key = std::env::var("FUNDING_RATE_REDIS_KEY")
            .unwrap_or_else(|_| DEFAULT_REDIS_KEY.to_string());

        Ok(Self {
            redis: RedisSettings {
                host: redis_host,
                port: 6379,
                db: 0,
                username: None,
                password: None,
                prefix: None,
            },
            redis_key,
            reload_interval_secs: RELOAD_INTERVAL_SECS,
        })
    }
}

/// 主引擎
struct Engine {
    cfg: Config,
    publisher: SignalPublisher,
    symbols: HashMap<String, SymbolState>,
    futures_index: HashMap<String, String>,
    stats: EngineStats,
    min_qty: MinQtyTable,
    qty_step_cache: RefCell<HashMap<String, QtyStepInfo>>,
    history_map: HashMap<String, Vec<f64>>,
    predicted_map: HashMap<String, f64>,
    http: Client,
    resample_msg_pub: ResamplePublisher,
    funding_series: HashMap<String, Vec<f64>>,
    loan_map: HashMap<String, f64>,
    funding_thresholds: HashMap<String, FundingThresholdEntry>,
    funding_frequency: HashMap<String, String>,
    last_params: Option<ParamsSnapshot>,
    th_4h: RateThresholds,
    th_8h: RateThresholds,
    warmup_done: bool,
    next_reload: Instant,
    next_compute_refresh: Instant,
    next_fetch_refresh: Instant,
    next_resample: Instant,
    next_loan_refresh: Instant,
}

impl Engine {
    async fn new(cfg: Config, publisher: SignalPublisher) -> Result<Self> {
        let resample_msg_pub = ResamplePublisher::new(FR_RESAMPLE_MSG_CHANNEL)?;
        Ok(Self {
            cfg,
            publisher,
            symbols: HashMap::new(),
            futures_index: HashMap::new(),
            stats: EngineStats::default(),
            min_qty: MinQtyTable::new(),
            qty_step_cache: RefCell::new(HashMap::new()),
            history_map: HashMap::new(),
            predicted_map: HashMap::new(),
            http: Client::new(),
            resample_msg_pub,
            funding_series: HashMap::new(),
            loan_map: HashMap::new(),
            funding_thresholds: HashMap::new(),
            funding_frequency: HashMap::new(),
            last_params: None,
            th_4h: RateThresholds::for_4h(),
            th_8h: RateThresholds::for_8h(),
            warmup_done: false,
            next_reload: Instant::now(),
            next_compute_refresh: Instant::now() + Duration::from_secs(30),
            next_fetch_refresh: Instant::now() + Duration::from_secs(60),
            next_resample: Instant::now() + Duration::from_secs(3),
            next_loan_refresh: Instant::now() + Duration::from_secs(60),
        })
    }

    async fn load_thresholds_from_redis(&self) -> Result<Vec<SymbolThreshold>> {
        let mut client = RedisClient::connect(self.cfg.redis.clone()).await?;
        let map = client.hgetall_map(&self.cfg.redis_key).await.unwrap_or_default();

        let mut thresholds = Vec::new();
        for (key, _) in &map {
            if !key.ends_with("_forward_open_threshold") {
                continue;
            }
            let spot_symbol = key.strip_suffix("_forward_open_threshold").unwrap().to_uppercase();
            let futures_symbol = format!("{}USDT", spot_symbol.strip_suffix("USDT").unwrap_or(&spot_symbol));

            let open_th = map.get(key).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            let cancel_key = format!("{}_forward_cancel_threshold", spot_symbol);
            let cancel_th = map.get(&cancel_key).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            let close_key = format!("{}_forward_close_threshold", spot_symbol);
            let close_th = map.get(&close_key).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);

            thresholds.push(SymbolThreshold {
                spot_symbol,
                futures_symbol,
                forward_open_threshold: open_th,
                forward_cancel_threshold: cancel_th,
                forward_close_threshold: close_th,
                forward_cancel_close_threshold: None,
            });
        }
        Ok(thresholds)
    }

    async fn reload_thresholds(&mut self) -> Result<bool> {
        let thresholds = self.load_thresholds_from_redis().await?;
        if thresholds.is_empty() {
            return Ok(false);
        }

        let mut changed = false;
        for th in thresholds {
            let key = th.spot_symbol.clone();
            if let Some(state) = self.symbols.get_mut(&key) {
                if !approx_equal(state.forward_open_threshold, th.forward_open_threshold)
                    || !approx_equal(state.forward_cancel_threshold, th.forward_cancel_threshold)
                    || !approx_equal(state.forward_close_threshold, th.forward_close_threshold)
                {
                    state.update_threshold(th.clone());
                    changed = true;
                }
            } else {
                let fut_sym = th.futures_symbol.clone();
                self.symbols.insert(key.clone(), SymbolState::new(th));
                self.futures_index.insert(fut_sym, key);
                changed = true;
            }
        }
        Ok(changed) 
    }

    fn handle_spread_msg(&mut self, msg: &AskBidSpreadMsg, is_spot: bool) {
        let symbol = msg.symbol.to_uppercase();

        if is_spot {
            if let Some(state) = self.symbols.get_mut(&symbol) {
                state.spot_quote.update(msg.bid_price, msg.ask_price, msg.timestamp);
                state.refresh_factors();
            }
        } else {
            if let Some(spot_key) = self.futures_index.get(&symbol) {
                if let Some(state) = self.symbols.get_mut(spot_key) {
                    state.futures_quote.update(msg.bid_price, msg.ask_price, msg.timestamp);
                    state.refresh_factors();
                }
            }
        }
    }

    fn handle_funding_msg(&mut self, msg: &FundingRateMsg) {
        let fut_symbol = msg.symbol.to_uppercase();
        if let Some(spot_key) = self.futures_index.get(&fut_symbol) {
            if let Some(state) = self.symbols.get_mut(spot_key) {
                state.funding_rate = msg.funding_rate;
                state.funding_ts = msg.timestamp;
                state.next_funding_time = msg.next_funding_time;
            }
        }
    } 

    async fn run(&mut self, token: CancellationToken) -> Result<()> {
        info!("{} 启动", PROCESS_NAME);

        // 初始加载 
        self.reload_thresholds().await?; 
        info!("已加载 {} 个交易对", self.symbols.len()); 

        loop {
            if token.is_cancelled() {
                break;
            } 

            // 定时任务 
            if self.next_reload.elapsed().is_zero() {
                if let Ok(changed) = self.reload_thresholds().await {
                    if changed {
                        info!("阈值已更新");
                    }
                } 
                self.next_reload = Instant::now() + Duration::from_secs(self.cfg.reload_interval_secs);
            } 

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("{} 退出", PROCESS_NAME);
        Ok(())
    }
}

fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut sigterm = unix_signal(SignalKind::terminate()).expect("failed to setup SIGTERM");
        let mut sigint = unix_signal(SignalKind::interrupt()).expect("failed to setup SIGINT");
        tokio::select! {
            _ = sigterm.recv() => info!("收到 SIGTERM"),
            _ = sigint.recv() => info!("收到 SIGINT"),
        }
        token_clone.cancel();
    });
    Ok(()) 
} 

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    info!("{} 初始化", PROCESS_NAME);

    let cfg = Config::load()?;
    let publisher = SignalPublisher::new(SIGNAL_CHANNEL)?;
    let mut engine = Engine::new(cfg, publisher).await?;

    let token = CancellationToken::new(); 
    setup_signal_handlers(&token)?; 

    engine.run(token).await 
} 
