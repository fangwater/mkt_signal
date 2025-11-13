//! Funding Rate 资金费率套利信号生成器（极简版）
//!
//! 基于资金费率和价差计算交易信号，发送给 pre_trade

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::signal::common::TradingVenue;
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    EngineStats, SymbolState, SymbolThreshold, approx_equal,
    mkt_channel::MktChannel, rate_fetcher::RateFetcher,
};

const PROCESS_NAME: &str = "fr_signal";
const SIGNAL_CHANNEL: &str = "funding_rate_signal";
const DEFAULT_REDIS_KEY: &str = "binance_forward_spread_thresholds";

// 配置常量
const RELOAD_INTERVAL_SECS: u64 = 60;
const TICK_INTERVAL_MS: u64 = 100;
const MIN_SIGNAL_INTERVAL_US: i64 = 1_000_000; // 1秒

/// 简化配置结构
struct Config {
    redis: RedisSettings,
    redis_key: String,
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
    next_reload: Instant,
}

impl Engine {
    async fn new(cfg: Config, publisher: SignalPublisher) -> Result<Self> {
        Ok(Self {
            cfg,
            publisher,
            symbols: HashMap::new(),
            futures_index: HashMap::new(),
            stats: EngineStats::default(),
            next_reload: Instant::now(),
        })
    }

    /// 从 Redis 加载阈值配置
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

    /// 重新加载阈值（热更新）
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

    /// 处理市场数据并计算信号
    fn process_market_data(&mut self) {
        let mkt = MktChannel::instance();
        let rate_fetcher = RateFetcher::instance();

        // 收集需要发送的信号
        let mut signals_to_emit = Vec::new();

        for (symbol_key, state) in &mut self.symbols {
            // 1️⃣ 更新盘口数据
            if let Some(spot_quote) = mkt.get_quote(&state.spot_symbol, TradingVenue::BinanceSpot) {
                state.spot_quote = spot_quote;
            }
            if let Some(fut_quote) = mkt.get_quote(&state.futures_symbol, TradingVenue::BinanceUm) {
                state.futures_quote = fut_quote;
            }

            // 2️⃣ 获取资金费率均值（从 MktChannel 获取）
            let current_fr_ma = mkt.get_funding_rate_mean(&state.futures_symbol, TradingVenue::BinanceUm)
                .unwrap_or(0.0);

            // 3️⃣ 更新预测资金费率
            if let Some(predicted) = rate_fetcher.get_predicted_funding_rate(&state.futures_symbol, TradingVenue::BinanceUm) {
                state.predicted_rate = predicted;
            }

            // 4️⃣ 更新借贷利率
            if let Some(loan) = rate_fetcher.get_current_lending_rate(&state.futures_symbol, TradingVenue::BinanceUm) {
                state.loan_rate = loan;
            }

            // 5️⃣ 刷新价差因子
            state.refresh_factors();

            // 6️⃣ 评估信号（传入 current_fr_ma）
            let decision = state.evaluate_signal(current_fr_ma);

            // 7️⃣ 收集需要发送的信号
            if decision.can_emit {
                signals_to_emit.push((symbol_key.clone(), decision));
            }
        }

        // 8️⃣ 发送收集的信号
        for (symbol_key, decision) in signals_to_emit {
            self.emit_signal(&symbol_key, &decision);
        }
    }

    /// 发送信号给下游
    fn emit_signal(&mut self, symbol_key: &str, decision: &mkt_signal::funding_rate::EvaluateDecision) {
        let now = get_timestamp_us();

        // 获取 state 信息
        let state = match self.symbols.get_mut(symbol_key) {
            Some(s) => s,
            None => return,
        };

        // 检查信号间隔限制
        if now - state.last_signal_ts < MIN_SIGNAL_INTERVAL_US {
            return;
        }

        let signal_type = match decision.final_signal {
            1 => SignalType::ArbOpen,      // 正套开仓
            -1 => SignalType::ArbCancel,   // 撤单
            -2 => SignalType::ArbOpen,     // 反套开仓
            2 => SignalType::ArbClose,     // 平仓
            _ => return,
        };

        // 提取需要的数据（避免借用冲突）
        let spot_symbol = state.spot_symbol.clone();
        let futures_symbol = state.futures_symbol.clone();
        let spread_rate = state.spread_rate.unwrap_or(0.0);
        let predicted_rate = state.predicted_rate;

        // 构造简单的信号上下文（包含symbol信息）
        let context_str = format!("{}|{}|{:.6}|{:.8}",
            spot_symbol,
            futures_symbol,
            spread_rate,
            predicted_rate
        );
        let context = bytes::Bytes::from(context_str);

        // 构造信号消息
        let signal = TradeSignal::create(
            signal_type.clone(),
            now,
            0.0,
            context,
        );

        // 发布信号
        let signal_bytes = signal.to_bytes();
        if let Err(e) = self.publisher.publish(&signal_bytes) {
            info!("发送信号失败: {} {:?}", spot_symbol, e);
            return;
        }

        // 更新状态
        state.last_signal = decision.final_signal;
        state.last_signal_ts = now;

        // 更新统计
        match signal_type {
            SignalType::ArbOpen => self.stats.open_signals += 1,
            SignalType::ArbCancel => self.stats.ladder_cancel_signals += 1,
            _ => {}
        }

        info!(
            "✅ 信号: {} type={:?} signal={} spread={:.6} predict_fr={:.8}",
            spot_symbol,
            signal_type,
            decision.final_signal,
            spread_rate,
            predicted_rate,
        );
    }

    /// 主运行循环
    async fn run(&mut self, token: CancellationToken) -> Result<()> {
        info!("{} 启动", PROCESS_NAME);

        // 1️⃣ 初始化市场数据订阅
        MktChannel::init_singleton()?;
        RateFetcher::init_singleton()?;
        info!("市场数据订阅已启动");

        // 2️⃣ 初始加载阈值
        self.reload_thresholds().await?;
        info!("已加载 {} 个交易对", self.symbols.len());

        // 3️⃣ 主循环
        loop {
            if token.is_cancelled() {
                break;
            }

            // 定时重载阈值（60秒）
            if self.next_reload.elapsed().as_secs() > 0 {
                if let Ok(changed) = self.reload_thresholds().await {
                    if changed {
                        info!("阈值已更新");
                    }
                }
                self.next_reload = Instant::now() + Duration::from_secs(RELOAD_INTERVAL_SECS);
            }

            // 处理市场数据并计算信号
            self.process_market_data();

            // 每100ms tick一次
            tokio::time::sleep(Duration::from_millis(TICK_INTERVAL_MS)).await;
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

#[tokio::main(flavor = "current_thread")] 
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init(); 
    info!("{} 初始化", PROCESS_NAME); 

    let cfg = Config::load()?; 
    let publisher = SignalPublisher::new(SIGNAL_CHANNEL)?;
    let mut engine = Engine::new(cfg, publisher).await?;

    let token = CancellationToken::new(); 
    setup_signal_handlers(&token)?; 

    // 使用 LocalSet 运行，因为 MktChannel 和 RateFetcher 需要 thread-local
    let local = tokio::task::LocalSet::new();
    local.run_until(engine.run(token)).await 
} 
