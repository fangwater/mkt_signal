use anyhow::{bail, Context, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::watch;

use crate::common::mkt_msg::AskBidSpreadMsg;
use crate::common::time_util::get_timestamp_us;
use crate::mkt_pub::cfg::Config;

use crate::spread_pbs::adapter::{create_adapter, BboFrame, VenueAdapter};
use crate::spread_pbs::latency::LatencyKll;
use crate::spread_pbs::publisher::SpreadPublisher;
use crate::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};

pub struct SpreadPbsApp {
    config: Config,
}

impl SpreadPbsApp {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// 主入口：拉 venue 全 symbol → 起双路 ws → 帧在 ws task 内同步处理（无 mpsc）。
    ///
    /// 必须在 `LocalSet` 上下文里 await（`main` 用 `LocalSet::run_until`）。
    pub async fn run(self) -> Result<()> {
        let venue = self.config.venue;
        let venue_slug: &'static str = venue.data_pub_slug();

        let adapter = match create_adapter(venue) {
            Some(a) => Rc::<dyn VenueAdapter>::from(a),
            None => bail!(
                "spread_pbs 当前不支持 venue {:?}（仅 OKex/Binance/Bybit/Gate/Bitget × spot+futures）",
                venue
            ),
        };
        log::info!(
            "spread_pbs starting venue={} adapter={}",
            venue_slug,
            adapter.name()
        );

        // ---- 拉 symbol list & 构造订阅消息 ----
        let symbols = self
            .config
            .get_symbols()
            .await
            .with_context(|| format!("fetch symbols for {}", venue_slug))?;
        if symbols.is_empty() {
            bail!("symbol list 为空（{}）", venue_slug);
        }
        let subscribe_msgs = adapter.build_subscribe(&symbols);
        if subscribe_msgs.is_empty() {
            bail!(
                "adapter.build_subscribe 返回空（{} symbols 数={}）",
                venue_slug,
                symbols.len()
            );
        }
        log::info!(
            "spread_pbs[{}] symbols={} subscribe_batches={}",
            venue_slug,
            symbols.len(),
            subscribe_msgs.len()
        );

        // ---- IceOryx publisher + 共享态（Rc<RefCell> 单线程零锁）----
        let publisher = Rc::new(
            SpreadPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx publisher for {}", venue_slug))?,
        );
        let net_label = format!("{}-net", venue_slug);
        let state: Rc<RefCell<SharedState>> = Rc::new(RefCell::new(SharedState {
            dedup: HashMap::with_capacity(2048),
            latency_e2e: LatencyKll::new(venue_slug),
            latency_net: LatencyKll::new(net_label),
            published: 0,
            dropped_by_seq: 0,
        }));

        // ---- 双路 ws：每条直接持有 adapter / publisher / state，无 mpsc 转交 ----
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let url = adapter.ws_url();

        let primary_handler =
            make_handler("primary", adapter.clone(), publisher.clone(), state.clone());
        let secondary_handler = make_handler(
            "secondary",
            adapter.clone(),
            publisher.clone(),
            state.clone(),
        );

        tokio::task::spawn_local(run_public_ws(
            WsLoopParams {
                label: "primary",
                url: url.clone(),
                local_ip: self.config.primary_local_ip.clone(),
                subscribe_msgs: subscribe_msgs.clone(),
                keepalive: adapter.keepalive(),
            },
            primary_handler,
            shutdown_rx.clone(),
        ));
        tokio::task::spawn_local(run_public_ws(
            WsLoopParams {
                label: "secondary",
                url,
                local_ip: self.config.secondary_local_ip.clone(),
                subscribe_msgs,
                keepalive: adapter.keepalive(),
            },
            secondary_handler,
            shutdown_rx.clone(),
        ));

        // ---- 主循环：仅 ctrl-c 与 30s stats 心跳，不再处理 frame ----
        let mut stats_ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        stats_ticker.tick().await;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    log::info!("spread_pbs[{}] SIGINT received, shutting down", venue_slug);
                    let _ = shutdown_tx.send(true);
                    break;
                }
                _ = stats_ticker.tick() => {
                    let s = state.borrow();
                    log::info!(
                        "spread_pbs[{}] stats published={} dropped_by_seq={} symbols_seen={}",
                        venue_slug, s.published, s.dropped_by_seq, s.dedup.len()
                    );
                }
            }
        }

        Ok(())
    }
}

struct SharedState {
    dedup: HashMap<String, i64>,
    /// 端到端：`publish_us - ts_ms*1000`（含 ws read → parse → dedup → IceOryx publish）
    latency_e2e: LatencyKll,
    /// 纯网络：`recv_us - ts_ms*1000`（ws read 命中那一刻 - 服务端 ts，剔除 pipeline 开销）
    latency_net: LatencyKll,
    published: u64,
    dropped_by_seq: u64,
}

fn make_handler(
    label: &'static str,
    adapter: Rc<dyn VenueAdapter>,
    publisher: Rc<SpreadPublisher>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |recv_us: i64, raw: &[u8]| {
        let text = match std::str::from_utf8(raw) {
            Ok(s) => s,
            Err(_) => return,
        };
        let frames = match adapter.parse_frame(text) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_frame failed: {:#} payload={}",
                    label, e, text
                );
                return;
            }
        };
        if frames.is_empty() {
            return;
        }
        let mut s = state.borrow_mut();
        for f in frames {
            process_frame(&mut s, &publisher, recv_us, f);
        }
    })
}

fn process_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadPublisher>,
    recv_us: i64,
    f: BboFrame,
) {
    let prev = state.dedup.get(&f.symbol).copied().unwrap_or(i64::MIN);
    if f.seq_id <= prev {
        state.dropped_by_seq += 1;
        return;
    }
    state.dedup.insert(f.symbol.clone(), f.seq_id);

    let msg = AskBidSpreadMsg::create(
        f.symbol.clone(),
        f.ts_ms,
        f.bid_price,
        f.bid_amount,
        f.ask_price,
        f.ask_amount,
    );
    let bytes = msg.to_bytes();
    if let Err(e) = publisher.publish(&bytes) {
        log::warn!("spread_pbs publish failed: {:#}", e);
        return;
    }
    state.published += 1;

    if f.ts_ms > 0 {
        let ts_us = f.ts_ms.saturating_mul(1000);
        // 纯网络：ws read 命中时刻 - 服务端 ts
        state.latency_net.push((recv_us - ts_us) as f64);
        // 端到端：publish 完成时刻 - 服务端 ts
        let publish_us = get_timestamp_us();
        state.latency_e2e.push((publish_us - ts_us) as f64);
    }
}
