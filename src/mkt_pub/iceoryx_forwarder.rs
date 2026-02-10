use crate::cfg::Config;
use crate::mkt_msg::FundingRateMsg;
use crate::mkt_msg::MktMsg;
use crate::mkt_msg::MktMsgType;
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn, Level};
use std::time::{Duration, Instant};

const TRADE_MAX_BYTES: usize = 64;
const KLINE_MAX_BYTES: usize = 128;
const DERIVATIVES_MAX_BYTES: usize = 128;
const SPREAD_MAX_BYTES: usize = 64;
const SIGNAL_MAX_BYTES: usize = 64;
const INC_HISTORY_SIZE: usize = 100;
const INC_MAX_SUBSCRIBERS: usize = 10;
const TRADE_HISTORY_SIZE: usize = 100;
const TRADE_MAX_SUBSCRIBERS: usize = 10;
const KLINE_HISTORY_SIZE: usize = 50;
const KLINE_MAX_SUBSCRIBERS: usize = 10;
const DERIVATIVES_HISTORY_SIZE: usize = 50;
const DERIVATIVES_MAX_SUBSCRIBERS: usize = 10;
const SPREAD_HISTORY_SIZE: usize = 100;
const SPREAD_MAX_SUBSCRIBERS: usize = 10;
const SIGNAL_HISTORY_SIZE: usize = 50;
const SIGNAL_MAX_SUBSCRIBERS: usize = 10;
const DERIVATIVES_DEBUG_INTERVAL: Duration = Duration::from_secs(5);

// 对应 max_levels_per_msg=50 的增量消息，实际消息体通常 <1KB；
// 这里保留 2KB 作为稳妥上限并减少共享内存占用。
const INC_CHANNEL_MAX_BYTES: usize = 2048;

pub struct IceOryxForwarder {
    exchange: String,
    // Publishers for different message types
    incremental_publisher: Option<Publisher<ipc::Service, [u8; INC_CHANNEL_MAX_BYTES], ()>>,
    trade_publisher: Option<Publisher<ipc::Service, [u8; TRADE_MAX_BYTES], ()>>,
    kline_publisher: Option<Publisher<ipc::Service, [u8; KLINE_MAX_BYTES], ()>>,
    derivatives_publisher: Option<Publisher<ipc::Service, [u8; DERIVATIVES_MAX_BYTES], ()>>, // 只有期货使用
    ask_bid_spread_publisher: Option<Publisher<ipc::Service, [u8; SPREAD_MAX_BYTES], ()>>,
    signal_publisher: Option<Publisher<ipc::Service, [u8; SIGNAL_MAX_BYTES], ()>>, // 时间信号

    // Statistics
    incremental_count: u64,
    trade_count: u64,
    kline_count: u64,
    derivatives_count: u64,
    ask_bid_spread_count: u64,
    signal_count: u64,
    message_count: u64,
    dropped_count: u64,
    incremental_max_bytes: usize,

    // window stats: max message size seen per channel
    inc_max_seen: usize,
    trade_max_seen: usize,
    kline_max_seen: usize,
    der_max_seen: usize,
    spread_max_seen: usize,
    signal_max_seen: usize,
    last_derivatives_debug: Instant,
}

impl IceOryxForwarder {
    pub fn new(config: &Config) -> Result<Self> {
        let exchange = config.get_exchange();
        let venue_slug = config.venue.data_pub_slug();

        info!(
            "Creating IceOryx forwarder for exchange: {} (venue={})",
            exchange, venue_slug
        );
        let exchange_name = venue_slug.to_string();

        // 创建Node
        let node_name = format!("mkt_signal_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        // 创建各个频道的publisher
        // 固定增量最大字节（编译期上限 8192），不从配置读取
        let inc_max = INC_CHANNEL_MAX_BYTES;

        let inc_hist = INC_HISTORY_SIZE;
        let trade_hist = TRADE_HISTORY_SIZE;
        let kline_hist = KLINE_HISTORY_SIZE;
        let der_hist = DERIVATIVES_HISTORY_SIZE;
        let spread_hist = SPREAD_HISTORY_SIZE;
        let signal_hist = SIGNAL_HISTORY_SIZE;

        let incremental_publisher = if config.data_types.enable_incremental {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "dat_pbs/{}/incremental",
                    venue_slug
                ))?)
                .publish_subscribe::<[u8; INC_CHANNEL_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(INC_MAX_SUBSCRIBERS)
                .history_size(inc_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let trade_publisher = if config.data_types.enable_trade {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "dat_pbs/{}/trade",
                    venue_slug
                ))?)
                .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(TRADE_MAX_SUBSCRIBERS)
                .history_size(trade_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let kline_publisher = if config.data_types.enable_kline {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "dat_pbs/{}/kline",
                    venue_slug
                ))?)
                .publish_subscribe::<[u8; KLINE_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(KLINE_MAX_SUBSCRIBERS)
                .history_size(kline_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        // Derivatives频道只在启用时创建（仅衍生品交易所使用）
        let derivatives_publisher = if config.data_types.enable_derivatives {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "dat_pbs/{}/derivatives",
                    venue_slug
                ))?)
                .publish_subscribe::<[u8; DERIVATIVES_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(DERIVATIVES_MAX_SUBSCRIBERS)
                .history_size(der_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let ask_bid_spread_publisher = if config.data_types.enable_ask_bid_spread {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "dat_pbs/{}/ask_bid_spread",
                    venue_slug
                ))?)
                .publish_subscribe::<[u8; SPREAD_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(SPREAD_MAX_SUBSCRIBERS)
                .history_size(spread_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        // Signal频道 - 用于时间信号（BTCUSDT深度变化触发）
        let signal_publisher = {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "dat_pbs/{}/signal",
                    venue_slug
                ))?)
                .publish_subscribe::<[u8; SIGNAL_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(SIGNAL_MAX_SUBSCRIBERS)
                .history_size(signal_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        };

        info!("IceOryx forwarder created successfully");
        info!("Enabled channels - incremental: {}, trade: {}, kline: {}, derivatives: {}, ask_bid_spread: {}, signal: {}",
              incremental_publisher.is_some(),
              trade_publisher.is_some(),
              kline_publisher.is_some(),
              derivatives_publisher.is_some(),
              ask_bid_spread_publisher.is_some(),
              signal_publisher.is_some());

        Ok(Self {
            exchange: exchange_name,
            incremental_publisher,
            trade_publisher,
            kline_publisher,
            derivatives_publisher,
            ask_bid_spread_publisher,
            signal_publisher,
            incremental_count: 0,
            trade_count: 0,
            kline_count: 0,
            derivatives_count: 0,
            ask_bid_spread_count: 0,
            signal_count: 0,
            message_count: 0,
            dropped_count: 0,
            incremental_max_bytes: inc_max,
            inc_max_seen: 0,
            trade_max_seen: 0,
            kline_max_seen: 0,
            der_max_seen: 0,
            spread_max_seen: 0,
            signal_max_seen: 0,
            last_derivatives_debug: Instant::now(),
        })
    }

    pub async fn send_msg(&mut self, msg: Bytes) -> bool {
        // 直接解析前4个字节获取消息类型
        if msg.len() < 4 {
            warn!("Message too short to parse");
            self.dropped_count += 1;
            return false;
        }

        let msg_type_bytes = [msg[0], msg[1], msg[2], msg[3]];
        let msg_type = u32::from_le_bytes(msg_type_bytes);

        // 根据消息类型选择合适的publisher
        let result = match msg_type {
            t if t == crate::mkt_msg::MktMsgType::OrderBookInc as u32 => {
                let len = msg.len();
                if len > self.inc_max_seen {
                    self.inc_max_seen = len;
                }
                if let Some(ref publisher) = self.incremental_publisher {
                    self.send_with_publisher(
                        publisher,
                        &msg,
                        self.incremental_max_bytes,
                        "incremental",
                    )
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::TradeInfo as u32 => {
                let len = msg.len();
                if len > self.trade_max_seen {
                    self.trade_max_seen = len;
                }
                if let Some(ref publisher) = self.trade_publisher {
                    self.send_with_publisher(publisher, &msg, TRADE_MAX_BYTES, "trade")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::Kline as u32 => {
                let len = msg.len();
                if len > self.kline_max_seen {
                    self.kline_max_seen = len;
                }
                if let Some(ref publisher) = self.kline_publisher {
                    self.send_with_publisher(publisher, &msg, KLINE_MAX_BYTES, "kline")
                } else {
                    warn!("Kline publisher is None, dropping message");
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::LiquidationOrder as u32
                || t == crate::mkt_msg::MktMsgType::MarkPrice as u32
                || t == crate::mkt_msg::MktMsgType::IndexPrice as u32
                || t == crate::mkt_msg::MktMsgType::FundingRate as u32 =>
            {
                let len = msg.len();
                if len > self.der_max_seen {
                    self.der_max_seen = len;
                }
                if let Some(ref publisher) = self.derivatives_publisher {
                    self.send_with_publisher(publisher, &msg, DERIVATIVES_MAX_BYTES, "derivatives")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::AskBidSpread as u32 => {
                let len = msg.len();
                if len > self.spread_max_seen {
                    self.spread_max_seen = len;
                }
                if let Some(ref publisher) = self.ask_bid_spread_publisher {
                    self.send_with_publisher(publisher, &msg, SPREAD_MAX_BYTES, "ask_bid_spread")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::TimeSignal as u32 => {
                let len = msg.len();
                if len > self.signal_max_seen {
                    self.signal_max_seen = len;
                }
                if let Some(ref publisher) = self.signal_publisher {
                    self.send_with_publisher(publisher, &msg, SIGNAL_MAX_BYTES, "signal")
                } else {
                    false
                }
            }
            _ => false,
        };

        if result {
            self.message_count += 1;
            // 更新具体类型的计数
            match msg_type {
                t if t == crate::mkt_msg::MktMsgType::OrderBookInc as u32 => {
                    self.incremental_count += 1
                }
                t if t == crate::mkt_msg::MktMsgType::TradeInfo as u32 => self.trade_count += 1,
                t if t == crate::mkt_msg::MktMsgType::Kline as u32 => {
                    self.kline_count += 1;
                    // 检查是否是BTCUSDT的K线消息（用于调试）
                    if msg.len() >= 132 {
                        let symbol_bytes = &msg[8..40];
                        if let Ok(symbol_str) = std::str::from_utf8(symbol_bytes) {
                            let symbol = symbol_str.trim_end_matches('\0');
                            if symbol.to_lowercase() == "btcusdt" {
                                info!("[IceOryx] BTCUSDT Kline sent successfully, total kline count: {}", self.kline_count);
                            }
                        }
                    }
                }
                t if t == crate::mkt_msg::MktMsgType::LiquidationOrder as u32
                    || t == crate::mkt_msg::MktMsgType::MarkPrice as u32
                    || t == crate::mkt_msg::MktMsgType::IndexPrice as u32
                    || t == crate::mkt_msg::MktMsgType::FundingRate as u32 =>
                {
                    self.derivatives_count += 1;
                    self.log_derivatives_debug(msg_type, msg.as_ref());
                }
                t if t == crate::mkt_msg::MktMsgType::AskBidSpread as u32 => {
                    self.ask_bid_spread_count += 1;
                }
                t if t == crate::mkt_msg::MktMsgType::TimeSignal as u32 => self.signal_count += 1,
                _ => {}
            }
        } else {
            self.dropped_count += 1;
        }

        result
    }

    fn send_with_publisher<const SIZE: usize>(
        &self,
        publisher: &Publisher<ipc::Service, [u8; SIZE], ()>,
        msg: &[u8],
        max_size: usize,
        label: &str,
    ) -> bool {
        if msg.len() > max_size {
            self.log_oversize(label, msg, max_size);
            return false;
        }

        // 创建固定大小的buffer并复制数据
        let mut buffer = [0u8; SIZE];
        buffer[..msg.len()].copy_from_slice(msg);

        // 发送消息
        match publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                matches!(sample.send(), Ok(_))
            }
            Err(_e) => false,
        }
    }

    fn log_oversize(&self, label: &str, msg: &[u8], max_size: usize) {
        let symbol_info = match label {
            "trade" | "ask_bid_spread" | "kline" | "derivatives" => {
                Self::extract_symbol(msg).map(|symbol| (symbol, symbol.len()))
            }
            _ => None,
        };

        if let Some((symbol, len)) = symbol_info {
            info!(
                "Message size {} exceeds max size {} for {} (symbol={} len={})",
                msg.len(),
                max_size,
                label,
                symbol,
                len
            );
        } else {
            info!(
                "Message size {} exceeds max size {} for {}",
                msg.len(),
                max_size,
                label
            );
        }
    }

    pub async fn send_tp_reset_msg(&mut self) -> bool {
        let tp_reset_msg = MktMsg::tp_reset();
        let msg_bytes = tp_reset_msg.to_bytes();

        info!("Sending tp reset message through IceOryx...");

        // 发送到所有活跃的频道
        let mut sent = false;

        if let Some(ref publisher) = self.incremental_publisher {
            if self.send_with_publisher(publisher, &msg_bytes, INC_CHANNEL_MAX_BYTES, "tp_reset_incremental") {
                sent = true;
            }
        }

        if let Some(ref publisher) = self.trade_publisher {
            if self.send_with_publisher(publisher, &msg_bytes, 1024, "tp_reset_trade") {
                sent = true;
            }
        }

        if sent {
            info!("Send tp reset msg success through IceOryx");
        } else {
            warn!("Failed to send tp reset msg through any channel");
        }

        sent
    }

    pub fn log_stats(&mut self) {
        // 打印统计信息（包含本窗口各类型最大消息大小）
        info!("IceOryx stats: total: {}, inc: {} (max {}), trade: {} (max {}), kline: {} (max {}), derivatives: {} (max {}), spread: {} (max {}), signal: {} (max {}), dropped: {}",
              self.message_count,
              self.incremental_count, self.inc_max_seen,
              self.trade_count, self.trade_max_seen,
              self.kline_count, self.kline_max_seen,
              self.derivatives_count, self.der_max_seen,
              self.ask_bid_spread_count, self.spread_max_seen,
              self.signal_count, self.signal_max_seen,
              self.dropped_count);

        // 清零统计信息
        self.incremental_count = 0;
        self.trade_count = 0;
        self.kline_count = 0;
        self.derivatives_count = 0;
        self.ask_bid_spread_count = 0;
        self.signal_count = 0;
        self.message_count = 0;
        self.dropped_count = 0;
        self.inc_max_seen = 0;
        self.trade_max_seen = 0;
        self.kline_max_seen = 0;
        self.der_max_seen = 0;
        self.spread_max_seen = 0;
        self.signal_max_seen = 0;
    }

    fn log_derivatives_debug(&mut self, msg_type: u32, payload: &[u8]) {
        if !log::log_enabled!(Level::Debug) {
            return;
        }
        let symbol_raw = Self::extract_symbol(payload).unwrap_or("UNKNOWN");
        let symbol = self.normalize_symbol(symbol_raw);
        let label = match msg_type {
            t if t == MktMsgType::LiquidationOrder as u32 => "liquidation",
            t if t == MktMsgType::MarkPrice as u32 => "mark_price",
            t if t == MktMsgType::IndexPrice as u32 => "index_price",
            t if t == MktMsgType::FundingRate as u32 => "funding_rate",
            _ => "derivatives",
        };

        // BTC* 的衍生品消息打印具体内容，不计数
        if symbol.to_ascii_lowercase().starts_with("btc") {
            let detail = match msg_type {
                t if t == MktMsgType::LiquidationOrder as u32 => Self::decode_liquidation(payload)
                    .map(|(side, qty, price, ts)| {
                        format!("side={} qty={} price={} ts={}", side, qty, price, ts)
                    }),
                t if t == MktMsgType::MarkPrice as u32 => Self::decode_mark_price(payload)
                    .map(|(price, ts)| format!("mark_price={} ts={}", price, ts)),
                t if t == MktMsgType::IndexPrice as u32 => Self::decode_index_price(payload)
                    .map(|(price, ts)| format!("index_price={} ts={}", price, ts)),
                t if t == MktMsgType::FundingRate as u32 => {
                    Self::decode_funding(payload).map(|(rate, next_time, ts)| {
                        format!("funding_rate={} next_time={} ts={}", rate, next_time, ts)
                    })
                }
                _ => None,
            }
            .unwrap_or_else(|| "decode_failed".to_string());

            debug!(
                "[IceOryx][{}] BTC derivatives: type={} symbol={} {}",
                self.exchange, label, symbol, detail
            );
            return;
        }

        // 其他符号维持节流打印
        let now = Instant::now();
        if now.duration_since(self.last_derivatives_debug) < DERIVATIVES_DEBUG_INTERVAL {
            return;
        }
        self.last_derivatives_debug = now;
        debug!(
            "[IceOryx][{}] derivatives live: type={} symbol={} bytes={}",
            self.exchange,
            label,
            symbol,
            payload.len()
        );
    }

    fn extract_symbol(payload: &[u8]) -> Option<&str> {
        if payload.len() < 8 {
            return None;
        }
        let len = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]) as usize;
        let end = 8 + len;
        if payload.len() < end {
            return None;
        }
        std::str::from_utf8(&payload[8..end]).ok()
    }

    fn normalize_symbol(&self, symbol: &str) -> String {
        // OKEx 衍生品去掉 -SWAP 后缀，方便匹配
        if self.exchange.to_ascii_lowercase().contains("okex") {
            let upper = symbol.to_ascii_uppercase();
            if upper.ends_with("-SWAP") && symbol.len() > 5 {
                return symbol[..symbol.len() - 5].to_string();
            }
        }
        symbol.to_string()
    }

    fn read_f64(payload: &[u8], offset: usize) -> Option<f64> {
        let slice = payload.get(offset..offset + 8)?;
        let arr: [u8; 8] = slice.try_into().ok()?;
        Some(f64::from_le_bytes(arr))
    }

    fn read_i64(payload: &[u8], offset: usize) -> Option<i64> {
        let slice = payload.get(offset..offset + 8)?;
        let arr: [u8; 8] = slice.try_into().ok()?;
        Some(i64::from_le_bytes(arr))
    }

    fn decode_mark_price(payload: &[u8]) -> Option<(f64, i64)> {
        let symbol_len = u32::from_le_bytes([
            payload.get(4)?.to_owned(),
            payload.get(5)?.to_owned(),
            payload.get(6)?.to_owned(),
            payload.get(7)?.to_owned(),
        ]) as usize;
        let offset = 8 + symbol_len;
        let price = Self::read_f64(payload, offset)?;
        let ts = Self::read_i64(payload, offset + 8)?;
        Some((price, ts))
    }

    fn decode_index_price(payload: &[u8]) -> Option<(f64, i64)> {
        // 同 mark price 结构
        Self::decode_mark_price(payload)
    }

    fn decode_funding(payload: &[u8]) -> Option<(f64, i64, i64)> {
        if payload.len() < 8 {
            return None;
        }
        let rate = FundingRateMsg::get_funding_rate(payload);
        let next_time = FundingRateMsg::get_next_funding_time(payload);
        let ts = FundingRateMsg::get_timestamp(payload);
        Some((rate, next_time, ts))
    }

    fn decode_liquidation(payload: &[u8]) -> Option<(char, f64, f64, i64)> {
        let symbol_len = u32::from_le_bytes([
            payload.get(4)?.to_owned(),
            payload.get(5)?.to_owned(),
            payload.get(6)?.to_owned(),
            payload.get(7)?.to_owned(),
        ]) as usize;
        let base = 8 + symbol_len;
        let side = *payload.get(base)?;
        let side_char = side as char;
        let qty = Self::read_f64(payload, base + 1)?;
        let price = Self::read_f64(payload, base + 9)?;
        let ts = Self::read_i64(payload, base + 17)?;
        Some((side_char, qty, price, ts))
    }
}

impl Drop for IceOryxForwarder {
    fn drop(&mut self) {
        info!("IceOryx forwarder stopping...");
        info!("IceOryx forwarder stopped successfully");
    }
}
