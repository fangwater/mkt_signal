use crate::cfg::Config;
use crate::mkt_msg::MktMsg;
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};

const TRADE_MAX_BYTES: usize = 64;
const KLINE_MAX_BYTES: usize = 128;
const DERIVATIVES_MAX_BYTES: usize = 128;
const SPREAD_MAX_BYTES: usize = 64;
const SIGNAL_MAX_BYTES: usize = 64;

pub struct IceOryxForwarder {
    // Publishers for different message types
    incremental_publisher: Option<Publisher<ipc::Service, [u8; 16384], ()>>,
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
}

impl IceOryxForwarder {
    pub fn new(config: &Config) -> Result<Self> {
        let exchange = config.get_exchange();

        info!("Creating IceOryx forwarder for exchange: {}", exchange);

        // 创建Node
        let node_name = format!("mkt_signal_{}", exchange.replace("-", "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        // 创建各个频道的publisher
        // 固定增量最大字节（编译期上限 8192），不从配置读取
        let inc_max = 16384usize;

        // 读取各频道的历史与订阅者配置
        let get_cfg = |ch: Option<&crate::cfg::ChannelCfg>,
                       default_hist: usize,
                       default_subs: usize|
         -> (usize, usize) {
            let h = ch.and_then(|c| c.history_size).unwrap_or(default_hist);
            let s = ch.and_then(|c| c.max_subscribers).unwrap_or(default_subs);
            (h, s)
        };
        let ice = config.iceoryx.as_ref();
        let (inc_hist, _inc_subs) = get_cfg(ice.and_then(|c| c.incremental.as_ref()), 100, 10);
        let (trade_hist, trade_subs) = get_cfg(ice.and_then(|c| c.trade.as_ref()), 100, 10);
        let (kline_hist, kline_subs) = get_cfg(ice.and_then(|c| c.kline.as_ref()), 50, 10);
        let (der_hist, der_subs) = get_cfg(ice.and_then(|c| c.derivatives.as_ref()), 50, 10);
        let (spread_hist, spread_subs) =
            get_cfg(ice.and_then(|c| c.ask_bid_spread.as_ref()), 100, 10);
        let (signal_hist, signal_subs) = get_cfg(ice.and_then(|c| c.signal.as_ref()), 50, 10);

        let incremental_publisher = if config.data_types.enable_incremental {
            let service = node
                .service_builder(&ServiceName::new(&format!(
                    "data_pubs/{}/incremental",
                    exchange
                ))?)
                .publish_subscribe::<[u8; 16384]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(inc_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let trade_publisher = if config.data_types.enable_trade {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/trade", exchange))?)
                .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(trade_subs)
                .history_size(trade_hist)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;

            Some(service.publisher_builder().create()?)
        } else {
            None
        };

        let kline_publisher = if config.data_types.enable_kline {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/kline", exchange))?)
                .publish_subscribe::<[u8; KLINE_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(kline_subs)
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
                    "data_pubs/{}/derivatives",
                    exchange
                ))?)
                .publish_subscribe::<[u8; DERIVATIVES_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(der_subs)
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
                    "data_pubs/{}/ask_bid_spread",
                    exchange
                ))?)
                .publish_subscribe::<[u8; SPREAD_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(spread_subs)
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
                    "data_pubs/{}/signal",
                    exchange
                ))?)
                .publish_subscribe::<[u8; SIGNAL_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(signal_subs)
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
                    self.derivatives_count += 1
                }
                t if t == crate::mkt_msg::MktMsgType::AskBidSpread as u32 => {
                    self.ask_bid_spread_count += 1
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
            warn!(
                "Message size {} exceeds max size {} for {}",
                msg.len(),
                max_size,
                label
            );
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

    pub async fn send_tp_reset_msg(&mut self) -> bool {
        let tp_reset_msg = MktMsg::tp_reset();
        let msg_bytes = tp_reset_msg.to_bytes();

        info!("Sending tp reset message through IceOryx...");

        // 发送到所有活跃的频道
        let mut sent = false;

        if let Some(ref publisher) = self.incremental_publisher {
            if self.send_with_publisher(publisher, &msg_bytes, 2048, "tp_reset_incremental") {
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
}

impl Drop for IceOryxForwarder {
    fn drop(&mut self) {
        info!("IceOryx forwarder stopping...");
        info!("IceOryx forwarder stopped successfully");
    }
}
