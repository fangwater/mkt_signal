use iceoryx2::prelude::*;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::service::ipc;
use crate::mkt_msg::MktMsg;
use bytes::Bytes;
use crate::cfg::Config;
use log::{info, warn};
use anyhow::Result;

pub struct IceOryxForwarder {
    is_futures: bool,  // 判断是期货还是现货
    
    // Publishers for different message types
    incremental_publisher: Option<Publisher<ipc::Service, [u8; 2048], ()>>,
    trade_publisher: Option<Publisher<ipc::Service, [u8; 1024], ()>>,
    kline_publisher: Option<Publisher<ipc::Service, [u8; 512], ()>>,
    derivatives_publisher: Option<Publisher<ipc::Service, [u8; 1024], ()>>,  // 只有期货使用
    ask_bid_spread_publisher: Option<Publisher<ipc::Service, [u8; 512], ()>>,
    signal_publisher: Option<Publisher<ipc::Service, [u8; 256], ()>>,  // 时间信号
    
    // Statistics
    incremental_count: u64,
    trade_count: u64,
    kline_count: u64,
    derivatives_count: u64,
    ask_bid_spread_count: u64,
    signal_count: u64,
    message_count: u64,
    dropped_count: u64,
}

impl IceOryxForwarder {
    pub fn new(config: &Config) -> Result<Self> {
        let exchange = config.get_exchange();
        let is_futures = exchange.contains("futures") || exchange.contains("swap");
        
        info!("Creating IceOryx forwarder for exchange: {}, is_futures: {}", 
              exchange, is_futures);
        
        // 创建Node
        let node_name = format!("mkt_signal_{}", exchange.replace("-", "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        
        // 创建各个频道的publisher
        let incremental_publisher = if config.data_types.enable_incremental {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/incremental", exchange))?)
                .publish_subscribe::<[u8; 2048]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .subscriber_max_buffer_size(200)
                .open_or_create()?;
            
            Some(service.publisher_builder().create()?)
        } else {
            None
        };
        
        let trade_publisher = if config.data_types.enable_trade {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/trade", exchange))?)
                .publish_subscribe::<[u8; 1024]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .subscriber_max_buffer_size(200)
                .open_or_create()?;
            
            Some(service.publisher_builder().create()?)
        } else {
            None
        };
        
        let kline_publisher = if config.data_types.enable_kline {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/kline", exchange))?)
                .publish_subscribe::<[u8; 512]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(50)
                .subscriber_max_buffer_size(100)
                .open_or_create()?;
            
            Some(service.publisher_builder().create()?)
        } else {
            None
        };
        
        // Derivatives频道只给期货创建
        let derivatives_publisher = if is_futures && config.data_types.enable_derivatives {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/derivatives", exchange))?)
                .publish_subscribe::<[u8; 1024]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(50)
                .subscriber_max_buffer_size(100)
                .open_or_create()?;
            
            Some(service.publisher_builder().create()?)
        } else {
            None
        };
        
        let ask_bid_spread_publisher = if config.data_types.enable_ask_bid_spread {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/ask_bid_spread", exchange))?)
                .publish_subscribe::<[u8; 512]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .subscriber_max_buffer_size(200)
                .open_or_create()?;
            
            Some(service.publisher_builder().create()?)
        } else {
            None
        };
        
        // Signal频道 - 用于时间信号（BTCUSDT深度变化触发）
        let signal_publisher = {
            let service = node
                .service_builder(&ServiceName::new(&format!("data_pubs/{}/signal", exchange))?)
                .publish_subscribe::<[u8; 256]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(50)
                .subscriber_max_buffer_size(100)
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
            is_futures,
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
                if let Some(ref publisher) = self.incremental_publisher {
                    self.send_with_publisher(publisher, &msg, 2048, "incremental")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::TradeInfo as u32 => {
                if let Some(ref publisher) = self.trade_publisher {
                    self.send_with_publisher(publisher, &msg, 1024, "trade")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::Kline as u32 => {
                if let Some(ref publisher) = self.kline_publisher {
                    self.send_with_publisher(publisher, &msg, 512, "kline")
                } else {
                    warn!("Kline publisher is None, dropping message");
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::LiquidationOrder as u32 ||
                t == crate::mkt_msg::MktMsgType::MarkPrice as u32 ||
                t == crate::mkt_msg::MktMsgType::IndexPrice as u32 ||
                t == crate::mkt_msg::MktMsgType::FundingRate as u32 => {
                if let Some(ref publisher) = self.derivatives_publisher {
                    self.send_with_publisher(publisher, &msg, 1024, "derivatives")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::AskBidSpread as u32 => {
                if let Some(ref publisher) = self.ask_bid_spread_publisher {
                    self.send_with_publisher(publisher, &msg, 512, "ask_bid_spread")
                } else {
                    false
                }
            }
            t if t == crate::mkt_msg::MktMsgType::TimeSignal as u32 => {
                if let Some(ref publisher) = self.signal_publisher {
                    self.send_with_publisher(publisher, &msg, 256, "signal")
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
                t if t == crate::mkt_msg::MktMsgType::OrderBookInc as u32 => self.incremental_count += 1,
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
                t if t == crate::mkt_msg::MktMsgType::LiquidationOrder as u32 ||
                    t == crate::mkt_msg::MktMsgType::MarkPrice as u32 ||
                    t == crate::mkt_msg::MktMsgType::IndexPrice as u32 ||
                    t == crate::mkt_msg::MktMsgType::FundingRate as u32 => self.derivatives_count += 1,
                t if t == crate::mkt_msg::MktMsgType::AskBidSpread as u32 => self.ask_bid_spread_count += 1,
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
            warn!("Message size {} exceeds max size {} for {}", msg.len(), max_size, label);
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
        // 打印统计信息
        info!("IceOryx stats: total: {}, inc: {}, trade: {}, kline: {}, derivatives: {}, spread: {}, signal: {}, dropped: {}",
              self.message_count,
              self.incremental_count,
              self.trade_count,
              self.kline_count,
              self.derivatives_count,
              self.ask_bid_spread_count,
              self.signal_count,
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
    }
}

impl Drop for IceOryxForwarder {
    fn drop(&mut self) {
        info!("IceOryx forwarder stopping...");
        info!("IceOryx forwarder stopped successfully");
    }
}
