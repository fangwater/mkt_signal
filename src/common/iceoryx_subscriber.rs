use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::service::ipc;
use log::{debug, info};
use std::collections::HashMap;

/// 支持的频道类型
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ChannelType {
    Incremental,
    Trade,
    Kline,
    Derivatives,
    AskBidSpread,
    Signal,
}

impl ChannelType {
    pub fn as_str(&self) -> &str {
        match self {
            ChannelType::Incremental => "incremental",
            ChannelType::Trade => "trade",
            ChannelType::Kline => "kline",
            ChannelType::Derivatives => "derivatives",
            ChannelType::AskBidSpread => "ask_bid_spread",
            ChannelType::Signal => "signal",
        }
    }
    
    /// 获取频道的最大消息大小
    pub fn max_size(&self) -> usize {
        match self {
            ChannelType::Incremental => 16384,
            ChannelType::Trade => 64,
            ChannelType::Kline => 128,
            ChannelType::Derivatives => 128,
            ChannelType::AskBidSpread => 64,
            ChannelType::Signal => 64,
        }
    }
}

/// 订阅参数
#[derive(Debug, Clone)]
pub struct SubscribeParams {
    pub exchange: String,
    pub channel: ChannelType,
}

/// 订阅器枚举，包含不同大小的具体类型
enum SubscriberEnum {
    Size64(Subscriber<ipc::Service, [u8; 64], ()>),
    Size128(Subscriber<ipc::Service, [u8; 128], ()>),
    Size16384(Subscriber<ipc::Service, [u8; 16384], ()>),
}

impl SubscriberEnum {
    fn receive_msg(&self) -> Result<Option<Bytes>> {
        match self {
            SubscriberEnum::Size64(sub) => Self::receive_from_subscriber(sub),
            SubscriberEnum::Size128(sub) => Self::receive_from_subscriber(sub),
            SubscriberEnum::Size16384(sub) => Self::receive_from_subscriber(sub),
        }
    }
    
    fn receive_from_subscriber<const SIZE: usize>(
        subscriber: &Subscriber<ipc::Service, [u8; SIZE], ()>
    ) -> Result<Option<Bytes>> {
        match subscriber.receive()? {
            Some(sample) => {
                let payload = sample.payload();
                // 找到实际消息长度（去掉尾部的0）
                let actual_len = payload.iter().rposition(|&x| x != 0)
                    .map(|pos| pos + 1)
                    .unwrap_or(0);
                
                if actual_len > 0 {
                    Ok(Some(Bytes::copy_from_slice(&payload[..actual_len])))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

/// 多频道订阅器
pub struct MultiChannelSubscriber {
    node: Node<ipc::Service>,
    subscribers: HashMap<String, SubscriberEnum>,
    // 用于轮询的索引和键列表
    poll_keys: Vec<String>,
    poll_index: usize,
    stats: SubscriberStats,
}

/// 统计信息
#[derive(Debug, Default)]
pub struct SubscriberStats {
    pub messages_received: u64,
    pub messages_by_channel: HashMap<String, u64>,
}

impl MultiChannelSubscriber {
    /// 创建一个新的多频道订阅器
    pub fn new(node_name: &str) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(node_name)?)
            .create::<ipc::Service>()?;
        
        Ok(Self {
            node,
            subscribers: HashMap::new(),
            poll_keys: Vec::new(),
            poll_index: 0,
            stats: SubscriberStats::default(),
        })
    }
    
    /// 订阅多个频道
    pub fn subscribe_channels(&mut self, params: Vec<SubscribeParams>) -> Result<()> {
        for param in params {
            self.subscribe_single(param)?;
        }
        Ok(())
    }
    
    /// 订阅单个频道
    pub fn subscribe_single(&mut self, param: SubscribeParams) -> Result<()> {
        let service_name: String = format!("data_pubs/{}/{}", 
                                  param.exchange, 
                                  param.channel.as_str());
        
        let key = format!("{}_{}", param.exchange, param.channel.as_str());
        
        // 如果已经订阅，跳过
        if self.subscribers.contains_key(&key) {
            debug!("Already subscribed to {}", service_name);
            return Ok(());
        }
        
        // 根据频道类型创建对应大小的订阅器
        let subscriber_enum = match param.channel {
            ChannelType::Incremental => {
                let service = self.node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; 16384]>()
                    .open_or_create()?;
                let subscriber = service
                    .subscriber_builder()
                    .create()?;
                SubscriberEnum::Size16384(subscriber)
            }
            ChannelType::Trade | ChannelType::AskBidSpread | ChannelType::Signal => {
                let service = self.node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; 64]>()
                    .open_or_create()?;
                let subscriber = service
                    .subscriber_builder()
                    .create()?;
                SubscriberEnum::Size64(subscriber)
            }
            ChannelType::Kline | ChannelType::Derivatives => {
                let service = self.node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; 128]>()
                    .open_or_create()?;
                let subscriber = service
                    .subscriber_builder()
                    .create()?;
                SubscriberEnum::Size128(subscriber)
            }
        };
        
        self.subscribers.insert(key.clone(), subscriber_enum);
        self.poll_keys.push(key);
        
        info!("Subscribed to {}", service_name);
        Ok(())
    }
    
    /// 轮询所有订阅的消息，尽量抽干，每次最多返回 max_msgs 条（None 表示不限制）
    /// 返回的是消息字节数据，调用者需要根据消息类型自行解析
    pub fn poll_msgs(&mut self, max_msgs: Option<usize>) -> Vec<Bytes> {
        let max_msgs = max_msgs.unwrap_or(usize::MAX);
        let mut messages = Vec::new();

        if self.poll_keys.is_empty() {
            return messages;
        }

        let total_channels = self.poll_keys.len();
        // 连续做完整轮询，直到没有任何频道产出新消息，或达到上限
        loop {
            let mut made_progress = false;

            for _ in 0..total_channels {
                let key = &self.poll_keys[self.poll_index];

                if let Some(subscriber) = self.subscribers.get(key) {
                    // 在同一频道内尽量多拿几条，直到为空或达到上限
                    loop {
                        match subscriber.receive_msg() {
                            Ok(Some(msg)) => {
                                messages.push(msg);
                                self.stats.messages_received += 1;
                                *self
                                    .stats
                                    .messages_by_channel
                                    .entry(key.clone())
                                    .or_insert(0) += 1;
                                made_progress = true;
                                if messages.len() >= max_msgs {
                                    return messages;
                                }
                            }
                            Ok(None) => break, // 该频道当前没有更多消息
                            Err(_) => break,   // 读取错误，跳出该频道
                        }
                    }
                }

                // 轮转到下一个频道
                self.poll_index = (self.poll_index + 1) % total_channels;
            }

            if !made_progress {
                break; // 一轮下来没有任何新消息，结束
            }
        }

        messages
    }
    
    /// 轮询单个频道的消息，最多返回max_msgs条消息
    pub fn poll_channel(&mut self, exchange: &str, channel: &ChannelType, max_msgs: Option<usize>) -> Vec<Bytes> {
        let max_msgs = max_msgs.unwrap_or(16);
        let key = format!("{}_{}", exchange, channel.as_str());
        let mut messages = Vec::new();
        
        if let Some(subscriber) = self.subscribers.get(&key) {
            for _ in 0..max_msgs {
                match subscriber.receive_msg() {
                    Ok(Some(msg)) => {
                        messages.push(msg);
                        
                        // 更新统计
                        self.stats.messages_received += 1;
                        *self.stats.messages_by_channel.entry(key.clone()).or_insert(0) += 1;
                    }
                    Ok(None) => break,  // 没有更多消息
                    Err(_) => break,
                }
            }
        }
        
        messages
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> &SubscriberStats {
        &self.stats
    }
    
    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = SubscriberStats::default();
    }
    
    /// 获取已订阅的频道列表
    pub fn get_subscribed_channels(&self) -> Vec<String> {
        self.poll_keys.clone()
    }
}

/// 便捷函数：创建一个订阅器并订阅指定频道
pub fn create_subscriber(
    node_name: &str,
    subscriptions: Vec<(String, ChannelType)>
) -> Result<MultiChannelSubscriber> {
    let mut subscriber = MultiChannelSubscriber::new(node_name)?;
    
    let params: Vec<SubscribeParams> = subscriptions
        .into_iter()
        .map(|(exchange, channel)| SubscribeParams { exchange, channel })
        .collect();
    
    subscriber.subscribe_channels(params)?;
    
    Ok(subscriber)
}
