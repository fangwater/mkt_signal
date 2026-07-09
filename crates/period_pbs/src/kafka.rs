use std::ffi::{CStr, CString};
use std::io::Read;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

use anyhow::{Context, Result};
use clap::ValueEnum;
use flate2::read::ZlibDecoder;
use prost::Message;
use rdkafka::bindings as rdsys;
use serde::Deserialize;

use crate::pb;

const RD_KAFKA_PARTITION_UA: i32 = -1;

unsafe extern "C" {
    fn rd_kafka_conf_set_resolve_cb(
        conf: *mut rdsys::rd_kafka_conf_t,
        resolve_cb: Option<
            unsafe extern "C" fn(
                node: *const c_char,
                service: *const c_char,
                hints: *const libc::addrinfo,
                res: *mut *mut libc::addrinfo,
                opaque: *mut c_void,
            ) -> c_int,
        >,
    );
}

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum PayloadCompressionMode {
    Auto,
    Zlib,
    Snappy,
    None,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerAddrRewrite {
    pub from_host: String,
    pub to_host: String,
    #[serde(default)]
    pub to_port: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaConsumerConfig {
    pub topics: Vec<String>,
    pub brokers: String,
    pub group_id: String,
    pub client_id: String,
    pub offset_reset: String,
    pub payload_compression: PayloadCompressionMode,
    pub poll_timeout_ms: u64,
    pub max_messages: Option<u64>,
    pub print_symbols: bool,
    pub enable_auto_commit: bool,
    pub enable_partition_eof: bool,
    pub security_protocol: String,
    pub sasl_mechanisms: String,
    pub sasl_username: String,
    pub sasl_password: String,
    pub max_partition_fetch_bytes: usize,
    pub fetch_max_bytes: usize,
    pub fetch_wait_max_ms: u64,
    pub socket_receive_buffer_bytes: usize,
    pub receive_message_max_bytes: usize,
    pub session_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_poll_interval_ms: u64,
    pub metadata_timeout_ms: u64,
    pub watermark_timeout_ms: u64,
    pub broker_addr_rewrites: Vec<BrokerAddrRewrite>,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            topics: vec!["binance-futures".to_string(), "binance-spot".to_string()],
            brokers: "156.231.137.189:1501".to_string(),
            group_id: "period_pbs_kafka_helper".to_string(),
            client_id: "period_pbs_kafka_helper".to_string(),
            offset_reset: "latest".to_string(),
            payload_compression: PayloadCompressionMode::Zlib,
            poll_timeout_ms: 3000,
            max_messages: None,
            print_symbols: false,
            enable_auto_commit: false,
            enable_partition_eof: false,
            security_protocol: "none".to_string(),
            sasl_mechanisms: "PLAIN".to_string(),
            sasl_username: String::new(),
            sasl_password: String::new(),
            max_partition_fetch_bytes: 10_485_760,
            fetch_max_bytes: 10_485_760,
            fetch_wait_max_ms: 100,
            socket_receive_buffer_bytes: 52_428_800,
            receive_message_max_bytes: 10_486_272,
            session_timeout_ms: 60_000,
            heartbeat_interval_ms: 3_000,
            max_poll_interval_ms: 300_000,
            metadata_timeout_ms: 5_000,
            watermark_timeout_ms: 5_000,
            broker_addr_rewrites: Vec::new(),
        }
    }
}

impl KafkaConsumerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.topics.is_empty() {
            anyhow::bail!("Kafka consumer config has empty topics");
        }
        Ok(())
    }
}

pub fn format_rewrites(rewrites: &[BrokerAddrRewrite]) -> String {
    if rewrites.is_empty() {
        return "none".to_string();
    }
    rewrites
        .iter()
        .map(|rewrite| match rewrite.to_port {
            Some(port) => format!("{}->{}:{}", rewrite.from_host, rewrite.to_host, port),
            None => format!("{}->{}", rewrite.from_host, rewrite.to_host),
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug)]
struct ResolveRewriteState {
    rewrites: Vec<BrokerAddrRewrite>,
}

unsafe extern "C" fn resolve_with_rewrite(
    node: *const c_char,
    service: *const c_char,
    hints: *const libc::addrinfo,
    res: *mut *mut libc::addrinfo,
    opaque: *mut c_void,
) -> c_int {
    if node.is_null() && service.is_null() && hints.is_null() {
        if !res.is_null() && unsafe { !(*res).is_null() } {
            unsafe { libc::freeaddrinfo(*res) };
        }
        return 0;
    }

    if node.is_null() || service.is_null() || res.is_null() {
        return libc::EAI_FAIL;
    }

    let node_str = unsafe { CStr::from_ptr(node) }.to_string_lossy();
    let service_str = unsafe { CStr::from_ptr(service) }.to_string_lossy();
    let mut target_node = node_str.as_ref().to_string();
    let mut target_service = service_str.as_ref().to_string();

    if !opaque.is_null() {
        let state = unsafe { &*(opaque as *const ResolveRewriteState) };
        if let Some(rewrite) = state
            .rewrites
            .iter()
            .find(|rewrite| rewrite.from_host == node_str.as_ref())
        {
            target_node = rewrite.to_host.clone();
            if let Some(port) = rewrite.to_port {
                target_service = port.to_string();
            }
            log::info!(
                "Kafka broker address rewrite {}:{} -> {}:{}",
                node_str,
                service_str,
                target_node,
                target_service
            );
        }
    }

    let target_node = match CString::new(target_node) {
        Ok(value) => value,
        Err(_) => return libc::EAI_FAIL,
    };
    let target_service = match CString::new(target_service) {
        Ok(value) => value,
        Err(_) => return libc::EAI_FAIL,
    };

    unsafe { libc::getaddrinfo(target_node.as_ptr(), target_service.as_ptr(), hints, res) }
}

pub struct RawKafkaConsumer {
    rk: *mut rdsys::rd_kafka_t,
    _resolve_state: Box<ResolveRewriteState>,
}

impl RawKafkaConsumer {
    pub fn new(config: &KafkaConsumerConfig) -> Result<Self> {
        config.validate()?;
        let mut resolve_state = Box::new(ResolveRewriteState {
            rewrites: config.broker_addr_rewrites.clone(),
        });
        let conf = unsafe { rdsys::rd_kafka_conf_new() };
        if conf.is_null() {
            anyhow::bail!("rd_kafka_conf_new returned null");
        }

        let create_result = (|| -> Result<*mut rdsys::rd_kafka_t> {
            set_kafka_conf(conf, "bootstrap.servers", &config.brokers)?;
            set_kafka_conf(conf, "group.id", &config.group_id)?;
            set_kafka_conf(conf, "client.id", &config.client_id)?;
            set_kafka_conf(conf, "auto.offset.reset", &config.offset_reset)?;
            set_kafka_conf(
                conf,
                "enable.auto.commit",
                if config.enable_auto_commit {
                    "true"
                } else {
                    "false"
                },
            )?;
            set_kafka_conf(
                conf,
                "enable.partition.eof",
                if config.enable_partition_eof {
                    "true"
                } else {
                    "false"
                },
            )?;
            set_kafka_conf(
                conf,
                "session.timeout.ms",
                &config.session_timeout_ms.to_string(),
            )?;
            set_kafka_conf(
                conf,
                "heartbeat.interval.ms",
                &config.heartbeat_interval_ms.to_string(),
            )?;
            set_kafka_conf(
                conf,
                "max.poll.interval.ms",
                &config.max_poll_interval_ms.to_string(),
            )?;
            set_kafka_conf(
                conf,
                "max.partition.fetch.bytes",
                &config.max_partition_fetch_bytes.to_string(),
            )?;
            set_kafka_conf(conf, "fetch.max.bytes", &config.fetch_max_bytes.to_string())?;
            set_kafka_conf(
                conf,
                "fetch.wait.max.ms",
                &config.fetch_wait_max_ms.to_string(),
            )?;
            set_kafka_conf(
                conf,
                "socket.receive.buffer.bytes",
                &config.socket_receive_buffer_bytes.to_string(),
            )?;
            set_kafka_conf(
                conf,
                "receive.message.max.bytes",
                &config.receive_message_max_bytes.to_string(),
            )?;

            let security_protocol = config.security_protocol.trim();
            let uses_sasl = security_protocol.to_ascii_uppercase().contains("SASL");
            if !security_protocol.is_empty() && !security_protocol.eq_ignore_ascii_case("none") {
                let protocol = if security_protocol.eq_ignore_ascii_case("plaintext") {
                    "PLAINTEXT"
                } else {
                    security_protocol
                };
                set_kafka_conf(conf, "security.protocol", protocol)?;
            }
            if uses_sasl && !config.sasl_mechanisms.trim().is_empty() {
                set_kafka_conf(conf, "sasl.mechanisms", config.sasl_mechanisms.trim())?;
            }
            if uses_sasl && !config.sasl_username.trim().is_empty() {
                set_kafka_conf(conf, "sasl.username", config.sasl_username.trim())?;
            }
            if uses_sasl && !config.sasl_password.trim().is_empty() {
                set_kafka_conf(conf, "sasl.password", config.sasl_password.trim())?;
            }

            if !resolve_state.rewrites.is_empty() {
                unsafe {
                    rdsys::rd_kafka_conf_set_opaque(
                        conf,
                        (&mut *resolve_state as *mut ResolveRewriteState).cast::<c_void>(),
                    );
                    rd_kafka_conf_set_resolve_cb(conf, Some(resolve_with_rewrite));
                }
            }

            let mut errbuf = vec![0 as c_char; 512];
            let rk = unsafe {
                rdsys::rd_kafka_new(
                    rdsys::rd_kafka_type_t::RD_KAFKA_CONSUMER,
                    conf,
                    errbuf.as_mut_ptr(),
                    errbuf.len(),
                )
            };
            if rk.is_null() {
                anyhow::bail!("rd_kafka_new failed: {}", c_buf_to_string(&errbuf));
            }
            Ok(rk)
        })();

        let rk = match create_result {
            Ok(rk) => rk,
            Err(err) => {
                unsafe { rdsys::rd_kafka_conf_destroy(conf) };
                return Err(err);
            }
        };

        let poll_set_err = unsafe { rdsys::rd_kafka_poll_set_consumer(rk) };
        if poll_set_err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let err = kafka_error_string(poll_set_err);
            unsafe { rdsys::rd_kafka_destroy(rk) };
            anyhow::bail!("rd_kafka_poll_set_consumer failed: {}", err);
        }

        let subscribe_result = subscribe_topics(rk, &config.topics);
        if let Err(err) = subscribe_result {
            unsafe { rdsys::rd_kafka_destroy(rk) };
            return Err(err);
        }

        Ok(Self {
            rk,
            _resolve_state: resolve_state,
        })
    }

    pub fn poll(&self, timeout_ms: u64) -> Option<Result<RawKafkaMessage>> {
        let timeout_ms = timeout_ms.min(c_int::MAX as u64) as c_int;
        let msg = unsafe { rdsys::rd_kafka_consumer_poll(self.rk, timeout_ms) };
        if msg.is_null() {
            return None;
        }

        let result = unsafe { self.message_to_result(msg) };
        unsafe { rdsys::rd_kafka_message_destroy(msg) };
        Some(result)
    }

    pub fn query_topic_watermarks(
        &self,
        topics: &[String],
        metadata_timeout_ms: u64,
        watermark_timeout_ms: u64,
    ) -> Result<Vec<KafkaPartitionWatermark>> {
        let mut watermarks = Vec::new();
        for topic in topics {
            let partitions = self
                .fetch_topic_partitions(topic, metadata_timeout_ms)
                .with_context(|| format!("fetch metadata partitions for topic={topic}"))?;
            for partition in partitions {
                watermarks.push(
                    self.query_partition_watermark(topic, partition, watermark_timeout_ms)
                        .with_context(|| {
                            format!("query watermark topic={topic} partition={partition}")
                        })?,
                );
            }
        }
        watermarks.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        Ok(watermarks)
    }

    fn fetch_topic_partitions(&self, topic: &str, timeout_ms: u64) -> Result<Vec<i32>> {
        let topic_c = CString::new(topic).with_context(|| format!("invalid topic {topic}"))?;
        let rkt = unsafe { rdsys::rd_kafka_topic_new(self.rk, topic_c.as_ptr(), ptr::null_mut()) };
        if rkt.is_null() {
            anyhow::bail!("rd_kafka_topic_new returned null for topic={topic}");
        }

        let mut metadata_ptr: *const rdsys::rd_kafka_metadata = ptr::null();
        let timeout_ms = timeout_ms.min(c_int::MAX as u64) as c_int;
        let metadata_result =
            unsafe { rdsys::rd_kafka_metadata(self.rk, 0, rkt, &mut metadata_ptr, timeout_ms) };
        unsafe { rdsys::rd_kafka_topic_destroy(rkt) };

        if metadata_result != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            anyhow::bail!(
                "rd_kafka_metadata failed for topic={}: {}",
                topic,
                kafka_error_string(metadata_result)
            );
        }
        if metadata_ptr.is_null() {
            anyhow::bail!("rd_kafka_metadata returned null for topic={topic}");
        }

        let result = unsafe { extract_topic_partitions(metadata_ptr, topic) };
        unsafe { rdsys::rd_kafka_metadata_destroy(metadata_ptr) };
        result
    }

    fn query_partition_watermark(
        &self,
        topic: &str,
        partition: i32,
        timeout_ms: u64,
    ) -> Result<KafkaPartitionWatermark> {
        let topic_c = CString::new(topic).with_context(|| format!("invalid topic {topic}"))?;
        let mut low = -1i64;
        let mut high = -1i64;
        let timeout_ms = timeout_ms.min(c_int::MAX as u64) as c_int;
        let err = unsafe {
            rdsys::rd_kafka_query_watermark_offsets(
                self.rk,
                topic_c.as_ptr(),
                partition,
                &mut low,
                &mut high,
                timeout_ms,
            )
        };
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            anyhow::bail!("{}", kafka_error_string(err));
        }

        Ok(KafkaPartitionWatermark {
            topic: topic.to_string(),
            partition,
            low,
            high,
        })
    }

    unsafe fn message_to_result(
        &self,
        msg: *mut rdsys::rd_kafka_message_t,
    ) -> Result<RawKafkaMessage> {
        let message = unsafe { &*msg };
        if message.err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            anyhow::bail!("Kafka message error: {}", kafka_error_string(message.err));
        }
        if message.payload.is_null() && message.len > 0 {
            anyhow::bail!("Kafka message has null payload with len={}", message.len);
        }

        let topic = if message.rkt.is_null() {
            String::new()
        } else {
            let topic_ptr = unsafe { rdsys::rd_kafka_topic_name(message.rkt) };
            unsafe { CStr::from_ptr(topic_ptr) }
                .to_string_lossy()
                .into_owned()
        };
        let payload = if message.len == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(message.payload.cast::<u8>(), message.len) }.to_vec()
        };

        Ok(RawKafkaMessage {
            topic,
            partition: message.partition,
            offset: message.offset,
            payload,
        })
    }
}

impl Drop for RawKafkaConsumer {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_consumer_close(self.rk);
            rdsys::rd_kafka_destroy(self.rk);
        }
    }
}

pub struct RawKafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct KafkaPartitionWatermark {
    pub topic: String,
    pub partition: i32,
    /// Earliest currently available offset for this partition.
    pub low: i64,
    /// Next offset after the partition's latest record.
    pub high: i64,
}

unsafe fn extract_topic_partitions(
    metadata_ptr: *const rdsys::rd_kafka_metadata,
    requested_topic: &str,
) -> Result<Vec<i32>> {
    let metadata = unsafe { &*metadata_ptr };
    if metadata.topic_cnt < 0 {
        anyhow::bail!("metadata returned negative topic count");
    }
    if metadata.topic_cnt > 0 && metadata.topics.is_null() {
        anyhow::bail!("metadata returned null topics count={}", metadata.topic_cnt);
    }
    let topic_count = metadata.topic_cnt as usize;
    let topics = if topic_count == 0 {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts(metadata.topics, topic_count) }
    };
    let Some(topic_meta) = topics.iter().find(|topic_meta| {
        if topic_meta.topic.is_null() {
            return false;
        }
        unsafe { CStr::from_ptr(topic_meta.topic) }.to_string_lossy() == requested_topic
    }) else {
        anyhow::bail!("metadata response did not include topic={requested_topic}");
    };

    if topic_meta.err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        anyhow::bail!(
            "metadata error for topic={}: {}",
            requested_topic,
            kafka_error_string(topic_meta.err)
        );
    }
    if topic_meta.partition_cnt < 0 {
        anyhow::bail!(
            "metadata returned negative partition count for topic={}",
            requested_topic
        );
    }
    if topic_meta.partition_cnt > 0 && topic_meta.partitions.is_null() {
        anyhow::bail!(
            "metadata returned null partitions for topic={} count={}",
            requested_topic,
            topic_meta.partition_cnt
        );
    }

    let partition_count = topic_meta.partition_cnt as usize;
    let partitions = if partition_count == 0 {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts(topic_meta.partitions, partition_count) }
    };
    let mut ids = Vec::with_capacity(partitions.len());
    for partition in partitions {
        if partition.err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            anyhow::bail!(
                "metadata error for topic={} partition={}: {}",
                requested_topic,
                partition.id,
                kafka_error_string(partition.err)
            );
        }
        ids.push(partition.id);
    }
    ids.sort_unstable();
    Ok(ids)
}

fn subscribe_topics(rk: *mut rdsys::rd_kafka_t, topics: &[String]) -> Result<()> {
    let list = unsafe { rdsys::rd_kafka_topic_partition_list_new(topics.len() as c_int) };
    if list.is_null() {
        anyhow::bail!("rd_kafka_topic_partition_list_new returned null");
    }

    let result = (|| -> Result<()> {
        let topic_names: Vec<CString> = topics
            .iter()
            .map(|topic| {
                CString::new(topic.as_str()).with_context(|| format!("invalid topic {topic}"))
            })
            .collect::<Result<_>>()?;
        for topic in &topic_names {
            let elem = unsafe {
                rdsys::rd_kafka_topic_partition_list_add(
                    list,
                    topic.as_ptr(),
                    RD_KAFKA_PARTITION_UA,
                )
            };
            if elem.is_null() {
                anyhow::bail!("rd_kafka_topic_partition_list_add returned null");
            }
        }
        let err = unsafe { rdsys::rd_kafka_subscribe(rk, list) };
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            anyhow::bail!("rd_kafka_subscribe failed: {}", kafka_error_string(err));
        }
        Ok(())
    })();

    unsafe { rdsys::rd_kafka_topic_partition_list_destroy(list) };
    result
}

fn set_kafka_conf(conf: *mut rdsys::rd_kafka_conf_t, key: &str, value: &str) -> Result<()> {
    let key_c = CString::new(key).with_context(|| format!("invalid Kafka config key {key}"))?;
    let value_c =
        CString::new(value).with_context(|| format!("invalid Kafka config value for key {key}"))?;
    let mut errbuf = vec![0 as c_char; 512];
    let ret = unsafe {
        rdsys::rd_kafka_conf_set(
            conf,
            key_c.as_ptr(),
            value_c.as_ptr(),
            errbuf.as_mut_ptr(),
            errbuf.len(),
        )
    };
    if ret != rdsys::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK {
        anyhow::bail!(
            "set Kafka config {}={} failed: {}",
            key,
            value,
            c_buf_to_string(&errbuf)
        );
    }
    Ok(())
}

fn kafka_error_string(err: rdsys::rd_kafka_resp_err_t) -> String {
    let ptr = unsafe { rdsys::rd_kafka_err2str(err) };
    if ptr.is_null() {
        format!("{:?}", err)
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

fn c_buf_to_string(buf: &[c_char]) -> String {
    let nul = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    let bytes = &buf[..nul];
    let bytes = unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), bytes.len()) };
    String::from_utf8_lossy(bytes).into_owned()
}

pub fn decode_period_payload(
    payload: &[u8],
    compression: PayloadCompressionMode,
) -> Result<(&'static str, usize, pb::PeriodMessage)> {
    match compression {
        PayloadCompressionMode::Auto => {
            let mut errors = Vec::new();
            for mode in [
                PayloadCompressionMode::Zlib,
                PayloadCompressionMode::Snappy,
                PayloadCompressionMode::None,
            ] {
                match decode_period_payload(payload, mode) {
                    Ok(ok) => return Ok(ok),
                    Err(err) => errors.push(format!("{:?}: {:#}", mode, err)),
                }
            }
            anyhow::bail!("auto decode failed: {}", errors.join("; "))
        }
        PayloadCompressionMode::Zlib => {
            let bytes = decompress_zlib(payload)?;
            let period =
                pb::PeriodMessage::decode(bytes.as_slice()).context("decode zlib protobuf")?;
            Ok(("zlib", bytes.len(), period))
        }
        PayloadCompressionMode::Snappy => {
            let bytes = snap::raw::Decoder::new()
                .decompress_vec(payload)
                .context("snappy raw decompress")?;
            let period =
                pb::PeriodMessage::decode(bytes.as_slice()).context("decode snappy protobuf")?;
            Ok(("snappy", bytes.len(), period))
        }
        PayloadCompressionMode::None => {
            let period = pb::PeriodMessage::decode(payload).context("decode raw protobuf")?;
            Ok(("none", payload.len(), period))
        }
    }
}

fn decompress_zlib(payload: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(payload);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).context("zlib decompress")?;
    Ok(out)
}
