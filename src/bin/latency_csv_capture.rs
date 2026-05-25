use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use mkt_signal::rolling_metrics::latency_snapshot::{
    ACTION_ID_CANCEL, ACTION_ID_MARKET_DATA, ACTION_ID_NEW, LATENCY_SNAPSHOT_MAX_BUCKETS,
    LATENCY_SNAPSHOT_MSG_TYPE, LATENCY_SNAPSHOT_PAYLOAD_LEN, LATENCY_SNAPSHOT_SCHEMA_VER,
    METRIC_ID_DOWNLINK, METRIC_ID_IPC_TO_WS, METRIC_ID_RTT, METRIC_ID_SERVER, METRIC_ID_SPREAD_E2E,
    METRIC_ID_SPREAD_NET, METRIC_ID_UPLINK,
};
use serde::Deserialize;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CSV_HEADER: &str = "recv_ts_us,recv_hour,topic,msg_type,schema_ver,venue_id,snapshot_time_us,snapshot_age_us,raw_n_buckets,bucket,metric_id,metric,action_id,action,n,p50_us,p90_us,p95_us,p99_us\n";

#[derive(Parser, Debug)]
#[command(name = "latency_csv_capture")]
#[command(
    about = "Subscribe to latency_stable_monitor ZMQ raw snapshots and write hourly CSV slices"
)]
struct Args {
    /// TOML config path.
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct CaptureConfig {
    /// Short deployment label used by scripts and logs, e.g. sg.
    instance: String,

    /// ZMQ publisher IP. Ignored when endpoint is set.
    ip: String,

    /// ZMQ publisher port. Ignored when endpoint is set.
    port: u16,

    /// Optional full ZMQ endpoint, e.g. tcp://47.131.162.78:6370.
    endpoint: Option<String>,

    /// ZMQ SUB topic prefix. Empty string subscribes all topics.
    topic: String,

    /// Output root directory. Files are written under <out_dir>/<topic>/<YYYYMMDD_HH>.csv.
    out_dir: PathBuf,

    /// Flush after every N received snapshots.
    flush_every: u64,

    /// ZMQ receive timeout in milliseconds, used to periodically flush and print stats.
    recv_timeout_ms: i32,

    /// Log stats every N seconds.
    stats_secs: u64,

    /// Exit after this many received snapshots. 0 means run forever.
    limit: u64,

    /// Also write spread_net rows. By default spread_pbs CSV keeps only spread_e2e.
    include_spread_net: bool,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            instance: "default".to_string(),
            ip: "127.0.0.1".to_string(),
            port: 6370,
            endpoint: None,
            topic: "latency.".to_string(),
            out_dir: PathBuf::from("data/latency_csv/default"),
            flush_every: 1,
            recv_timeout_ms: 1000,
            stats_secs: 30,
            limit: 0,
            include_spread_net: false,
        }
    }
}

impl CaptureConfig {
    fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let cfg: Self = toml::from_str(&text)
            .with_context(|| format!("failed to parse TOML config {}", path.display()))?;
        cfg.validate(path)?;
        Ok(cfg)
    }

    fn validate(&self, path: &Path) -> Result<()> {
        if self.instance.trim().is_empty() {
            bail!("config {} has empty instance", path.display());
        }
        if self
            .endpoint
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
            && self.ip.trim().is_empty()
        {
            bail!("config {} has empty ip and endpoint", path.display());
        }
        if self
            .endpoint
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
            && self.port == 0
        {
            bail!("config {} has invalid port=0", path.display());
        }
        if self.out_dir.as_os_str().is_empty() {
            bail!("config {} has empty out_dir", path.display());
        }
        if self.recv_timeout_ms <= 0 {
            bail!(
                "config {} has invalid recv_timeout_ms={}",
                path.display(),
                self.recv_timeout_ms
            );
        }
        Ok(())
    }

    fn endpoint(&self) -> String {
        match self
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(endpoint) => endpoint.to_string(),
            None => format!("tcp://{}:{}", self.ip.trim(), self.port),
        }
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let cfg = CaptureConfig::load(&args.config)?;
    let endpoint = cfg.endpoint();

    fs::create_dir_all(&cfg.out_dir)
        .with_context(|| format!("failed to create out_dir {}", cfg.out_dir.display()))?;

    let ctx = zmq::Context::new();
    let sub = ctx
        .socket(zmq::SUB)
        .context("failed to create ZMQ SUB socket")?;
    sub.set_rcvtimeo(cfg.recv_timeout_ms)
        .context("failed to set ZMQ receive timeout")?;
    sub.set_subscribe(cfg.topic.as_bytes())
        .with_context(|| format!("failed to subscribe topic prefix {:?}", cfg.topic))?;
    sub.connect(endpoint.trim())
        .with_context(|| format!("failed to connect ZMQ SUB to {}", endpoint.trim()))?;

    log::info!(
        "latency_csv_capture started: config={} instance={} endpoint={} topic_prefix={:?} out_dir={} flush_every={} recv_timeout_ms={} include_spread_net={}",
        args.config.display(),
        cfg.instance,
        endpoint.trim(),
        cfg.topic,
        cfg.out_dir.display(),
        cfg.flush_every,
        cfg.recv_timeout_ms,
        cfg.include_spread_net,
    );

    let mut writer = HourlyCsvWriter::new(cfg.out_dir.clone())?;
    let mut received = 0u64;
    let mut rows = 0u64;
    let mut decode_errors = 0u64;
    let mut last_stats = std::time::Instant::now();

    loop {
        if cfg.limit > 0 && received >= cfg.limit {
            break;
        }

        match sub.recv_multipart(0) {
            Ok(parts) => {
                if parts.len() != 2 {
                    decode_errors = decode_errors.saturating_add(1);
                    log::warn!("unexpected ZMQ multipart frame count: {}", parts.len());
                    continue;
                }
                let topic = String::from_utf8_lossy(&parts[0]).to_string();
                let payload = &parts[1];
                let recv_ts_us = now_us();
                received = received.saturating_add(1);

                match decode_snapshot(payload) {
                    Ok(snapshot) => {
                        let written = writer.write_snapshot(
                            recv_ts_us,
                            &topic,
                            &snapshot,
                            cfg.include_spread_net,
                        )?;
                        rows = rows.saturating_add(written as u64);
                        if cfg.flush_every > 0 && received % cfg.flush_every == 0 {
                            writer.flush()?;
                        }
                    }
                    Err(err) => {
                        decode_errors = decode_errors.saturating_add(1);
                        log::warn!(
                            "failed to decode latency snapshot: topic={} payload_len={} err={:#}",
                            topic,
                            payload.len(),
                            err
                        );
                    }
                }
            }
            Err(err) => {
                if err != zmq::Error::EAGAIN {
                    return Err(err).context("ZMQ receive failed");
                }
                writer.flush()?;
            }
        }

        if cfg.stats_secs > 0 && last_stats.elapsed() >= Duration::from_secs(cfg.stats_secs) {
            log::info!(
                "latency_csv_capture stats: received={} rows={} decode_errors={} current_file={}",
                received,
                rows,
                decode_errors,
                writer.current_path_display()
            );
            last_stats = std::time::Instant::now();
        }
    }

    writer.flush()?;
    log::info!(
        "latency_csv_capture done: received={} rows={} decode_errors={}",
        received,
        rows,
        decode_errors
    );
    Ok(())
}

struct Snapshot {
    msg_type: u32,
    schema_ver: u32,
    venue_id: u32,
    raw_n_buckets: u32,
    snapshot_time_us: i64,
    buckets: Vec<Bucket>,
}

struct Bucket {
    bucket: usize,
    metric_id: u8,
    action_id: u8,
    n: u64,
    p50_us: i64,
    p90_us: i64,
    p95_us: i64,
    p99_us: i64,
}

struct HourlyCsvWriter {
    out_dir: PathBuf,
    current_key: Option<FileKey>,
    writer: Option<BufWriter<File>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileKey {
    topic_dir: String,
    hour: String,
    path: PathBuf,
}

impl HourlyCsvWriter {
    fn new(out_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            out_dir,
            current_key: None,
            writer: None,
        })
    }

    fn write_snapshot(
        &mut self,
        recv_ts_us: i64,
        topic: &str,
        snapshot: &Snapshot,
        include_spread_net: bool,
    ) -> Result<usize> {
        let hour = hour_string_from_us(recv_ts_us)?;
        let key = FileKey {
            topic_dir: sanitize_topic(topic),
            hour: hour.clone(),
            path: self
                .out_dir
                .join(sanitize_topic(topic))
                .join(format!("{}.csv", hour)),
        };
        self.ensure_writer(key)?;

        let age = recv_ts_us.saturating_sub(snapshot.snapshot_time_us);
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("writer missing after ensure_writer"))?;
        let mut written = 0usize;
        for bucket in &snapshot.buckets {
            if should_skip_bucket(topic, bucket, include_spread_net) {
                continue;
            }
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                recv_ts_us,
                hour,
                csv_escape(topic),
                snapshot.msg_type,
                snapshot.schema_ver,
                snapshot.venue_id,
                snapshot.snapshot_time_us,
                age,
                snapshot.raw_n_buckets,
                bucket.bucket,
                bucket.metric_id,
                metric_name(bucket.metric_id),
                bucket.action_id,
                action_name(bucket.action_id),
                bucket.n,
                bucket.p50_us,
                bucket.p90_us,
                bucket.p95_us,
                bucket.p99_us,
            )?;
            written += 1;
        }
        Ok(written)
    }

    fn ensure_writer(&mut self, key: FileKey) -> Result<()> {
        if self.current_key.as_ref() == Some(&key) {
            return Ok(());
        }
        self.flush()?;
        if let Some(parent) = key.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let new_file = !key.path.exists() || key.path.metadata()?.len() == 0;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&key.path)
            .with_context(|| format!("failed to open csv {}", key.path.display()))?;
        let mut writer = BufWriter::new(file);
        if new_file {
            writer.write_all(CSV_HEADER.as_bytes())?;
        }
        log::info!("latency_csv_capture writing {}", key.path.display());
        self.current_key = Some(key);
        self.writer = Some(writer);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    fn current_path_display(&self) -> String {
        self.current_key
            .as_ref()
            .map(|key| key.path.display().to_string())
            .unwrap_or_else(|| "-".to_string())
    }
}

fn should_skip_bucket(topic: &str, bucket: &Bucket, include_spread_net: bool) -> bool {
    if !topic.starts_with("latency.public.spread_pbs.") {
        return false;
    }
    match bucket.metric_id {
        METRIC_ID_SPREAD_E2E => false,
        METRIC_ID_SPREAD_NET => !include_spread_net,
        _ => false,
    }
}

fn decode_snapshot(payload: &[u8]) -> Result<Snapshot> {
    if payload.len() != LATENCY_SNAPSHOT_PAYLOAD_LEN {
        bail!(
            "unexpected latency payload len={} expected={}",
            payload.len(),
            LATENCY_SNAPSHOT_PAYLOAD_LEN
        );
    }
    let msg_type = read_u32(payload, 0)?;
    let schema_ver = read_u32(payload, 4)?;
    if msg_type != LATENCY_SNAPSHOT_MSG_TYPE {
        bail!(
            "unexpected msg_type={} expected={}",
            msg_type,
            LATENCY_SNAPSHOT_MSG_TYPE
        );
    }
    if schema_ver != LATENCY_SNAPSHOT_SCHEMA_VER {
        bail!(
            "unexpected schema_ver={} expected={}",
            schema_ver,
            LATENCY_SNAPSHOT_SCHEMA_VER
        );
    }

    let venue_id = read_u32(payload, 8)?;
    let raw_n_buckets = read_u32(payload, 12)?;
    let snapshot_time_us = read_i64(payload, 16)?;
    let n_buckets = (raw_n_buckets as usize).min(LATENCY_SNAPSHOT_MAX_BUCKETS);
    let mut buckets = Vec::with_capacity(n_buckets);
    for idx in 0..n_buckets {
        let off = 32 + idx * 48;
        buckets.push(Bucket {
            bucket: idx,
            metric_id: payload[off],
            action_id: payload[off + 1],
            n: read_u64(payload, off + 8)?,
            p50_us: read_i64(payload, off + 16)?,
            p90_us: read_i64(payload, off + 24)?,
            p95_us: read_i64(payload, off + 32)?,
            p99_us: read_i64(payload, off + 40)?,
        });
    }

    Ok(Snapshot {
        msg_type,
        schema_ver,
        venue_id,
        raw_n_buckets,
        snapshot_time_us,
        buckets,
    })
}

fn sanitize_topic(topic: &str) -> String {
    topic
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn metric_name(id: u8) -> &'static str {
    match id {
        METRIC_ID_IPC_TO_WS => "ipc_to_ws",
        METRIC_ID_UPLINK => "uplink",
        METRIC_ID_SERVER => "server",
        METRIC_ID_DOWNLINK => "downlink",
        METRIC_ID_RTT => "rtt",
        METRIC_ID_SPREAD_NET => "spread_net",
        METRIC_ID_SPREAD_E2E => "spread_e2e",
        _ => "unknown",
    }
}

fn action_name(id: u8) -> &'static str {
    match id {
        ACTION_ID_NEW => "new",
        ACTION_ID_CANCEL => "cancel",
        ACTION_ID_MARKET_DATA => "market_data",
        _ => "unknown",
    }
}

fn hour_string_from_us(ts_us: i64) -> Result<String> {
    let secs = ts_us.div_euclid(1_000_000);
    let mut days = secs.div_euclid(86_400);
    let mut sec_of_day = secs.rem_euclid(86_400);
    if sec_of_day < 0 {
        days -= 1;
        sec_of_day += 86_400;
    }
    let (year, month, day) = civil_from_days(days);
    let hour = sec_of_day / 3_600;
    Ok(format!("{:04}{:02}{:02}_{:02}", year, month, day, hour))
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

fn now_us() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

fn read_u32(payload: &[u8], off: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(payload[off..off + 4].try_into()?))
}

fn read_u64(payload: &[u8], off: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(payload[off..off + 8].try_into()?))
}

fn read_i64(payload: &[u8], off: usize) -> Result<i64> {
    Ok(i64::from_le_bytes(payload[off..off + 8].try_into()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_string_uses_utc_hour() {
        assert_eq!(hour_string_from_us(0).unwrap(), "19700101_00");
        assert_eq!(hour_string_from_us(3_599_999_999).unwrap(), "19700101_00");
        assert_eq!(hour_string_from_us(3_600_000_000).unwrap(), "19700101_01");
    }

    #[test]
    fn sanitize_topic_preserves_common_chars() {
        assert_eq!(
            sanitize_topic("latency.private.te.bybit_intra_arb01"),
            "latency.private.te.bybit_intra_arb01"
        );
        assert_eq!(sanitize_topic("a/b c"), "a_b_c");
    }

    #[test]
    fn spread_filter_keeps_e2e_by_default() {
        let net = Bucket {
            bucket: 0,
            metric_id: METRIC_ID_SPREAD_NET,
            action_id: ACTION_ID_MARKET_DATA,
            n: 1,
            p50_us: 1,
            p90_us: 1,
            p95_us: 1,
            p99_us: 1,
        };
        let e2e = Bucket {
            bucket: 1,
            metric_id: METRIC_ID_SPREAD_E2E,
            action_id: ACTION_ID_MARKET_DATA,
            n: 1,
            p50_us: 1,
            p90_us: 1,
            p95_us: 1,
            p99_us: 1,
        };
        assert!(should_skip_bucket(
            "latency.public.spread_pbs.bybit-futures",
            &net,
            false
        ));
        assert!(!should_skip_bucket(
            "latency.public.spread_pbs.bybit-futures",
            &e2e,
            false
        ));
        assert!(!should_skip_bucket(
            "latency.public.spread_pbs.bybit-futures",
            &net,
            true
        ));
        assert!(!should_skip_bucket(
            "latency.private.te.bybit_intra_arb01",
            &net,
            false
        ));
    }

    #[test]
    fn config_builds_endpoint_from_ip_and_port() {
        let cfg: CaptureConfig = toml::from_str(
            r#"
instance = "sg"
ip = "47.131.162.78"
port = 6370
out_dir = "data/latency_csv/sg"
pm2_name = "latency_csv_capture-sg"
"#,
        )
        .unwrap();
        assert_eq!(cfg.endpoint(), "tcp://47.131.162.78:6370");
        assert_eq!(cfg.topic, "latency.");
        assert!(!cfg.include_spread_net);
    }
}
