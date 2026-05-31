use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, info, warn};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use crate::common::time_util::get_timestamp_us;

use super::order_update::CF_ORDER_UPDATE_UNMATCHED;
use super::storage::RocksDbStore;
use super::trade_update::CF_TRADE_UPDATE_UNMATCHED;
use super::uniform_order_persist::CF_UNIFORM_ORDER;

pub mod pb {
    tonic::include_proto!("persist_sync");
}

use pb::persist_sync_source_client::PersistSyncSourceClient;
use pb::persist_sync_source_server::{PersistSyncSource, PersistSyncSourceServer};
use pb::{
    AckRequest, AckResponse, BucketDigestBatch, BucketDigestRequest, BucketSummary,
    BucketSummaryRequest, BucketSummaryResponse, FetchRecordsRequest, RecordBatch, RecordDigest,
    SubscribeRequest, SyncRecord,
};

pub const CF_SYNC_OUTBOX: &str = "sync_outbox";
pub const CF_SYNC_META: &str = "sync_meta";
pub const CF_SYNC_APPLIED: &str = "sync_applied";

const OUTBOX_RECORD_VERSION: u8 = 1;
const DEFAULT_BATCH_RECORDS: usize = 1_000;
const DEFAULT_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_REPAIR_INTERVAL_SECS: u64 = 30 * 60;
const DEFAULT_REPAIR_LOOKBACK_HOURS: u64 = 24;
const DEFAULT_REPAIR_BUCKET_US: u64 = 60_000_000;
const STREAM_IDLE_SLEEP_MS: u64 = 50;
const STREAM_CHANNEL_CAPACITY: usize = 8;
const CENTER_KEY_SEPARATOR: u8 = b'|';
const META_NEXT_SEQ_KEY: &[u8] = b"next_seq";

#[derive(Debug, Clone)]
pub struct SyncOutboxRecord {
    pub seq: u64,
    pub cf_name: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub value_crc32: u32,
    pub write_ts_us: i64,
}

#[derive(Debug, Clone)]
pub struct PersistSyncConfig {
    pub source_id: String,
    pub bind_addr: Option<SocketAddr>,
}

impl PersistSyncConfig {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(source_id) = read_non_empty_env("PERSIST_SYNC_SOURCE_ID") else {
            return Ok(None);
        };
        let bind_addr = match read_non_empty_env("PERSIST_SYNC_BIND") {
            Some(raw) => Some(
                raw.parse()
                    .with_context(|| format!("invalid PERSIST_SYNC_BIND: {raw}"))?,
            ),
            None => None,
        };
        Ok(Some(Self {
            source_id,
            bind_addr,
        }))
    }

    pub fn enabled(&self) -> bool {
        !self.source_id.trim().is_empty()
    }
}

pub fn sync_column_families() -> &'static [&'static str] {
    &[CF_SYNC_OUTBOX, CF_SYNC_META, CF_SYNC_APPLIED]
}

pub fn order_export_sync_column_families() -> &'static [&'static str] {
    &[
        CF_ORDER_UPDATE_UNMATCHED,
        CF_TRADE_UPDATE_UNMATCHED,
        CF_UNIFORM_ORDER,
    ]
}

pub fn is_order_export_sync_cf(cf_name: &str) -> bool {
    order_export_sync_column_families()
        .iter()
        .any(|candidate| *candidate == cf_name)
}

pub fn required_center_column_families(base_cfs: &[&'static str]) -> Vec<&'static str> {
    let mut names = Vec::with_capacity(base_cfs.len() + 3);
    names.extend_from_slice(base_cfs);
    names.extend_from_slice(sync_column_families());
    names
}

fn required_multi_center_column_families(sources: &[MultiCollectorSource]) -> Vec<String> {
    let mut names = sync_column_families()
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    for source in sources {
        for base_cf in order_export_sync_column_families() {
            names.push(center_source_cf_name(&source.id, base_cf));
        }
    }
    names.sort();
    names.dedup();
    names
}

pub fn persist_with_outbox(
    store: &RocksDbStore,
    cf_name: &str,
    key: &[u8],
    value: &[u8],
    write_ts_us: i64,
    sync_enabled: bool,
) -> Result<()> {
    if !sync_enabled || !is_order_export_sync_cf(cf_name) {
        return store.put(cf_name, key, value);
    }

    let seq = next_outbox_seq(store)?;
    let outbox = SyncOutboxRecord {
        seq,
        cf_name: cf_name.to_string(),
        key: key.to_vec(),
        value: value.to_vec(),
        value_crc32: crc32(value),
        write_ts_us,
    };
    let outbox_key = seq_key(seq);
    let next_seq_value = (seq.saturating_add(1)).to_be_bytes().to_vec();
    store.put_many(&[
        (cf_name.to_string(), key.to_vec(), value.to_vec()),
        (
            CF_SYNC_OUTBOX.to_string(),
            outbox_key,
            encode_outbox_record(&outbox),
        ),
        (
            CF_SYNC_META.to_string(),
            META_NEXT_SEQ_KEY.to_vec(),
            next_seq_value,
        ),
    ])
}

pub async fn serve_sync_source(
    store: Arc<RocksDbStore>,
    addr: SocketAddr,
    source_id: String,
) -> Result<()> {
    let service = SyncSourceService { store, source_id };
    info!("persist sync source serving on {addr}");
    Server::builder()
        .add_service(PersistSyncSourceServer::new(service))
        .serve(addr)
        .await
        .context("persist sync source server failed")
}

pub async fn run_collector(args: CollectorArgs) -> Result<()> {
    let source_args = args.clone().into_source_args();
    let center_store = open_center_store_for_sources(
        &args.center_db_path,
        args.sync_writes,
        std::slice::from_ref(&source_args.source_id),
    )?;
    run_source_collector_once(center_store, source_args).await
}

pub async fn run_multi_collector(config: MultiCollectorConfig) -> Result<()> {
    let center_db = config
        .center_db
        .clone()
        .unwrap_or_else(|| "data/persist_sync_center".to_string());
    let sync_writes = config.sync_writes.unwrap_or(false);
    let batch_records = config.batch_records.unwrap_or(DEFAULT_BATCH_RECORDS);
    let batch_bytes = config.batch_bytes.unwrap_or(DEFAULT_BATCH_BYTES);
    let reconnect_delay = Duration::from_millis(config.reconnect_delay_ms.unwrap_or(1_000));
    let repair_config = RepairSchedulerConfig::from_config(&config);
    let sources = config.sources.clone();

    if sources.is_empty() {
        return Err(anyhow!(
            "persist sync collector config must contain at least one source"
        ));
    }

    let mut seen = std::collections::HashSet::new();
    for source in &sources {
        if !seen.insert(source.id.clone()) {
            return Err(anyhow!("duplicate source id in config: {}", source.id));
        }
    }

    let center_store = open_center_store_for_config(&center_db, sync_writes, &config)?;

    for source in sources.clone() {
        let args = SourceCollectorArgs {
            source_id: source.id,
            manager_endpoint: source.url,
            batch_records,
            batch_bytes,
        };
        let store = center_store.clone();
        tokio::task::spawn_local(async move {
            loop {
                match run_source_collector_once(store.clone(), args.clone()).await {
                    Ok(()) => warn!(
                        "persist sync source stream ended source_id={} endpoint={}",
                        args.source_id, args.manager_endpoint
                    ),
                    Err(err) => warn!(
                        "persist sync source failed source_id={} endpoint={}: {err:#}",
                        args.source_id, args.manager_endpoint
                    ),
                }
                tokio::time::sleep(reconnect_delay).await;
            }
        });
    }

    if repair_config.enabled {
        spawn_repair_scheduler(center_store.clone(), sources, repair_config);
    }

    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for ctrl_c")?;
    Ok(())
}

fn spawn_repair_scheduler(
    center_store: Arc<RocksDbStore>,
    sources: Vec<MultiCollectorSource>,
    config: RepairSchedulerConfig,
) {
    let jobs = build_repair_jobs(&sources);
    if jobs.is_empty() {
        return;
    }

    info!(
        "persist sync repair scheduler enabled jobs={} interval_secs={} lookback_hours={} bucket_us={}",
        jobs.len(),
        config.interval.as_secs(),
        config.lookback_hours,
        config.bucket_us
    );

    let total_jobs = jobs.len();
    for (index, (source, cf_name)) in jobs.into_iter().enumerate() {
        let initial_delay = stagger_delay(config.interval, index, total_jobs);
        let store = center_store.clone();
        let job_config = config.clone();
        info!(
            "persist sync repair job scheduled source_id={} cf={} initial_delay_secs={}",
            source.id,
            cf_name,
            initial_delay.as_secs()
        );
        tokio::task::spawn_local(async move {
            if initial_delay > Duration::ZERO {
                tokio::time::sleep(initial_delay).await;
            }
            loop {
                let started_us = get_timestamp_us();
                match run_scheduled_repair_once(store.as_ref(), &source, cf_name.as_str(), &job_config)
                    .await
                {
                    Ok(report) => info!(
                        "persist sync repair complete source_id={} cf={} matched_buckets={} mismatched_buckets={} missing_records={} mismatched_records={} repaired_records={} elapsed_ms={}",
                        source.id,
                        cf_name,
                        report.matched_buckets,
                        report.mismatched_buckets,
                        report.missing_records,
                        report.mismatched_records,
                        report.repaired_records,
                        elapsed_ms_since(started_us)
                    ),
                    Err(err) => warn!(
                        "persist sync repair failed source_id={} cf={}: {err:#}",
                        source.id, cf_name
                    ),
                }
                tokio::time::sleep(job_config.interval).await;
            }
        });
    }
}

fn build_repair_jobs(sources: &[MultiCollectorSource]) -> Vec<(MultiCollectorSource, String)> {
    let mut jobs = Vec::new();
    for source in sources {
        for cf_name in order_export_sync_column_families() {
            jobs.push((source.clone(), (*cf_name).to_string()));
        }
    }
    jobs.sort_by_key(|(source, cf_name)| repair_job_hash(source.id.as_str(), cf_name.as_str()));
    jobs
}

fn repair_job_hash(source_id: &str, cf_name: &str) -> u32 {
    let mut input = Vec::with_capacity(source_id.len() + cf_name.len() + 1);
    input.extend_from_slice(source_id.as_bytes());
    input.push(b'|');
    input.extend_from_slice(cf_name.as_bytes());
    crc32(&input)
}

fn stagger_delay(interval: Duration, index: usize, total_jobs: usize) -> Duration {
    if total_jobs <= 1 || index == 0 {
        return Duration::ZERO;
    }
    let delay_ms = interval.as_millis().saturating_mul(index as u128) / total_jobs as u128;
    Duration::from_millis(delay_ms.min(u64::MAX as u128) as u64)
}

async fn run_scheduled_repair_once(
    center_store: &RocksDbStore,
    source: &MultiCollectorSource,
    cf_name: &str,
    config: &RepairSchedulerConfig,
) -> Result<VerifyReport> {
    let end_us = get_timestamp_us() as u64;
    let lookback_us = config.lookback_hours.saturating_mul(3_600_000_000);
    let start_us = end_us.saturating_sub(lookback_us);
    verify_recent_with_store(
        center_store,
        VerifyArgs {
            source_id: source.id.clone(),
            manager_endpoint: source.url.clone(),
            center_db_path: String::new(),
            cf_names: vec![cf_name.to_string()],
            start_us,
            end_us,
            bucket_us: config.bucket_us,
            repair: true,
            sync_writes: false,
        },
    )
    .await
}

fn elapsed_ms_since(start_us: i64) -> i64 {
    get_timestamp_us().saturating_sub(start_us) / 1_000
}

async fn run_source_collector_once(
    center_store: Arc<RocksDbStore>,
    args: SourceCollectorArgs,
) -> Result<()> {
    let mut client = PersistSyncSourceClient::connect(args.manager_endpoint.clone())
        .await
        .with_context(|| {
            format!(
                "connect manager sync source failed: {}",
                args.manager_endpoint
            )
        })?;

    let after_seq = read_center_cursor(&center_store, &args.source_id)?;
    info!(
        "persist sync collector connected source_id={} endpoint={} after_seq={}",
        args.source_id, args.manager_endpoint, after_seq
    );

    let request = SubscribeRequest {
        source_id: args.source_id.clone(),
        after_seq,
        max_records: args.batch_records as u32,
        max_bytes: args.batch_bytes as u32,
    };
    let mut stream = client
        .subscribe_changes(request)
        .await
        .context("subscribe changes failed")?
        .into_inner();

    while let Some(batch) = stream
        .message()
        .await
        .context("receive sync batch failed")?
    {
        if batch.records.is_empty() {
            continue;
        }
        let last_seq = apply_record_batch(&center_store, &args.source_id, &batch)?;
        write_center_cursor(&center_store, &args.source_id, last_seq)?;
        let ack = AckRequest {
            source_id: args.source_id.clone(),
            ack_seq: last_seq,
        };
        if let Err(err) = client.ack_applied(ack).await {
            warn!(
                "ack manager failed source_id={} seq={}: {err:#}",
                args.source_id, last_seq
            );
        }
        debug!(
            "persist sync collector applied source_id={} first_seq={} last_seq={} records={}",
            args.source_id,
            batch.first_seq,
            last_seq,
            batch.records.len()
        );
    }

    Ok(())
}

fn open_center_store_for_config(
    center_db_path: &str,
    sync_writes: bool,
    config: &MultiCollectorConfig,
) -> Result<Arc<RocksDbStore>> {
    let cf_names = required_multi_center_column_families(&config.sources);
    let refs = cf_names.iter().map(String::as_str).collect::<Vec<_>>();
    let tuning = crate::persist_manager::default_tuning();
    Ok(Arc::new(RocksDbStore::open_with_tuning(
        center_db_path,
        refs.as_slice(),
        sync_writes,
        &tuning,
    )?))
}

fn open_center_store_for_sources(
    center_db_path: &str,
    sync_writes: bool,
    source_ids: &[String],
) -> Result<Arc<RocksDbStore>> {
    let sources = source_ids
        .iter()
        .map(|id| MultiCollectorSource {
            id: id.clone(),
            url: String::new(),
        })
        .collect::<Vec<_>>();
    let cf_names = required_multi_center_column_families(&sources);
    let refs = cf_names.iter().map(String::as_str).collect::<Vec<_>>();
    let tuning = crate::persist_manager::default_tuning();
    Ok(Arc::new(RocksDbStore::open_with_tuning(
        center_db_path,
        refs.as_slice(),
        sync_writes,
        &tuning,
    )?))
}

pub async fn run_verify_recent(args: VerifyArgs) -> Result<VerifyReport> {
    let center_store = open_center_store_for_sources(
        &args.center_db_path,
        args.sync_writes,
        std::slice::from_ref(&args.source_id),
    )?;
    verify_recent_with_store(center_store.as_ref(), args).await
}

async fn verify_recent_with_store(
    center_store: &RocksDbStore,
    args: VerifyArgs,
) -> Result<VerifyReport> {
    let mut client = PersistSyncSourceClient::connect(args.manager_endpoint.clone())
        .await
        .with_context(|| {
            format!(
                "connect manager sync source failed: {}",
                args.manager_endpoint
            )
        })?;

    let cf_names = if args.cf_names.is_empty() {
        order_export_sync_column_families()
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>()
    } else {
        args.cf_names.clone()
    };

    let request = BucketSummaryRequest {
        source_id: args.source_id.clone(),
        cf_names: cf_names.clone(),
        start_us: args.start_us,
        end_us: args.end_us,
        bucket_us: args.bucket_us,
    };
    let remote = client
        .get_bucket_summary(request)
        .await
        .context("get remote bucket summary failed")?
        .into_inner();
    let local = build_center_bucket_summaries(
        center_store,
        &args.source_id,
        &cf_names,
        args.start_us,
        args.end_us,
        args.bucket_us,
    )?;

    let mut report = VerifyReport::default();
    let local_map = local
        .into_iter()
        .map(|summary| bucket_key(&summary).map(|key| (key, summary)))
        .collect::<Result<HashMap<_, _>>>()?;

    for remote_summary in remote.summaries {
        let key = bucket_key(&remote_summary)?;
        match local_map.get(&key) {
            Some(local_summary) if same_summary(local_summary, &remote_summary) => {
                report.matched_buckets += 1;
            }
            _ => {
                report.mismatched_buckets += 1;
                let repaired = repair_bucket(
                    &mut client,
                    center_store,
                    &args.source_id,
                    remote_summary.cf_name.as_str(),
                    remote_summary.bucket_start_us,
                    remote_summary.bucket_end_us,
                    args.repair,
                )
                .await?;
                report.missing_records += repaired.missing_records;
                report.mismatched_records += repaired.mismatched_records;
                report.repaired_records += repaired.repaired_records;
            }
        }
    }

    Ok(report)
}

pub async fn run_verify_config(args: VerifyConfigArgs) -> Result<Vec<(String, VerifyReport)>> {
    let center_db = args
        .config
        .center_db
        .clone()
        .unwrap_or_else(|| "data/persist_sync_center".to_string());
    let sync_writes = args.config.sync_writes.unwrap_or(false);
    let mut reports = Vec::new();
    for source in args.config.sources {
        if let Some(filter_id) = args.only_id.as_ref() {
            if source.id != *filter_id {
                continue;
            }
        }
        let report = run_verify_recent(VerifyArgs {
            source_id: source.id.clone(),
            manager_endpoint: source.url,
            center_db_path: center_db.clone(),
            cf_names: Vec::new(),
            start_us: args.start_us,
            end_us: args.end_us,
            bucket_us: args.bucket_us,
            repair: args.repair,
            sync_writes,
        })
        .await?;
        reports.push((source.id, report));
    }
    if reports.is_empty() {
        if let Some(filter_id) = args.only_id {
            return Err(anyhow!("source id not found in config: {filter_id}"));
        }
    }
    Ok(reports)
}

#[derive(Debug, Clone)]
pub struct CollectorArgs {
    pub source_id: String,
    pub manager_endpoint: String,
    pub center_db_path: String,
    pub batch_records: usize,
    pub batch_bytes: usize,
    pub sync_writes: bool,
}

impl CollectorArgs {
    fn into_source_args(self) -> SourceCollectorArgs {
        SourceCollectorArgs {
            source_id: self.source_id,
            manager_endpoint: self.manager_endpoint,
            batch_records: self.batch_records,
            batch_bytes: self.batch_bytes,
        }
    }
}

#[derive(Debug, Clone)]
struct SourceCollectorArgs {
    source_id: String,
    manager_endpoint: String,
    batch_records: usize,
    batch_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MultiCollectorConfig {
    pub center_db: Option<String>,
    pub batch_records: Option<usize>,
    pub batch_bytes: Option<usize>,
    pub reconnect_delay_ms: Option<u64>,
    pub repair_enabled: Option<bool>,
    pub repair_interval_secs: Option<u64>,
    pub repair_lookback_hours: Option<u64>,
    pub repair_bucket_us: Option<u64>,
    pub sync_writes: Option<bool>,
    pub sources: Vec<MultiCollectorSource>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MultiCollectorSource {
    pub id: String,
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct VerifyConfigArgs {
    pub config: MultiCollectorConfig,
    pub only_id: Option<String>,
    pub start_us: u64,
    pub end_us: u64,
    pub bucket_us: u64,
    pub repair: bool,
}

#[derive(Debug, Clone)]
pub struct VerifyArgs {
    pub source_id: String,
    pub manager_endpoint: String,
    pub center_db_path: String,
    pub cf_names: Vec<String>,
    pub start_us: u64,
    pub end_us: u64,
    pub bucket_us: u64,
    pub repair: bool,
    pub sync_writes: bool,
}

#[derive(Debug, Default, Clone)]
pub struct VerifyReport {
    pub matched_buckets: u64,
    pub mismatched_buckets: u64,
    pub missing_records: u64,
    pub mismatched_records: u64,
    pub repaired_records: u64,
}

#[derive(Debug, Default)]
struct RepairStats {
    missing_records: u64,
    mismatched_records: u64,
    repaired_records: u64,
}

#[derive(Debug, Clone)]
struct RepairSchedulerConfig {
    enabled: bool,
    interval: Duration,
    lookback_hours: u64,
    bucket_us: u64,
}

impl RepairSchedulerConfig {
    fn from_config(config: &MultiCollectorConfig) -> Self {
        Self {
            enabled: config.repair_enabled.unwrap_or(true),
            interval: Duration::from_secs(
                config
                    .repair_interval_secs
                    .unwrap_or(DEFAULT_REPAIR_INTERVAL_SECS)
                    .max(1),
            ),
            lookback_hours: config
                .repair_lookback_hours
                .unwrap_or(DEFAULT_REPAIR_LOOKBACK_HOURS)
                .max(1),
            bucket_us: config
                .repair_bucket_us
                .unwrap_or(DEFAULT_REPAIR_BUCKET_US)
                .max(1),
        }
    }
}

struct SyncSourceService {
    store: Arc<RocksDbStore>,
    source_id: String,
}

#[tonic::async_trait]
impl PersistSyncSource for SyncSourceService {
    type SubscribeChangesStream = ReceiverStream<Result<RecordBatch, Status>>;
    type ListBucketDigestsStream = ReceiverStream<Result<BucketDigestBatch, Status>>;
    type FetchRecordsStream = ReceiverStream<Result<RecordBatch, Status>>;

    async fn subscribe_changes(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeChangesStream>, Status> {
        let req = request.into_inner();
        self.validate_source_id(req.source_id.as_str())?;
        let store = Arc::clone(&self.store);
        let source_id = self.source_id.clone();
        let max_records = clamp_batch_records(req.max_records);
        let max_bytes = clamp_batch_bytes(req.max_bytes);
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        tokio::task::spawn_local(async move {
            let mut after_seq = req.after_seq;
            loop {
                match load_outbox_batch(&store, after_seq, max_records, max_bytes) {
                    Ok(records) if records.is_empty() => {
                        tokio::time::sleep(Duration::from_millis(STREAM_IDLE_SLEEP_MS)).await;
                    }
                    Ok(records) => {
                        let first_seq = records.first().map(|record| record.seq).unwrap_or(0);
                        let last_seq = records.last().map(|record| record.seq).unwrap_or(after_seq);
                        after_seq = last_seq;
                        let batch = RecordBatch {
                            source_id: source_id.clone(),
                            first_seq,
                            last_seq,
                            records: records.into_iter().map(outbox_to_pb).collect(),
                        };
                        if tx.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "load outbox batch failed: {err:#}"
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_applied(
        &self,
        request: Request<AckRequest>,
    ) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();
        self.validate_source_id(req.source_id.as_str())?;
        let deleted = delete_outbox_through(&self.store, req.ack_seq)
            .map_err(|err| Status::internal(format!("delete outbox failed: {err:#}")))?;
        Ok(Response::new(AckResponse {
            deleted_records: deleted as u64,
        }))
    }

    async fn get_bucket_summary(
        &self,
        request: Request<BucketSummaryRequest>,
    ) -> Result<Response<BucketSummaryResponse>, Status> {
        let req = request.into_inner();
        self.validate_source_id(req.source_id.as_str())?;
        let cf_names = if req.cf_names.is_empty() {
            order_export_sync_column_families()
                .iter()
                .map(|name| name.to_string())
                .collect()
        } else {
            req.cf_names
        };
        let summaries = build_source_bucket_summaries(
            &self.store,
            &cf_names,
            req.start_us,
            req.end_us,
            req.bucket_us,
        )
        .map_err(|err| Status::internal(format!("build bucket summary failed: {err:#}")))?;
        Ok(Response::new(BucketSummaryResponse { summaries }))
    }

    async fn list_bucket_digests(
        &self,
        request: Request<BucketDigestRequest>,
    ) -> Result<Response<Self::ListBucketDigestsStream>, Status> {
        let req = request.into_inner();
        self.validate_source_id(req.source_id.as_str())?;
        let store = Arc::clone(&self.store);
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        tokio::task::spawn_local(async move {
            let start_key = format_time_key(req.start_us);
            let end_key = end_time_key(req.end_us);
            let limit = max_limit(req.max_records);
            let entries = match store.scan_range(
                req.cf_name.as_str(),
                start_key.as_bytes(),
                end_key.as_bytes(),
                None,
            ) {
                Ok(entries) => entries,
                Err(err) => {
                    let _ = tx
                        .send(Err(Status::internal(format!(
                            "scan bucket digests failed: {err:#}"
                        ))))
                        .await;
                    return;
                }
            };
            let mut batch = BucketDigestBatch {
                cf_name: req.cf_name.clone(),
                digests: Vec::with_capacity(limit.min(entries.len())),
            };
            for (key, value) in entries {
                batch.digests.push(RecordDigest {
                    key,
                    value_crc32: crc32(&value),
                    value_len: value.len() as u32,
                });
                if batch.digests.len() >= limit {
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                    batch = BucketDigestBatch {
                        cf_name: req.cf_name.clone(),
                        digests: Vec::with_capacity(limit),
                    };
                }
            }
            if !batch.digests.is_empty() {
                let _ = tx.send(Ok(batch)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn fetch_records(
        &self,
        request: Request<FetchRecordsRequest>,
    ) -> Result<Response<Self::FetchRecordsStream>, Status> {
        let req = request.into_inner();
        self.validate_source_id(req.source_id.as_str())?;
        let store = Arc::clone(&self.store);
        let source_id = self.source_id.clone();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        tokio::task::spawn_local(async move {
            let limit = max_limit(req.max_records);
            let mut records = Vec::with_capacity(limit.min(req.keys.len()));
            for key in req.keys {
                match store.get(req.cf_name.as_str(), &key) {
                    Ok(Some(value)) => records.push(SyncRecord {
                        seq: 0,
                        cf_name: req.cf_name.clone(),
                        key,
                        value_crc32: crc32(&value),
                        value,
                        write_ts_us: 0,
                    }),
                    Ok(None) => {}
                    Err(err) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "fetch record failed: {err:#}"
                            ))))
                            .await;
                        return;
                    }
                }
                if records.len() >= limit {
                    let batch = RecordBatch {
                        source_id: source_id.clone(),
                        first_seq: 0,
                        last_seq: 0,
                        records: std::mem::take(&mut records),
                    };
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                }
            }
            if !records.is_empty() {
                let batch = RecordBatch {
                    source_id,
                    first_seq: 0,
                    last_seq: 0,
                    records,
                };
                let _ = tx.send(Ok(batch)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl SyncSourceService {
    fn validate_source_id(&self, requested: &str) -> Result<(), Status> {
        if requested.is_empty() || requested == self.source_id {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "source_id mismatch: requested={} served={}",
                requested, self.source_id
            )))
        }
    }
}

fn read_non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn next_outbox_seq(store: &RocksDbStore) -> Result<u64> {
    match store.get(CF_SYNC_META, META_NEXT_SEQ_KEY)? {
        Some(raw) if raw.len() == 8 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&raw);
            Ok(u64::from_be_bytes(buf))
        }
        Some(raw) => Err(anyhow!(
            "invalid sync next_seq meta length: {} bytes",
            raw.len()
        )),
        None => Ok(1),
    }
}

fn encode_outbox_record(record: &SyncOutboxRecord) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(
        1 + 8 + 2 + record.cf_name.len() + 4 + record.key.len() + 4 + record.value.len() + 4 + 8,
    );
    buf.put_u8(OUTBOX_RECORD_VERSION);
    buf.put_u64(record.seq);
    put_string_u16(&mut buf, record.cf_name.as_str());
    put_bytes_u32(&mut buf, &record.key);
    put_bytes_u32(&mut buf, &record.value);
    buf.put_u32(record.value_crc32);
    buf.put_i64(record.write_ts_us);
    buf.to_vec()
}

fn decode_outbox_record(raw: &[u8]) -> Result<SyncOutboxRecord> {
    let mut cursor = Bytes::copy_from_slice(raw);
    if cursor.remaining() < 1 + 8 {
        return Err(anyhow!("outbox record too short"));
    }
    let version = cursor.get_u8();
    if version != OUTBOX_RECORD_VERSION {
        return Err(anyhow!("unsupported outbox record version: {version}"));
    }
    let seq = cursor.get_u64();
    let cf_name = read_string_u16(&mut cursor, "cf_name")?;
    let key = read_bytes_u32(&mut cursor, "key")?;
    let value = read_bytes_u32(&mut cursor, "value")?;
    if cursor.remaining() < 4 + 8 {
        return Err(anyhow!("outbox record missing crc/write_ts"));
    }
    let value_crc32 = cursor.get_u32();
    let write_ts_us = cursor.get_i64();
    if cursor.remaining() != 0 {
        return Err(anyhow!("outbox record has trailing bytes"));
    }
    Ok(SyncOutboxRecord {
        seq,
        cf_name,
        key,
        value,
        value_crc32,
        write_ts_us,
    })
}

fn put_string_u16(buf: &mut BytesMut, value: &str) {
    let bytes = value.as_bytes();
    let len = bytes.len().min(u16::MAX as usize);
    buf.put_u16(len as u16);
    buf.put_slice(&bytes[..len]);
}

fn put_bytes_u32(buf: &mut BytesMut, value: &[u8]) {
    buf.put_u32(value.len() as u32);
    buf.put_slice(value);
}

fn read_string_u16(cursor: &mut Bytes, field: &str) -> Result<String> {
    if cursor.remaining() < 2 {
        return Err(anyhow!("not enough bytes for {field} length"));
    }
    let len = cursor.get_u16() as usize;
    if cursor.remaining() < len {
        return Err(anyhow!("not enough bytes for {field}"));
    }
    Ok(String::from_utf8_lossy(cursor.copy_to_bytes(len).as_ref()).into_owned())
}

fn read_bytes_u32(cursor: &mut Bytes, field: &str) -> Result<Vec<u8>> {
    if cursor.remaining() < 4 {
        return Err(anyhow!("not enough bytes for {field} length"));
    }
    let len = cursor.get_u32() as usize;
    if cursor.remaining() < len {
        return Err(anyhow!("not enough bytes for {field}"));
    }
    Ok(cursor.copy_to_bytes(len).to_vec())
}

fn seq_key(seq: u64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

fn seq_from_key(key: &[u8]) -> Result<u64> {
    if key.len() != 8 {
        return Err(anyhow!("invalid seq key length: {}", key.len()));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(key);
    Ok(u64::from_be_bytes(buf))
}

fn load_outbox_batch(
    store: &RocksDbStore,
    after_seq: u64,
    max_records: usize,
    max_bytes: usize,
) -> Result<Vec<SyncOutboxRecord>> {
    let start_seq = after_seq.saturating_add(1);
    let entries = store.scan(
        CF_SYNC_OUTBOX,
        Some(&seq_key(start_seq)),
        false,
        Some(max_records),
    )?;
    let mut records = Vec::with_capacity(entries.len());
    let mut total_bytes = 0usize;
    for (key, value) in entries {
        let key_seq = seq_from_key(&key)?;
        let record = decode_outbox_record(&value)?;
        if record.seq != key_seq {
            return Err(anyhow!(
                "outbox seq mismatch key_seq={} record_seq={}",
                key_seq,
                record.seq
            ));
        }
        total_bytes += record.key.len() + record.value.len() + record.cf_name.len() + 32;
        if !records.is_empty() && total_bytes > max_bytes {
            break;
        }
        records.push(record);
    }
    Ok(records)
}

fn delete_outbox_through(store: &RocksDbStore, ack_seq: u64) -> Result<usize> {
    if ack_seq == 0 {
        return Ok(0);
    }
    let end = ack_seq.saturating_add(1);
    let entries = store.scan_range(CF_SYNC_OUTBOX, &seq_key(1), &seq_key(end), None)?;
    let keys = entries.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
    store.delete_many(CF_SYNC_OUTBOX, &keys)
}

fn outbox_to_pb(record: SyncOutboxRecord) -> SyncRecord {
    SyncRecord {
        seq: record.seq,
        cf_name: record.cf_name,
        key: record.key,
        value: record.value,
        value_crc32: record.value_crc32,
        write_ts_us: record.write_ts_us,
    }
}

fn clamp_batch_records(raw: u32) -> usize {
    let value = if raw == 0 {
        DEFAULT_BATCH_RECORDS
    } else {
        raw as usize
    };
    value.clamp(1, 50_000)
}

fn clamp_batch_bytes(raw: u32) -> usize {
    let value = if raw == 0 {
        DEFAULT_BATCH_BYTES
    } else {
        raw as usize
    };
    value.clamp(64 * 1024, 64 * 1024 * 1024)
}

fn max_limit(raw: u32) -> usize {
    if raw == 0 {
        DEFAULT_BATCH_RECORDS
    } else {
        (raw as usize).clamp(1, 50_000)
    }
}

fn crc32(value: &[u8]) -> u32 {
    crc32fast::hash(value)
}

fn apply_record_batch(store: &RocksDbStore, source_id: &str, batch: &RecordBatch) -> Result<u64> {
    if batch.source_id != source_id {
        return Err(anyhow!(
            "batch source_id mismatch expected={} got={}",
            source_id,
            batch.source_id
        ));
    }
    let mut writes = Vec::with_capacity(batch.records.len() * 2);
    let mut last_seq = 0u64;
    for record in &batch.records {
        validate_record(record)?;
        let center_cf = center_source_cf_name(source_id, record.cf_name.as_str());
        writes.push((center_cf, record.key.clone(), record.value.clone()));
        writes.push((
            CF_SYNC_APPLIED.to_string(),
            applied_key(source_id, record.seq),
            encode_applied_meta(record),
        ));
        last_seq = last_seq.max(record.seq);
    }
    if !writes.is_empty() {
        store.put_many(&writes)?;
    }
    Ok(last_seq)
}

fn validate_record(record: &SyncRecord) -> Result<()> {
    if !is_order_export_sync_cf(record.cf_name.as_str()) {
        return Err(anyhow!("unexpected sync cf: {}", record.cf_name));
    }
    let actual = crc32(&record.value);
    if actual != record.value_crc32 {
        return Err(anyhow!(
            "record crc mismatch seq={} cf={} key={} expected={} actual={}",
            record.seq,
            record.cf_name,
            hex_key(&record.key),
            record.value_crc32,
            actual
        ));
    }
    Ok(())
}

fn encode_applied_meta(record: &SyncRecord) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(4 + record.cf_name.len() + 4 + record.key.len() + 4 + 8);
    put_string_u16(&mut buf, record.cf_name.as_str());
    put_bytes_u32(&mut buf, &record.key);
    buf.put_u32(record.value_crc32);
    buf.put_i64(record.write_ts_us);
    buf.to_vec()
}

fn read_center_cursor(store: &RocksDbStore, source_id: &str) -> Result<u64> {
    let key = center_cursor_key(source_id);
    match store.get(CF_SYNC_META, &key)? {
        Some(raw) if raw.len() == 8 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&raw);
            Ok(u64::from_be_bytes(buf))
        }
        Some(raw) => Err(anyhow!(
            "invalid center cursor length source_id={} len={}",
            source_id,
            raw.len()
        )),
        None => Ok(0),
    }
}

fn write_center_cursor(store: &RocksDbStore, source_id: &str, seq: u64) -> Result<()> {
    store.put(
        CF_SYNC_META,
        &center_cursor_key(source_id),
        &seq.to_be_bytes(),
    )
}

fn center_cursor_key(source_id: &str) -> Vec<u8> {
    format!("center_cursor:{source_id}").into_bytes()
}

fn applied_key(source_id: &str, seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(source_id.len() + 1 + 8);
    key.extend_from_slice(source_id.as_bytes());
    key.push(CENTER_KEY_SEPARATOR);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

pub fn center_source_cf_name(source_id: &str, cf_name: &str) -> String {
    let sanitized = source_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{sanitized}__{cf_name}")
}

fn format_time_key(ts_us: u64) -> String {
    format!("{:020}", ts_us)
}

fn end_time_key(end_us: u64) -> String {
    format_time_key(end_us.saturating_add(1))
}

fn build_source_bucket_summaries(
    store: &RocksDbStore,
    cf_names: &[String],
    start_us: u64,
    end_us: u64,
    bucket_us: u64,
) -> Result<Vec<BucketSummary>> {
    let bucket_us = bucket_us.max(1);
    let mut summaries = Vec::new();
    for cf_name in cf_names {
        let start_key = format_time_key(start_us);
        let end_key = end_time_key(end_us);
        let entries = store.scan_range(
            cf_name.as_str(),
            start_key.as_bytes(),
            end_key.as_bytes(),
            None,
        )?;
        let mut buckets: BTreeMap<u64, BucketSummary> = BTreeMap::new();
        for (key, value) in entries {
            let Some(ts) = parse_time_key(&key) else {
                continue;
            };
            let bucket_start = bucket_start(ts, start_us, bucket_us);
            let bucket_end = bucket_start
                .saturating_add(bucket_us)
                .saturating_sub(1)
                .min(end_us);
            let summary = buckets
                .entry(bucket_start)
                .or_insert_with(|| BucketSummary {
                    cf_name: cf_name.clone(),
                    bucket_start_us: bucket_start,
                    bucket_end_us: bucket_end,
                    count: 0,
                    key_hash: 0,
                    value_hash: 0,
                });
            fold_summary(summary, &key, &value);
        }
        summaries.extend(buckets.into_values());
    }
    Ok(summaries)
}

fn build_center_bucket_summaries(
    store: &RocksDbStore,
    source_id: &str,
    cf_names: &[String],
    start_us: u64,
    end_us: u64,
    bucket_us: u64,
) -> Result<Vec<BucketSummary>> {
    let bucket_us = bucket_us.max(1);
    let mut summaries = Vec::new();
    for cf_name in cf_names {
        let start_key = format_time_key(start_us);
        let end_key = end_time_key(end_us);
        let center_cf = center_source_cf_name(source_id, cf_name);
        let entries = store.scan_range(
            center_cf.as_str(),
            start_key.as_bytes(),
            end_key.as_bytes(),
            None,
        )?;
        let mut buckets: BTreeMap<u64, BucketSummary> = BTreeMap::new();
        for (key, value) in entries {
            let Some(ts) = parse_time_key(&key) else {
                continue;
            };
            let bucket_start = bucket_start(ts, start_us, bucket_us);
            let bucket_end = bucket_start
                .saturating_add(bucket_us)
                .saturating_sub(1)
                .min(end_us);
            let summary = buckets
                .entry(bucket_start)
                .or_insert_with(|| BucketSummary {
                    cf_name: cf_name.clone(),
                    bucket_start_us: bucket_start,
                    bucket_end_us: bucket_end,
                    count: 0,
                    key_hash: 0,
                    value_hash: 0,
                });
            fold_summary(summary, &key, &value);
        }
        summaries.extend(buckets.into_values());
    }
    Ok(summaries)
}

fn fold_summary(summary: &mut BucketSummary, key: &[u8], value: &[u8]) {
    summary.count = summary.count.saturating_add(1);
    summary.key_hash ^= hash64(key);
    let mut value_hash_input = Vec::with_capacity(key.len() + 4 + 8);
    value_hash_input.extend_from_slice(key);
    value_hash_input.extend_from_slice(&crc32(value).to_be_bytes());
    value_hash_input.extend_from_slice(&(value.len() as u64).to_be_bytes());
    summary.value_hash ^= hash64(&value_hash_input);
}

fn hash64(value: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(value);
    hasher.finish()
}

fn parse_time_key(key: &[u8]) -> Option<u64> {
    if key.len() < 20 {
        return None;
    }
    std::str::from_utf8(&key[..20]).ok()?.parse::<u64>().ok()
}

fn bucket_start(ts: u64, start_us: u64, bucket_us: u64) -> u64 {
    if ts <= start_us {
        return start_us;
    }
    start_us + ((ts - start_us) / bucket_us) * bucket_us
}

fn bucket_key(summary: &BucketSummary) -> Result<(String, u64, u64)> {
    Ok((
        summary.cf_name.clone(),
        summary.bucket_start_us,
        summary.bucket_end_us,
    ))
}

fn same_summary(left: &BucketSummary, right: &BucketSummary) -> bool {
    left.count == right.count
        && left.key_hash == right.key_hash
        && left.value_hash == right.value_hash
}

async fn repair_bucket(
    client: &mut PersistSyncSourceClient<Channel>,
    center_store: &RocksDbStore,
    source_id: &str,
    cf_name: &str,
    start_us: u64,
    end_us: u64,
    repair: bool,
) -> Result<RepairStats> {
    let mut stream = client
        .list_bucket_digests(BucketDigestRequest {
            source_id: source_id.to_string(),
            cf_name: cf_name.to_string(),
            start_us,
            end_us,
            max_records: DEFAULT_BATCH_RECORDS as u32,
        })
        .await
        .context("list bucket digests failed")?
        .into_inner();

    let mut stats = RepairStats::default();
    let mut missing_keys = Vec::new();
    while let Some(batch) = stream
        .message()
        .await
        .context("receive digest batch failed")?
    {
        for digest in batch.digests {
            let center_cf = center_source_cf_name(source_id, cf_name);
            match center_store.get(center_cf.as_str(), &digest.key)? {
                Some(value) => {
                    if crc32(&value) != digest.value_crc32
                        || value.len() != digest.value_len as usize
                    {
                        stats.mismatched_records += 1;
                        missing_keys.push(digest.key);
                    }
                }
                None => {
                    stats.missing_records += 1;
                    missing_keys.push(digest.key);
                }
            }
        }
    }

    if repair && !missing_keys.is_empty() {
        let repaired =
            fetch_and_apply_records(client, center_store, source_id, cf_name, missing_keys).await?;
        stats.repaired_records += repaired as u64;
    }
    Ok(stats)
}

async fn fetch_and_apply_records(
    client: &mut PersistSyncSourceClient<Channel>,
    center_store: &RocksDbStore,
    source_id: &str,
    cf_name: &str,
    keys: Vec<Vec<u8>>,
) -> Result<usize> {
    let mut repaired = 0usize;
    for chunk in keys.chunks(DEFAULT_BATCH_RECORDS) {
        let mut stream = client
            .fetch_records(FetchRecordsRequest {
                source_id: source_id.to_string(),
                cf_name: cf_name.to_string(),
                keys: chunk.to_vec(),
                max_records: DEFAULT_BATCH_RECORDS as u32,
            })
            .await
            .context("fetch records failed")?
            .into_inner();
        while let Some(batch) = stream
            .message()
            .await
            .context("receive fetch batch failed")?
        {
            let mut writes = Vec::with_capacity(batch.records.len());
            for record in batch.records {
                validate_record(&record)?;
                let center_cf = center_source_cf_name(source_id, cf_name);
                writes.push((center_cf, record.key, record.value));
            }
            repaired += writes.len();
            center_store.put_many(&writes)?;
        }
    }
    Ok(repaired)
}

fn hex_key(key: &[u8]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::{decode_outbox_record, encode_outbox_record, SyncOutboxRecord};

    #[test]
    fn outbox_record_roundtrip() {
        let record = SyncOutboxRecord {
            seq: 42,
            cf_name: "uniform_orders".to_string(),
            key: b"00000000000000000123".to_vec(),
            value: b"payload".to_vec(),
            value_crc32: crc32fast::hash(b"payload"),
            write_ts_us: 123,
        };
        let encoded = encode_outbox_record(&record);
        let decoded = decode_outbox_record(&encoded).unwrap();
        assert_eq!(decoded.seq, record.seq);
        assert_eq!(decoded.cf_name, record.cf_name);
        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value, record.value);
        assert_eq!(decoded.value_crc32, record.value_crc32);
        assert_eq!(decoded.write_ts_us, record.write_ts_us);
    }
}
