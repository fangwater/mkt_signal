use anyhow::{anyhow, Context, Result};
use log::{debug, info};
use std::io::ErrorKind;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::query_msg::{
    build_depth_query_socket_path, read_depth_query_frame, resp_status_name,
    write_depth_query_frame, DepthQueryHeader, DepthQueryLoadTlenBatchReq,
    DepthQueryLoadTlenBatchResp, DepthQueryLoadTlenSingleReq, DepthQueryLoadTlenSingleResp,
    DepthQueryTop5PriceTlenReq, DepthQueryTop5PriceTlenResp, DepthQueryType, DEPTH_QUERY_PAYLOAD,
    RESP_STATUS_OK,
};
use crate::signal::common::TradingVenue;

const DEPTH_QUERY_SOCKET_TIMEOUT_MS: u64 = 200;
const DEPTH_QUERY_BATCH_FAST_TIMEOUT_US: u64 = 100_000;
const DEPTH_QUERY_BATCH_FAST_RETRY_ATTEMPTS: usize = 3;
const DEPTH_QUERY_BATCH_FAST_RETRY_SLEEP_US: u64 = 200;

#[derive(Debug, Clone)]
pub struct DepthQueryClient {
    venue_slug: String,
    socket_path: PathBuf,
    connection: Arc<Mutex<DepthQueryConnection>>,
    persistent_connection: bool,
}

#[derive(Debug)]
struct DepthQueryConnection {
    stream: Option<UnixStream>,
}

impl DepthQueryClient {
    pub fn new(venue: TradingVenue) -> Result<Self> {
        Self::from_venue_slug(venue.data_pub_slug())
    }

    pub fn new_persistent(venue: TradingVenue) -> Result<Self> {
        Self::from_venue_slug_with_mode(venue.data_pub_slug(), true)
    }

    pub fn new_short_lived(venue: TradingVenue) -> Result<Self> {
        Self::from_venue_slug_with_mode(venue.data_pub_slug(), false)
    }

    pub fn from_venue_slug(venue_slug: impl Into<String>) -> Result<Self> {
        Self::from_venue_slug_with_mode(venue_slug, true)
    }

    pub fn from_venue_slug_persistent(venue_slug: impl Into<String>) -> Result<Self> {
        Self::from_venue_slug_with_mode(venue_slug, true)
    }

    pub fn from_venue_slug_short_lived(venue_slug: impl Into<String>) -> Result<Self> {
        Self::from_venue_slug_with_mode(venue_slug, false)
    }

    fn from_venue_slug_with_mode(
        venue_slug: impl Into<String>,
        persistent_connection: bool,
    ) -> Result<Self> {
        let venue_slug = venue_slug.into();
        let socket_path = build_depth_query_socket_path(&venue_slug);
        info!("depth query client ready: {}", socket_path.display());
        Ok(Self {
            venue_slug,
            socket_path,
            connection: Arc::new(Mutex::new(DepthQueryConnection { stream: None })),
            persistent_connection,
        })
    }

    pub fn venue_slug(&self) -> &str {
        &self.venue_slug
    }

    pub fn query_single_tick_index(&self, symbol: &str, tick_index: i64) -> Result<f64> {
        let mut req_buf = [0u8; DEPTH_QUERY_PAYLOAD];
        let header_len =
            DepthQueryHeader::write(&mut req_buf, DepthQueryType::LoadTlenSingle as u8, symbol)
                .map_err(|err| anyhow!(err.to_string()))?;
        let req = DepthQueryLoadTlenSingleReq {
            timestamp_us: crate::common::time_util::get_timestamp_us(),
            tick_index,
        };
        let payload_len = req
            .write_to(&mut req_buf[header_len..])
            .map_err(|err| anyhow!(err.to_string()))?;
        let body = self.send_query(
            &req_buf[..header_len + payload_len],
            DepthQueryType::LoadTlenSingle,
        )?;
        let resp = DepthQueryLoadTlenSingleResp::from_payload(&body)
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(resp.amount)
    }

    pub fn query_batch_tick_indices(&self, symbol: &str, tick_indices: &[i64]) -> Result<Vec<f64>> {
        if tick_indices.is_empty() {
            return Ok(Vec::new());
        }

        let mut req_buf = [0u8; DEPTH_QUERY_PAYLOAD];
        let header_len =
            DepthQueryHeader::write(&mut req_buf, DepthQueryType::LoadTlenBatch as u8, symbol)
                .map_err(|err| anyhow!(err.to_string()))?;
        let payload_len = DepthQueryLoadTlenBatchReq::write_to(
            &mut req_buf[header_len..],
            crate::common::time_util::get_timestamp_us(),
            tick_indices,
        )
        .map_err(|err| anyhow!(err.to_string()))?;

        let req = &req_buf[..header_len + payload_len];
        let mut last_err = None;
        let mut body = None;
        for attempt in 0..DEPTH_QUERY_BATCH_FAST_RETRY_ATTEMPTS {
            match self.send_query_with_timeout(
                req,
                DepthQueryType::LoadTlenBatch,
                Duration::from_micros(DEPTH_QUERY_BATCH_FAST_TIMEOUT_US),
            ) {
                Ok(resp) => {
                    body = Some(resp);
                    break;
                }
                Err(err) => {
                    let retryable = Self::is_fast_retryable_query_error(&err);
                    if !retryable || attempt + 1 >= DEPTH_QUERY_BATCH_FAST_RETRY_ATTEMPTS {
                        return Err(err).with_context(|| {
                            format!(
                                "depth query batch failed venue={} symbol={} levels={} attempts={} timeout_us={}",
                                self.venue_slug,
                                symbol,
                                tick_indices.len(),
                                attempt + 1,
                                DEPTH_QUERY_BATCH_FAST_TIMEOUT_US
                            )
                        });
                    }
                    debug!(
                        "depth query batch fast-retry venue={} symbol={} levels={} attempt={}/{} err={:#}",
                        self.venue_slug,
                        symbol,
                        tick_indices.len(),
                        attempt + 1,
                        DEPTH_QUERY_BATCH_FAST_RETRY_ATTEMPTS,
                        err
                    );
                    last_err = Some(err);
                    std::thread::sleep(Duration::from_micros(
                        DEPTH_QUERY_BATCH_FAST_RETRY_SLEEP_US,
                    ));
                }
            }
        }
        let body = body.ok_or_else(|| {
            last_err.unwrap_or_else(|| anyhow!("depth query batch failed without response"))
        })?;
        let resp = DepthQueryLoadTlenBatchResp::from_payload(&body)
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(resp.amounts)
    }

    pub fn query_top5(&self, symbol: &str) -> Result<DepthQueryTop5PriceTlenResp> {
        let mut req_buf = [0u8; DEPTH_QUERY_PAYLOAD];
        let header_len =
            DepthQueryHeader::write(&mut req_buf, DepthQueryType::Top5PriceTlen as u8, symbol)
                .map_err(|err| anyhow!(err.to_string()))?;
        let req = DepthQueryTop5PriceTlenReq {
            timestamp_us: crate::common::time_util::get_timestamp_us(),
        };
        let payload_len = req
            .write_to(&mut req_buf[header_len..])
            .map_err(|err| anyhow!(err.to_string()))?;
        let body = self.send_query(
            &req_buf[..header_len + payload_len],
            DepthQueryType::Top5PriceTlen,
        )?;
        DepthQueryTop5PriceTlenResp::from_payload(&body).map_err(|err| anyhow!(err.to_string()))
    }

    fn send_query(&self, req: &[u8], expected_type: DepthQueryType) -> Result<Vec<u8>> {
        self.send_query_with_timeout(
            req,
            expected_type,
            Duration::from_millis(DEPTH_QUERY_SOCKET_TIMEOUT_MS),
        )
    }

    fn send_query_with_timeout(
        &self,
        req: &[u8],
        expected_type: DepthQueryType,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        if self.persistent_connection {
            return self.send_query_with_persistent_connection(req, expected_type, timeout);
        }
        self.send_query_short_lived(req, expected_type, timeout)
    }

    fn send_query_short_lived(
        &self,
        req: &[u8],
        expected_type: DepthQueryType,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let mut stream = self.connect_stream()?;
        Self::configure_stream_timeout(&mut stream, timeout)?;
        Self::exchange_query(&mut stream, req, expected_type)
    }

    fn send_query_with_persistent_connection(
        &self,
        req: &[u8],
        expected_type: DepthQueryType,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let mut last_err = None;
        for attempt in 0..2 {
            let mut conn = self
                .connection
                .lock()
                .map_err(|_| anyhow!("depth query client connection mutex poisoned"))?;
            if conn.stream.is_none() {
                conn.stream = Some(self.connect_stream()?);
            }

            let result = {
                let stream = conn.stream.as_mut().expect("stream just initialized");
                Self::configure_stream_timeout(stream, timeout)?;
                Self::exchange_query(stream, req, expected_type)
            };

            match result {
                Ok(payload) => return Ok(payload),
                Err(err) => {
                    conn.stream = None;
                    let retryable = attempt == 0 && Self::is_reconnectable_query_error(&err);
                    drop(conn);
                    if retryable {
                        debug!(
                            "depth query reconnecting venue={} type={:?} err={:#}",
                            self.venue_slug, expected_type, err
                        );
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("depth query persistent request failed")))
    }

    fn connect_stream(&self) -> Result<UnixStream> {
        UnixStream::connect(&self.socket_path).with_context(|| {
            format!(
                "connect depth query socket failed: {}",
                self.socket_path.display()
            )
        })
    }

    fn configure_stream_timeout(stream: &mut UnixStream, timeout: Duration) -> Result<()> {
        let timeout = Some(timeout);
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(timeout)?;
        Ok(())
    }

    fn exchange_query(
        stream: &mut UnixStream,
        req: &[u8],
        expected_type: DepthQueryType,
    ) -> Result<Vec<u8>> {
        write_depth_query_frame(stream, req)?;
        let payload = read_depth_query_frame(stream, DEPTH_QUERY_PAYLOAD)?;
        let header = DepthQueryHeader::parse(&payload).map_err(|err| anyhow!(err.to_string()))?;
        if header.query_type != expected_type as u8 {
            return Err(anyhow!(
                "unexpected response query_type={}, expected={}",
                header.query_type,
                expected_type as u8
            ));
        }

        let resp_payload = &payload[header.payload_offset..];
        if resp_payload.is_empty() {
            return Err(anyhow!("response payload is empty"));
        }
        let status = resp_payload[0];
        if status != RESP_STATUS_OK {
            return Err(anyhow!(
                "depth query failed: status={}({})",
                status,
                resp_status_name(status)
            ));
        }

        Ok(resp_payload[1..].to_vec())
    }

    fn is_fast_retryable_query_error(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io_err| {
                    matches!(
                        io_err.kind(),
                        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                    )
                })
        })
    }

    fn is_reconnectable_query_error(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io_err| {
                    matches!(
                        io_err.kind(),
                        ErrorKind::WouldBlock
                            | ErrorKind::TimedOut
                            | ErrorKind::Interrupted
                            | ErrorKind::UnexpectedEof
                            | ErrorKind::BrokenPipe
                            | ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                            | ErrorKind::NotConnected
                    )
                })
        })
    }
}
