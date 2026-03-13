use anyhow::{anyhow, Context, Result};
use log::info;
use std::os::unix::net::UnixStream;
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

#[derive(Debug, Clone)]
pub struct DepthQueryClient {
    venue_slug: String,
}

impl DepthQueryClient {
    pub fn new(venue: TradingVenue) -> Result<Self> {
        Self::from_venue_slug(venue.data_pub_slug())
    }

    pub fn from_venue_slug(venue_slug: impl Into<String>) -> Result<Self> {
        let venue_slug = venue_slug.into();
        let socket_path = build_depth_query_socket_path(&venue_slug);
        info!("depth query client ready: {}", socket_path.display());
        Ok(Self { venue_slug })
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

        let body = self.send_query(
            &req_buf[..header_len + payload_len],
            DepthQueryType::LoadTlenBatch,
        )?;
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
        let socket_path = build_depth_query_socket_path(&self.venue_slug);
        let mut stream = UnixStream::connect(&socket_path).with_context(|| {
            format!(
                "connect depth query socket failed: {}",
                socket_path.display()
            )
        })?;
        let timeout = Some(Duration::from_millis(DEPTH_QUERY_SOCKET_TIMEOUT_MS));
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(timeout)?;

        write_depth_query_frame(&mut stream, req)?;
        let payload = read_depth_query_frame(&mut stream, DEPTH_QUERY_PAYLOAD)?;
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
}
