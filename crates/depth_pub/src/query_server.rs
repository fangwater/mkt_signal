use anyhow::{anyhow, Result};
use log::{info, warn};
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use super::query_msg::{build_depth_query_socket_path, DEPTH_QUERY_PAYLOAD};

const FRAME_LEN: usize = 2;

pub struct DepthQuerySocketServer {
    listener: UnixListener,
    socket_path: PathBuf,
}

pub struct DepthQueryConnection {
    stream: UnixStream,
    read_len_buf: [u8; FRAME_LEN],
    read_len_pos: usize,
    read_payload: Vec<u8>,
    read_payload_pos: usize,
    write_buf: Vec<u8>,
    write_pos: usize,
}

impl DepthQueryConnection {
    fn new(stream: UnixStream) -> Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            read_len_buf: [0u8; FRAME_LEN],
            read_len_pos: 0,
            read_payload: Vec::new(),
            read_payload_pos: 0,
            write_buf: Vec::new(),
            write_pos: 0,
        })
    }

    fn poll_request(&mut self) -> Result<Option<Vec<u8>>> {
        if !self.flush_pending_write()? {
            return Ok(None);
        }

        while self.read_len_pos < FRAME_LEN {
            match self
                .stream
                .read(&mut self.read_len_buf[self.read_len_pos..])
            {
                Ok(0) => return Err(anyhow!("depth query connection closed while reading len")),
                Ok(n) => self.read_len_pos += n,
                Err(err) if err.kind() == ErrorKind::WouldBlock => return Ok(None),
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(err.into()),
            }
        }

        if self.read_payload.is_empty() {
            let payload_len = u16::from_le_bytes(self.read_len_buf) as usize;
            if payload_len == 0 || payload_len > DEPTH_QUERY_PAYLOAD {
                return Err(anyhow!(
                    "invalid depth query frame len: {} max={}",
                    payload_len,
                    DEPTH_QUERY_PAYLOAD
                ));
            }
            self.read_payload.resize(payload_len, 0);
            self.read_payload_pos = 0;
        }

        while self.read_payload_pos < self.read_payload.len() {
            match self
                .stream
                .read(&mut self.read_payload[self.read_payload_pos..])
            {
                Ok(0) => {
                    return Err(anyhow!(
                        "depth query connection closed while reading payload"
                    ))
                }
                Ok(n) => self.read_payload_pos += n,
                Err(err) if err.kind() == ErrorKind::WouldBlock => return Ok(None),
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(err.into()),
            }
        }

        let payload = std::mem::take(&mut self.read_payload);
        self.read_len_buf = [0u8; FRAME_LEN];
        self.read_len_pos = 0;
        self.read_payload_pos = 0;
        Ok(Some(payload))
    }

    fn queue_response(&mut self, payload: &[u8]) -> Result<()> {
        if payload.is_empty() {
            return Err(anyhow!("depth query response payload is empty"));
        }
        if payload.len() > u16::MAX as usize {
            return Err(anyhow!("depth query response too large: {}", payload.len()));
        }
        if !self.write_buf.is_empty() && self.write_pos < self.write_buf.len() {
            return Err(anyhow!(
                "depth query response queued while previous response is pending"
            ));
        }

        self.write_buf.clear();
        self.write_buf
            .extend_from_slice(&(payload.len() as u16).to_le_bytes());
        self.write_buf.extend_from_slice(payload);
        self.write_pos = 0;
        let _ = self.flush_pending_write()?;
        Ok(())
    }

    fn flush_pending_write(&mut self) -> Result<bool> {
        while self.write_pos < self.write_buf.len() {
            match self.stream.write(&self.write_buf[self.write_pos..]) {
                Ok(0) => return Err(anyhow!("depth query connection closed while writing")),
                Ok(n) => self.write_pos += n,
                Err(err) if err.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(err.into()),
            }
        }

        if !self.write_buf.is_empty() {
            self.write_buf.clear();
            self.write_pos = 0;
        }
        Ok(true)
    }
}

impl DepthQuerySocketServer {
    pub fn bind(venue: &str) -> Result<Self> {
        let socket_path = build_depth_query_socket_path(venue);
        match fs::remove_file(&socket_path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }

        let listener = UnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;
        info!("Depth query UDS server ready: {}", socket_path.display());

        Ok(Self {
            listener,
            socket_path,
        })
    }

    pub fn poll<F>(
        &self,
        connections: &mut Vec<DepthQueryConnection>,
        max_accepts: usize,
        max_requests: usize,
        mut handler: F,
    ) -> Result<usize>
    where
        F: FnMut(&[u8], &mut [u8; DEPTH_QUERY_PAYLOAD]) -> usize,
    {
        let mut activity = 0usize;
        for _ in 0..max_accepts {
            match self.listener.accept() {
                Ok((stream, _addr)) => {
                    connections.push(DepthQueryConnection::new(stream)?);
                    activity = activity.saturating_add(1);
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }

        let mut handled = 0usize;
        let mut idx = 0usize;
        while idx < connections.len() {
            if handled >= max_requests {
                break;
            }

            let payload = match connections[idx].poll_request() {
                Ok(Some(payload)) => payload,
                Ok(None) => {
                    idx += 1;
                    continue;
                }
                Err(err) => {
                    warn!("Depth query connection dropped: {err:#}");
                    connections.swap_remove(idx);
                    activity = activity.saturating_add(1);
                    continue;
                }
            };

            let mut resp = [0u8; DEPTH_QUERY_PAYLOAD];
            let total_len = handler(&payload, &mut resp);
            if let Err(err) = connections[idx].queue_response(&resp[..total_len]) {
                warn!("Depth query response failed, dropping connection: {err:#}");
                connections.swap_remove(idx);
                activity = activity.saturating_add(1);
                continue;
            }

            handled += 1;
            activity = activity.saturating_add(1);
            idx += 1;
        }

        Ok(activity)
    }
}

impl Drop for DepthQuerySocketServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
    }
}
