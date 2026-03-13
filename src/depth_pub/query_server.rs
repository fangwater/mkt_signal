use anyhow::Result;
use log::info;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Duration;

use super::query_msg::{
    build_depth_query_socket_path, read_depth_query_frame, write_depth_query_frame,
    DEPTH_QUERY_PAYLOAD,
};

const DEPTH_QUERY_STREAM_TIMEOUT_MS: u64 = 20;

pub struct DepthQuerySocketServer {
    listener: UnixListener,
    socket_path: PathBuf,
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

    pub fn serve_once<F>(&self, mut handler: F) -> Result<bool>
    where
        F: FnMut(&[u8], &mut [u8; DEPTH_QUERY_PAYLOAD]) -> usize,
    {
        let Some(mut stream) = self.accept()? else {
            return Ok(false);
        };

        let timeout = Some(Duration::from_millis(DEPTH_QUERY_STREAM_TIMEOUT_MS));
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(timeout)?;

        self.handle_stream(&mut stream, &mut handler)?;
        Ok(true)
    }

    fn accept(&self) -> Result<Option<UnixStream>> {
        match self.listener.accept() {
            Ok((stream, _addr)) => Ok(Some(stream)),
            Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn handle_stream<F>(&self, stream: &mut UnixStream, handler: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &mut [u8; DEPTH_QUERY_PAYLOAD]) -> usize,
    {
        let payload = read_depth_query_frame(stream, DEPTH_QUERY_PAYLOAD)?;
        let mut resp = [0u8; DEPTH_QUERY_PAYLOAD];
        let total_len = handler(&payload, &mut resp);
        write_depth_query_frame(stream, &resp[..total_len])?;
        Ok(())
    }
}

impl Drop for DepthQuerySocketServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
    }
}
