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

const DEPTH_QUERY_STREAM_WRITE_TIMEOUT_MS: u64 = 20;

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

    pub fn accept(&self) -> Result<Option<UnixStream>> {
        match self.listener.accept() {
            Ok((stream, _addr)) => {
                stream.set_nonblocking(false)?;
                Ok(Some(stream))
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn serve_stream<F>(mut stream: UnixStream, mut handler: F) -> Result<()>
    where
        F: FnMut(&[u8], &mut [u8; DEPTH_QUERY_PAYLOAD]) -> usize,
    {
        stream.set_read_timeout(None)?;
        let timeout = Some(Duration::from_millis(DEPTH_QUERY_STREAM_WRITE_TIMEOUT_MS));
        stream.set_write_timeout(timeout)?;

        loop {
            let payload = match read_depth_query_frame(&mut stream, DEPTH_QUERY_PAYLOAD) {
                Ok(payload) => payload,
                Err(err) if is_stream_closed_error(&err) => return Ok(()),
                Err(err) => return Err(err),
            };
            let mut resp = [0u8; DEPTH_QUERY_PAYLOAD];
            let total_len = handler(&payload, &mut resp);
            write_depth_query_frame(&mut stream, &resp[..total_len])?;
        }
    }
}

fn is_stream_closed_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                matches!(
                    io_err.kind(),
                    ErrorKind::UnexpectedEof
                        | ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::NotConnected
                )
            })
    })
}

impl Drop for DepthQuerySocketServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
    }
}
