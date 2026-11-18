use super::{
    manager::{ManagerPlanConfig, PlanReader},
    valid_id,
    wire::position_targets_server::{PositionTargets, PositionTargetsServer},
    wire::{Admission, PositionTarget, TargetReceipt},
    Target,
};
use anyhow::{ensure, Context, Result};
use crossbeam_channel::{Receiver, Sender, TrySendError};
use serde::Deserialize;
use std::{
    fs,
    net::{SocketAddr, TcpListener},
    os::unix::{
        fs::{FileTypeExt, MetadataExt},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Semaphore};
use tokio_stream::{
    wrappers::{TcpListenerStream, UnixListenerStream},
    Stream,
};
use tonic::{
    transport::{Certificate, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};

const QUEUE_CAPACITY: usize = 8;
pub(crate) const MAX_WIRE_SIZE: usize = 16 * 1024;
const ADMISSION_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_LEASE_US: u64 = 60_000_000;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source_id: String,
    /// Socket parent must be an existing directory owned by Exec with mode 0700.
    pub ipc_socket: Option<PathBuf>,
    pub grpc: Option<GrpcConfig>,
    pub manager_plan: Option<ManagerPlanConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    pub bind: SocketAddr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub client_ca: PathBuf,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            valid_id(&self.source_id),
            "invalid target ingress source_id"
        );
        ensure!(
            self.ipc_socket.is_some() || self.grpc.is_some() || self.manager_plan.is_some(),
            "configure a Manager plan reader, an IPC socket or a gRPC listener"
        );
        if let Some(manager) = &self.manager_plan {
            manager.validate()?;
        }
        if let Some(path) = &self.ipc_socket {
            ensure!(
                path.is_absolute() && path.file_name().is_some(),
                "target IPC socket path must be absolute"
            );
        }
        if let Some(grpc) = &self.grpc {
            ensure!(
                !grpc.bind.ip().is_unspecified() && grpc.bind.port() > 0,
                "target gRPC requires an explicit bind IP and port"
            );
            ensure!(
                [&grpc.certificate, &grpc.private_key, &grpc.client_ca]
                    .into_iter()
                    .all(|path| path.is_absolute()),
                "target gRPC mTLS paths must be absolute"
            );
        }
        Ok(())
    }
}

struct SocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl SocketPath {
    fn bind(path: &Path) -> Result<(UnixListener, Self)> {
        let parent = fs::symlink_metadata(path.parent().context("target IPC socket parent")?)?;
        ensure!(
            parent.is_dir()
                && parent.uid() == unsafe { libc::geteuid() }
                && parent.mode() & 0o077 == 0,
            "target IPC parent must be privately owned by the Exec user"
        );
        let listener = UnixListener::bind(path).context("bind private target IPC socket")?;
        let meta = fs::symlink_metadata(path)?;
        let socket = Self {
            path: path.into(),
            device: meta.dev(),
            inode: meta.ino(),
        };
        listener.set_nonblocking(true)?;
        Ok((listener, socket))
    }
}
impl Drop for SocketPath {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|meta| {
            meta.file_type().is_socket() && meta.ino() == self.inode && meta.dev() == self.device
        }) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct Pending {
    target: Target,
    expires: Instant,
    reply: oneshot::Sender<Result<(), String>>,
}

/// Target inputs use a single bounded queue. Only the Exec business loop
/// may consume it. The I/O worker never receives an SDK, OMS or strategy handle.
pub struct Ingress {
    pending: Receiver<Pending>,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl Ingress {
    pub fn start(config: &Config) -> Result<Self> {
        let manager = config
            .manager_plan
            .as_ref()
            .map(|manager| PlanReader::from_env(manager, &config.source_id))
            .transpose()?;
        Self::start_with_reader(config, manager)
    }

    pub(crate) fn start_with_reader(config: &Config, manager: Option<PlanReader>) -> Result<Self> {
        config.validate()?;
        let local = config
            .ipc_socket
            .as_deref()
            .map(SocketPath::bind)
            .transpose()?;
        let remote = config
            .grpc
            .as_ref()
            .map(|cfg| -> Result<_> {
                let listener = TcpListener::bind(cfg.bind).context("bind target gRPC listener")?;
                listener.set_nonblocking(true)?;
                let tls = ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        fs::read(&cfg.certificate)?,
                        fs::read(&cfg.private_key)?,
                    ))
                    .client_ca_root(Certificate::from_pem(fs::read(&cfg.client_ca)?));
                let server = server()
                    .tls_config(tls)
                    .context("load target gRPC mTLS identity and client CA")?;
                Ok((listener, server))
            })
            .transpose()?;
        let (send, pending) = crossbeam_channel::bounded(QUEUE_CAPACITY);
        let service = Service {
            source_id: config.source_id.clone(),
            started_us: now_us(),
            pending: send,
            streams: Arc::new(Semaphore::new(8)),
        };
        let (stop, stopping) = oneshot::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let worker = std::thread::Builder::new()
            .name("cn-futures-target-io".into())
            .spawn(move || {
                let remote_service = service.clone();
                let manager_service = service.clone();
                runtime.block_on(async move {
                    tokio::select! {
                    _ = stopping => Ok(()),
                    result = async move {
                        let Some((listener, _path)) = local else { return std::future::pending().await; };
                        let listener = UnixListenerStream::new(tokio::net::UnixListener::from_std(listener)?);
                        server().add_service(grpc_service(service)).serve_with_incoming(listener).await?;
                        Ok(())
                    } => result,
                    result = async move {
                        let Some((listener, mut server)) = remote else { return std::future::pending().await; };
                        let listener = TcpListenerStream::new(tokio::net::TcpListener::from_std(listener)?);
                        server.add_service(grpc_service(remote_service)).serve_with_incoming(listener).await?;
                        Ok(())
                    } => result,
                    result = async move {
                        let Some(manager) = manager else { return std::future::pending().await; };
                        manager.run(manager_service).await
                    } => result,
                    }
                })
            })
            .context("spawn target ingress I/O worker")?;
        Ok(Self {
            pending,
            stop: Some(stop),
            worker: Some(worker),
        })
    }

    /// Admit at most one target per existing business-loop iteration. Neither
    /// queue polling nor completing the one-shot receipt waits for I/O.
    pub fn poll(&self, admit: impl FnOnce(Target) -> Result<()>) -> bool {
        let Ok(request) = self.pending.try_recv() else {
            return false;
        };
        if !request.reply.is_closed()
            && Instant::now() < request.expires
            && now_us() < request.target.expires_at_us
        {
            let _ = request
                .reply
                .send(admit(request.target).map_err(|e| e.to_string()));
        }
        true
    }

    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_some_and(JoinHandle::is_finished)
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("position target ingress stopped: {error:#}"),
                Err(_) => eprintln!("position target ingress worker panicked"),
            }
        }
    }
}

fn server() -> Server {
    Server::builder()
        .tcp_nodelay(true)
        .max_concurrent_streams(Some(4))
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
}

fn grpc_service(service: Service) -> PositionTargetsServer<Service> {
    PositionTargetsServer::new(service)
        .max_decoding_message_size(MAX_WIRE_SIZE)
        .max_encoding_message_size(MAX_WIRE_SIZE)
}

#[derive(Clone)]
pub(crate) struct Service {
    source_id: String,
    started_us: u64,
    pending: Sender<Pending>,
    streams: Arc<Semaphore>,
}

#[tonic::async_trait]
impl PositionTargets for Service {
    type PushStream = Pin<Box<dyn Stream<Item = Result<TargetReceipt, Status>> + Send>>;

    async fn push(
        &self,
        request: Request<tonic::Streaming<PositionTarget>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        let permit = self
            .streams
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("target stream limit"))?;
        let mut stream = request.into_inner();
        let service = self.clone();
        Ok(Response::new(Box::pin(async_stream::try_stream! {
            let _permit = permit;
            while let Some(message) = stream.message().await? {
                yield service.submit(message).await;
            }
        })))
    }
}

impl Service {
    pub(crate) async fn submit(&self, message: PositionTarget) -> TargetReceipt {
        let mut receipt = TargetReceipt {
            source_id: message.source_id.clone(),
            batch_id: message.batch_id.clone(),
            target_revision: message.target_revision,
            admission: Admission::Rejected as i32,
            reason: String::new(),
        };
        let now = now_us();
        if !(self.started_us..=now).contains(&message.decision_us)
            || message.expires_at_us <= now
            || message.expires_at_us > message.decision_us.saturating_add(MAX_LEASE_US)
        {
            receipt.reason =
                "target is expired, future-dated, or predates this Exec runtime".into();
            return receipt;
        }
        let target = match Target::try_from(message) {
            Ok(target) if target.source_id == self.source_id => target,
            Ok(_) => {
                receipt.reason = "wrong execution source".into();
                return receipt;
            }
            Err(error) => {
                receipt.reason = error.to_string();
                return receipt;
            }
        };
        let expires = Instant::now() + ADMISSION_TIMEOUT;
        let (reply, wait) = oneshot::channel();
        match self.pending.try_send(Pending {
            target,
            expires,
            reply,
        }) {
            Ok(()) => match tokio::time::timeout_at(tokio::time::Instant::from_std(expires), wait)
                .await
            {
                Ok(Ok(Ok(()))) => receipt.admission = Admission::Accepted as i32,
                Ok(Ok(Err(error))) => receipt.reason = error,
                _ => {
                    receipt.admission = Admission::Busy as i32;
                    receipt.reason = "admission not confirmed; retry the identical target".into();
                }
            },
            Err(TrySendError::Full(_)) => {
                receipt.admission = Admission::Busy as i32;
                receipt.reason = "target queue full".into();
            }
            Err(TrySendError::Disconnected(_)) => receipt.reason = "Exec loop is stopped".into(),
        }
        receipt
    }
}

pub(crate) fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
