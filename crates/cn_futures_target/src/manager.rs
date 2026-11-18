use super::{
    ingress::{now_us, Service, MAX_WIRE_SIZE},
    wire::{manager_plans_client::ManagerPlansClient, Admission, PlanSubscription, PositionTarget},
};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::{sleep, sleep_until, Instant};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::{ClientTlsConfig, Endpoint},
    Request,
};

const INITIAL_BACKOFF: Duration = Duration::from_millis(25);

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagerPlanConfig {
    pub grpc_url: String,
    pub token_env: String,
}

impl ManagerPlanConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        self.endpoint()?;
        ensure!(
            !self.token_env.is_empty()
                && self
                    .token_env
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid Manager plan token environment name"
        );
        Ok(())
    }

    fn endpoint(&self) -> Result<Endpoint> {
        let endpoint =
            Endpoint::from_shared(self.grpc_url.clone()).context("invalid Manager gRPC URL")?;
        let uri = endpoint.uri();
        ensure!(
            uri.authority()
                .is_some_and(|value| !value.as_str().contains('@'))
                && matches!(uri.scheme_str(), Some("http" | "https"))
                && matches!(uri.path(), "" | "/")
                && uri.query().is_none(),
            "Manager gRPC requires an HTTP/2 origin using http:// or https://"
        );
        let endpoint = if uri.scheme_str() == Some("https") {
            endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?
        } else {
            endpoint
        };
        Ok(endpoint
            .connect_timeout(Duration::from_secs(3))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(15))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true))
    }
}

pub(crate) struct PlanReader {
    endpoint: Endpoint,
    token: MetadataValue<Ascii>,
    source_id: String,
}

impl PlanReader {
    pub(crate) fn from_env(config: &ManagerPlanConfig, source_id: &str) -> Result<Self> {
        let token = std::env::var(&config.token_env).context("missing Manager plan credential")?;
        Self::new(config, source_id, &token)
    }

    pub(crate) fn new(config: &ManagerPlanConfig, source_id: &str, token: &str) -> Result<Self> {
        ensure!(
            (32..=256).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_graphic()),
            "invalid Manager plan credential"
        );
        let mut token: MetadataValue<Ascii> =
            token.parse().context("invalid Manager plan credential")?;
        token.set_sensitive(true);
        Ok(Self {
            endpoint: config.endpoint()?,
            source_id: source_id.to_owned(),
            token,
        })
    }

    pub(crate) async fn run(self, service: Service) -> Result<()> {
        let mut admitted = None;
        let mut last_error = None;
        let mut reconnect = Duration::from_millis(100);
        loop {
            let outcome = self
                .read_stream(&service, &mut admitted, &mut reconnect)
                .await;
            let error = outcome.err().map(|error| format!("{error:#}"));
            if error != last_error {
                if let Some(error) = &error {
                    eprintln!("Manager target stream: {error}");
                }
                last_error = error;
            }
            sleep(reconnect).await;
            reconnect = (reconnect * 2).min(Duration::from_secs(2));
        }
    }

    async fn read_stream(
        &self,
        service: &Service,
        admitted: &mut Option<PositionTarget>,
        reconnect: &mut Duration,
    ) -> Result<()> {
        let channel = self
            .endpoint
            .connect()
            .await
            .context("connect Manager gRPC")?;
        let mut request = Request::new(PlanSubscription {
            source_id: self.source_id.clone(),
        });
        request
            .metadata_mut()
            .insert("x-cta-plan-token", self.token.clone());
        let mut stream = tokio::time::timeout(
            Duration::from_secs(3),
            ManagerPlansClient::new(channel)
                .max_decoding_message_size(MAX_WIRE_SIZE)
                .watch(request),
        )
        .await
        .context("open Manager plan subscription timed out")??
        .into_inner();
        let mut pending: Option<PositionTarget> = None;
        let mut delay = INITIAL_BACKOFF;
        let mut retry_at = Instant::now();
        let mut rejection = None;
        loop {
            let attempt = async {
                sleep_until(retry_at).await;
                let plan = pending.as_ref().expect("admission requires a pending plan");
                if plan.expires_at_us <= now_us() {
                    return None;
                }
                Some(service.submit(plan.clone()).await)
            };
            tokio::select! {
                biased;
                update = stream.message() => {
                    let update = update?.context("Manager plan subscription closed")?;
                    *reconnect = Duration::from_millis(100);
                    let next = update.target.filter(|plan| admitted.as_ref() != Some(plan));
                    if next != pending {
                        pending = next;
                        delay = INITIAL_BACKOFF;
                        retry_at = Instant::now();
                        rejection = None;
                    }
                }
                receipt = attempt, if pending.is_some() => {
                    match receipt {
                        None => pending = None,
                        Some(receipt) if receipt.admission == Admission::Accepted as i32 => {
                            eprintln!("Manager target admitted: source={} batch={} revision={}",
                                receipt.source_id, receipt.batch_id, receipt.target_revision);
                            *admitted = pending.take();
                        }
                        Some(receipt) => {
                            if rejection.as_ref() != Some(&receipt.reason) {
                                eprintln!("Manager target admission: {}", receipt.reason);
                                rejection = Some(receipt.reason);
                            }
                            retry_at = Instant::now() + delay;
                            delay = (delay * 2).min(Duration::from_millis(250));
                        }
                    }
                }
            }
        }
    }
}
