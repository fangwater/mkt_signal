use super::{
    ingress::{now_us, Service, MAX_WIRE_SIZE},
    valid_id,
    wire::{
        manager_plans_client::ManagerPlansClient, Admission, PlanStatus, PlanSubscription,
        PlanUpdate, PositionTarget,
    },
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

pub(super) fn update_status(update: &PlanUpdate, observation: bool) -> Result<PlanStatus> {
    let status = PlanStatus::try_from(update.status).context("unknown Manager plan status")?;
    ensure!(
        status != PlanStatus::Unspecified,
        "Manager plan status is missing"
    );
    if status == PlanStatus::NoBatch {
        ensure!(
            update.batch_id.is_empty() && update.target_revision == 0,
            "empty Manager plan status refers to a batch"
        );
    } else {
        ensure!(
            valid_id(&update.batch_id) && update.target_revision > 0,
            "invalid Manager plan status identity"
        );
    }
    let expect_target = if observation {
        matches!(
            status,
            PlanStatus::ObserveOnly | PlanStatus::NoExternalDemand
        )
    } else {
        status == PlanStatus::Ready
    };
    ensure!(
        update.target.is_some() == expect_target,
        "Manager target payload does not match its status and subscription mode"
    );
    if let Some(target) = &update.target {
        ensure!(
            target.batch_id == update.batch_id && target.target_revision == update.target_revision,
            "Manager target identity does not match its status"
        );
    }
    Ok(status)
}

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
    observation: bool,
}

struct SubscriptionState {
    admitted: Option<PositionTarget>,
    last_status: Option<(PlanStatus, String, i64)>,
    last_error: Option<String>,
    reconnect: Duration,
}

impl PlanReader {
    pub(crate) fn from_env(
        config: &ManagerPlanConfig,
        source_id: &str,
        observation: bool,
    ) -> Result<Self> {
        let token = std::env::var(&config.token_env).context("missing Manager plan credential")?;
        Self::new(config, source_id, &token, observation)
    }

    pub(crate) fn new(
        config: &ManagerPlanConfig,
        source_id: &str,
        token: &str,
        observation: bool,
    ) -> Result<Self> {
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
            observation,
        })
    }

    pub(crate) async fn run(self, service: Service) -> Result<()> {
        let mut state = SubscriptionState {
            admitted: None,
            last_status: None,
            last_error: None,
            reconnect: Duration::from_millis(100),
        };
        loop {
            let outcome = self.read_stream(&service, &mut state).await;
            let error = outcome.err().map(|error| format!("{error:#}"));
            if error != state.last_error {
                if let Some(error) = &error {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "type":"manager_plan_transport_error", "source_id":self.source_id,
                            "level":"warn", "error":error,
                        })
                    );
                }
                state.last_error = error;
            }
            sleep(state.reconnect).await;
            state.reconnect = (state.reconnect * 2).min(Duration::from_secs(2));
        }
    }

    async fn read_stream(&self, service: &Service, state: &mut SubscriptionState) -> Result<()> {
        let channel = self
            .endpoint
            .connect()
            .await
            .context("connect Manager gRPC")?;
        let mut request = Request::new(PlanSubscription {
            source_id: self.source_id.clone(),
            observe_only: self.observation,
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
        let mut first_update = true;
        loop {
            let attempt = async {
                sleep_until(retry_at).await;
                let plan = pending.as_ref().expect("admission requires a pending plan");
                if !self.observation && plan.expires_at_us <= now_us() {
                    return None;
                }
                Some(service.submit(plan.clone(), self.observation).await)
            };
            tokio::select! {
                biased;
                update = stream.message() => {
                    let update = update?.context("Manager plan subscription closed")?;
                    let status = update_status(&update, self.observation)?;
                    state.reconnect = Duration::from_millis(100);
                    if first_update {
                        eprintln!("Manager target subscription connected: source={} observe_only={}",
                            self.source_id, self.observation);
                        if state.last_error.take().is_some() {
                            eprintln!("{}", serde_json::json!({
                                "type":"manager_plan_transport_recovered","level":"info",
                                "source_id":self.source_id,
                            }));
                        }
                        first_update = false;
                    }
                    let status_key = (status, update.batch_id, update.target_revision);
                    if state.last_status.as_ref() != Some(&status_key) {
                        let level = match status {
                            PlanStatus::Ready | PlanStatus::NoBatch | PlanStatus::NoExternalDemand |
                            PlanStatus::ObserveOnly | PlanStatus::Refreshing => "info",
                            _ => "warn",
                        };
                        eprintln!("{}", serde_json::json!({
                            "type":"manager_plan_status","level":level,"source_id":self.source_id,
                            "observe_only":self.observation,"batch_id":status_key.1,
                            "target_revision":status_key.2,
                            "status":status.as_str_name().trim_start_matches("PLAN_STATUS_").to_ascii_lowercase(),
                        }));
                        state.last_status = Some(status_key);
                    }
                    let next = update.target.filter(|plan| state.admitted.as_ref() != Some(plan));
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
                            eprintln!("Manager target delivered: source={} batch={} revision={} observe_only={}",
                                receipt.source_id, receipt.batch_id, receipt.target_revision, self.observation);
                            state.admitted = pending.take();
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
