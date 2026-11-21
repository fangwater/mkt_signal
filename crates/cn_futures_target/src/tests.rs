use super::wire::{
    manager_plans_server::{ManagerPlans, ManagerPlansServer},
    PlanStatus, PlanSubscription, PlanUpdate, PositionTarget,
};
use super::{ingress::now_us, wire::Admission, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{fs::Permissions, os::unix::fs::PermissionsExt, pin::Pin, time::Instant};
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};
use tokio_stream::{wrappers::TcpListenerStream, Stream};
use tonic::{Request, Response, Status};

fn target(revision: i64, decision_us: u64) -> Target {
    Target {
        source_id: "account-a".into(),
        batch_id: format!("decision-{revision}"),
        target_revision: revision,
        trading_day: "20261118".into(),
        expires_at_us: decision_us + 5_000_000,
        strategies: BTreeMap::from([
            (
                "alpha".into(),
                BTreeMap::from([("SHFE:zn2701".into(), 0), ("SHFE:zn2702".into(), 2)]),
            ),
            (
                "beta".into(),
                BTreeMap::from([("SHFE:zn2701".into(), 0), ("SHFE:zn2702".into(), -1)]),
            ),
        ]),
    }
}

fn ipc() -> (tempfile::TempDir, Config) {
    let private = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let config = Config {
        source_id: "account-a".into(),
        ipc_socket: Some(private.path().join("positions.sock")),
        grpc: None,
        manager_plan: None,
    };
    (private, config)
}

/// The Exec side: one thread owns the IoLoop and Ingress, driving I/O and
/// polling admission in its loop exactly like the business thread.
struct Owner {
    stop: crossbeam_channel::Sender<()>,
    polling: Arc<std::sync::atomic::AtomicBool>,
    admitted: Arc<std::sync::Mutex<Vec<Target>>>,
    thread: std::thread::JoinHandle<()>,
}
type Policy = Box<dyn FnMut(&Target, usize) -> anyhow::Result<()> + Send>;
fn owner(
    cfg: Config,
    reader: Option<manager::PlanReader>,
    polling: bool,
    mut policy: Policy,
) -> Owner {
    let (stop, stopped) = crossbeam_channel::bounded::<()>(1);
    let (ready, started) = crossbeam_channel::bounded::<()>(1);
    let polling = Arc::new(std::sync::atomic::AtomicBool::new(polling));
    let admitted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (poll_flag, accepted) = (polling.clone(), admitted.clone());
    let thread = std::thread::spawn(move || {
        let mut io = IoLoop::new(|| {}).unwrap();
        let ingress = Ingress::start_with_reader(&cfg, reader, &io).unwrap();
        ready.send(()).unwrap();
        let mut attempts = 0;
        while stopped.try_recv().is_err() {
            io.drive(Instant::now());
            if poll_flag.load(Ordering::Acquire) {
                ingress.poll(|target| {
                    attempts += 1;
                    let result = policy(&target, attempts);
                    if result.is_ok() {
                        accepted.lock().unwrap().push(target);
                    }
                    result
                });
            }
            std::thread::yield_now();
        }
    });
    started.recv().unwrap();
    Owner {
        stop,
        polling,
        admitted,
        thread,
    }
}
impl Owner {
    fn admitted(&self) -> Vec<Target> {
        self.admitted.lock().unwrap().clone()
    }
    fn finish(self) -> Vec<Target> {
        self.stop.send(()).unwrap();
        self.thread.join().expect("owner loop panicked");
        self.admitted.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn one_reused_local_stream_delivers_complete_targets_to_the_owner_loop() {
    let (_private, cfg) = ipc();
    let socket = cfg.ipc_socket.clone().unwrap();
    let owner = owner(
        cfg,
        None,
        true,
        Box::new(|target, n| {
            assert_eq!(target.target_revision, n as i64);
            assert_eq!(target.net("SHFE:zn2701"), 0);
            assert_eq!(target.net("SHFE:zn2702"), 1);
            assert_eq!(
                target.strategy_targets("SHFE:zn2702"),
                vec![("alpha".to_string(), 2), ("beta".to_string(), -1)]
            );
            Ok(())
        }),
    );
    let mut client = Client::connect_ipc(socket).await.unwrap();
    let mut micros = Vec::new();
    for revision in 1..=128 {
        let decision = now_us();
        let started = Instant::now();
        let result = client
            .send(target(revision, decision), decision)
            .await
            .unwrap();
        assert_eq!(result.admission, Admission::Accepted as i32);
        micros.push(started.elapsed().as_micros());
    }
    micros.sort_unstable();
    println!(
        "128-target Unix gRPC stream p50={}us p95={}us",
        micros[64], micros[121]
    );
    let decision = now_us();
    let mut wrong = target(129, decision);
    wrong.source_id = "account-b".into();
    assert_eq!(
        client.send(wrong, decision).await.unwrap().admission,
        Admission::Rejected as i32
    );
    assert_eq!(owner.finish().len(), 128);
}

#[tokio::test]
async fn an_unpolled_request_times_out_and_never_subsequently_reaches_admission() {
    let (_private, cfg) = ipc();
    let socket = cfg.ipc_socket.clone().unwrap();
    let owner = owner(
        cfg,
        None,
        false,
        Box::new(|_, _| panic!("timed-out target reached admission")),
    );
    let mut client = Client::connect_ipc(socket).await.unwrap();
    let decision = now_us();
    let result = client.send(target(1, decision), decision).await.unwrap();
    assert_eq!(result.admission, Admission::Busy as i32);
    owner.polling.store(true, Ordering::Release);
    sleep(Duration::from_millis(20)).await;
    let result = client.send(target(2, decision), 0).await.unwrap();
    assert_eq!(result.admission, Admission::Rejected as i32);
    assert!(owner.finish().is_empty());
}

const PLAN_TOKEN: &str = "transport-test-token-for-account-a";

#[test]
fn manager_plan_allows_a_plaintext_cross_host_origin() {
    let mut settings = ManagerPlanConfig {
        grpc_url: "http://192.0.2.10:18212".into(),
        token_env: "CTAKER_PLAN_TEST_TOKEN".into(),
    };
    settings.validate().unwrap();
    settings.grpc_url = "http://cn-manager.internal:18212".into();
    settings.validate().unwrap();
    settings.grpc_url.push_str("/manager");
    assert!(settings.validate().is_err());
}

struct MockManager {
    plan: watch::Sender<Option<PositionTarget>>,
    subscriptions: AtomicUsize,
    observation: bool,
}

#[tonic::async_trait]
impl ManagerPlans for Arc<MockManager> {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<PlanUpdate, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<PlanSubscription>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        assert_eq!(
            request.metadata().get("x-cta-plan-token").unwrap(),
            PLAN_TOKEN
        );
        assert_eq!(request.get_ref().source_id, "account-a");
        assert_eq!(request.get_ref().observe_only, self.observation);
        let mut plans = self.plan.subscribe();
        let observation = self.observation;
        self.subscriptions.fetch_add(1, Ordering::Release);
        let updates = async_stream::stream! {
            loop {
                let target = plans.borrow_and_update().clone();
                let (status, batch_id, target_revision) = match &target {
                    Some(target) => (
                        if observation { PlanStatus::ObserveOnly } else { PlanStatus::Ready },
                        target.batch_id.clone(),
                        target.target_revision,
                    ),
                    None => (PlanStatus::NoBatch, String::new(), 0),
                };
                yield Ok(PlanUpdate { target, status: status.into(), batch_id, target_revision });
                if plans.changed().await.is_err() { break; }
            }
        };
        Ok(Response::new(Box::pin(updates)))
    }
}

async fn wait_for(mut ready: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !ready() {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("Manager plan delivery did not progress");
}

async fn mock_manager(
    observation: bool,
    policy: Policy,
) -> (Arc<MockManager>, Owner, tokio::task::JoinHandle<()>) {
    let manager = Arc::new(MockManager {
        plan: watch::channel(None).0,
        subscriptions: AtomicUsize::new(0),
        observation,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let settings = ManagerPlanConfig {
        grpc_url: format!("http://{}", listener.local_addr().unwrap()),
        token_env: "CTAKER_PLAN_TEST_TOKEN".into(),
    };
    let mock = manager.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ManagerPlansServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap()
    });
    let cfg = Config {
        source_id: "account-a".into(),
        ipc_socket: None,
        grpc: None,
        manager_plan: Some(settings.clone()),
    };
    let reader =
        manager::PlanReader::new(&settings, &cfg.source_id, PLAN_TOKEN, observation).unwrap();
    (manager, owner(cfg, Some(reader), true, policy), server)
}

#[tokio::test]
async fn manager_stream_delivers_changes_and_retries_only_until_admitted() {
    let (manager, owner, server) = mock_manager(
        false,
        Box::new(|target, attempt| {
            assert_eq!(
                target.target_revision, 2,
                "only the live plan reaches admission"
            );
            if attempt == 1 {
                anyhow::bail!("temporary owner-loop recovery")
            }
            Ok(())
        }),
    )
    .await;
    let decision = now_us();
    let current = target(2, decision);
    let subscriptions = || manager.subscriptions.load(Ordering::Acquire);
    wait_for(|| subscriptions() == 1).await;
    let mut expired = target(1, decision).into_wire(decision);
    expired.expires_at_us = decision;
    manager.plan.send_replace(Some(expired));
    sleep(Duration::from_millis(40)).await;
    assert!(owner.admitted().is_empty());

    let plan = current.clone().into_wire(decision);
    let started = Instant::now();
    manager.plan.send_replace(Some(plan.clone()));
    wait_for(|| owner.admitted().len() == 1).await;
    println!(
        "Manager notification to Exec loop in {:?}",
        started.elapsed()
    );
    manager.plan.send_replace(Some(plan));
    sleep(Duration::from_millis(80)).await;
    assert_eq!(subscriptions(), 1);
    assert_eq!(
        owner.finish(),
        vec![current],
        "an admitted plan was queued again"
    );
    server.abort();
}

#[tokio::test]
async fn read_only_subscription_observes_existing_targets_without_an_execution_lease() {
    let mut archived = target(1, 1);
    archived.expires_at_us = 0;
    archived.trading_day.clear();
    let expected = archived.clone();
    let (manager, owner, server) = mock_manager(
        true,
        Box::new(move |target, _| {
            assert_eq!(*target, expected);
            Ok(())
        }),
    )
    .await;
    let message = archived.clone().into_wire(1);
    assert!(Target::try_from(message.clone()).is_err());
    manager.plan.send_replace(Some(message));
    wait_for(|| owner.admitted().len() == 1).await;
    owner.finish();
    server.abort();
}

#[test]
fn blocked_and_observational_statuses_cannot_carry_executable_targets() {
    let decision = now_us();
    let target = target(2, decision).into_wire(decision);
    let mut update = PlanUpdate {
        status: PlanStatus::LeaseExpired.into(),
        batch_id: target.batch_id.clone(),
        target_revision: target.target_revision,
        target: None,
    };
    assert_eq!(
        manager::update_status(&update, false).unwrap(),
        PlanStatus::LeaseExpired
    );
    update.target = Some(target);
    assert!(manager::update_status(&update, false).is_err());
    update.status = PlanStatus::Ready.into();
    assert!(manager::update_status(&update, false).is_ok());
    assert!(manager::update_status(&update, true).is_err());
    update.status = PlanStatus::Unspecified.into();
    assert!(manager::update_status(&update, false).is_err());
}

#[test]
fn strategy_vectors_must_be_complete_and_identical() {
    let mut incomplete = target(1, now_us());
    incomplete
        .strategies
        .get_mut("beta")
        .unwrap()
        .remove("SHFE:zn2701");
    assert!(Target::try_from(incomplete.into_wire(now_us())).is_err());
    let mut reserved = target(1, now_us());
    let row = reserved.strategies.remove("beta").unwrap();
    reserved.strategies.insert(ledger::UNALLOCATED.into(), row);
    assert!(Target::try_from(reserved.into_wire(now_us())).is_err());
    let round_trip = target(1, now_us());
    assert_eq!(
        Target::try_from(round_trip.clone().into_wire(now_us())).unwrap(),
        round_trip
    );
}
