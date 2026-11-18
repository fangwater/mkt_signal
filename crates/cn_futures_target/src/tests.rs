use super::wire::{
    manager_plans_server::{ManagerPlans, ManagerPlansServer},
    PlanSubscription, PlanUpdate, PositionTarget,
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
        net_targets: BTreeMap::from([("SHFE:zn2701".into(), 0), ("SHFE:zn2702".into(), 1)]),
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

#[tokio::test]
async fn one_reused_local_stream_delivers_complete_targets_to_the_owner_loop() {
    let (_private, cfg) = ipc();
    let ingress = Ingress::start(&cfg).unwrap();
    let mut client = Client::connect_ipc(cfg.ipc_socket.unwrap()).await.unwrap();
    let (stop, stopped) = crossbeam_channel::bounded::<()>(1);
    let owner = std::thread::spawn(move || {
        let mut admitted = 0;
        while stopped.try_recv().is_err() {
            if !ingress.poll(|target| {
                admitted += 1;
                assert_eq!(target.target_revision, admitted);
                assert_eq!(target.net_targets["SHFE:zn2701"], 0);
                assert_eq!(target.net_targets["SHFE:zn2702"], 1);
                Ok(())
            }) {
                std::thread::yield_now();
            }
        }
        admitted
    });
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
    stop.send(()).unwrap();
    assert_eq!(owner.join().unwrap(), 128);
}

#[tokio::test]
async fn an_unpolled_request_times_out_and_never_subsequently_reaches_admission() {
    let (_private, cfg) = ipc();
    let ingress = Ingress::start(&cfg).unwrap();
    let mut client = Client::connect_ipc(cfg.ipc_socket.unwrap()).await.unwrap();
    let decision = now_us();
    let result = client.send(target(1, decision), decision).await.unwrap();
    assert_eq!(result.admission, Admission::Busy as i32);
    assert!(ingress.poll(|_| panic!("timed-out target reached admission")));
    let result = client.send(target(2, decision), 0).await.unwrap();
    assert_eq!(result.admission, Admission::Rejected as i32);
    assert!(!ingress.poll(|_| panic!("target from the previous runtime reached admission")));
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
        let mut plans = self.plan.subscribe();
        self.subscriptions.fetch_add(1, Ordering::Release);
        let updates = async_stream::stream! {
            loop {
                let target = plans.borrow_and_update().clone();
                yield Ok(PlanUpdate { target });
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

#[tokio::test]
async fn manager_stream_delivers_changes_and_retries_only_until_admitted() {
    let manager = Arc::new(MockManager {
        plan: watch::channel(None).0,
        subscriptions: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let settings = ManagerPlanConfig {
        grpc_url: format!("http://{}", listener.local_addr().unwrap()),
        token_env: "CTAKER_PLAN_TEST_TOKEN".into(),
    };
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ManagerPlansServer::new(manager.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let cfg = Config {
        source_id: "account-a".into(),
        ipc_socket: None,
        grpc: None,
        manager_plan: Some(settings.clone()),
    };
    let reader = manager::PlanReader::new(&settings, &cfg.source_id, PLAN_TOKEN).unwrap();
    let ingress = Ingress::start_with_reader(&cfg, Some(reader)).unwrap();
    let subscriptions = || manager.subscriptions.load(Ordering::Acquire);
    wait_for(|| subscriptions() == 1).await;
    assert!(!ingress.poll(|_| panic!("empty subscription produced a target")));
    let decision = now_us();
    let mut expired = target(1, decision).into_wire(decision);
    expired.expires_at_us = decision;
    manager.plan.send_replace(Some(expired));
    sleep(Duration::from_millis(40)).await;
    assert!(!ingress.poll(|_| panic!("expired Manager plan reached the owner loop")));

    let decision = now_us();
    let current = target(2, decision);
    let plan = current.clone().into_wire(decision);
    let started = Instant::now();
    manager.plan.send_replace(Some(plan.clone()));
    wait_for(|| {
        ingress.poll(|first| {
            assert_eq!(first, current);
            anyhow::bail!("temporary owner-loop recovery")
        })
    })
    .await;
    println!(
        "Manager notification to Exec loop in {:?}",
        started.elapsed()
    );
    wait_for(|| {
        ingress.poll(|retry| {
            assert_eq!(retry, current);
            Ok(())
        })
    })
    .await;
    manager.plan.send_replace(Some(plan));
    sleep(Duration::from_millis(80)).await;
    assert!(!ingress.poll(|_| panic!("an admitted Manager plan was queued again")));
    assert_eq!(subscriptions(), 1);
    server.abort();
}
