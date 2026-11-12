use super::*;
use axum::body::{to_bytes, Body};
use axum::http::HeaderMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use tokio::task::JoinHandle;

#[derive(Default)]
struct Exchange {
    posts: AtomicUsize,
    ambiguous: AtomicBool,
    wrong_identity: AtomicBool,
    amount_available: Mutex<String>,
    submitted: Mutex<Option<Value>>,
}

async fn mock(
    State(exchange): State<Arc<Exchange>>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Response {
    let path = request.uri().path().to_owned();
    if path == "/auth/verify-login" {
        return if headers.get("cookie").and_then(|v| v.to_str().ok()) == Some("session=valid") {
            (StatusCode::NO_CONTENT, [("x-parameter-actor", "lgq")]).into_response()
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        };
    }
    let method = request.method().clone();
    let query: BTreeMap<String, String> =
        url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let body = to_bytes(request.into_body(), 16 * 1024).await.unwrap();
    let body: Option<Value> = if body.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(&body).unwrap())
    };
    let params = body
        .as_ref()
        .map(|v| {
            v.as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), model::scalar(v).unwrap_or_else(|| v.to_string())))
                .collect()
        })
        .unwrap_or_else(|| query.clone());
    let nonce = headers.get("nonce").unwrap().to_str().unwrap();
    assert_eq!(
        headers.get("signature").unwrap().to_str().unwrap(),
        trade_engine::ltp_rest::sign_params("fixture-secret", &params, nonce).unwrap()
    );
    assert_eq!(headers.get("x-mbx-apikey").unwrap(), "fixture-key");
    let result = match path.as_str() {
        "/api/v1/tradeAccount/list" => json!([]),
        "/api/v2/account/assets/trading/account-view" => json!({"totalSize":2,"list":[
            {"id":"1","primeType":"dma","venue":"BINANCE","accountMode":"CLASSIC","accountName":"Binance Test","status":"ACTIVE"},
            {"id":"2","primeType":"dma","venue":"OKX","accountMode":"3","accountName":"OKX Test","status":"ACTIVE"}
        ]}),
        "/api/v1/asset/currencies" => json!([{"USDT":{"networks":[{
            "network":"ERC20","withdrawEnable":if query.contains_key("fromAccount") {"true"} else {"false"},
            "depositEnable":"false",
            "minWithdraw":"5","minDeposit":"1","precision":6,"estNetworkFee":"0.6","estNetworkFeeCurrency":"USDT"
        }]}}]),
        "/api/v1/asset/getAccountCurrencyInfo" => {
            let id = query.get("accountId").unwrap();
            json!([{"accountId": id,"accountType":if id=="1" {"USDT/C-M"} else {"Trading"},"assetInfo":[
                {"currency":"USDT","totalBalance":"100","available":exchange.amount_available.lock().await.clone(),"withdrawAvailable":"80","network":null}
            ]}])
        }
        "/api/v1/transfer/apply" => {
            assert_eq!(method, Method::POST);
            exchange.posts.fetch_add(1, AtomicOrdering::SeqCst);
            if exchange.ambiguous.load(AtomicOrdering::SeqCst) {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"code":500000,"message":"upstream timeout"})),
                )
                    .into_response();
            }
            *exchange.submitted.lock().await = body;
            json!({"transferId": "9007199254740993"})
        }
        "/api/v1/transfer/get" => {
            let Some(mut submitted) = exchange.submitted.lock().await.clone() else {
                return Json(json!({"code":100001,"message":"not yet found"})).into_response();
            };
            if exchange.wrong_identity.load(AtomicOrdering::SeqCst) {
                submitted["amount"] = json!("11");
            }
            submitted["status"] = json!(1);
            submitted["transferId"] = json!("9007199254740993");
            submitted["rapidTransfer"] = json!(false);
            submitted["amountReceived"] = json!("9.4");
            submitted["networkFee"] = json!("0.6");
            submitted["feeCurrency"] = json!("USDT");
            submitted
        }
        "/api/v1/transfer/list" => json!([{
            "transferId":"out-of-scope","clientOrderId":null,"fromTradeAccountId":"999","toTradeAccountId":"998",
            "currency":"USDT","amount":"10","status":1,"createdAt":1700000000000_i64
        }]),
        _ => panic!("unexpected external endpoint {path}"),
    };
    Json(json!({"code":200,"data":result})).into_response()
}

struct Harness {
    app: Arc<App>,
    exchange: Arc<Exchange>,
    base: String,
    http: reqwest::Client,
    tasks: Vec<JoinHandle<()>>,
    directory: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

impl Harness {
    async fn new() -> Self {
        let exchange = Arc::new(Exchange::default());
        *exchange.amount_available.lock().await = "100".into();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_base = format!("http://{}", listener.local_addr().unwrap());
        let mock_router = Router::new().fallback(mock).with_state(exchange.clone());
        let mock_task = tokio::spawn(async move {
            axum::serve(listener, mock_router).await.unwrap();
        });
        let directory = std::env::temp_dir().join(format!("ltp-transfer-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let app = Arc::new(App {
            ltp: Ltp {
                http: http.clone(),
                base: mock_base.clone(),
                key: "fixture-key".into(),
                secret: "fixture-secret".into(),
            },
            auth_http: http.clone(),
            auth_url: format!("{mock_base}/auth/verify-login"),
            origin: "https://delist.example".into(),
            web_dir: directory.clone(),
            catalog: Mutex::new(None),
            previews: Mutex::new(BTreeMap::new()),
            store: Mutex::new(Store::open(&directory.join("audit")).unwrap()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = router(app.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            app,
            exchange,
            base,
            http,
            tasks: vec![mock_task, server],
            directory,
        }
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base))
            .header("Cookie", "session=valid")
            .header("Origin", "https://delist.example")
    }

    async fn preview(&self) -> Value {
        let response = self
            .request(Method::POST, "/api/transfers/preview")
            .json(&transfer())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }
}

fn transfer() -> TransferRequest {
    TransferRequest {
        from_account_id: "1".into(),
        to_account_id: "2".into(),
        from_account_type: "U_MARGINED_FUTURES".into(),
        to_account_type: "TRADING".into(),
        currency: "USDT".into(),
        amount: "10.000001".into(),
        network: Some("ERC20".into()),
        rapid_transfer: false,
    }
}

#[test]
fn decimals_and_identifiers_never_round() {
    assert_eq!(
        decimal_cmp(
            "9007199254740993.000000000000000001",
            "9007199254740993.000000000000000002"
        )
        .unwrap(),
        Ordering::Less
    );
    assert_eq!(decimal_cmp("00010.1000", "10.1").unwrap(), Ordering::Equal);
    for invalid in ["1e2", "NaN", "-1", ".5", "1.", "1.0.0", ""] {
        assert!(decimal_cmp(invalid, "0").is_err());
    }
    let mut request = transfer();
    request.from_account_id = "9007199254740993".into();
    let payload = request.ltp_request("unique", (false, false)).unwrap();
    assert_eq!(
        payload["fromTradeAccountId"].as_u64(),
        Some(9007199254740993)
    );
    assert_eq!(payload["amount"], "10.000001");
}

#[test]
fn timeout_and_unknown_business_errors_require_reconciliation() {
    for code in [400010, 400013, 404006, 500000, 123456] {
        assert!(!definitely_rejected(&ltp::Reply {
            http_status: 200,
            body: json!({"code":code})
        }));
    }
    assert!(definitely_rejected(&ltp::Reply {
        http_status: 200,
        body: json!({"code":403013})
    }));
    assert!(!definitely_rejected(&ltp::Reply {
        http_status: 503,
        body: json!({"code":403013})
    }));
}

#[tokio::test]
async fn login_origin_and_actor_cannot_be_forged() {
    let h = Harness::new().await;
    assert_eq!(
        h.http
            .get(format!("{}/api/catalog", h.base))
            .header("X-Parameter-Actor", "admin")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        h.http
            .post(format!("{}/api/transfers/preview", h.base))
            .header("Cookie", "session=valid")
            .json(&transfer())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        h.request(Method::POST, "/api/transfers/preview")
            .header("Origin", "https://attacker.example")
            .json(&transfer())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let p = h.preview().await;
    assert_eq!(
        h.app
            .previews
            .lock()
            .await
            .get(p["preview_id"].as_str().unwrap())
            .unwrap()
            .actor,
        "lgq"
    );
    assert_eq!(h.exchange.posts.load(AtomicOrdering::SeqCst), 0);
}

#[tokio::test]
async fn preview_checks_wallets_network_precision_and_fresh_balance() {
    let h = Harness::new().await;
    for request in [
        TransferRequest {
            from_account_id: "999".into(),
            ..transfer()
        },
        TransferRequest {
            from_account_type: "FUNDING".into(),
            ..transfer()
        },
        TransferRequest {
            amount: "10.0000001".into(),
            ..transfer()
        },
        TransferRequest {
            network: Some("TRC20".into()),
            ..transfer()
        },
        TransferRequest {
            amount: "81".into(),
            ..transfer()
        },
    ] {
        assert_eq!(
            h.request(Method::POST, "/api/transfers/preview")
                .json(&request)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let p = h.preview().await;
    *h.exchange.amount_available.lock().await = "1".into();
    let response = h
        .request(Method::POST, "/api/transfers")
        .json(&Submit {
            preview_id: p["preview_id"].as_str().unwrap().into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.json::<Value>().await.unwrap()["submitted"], false);
    assert_eq!(h.exchange.posts.load(AtomicOrdering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_submission_is_sent_exactly_once_and_preserves_receipt() {
    let h = Harness::new().await;
    let p = h.preview().await;
    let input = Submit {
        preview_id: p["preview_id"].as_str().unwrap().into(),
    };
    for _ in 0..2 {
        let response = h
            .request(Method::POST, "/api/transfers")
            .json(&input)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let record: Value = response.json().await.unwrap();
        assert_eq!(record["status"], "success");
        assert_eq!(record["transfer_id"], "9007199254740993");
        assert_eq!(record["amount_received"], "9.4");
        assert_eq!(record["network_fee"], "0.6");
    }
    assert_eq!(h.exchange.posts.load(AtomicOrdering::SeqCst), 1);
    let response = h
        .request(Method::GET, "/api/transfers")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(response["records"].as_array().unwrap().len(), 1);
    assert_eq!(response["records"][0]["actor"], "lgq");
}

#[tokio::test]
async fn ambiguous_post_blocks_replacement_and_survives_store_reopen() {
    let h = Harness::new().await;
    h.exchange.ambiguous.store(true, AtomicOrdering::SeqCst);
    let p = h.preview().await;
    let input = Submit {
        preview_id: p["preview_id"].as_str().unwrap().into(),
    };
    let response = h
        .request(Method::POST, "/api/transfers")
        .json(&input)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.json::<Value>().await.unwrap()["status"], "unknown");
    assert_eq!(
        h.request(Method::POST, "/api/transfers/preview")
            .json(&transfer())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let response = h
        .request(Method::POST, "/api/transfers")
        .json(&input)
        .send()
        .await
        .unwrap();
    assert_eq!(response.json::<Value>().await.unwrap()["status"], "unknown");
    assert_eq!(h.exchange.posts.load(AtomicOrdering::SeqCst), 1);
    let copy = h.directory.join("reopened");
    std::fs::create_dir_all(&copy).unwrap();
    std::fs::copy(
        h.directory
            .join("audit")
            .join(format!("{}.json", input.preview_id)),
        copy.join(format!("{}.json", input.preview_id)),
    )
    .unwrap();
    let reopened = Store::open(&copy).unwrap();
    assert!(reopened.blocking("1").unwrap().is_some());
    assert!(Store::open(&copy).is_err());
}

#[tokio::test]
async fn mismatched_exchange_evidence_does_not_finalize_a_transfer() {
    let h = Harness::new().await;
    h.exchange
        .wrong_identity
        .store(true, AtomicOrdering::SeqCst);
    let p = h.preview().await;
    let response = h
        .request(Method::POST, "/api/transfers")
        .json(&Submit {
            preview_id: p["preview_id"].as_str().unwrap().into(),
        })
        .send()
        .await
        .unwrap();
    let record: Value = response.json().await.unwrap();
    assert_eq!(record["status"], "pending");
    assert!(record["reason"]
        .as_str()
        .unwrap()
        .contains("金额与原请求不一致"));
    assert!(h.app.store.lock().await.blocking("1").unwrap().is_some());
}

#[tokio::test]
async fn expired_preview_cannot_submit_and_audit_corruption_fails_closed() {
    let h = Harness::new().await;
    let p = h.preview().await;
    let id = p["preview_id"].as_str().unwrap();
    h.app.previews.lock().await.get_mut(id).unwrap().expires_at =
        Utc::now() - chrono::Duration::seconds(1);
    let response = h
        .request(Method::POST, "/api/transfers")
        .json(&Submit {
            preview_id: id.into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(response.json::<Value>().await.unwrap()["submitted"], false);
    assert_eq!(h.exchange.posts.load(AtomicOrdering::SeqCst), 0);
    let directory = h.directory.join("corrupt");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("attempt.json"), "broken evidence").unwrap();
    assert!(Store::open(&directory).is_err());
}
