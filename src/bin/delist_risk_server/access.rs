//! Login and per-session FR entry grants. Nginx checks the same session for
//! HTML, API and websocket requests; upstream ports must remain private.

use anyhow::{ensure, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Form, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use openssl::{hash::MessageDigest, memcmp, pkcs5::pbkdf2_hmac, rand::rand_bytes};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

const COOKIE: &str = "delist_session";
const MAX_SESSIONS: usize = 1024;
const MAX_LOGIN_SOURCES: usize = 4096;
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    username: String,
    password_salt: String,
    password_hash: String,
    password_iterations: usize,
    public_origin: String,
    fr_envs: BTreeSet<String>,
}

struct Session {
    expires: Instant,
    grants: BTreeSet<String>,
}

pub(super) struct Access {
    config: Config,
    salt: Vec<u8>,
    hash: Vec<u8>,
    sessions: Mutex<BTreeMap<String, Session>>,
    attempts: Mutex<BTreeMap<String, (Instant, usize)>>,
    login_slots: tokio::sync::Semaphore,
}

impl Access {
    pub(super) fn load(path: &Path) -> Result<Arc<Self>> {
        let config: Config = serde_json::from_slice(
            &std::fs::read(path)
                .with_context(|| format!("read access config {}", path.display()))?,
        )
        .context("parse delist access config")?;
        Self::new(config).map(Arc::new)
    }

    fn new(config: Config) -> Result<Self> {
        ensure!(!config.username.is_empty(), "access username is required");
        ensure!(
            (600_000..=2_000_000).contains(&config.password_iterations),
            "invalid password iterations"
        );
        let salt = hex::decode(&config.password_salt).context("invalid password salt")?;
        let hash = hex::decode(&config.password_hash).context("invalid password hash")?;
        ensure!(
            salt.len() >= 16 && hash.len() == 32,
            "invalid password hash length"
        );
        let origin = reqwest::Url::parse(&config.public_origin).context("invalid public origin")?;
        ensure!(
            matches!(origin.scheme(), "http" | "https")
                && origin.origin().ascii_serialization() == config.public_origin,
            "public_origin must be an HTTP origin without a path"
        );
        ensure!(
            !config.fr_envs.is_empty()
                && config.fr_envs.iter().all(|env| env.contains("_fr_arb")
                    && env.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')),
            "invalid FR environment allowlist"
        );
        Ok(Self {
            config,
            salt,
            hash,
            sessions: Mutex::new(BTreeMap::new()),
            attempts: Mutex::new(BTreeMap::new()),
            login_slots: tokio::sync::Semaphore::new(2),
        })
    }

    fn token(headers: &HeaderMap) -> Option<&str> {
        let mut tokens = headers
            .get_all(header::COOKIE)
            .iter()
            .flat_map(|h| h.to_str().ok())
            .flat_map(|h| h.split(';'))
            .filter_map(|part| part.trim().split_once('='))
            .filter_map(|(name, value)| (name == COOKIE).then_some(value));
        let token = tokens.next()?;
        (tokens.next().is_none()
            && token.len() == 64
            && token.bytes().all(|c| c.is_ascii_hexdigit()))
        .then_some(token)
    }

    fn authorized(&self, headers: &HeaderMap, env: Option<&str>) -> bool {
        let Some(token) = Self::token(headers) else {
            return false;
        };
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, session| session.expires > Instant::now());
        sessions.get(token).is_some_and(|session| {
            env.is_none_or(|env| self.config.fr_envs.contains(env) && session.grants.contains(env))
        })
    }

    fn same_origin(&self, headers: &HeaderMap) -> bool {
        headers.get(header::ORIGIN).and_then(|h| h.to_str().ok())
            == Some(self.config.public_origin.as_str())
            && headers
                .get("sec-fetch-site")
                .is_none_or(|h| h != "cross-site")
    }

    fn cookie(&self, token: &str, age: u64) -> String {
        format!(
            "{COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={age}{}",
            if self.config.public_origin.starts_with("https:") {
                "; Secure"
            } else {
                ""
            }
        )
    }

    fn allow_attempt(&self, headers: &HeaderMap) -> bool {
        // Only the loopback Nginx proxy supplies this header. Do not expose the backend.
        let ip = headers
            .get("x-real-ip")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("local");
        let now = Instant::now();
        let mut attempts = self.attempts.lock();
        attempts.retain(|_, (start, _)| now.duration_since(*start) < LOGIN_WINDOW);
        if !attempts.contains_key(ip) && attempts.len() >= MAX_LOGIN_SOURCES {
            return false;
        }
        let (_, count) = attempts.entry(ip.to_owned()).or_insert((now, 0));
        *count += 1;
        *count <= 10
    }

    fn check_password(&self, username: &str, password: &str) -> Result<bool> {
        let mut hash = [0u8; 32];
        pbkdf2_hmac(
            password.as_bytes(),
            &self.salt,
            self.config.password_iterations,
            MessageDigest::sha256(),
            &mut hash,
        )
        .context("verify password hash")?;
        Ok(memcmp::eq(&hash, &self.hash) && username == self.config.username)
    }

    fn create_session(&self) -> Result<String> {
        let mut bytes = [0u8; 32];
        rand_bytes(&mut bytes).context("generate session")?;
        let token = hex::encode(bytes);
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, session| session.expires > Instant::now());
        ensure!(sessions.len() < MAX_SESSIONS, "session capacity reached");
        sessions.insert(
            token.clone(),
            Session {
                expires: Instant::now() + SESSION_TTL,
                grants: BTreeSet::new(),
            },
        );
        Ok(token)
    }
}

pub(super) fn protect(app: Router, access: Arc<Access>) -> Router {
    app.layer(middleware::from_fn_with_state(
        access.clone(),
        require_session,
    ))
    .merge(
        Router::new()
            .route("/login", get(login_page).post(login))
            .route("/logout", post(logout))
            .route("/open", get(open_panel))
            .route("/auth/verify", get(verify))
            .layer(DefaultBodyLimit::max(4096))
            .with_state(access),
    )
}

async fn require_session(
    State(access): State<Arc<Access>>,
    request: Request,
    next: Next,
) -> Response {
    if !access.authorized(request.headers(), None) {
        return if matches!(request.uri().path(), "/" | "/index.html") {
            Redirect::to("/delist/login").into_response()
        } else {
            denied(StatusCode::UNAUTHORIZED, "请先登录 delist")
        };
    }
    if request.method() != "GET"
        && request.method() != "HEAD"
        && !access.same_origin(request.headers())
    {
        return denied(StatusCode::FORBIDDEN, "请求来源无效");
    }
    private(next.run(request).await)
}

fn private(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::X_FRAME_OPTIONS, "DENY".parse().unwrap());
    response
}

fn denied(status: StatusCode, message: &str) -> Response {
    private((status, Json(json!({"error": message}))).into_response())
}

async fn login_page(State(access): State<Arc<Access>>, headers: HeaderMap) -> Response {
    if access.authorized(&headers, None) {
        return Redirect::to("/delist/").into_response();
    }
    private(Html(include_str!("../../../web/delist_risk/login.html")).into_response())
}

#[derive(Deserialize)]
struct Login {
    username: String,
    password: String,
}

async fn login(
    State(access): State<Arc<Access>>,
    headers: HeaderMap,
    Form(input): Form<Login>,
) -> Response {
    if !access.same_origin(&headers) {
        return denied(StatusCode::FORBIDDEN, "请求来源无效");
    }
    if !access.allow_attempt(&headers) {
        return denied(
            StatusCode::TOO_MANY_REQUESTS,
            "尝试过于频繁，请一分钟后重试",
        );
    }
    let Ok(_slot) = access.login_slots.try_acquire() else {
        return denied(StatusCode::TOO_MANY_REQUESTS, "请稍后重试");
    };
    let check = access.clone();
    let verified =
        tokio::task::spawn_blocking(move || check.check_password(&input.username, &input.password))
            .await;
    match verified {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => return denied(StatusCode::UNAUTHORIZED, "账户或密码错误"),
        _ => return denied(StatusCode::SERVICE_UNAVAILABLE, "登录暂不可用"),
    }
    let Ok(token) = access.create_session() else {
        return denied(StatusCode::SERVICE_UNAVAILABLE, "登录暂不可用");
    };
    // Re-login replaces the old session, including all its panel grants.
    if let Some(old) = Access::token(&headers) {
        access.sessions.lock().remove(old);
    }
    private(
        (
            [(
                header::SET_COOKIE,
                access.cookie(&token, SESSION_TTL.as_secs()),
            )],
            Json(json!({"ok": true})),
        )
            .into_response(),
    )
}

async fn logout(State(access): State<Arc<Access>>, headers: HeaderMap) -> Response {
    if !access.same_origin(&headers) {
        return denied(StatusCode::FORBIDDEN, "请求来源无效");
    }
    if let Some(token) = Access::token(&headers) {
        access.sessions.lock().remove(token);
    }
    private(
        (
            [(header::SET_COOKIE, access.cookie("", 0))],
            Json(json!({"ok": true})),
        )
            .into_response(),
    )
}

#[derive(Deserialize)]
struct Panel {
    env: String,
    panel: String,
}

async fn open_panel(
    State(access): State<Arc<Access>>,
    headers: HeaderMap,
    Query(panel): Query<Panel>,
) -> Response {
    if !access.authorized(&headers, None) {
        return Redirect::to("/delist/").into_response();
    }
    if !access.config.fr_envs.contains(&panel.env)
        || !matches!(panel.panel.as_str(), "viz" | "config")
    {
        return denied(StatusCode::FORBIDDEN, "面板未授权");
    }
    // A link from the risk board grants this environment for the current session.
    let from_board = headers
        .get(header::REFERER)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| reqwest::Url::parse(s).ok())
        .is_some_and(|url| {
            url.origin().ascii_serialization() == access.config.public_origin
                && matches!(url.path(), "/delist/" | "/delist/index.html")
        });
    if !from_board {
        return Redirect::to("/delist/").into_response();
    }
    let mut sessions = access.sessions.lock();
    let Some(session) = Access::token(&headers).and_then(|token| sessions.get_mut(token)) else {
        return Redirect::to("/delist/").into_response();
    };
    session.grants.insert(panel.env.clone());
    private(
        Redirect::to(&format!(
            "/fr/{}/{}",
            panel.env,
            if panel.panel == "config" {
                "config/"
            } else {
                ""
            }
        ))
        .into_response(),
    )
}

async fn verify(State(access): State<Arc<Access>>, headers: HeaderMap) -> Response {
    // These original-request headers are overwritten by the internal Nginx location.
    let uri = headers
        .get("x-original-uri")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let env = uri
        .split('?')
        .next()
        .unwrap_or("")
        .strip_prefix("/fr/")
        .and_then(|path| path.split('/').next());
    let method = headers
        .get("x-original-method")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    // The autonomous risk scan reads local snapshots through the same gateway.
    // Nginx overwrites the peer address, so forwarded/client headers cannot grant this.
    if method == "GET"
        && headers
            .get("x-original-remote-addr")
            .is_some_and(|h| h == "127.0.0.1")
        && env.is_some_and(|env| {
            access.config.fr_envs.contains(env) && uri == format!("/fr/{env}/snapshot")
        })
    {
        return private(StatusCode::NO_CONTENT.into_response());
    }
    if env.is_none() || !access.authorized(&headers, env) {
        return denied(StatusCode::UNAUTHORIZED, "请从 delist 打开面板");
    }
    let websocket = headers
        .get("x-original-upgrade")
        .is_some_and(|h| h.as_bytes().eq_ignore_ascii_case(b"websocket"));
    if ((!matches!(method, "GET" | "HEAD")) || websocket) && !access.same_origin(&headers) {
        return denied(StatusCode::FORBIDDEN, "请求来源无效");
    }
    private(StatusCode::NO_CONTENT.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    fn access() -> Arc<Access> {
        let salt = [7u8; 16];
        let mut hash = [0u8; 32];
        pbkdf2_hmac(
            b"test-password",
            &salt,
            600_000,
            MessageDigest::sha256(),
            &mut hash,
        )
        .unwrap();
        Arc::new(
            Access::new(Config {
                username: "tester".into(),
                password_salt: hex::encode(salt),
                password_hash: hex::encode(hash),
                password_iterations: 600_000,
                public_origin: "http://example.test:4191".into(),
                fr_envs: BTreeSet::from(["gate_fr_arb01".into()]),
            })
            .unwrap(),
        )
    }

    async fn call(
        app: &Router,
        path: &str,
        token: &str,
        method: &str,
        origin: &str,
        referer: &str,
        body: &str,
    ) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
        if !token.is_empty() {
            request = request.header(header::COOKIE, format!("{COOKIE}={token}"));
        }
        if !origin.is_empty() {
            request = request.header(header::ORIGIN, origin);
        }
        if !referer.is_empty() {
            request = request.header(header::REFERER, referer);
        }
        app.clone()
            .oneshot(
                request
                    .body(axum::body::Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn login_grant_csrf_and_logout() {
        let access = access();
        let app = protect(
            Router::new()
                .route("/", get(|| async { "private" }))
                .route("/accounts", get(|| async { "accounts" })),
            access.clone(),
        );
        assert_eq!(
            call(&app, "/accounts", "", "GET", "", "", "")
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let origin = "http://example.test:4191";
        assert_eq!(
            call(
                &app,
                "/login",
                "",
                "POST",
                "http://evil.test",
                "",
                "username=tester&password=test-password"
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            call(
                &app,
                "/login",
                "",
                "POST",
                origin,
                "",
                "username=tester&password=wrong"
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
        let response = call(
            &app,
            "/login",
            "",
            "POST",
            origin,
            "",
            "username=tester&password=test-password",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
        assert!(cookie.contains("HttpOnly; SameSite=Strict"));
        let token = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, format!("{COOKIE}={token}").parse().unwrap());
        headers.insert(
            "x-original-uri",
            "/fr/gate_fr_arb01/config/api/symbol-lists".parse().unwrap(),
        );
        headers.insert("x-original-method", "GET".parse().unwrap());
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(
                &app,
                "/open?env=gate_fr_arb01&panel=config",
                token,
                "GET",
                "",
                "http://evil.test/delist/",
                ""
            )
            .await
            .headers()[header::LOCATION],
            "/delist/"
        );
        let response = call(
            &app,
            "/open?env=gate_fr_arb01&panel=config",
            token,
            "GET",
            "",
            &format!("{origin}/delist/"),
            "",
        )
        .await;
        assert_eq!(
            response.headers()[header::LOCATION],
            "/fr/gate_fr_arb01/config/"
        );
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        headers.insert("x-original-method", "POST".parse().unwrap());
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        headers.insert(header::ORIGIN, origin.parse().unwrap());
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        headers.insert("x-original-method", "GET".parse().unwrap());
        headers.insert("x-original-upgrade", "websocket".parse().unwrap());
        headers.insert(header::ORIGIN, "http://evil.test".parse().unwrap());
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        headers.insert(header::ORIGIN, origin.parse().unwrap());
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        headers.insert(
            "x-original-uri",
            "/fr/binance_fr_arb01/config/".parse().unwrap(),
        );
        assert_eq!(
            verify(State(access.clone()), headers).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(&app, "/logout", token, "POST", origin, "", "")
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            call(&app, "/accounts", token, "GET", "", "", "")
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn only_loopback_snapshot_reads_bypass_browser_grants() {
        let access = access();
        let mut headers = HeaderMap::new();
        headers.insert("x-original-remote-addr", "127.0.0.1".parse().unwrap());
        headers.insert("x-original-method", "GET".parse().unwrap());
        headers.insert(
            "x-original-uri",
            "/fr/gate_fr_arb01/snapshot".parse().unwrap(),
        );
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        headers.insert("x-original-remote-addr", "203.0.113.1".parse().unwrap());
        assert_eq!(
            verify(State(access.clone()), headers.clone())
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        headers.insert("x-original-remote-addr", "127.0.0.1".parse().unwrap());
        headers.insert(
            "x-original-uri",
            "/fr/gate_fr_arb01/config/api/symbol-lists".parse().unwrap(),
        );
        assert_eq!(
            verify(State(access.clone()), headers).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn expired_sessions_and_login_throttle() {
        let access = access();
        let token = access.create_session().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, format!("{COOKIE}={token}").parse().unwrap());
        assert!(access.authorized(&headers, None));
        access.sessions.lock().get_mut(&token).unwrap().expires = Instant::now();
        assert!(!access.authorized(&headers, None));
        for _ in 0..10 {
            assert!(access.allow_attempt(&headers));
        }
        assert!(!access.allow_attempt(&headers));
        headers.insert(
            header::COOKIE,
            format!("{COOKIE}={token}; {COOKIE}={token}")
                .parse()
                .unwrap(),
        );
        assert!(Access::token(&headers).is_none());
    }
}
