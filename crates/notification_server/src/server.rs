use crate::model::{
    AcceptedResponse, ErrorResponse, HealthResponse, NotifyRequest, QueuedNotification,
};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub struct DeliveryStats {
    pub enqueued: AtomicU64,
    pub delivered: AtomicU64,
    pub failed: AtomicU64,
    pub rejected: AtomicU64,
}

#[derive(Clone)]
pub struct AppState {
    sender: mpsc::Sender<QueuedNotification>,
    stats: Arc<DeliveryStats>,
    queue_capacity: usize,
    max_message_chars: usize,
    api_token: Option<String>,
    dry_run: bool,
}

pub fn build_router(
    sender: mpsc::Sender<QueuedNotification>,
    stats: Arc<DeliveryStats>,
    queue_capacity: usize,
    max_message_chars: usize,
    api_token: Option<String>,
    dry_run: bool,
) -> Router {
    let state = AppState {
        sender,
        stats,
        queue_capacity,
        max_message_chars,
        api_token,
        dry_run,
    };
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/notify", post(notify))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        queue_capacity: state.queue_capacity,
        queue_available: state.sender.capacity(),
        enqueued: state.stats.enqueued.load(Ordering::Relaxed),
        delivered: state.stats.delivered.load(Ordering::Relaxed),
        failed: state.stats.failed.load(Ordering::Relaxed),
        rejected: state.stats.rejected.load(Ordering::Relaxed),
        dry_run: state.dry_run,
    })
}

async fn notify(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<NotifyRequest>,
) -> Response {
    if !authorized(&headers, state.api_token.as_deref()) {
        return error_response(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let request = match request.validate_and_normalize(state.max_message_chars) {
        Ok(request) => request,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err.to_string()),
    };
    let notification = QueuedNotification::new(request);
    if let Err(err) = notification.validate_rendered_chars(state.max_message_chars) {
        state.stats.rejected.fetch_add(1, Ordering::Relaxed);
        return error_response(StatusCode::BAD_REQUEST, err.to_string());
    }
    let event_id = notification.id.to_string();
    match state.sender.try_send(notification) {
        Ok(()) => {
            state.stats.enqueued.fetch_add(1, Ordering::Relaxed);
            (
                StatusCode::ACCEPTED,
                Json(AcceptedResponse {
                    accepted: true,
                    event_id,
                }),
            )
                .into_response()
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            state.stats.rejected.fetch_add(1, Ordering::Relaxed);
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "notification queue is full",
            )
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            state.stats.rejected.fetch_add(1, Ordering::Relaxed);
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "notification worker is unavailable",
            )
        }
    }
}

fn authorized(headers: &HeaderMap, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    let bearer = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let custom = headers
        .get("x-notification-token")
        .and_then(|value| value.to_str().ok());
    bearer == Some(expected) || custom == Some(expected)
}

fn error_response(status: StatusCode, error: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn authentication_supports_bearer_and_custom_header() {
        let mut headers = HeaderMap::new();
        assert!(authorized(&headers, None));
        assert!(!authorized(&headers, Some("secret")));

        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        assert!(authorized(&headers, Some("secret")));
        headers.remove(AUTHORIZATION);
        headers.insert("x-notification-token", HeaderValue::from_static("secret"));
        assert!(authorized(&headers, Some("secret")));
    }
}
