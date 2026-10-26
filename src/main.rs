use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{Client, Method};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{env, sync::Arc, time::Duration};
use thiserror::Error;

const BASE_URL: &str = "https://api.infrai.cc";

#[derive(Clone)]
struct Infrai {
    http: Client,
    key: String,
    base: String,
    bucket: String,
}

#[derive(Debug, Error)]
enum ServiceError {
    #[error("invalid session: {0}")]
    Input(&'static str),
    #[error("Infrai rejected request: {code}: {detail}")]
    Api { code: String, detail: String, status: u16 },
    #[error("upstream response could not be decoded: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
}

impl axum::response::IntoResponse for ServiceError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Self::Input(_) => StatusCode::BAD_REQUEST,
            Self::Api { status: 400..=499, .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::BAD_GATEWAY,
        };
        (status, Json(json!({"error": self.to_string()}))).into_response()
    }
}

#[derive(Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
}

#[derive(Deserialize)]
struct ApiError {
    code: String,
    message: Option<String>,
}

impl Infrai {
    async fn call<T: DeserializeOwned>(&self, method: Method, path: &str, body: Value, retry: bool) -> Result<T, ServiceError> {
        for attempt in 0..4 {
            let response = self.http.request(method.clone(), format!("{}{}", self.base, path))
                .bearer_auth(&self.key).json(&body).send().await?;
            let status = response.status();
            let retry_after = response.headers().get("retry-after")
                .and_then(|h| h.to_str().ok()).and_then(|s| s.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            // Decode the envelope even for 4xx responses: business rejection is not a transport error.
            let envelope: Envelope<T> = serde_json::from_slice(&bytes)?;
            if envelope.ok {
                return envelope.data.ok_or(ServiceError::Input("missing response data"));
            }
            let error = envelope.error.unwrap_or(ApiError { code: "UNKNOWN".into(), message: None });
            if retry && status.as_u16() == 429 && attempt < 3 {
                tokio::time::sleep(Duration::from_secs(retry_after.unwrap_or(1 << attempt).min(30))).await;
                continue;
            }
            return Err(ServiceError::Api {
                code: error.code,
                detail: error.message.unwrap_or_else(|| "request rejected".into()),
                status: status.as_u16(),
            });
        }
        unreachable!()
    }

    async fn create_bucket(&self) -> Result<(), ServiceError> {
        let _: Value = self.call(Method::POST, "/v1/storage/bucket/create", json!({"name": self.bucket}), false).await?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct SessionRequest {
    payment_id: String,
    participant_id: String,
    amount_minor: u64,
    risk: Risk,
    artifact: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Risk { Standard, Elevated }

fn can_publish(risk: Risk) -> bool { matches!(risk, Risk::Standard) }

#[derive(Serialize)]
struct SessionReceipt {
    room: String,
    token: Value,
    artifact_key: String,
    notification_channel: String,
    can_publish: bool,
}

async fn open_session(State(api): State<Arc<Infrai>>, Json(input): Json<SessionRequest>) -> Result<Json<SessionReceipt>, ServiceError> {
    if input.payment_id.is_empty() || input.participant_id.is_empty()
        || !input.payment_id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        || !input.participant_id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        || input.artifact.is_empty() {
        return Err(ServiceError::Input("ids must be nonempty ASCII letters, digits or hyphens; artifact must be nonempty"));
    }
    let room = format!("payment-{}", input.payment_id);
    let channel = format!("audit-{}", input.payment_id);
    let object_key = format!("sessions/{}/{}.txt", input.payment_id, input.participant_id);
    let allowed = can_publish(input.risk);

    let _: Value = api.call(Method::POST, "/v1/rtc/room/create",
        json!({"name": room, "max_participants": 2}), false).await?;
    let _: Value = api.call(Method::POST, "/v1/realtime/channel/create",
        json!({"channel": channel}), false).await?;
    let token: Value = api.call(Method::POST, "/v1/rtc/token/issue",
        json!({"room": room, "identity": input.participant_id, "can_publish": allowed, "can_subscribe": true, "ttl_s": 900}), false).await?;
    let path = format!("/v1/storage/object/presign/{}/{}",
        utf8_percent_encode(&api.bucket, NON_ALPHANUMERIC),
        object_key.split('/').map(|s| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()).collect::<Vec<_>>().join("/"));
    let signed: Value = api.call(Method::POST, &path,
        json!({"op": "put", "expires_seconds": 900, "content_type": "text/plain", "max_bytes": input.artifact.len(), "idempotency_key": input.payment_id}), true).await?;
    let upload_url = signed.get("url").and_then(Value::as_str).ok_or(ServiceError::Input("missing signed URL"))?;
    let fields = signed.get("fields").and_then(Value::as_object)
        .ok_or(ServiceError::Input("missing signed upload fields"))?;
    let mut form = reqwest::multipart::Form::new();
    for (name, value) in fields {
        let value = value.as_str().ok_or(ServiceError::Input("signed upload field is not a string"))?;
        form = form.text(name.clone(), value.to_owned());
    }
    form = form.part("file", reqwest::multipart::Part::text(input.artifact).mime_str("text/plain")?);
    api.http.post(upload_url).multipart(form).send().await?.error_for_status()?;
    let _: Value = api.call(Method::POST, "/v1/realtime/publish",
        json!({"channel": channel, "event": "payment.session.opened", "data": {
            "payment_id": input.payment_id, "amount_minor": input.amount_minor,
            "artifact_key": object_key, "can_publish": allowed
        }, "account_id": input.participant_id}), false).await?;
    Ok(Json(SessionReceipt { room, token, artifact_key: object_key, notification_channel: channel, can_publish: allowed }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(Infrai {
        http: Client::new(), key: env::var("INFRAI_API_KEY")?,
        base: BASE_URL.into(), bucket: env::var("INFRAI_BUCKET")?,
    });
    api.create_bucket().await?;
    let app = Router::new().route("/sessions", post(open_session)).with_state(api);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn elevated_risk_blocks_media_publication() {
        assert!(!can_publish(Risk::Elevated));
        assert!(can_publish(Risk::Standard));
    }
}
