use super::{Actor, ApiError, error};
use axum::{
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Url;
use ring::signature::{RSA_PKCS1_2048_8192_SHA256, RsaPublicKeyComponents};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const KEY_TTL: Duration = Duration::from_secs(300);
const MIN_REFRESH: Duration = Duration::from_secs(10);
const MAX_JWKS: usize = 64 * 1024;

#[derive(Clone)]
pub(in crate::server) struct Access(Arc<Config>);

struct Config {
    issuer: String,
    audience: String,
    origin: String,
    certs: Url,
    http: reqwest::Client,
    keys: Mutex<Keys>,
}

#[derive(Default)]
struct Keys {
    last_refresh: Option<Instant>,
    valid_until: Option<Instant>,
    rsa: HashMap<String, Arc<RsaPublicKeyComponents<Vec<u8>>>>,
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
    kid: String,
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    aud: Vec<String>,
    exp: u64,
    nbf: Option<u64>,
    #[serde(rename = "type")]
    kind: String,
    email: String,
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    alg: Option<String>,
    n: Option<String>,
    e: Option<String>,
}

impl Access {
    pub(in crate::server) fn from_env() -> crate::Result<Self> {
        let issuer = https_origin("PRIME_ACCESS_ISSUER")?;
        let origin = https_origin("PRIME_ADMIN_ORIGIN")?;
        let audience = super::secret("PRIME_ACCESS_AUDIENCE")?;
        Ok(Self(Arc::new(Config {
            certs: Url::parse(&format!("{issuer}/cdn-cgi/access/certs"))?,
            issuer,
            audience,
            origin,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            keys: Mutex::new(Keys::default()),
        })))
    }

    async fn key(&self, kid: &str) -> Option<Arc<RsaPublicKeyComponents<Vec<u8>>>> {
        let mut cache = self.0.keys.lock().await;
        let now = Instant::now();
        if cache.valid_until.is_some_and(|until| until > now) {
            if let Some(key) = cache.rsa.get(kid) {
                return Some(key.clone());
            }
        }
        if cache
            .last_refresh
            .is_some_and(|last| now.duration_since(last) < MIN_REFRESH)
        {
            return None;
        }
        cache.last_refresh = Some(now);
        let mut response = self
            .0
            .http
            .get(self.0.certs.clone())
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if chunk.len() > MAX_JWKS.saturating_sub(body.len()) {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        let keys: Jwks = serde_json::from_slice(&body).ok()?;
        cache.rsa = keys
            .keys
            .into_iter()
            .filter_map(|jwk| {
                if jwk.kty != "RSA" || jwk.alg.as_deref().is_some_and(|alg| alg != "RS256") {
                    return None;
                }
                let n = URL_SAFE_NO_PAD.decode(jwk.n?).ok()?;
                let e = URL_SAFE_NO_PAD.decode(jwk.e?).ok()?;
                Some((jwk.kid, Arc::new(RsaPublicKeyComponents { n, e })))
            })
            .collect();
        cache.valid_until = Some(now + KEY_TTL);
        cache.rsa.get(kid).cloned()
    }

    async fn verify(&self, token: &str) -> Option<Actor> {
        if token.len() > 8192 {
            return None;
        }
        let (message, signature) = token.rsplit_once('.')?;
        let (header, payload) = message.split_once('.')?;
        let header: JwtHeader =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).ok()?).ok()?;
        if header.alg != "RS256" || header.kid.is_empty() || header.kid.len() > 128 {
            return None;
        }
        let claims: Claims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
        let now = super::now();
        if claims.iss != self.0.issuer
            || !claims.aud.iter().any(|aud| aud == &self.0.audience)
            || claims.kind != "app"
            || claims.exp <= now
            || claims.nbf.is_some_and(|nbf| nbf > now)
            || claims.email.is_empty()
            || claims.email.len() > 254
        {
            return None;
        }
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        self.key(&header.kid)
            .await?
            .verify(&RSA_PKCS1_2048_8192_SHA256, message.as_bytes(), &signature)
            .ok()?;
        Some(Actor(claims.email))
    }
}

fn https_origin(name: &str) -> crate::Result<String> {
    let value = std::env::var(name)?;
    let url = Url::parse(&value)?;
    if url.scheme() != "https" || url.origin().ascii_serialization() != value {
        return Err(format!("{name} must contain a bare HTTPS origin").into());
    }
    Ok(value)
}

pub(super) async fn authorize(
    State(access): State<Access>,
    mut request: Request,
    next: Next,
) -> Response {
    let assertion = request
        .headers()
        .get("cf-access-jwt-assertion")
        .and_then(|value| value.to_str().ok());
    let actor = match assertion {
        Some(token) => access.verify(token).await,
        None => None,
    };
    let Some(actor) = actor else {
        return error(
            StatusCode::UNAUTHORIZED,
            "Cloudflare Access authentication required",
        )
        .into_response();
    };
    if request.method() != Method::GET
        && request.method() != Method::HEAD
        && request
            .headers()
            .get(header::ORIGIN)
            .and_then(|value| value.to_str().ok())
            != Some(access.0.origin.as_str())
    {
        return ApiError(StatusCode::FORBIDDEN, "Cross-origin write".into()).into_response();
    }
    request.extensions_mut().insert(actor);
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        axum::http::HeaderValue::from_static(
            "default-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'",
        ),
    );
    response
}
