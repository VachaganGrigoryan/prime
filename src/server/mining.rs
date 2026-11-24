mod access;

pub(super) use access::Access;

use super::{Api, ApiError, App, PUBLIC_DB_REQUESTS, internal, now, page, secret, summary};
use axum::{
    Form, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use prime_finder::{
    archive::{
        Lease, MiningAdminDeviceDetail, MiningAdminDevices, MiningAdminOverview, MiningBenchmark,
        MiningWorker, Span,
    },
    block_codec::{self, BLOCK_WIDTH},
    engine::Engine,
    evidence,
    mining_protocol::{BENCHMARK_COUNT, BENCHMARK_DIGEST, BENCHMARK_VERSION},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const PREFIX: &str = "/api/v1/mining";
const BOOTSTRAP_LIMIT: usize = 8192;
const ORDERED_PRIMES: &str = "primes-v1";

pub(super) struct PublicConfig {
    site_key: String,
    secret: String,
    hostname: String,
    ui_origin: HeaderValue,
    serve_dashboard: bool,
    http: reqwest::Client,
}

impl PublicConfig {
    pub(super) fn from_env() -> crate::Result<Self> {
        let site_key = secret("PRIME_TURNSTILE_SITE_KEY")?;
        if !site_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            return Err("Invalid Turnstile site key".into());
        }
        let hostname = std::env::var("PRIME_TURNSTILE_HOSTNAME")?;
        if hostname.is_empty()
            || !hostname
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-.".contains(&b))
        {
            return Err("Invalid Turnstile hostname".into());
        }
        let ui_origin = std::env::var("PRIME_UI_ORIGIN")
            .unwrap_or_else(|_| "https://prime.notallow.cc".into())
            .parse()?;
        Ok(Self {
            site_key,
            secret: secret("PRIME_TURNSTILE_SECRET")?,
            hostname,
            ui_origin,
            serve_dashboard: std::env::var("PRIME_SERVE_DASHBOARD").as_deref() == Ok("1"),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(8))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
}

#[derive(Clone)]
struct PublicState {
    app: App,
    config: Arc<PublicConfig>,
    enrollment: Arc<Semaphore>,
    uploads: Arc<Semaphore>,
    verification: Arc<Semaphore>,
    engine: Arc<Engine>,
}

pub(super) fn public_router(mut app: App, config: PublicConfig) -> Router {
    let origin = config.ui_origin.clone();
    let serve_dashboard = config.serve_dashboard;
    app.public_db = Some(Arc::new(Semaphore::new(PUBLIC_DB_REQUESTS)));
    let reads = Router::new()
        .route("/api/v1/primes", get(page))
        .route("/api/v1/summary", get(summary))
        .route(&format!("{PREFIX}/progress"), get(progress))
        .with_state(app.clone())
        .layer(middleware::from_fn_with_state(origin, public_read));
    let state = PublicState {
        app,
        config: Arc::new(config),
        enrollment: Arc::new(Semaphore::new(4)),
        uploads: Arc::new(Semaphore::new(4)),
        verification: Arc::new(Semaphore::new(1)),
        engine: Arc::new(Engine::new(100_000_000)),
    };
    let submissions = Router::new()
        .route(&format!("{PREFIX}/submit"), post(submit))
        .layer(DefaultBodyLimit::max(
            evidence::MAX_BYTES.div_ceil(3) * 4 + 1024,
        ))
        .with_state(state.clone())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            admit_submission,
        ));
    let workers = Router::new()
        .route("/join", get(join).post(enroll))
        .route(&format!("{PREFIX}/exchange"), post(exchange))
        .route(&format!("{PREFIX}/worker"), get(worker))
        .route(&format!("{PREFIX}/benchmark"), post(benchmark))
        .route(&format!("{PREFIX}/claim"), post(claim))
        .route(&format!("{PREFIX}/heartbeat"), post(heartbeat))
        .layer(DefaultBodyLimit::max(BOOTSTRAP_LIMIT))
        .with_state(state)
        .merge(submissions);
    let api = Router::new()
        .route("/healthz", get(|| async { "ready" }))
        .merge(reads)
        .merge(workers.layer(middleware::from_fn(private_response)));
    if serve_dashboard {
        api.merge(prime_finder::client_assets::router())
    } else {
        api
    }
}

async fn admit_submission(
    State(state): State<PublicState>,
    request: Request,
    next: Next,
) -> Response {
    let token = match credential(request.headers()) {
        Ok(token) => token,
        Err(error) => return error.into_response(),
    };
    let Ok(_permit) = state.uploads.try_acquire() else {
        return busy().into_response();
    };
    match state
        .app
        .db(move |db| db.mining_worker(&token)?.ok_or_else(unauthorized))
        .await
    {
        Ok(worker) if worker.state == "approved" => next.run(request).await,
        Ok(_) => error(StatusCode::CONFLICT, "The worker is not approved").into_response(),
        Err(error) => error.into_response(),
    }
}

async fn private_response(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert("x-robots-tag", HeaderValue::from_static("noindex"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn public_read(State(allowed): State<HeaderValue>, request: Request, next: Next) -> Response {
    let allow = request.headers().get(header::ORIGIN) == Some(&allowed);
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Origin"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if allow {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, allowed);
    }
    response
}

fn storage_limited(app: &App) -> bool {
    app.gap_space(app.shared.config.reserve_bytes.saturating_add(1))
        .is_err()
}

async fn progress(State(app): State<App>) -> Api<Json<Value>> {
    let storage_limited = storage_limited(&app);
    let stats = app.db(|db| Ok(db.mining_status()?)).await?;
    let mut stats = serde_json::to_value(stats).map_err(internal)?;
    if storage_limited && stats["state"] != "exhausted" {
        stats["state"] = json!("storage-limited");
    }
    Ok(Json(stats))
}

async fn join(State(state): State<PublicState>) -> Html<String> {
    Html(format!(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Join Prime</title><h1>Join the Prime worker pool</h1>
<form method="post" action="/join">
<label>Device name <input name="name" required maxlength="80" pattern="[A-Za-z0-9._-]+"></label>
<div class="cf-turnstile" data-sitekey="{}" data-action="prime-enroll"></div>
<p><button>Get enrollment code</button></p></form>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script></html>"#,
        state.config.site_key
    ))
}

#[derive(Deserialize)]
struct Enrollment {
    name: String,
    #[serde(rename = "cf-turnstile-response")]
    challenge: String,
}

#[derive(Deserialize)]
struct SiteVerify {
    success: bool,
    hostname: Option<String>,
    action: Option<String>,
}

fn hash(bytes: impl AsRef<[u8]>) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn random_secret() -> Api<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(internal)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn valid_secret(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}

fn error(status: StatusCode, message: &str) -> ApiError {
    ApiError(status, message.into())
}

fn busy() -> ApiError {
    error(StatusCode::TOO_MANY_REQUESTS, "Try again shortly")
}

fn credential(headers: &HeaderMap) -> Api<[u8; 32]> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| valid_secret(value))
        .ok_or_else(unauthorized)?;
    Ok(hash(token))
}

fn unauthorized() -> ApiError {
    error(
        StatusCode::UNAUTHORIZED,
        "A valid Prime device credential is required",
    )
}

async fn enroll(
    State(state): State<PublicState>,
    headers: HeaderMap,
    Form(input): Form<Enrollment>,
) -> Api<Html<String>> {
    if input.name.is_empty()
        || input.name.len() > 80
        || !input
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        || input.challenge.is_empty()
        || input.challenge.len() > 2048
    {
        return Err(error(StatusCode::BAD_REQUEST, "Invalid enrollment form"));
    }
    let _permit = state.enrollment.try_acquire().map_err(|_| busy())?;
    let address = headers
        .get("cf-connecting-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<IpAddr>().ok());
    let source = hash(address.map_or_else(String::new, |ip| ip.to_string()));
    let proof = hash(&input.challenge);
    let answer = state
        .config
        .http
        .post("https://challenges.cloudflare.com/turnstile/v0/siteverify")
        .json(&json!({"secret":state.config.secret,"response":input.challenge,"remoteip":address}))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Enrollment verification unavailable",
            )
        })?
        .json::<SiteVerify>()
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Enrollment verification unavailable",
            )
        })?;
    if !answer.success
        || answer.hostname.as_deref() != Some(&state.config.hostname)
        || answer.action.as_deref() != Some("prime-enroll")
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Challenge failed. Return to the join page and try again.",
        ));
    }
    let code = random_secret()?;
    let code_hash = hash(&code);
    let expires_at = now() as i64 + 600;
    state
        .app
        .db(move |db| Ok(db.mining_enroll(&input.name, &code_hash, &proof, &source, expires_at)?))
        .await?;
    Ok(Html(format!(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<title>Prime enrollment code</title><h1>Prime enrollment code</h1>
<p>Paste this one-time code into your running Prime worker. It expires in 10 minutes.</p>
<label>Enrollment code <input value="{code}" size="50" readonly></label></html>"#
    )))
}

#[derive(Deserialize)]
struct Exchange {
    code: String,
    token: String,
}

async fn exchange(
    State(state): State<PublicState>,
    Json(input): Json<Exchange>,
) -> Api<Json<Value>> {
    if !valid_secret(&input.code) || !valid_secret(&input.token) {
        return Err(unauthorized());
    }
    let token = input.token;
    let token_hash = hash(&token);
    let code_hash = hash(input.code);
    let worker = state
        .app
        .db(move |db| {
            db.mining_exchange(&code_hash, &token_hash)?
                .ok_or_else(unauthorized)
        })
        .await?;
    Ok(Json(json!({"token":token,"worker":worker})))
}

async fn worker(State(state): State<PublicState>, headers: HeaderMap) -> Api<Json<MiningWorker>> {
    let token = credential(&headers)?;
    state
        .app
        .db(move |db| Ok(Json(db.mining_worker(&token)?.ok_or_else(unauthorized)?)))
        .await
}

#[derive(Deserialize)]
struct Benchmark {
    version: u32,
    count: u64,
    digest: String,
    elapsed_ms: u64,
    platform: String,
    client_version: String,
}

async fn benchmark(
    State(state): State<PublicState>,
    headers: HeaderMap,
    Json(input): Json<Benchmark>,
) -> Api<Json<MiningWorker>> {
    let token = credential(&headers)?;
    if input.version != BENCHMARK_VERSION
        || input.digest.len() != 64
        || !input.digest.bytes().all(|b| b.is_ascii_hexdigit())
        || !(1..=3_600_000).contains(&input.elapsed_ms)
        || input.platform.is_empty()
        || input.platform.len() > 128
        || input.client_version.is_empty()
        || input.client_version.len() > 64
    {
        return Err(error(StatusCode::BAD_REQUEST, "Invalid benchmark result"));
    }
    let report = MiningBenchmark {
        version: input.version.to_string(),
        count: input.count,
        passed: input.count == BENCHMARK_COUNT && input.digest == BENCHMARK_DIGEST,
        digest: input.digest,
        elapsed_ms: input.elapsed_ms,
        platform: input.platform,
        client_version: input.client_version,
    };
    state
        .app
        .db(move |db| Ok(Json(db.mining_benchmark(&token, report)?)))
        .await
}

async fn claim(State(state): State<PublicState>, headers: HeaderMap) -> Api<Json<Option<Lease>>> {
    let token = credential(&headers)?;
    let storage = state.app.clone();
    state
        .app
        .db(move |db| {
            let worker = db.mining_worker(&token)?.ok_or_else(unauthorized)?;
            if worker.state != "approved" {
                return Ok(Json(None));
            }
            if !db.mining_worker_paused(worker.id)? {
                storage.gap_space(storage.shared.config.reserve_bytes.saturating_add(1))?;
            }
            Ok(Json(db.mining_claim(&token)?))
        })
        .await
}

#[derive(Deserialize)]
struct Heartbeat {
    lease: String,
}

async fn heartbeat(
    State(state): State<PublicState>,
    headers: HeaderMap,
    Json(input): Json<Heartbeat>,
) -> Api<Json<Value>> {
    let token = credential(&headers)?;
    state
        .app
        .db(move |db| Ok(db.mining_renew(&token, &input.lease)?))
        .await?;
    Ok(Json(json!({})))
}

#[derive(Deserialize)]
struct Submission {
    lease: String,
    kind: String,
    payload: String,
    count: Option<u64>,
}

fn verify_result(engine: &Engine, span: Span, input: &Submission) -> Option<Vec<u64>> {
    let bytes = STANDARD.decode(&input.payload).ok()?;
    match input.kind.as_str() {
        evidence::KIND
            if input.count.is_none()
                && span.lo >= evidence::MIN_LO
                && span.lo.is_multiple_of(BLOCK_WIDTH)
                && span.hi - span.lo == BLOCK_WIDTH - 1 =>
        {
            evidence::verify(span.lo, span.hi, &bytes).ok()
        }
        ORDERED_PRIMES if bytes.len() <= BLOCK_WIDTH as usize + 16 => {
            let supplied = block_codec::decode(span.lo, span.hi, input.count?, &bytes).ok()?;
            let exact = engine.enumerate(span.lo, span.hi);
            (supplied == exact).then_some(exact)
        }
        _ => None,
    }
}

async fn submit(
    State(state): State<PublicState>,
    headers: HeaderMap,
    Json(input): Json<Submission>,
) -> Api<Json<Value>> {
    let token = credential(&headers)?;
    if input.lease.len() > 128 || input.kind.len() > 32 {
        return Err(error(StatusCode::BAD_REQUEST, "Invalid block submission"));
    }
    let lease = input.lease.clone();
    let assigned = state
        .app
        .db(move |db| Ok(db.mining_submission(&token, &lease)?))
        .await?;
    let mut digest = Sha256::new();
    digest.update(input.kind.as_bytes());
    digest.update([0]);
    digest.update([u8::from(input.count.is_some())]);
    if let Some(count) = input.count {
        digest.update(count.to_be_bytes());
    }
    digest.update(input.payload.as_bytes());
    let digest = format!("{:x}", digest.finalize());
    if let Some(committed) = assigned.committed_digest {
        return if digest == committed {
            Ok(Json(json!({"accepted":true})))
        } else {
            Err(error(
                StatusCode::CONFLICT,
                "Different result already committed",
            ))
        };
    }
    let permit = state.verification.try_acquire_owned().map_err(|_| busy())?;
    let engine = state.engine;
    let lease = input.lease.clone();
    let span = assigned.span;
    let (primes, milliseconds, permit) = tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        let primes = verify_result(&engine, span, &input);
        (
            primes,
            start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            permit,
        )
    })
    .await
    .map_err(internal)?;
    let storage = state.app.clone();
    state
        .app
        .db(move |db| {
            let _permit = permit;
            if let Some(primes) = primes {
                let result = db.mining_commit(
                    &token,
                    &lease,
                    span,
                    &primes,
                    "prime-exact-v1",
                    &digest,
                    milliseconds,
                    |required| {
                        storage
                            .gap_space(storage.shared.config.reserve_bytes.saturating_add(required))
                    },
                );
                storage.gap_write(result)
            } else {
                db.mining_reject_result(&token, &lease, milliseconds)?;
                Err(error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Block failed exact verification",
                ))
            }
        })
        .await?;
    Ok(Json(json!({"accepted":true})))
}

pub(super) fn private_operator_router(app: App) -> Router {
    admin_api(app)
        .layer(axum::Extension(Actor("private-admin".into())))
        .layer(middleware::from_fn(private_response))
}

async fn admin_page() -> Html<&'static str> {
    Html(include_str!("admin/index.html"))
}

async fn admin_device_page(Path(_id): Path<i64>) -> Html<&'static str> {
    admin_page().await
}

pub(super) fn operator_router(app: App, access: Access) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ready" }))
        .merge(
            prime_finder::client_assets::icon_router()
                .route("/", get(admin_page))
                .route("/devices/{id}", get(admin_device_page))
                .route(
                    "/admin.js",
                    get(|| async {
                        (
                            [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                            include_str!("admin/admin.js"),
                        )
                    }),
                )
                .route(
                    "/admin.css",
                    get(|| async {
                        (
                            [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                            include_str!("admin/admin.css"),
                        )
                    }),
                )
                .merge(admin_api(app))
                .layer(middleware::from_fn_with_state(access, access::authorize))
                .layer(middleware::from_fn(private_response)),
        )
}

#[derive(Clone)]
struct Actor(String);

fn admin_api(app: App) -> Router {
    Router::new()
        .route(&format!("{PREFIX}/progress"), get(progress))
        .route(&format!("{PREFIX}/admin/workers"), get(workers))
        .route(&format!("{PREFIX}/admin/overview"), get(admin_overview))
        .route(
            &format!("{PREFIX}/admin/resources/history"),
            get(resource_history),
        )
        .route(&format!("{PREFIX}/admin/devices"), get(admin_devices))
        .route(&format!("{PREFIX}/admin/devices/{{id}}"), get(admin_device))
        .route(
            &format!("{PREFIX}/admin/workers/{{id}}/{{action}}"),
            post(decide),
        )
        .route(&format!("{PREFIX}/admin/pause"), post(pause))
        .layer(DefaultBodyLimit::max(BOOTSTRAP_LIMIT))
        .with_state(app)
}

async fn workers(State(app): State<App>) -> Api<Json<Vec<MiningWorker>>> {
    app.db(|db| Ok(Json(db.mining_workers()?))).await
}

#[derive(Serialize)]
struct AdminOverview {
    #[serde(flatten)]
    mining: MiningAdminOverview,
    resources: Option<super::resources::Sample>,
}

async fn resource_history(State(app): State<App>) -> Api<Json<Value>> {
    let dir = app.shared.config.archive_dir.clone();
    let samples = tokio::task::spawn_blocking(move || super::resources::history(&dir))
        .await
        .map_err(internal)?
        .map_err(internal)?;
    Ok(Json(json!({"server_time": now(), "samples": samples})))
}

async fn admin_overview(State(app): State<App>) -> Api<Json<AdminOverview>> {
    let storage_limited = storage_limited(&app);
    let mut overview = app.db(|db| Ok(db.mining_admin_overview()?)).await?;
    if storage_limited && overview.state != "exhausted" {
        overview.state = "storage-limited".into();
    }
    Ok(Json(AdminOverview {
        mining: overview,
        resources: app.shared.resources.latest(),
    }))
}

#[derive(Deserialize)]
struct DeviceQuery {
    #[serde(default)]
    q: String,
    #[serde(default)]
    approval: String,
    #[serde(default)]
    work: String,
    page: Option<u32>,
}

async fn admin_devices(
    State(app): State<App>,
    Query(query): Query<DeviceQuery>,
) -> Api<Json<MiningAdminDevices>> {
    app.db(move |db| {
        Ok(Json(db.mining_admin_devices(
            query.q.trim(),
            &query.approval,
            &query.work,
            query.page.unwrap_or(1),
        )?))
    })
    .await
}

async fn admin_device(
    State(app): State<App>,
    Path(id): Path<i64>,
) -> Api<Json<MiningAdminDeviceDetail>> {
    app.db(move |db| Ok(Json(db.mining_admin_device(id)?)))
        .await
}

async fn decide(
    State(app): State<App>,
    axum::Extension(actor): axum::Extension<Actor>,
    Path((id, action)): Path<(i64, String)>,
    _body: Json<Value>,
) -> Api<Json<MiningWorker>> {
    if !matches!(
        action.as_str(),
        "approve" | "reject" | "pause" | "resume" | "revoke"
    ) {
        return Err(error(StatusCode::NOT_FOUND, "Unknown worker action"));
    }
    app.db(move |db| Ok(Json(db.mining_decide(id, &action, &actor.0)?)))
        .await
}

#[derive(Deserialize)]
struct Pause {
    paused: bool,
}

async fn pause(State(app): State<App>, Json(input): Json<Pause>) -> Api<Json<Value>> {
    if !input.paused {
        app.gap_space(app.shared.config.reserve_bytes.saturating_add(1))?;
    }
    app.db(move |db| Ok(db.mining_set_paused(input.paused)?))
        .await?;
    progress(State(app)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::{TestDir, test_app};
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    async fn call(
        router: &Router,
        method: &str,
        path: &str,
        token: &str,
        data: Value,
    ) -> (StatusCode, Value) {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(data.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    fn test_config(serve_dashboard: bool) -> PublicConfig {
        PublicConfig {
            site_key: "test".into(),
            secret: "test".into(),
            hostname: "localhost".into(),
            ui_origin: HeaderValue::from_static("https://prime.notallow.cc"),
            serve_dashboard,
            http: reqwest::Client::new(),
        }
    }

    #[tokio::test]
    async fn dashboard_is_opt_in_on_public_mining_api() {
        let dir = TestDir::new();
        let app = test_app(&dir, 1_000_000_000, 1_000_000);
        for (serve_dashboard, static_status) in
            [(false, StatusCode::NOT_FOUND), (true, StatusCode::OK)]
        {
            let router = public_router(app.clone(), test_config(serve_dashboard));
            for path in [
                "/",
                "/research.html",
                "/docs.html",
                "/style.css",
                "/app.js",
                "/estimates.html",
                "/estimates.css",
                "/estimates.js",
                "/prime-experiment.json",
                "/prime-prediction.json",
                "/prime-score.json",
                "/favicon.ico",
                "/favicon-96.png",
                "/favicon.svg",
                "/apple-touch-icon.png",
                "/social-card.png",
                "/robots.txt",
                "/sitemap.xml",
            ] {
                let (status, _) = call(&router, "GET", path, "", Value::Null).await;
                assert_eq!(status, static_status, "{path}");
            }
            for path in ["/healthz", "/join", "/api/v1/mining/progress"] {
                let (status, _) = call(&router, "GET", path, "", Value::Null).await;
                assert_eq!(status, StatusCode::OK, "{path}");
            }
            let (status, _) = call(
                &router,
                "GET",
                "/api/v1/mining/admin/overview",
                "",
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn enrollment_approval_exact_submission_replay_and_revocation() {
        let dir = TestDir::new();
        let app = test_app(&dir, 1_000_000_000, 1_000_000);
        let router = public_router(app.clone(), test_config(false));
        let code = random_secret().unwrap();
        let code_hash = hash(&code);
        app.db(move |db| {
            db.mining_enroll(
                "test-mac",
                &code_hash,
                &[1; 32],
                &[2; 32],
                now() as i64 + 60,
            )?;
            db.mining_set_paused(false)?;
            Ok(())
        })
        .await
        .unwrap();

        let exchange = format!("{PREFIX}/exchange");
        let proposed_token = random_secret().unwrap();
        let (status, issued) = call(
            &router,
            "POST",
            &exchange,
            "",
            json!({"code":code,"token":proposed_token}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{issued}");
        assert_eq!(
            call(
                &router,
                "POST",
                &exchange,
                "",
                json!({"code":code,"token":random_secret().unwrap()})
            )
            .await
            .0,
            StatusCode::UNAUTHORIZED
        );
        let (_, replay) = call(
            &router,
            "POST",
            &exchange,
            "",
            json!({"code":code,"token":proposed_token}),
        )
        .await;
        assert_eq!(replay, issued);
        let token = issued["token"].as_str().unwrap();
        let id = issued["worker"]["id"].as_i64().unwrap();
        let claim_url = format!("{PREFIX}/claim");
        assert_eq!(
            call(&router, "POST", &claim_url, token, json!({})).await,
            (StatusCode::OK, Value::Null)
        );
        assert!(
            app.db(move |db| Ok(db.mining_decide(id, "approve", "test")?))
                .await
                .is_err()
        );

        let (status, report) = call(
            &router,
            "POST",
            &format!("{PREFIX}/benchmark"),
            token,
            json!({"version": BENCHMARK_VERSION, "count": BENCHMARK_COUNT,
                   "digest": BENCHMARK_DIGEST, "elapsed_ms":25, "platform":"test",
                   "client_version":"test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{report}");
        assert_eq!(report["benchmark"]["passed"], true);
        app.db(move |db| Ok(db.mining_decide(id, "approve", "test")?))
            .await
            .unwrap();

        let (status, lease) = call(&router, "POST", &claim_url, token, json!({})).await;
        assert_eq!(status, StatusCode::OK, "{lease}");
        let lease: Lease = serde_json::from_value(lease).unwrap();
        let span = lease.blocks[0];
        let submit_url = format!("{PREFIX}/submit");
        let payload = block_codec::encode(span.lo, span.hi, &[2, 3]).unwrap();
        let (status, body) = call(&router, "POST", &submit_url, token,
            json!({"lease":lease.token,"kind":ORDERED_PRIMES,"count":2,"payload":STANDARD.encode(payload)})).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

        let (_, lease) = call(&router, "POST", &claim_url, token, json!({})).await;
        let lease: Lease = serde_json::from_value(lease).unwrap();
        assert_eq!(lease.blocks, [span]);
        let admin = private_operator_router(app.clone());
        let overview_url = format!("{PREFIX}/admin/overview");
        let devices_url = format!("{PREFIX}/admin/devices");
        let device_url = format!("{devices_url}/{id}");
        let pause_url = format!("{PREFIX}/admin/workers/{id}/pause");
        let history_url = format!("{PREFIX}/admin/resources/history");
        for url in [&overview_url, &devices_url, &device_url, &history_url] {
            for credential in ["", token] {
                assert_eq!(
                    call(&router, "GET", url, credential, Value::Null).await.0,
                    StatusCode::NOT_FOUND
                );
            }
        }
        assert_eq!(
            call(&router, "POST", &pause_url, token, json!({})).await.0,
            StatusCode::NOT_FOUND
        );
        for _ in 0..2 {
            assert_eq!(
                call(&admin, "POST", &pause_url, "", json!({})).await.0,
                StatusCode::OK
            );
        }
        let (status, device) = call(&admin, "GET", &device_url, "", Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(device["paused"], true);
        assert_eq!(device["benchmark_prime_count"], BENCHMARK_COUNT.to_string());
        assert_eq!(device["assignment"], json!(span));
        assert!(device["lease_expires_at"].is_i64());
        assert_eq!(device["decisions"].as_array().unwrap().len(), 2);
        assert_eq!(device["decisions"][0]["action"], "pause");
        assert_eq!(device["decisions"][0]["actor"], "private-admin");
        let (_, page) = call(
            &admin,
            "GET",
            &format!("{devices_url}?q=TEST&approval=approved&work=finishing"),
            "",
            Value::Null,
        )
        .await;
        assert_eq!(page["total"], 1);
        assert_eq!(page["devices"][0]["id"], id);
        // A failed telemetry database must leave overview and leased block submission usable.
        std::fs::write(
            app.shared
                .config
                .archive_dir
                .join("resource-history.sqlite"),
            b"invalid SQLite file",
        )
        .unwrap();
        assert_eq!(
            call(&admin, "GET", &history_url, "", Value::Null).await.0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        let (_, overview) = call(&admin, "GET", &overview_url, "", Value::Null).await;
        assert_eq!(overview["paused"], false);
        assert_eq!(overview["paused_devices"], 1);
        assert_eq!(overview["active_blocks"], 1);
        let (_, recovered) = call(&router, "POST", &claim_url, token, json!({})).await;
        assert_eq!(recovered["token"], lease.token);
        assert_eq!(
            call(
                &router,
                "POST",
                &format!("{PREFIX}/heartbeat"),
                token,
                json!({"lease":lease.token}),
            )
            .await
            .0,
            StatusCode::OK
        );
        let exact = Engine::new(100_000).enumerate(span.lo, span.hi);
        let payload = block_codec::encode(span.lo, span.hi, &exact).unwrap();
        let data = json!({"lease":lease.token,"kind":ORDERED_PRIMES,"count":exact.len(),
                          "payload":STANDARD.encode(payload)});
        for _ in 0..2 {
            let (status, body) = call(&router, "POST", &submit_url, token, data.clone()).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        let (_, progress) = call(
            &router,
            "GET",
            &format!("{PREFIX}/progress"),
            "",
            Value::Null,
        )
        .await;
        assert_eq!(progress["verified_through"], span.hi.to_string());
        assert_eq!(progress["verified_primes"], exact.len().to_string());
        assert_eq!(progress["state"], "waiting");
        assert_eq!(
            call(&router, "POST", &claim_url, token, json!({})).await,
            (StatusCode::OK, Value::Null)
        );
        assert_eq!(
            call(
                &admin,
                "POST",
                &format!("{PREFIX}/admin/workers/{id}/resume"),
                "",
                json!({}),
            )
            .await
            .0,
            StatusCode::OK
        );

        let (_, next) = call(&router, "POST", &claim_url, token, json!({})).await;
        app.db(move |db| Ok(db.mining_decide(id, "revoke", "test")?))
            .await
            .unwrap();
        let (status, _) = call(
            &router,
            "POST",
            &format!("{PREFIX}/heartbeat"),
            token,
            json!({"lease":next["token"]}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            call(&router, "POST", &claim_url, token, json!({})).await,
            (StatusCode::OK, Value::Null)
        );
    }
}
