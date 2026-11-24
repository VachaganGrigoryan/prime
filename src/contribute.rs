//! Untrusted submissions are staged in a separate database and cannot commit archive blocks.

use axum::{
    Form, Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prime_finder::archive::{Span, decode};
use reqwest::Url;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    net::IpAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

const BLOCK: u128 = 1 << 20;
const BLOCK_PAYLOAD_LIMIT: usize = BLOCK as usize + 16;
const LEASE_SECONDS: i64 = 120;
const ACTIVE_LIMIT: i64 = 64;
const RECENT_RECEIPTS: i64 = 4096;
const DEVICE_LIMIT: i64 = 4096;
const CAMPAIGN_LIMIT: i64 = 16;
const ENROLLMENTS_PER_MINUTE: i64 = 3;
const CHALLENGE_RETENTION_SECONDS: i64 = 600;
const DATABASE_LIMIT_BYTES: i64 = 96 * 1024 * 1024;

#[derive(Clone)]
pub struct Contributions(Arc<Shared>);

struct Shared {
    db: Mutex<Connection>,
    admission: Arc<Semaphore>,
    public_requests: Arc<Semaphore>,
    enrollments: Arc<Semaphore>,
    http: reqwest::Client,
    host: String,
    site_key: String,
    secret: String,
}

#[derive(Debug)]
struct Error(StatusCode, &'static str);
type Api<T> = Result<T, Error>;

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(&error, rusqlite::Error::SqliteFailure(e, _) if e.code == rusqlite::ErrorCode::DiskFull)
        {
            unavailable("Contribution storage is full")
        } else {
            internal(error)
        }
    }
}

fn internal(error: impl std::fmt::Display) -> Error {
    eprintln!("contributions: {error}");
    Error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Contribution operation failed",
    )
}

fn invalid(message: &'static str) -> Error {
    Error(StatusCode::BAD_REQUEST, message)
}

fn conflict(message: &'static str) -> Error {
    Error(StatusCode::CONFLICT, message)
}

fn unavailable(message: &'static str) -> Error {
    Error(StatusCode::SERVICE_UNAVAILABLE, message)
}

fn denied() -> Error {
    Error(
        StatusCode::UNAUTHORIZED,
        "Valid volunteer credential required",
    )
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn bounds(row: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<Span> {
    Ok(Span {
        lo: u64::from_be_bytes(row.get(at)?),
        hi: u64::from_be_bytes(row.get(at + 1)?),
    })
}

fn random_token() -> Api<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(internal)?;
    Ok(hex(&bytes))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn valid_token(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

pub fn open(path: &Path, site_key: String, secret: String) -> crate::Result<Contributions> {
    open_for_host(path, "test-math-workers.notallow.cc", site_key, secret)
}

fn open_for_host(
    path: &Path,
    host: &str,
    site_key: String,
    secret: String,
) -> crate::Result<Contributions> {
    if !valid_name(&site_key) || secret.is_empty() {
        return Err("Configure both Turnstile site key and secret".into());
    }
    let db = Connection::open(path)?;
    db.busy_timeout(Duration::from_secs(3))?;
    let journal_mode: String = db.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err("Contribution storage requires SQLite WAL mode".into());
    }
    db.execute_batch(
        "PRAGMA synchronous=FULL;
         PRAGMA foreign_keys=ON;
         PRAGMA journal_size_limit=4194304;
         BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS devices (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             name TEXT NOT NULL, digest BLOB NOT NULL UNIQUE, revoked INTEGER NOT NULL DEFAULT 0,
             verified INTEGER NOT NULL DEFAULT 0, rejected INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE IF NOT EXISTS challenges (
             digest BLOB PRIMARY KEY, source BLOB NOT NULL, enrolled_at INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS challenges_source ON challenges(source,enrolled_at);
         CREATE TABLE IF NOT EXISTS campaigns (
             id INTEGER PRIMARY KEY,
             lo BLOB NOT NULL, hi BLOB NOT NULL,
             next BLOB
         );
         CREATE TABLE IF NOT EXISTS blocks (
             id INTEGER PRIMARY KEY,
             campaign INTEGER NOT NULL REFERENCES campaigns(id),
             lo BLOB NOT NULL, hi BLOB NOT NULL,
             device INTEGER NOT NULL REFERENCES devices(id),
             lease TEXT NOT NULL UNIQUE, lease_end INTEGER NOT NULL,
             payload BLOB, checksum TEXT, count INTEGER,
             verifier TEXT, verify_lease TEXT UNIQUE, verify_end INTEGER,
             UNIQUE(campaign,lo)
         );
         CREATE INDEX IF NOT EXISTS blocks_device ON blocks(device,lease_end);
         CREATE TABLE IF NOT EXISTS receipts (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             device INTEGER NOT NULL REFERENCES devices(id),
             lease TEXT NOT NULL UNIQUE,
             submitted_checksum TEXT NOT NULL, submitted_count INTEGER NOT NULL,
             verifier TEXT NOT NULL, verify_lease TEXT NOT NULL UNIQUE,
             checked_checksum TEXT NOT NULL, checked_count INTEGER NOT NULL,
             verified INTEGER NOT NULL
         );
         COMMIT;",
    )?;
    let page_size: i64 = db.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let max_pages = DATABASE_LIMIT_BYTES / page_size;
    let actual: i64 = db.query_row(&format!("PRAGMA max_page_count={max_pages}"), [], |row| {
        row.get(0)
    })?;
    if actual > max_pages {
        return Err("Contribution database exceeds its storage limit".into());
    }

    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    Ok(Contributions(Arc::new(Shared {
        db: Mutex::new(db),
        admission: Arc::new(Semaphore::new(8)),
        public_requests: Arc::new(Semaphore::new(12)),
        enrollments: Arc::new(Semaphore::new(4)),
        http,
        host: host.into(),
        site_key,
        secret,
    })))
}

impl Contributions {
    async fn db<T, F>(&self, operation: F) -> Api<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Api<T> + Send + 'static,
    {
        let permit = self
            .0
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable("Contribution queue is busy. Try again shortly."))?;
        let state = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut database = state.0.db.lock().map_err(internal)?;
            operation(&mut database)
        })
        .await
        .map_err(internal)?
    }
}

async fn protected_headers(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response.headers_mut().insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(
        "default-src 'none'; script-src https://challenges.cloudflare.com; frame-src https://challenges.cloudflare.com; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"));
    response
}

async fn admit_public(
    State(state): State<Contributions>,
    request: Request,
    next: Next,
) -> Response {
    // Bound concurrent large bodies before Axum's JSON extractor allocates them.
    let Ok(_permit) = state.0.public_requests.clone().try_acquire_owned() else {
        return unavailable("Contribution endpoint is busy. Try again shortly.").into_response();
    };
    next.run(request).await
}

pub fn public_routes(state: Contributions) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ready" }))
        .route("/", get(join))
        .route(
            "/join",
            get(join)
                .post(enroll_form)
                .layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/api/v1/contribute/enroll",
            post(enroll_json).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route("/api/v1/contribute/claim", post(claim))
        .route(
            "/api/v1/contribute/heartbeat",
            post(heartbeat).layer(DefaultBodyLimit::max(1024)),
        )
        .route(
            "/api/v1/contribute/submit",
            post(submit).layer(DefaultBodyLimit::max(2 * 1024 * 1024)),
        )
        .layer(middleware::from_fn_with_state(state.clone(), admit_public))
        .layer(middleware::map_response(protected_headers))
        .with_state(state)
}

/// Mount under the coordinator's private authentication middleware.
pub fn private_routes(state: Contributions) -> Router {
    Router::new()
        .route("/api/v1/contribute/admin/campaign", post(campaign))
        .route("/api/v1/contribute/admin/revoke", post(revoke))
        .route("/api/v1/contribute/admin/status", get(status))
        .route("/api/v1/worker/contribute/claim", post(verify_claim))
        .route("/api/v1/worker/contribute/complete", post(verify_complete))
        .layer(DefaultBodyLimit::max(8 * 1024))
        .with_state(state)
}

async fn join(State(state): State<Contributions>) -> Html<String> {
    Html(format!(
        r#"<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join Prime Finder</title>
<h1>Contribute to Prime Finder</h1>
<p>Compute prime blocks on your computer. Trusted workers check submitted blocks against independent results.</p>
<form action="/join" method="post">
<label>Device name <input name="name" required maxlength="80" pattern="[A-Za-z0-9._-]+"></label>
<div class="cf-turnstile" data-sitekey="{}" data-action="enroll"></div>
<p><button>Get device token</button></p>
</form>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
</html>"#,
        state.0.site_key
    ))
}

#[derive(Deserialize)]
struct Enroll {
    name: String,
    #[serde(alias = "cf-turnstile-response")]
    challenge: String,
}

#[derive(Deserialize)]
struct SiteVerify {
    success: bool,
    hostname: Option<String>,
    action: Option<String>,
}

#[derive(Serialize)]
struct Credential {
    id: i64,
    token: String,
}

async fn enroll_form(
    State(state): State<Contributions>,
    headers: HeaderMap,
    Form(request): Form<Enroll>,
) -> Api<Html<String>> {
    let credential = enroll(state, headers, request).await?;
    Ok(Html(format!(
        r#"<!doctype html><html lang="en"><meta charset="utf-8">
<title>Prime Finder device enrolled</title><h1>Device enrolled</h1>
<p>Copy this device token into the worker when prompted.</p>
<p><label>Device token <input value="{}" size="70" readonly></label></p>
<p>Device ID {}</p></html>"#,
        credential.token, credential.id
    )))
}

async fn enroll_json(
    State(state): State<Contributions>,
    headers: HeaderMap,
    Json(request): Json<Enroll>,
) -> Api<Json<Credential>> {
    enroll(state, headers, request).await.map(Json)
}

async fn enroll(state: Contributions, headers: HeaderMap, request: Enroll) -> Api<Credential> {
    if !valid_name(&request.name) || request.challenge.is_empty() || request.challenge.len() > 2048
    {
        return Err(invalid("Device name or enrollment challenge is invalid"));
    }
    let _permit = state
        .0
        .enrollments
        .clone()
        .try_acquire_owned()
        .map_err(|_| unavailable("Enrollment is busy. Try again shortly."))?;
    let source = headers
        .get("cf-connecting-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<IpAddr>().ok());
    let source_key: [u8; 32] =
        Sha256::digest(source.map_or_else(String::new, |v| v.to_string())).into();
    let challenge_key: [u8; 32] = Sha256::digest(request.challenge.as_bytes()).into();
    state
        .db(move |db| check_enrollment(db, &challenge_key, &source_key, now()))
        .await?;

    let url = Url::parse("https://challenges.cloudflare.com/turnstile/v0/siteverify")
        .map_err(internal)?;
    let challenge = json!({"secret":state.0.secret,"response":request.challenge,"remoteip":source});
    let answer = state
        .0
        .http
        .post(url)
        .json(&challenge)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|_| unavailable("Enrollment verification unavailable"))?
        .json::<SiteVerify>()
        .await
        .map_err(|_| unavailable("Enrollment verification unavailable"))?;
    if !answer.success
        || answer.hostname.as_deref() != Some(&state.0.host)
        || answer.action.as_deref() != Some("enroll")
    {
        return Err(invalid(
            "Enrollment challenge failed. Return to the join page and try again.",
        ));
    }
    state
        .db(move |db| create_credential(db, &request.name, &challenge_key, &source_key))
        .await
}

fn check_enrollment(
    db: &Connection,
    challenge: &[u8; 32],
    source: &[u8; 32],
    time: i64,
) -> Api<()> {
    let used: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM challenges WHERE digest=?)",
        [challenge],
        |row| row.get(0),
    )?;
    if used {
        return Err(conflict("Enrollment challenge already used"));
    }
    let recent: i64 = db.query_row(
        "SELECT count(*) FROM challenges WHERE source=? AND enrolled_at>?",
        params![source, time - 60],
        |row| row.get(0),
    )?;
    if recent >= ENROLLMENTS_PER_MINUTE {
        return Err(Error(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many enrollments. Try again in a minute.",
        ));
    }
    let devices: i64 = db.query_row("SELECT count(*) FROM devices WHERE revoked=0", [], |row| {
        row.get(0)
    })?;
    if devices >= DEVICE_LIMIT {
        return Err(unavailable("Enrollment limit reached"));
    }
    Ok(())
}

fn create_credential(
    db: &mut Connection,
    name: &str,
    challenge: &[u8; 32],
    source: &[u8; 32],
) -> Api<Credential> {
    let token = random_token()?;
    let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let time = now();
    tx.execute(
        "DELETE FROM challenges WHERE enrolled_at<?",
        [time - CHALLENGE_RETENTION_SECONDS],
    )?;
    check_enrollment(&tx, challenge, source, time)?;
    tx.execute(
        "INSERT INTO challenges VALUES (?,?,?)",
        params![challenge, source, time],
    )?;
    tx.execute(
        "INSERT INTO devices(name,digest) VALUES (?,?)",
        params![name, digest],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(Credential { id, token })
}

fn bearer(headers: &HeaderMap) -> Api<[u8; 32]> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| valid_token(v))
        .ok_or_else(denied)?;
    Ok(Sha256::digest(token.as_bytes()).into())
}

fn device(db: &Connection, digest: &[u8; 32]) -> Api<i64> {
    db.query_row(
        "SELECT id FROM devices WHERE digest=? AND revoked=0",
        [digest],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(denied)
}

#[derive(Serialize)]
struct Work {
    lease: String,
    #[serde(flatten)]
    span: Span,
}

fn read_work(row: &rusqlite::Row<'_>) -> rusqlite::Result<Work> {
    Ok(Work {
        lease: row.get(0)?,
        span: bounds(row, 1)?,
    })
}

async fn claim(State(state): State<Contributions>, headers: HeaderMap) -> Api<Json<Option<Work>>> {
    let digest = bearer(&headers)?;
    state
        .db(move |db| volunteer_claim(db, digest))
        .await
        .map(Json)
}

fn volunteer_claim(db: &mut Connection, digest: [u8; 32]) -> Api<Option<Work>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let owner = device(&tx, &digest)?;
    let time = now();
    let existing = tx
        .query_row(
            "SELECT lease,lo,hi FROM blocks WHERE device=? AND payload IS NULL AND lease_end>?",
            params![owner, time],
            read_work,
        )
        .optional()?;
    if let Some(work) = existing {
        return Ok(Some(work));
    }
    let expired = tx.query_row(
        "SELECT id,lo,hi FROM blocks WHERE payload IS NULL AND lease_end<=? ORDER BY id LIMIT 1",
        [time], |row| Ok((row.get::<_,i64>(0)?, bounds(row,1)?))).optional()?;
    let lease = random_token()?;
    let span = if let Some((id, span)) = expired {
        tx.execute(
            "UPDATE blocks SET device=?,lease=?,lease_end=? WHERE id=?",
            params![owner, lease, time + LEASE_SECONDS, id],
        )?;
        span
    } else {
        let total: i64 = tx.query_row("SELECT count(*) FROM blocks", [], |row| row.get(0))?;
        if total >= ACTIVE_LIMIT {
            return Ok(None);
        }
        let candidate = tx
            .query_row(
                "SELECT id,next,hi FROM campaigns WHERE next IS NOT NULL ORDER BY id LIMIT 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, bounds(row, 1)?)),
            )
            .optional()?;
        let Some((campaign, remaining)) = candidate else {
            return Ok(None);
        };
        let last = ((u128::from(remaining.lo) / BLOCK + 1) * BLOCK - 1) as u64;
        let span = Span {
            lo: remaining.lo,
            hi: last.min(remaining.hi),
        };
        let next = (span.hi < remaining.hi).then(|| (span.hi + 1).to_be_bytes());
        tx.execute(
            "UPDATE campaigns SET next=? WHERE id=?",
            params![next, campaign],
        )?;
        tx.execute(
            "INSERT INTO blocks(campaign,lo,hi,device,lease,lease_end) VALUES (?,?,?,?,?,?)",
            params![
                campaign,
                span.lo.to_be_bytes(),
                span.hi.to_be_bytes(),
                owner,
                lease,
                time + LEASE_SECONDS
            ],
        )?;
        span
    };
    tx.commit()?;
    Ok(Some(Work { lease, span }))
}

#[derive(Deserialize)]
struct Heartbeat {
    lease: String,
}

async fn heartbeat(
    State(state): State<Contributions>,
    headers: HeaderMap,
    Json(request): Json<Heartbeat>,
) -> Api<Json<Value>> {
    let digest = bearer(&headers)?;
    if !valid_token(&request.lease) {
        return Err(invalid("Invalid lease"));
    }
    state.db(move |db| {
        let owner = device(db, &digest)?;
        let time = now();
        let changed = db.execute(
            "UPDATE blocks SET lease_end=? WHERE device=? AND lease=? AND payload IS NULL AND lease_end>?",
            params![time+LEASE_SECONDS,owner,request.lease,time])?;
        if changed == 0 { return Err(conflict("Lease expired or unknown")); }
        Ok(Json(json!({"ok":true})))
    }).await
}

#[derive(Deserialize)]
struct Submission {
    lease: String,
    payload: String,
    checksum: String,
    count: u64,
}

async fn submit(
    State(state): State<Contributions>,
    headers: HeaderMap,
    Json(request): Json<Submission>,
) -> Api<Json<Value>> {
    let digest = bearer(&headers)?;
    if !valid_token(&request.lease)
        || !valid_token(&request.checksum)
        || request.payload.len() > BLOCK_PAYLOAD_LIMIT.div_ceil(3) * 4
        || request.count > BLOCK as u64
    {
        return Err(invalid("Invalid contribution submission"));
    }
    state
        .db(move |db| {
            volunteer_submit(db, digest, request)?;
            Ok(Json(json!({"ok":true})))
        })
        .await
}

fn volunteer_submit(db: &mut Connection, digest: [u8; 32], request: Submission) -> Api<()> {
    let payload = STANDARD
        .decode(&request.payload)
        .map_err(|_| invalid("Invalid base64 payload"))?;
    if payload.len() > BLOCK_PAYLOAD_LIMIT || hex(&Sha256::digest(&payload)) != request.checksum {
        return Err(invalid("Contribution payload or checksum is invalid"));
    }
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let owner = device(&tx, &digest)?;
    let block = tx
        .query_row(
            "SELECT lo,hi,lease_end,checksum,count FROM blocks WHERE device=? AND lease=?",
            params![owner, request.lease],
            |row| {
                Ok((
                    bounds(row, 0)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<u32>>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((span, time, checksum, count)) = block else {
        let replay: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM receipts WHERE device=? AND lease=? AND submitted_checksum=? AND submitted_count=?)",
            params![owner,request.lease,request.checksum,request.count as u32], |row| row.get(0))?;
        return if replay {
            Ok(())
        } else {
            Err(conflict("Lease expired or unknown"))
        };
    };
    if let Some(checksum) = checksum {
        return if checksum == request.checksum && count.map(u64::from) == Some(request.count) {
            Ok(())
        } else {
            Err(conflict("Lease already has a different submission"))
        };
    }
    if time <= now() {
        return Err(conflict("Lease expired or unknown"));
    }
    decode(span, request.count, &payload).map_err(|_| invalid("Invalid prime block encoding"))?;
    tx.execute(
        "UPDATE blocks SET payload=?,checksum=?,count=? WHERE lease=?",
        params![
            payload,
            request.checksum,
            request.count as u32,
            request.lease
        ],
    )?;
    tx.commit()?;
    Ok(())
}

async fn campaign(State(state): State<Contributions>, Json(span): Json<Span>) -> Api<Json<Value>> {
    span.validate()
        .map_err(|_| invalid("Invalid campaign range"))?;
    state
        .db(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let overlap: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM campaigns WHERE lo<=? AND hi>=?)",
                params![span.hi.to_be_bytes(), span.lo.to_be_bytes()],
                |row| row.get(0),
            )?;
            if overlap {
                return Err(conflict(
                    "Campaign already covers part of the requested range",
                ));
            }
            let total: i64 =
                tx.query_row("SELECT count(*) FROM campaigns", [], |row| row.get(0))?;
            if total >= CAMPAIGN_LIMIT {
                return Err(conflict("Campaign limit reached"));
            }
            tx.execute(
                "INSERT INTO campaigns(lo,hi,next) VALUES (?,?,?)",
                params![
                    span.lo.to_be_bytes(),
                    span.hi.to_be_bytes(),
                    span.lo.to_be_bytes()
                ],
            )?;
            let id = tx.last_insert_rowid();
            tx.commit()?;
            Ok(Json(json!({"id":id,"span":span})))
        })
        .await
}

#[derive(Deserialize)]
struct Revoke {
    id: i64,
}

async fn revoke(
    State(state): State<Contributions>,
    Json(request): Json<Revoke>,
) -> Api<Json<Value>> {
    state
        .db(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if tx.execute("UPDATE devices SET revoked=1 WHERE id=?", [request.id])? == 0 {
                return Err(Error(StatusCode::NOT_FOUND, "Device not found"));
            }
            tx.execute(
                "UPDATE blocks SET lease_end=0 WHERE device=? AND payload IS NULL",
                [request.id],
            )?;
            tx.commit()?;
            Ok(Json(json!({"ok":true})))
        })
        .await
}

async fn status(State(state): State<Contributions>) -> Api<Json<Value>> {
    state
        .db(move |db| {
            let (devices, verified, rejected): (i64, i64, i64) = db.query_row(
                "SELECT COUNT(*),COALESCE(SUM(verified),0),COALESCE(SUM(rejected),0) FROM devices",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let pending: i64 = db.query_row(
                "SELECT count(*) FROM blocks WHERE payload IS NOT NULL",
                [],
                |row| row.get(0),
            )?;
            let campaigns: i64 =
                db.query_row("SELECT count(*) FROM campaigns", [], |row| row.get(0))?;
            Ok(Json(
                json!({"devices":devices,"campaigns":campaigns,"pending":pending,
            "submitted":pending+verified+rejected,"verified":verified,"rejected":rejected}),
            ))
        })
        .await
}

#[derive(Deserialize)]
struct Verifier {
    worker: String,
}

async fn verify_claim(
    State(state): State<Contributions>,
    Json(request): Json<Verifier>,
) -> Api<Json<Option<Work>>> {
    if !valid_name(&request.worker) {
        return Err(invalid("Invalid worker ID"));
    }
    state
        .db(move |db| verification_claim(db, &request.worker))
        .await
        .map(Json)
}

fn verification_claim(db: &mut Connection, worker: &str) -> Api<Option<Work>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let time = now();
    let existing = tx
        .query_row(
            "SELECT verify_lease,lo,hi FROM blocks WHERE verifier=? AND verify_end>?",
            params![worker, time],
            read_work,
        )
        .optional()?;
    if let Some(work) = existing {
        return Ok(Some(work));
    }
    let block = tx.query_row(
        "SELECT id,lo,hi FROM blocks WHERE payload IS NOT NULL AND (verify_end IS NULL OR verify_end<=?) ORDER BY id LIMIT 1",
        [time], |row| Ok((row.get::<_,i64>(0)?,bounds(row,1)?))).optional()?;
    let Some((id, span)) = block else {
        return Ok(None);
    };
    let lease = random_token()?;
    tx.execute(
        "UPDATE blocks SET verifier=?,verify_lease=?,verify_end=? WHERE id=?",
        params![worker, lease, time + LEASE_SECONDS, id],
    )?;
    tx.commit()?;
    Ok(Some(Work { lease, span }))
}

#[derive(Deserialize)]
struct Verification {
    worker: String,
    lease: String,
    checksum: String,
    count: u64,
}

async fn verify_complete(
    State(state): State<Contributions>,
    Json(request): Json<Verification>,
) -> Api<Json<Value>> {
    if !valid_name(&request.worker)
        || !valid_token(&request.lease)
        || !valid_token(&request.checksum)
        || request.count > BLOCK as u64
    {
        return Err(invalid("Invalid verification result"));
    }
    state
        .db(move |db| {
            let verified = verify(db, request)?;
            Ok(Json(json!({"verified":verified})))
        })
        .await
}

fn verify(db: &mut Connection, request: Verification) -> Api<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let previous = tx.query_row(
        "SELECT checked_checksum,checked_count,verified FROM receipts WHERE verifier=? AND verify_lease=?",
        params![request.worker,request.lease],
        |row| Ok((row.get::<_,String>(0)?,row.get::<_,u32>(1)?,row.get::<_,bool>(2)?))).optional()?;
    if let Some((checksum, count, verified)) = previous {
        return if checksum == request.checksum && u64::from(count) == request.count {
            Ok(verified)
        } else {
            Err(conflict("Lease already has a different verification"))
        };
    }
    let block = tx.query_row(
        "SELECT id,device,lease,checksum,count,verify_end FROM blocks WHERE verifier=? AND verify_lease=?",
        params![request.worker,request.lease],
        |row| Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,String>(2)?,
            row.get::<_,String>(3)?,row.get::<_,u32>(4)?,row.get::<_,i64>(5)?))).optional()?;
    let Some((id, owner, lease, checksum, count, time)) = block else {
        return Err(conflict("Verification lease expired or unknown"));
    };
    if time <= now() {
        return Err(conflict("Verification lease expired or unknown"));
    }
    let verified = request.checksum == checksum && request.count == u64::from(count);
    tx.execute(
        "UPDATE devices SET verified=verified+?,rejected=rejected+? WHERE id=?",
        params![verified, !verified, owner],
    )?;
    tx.execute(
        "INSERT INTO receipts(device,lease,submitted_checksum,submitted_count,verifier,verify_lease,checked_checksum,checked_count,verified)
         VALUES (?,?,?,?,?,?,?,?,?)",
        params![owner,lease,checksum,count,request.worker,request.lease,request.checksum,request.count as u32,verified])?;
    if verified {
        tx.execute("DELETE FROM blocks WHERE id=?", [id])?;
    } else {
        tx.execute(
            "UPDATE blocks SET payload=NULL,checksum=NULL,count=NULL,lease_end=0,
             verifier=NULL,verify_lease=NULL,verify_end=NULL WHERE id=?",
            [id],
        )?;
    }
    tx.execute(
        "DELETE FROM receipts WHERE id <= (SELECT MAX(id)-? FROM receipts)",
        [RECENT_RECEIPTS],
    )?;
    tx.commit()?;
    Ok(verified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use prime_finder::{archive::encode, engine::Engine};
    use std::path::PathBuf;
    use tower::ServiceExt;

    struct TempDb(PathBuf);

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("prime-contribution-{}", random_token().unwrap()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn open(&self) -> Contributions {
            open(
                &self.0.join("contributions.sqlite"),
                "test-key".into(),
                "test-secret".into(),
            )
            .unwrap()
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn request(
        router: &Router,
        method: Method,
        path: &str,
        auth: Option<&str>,
        body: Value,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(auth) = auth {
            request = request.header(header::AUTHORIZATION, format!("Bearer {auth}"));
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn submit_payload(work: &Value, primes: &[u64]) -> Value {
        let span = Span {
            lo: work["lo"].as_str().unwrap().parse().unwrap(),
            hi: work["hi"].as_str().unwrap().parse().unwrap(),
        };
        let payload = encode(span, primes).unwrap();
        json!({"lease":work["lease"],"checksum":hex(&Sha256::digest(&payload)),
            "count":primes.len(),"payload":STANDARD.encode(payload)})
    }

    #[tokio::test]
    async fn contribution_survives_reopen_and_verifies_exactly() {
        let temp = TempDb::new();
        let state = temp.open();
        let device = state
            .db(|db| create_credential(db, "test", &[1; 32], &[2; 32]))
            .await
            .unwrap();
        let public = public_routes(state.clone());
        let private = private_routes(state.clone());

        let (code, _) = request(
            &public,
            Method::POST,
            "/api/v1/worker/contribute/claim",
            Some(&device.token),
            json!({"worker":"mac"}),
        )
        .await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, _) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/admin/campaign",
            Some(&device.token),
            json!({"lo":"1","hi":"19"}),
        )
        .await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, _) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/claim",
            None,
            json!({}),
        )
        .await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);

        let (code, _) = request(
            &private,
            Method::POST,
            "/api/v1/contribute/admin/campaign",
            None,
            json!({"lo":"0","hi":"19"}),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let (code, work) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/claim",
            Some(&device.token),
            json!({}),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            (work["lo"].as_str(), work["hi"].as_str()),
            (Some("0"), Some("19"))
        );
        let (_, repeat) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/claim",
            Some(&device.token),
            json!({}),
        )
        .await;
        assert_eq!(work, repeat);
        let (code, _) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/heartbeat",
            Some(&device.token),
            json!({"lease":work["lease"]}),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let primes = Engine::new(100_000).enumerate(0, 19);
        let payload = submit_payload(&work, &primes);
        let (code, _) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/submit",
            Some(&device.token),
            payload.clone(),
        )
        .await;
        assert_eq!(code, StatusCode::OK);

        drop(public);
        drop(private);
        drop(state);
        let state = temp.open();
        let public = public_routes(state.clone());
        let private = private_routes(state.clone());
        let (code, checked) = request(
            &private,
            Method::POST,
            "/api/v1/worker/contribute/claim",
            None,
            json!({"worker":"mac"}),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            (checked["lo"].as_str(), checked["hi"].as_str()),
            (Some("0"), Some("19"))
        );
        let verification = json!({"worker":"mac","lease":checked["lease"],"checksum":payload["checksum"],"count":payload["count"]});
        for _ in 0..2 {
            let (code, response) = request(
                &private,
                Method::POST,
                "/api/v1/worker/contribute/complete",
                None,
                verification.clone(),
            )
            .await;
            assert_eq!(code, StatusCode::OK);
            assert_eq!(response["verified"], true);
        }
        let (code, _) = request(
            &public,
            Method::POST,
            "/api/v1/contribute/submit",
            Some(&device.token),
            payload,
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let (code, counters) = request(
            &private,
            Method::GET,
            "/api/v1/contribute/admin/status",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            counters,
            json!({"devices":1,"campaigns":1,"pending":0,"submitted":1,"verified":1,"rejected":0})
        );
    }

    #[test]
    fn block_boundary_revocation_and_stage_limit() {
        let temp = TempDb::new();
        let state = temp.open();
        let mut db = state.0.db.lock().unwrap();
        let a = create_credential(&mut db, "a", &[1; 32], &[6; 32]).unwrap();
        let b = create_credential(&mut db, "b", &[2; 32], &[7; 32]).unwrap();
        assert_eq!(
            create_credential(&mut db, "replay", &[1; 32], &[8; 32])
                .err()
                .unwrap()
                .0,
            StatusCode::CONFLICT
        );
        let hash_a = Sha256::digest(a.token.as_bytes()).into();
        let hash_b = Sha256::digest(b.token.as_bytes()).into();
        let lo = u64::MAX - 1;
        let hi = u64::MAX;
        db.execute(
            "INSERT INTO campaigns(lo,hi,next) VALUES(?,?,?)",
            params![lo.to_be_bytes(), hi.to_be_bytes(), lo.to_be_bytes()],
        )
        .unwrap();
        let first = volunteer_claim(&mut db, hash_a).unwrap().unwrap();
        assert_eq!(first.span, Span { lo, hi });
        assert!(volunteer_claim(&mut db, hash_b).unwrap().is_none());
        db.execute("UPDATE devices SET revoked=1 WHERE id=?", [a.id])
            .unwrap();
        db.execute("UPDATE blocks SET lease_end=0 WHERE device=?", [a.id])
            .unwrap();
        assert_eq!(
            volunteer_claim(&mut db, hash_a).err().unwrap().0,
            StatusCode::UNAUTHORIZED
        );
        let second = volunteer_claim(&mut db, hash_b).unwrap().unwrap();
        assert_eq!(first.span, second.span);
        assert_ne!(first.lease, second.lease);
        let forged = encode(second.span, &[u64::MAX - 1]).unwrap();
        let submission = Submission {
            lease: second.lease,
            payload: STANDARD.encode(&forged),
            checksum: hex(&Sha256::digest(&forged)),
            count: 1,
        };
        volunteer_submit(&mut db, hash_b, submission).unwrap();
        let check = verification_claim(&mut db, "mac").unwrap().unwrap();
        let actual = encode(check.span, &[]).unwrap();
        assert!(
            !verify(
                &mut db,
                Verification {
                    worker: "mac".into(),
                    lease: check.lease,
                    checksum: hex(&Sha256::digest(&actual)),
                    count: 0
                }
            )
            .unwrap()
        );
        let rejected: i64 = db
            .query_row("SELECT rejected FROM devices WHERE id=?", [b.id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rejected, 1);

        let retry = volunteer_claim(&mut db, hash_b).unwrap().unwrap();
        assert_eq!(retry.span, first.span);
        assert_ne!(retry.lease, first.lease);
        volunteer_submit(
            &mut db,
            hash_b,
            Submission {
                lease: retry.lease,
                payload: STANDARD.encode(&actual),
                checksum: hex(&Sha256::digest(&actual)),
                count: 0,
            },
        )
        .unwrap();
        let verified = verification_claim(&mut db, "mac").unwrap().unwrap();
        assert!(
            verify(
                &mut db,
                Verification {
                    worker: "mac".into(),
                    lease: verified.lease,
                    checksum: hex(&Sha256::digest(&actual)),
                    count: 0,
                }
            )
            .unwrap()
        );

        let c = db
            .execute(
                "INSERT INTO campaigns(lo,hi,next) VALUES(?,?,?)",
                params![
                    0u64.to_be_bytes(),
                    ((BLOCK as u64) * (ACTIVE_LIMIT as u64 + 1)).to_be_bytes(),
                    0u64.to_be_bytes()
                ],
            )
            .unwrap();
        assert_eq!(c, 1);
        let campaign_id = db.last_insert_rowid();
        for i in 0..ACTIVE_LIMIT {
            let lo = i as u64 * BLOCK as u64;
            db.execute(
                "INSERT INTO blocks(campaign,lo,hi,device,lease,lease_end,payload,checksum,count)
                      VALUES (?,?,?,?,?,0,X'','',0)",
                params![
                    campaign_id,
                    lo.to_be_bytes(),
                    (lo + BLOCK as u64 - 1).to_be_bytes(),
                    b.id,
                    i.to_string()
                ],
            )
            .unwrap();
        }
        assert!(volunteer_claim(&mut db, hash_b).unwrap().is_none());
    }
}
