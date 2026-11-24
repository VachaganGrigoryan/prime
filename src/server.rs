use crate::{Result, secret};
use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as RoutePath, Query, Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prime_finder::archive::{Archive, ArchiveError, Commit, Job, PreparedBatch, Span};
use prime_finder::gap_store::{GapLease, GapPage, GapReport, GapStatus};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{self, OpenOptions},
    net::SocketAddr,
    os::unix::{ffi::OsStrExt, io::AsRawFd},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tower_http::services::{ServeDir, ServeFile};

const PUBLIC_DB_REQUESTS: usize = 4;
const PUBLIC_PRIME_LIMIT: usize = 4225;
const PUBLIC_BUCKET_LIMIT: usize = 160;
pub const UPLOAD_BODY_LIMIT: usize = 2 * (1 << 20);

pub struct Config {
    pub bind: SocketAddr,
    pub public_bind: Option<SocketAddr>,
    pub contribute_bind: Option<SocketAddr>,
    pub archive_dir: PathBuf,
    pub web_root: PathBuf,
    pub quota_bytes: u64,
    pub reserve_bytes: u64,
    pub mount_id: Option<String>,
}

#[derive(Clone)]
struct App {
    shared: Arc<Shared>,
    public_db: Option<Arc<Semaphore>>,
}
struct Shared {
    archive: Mutex<Archive>,
    uploads: Arc<Semaphore>,
    workers: Mutex<BTreeMap<String, Worker>>,
    admin: String,
    worker: String,
    config: Config,
    storage_fault: AtomicBool,
}

#[derive(Clone, Serialize)]
struct Worker {
    id: String,
    last_seen: String,
    slots: u16,
}

#[derive(Debug)]
struct ApiError(StatusCode, String);
type Api<T> = std::result::Result<T, ApiError>;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<ArchiveError> for ApiError {
    fn from(error: ArchiveError) -> Self {
        use ArchiveError::*;
        let status = match &error {
            Invalid(_) => StatusCode::BAD_REQUEST,
            NotFound => StatusCode::NOT_FOUND,
            Conflict(_) | Uncovered => StatusCode::CONFLICT,
            StoragePaused => StatusCode::INSUFFICIENT_STORAGE,
            Database(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::DiskFull =>
            {
                StatusCode::INSUFFICIENT_STORAGE
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            eprintln!("archive: {error}");
            return Self(status, "Archive operation failed".into());
        }
        Self(status, error.to_string())
    }
}

fn internal(error: impl std::fmt::Display) -> ApiError {
    eprintln!("coordinator: {error}");
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Coordinator operation failed".into(),
    )
}

impl App {
    async fn db<T, F>(&self, run: F) -> Api<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Archive) -> Api<T> + Send + 'static,
    {
        let app = self.clone();
        let permit = self
            .public_db
            .as_ref()
            .map(|limit| {
                limit.clone().try_acquire_owned().map_err(|_| {
                    ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Archive is busy. Try again shortly.".into(),
                    )
                })
            })
            .transpose()?;
        tokio::task::spawn_blocking(move || {
            // Retain the admission slot even if the HTTP client disconnects.
            let _permit = permit;
            run(&mut *app
                .shared
                .archive
                .lock()
                .map_err(|_| internal("archive lock poisoned"))?)
        })
        .await
        .map_err(internal)?
    }

    fn storage(&self) -> Api<(u64, u64)> {
        let dir = &self.shared.config.archive_dir;
        let configured = self.shared.config.quota_bytes;
        let mount_free = volume_free(dir).map_err(internal)?;
        let used = [
            "archive.sqlite",
            "archive.sqlite-wal",
            "archive.sqlite-shm",
            "contributions.sqlite",
            "contributions.sqlite-wal",
            "contributions.sqlite-shm",
        ]
        .into_iter()
        .filter_map(|name| fs::metadata(dir.join(name)).ok().map(|info| info.len()))
        .fold(0_u64, u64::saturating_add);
        Ok((configured, mount_free.min(configured.saturating_sub(used))))
    }

    fn record_worker(&self, id: &str, slots: u16) -> Api<()> {
        if id.is_empty()
            || id.len() > 80
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || !(1..=32).contains(&slots)
        {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Invalid worker id or slot count".into(),
            ));
        }
        let now = now();
        let mut workers = self
            .shared
            .workers
            .lock()
            .map_err(|_| internal("worker lock poisoned"))?;
        workers.retain(|_, worker| {
            worker
                .last_seen
                .parse::<u64>()
                .is_ok_and(|t| now.saturating_sub(t) < 600)
        });
        if workers.len() >= 256 && !workers.contains_key(id) {
            return Err(ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "Worker limit reached".into(),
            ));
        }
        workers.insert(
            id.into(),
            Worker {
                id: id.into(),
                last_seen: now.to_string(),
                slots,
            },
        );
        Ok(())
    }

    fn gap_space(&self, required: u64) -> prime_finder::archive::Result<()> {
        if self.shared.storage_fault.load(Ordering::Relaxed)
            || self.storage().map_err(|_| ArchiveError::StoragePaused)?.1 < required
        {
            return Err(ArchiveError::StoragePaused);
        }
        Ok(())
    }

    fn gap_write<T>(&self, result: prime_finder::archive::Result<T>) -> Api<T> {
        if matches!(&result, Err(ArchiveError::Database(rusqlite::Error::SqliteFailure(error, _)))
            if matches!(error.code, rusqlite::ErrorCode::DiskFull | rusqlite::ErrorCode::SystemIoFailure))
        {
            self.shared.storage_fault.store(true, Ordering::Relaxed);
            return Err(ArchiveError::StoragePaused.into());
        }
        Ok(result?)
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn volume_free(path: &Path) -> std::io::Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is nul terminated and statvfs writes the complete output on success.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the successful statvfs call initialized stats.
    let stats = unsafe { stats.assume_init() };
    Ok((stats.f_bavail as u128 * stats.f_frsize as u128).min(u128::from(u64::MAX)) as u64)
}

fn verify_mount(path: &Path, identity: &str) -> Result<()> {
    let path = path.canonicalize()?;
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
    let matching_mount = mountinfo.lines().any(|line| {
        let Some((mount, device)) = line.split_once(" - ") else {
            return false;
        };
        mount
            .split_whitespace()
            .nth(4)
            .is_some_and(|target| path == Path::new(target))
            && device.split_whitespace().next() == Some("ext4")
    });
    if !matching_mount || fs::read_to_string(path.join(".prime-archive-id"))?.trim() != identity {
        return Err(
            "Expected ext4 prime archive mount is missing or has the wrong identity".into(),
        );
    }
    Ok(())
}

pub async fn serve(config: Config) -> Result<()> {
    if config.reserve_bytes >= config.quota_bytes {
        return Err("Archive reserve must be smaller than its quota".into());
    }
    let admin = format!(
        "Basic {}",
        STANDARD.encode(format!("admin:{}", secret("PRIME_ADMIN_TOKEN")?))
    );
    let worker = format!("Bearer {}", secret("PRIME_WORKER_TOKEN")?);
    if let Some(identity) = &config.mount_id {
        verify_mount(&config.archive_dir, identity)?;
    } else {
        fs::create_dir_all(&config.archive_dir)?;
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(config.archive_dir.join("coordinator.lock"))?;
    // SAFETY: hold the open descriptor for the lifetime of the coordinator.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("Another coordinator owns this archive".into());
    }
    let archive = Archive::open(&config.archive_dir.join("archive.sqlite"))?;
    let web_root = config.web_root.clone();
    let bind = config.bind;
    let listener = TcpListener::bind(bind).await?;
    let public = match config.public_bind {
        Some(address) => Some(TcpListener::bind(address).await?),
        None => None,
    };
    let contributions = match config.contribute_bind {
        Some(address) => {
            let state = crate::contribute::open(
                &config.archive_dir.join("contributions.sqlite"),
                secret("PRIME_TURNSTILE_SITE_KEY")?,
                secret("PRIME_TURNSTILE_SECRET")?,
            )?;
            Some((TcpListener::bind(address).await?, state))
        }
        None => None,
    };
    let app = App {
        shared: Arc::new(Shared {
            archive: Mutex::new(archive),
            uploads: Arc::new(Semaphore::new(8)),
            workers: Mutex::new(BTreeMap::new()),
            admin,
            worker,
            config,
            storage_fault: AtomicBool::new(false),
        }),
        public_db: None,
    };
    eprintln!(
        "Prime Finder private listener on http://{}",
        listener.local_addr()?
    );
    let private_routes = private_router(
        app.clone(),
        web_root.clone(),
        contributions.as_ref().map(|(_, state)| state.clone()),
    );
    tokio::try_join!(
        serve_http(listener, private_routes),
        async {
            if let Some(public) = public {
                eprintln!(
                    "Test Math archive listener on http://{}",
                    public.local_addr()?
                );
                serve_http(public, public_routes(app, web_root)).await?;
            }
            Ok(())
        },
        async {
            if let Some((listener, state)) = contributions {
                eprintln!(
                    "Test Math contribution listener on http://{}",
                    listener.local_addr()?
                );
                serve_http(listener, crate::contribute::public_routes(state)).await?;
            }
            Ok(())
        },
    )?;
    drop(lock);
    Ok(())
}

async fn serve_http(listener: TcpListener, router: Router) -> std::io::Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}

#[cfg(test)]
fn routes(app: App, web_root: PathBuf) -> Router {
    private_router(app, web_root, None)
}

fn private_router(
    app: App,
    web_root: PathBuf,
    contributions: Option<crate::contribute::Contributions>,
) -> Router {
    let archive = Router::new()
        .route("/healthz", get(|| async { "ready" }))
        .route(
            "/api/v1/capabilities",
            get(|| async { Json(json!({ "mode": "private" })) }),
        )
        .route("/api/v1/status", get(status))
        .route("/api/v1/jobs", post(create_job))
        .route("/api/v1/jobs/{id}", get(get_job))
        .route("/api/v1/jobs/{id}/{action}", post(job_action))
        .route("/api/v1/primes", get(page))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/export", get(export))
        .route("/api/v1/gaps", get(gap_status).post(queue_gaps))
        .route("/api/v1/worker/claim", post(claim))
        .route("/api/v1/worker/heartbeat", post(heartbeat))
        .route("/api/v1/worker/commit", post(commit))
        .route("/api/v1/worker/commit-batch", post(commit_batch))
        .route("/api/v1/worker/gaps/claim", post(claim_gaps))
        .route("/api/v1/worker/gaps/blocks", get(gap_blocks))
        .route("/api/v1/worker/gaps/heartbeat", post(heartbeat_gaps))
        .route("/api/v1/worker/gaps/commit", post(commit_gaps))
        .route("/api/v1/worker/gaps/fail", post(fail_gaps))
        .fallback_service(ServeDir::new(web_root))
        .with_state(app.clone());
    let archive = match contributions {
        Some(state) => archive
            .merge(crate::contribute::private_routes(state.clone()))
            .merge(
                Router::new()
                    .route(
                        "/api/v1/contribute/admin/evidence/campaign",
                        post(evidence_campaign),
                    )
                    .layer(Extension(state))
                    .with_state(app.clone()),
            ),
        None => archive,
    };
    archive
        .layer(DefaultBodyLimit::max(UPLOAD_BODY_LIMIT))
        .layer(middleware::from_fn_with_state(app, auth))
}

#[derive(Deserialize)]
struct EvidenceCampaignRequest {
    job_id: i64,
    #[serde(default)]
    close: bool,
}

async fn evidence_campaign(
    State(app): State<App>,
    Extension(contributions): Extension<crate::contribute::Contributions>,
    Json(request): Json<EvidenceCampaignRequest>,
) -> Api<Response> {
    let id = request.job_id;
    if request.close {
        return Ok(crate::contribute::close_evidence_campaign(contributions, id).await);
    }
    let job = app.db(move |db| Ok(db.job(id)?)).await?;
    if !matches!(job.state.as_str(), "queued" | "running" | "paused") {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "An active archive job is required".into(),
        ));
    }
    Ok(crate::contribute::start_evidence_campaign(contributions, id, job.span).await)
}

fn public_routes(mut app: App, web_root: PathBuf) -> Router {
    app.public_db = Some(Arc::new(Semaphore::new(PUBLIC_DB_REQUESTS)));
    Router::new()
        .route("/healthz", get(|| async { "ready" }))
        .route(
            "/api/v1/capabilities",
            get(|| async { Json(json!({ "mode": "public" })) }),
        )
        .route("/api/v1/primes", get(page))
        .route("/api/v1/summary", get(summary))
        .route("/api/v1/gaps", get(gap_status))
        .route_service("/", ServeFile::new(web_root.join("index.html")))
        .route_service(
            "/primes",
            ServeFile::new(web_root.join("primes/index.html")),
        )
        .route_service(
            "/primes/",
            ServeFile::new(web_root.join("primes/index.html")),
        )
        .route_service(
            "/riemann",
            ServeFile::new(web_root.join("riemann/index.html")),
        )
        .route_service(
            "/riemann/",
            ServeFile::new(web_root.join("riemann/index.html")),
        )
        .route_service(
            "/data/math-lab-v1.json",
            ServeFile::new(web_root.join("data/math-lab-v1.json")),
        )
        .nest_service("/assets", ServeDir::new(web_root.join("assets")))
        .with_state(app)
}

async fn auth(State(app): State<App>, request: Request, next: Next) -> Response {
    let path = request.uri().path();
    if path == "/healthz" {
        return next.run(request).await;
    }
    let worker = path.starts_with("/api/v1/worker/");
    let expected = if worker {
        &app.shared.worker
    } else {
        &app.shared.admin
    };
    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .map(HeaderValue::as_bytes)
        .unwrap_or_default();
    if !same_secret(provided, expected.as_bytes()) {
        let mut response =
            ApiError(StatusCode::UNAUTHORIZED, "Authentication required".into()).into_response();
        if !worker {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"Prime Finder\""),
            );
        }
        return response;
    }
    if request.method() != Method::GET
        && request.headers().get(header::ORIGIN).is_some_and(|origin| {
            let host = request
                .headers()
                .get(header::HOST)
                .and_then(|host| host.to_str().ok());
            host.is_none_or(|host| origin.to_str().ok() != Some(format!("http://{host}").as_str()))
        })
    {
        return ApiError(StatusCode::FORBIDDEN, "Cross-origin write".into()).into_response();
    }
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn same_secret(provided: &[u8], expected: &[u8]) -> bool {
    provided.len() == expected.len()
        && provided
            .iter()
            .zip(expected)
            .fold(0_u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn job_json(job: Job) -> Value {
    json!({ "id": job.id.to_string(), "lo":job.span.lo.to_string(), "hi":job.span.hi.to_string(),
        "state":job.state, "width":job.width, "covered":job.covered, "count":job.count })
}

async fn status(State(app): State<App>) -> Api<Json<Value>> {
    let (total, available) = app.storage()?;
    let fault = app.shared.storage_fault.load(Ordering::Relaxed);
    let workers: Vec<_> = app
        .shared
        .workers
        .lock()
        .map_err(|_| internal("worker lock poisoned"))?
        .values()
        .filter(|worker| {
            worker
                .last_seen
                .parse::<u64>()
                .is_ok_and(|t| now().saturating_sub(t) < 60)
        })
        .cloned()
        .collect();
    let (jobs, revision) = app
        .db(|archive| Ok((archive.jobs()?, archive.snapshot()?)))
        .await?;
    Ok(Json(
        json!({ "jobs": jobs.into_iter().map(job_json).collect::<Vec<_>>(), "workers":workers,
        "storage": {"available_bytes":available.to_string(), "total_bytes":total.to_string(),
        "paused":fault || available <= app.shared.config.reserve_bytes,
        "reason": if fault {Some("Archive write failed. Check NAS capacity and restart the coordinator.")} else {None}}, "revision":revision.to_string() }),
    ))
}

async fn create_job(State(app): State<App>, Json(span): Json<Span>) -> Api<Json<Value>> {
    Ok(Json(job_json(
        app.db(move |db| Ok(db.create_job(span)?)).await?,
    )))
}

async fn get_job(State(app): State<App>, RoutePath(id): RoutePath<i64>) -> Api<Json<Value>> {
    Ok(Json(job_json(app.db(move |db| Ok(db.job(id)?)).await?)))
}

async fn job_action(
    State(app): State<App>,
    RoutePath((id, action)): RoutePath<(i64, String)>,
) -> Api<Json<Value>> {
    Ok(Json(job_json(
        app.db(move |db| Ok(db.update_job(id, &action)?)).await?,
    )))
}

#[derive(Deserialize)]
struct RangeQuery {
    lo: u64,
    hi: u64,
    revision: Option<i64>,
    after: Option<u64>,
    cursor: Option<u64>,
    limit: Option<usize>,
    buckets: Option<usize>,
}

async fn page(State(app): State<App>, Query(q): Query<RangeQuery>) -> Api<Json<Value>> {
    let limit = q.limit.unwrap_or(100);
    if app.public_db.is_some() && limit > PUBLIC_PRIME_LIMIT {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("Public page limit must not exceed {PUBLIC_PRIME_LIMIT}"),
        ));
    }
    let result = app
        .db(move |db| Ok(db.page(Span { lo: q.lo, hi: q.hi }, q.after, limit, q.revision)?))
        .await?;
    Ok(Json(
        json!({ "revision":result.snapshot.to_string(), "primes":result.primes, "next_after":result.next_after }),
    ))
}

async fn summary(State(app): State<App>, Query(q): Query<RangeQuery>) -> Api<Json<Value>> {
    let buckets = q.buckets.unwrap_or(64);
    if app.public_db.is_some() && buckets > PUBLIC_BUCKET_LIMIT {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("Public bucket count must not exceed {PUBLIC_BUCKET_LIMIT}"),
        ));
    }
    let result = app
        .db(move |db| Ok(db.summary(Span { lo: q.lo, hi: q.hi }, buckets, q.revision, q.cursor)?))
        .await?;
    Ok(Json(
        json!({ "revision":result.snapshot.to_string(), "buckets":result.bins, "next_cursor":result.next_after }),
    ))
}

async fn export(State(app): State<App>, Query(q): Query<RangeQuery>) -> Api<Response> {
    let span = Span { lo: q.lo, hi: q.hi };
    let revision = app
        .db(move |db| Ok(db.verify_full(span, q.revision)?))
        .await?;
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<String>>(2);
    tokio::spawn(async move {
        let mut after = None;
        loop {
            let page = app
                .db(move |db| Ok(db.page(span, after, 4096, Some(revision))?))
                .await;
            let page = match page {
                Ok(page) => page,
                Err(error) => {
                    let _ = tx.send(Err(std::io::Error::other(error.1))).await;
                    break;
                }
            };
            if !page.primes.is_empty()
                && tx
                    .send(Ok(format!("{}\n", page.primes.join("\n"))))
                    .await
                    .is_err()
            {
                break;
            }
            let Some(cursor) = page.next_after else { break };
            after = cursor.parse::<u64>().ok();
            if after.is_none() {
                break;
            }
        }
    });
    Ok((
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=primes.txt",
            ),
        ],
        Body::from_stream(ReceiverStream::new(rx)),
    )
        .into_response())
}

#[derive(Deserialize)]
struct GapRangeQuery {
    lo: u64,
    hi: u64,
    revision: i64,
}

async fn gap_status(
    State(app): State<App>,
    Query(q): Query<GapRangeQuery>,
) -> Api<Json<GapStatus>> {
    let public = app.public_db.is_some();
    app.db(move |db| {
        let mut status = db.gap_status(Span { lo: q.lo, hi: q.hi }, q.revision)?;
        if public && status.state != "complete" {
            status = GapStatus {
                state: "missing",
                report: None,
                error: None,
            };
        }
        Ok(Json(status))
    })
    .await
}

#[derive(Deserialize)]
struct GapRequest {
    #[serde(flatten)]
    span: Span,
    revision: String,
}

async fn queue_gaps(
    State(app): State<App>,
    Json(request): Json<GapRequest>,
) -> Api<Json<GapStatus>> {
    let revision = request
        .revision
        .parse::<i64>()
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "Invalid archive revision".into()))?;
    let storage = app.clone();
    app.db(move |db| {
        let status = db.queue_gaps(request.span, revision, || {
            storage.gap_space(storage.shared.config.reserve_bytes + 1)
        });
        storage.gap_write(status).map(Json)
    })
    .await
}

#[derive(Serialize, Deserialize)]
pub struct WorkerPing {
    pub worker: String,
    pub slots: u16,
    pub token: Option<String>,
}

async fn claim(State(app): State<App>, Json(ping): Json<WorkerPing>) -> Api<Json<Value>> {
    app.record_worker(&ping.worker, ping.slots)?;
    let storage = app.clone();
    let result = app
        .db(move |db| {
            if storage.shared.storage_fault.load(Ordering::Relaxed)
                || storage.storage()?.1 <= storage.shared.config.reserve_bytes
            {
                return Ok(None);
            }
            Ok(db.claim(&ping.worker)?)
        })
        .await?;
    Ok(Json(json!(result)))
}

async fn heartbeat(State(app): State<App>, Json(ping): Json<WorkerPing>) -> Api<Json<Value>> {
    app.record_worker(&ping.worker, ping.slots)?;
    let token = ping.token.ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Lease token required".into(),
    ))?;
    app.db(move |db| Ok(db.heartbeat(&token, &ping.worker)?))
        .await?;
    Ok(Json(json!({"ok":true})))
}

async fn claim_gaps(
    State(app): State<App>,
    Json(ping): Json<WorkerPing>,
) -> Api<Json<Option<GapLease>>> {
    app.record_worker(&ping.worker, ping.slots)?;
    let storage = app.clone();
    app.db(move |db| {
        if storage
            .gap_space(storage.shared.config.reserve_bytes + 1)
            .is_err()
        {
            return Ok(Json(None));
        }
        storage.gap_write(db.claim_gaps(&ping.worker)).map(Json)
    })
    .await
}

#[derive(Deserialize)]
struct GapBlockQuery {
    token: String,
    worker: String,
    after: Option<u64>,
}

async fn gap_blocks(State(app): State<App>, Query(q): Query<GapBlockQuery>) -> Api<Json<GapPage>> {
    app.db(move |db| Ok(Json(db.gap_blocks(&q.worker, &q.token, q.after)?)))
        .await
}

async fn heartbeat_gaps(State(app): State<App>, Json(ping): Json<WorkerPing>) -> Api<Json<Value>> {
    app.record_worker(&ping.worker, ping.slots)?;
    let token = ping.token.ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Lease token required".into(),
    ))?;
    let storage = app.clone();
    app.db(move |db| storage.gap_write(db.heartbeat_gaps(&ping.worker, &token)))
        .await?;
    Ok(Json(json!({"ok":true})))
}

#[derive(Deserialize)]
struct GapUpload {
    worker: String,
    token: String,
    report: GapReport,
}

async fn commit_gaps(State(app): State<App>, Json(upload): Json<GapUpload>) -> Api<Json<Value>> {
    let storage = app.clone();
    app.db(move |db| {
        let result = db.commit_gaps(&upload.worker, &upload.token, &upload.report, |len| {
            storage.gap_space(4 * (1 << 20) + len as u64 * 8)
        });
        storage.gap_write(result)
    })
    .await?;
    Ok(Json(json!({"ok":true})))
}

#[derive(Deserialize)]
struct GapFailure {
    worker: String,
    token: String,
    error: String,
}

async fn fail_gaps(State(app): State<App>, Json(failure): Json<GapFailure>) -> Api<Json<Value>> {
    let storage = app.clone();
    app.db(move |db| {
        storage.gap_write(db.fail_gaps(&failure.worker, &failure.token, &failure.error))
    })
    .await?;
    Ok(Json(json!({"ok":true})))
}

#[derive(Serialize, Deserialize)]
pub struct Upload {
    pub token: String,
    pub worker: String,
    pub span: Span,
    pub count: u64,
    pub payload: String,
    pub engine: String,
    pub checksum: String,
}

fn decode_upload(upload: Upload) -> Api<Commit> {
    let payload = STANDARD
        .decode(upload.payload)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "Invalid base64 payload".into()))?;
    Ok(Commit {
        token: upload.token,
        worker: upload.worker,
        span: upload.span,
        count: upload.count,
        payload,
        engine: upload.engine,
        checksum: Some(upload.checksum),
    })
}

async fn commit(State(app): State<App>, Json(upload): Json<Upload>) -> Api<Json<Value>> {
    commit_uploads(app, vec![upload]).await
}

async fn commit_batch(
    State(app): State<App>,
    Json(uploads): Json<Vec<Upload>>,
) -> Api<Json<Value>> {
    if !(1..=16).contains(&uploads.len()) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Expected 1 to 16 blocks".into(),
        ));
    }
    commit_uploads(app, uploads).await
}

async fn commit_uploads(app: App, uploads: Vec<Upload>) -> Api<Json<Value>> {
    let permit = app
        .shared
        .uploads
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Archive uploads are busy. Try again shortly.".into(),
            )
        })?;
    let (batch, permit) = tokio::task::spawn_blocking(move || -> Api<_> {
        let requests = uploads
            .into_iter()
            .map(decode_upload)
            .collect::<Api<Vec<_>>>()?;
        Ok((PreparedBatch::prepare(requests)?, permit))
    })
    .await
    .map_err(internal)??;
    let storage = app.clone();
    app.db(move |db| {
        let _permit = permit;
        let result = db.commit_prepared_batch_with_guard(batch, |pending_size| {
            let required = 4 * (1 << 20) + pending_size as u64 * 8;
            if storage.shared.storage_fault.load(Ordering::Relaxed)
                || storage.storage().map_err(|_| ArchiveError::StoragePaused)?.1 < required
            {
                return Err(ArchiveError::StoragePaused);
            }
            Ok(())
        });
        if matches!(&result, Err(ArchiveError::Database(rusqlite::Error::SqliteFailure(error, _)))
            if matches!(error.code, rusqlite::ErrorCode::DiskFull | rusqlite::ErrorCode::SystemIoFailure))
        {
            storage.shared.storage_fault.store(true, Ordering::Relaxed);
            return Err(ArchiveError::StoragePaused.into());
        }
        Ok(result?)
    }).await?;
    Ok(Json(json!({"ok":true})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use prime_finder::{archive::encode, engine::Engine};
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;

    const ADMIN: &str = "Basic test-admin";
    const WORKER: &str = "Bearer test-worker";

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let mut token = [0; 16];
            getrandom::fill(&mut token).unwrap();
            let path =
                std::env::temp_dir().join(format!("prime-api-{:032x}", u128::from_be_bytes(token)));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_app(dir: &TestDir, quota_bytes: u64, reserve_bytes: u64) -> App {
        let archive = Archive::open(&dir.0.join("archive.sqlite")).unwrap();
        let config = Config {
            bind: "127.0.0.1:8787".parse().unwrap(),
            public_bind: None,
            contribute_bind: None,
            archive_dir: dir.0.clone(),
            web_root: dir.0.clone(),
            quota_bytes,
            reserve_bytes,
            mount_id: None,
        };
        App {
            shared: Arc::new(Shared {
                archive: Mutex::new(archive),
                uploads: Arc::new(Semaphore::new(8)),
                config,
                admin: ADMIN.into(),
                worker: WORKER.into(),
                workers: Mutex::new(BTreeMap::new()),
                storage_fault: AtomicBool::new(false),
            }),
            public_db: None,
        }
    }

    fn router(dir: &TestDir, quota_bytes: u64, reserve_bytes: u64) -> Router {
        routes(test_app(dir, quota_bytes, reserve_bytes), dir.0.clone())
    }

    async fn send(
        app: &Router,
        method: Method,
        path: &str,
        auth: &str,
        data: Value,
    ) -> (StatusCode, String) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .method(method)
                    .header(header::AUTHORIZATION, auth)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(data.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 10 << 20).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn post(app: &Router, path: &str, auth: &str, data: Value) -> Value {
        let (status, body) = send(app, Method::POST, path, auth, data).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_str(&body).unwrap()
    }

    async fn get(app: &Router, path: &str) -> Value {
        let (status, body) = send(app, Method::GET, path, ADMIN, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_str(&body).unwrap()
    }

    #[tokio::test]
    async fn evidence_campaign_requires_private_admin_and_matching_archive_job() {
        let dir = TestDir::new();
        let app = test_app(&dir, 50_000_000, 5_000_000);
        let contributions = crate::contribute::open(
            &dir.0.join("contributions.sqlite"),
            "test-key".into(),
            "test-secret".into(),
        )
        .unwrap();
        let public = crate::contribute::public_routes(contributions.clone());
        let private = private_router(app.clone(), dir.0.clone(), Some(contributions));
        let path = "/api/v1/contribute/admin/evidence/campaign";
        assert_eq!(
            send(&public, Method::POST, path, ADMIN, json!({"job_id":1}))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            send(&private, Method::POST, path, WORKER, json!({"job_id":1}))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            send(&private, Method::POST, path, ADMIN, json!({"job_id":1}))
                .await
                .0,
            StatusCode::NOT_FOUND
        );

        let width = prime_finder::block_codec::BLOCK_WIDTH;
        let hi = u64::MAX;
        let lo = hi - width + 1;
        let job = app
            .db(move |archive| Ok(archive.create_job(Span { lo, hi })?))
            .await
            .unwrap();
        let configured = post(&private, path, ADMIN, json!({"job_id":job.id})).await;
        assert_eq!(configured["span"], serde_json::to_value(job.span).unwrap());
        let status = get(&private, "/api/v1/contribute/admin/evidence/status").await;
        assert_eq!(status["campaign"]["job_id"], job.id);
        assert_eq!(status["used_blocks"], 0);
        assert_eq!(
            send(
                &private,
                Method::POST,
                "/api/v1/worker/contribute/evidence",
                WORKER,
                serde_json::to_value(job.span).unwrap()
            )
            .await
            .0,
            StatusCode::OK
        );
        let span = Span { lo: lo + 1, hi };
        let unaligned = app
            .db(move |archive| Ok(archive.create_job(span)?))
            .await
            .unwrap();
        post(&private, path, ADMIN, json!({"job_id":job.id,"close":true})).await;
        assert_eq!(
            send(
                &private,
                Method::POST,
                path,
                ADMIN,
                json!({"job_id":unaligned.id})
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn public_routes_expose_only_read_only_views() {
        let dir = TestDir::new();
        for leaf in [
            "index.html",
            "primes/index.html",
            "riemann/index.html",
            "assets/lab.js",
            "data/math-lab-v1.json",
            "private.txt",
        ] {
            let file = dir.0.join(leaf);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(file, leaf).unwrap();
        }
        let app = test_app(&dir, 50_000_000, 5_000_000);
        let public = public_routes(app.clone(), dir.0.clone());
        let private = routes(app, dir.0.clone());
        for (path, expected) in [
            ("/", "index.html"),
            ("/primes/", "primes/index.html"),
            ("/riemann/", "riemann/index.html"),
            ("/assets/lab.js", "assets/lab.js"),
            ("/data/math-lab-v1.json", "data/math-lab-v1.json"),
            ("/healthz", "ready"),
        ] {
            let (status, body) = send(&public, Method::GET, path, "", Value::Null).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(body, expected);
            let (status, body) = send(&public, Method::HEAD, path, "", Value::Null).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert!(body.is_empty());
        }
        let (status, body) = send(
            &public,
            Method::GET,
            "/api/v1/capabilities",
            "",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            json!({"mode":"public"})
        );
        assert_eq!(
            get(&private, "/api/v1/capabilities").await,
            json!({"mode":"private"})
        );

        let response = public
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response.headers().get(header::WWW_AUTHENTICATE).is_none());

        for (method, path, auth) in [
            (Method::GET, "/private.txt", ""),
            (Method::GET, "/archive.sqlite", ""),
            (Method::GET, "/api/v1/jobs/1", ADMIN),
            (Method::GET, "/api/v1/export?lo=1&hi=7", ADMIN),
            (Method::POST, "/api/v1/jobs", ADMIN),
            (Method::POST, "/api/v1/worker/claim", WORKER),
            (Method::POST, "/api/v1/worker/commit", WORKER),
        ] {
            let (status, _) = send(&public, method, path, auth, json!({"lo":"1","hi":"7"})).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        }
        for (path, expected) in [
            ("/api/v1/gaps", StatusCode::METHOD_NOT_ALLOWED),
            ("/primes/", StatusCode::METHOD_NOT_ALLOWED),
        ] {
            let (status, _) = send(
                &public,
                Method::POST,
                path,
                ADMIN,
                json!({"lo":"1","hi":"7","revision":"0"}),
            )
            .await;
            assert_eq!(status, expected, "{path}");
        }
        let (status, _) = send(
            &private,
            Method::GET,
            "/api/v1/capabilities",
            "",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(get(&private, "/api/v1/status").await["jobs"], json!([]));

        for path in [
            "/api/v1/primes?lo=1&hi=7&limit=4225",
            "/api/v1/summary?lo=1&hi=7&buckets=160",
            "/api/v1/gaps?lo=1&hi=7&revision=0",
        ] {
            let (status, _) = send(&public, Method::GET, path, "", Value::Null).await;
            assert_eq!(status, StatusCode::OK, "{path}");
        }
        for path in [
            "/api/v1/primes?lo=1&hi=7&limit=4226",
            "/api/v1/summary?lo=1&hi=7&buckets=161",
        ] {
            let (status, _) = send(&public, Method::GET, path, "", Value::Null).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            let (status, _) = send(&private, Method::GET, path, ADMIN, Value::Null).await;
            assert_eq!(status, StatusCode::OK, "{path}");
        }
    }

    #[tokio::test]
    async fn cancelled_public_request_keeps_db_slot_until_blocking_work_finishes() {
        let dir = TestDir::new();
        let mut app = test_app(&dir, 50_000_000, 5_000_000);
        let semaphore = Arc::new(Semaphore::new(1));
        app.public_db = Some(semaphore.clone());
        let (started, wait_for_start) = tokio::sync::oneshot::channel();
        let (finish, wait_for_finish) = tokio::sync::oneshot::channel();
        let task_app = app.clone();
        let task = tokio::spawn(async move {
            task_app
                .db(move |_| {
                    started.send(()).unwrap();
                    wait_for_finish.blocking_recv().unwrap();
                    Ok(())
                })
                .await
        });
        wait_for_start.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let error = app.db(|_| Ok(())).await.unwrap_err();
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        finish.send(()).unwrap();
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(5), semaphore.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn range_job_upload_restart_and_export() {
        let dir = TestDir::new();
        let app = router(&dir, 50_000_000, 5_000_000);
        let query = "lo=18446744073709551556&hi=18446744073709551615";
        let (status, _) = send(&app, Method::GET, "/api/v1/status", WORKER, Value::Null).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let job = post(
            &app,
            "/api/v1/jobs",
            ADMIN,
            json!({"lo":"18446744073709551556","hi":"18446744073709551615"}),
        )
        .await;
        assert_eq!(job["id"], "1");
        assert_eq!(job["width"], "60");
        let ping = json!({"worker":"first","slots":1});
        post(&app, "/api/v1/jobs/1/pause", ADMIN, Value::Null).await;
        assert!(
            post(&app, "/api/v1/worker/claim", WORKER, ping.clone())
                .await
                .is_null()
        );
        post(&app, "/api/v1/jobs/1/resume", ADMIN, Value::Null).await;
        let old_lease = post(&app, "/api/v1/worker/claim", WORKER, ping).await;
        assert_eq!(old_lease["blocks"][0]["hi"], "18446744073709551615");
        let (status, _) = send(
            &app,
            Method::GET,
            &format!("/api/v1/export?{query}"),
            ADMIN,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        drop(app);
        let app = router(&dir, 50_000_000, 5_000_000);
        let lease = post(
            &app,
            "/api/v1/worker/claim",
            WORKER,
            json!({"worker":"replacement","slots":1}),
        )
        .await;
        assert_ne!(old_lease["token"], lease["token"]);
        let span: Span = serde_json::from_value(lease["blocks"][0].clone()).unwrap();
        let primes = Engine::new(2).enumerate(span.lo, span.hi);
        let payload = encode(span, &primes).unwrap();
        let upload = json!({"worker":"replacement","token":lease["token"],"span":span,
            "count":primes.len(),"payload":STANDARD.encode(&payload),
            "checksum":format!("{:x}", Sha256::digest(&payload)), "engine":"test"});
        post(&app, "/api/v1/worker/commit", WORKER, upload.clone()).await;
        post(&app, "/api/v1/worker/commit", WORKER, upload).await;
        let status = get(&app, "/api/v1/status").await;
        assert_eq!(status["jobs"][0]["state"], "complete");
        assert_eq!(status["jobs"][0]["count"], "1");
        let page = get(&app, &format!("/api/v1/primes?{query}")).await;
        assert_eq!(page["primes"], json!(["18446744073709551557"]));
        let summary = get(&app, &format!("/api/v1/summary?{query}&buckets=2")).await;
        assert_eq!(summary["buckets"][0]["covered"], "30");
        assert_eq!(summary["buckets"][0]["count"], "1");
        assert_eq!(summary["buckets"][1]["count"], "0");
        let (status, body) = send(
            &app,
            Method::GET,
            &format!("/api/v1/export?{query}"),
            ADMIN,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "18446744073709551557\n");
    }

    #[tokio::test]
    async fn storage_reserve_blocks_new_work_without_losing_the_job() {
        let dir = TestDir::new();
        let app = router(&dir, 60_000, 59_999);
        post(&app, "/api/v1/jobs", ADMIN, json!({"lo":"2","hi":"100"})).await;
        assert!(
            post(
                &app,
                "/api/v1/worker/claim",
                WORKER,
                json!({"worker":"w","slots":1})
            )
            .await
            .is_null()
        );
        let status = get(&app, "/api/v1/status").await;
        assert_eq!(status["storage"]["paused"], true);
        assert_eq!(status["jobs"][0]["covered"], "0");
        drop(app);
        let app = router(&dir, 50_000_000, 5_000_000);
        let lease = post(
            &app,
            "/api/v1/worker/claim",
            WORKER,
            json!({"worker":"w","slots":1}),
        )
        .await;
        assert_eq!(lease["blocks"], json!([{"lo":"2","hi":"100"}]));
    }

    #[tokio::test]
    async fn gap_reports_pin_coverage_and_recover_worker_leases() {
        let dir = TestDir::new();
        let app = router(&dir, 50_000_000, 5_000_000);
        let ping = json!({"worker":"mac","slots":1});
        let engine = Engine::new(1);
        for (lo, hi) in [(0, 2), (3, 7), (8, 10), (11, 13), (14, 16), (17, 20)] {
            let span = Span { lo, hi };
            post(&app, "/api/v1/jobs", ADMIN, json!(span)).await;
            let lease = post(&app, "/api/v1/worker/claim", WORKER, ping.clone()).await;
            let primes = engine.enumerate(lo, hi);
            let payload = encode(span, &primes).unwrap();
            post(
                &app,
                "/api/v1/worker/commit",
                WORKER,
                json!({"worker":"mac","token":lease["token"],"span":span,
                    "count":primes.len(),"payload":STANDARD.encode(&payload),
                    "checksum":format!("{:x}", Sha256::digest(&payload)), "engine":"test"}),
            )
            .await;
        }

        let status_path = "/api/v1/gaps?lo=1&hi=18&revision=6";
        let request = json!({"lo":"1","hi":"18","revision":"6"});
        assert_eq!(get(&app, status_path).await["state"], "missing");
        let (status, _) = send(
            &app,
            Method::POST,
            "/api/v1/gaps",
            ADMIN,
            json!({"lo":"1","hi":"18","revision":"5"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        for _ in 0..2 {
            assert_eq!(
                post(&app, "/api/v1/gaps", ADMIN, request.clone()).await["state"],
                "queued"
            );
        }

        let claim = "/api/v1/worker/gaps/claim";
        let lease = post(&app, claim, WORKER, ping.clone()).await;
        assert_eq!(post(&app, claim, WORKER, ping.clone()).await, lease);
        assert_eq!(lease["prime_count"], "7");
        assert_eq!(get(&app, status_path).await["state"], "running");
        drop(app);

        let shared = test_app(&dir, 50_000_000, 5_000_000);
        let public = public_routes(shared.clone(), dir.0.clone());
        let app = routes(shared, dir.0.clone());
        assert_eq!(get(&app, status_path).await["state"], "queued");
        assert_eq!(get(&public, status_path).await, json!({"state":"missing"}));
        let blocks = "/api/v1/worker/gaps/blocks";
        let old_token = lease["token"].as_str().unwrap();
        let (status, _) = send(
            &app,
            Method::GET,
            &format!("{blocks}?worker=mac&token={old_token}"),
            WORKER,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let lease = post(&app, claim, WORKER, ping.clone()).await;
        let token = lease["token"].as_str().unwrap();
        assert_eq!(get(&public, status_path).await, json!({"state":"missing"}));
        let block_url = format!("{blocks}?worker=mac&token={token}");
        let (status, _) = send(&app, Method::GET, &block_url, ADMIN, Value::Null).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, body) = send(&app, Method::GET, &block_url, WORKER, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let page: GapPage = serde_json::from_str(&body).unwrap();
        assert_eq!(
            page.blocks.iter().map(|block| block.lo).collect::<Vec<_>>(),
            [0, 3, 8, 11]
        );
        assert_eq!(page.blocks[2].count, 0);
        let (status, body) = send(
            &app,
            Method::GET,
            &format!("{block_url}&after={}", page.next_after.unwrap()),
            WORKER,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let page: GapPage = serde_json::from_str(&body).unwrap();
        assert_eq!(
            page.blocks.iter().map(|block| block.lo).collect::<Vec<_>>(),
            [14, 17]
        );
        assert_eq!(page.blocks[1].hi, 20);
        assert!(page.next_after.is_none());

        let fail = "/api/v1/worker/gaps/fail";
        let failure = json!({"worker":"mac","token":token,"error":"interrupted"});
        for _ in 0..2 {
            post(&app, fail, WORKER, failure.clone()).await;
        }
        assert_eq!(get(&app, status_path).await["error"], "interrupted");
        assert_eq!(get(&public, status_path).await, json!({"state":"missing"}));
        assert_eq!(
            post(&app, "/api/v1/gaps", ADMIN, request.clone()).await["state"],
            "queued"
        );
        let lease = post(&app, claim, WORKER, ping).await;
        assert_ne!(lease["token"], token);
        let report = json!({"prime_count":"7","gap_count":"6","twin_count":"3",
            "histogram":[{"gap":"1","count":"1"},{"gap":"2","count":"3"},{"gap":"4","count":"2"}],
            "largest":[{"gap":"4","left":"7","right":"11"},{"gap":"4","left":"13","right":"17"},
                {"gap":"2","left":"3","right":"5"},{"gap":"2","left":"5","right":"7"},
                {"gap":"2","left":"11","right":"13"},{"gap":"1","left":"2","right":"3"}]});
        let commit = "/api/v1/worker/gaps/commit";
        let upload = json!({"worker":"mac","token":lease["token"],"report":report});
        let mut bad = upload.clone();
        bad["report"]["prime_count"] = json!("8");
        assert_eq!(
            send(&app, Method::POST, commit, WORKER, bad).await.0,
            StatusCode::BAD_REQUEST
        );
        for _ in 0..2 {
            post(&app, commit, WORKER, upload.clone()).await;
        }
        let complete = json!({"state":"complete","report":report});
        assert_eq!(post(&app, "/api/v1/gaps", ADMIN, request).await, complete);
        assert_eq!(get(&public, status_path).await, complete);
        drop(public);
        drop(app);
        let app = router(&dir, 50_000_000, 5_000_000);
        assert_eq!(get(&app, status_path).await, complete);
    }
}
