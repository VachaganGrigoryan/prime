use axum::{
    Router,
    extract::{OriginalUri, State},
    http::header,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::{Parser, Subcommand};
use prime_finder::{
    block_codec::{self, BLOCK_WIDTH},
    engine::Engine,
    evidence,
    mining_protocol::{self, BENCHMARK_VERSION},
};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fmt, fs,
    io::{self, IsTerminal, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time;

const DEFAULT_SERVER: &str = "https://prime-api.notallow.cc";
const API: &str = "/api/v1/mining";
const HEARTBEAT: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_TOKEN_BYTES: usize = 192;
const MAX_WORKER_RESPONSE: usize = 32 * 1024;
const MAX_UI_RESPONSE: usize = 512 * 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(version, about = "Join the Prime search or explore its progress")]
struct Args {
    /// Prime coordinator API origin. HTTP is allowed on loopback for development.
    #[arg(long, global = true, default_value = DEFAULT_SERVER, value_parser = server_origin)]
    server: Url,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Enroll, benchmark, and contribute approved work. Stop with Ctrl+C.
    Worker,
    /// Serve the public Prime dashboard on this machine.
    Ui {
        /// Local dashboard port.
        #[arg(long, default_value_t = 8786)]
        port: u16,
    },
}

fn server_origin(value: &str) -> std::result::Result<Url, String> {
    let url = Url::parse(value).map_err(|_| "enter a valid API origin".to_owned())?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
        || !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("use an HTTPS origin, or an HTTP loopback origin for local development".into());
    }
    Ok(url)
}

#[derive(Clone)]
struct Client {
    http: reqwest::Client,
    server: Url,
}

impl Client {
    fn new(server: Url) -> Result<Self> {
        let builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT);
        let http = if server.scheme() == "https" {
            builder.https_only(true)
        } else {
            builder.no_proxy()
        }
        .build()?;
        Ok(Self { http, server })
    }

    async fn worker_request<T: Serialize, R: DeserializeOwned>(
        &self,
        url: &str,
        token: Option<&str>,
        body: Option<&T>,
    ) -> std::result::Result<R, ApiError> {
        let url = self
            .server
            .join(&format!("{API}/{url}"))
            .map_err(|_| ApiError::InvalidResponse)?;
        let mut request = if let Some(body) = body {
            self.http.post(url).json(body)
        } else {
            self.http.get(url)
        };
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(ApiError::transport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::Http(status));
        }
        let body = read_response(response, MAX_WORKER_RESPONSE).await?;
        serde_json::from_slice(&body).map_err(|_| ApiError::InvalidResponse)
    }
}

#[derive(Debug)]
enum ApiError {
    Transport(reqwest::Error),
    Http(StatusCode),
    InvalidResponse,
}

impl ApiError {
    fn transport(error: reqwest::Error) -> Self {
        Self::Transport(error.without_url())
    }

    fn retry(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Http(status) => {
                status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
            Self::InvalidResponse => false,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "coordinator connection failed: {error}"),
            Self::Http(status) => write!(formatter, "coordinator returned HTTP {status}"),
            Self::InvalidResponse => formatter.write_str("invalid coordinator response"),
        }
    }
}

impl std::error::Error for ApiError {}

async fn read_response(
    mut response: reqwest::Response,
    limit: usize,
) -> std::result::Result<Vec<u8>, ApiError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(ApiError::transport)? {
        if chunk.len() > limit - body.len() {
            return Err(ApiError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn credential_path(server: &Url) -> Result<PathBuf> {
    #[cfg(windows)]
    let state_dir = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
    #[cfg(not(windows))]
    let state_dir = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| std::env::home_dir().map(|home| home.join(".local/state")));
    let state_dir = state_dir.ok_or("could not find a state directory for Prime")?;
    let origin = format!(
        "{:x}",
        Sha256::digest(server.origin().ascii_serialization())
    );
    Ok(state_dir.join("prime").join(format!("{origin}.credential")))
}

fn validate_token(input: &str) -> Result<&str> {
    let token = input.trim_ascii();
    if !(16..=MAX_TOKEN_BYTES).contains(&token.len())
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
    {
        return Err("invalid Prime credential or enrollment code".into());
    }
    Ok(token)
}

fn load_credential(path: &Path) -> Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() > MAX_TOKEN_BYTES as u64 {
        return Err("invalid Prime credential file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("Prime credential file must be readable only by its owner".into());
        }
    }
    let mut input = String::new();
    fs::File::open(path)?
        .take((MAX_TOKEN_BYTES + 1) as u64)
        .read_to_string(&mut input)?;
    Ok(Some(validate_token(&input)?.into()))
}

fn save_credential(path: &Path, token: &str) -> Result<()> {
    let token = validate_token(token)?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let pending = path.with_extension(format!("pending-{}", std::process::id()));
    let mut file = options.open(&pending)?;
    let write = file
        .write_all(token.as_bytes())
        .and_then(|()| file.sync_all());
    drop(file);
    // Publish a complete credential and preserve an existing registration if
    // two instances exchanged enrollment codes at the same time.
    let result = write.and_then(|()| fs::hard_link(&pending, path));
    let _ = fs::remove_file(&pending);
    Ok(result?)
}

fn open_browser(url: &Url) {
    let program = match std::env::consts::OS {
        "macos" => "open",
        "windows" => "explorer.exe",
        "linux" => "xdg-open",
        _ => return,
    };
    let launch = std::process::Command::new(program)
        .arg(url.as_str())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    if let Ok(mut child) = launch {
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
}

fn read_code() -> Result<String> {
    if io::stdin().is_terminal() {
        let code = rpassword::prompt_password("Paste the one-time enrollment code: ")?;
        return Ok(validate_token(&code)?.into());
    }
    let mut input = String::new();
    io::stdin()
        .take((MAX_TOKEN_BYTES + 3) as u64)
        .read_to_string(&mut input)?;
    Ok(validate_token(&input)?.into())
}

#[derive(Deserialize)]
struct Exchange {
    token: String,
}

async fn credential(client: &Client, path: &Path) -> Result<String> {
    let token = if let Some(token) = load_credential(path)? {
        match client
            .worker_request::<(), Worker>("worker", Some(&token), None)
            .await
        {
            Ok(_) => return Ok(token),
            Err(ApiError::Http(StatusCode::UNAUTHORIZED)) => token,
            Err(error) => return Err(error.into()),
        }
    } else {
        fs::create_dir_all(path.parent().ok_or("invalid credential path")?)?;
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)?;
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        save_credential(path, &token)?;
        token
    };
    let enrollment = client.server.join("/join")?;
    println!("Enroll this worker at {enrollment}");
    if io::stdin().is_terminal() {
        open_browser(&enrollment);
    }
    let code = tokio::task::spawn_blocking(read_code).await??;
    let exchange: Exchange = client
        .worker_request(
            "exchange",
            None,
            Some(&json!({ "code": code, "token": token })),
        )
        .await?;
    if exchange.token != token {
        return Err("coordinator returned a different worker credential".into());
    }
    Ok(token)
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum WorkerState {
    Pending,
    Approved,
    Rejected,
    Revoked,
}

#[derive(Deserialize)]
struct BenchmarkStatus {
    passed: bool,
}

#[derive(Deserialize)]
struct Worker {
    state: WorkerState,
    benchmark: Option<BenchmarkStatus>,
}

#[derive(Serialize)]
struct Benchmark {
    version: u32,
    count: u64,
    digest: String,
    elapsed_ms: u64,
    platform: String,
    client_version: &'static str,
}

async fn run_benchmark(client: &Client, token: &str, engine: Arc<Engine>) -> Result<()> {
    println!("Running the Prime benchmark.");
    let (count, digest, elapsed_ms) = tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        let (count, digest) = mining_protocol::benchmark(&engine);
        let elapsed_ms = u64::try_from(start.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        (count, digest, elapsed_ms)
    })
    .await?;
    let report = Benchmark {
        version: BENCHMARK_VERSION,
        count,
        digest,
        elapsed_ms,
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        client_version: env!("CARGO_PKG_VERSION"),
    };
    loop {
        match client
            .worker_request::<_, Worker>("benchmark", Some(token), Some(&report))
            .await
        {
            Ok(worker)
                if worker
                    .benchmark
                    .as_ref()
                    .is_some_and(|result| result.passed) =>
            {
                println!("Benchmark accepted. Waiting for admin approval.");
                return Ok(());
            }
            Ok(_) => {
                return Err(
                    "Atlas did not accept the benchmark. Restart the worker to retry.".into(),
                );
            }
            Err(error) if error.retry() => {
                eprintln!("{error}. Retrying the benchmark submission.");
                time::sleep(POLL).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[derive(Deserialize)]
struct Span {
    lo: String,
    hi: String,
}

#[derive(Deserialize)]
struct Lease {
    token: String,
    blocks: Vec<Span>,
}

impl Lease {
    fn bounds(&self) -> Result<(u64, u64)> {
        let [span] = self.blocks.as_slice() else {
            return Err("Atlas offered an invalid number of work blocks".into());
        };
        let lo = span.lo.parse::<u64>()?;
        let hi = span.hi.parse::<u64>()?;
        validate_token(&self.token)?;
        if lo < 2 || hi.checked_sub(lo).is_none_or(|width| width >= BLOCK_WIDTH) {
            return Err("Atlas offered an invalid work block".into());
        }
        Ok((lo, hi))
    }
}

#[derive(Serialize)]
struct Submission<'a> {
    lease: &'a str,
    kind: &'static str,
    payload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
}

#[derive(Deserialize)]
struct SubmissionStatus {
    accepted: bool,
}

struct Block {
    kind: &'static str,
    payload: String,
    count: Option<u64>,
}

fn compute_block(engine: &Engine, lo: u64, hi: u64) -> Result<Block> {
    if lo >= evidence::MIN_LO && lo.is_multiple_of(BLOCK_WIDTH) && hi - lo == BLOCK_WIDTH - 1 {
        let evidence = evidence::generate_with_certificates(engine, lo, hi)?;
        return Ok(Block {
            kind: evidence::KIND,
            payload: STANDARD.encode(evidence),
            count: None,
        });
    }
    let primes = engine.enumerate(lo, hi);
    let payload = block_codec::encode(lo, hi, &primes)?;
    Ok(Block {
        kind: "primes-v1",
        payload: STANDARD.encode(payload),
        count: Some(primes.len() as u64),
    })
}

async fn renew_lease(client: &Client, token: &str, lease: &str) -> Result<()> {
    let body = json!({ "lease": lease });
    let mut heartbeat = time::interval_at(time::Instant::now() + HEARTBEAT, HEARTBEAT);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        heartbeat.tick().await;
        match client
            .worker_request::<_, serde_json::Value>("heartbeat", Some(token), Some(&body))
            .await
        {
            Ok(_) => {}
            Err(ApiError::Http(StatusCode::CONFLICT | StatusCode::GONE)) => return Ok(()),
            Err(error) if error.retry() => eprintln!("{error}. Lease renewal will retry."),
            Err(error) => return Err(error.into()),
        }
    }
}

async fn submit(client: &Client, token: &str, block: Submission<'_>) -> Result<bool> {
    loop {
        match client
            .worker_request::<_, SubmissionStatus>("submit", Some(token), Some(&block))
            .await
        {
            Ok(status) => return Ok(status.accepted),
            Err(ApiError::Http(StatusCode::CONFLICT | StatusCode::GONE)) => return Ok(false),
            Err(ApiError::Http(StatusCode::UNPROCESSABLE_ENTITY)) => {
                eprintln!("Atlas found an invalid result and will reassign this block.");
                return Ok(false);
            }
            Err(error) if error.retry() => {
                eprintln!("{error}. Retrying block submission.");
                time::sleep(POLL).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn process_lease(
    client: &Client,
    token: &str,
    engine: Arc<Engine>,
    lease: Lease,
) -> Result<()> {
    let (lo, hi) = lease.bounds()?;
    let mut compute = tokio::task::spawn_blocking(move || compute_block(&engine, lo, hi));
    let renew = renew_lease(client, token, &lease.token);
    tokio::pin!(renew);
    let block = tokio::select! {
        result = &mut compute => result??,
        result = &mut renew => {
            compute.await??;
            return result;
        }
    };
    let submission = Submission {
        lease: &lease.token,
        kind: block.kind,
        payload: block.payload,
        count: block.count,
    };
    tokio::select! {
        biased;
        result = submit(client, token, submission) => {
            if result? {
                println!("Verified block {lo}–{hi}.");
            }
        }
        result = &mut renew => result?,
    }
    Ok(())
}

async fn claim_work(client: &Client, token: &str, engine: &Arc<Engine>) -> Result<()> {
    let empty = json!({});
    loop {
        match client
            .worker_request::<_, Option<Lease>>("claim", Some(token), Some(&empty))
            .await
        {
            Ok(Some(lease)) => process_lease(client, token, engine.clone(), lease).await?,
            Ok(None) | Err(ApiError::Http(StatusCode::FORBIDDEN | StatusCode::CONFLICT)) => {
                return Ok(());
            }
            Err(error) if error.retry() => {
                eprintln!("{error}. Waiting for work.");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn run_worker(client: Client, path: PathBuf, token: String) -> Result<()> {
    let engine = Arc::new(Engine::new(100_000_000));
    let mut last_state = None;
    loop {
        let worker = match client
            .worker_request::<(), Worker>("worker", Some(&token), None)
            .await
        {
            Ok(worker) => worker,
            Err(error) if error.retry() => {
                eprintln!("{error}. Reconnecting.");
                time::sleep(POLL).await;
                continue;
            }
            Err(ApiError::Http(StatusCode::UNAUTHORIZED)) => {
                return Err(format!(
                    "Atlas refused this credential. To enroll again, remove {} and restart.",
                    path.display()
                )
                .into());
            }
            Err(error) => return Err(error.into()),
        };
        let previous = last_state.replace(worker.state);
        match worker.state {
            WorkerState::Pending => {
                if worker.benchmark.is_none_or(|result| !result.passed) {
                    run_benchmark(&client, &token, engine.clone()).await?;
                } else if previous != Some(WorkerState::Pending) {
                    println!("Waiting for admin approval.");
                }
            }
            WorkerState::Approved => {
                if previous != Some(WorkerState::Approved) {
                    println!("Worker approved. Looking for the next block.");
                }
                claim_work(&client, &token, &engine).await?;
            }
            WorkerState::Rejected => return Err("Admin rejected this worker.".into()),
            WorkerState::Revoked => return Err("Admin revoked this worker.".into()),
        }
        time::sleep(POLL).await;
    }
}

async fn ui_proxy(State(client): State<Client>, OriginalUri(uri): OriginalUri) -> Response {
    let Ok(mut upstream) = client.server.join(uri.path()) else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    upstream.set_query(uri.query());
    let Ok(response) = client.http.get(upstream).send().await else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    let status = response.status();
    match read_response(response, MAX_UI_RESPONSE).await {
        Ok(body) => (status, [(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

fn ui_router(client: Client) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { axum::response::Html(include_str!("../client_assets/index.html")) }),
        )
        .route(
            "/style.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("../client_assets/style.css"),
                )
            }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("../client_assets/app.js"),
                )
            }),
        )
        .route("/api/v1/mining/progress", get(ui_proxy))
        .route("/api/v1/primes", get(ui_proxy))
        .route("/api/v1/summary", get(ui_proxy))
        .with_state(client)
}

async fn run_ui(client: Client, port: u16) -> Result<()> {
    let listener =
        tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port))).await?;
    println!("Prime dashboard at http://{}", listener.local_addr()?);
    axum::serve(listener, ui_router(client))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::new(args.server)?;
    match args.command {
        Command::Worker => {
            // Keep terminal input outside the async signal watcher. Blocking reads
            // cannot be interrupted by dropping the worker future.
            let path = credential_path(&client.server)?;
            let token = credential(&client, &path).await?;
            tokio::select! {
                result = run_worker(client, path, token) => result,
                signal = tokio::signal::ctrl_c() => Ok(signal?),
            }
        }
        Command::Ui { port } => run_ui(client, port).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_origin_restricts_cleartext_to_loopback() {
        for origin in [DEFAULT_SERVER, "http://127.0.0.1:8789", "http://[::1]:8789"] {
            assert!(server_origin(origin).is_ok());
        }
        for origin in [
            "http://prime-api.notallow.cc",
            "https://name:password@prime-api.notallow.cc",
            "https://prime-api.notallow.cc/join",
        ] {
            assert!(server_origin(origin).is_err());
        }
    }

    #[tokio::test]
    async fn local_ui_serves_only_the_dashboard_and_public_routes() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt;

        let client = Client::new(server_origin(DEFAULT_SERVER).unwrap()).unwrap();
        let router = ui_router(client);
        for (method, path, status) in [
            ("GET", "/", StatusCode::OK),
            ("GET", "/style.css", StatusCode::OK),
            ("GET", "/app.js", StatusCode::OK),
            ("GET", "/api/v1/mining/admin/workers", StatusCode::NOT_FOUND),
            (
                "POST",
                "/api/v1/mining/progress",
                StatusCode::METHOD_NOT_ALLOWED,
            ),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{method} {path}");
        }
    }

    #[test]
    fn work_bounds_preserve_u64_max() {
        let lease: Lease = serde_json::from_str(
            r#"{"token":"valid-test-lease-token","blocks":[{"lo":"18446744073709551615","hi":"18446744073709551615"}]}"#,
        ).unwrap();
        assert_eq!(lease.bounds().unwrap(), (u64::MAX, u64::MAX));
    }
}
