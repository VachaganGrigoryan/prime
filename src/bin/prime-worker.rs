use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::{Parser, Subcommand};
use prime_finder::{block_codec, engine::Engine, evidence};
use reqwest::{StatusCode, Url, redirect};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    io::{self, IsTerminal, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::time::{self, Instant};

const DEFAULT_SERVER: &str = "https://test-math-workers.notallow.cc";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const HEARTBEAT: Duration = Duration::from_secs(25);
const IDLE_POLL: Duration = Duration::from_secs(15);
const MAX_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_TOKEN_BYTES: usize = 192;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    name = "prime-worker",
    version,
    about = "Contribute exact prime blocks to Test Math"
)]
struct Args {
    /// HTTPS origin hosting enrollment and volunteer work.
    #[arg(long, global = true, default_value = DEFAULT_SERVER, value_parser = https_origin)]
    server: Url,
    /// Trust an additional PEM root for a private test deployment.
    #[arg(long, global = true, value_name = "PEM")]
    ca_certificate: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show where to enroll this computer and obtain its credential.
    Join,
    /// Process one leased prime block at a time. Press Ctrl+C to stop.
    Run {
        /// Generate only divisor evidence for the high-range benchmark.
        #[arg(long)]
        divisors_only: bool,
    },
}

fn https_origin(value: &str) -> std::result::Result<Url, String> {
    let url = Url::parse(value).map_err(|_| "enter a valid HTTPS origin".to_owned())?;
    if url.scheme() != "https"
        || !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "use an HTTPS origin without credentials, a path, a query, or a fragment".into(),
        );
    }
    Ok(url)
}

fn parse_token(input: &str) -> Result<String> {
    let token = input.trim_ascii();
    if !(16..=MAX_TOKEN_BYTES).contains(&token.len())
        || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err("enter the worker credential shown on the enrollment page".into());
    }
    Ok(token.to_owned())
}

fn read_token() -> Result<String> {
    if io::stdin().is_terminal() {
        let input = rpassword::prompt_password("Paste your worker credential: ")?;
        return parse_token(&input);
    }
    let mut input = String::new();
    io::stdin()
        .take((MAX_TOKEN_BYTES + 3) as u64)
        .read_to_string(&mut input)?;
    if input.len() > MAX_TOKEN_BYTES + 2 {
        return Err("worker credential is too long".into());
    }
    parse_token(&input)
}

#[derive(Deserialize)]
struct Lease {
    lease: String,
    lo: String,
    hi: String,
}

#[derive(Serialize)]
struct LeaseToken<'a> {
    lease: &'a str,
}

#[derive(Serialize)]
struct Submission<'a> {
    lease: &'a str,
    payload: String,
    checksum: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'static str>,
}

#[derive(Deserialize)]
struct Ack {
    ok: bool,
}

#[derive(Debug)]
enum ApiError {
    Transport(reqwest::Error),
    Status(StatusCode),
    Protocol(&'static str),
}

impl ApiError {
    fn retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status(code) => code.is_server_error() || *code == StatusCode::TOO_MANY_REQUESTS,
            Self::Protocol(_) => false,
        }
    }

    fn lease_lost(&self) -> bool {
        matches!(self, Self::Status(StatusCode::CONFLICT | StatusCode::GONE))
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "connection failed: {error}"),
            Self::Status(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) => formatter.write_str(
                "worker credential was refused; enroll again if it expired or was revoked",
            ),
            Self::Status(code) => write!(formatter, "coordinator returned HTTP {}", code.as_u16()),
            Self::Protocol(detail) => {
                write!(formatter, "unexpected coordinator response: {detail}")
            }
        }
    }
}

impl std::error::Error for ApiError {}

struct Client {
    client: reqwest::Client,
    server: Url,
    token: String,
}

impl Client {
    fn new(server: Url, token: String, ca_certificate: Option<&Path>) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .redirect(redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT);
        if let Some(path) = ca_certificate {
            let certificate = reqwest::Certificate::from_pem(&std::fs::read(path)?)?;
            builder = builder.tls_certs_merge([certificate]);
        }
        Ok(Self {
            client: builder.build()?,
            server,
            token,
        })
    }

    async fn post<T: Serialize, R: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &T,
    ) -> std::result::Result<R, ApiError> {
        let url = self
            .server
            .join(&format!("/api/v1/contribute/{endpoint}"))
            .map_err(|_| ApiError::Protocol("invalid endpoint"))?;
        let mut response = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|error| ApiError::Transport(error.without_url()))?;
        if !response.status().is_success() {
            return Err(ApiError::Status(response.status()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| ApiError::Transport(error.without_url()))?
        {
            if chunk.len() > MAX_RESPONSE_BYTES - bytes.len() {
                return Err(ApiError::Protocol("response exceeded the permitted size"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| ApiError::Protocol("invalid JSON"))
    }

    async fn acknowledge<T: Serialize>(
        &self,
        endpoint: &str,
        body: &T,
    ) -> std::result::Result<(), ApiError> {
        let ack: Ack = self.post(endpoint, body).await?;
        if !ack.ok {
            return Err(ApiError::Protocol("work was not acknowledged"));
        }
        Ok(())
    }
}

fn lease_bounds(lease: &Lease) -> Result<(u64, u64)> {
    let lo = lease.lo.parse::<u64>()?;
    let hi = lease.hi.parse::<u64>()?;
    if hi
        .checked_sub(lo)
        .is_none_or(|distance| distance >= block_codec::BLOCK_WIDTH)
        || lease.lease.is_empty()
        || lease.lease.len() > MAX_TOKEN_BYTES
        || !lease.lease.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err("coordinator offered an invalid block lease".into());
    }
    Ok((lo, hi))
}

#[derive(Clone, Copy)]
enum WorkMode {
    Evidence { divisors_only: bool },
    CrossCheck,
}

async fn compute_and_submit(
    client: &Client,
    engine: Arc<Engine>,
    lease: Lease,
    mode: WorkMode,
) -> Result<()> {
    let (lo, hi) = lease_bounds(&lease)?;
    let mut computation = tokio::task::spawn_blocking(move || -> Result<_> {
        let (payload, count) = match mode {
            WorkMode::Evidence {
                divisors_only: true,
            } => (evidence::generate(&engine, lo, hi)?, None),
            WorkMode::Evidence {
                divisors_only: false,
            } => (evidence::generate_with_certificates(&engine, lo, hi)?, None),
            WorkMode::CrossCheck => {
                let primes = engine.enumerate(lo, hi);
                (
                    block_codec::encode(lo, hi, &primes)?,
                    Some(primes.len() as u64),
                )
            }
        };
        let checksum = format!("{:x}", Sha256::digest(&payload));
        Ok((STANDARD.encode(payload), checksum, count))
    });
    let (renew_route, submit_route, kind) = match mode {
        WorkMode::Evidence { .. } => (
            "evidence/heartbeat",
            "evidence/submit",
            Some(evidence::KIND),
        ),
        WorkMode::CrossCheck => ("heartbeat", "submit", None),
    };
    let mut heartbeat = time::interval_at(Instant::now() + HEARTBEAT, HEARTBEAT);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let body = LeaseToken {
        lease: &lease.lease,
    };
    let (payload, checksum, count) = loop {
        tokio::select! {
            biased;
            result = &mut computation => break result??,
            _ = heartbeat.tick() => {
                if let Err(error) = client.acknowledge(renew_route, &body).await {
                    if error.lease_lost() {
                        // A blocking task cannot be cancelled. Let it finish before claiming again.
                        computation.await??;
                        eprintln!("Lease expired for {lo}..={hi}; waiting for more work.");
                        return Ok(());
                    }
                    if !error.retryable() {
                        computation.await??;
                        return Err(error.into());
                    }
                    eprintln!("Could not renew lease: {error}");
                }
            }
        }
    };
    let submission = Submission {
        lease: &lease.lease,
        payload,
        checksum,
        count,
        kind,
    };
    let mut retries = 0;
    loop {
        match client.acknowledge(submit_route, &submission).await {
            Ok(()) => {
                if let Some(count) = count {
                    println!("Submitted {count} primes for {lo}..={hi}; verification pending.");
                } else {
                    println!("Submitted evidence for {lo}..={hi}; verification pending.");
                }
                return Ok(());
            }
            Err(error) if error.lease_lost() => {
                eprintln!("Lease expired for {lo}..={hi}; waiting for more work.");
                return Ok(());
            }
            Err(error) if error.retryable() && retries < 2 => {
                eprintln!("Could not submit block: {error}; retrying.");
                time::sleep(Duration::from_secs(2_u64 << retries)).await;
                retries += 1;
            }
            Err(error) if error.retryable() => {
                eprintln!("Submission unavailable: {error}; retrying the work queue.");
                time::sleep(IDLE_POLL).await;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn run(client: Client, divisors_only: bool) -> Result<()> {
    let engine = Arc::new(Engine::new(100_000_000));
    loop {
        let empty = serde_json::json!({});
        let (lease, mode) = match client
            .post::<_, Option<Lease>>("evidence/claim", &empty)
            .await
        {
            Ok(None) | Err(ApiError::Status(StatusCode::NOT_FOUND)) => (
                client.post::<_, Option<Lease>>("claim", &empty).await,
                WorkMode::CrossCheck,
            ),
            candidate => (candidate, WorkMode::Evidence { divisors_only }),
        };
        match lease {
            Ok(Some(lease)) => compute_and_submit(&client, engine.clone(), lease, mode).await?,
            Ok(None) => time::sleep(IDLE_POLL).await,
            Err(error) if error.retryable() => {
                eprintln!("Could not fetch work: {error}; retrying.");
                time::sleep(IDLE_POLL).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Join => {
            let enrollment = args.server.join("/join")?;
            println!("Open {} to enroll this computer.", enrollment,);
            if cfg!(target_os = "macos") && io::stdout().is_terminal() {
                let _ = std::process::Command::new("open")
                    .arg(enrollment.as_str())
                    .status();
            }
            println!("Run prime-worker run and paste the credential when prompted.");
        }
        Command::Run { divisors_only } => {
            let client = Client::new(args.server, read_token()?, args.ca_certificate.as_deref())?;
            println!("Waiting for prime research work. Press Ctrl+C to stop.");
            tokio::select! {
                result = run(client, divisors_only) => result?,
                signal = tokio::signal::ctrl_c() => signal?,
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_only_bare_https_origins() {
        assert!(https_origin(DEFAULT_SERVER).is_ok());
        for url in [
            "http://example.org",
            "https://alice:secret@example.org",
            "https://example.org/work",
            "https://example.org/?redirect=on",
            "https://example.org/#join",
        ] {
            assert!(https_origin(url).is_err(), "accepted {url}");
        }
    }

    #[test]
    fn enforces_block_bounds_and_keeps_wire_numbers_exact() {
        let lease: Lease = serde_json::from_str(
            r#"{"lease":"test","lo":"18446744073709551615","hi":"18446744073709551615"}"#,
        )
        .unwrap();
        assert_eq!(lease_bounds(&lease).unwrap(), (u64::MAX, u64::MAX));

        let lease = Lease {
            lease: "test".into(),
            lo: "0".into(),
            hi: "1048576".into(),
        };
        assert!(lease_bounds(&lease).is_err());
        assert_eq!(
            parse_token("example-device-credential\r\n").unwrap(),
            "example-device-credential"
        );
        assert!(parse_token("example-device\ncredential").is_err());
    }
}
