use crate::{
    Result,
    contribute::UsedEvidence,
    gap_compute::GapAccumulator,
    secret,
    server::{UPLOAD_BODY_LIMIT, Upload, WorkerPing},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prime_finder::{
    archive::{Lease, Span, encode},
    block_codec::BLOCK_WIDTH,
    engine::Engine,
    evidence,
    gap_store::{GapLease, GapPage, GapReport},
};
use reqwest::{Method, Response, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    base: String,
    auth: String,
}

impl Client {
    pub fn admin(base: String) -> Result<Self> {
        Self::new(
            base,
            format!(
                "Basic {}",
                STANDARD.encode(format!("admin:{}", secret("PRIME_ADMIN_TOKEN")?))
            ),
        )
    }

    fn worker(base: String) -> Result<Self> {
        Self::new(base, format!("Bearer {}", secret("PRIME_WORKER_TOKEN")?))
    }

    fn new(base: String, auth: String) -> Result<Self> {
        let base = base.trim_end_matches('/');
        let url = Url::parse(base)?;
        if url.scheme() != "http"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err("Coordinator URL must be an HTTP origin reached through the private tunnel or cluster".into());
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(30))
                .build()?,
            base: format!("{base}/api/v1"),
            auth,
        })
    }

    async fn request<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
    ) -> Result<Response> {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.base))
            .header(reqwest::header::AUTHORIZATION, &self.auth);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let detail = response.text().await?;
        Err(HttpFailure { status, detail }.into())
    }

    pub async fn download(&self, path: &str) -> Result<Response> {
        self.request::<()>(Method::GET, path, None).await
    }

    pub async fn get<R: DeserializeOwned>(&self, path: &str) -> Result<R> {
        Ok(self.download(path).await?.json().await?)
    }

    pub async fn post<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R> {
        Ok(self
            .request(Method::POST, path, Some(body))
            .await?
            .json()
            .await?)
    }

    pub async fn action(&self, id: i64, name: &str) -> Result<Value> {
        self.post(&format!("/jobs/{id}/{name}"), &()).await
    }
}

#[derive(Debug)]
struct HttpFailure {
    status: StatusCode,
    detail: String,
}
impl std::fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.detail)
    }
}
impl std::error::Error for HttpFailure {}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EvidenceMode {
    Off,
    Local,
    Volunteer,
}

pub async fn run(
    base: String,
    worker: String,
    slots: u16,
    base_limit: u32,
    batch_upload: bool,
    verify_contributions: bool,
    evidence: EvidenceMode,
) -> Result<()> {
    let client = Client::worker(base)?;
    let engine = Arc::new(Engine::new(base_limit));
    let mut tasks = tokio::task::JoinSet::new();
    for slot in 0..slots {
        tasks.spawn(worker_loop(
            client.clone(),
            worker.clone(),
            slots,
            engine.clone(),
            cfg!(target_os = "macos") && slot == 0,
            batch_upload,
            verify_contributions && slot == 0,
            evidence,
        ));
    }
    tokio::select! {
        result = tasks.join_next() => {
            if let Some(result) = result { result??; }
        }
        signal = tokio::signal::ctrl_c() => signal?,
    }
    Ok(())
}

async fn worker_loop(
    client: Client,
    worker: String,
    slots: u16,
    engine: Arc<Engine>,
    analyze: bool,
    mut batch_upload: bool,
    verify_contributions: bool,
    evidence: EvidenceMode,
) -> Result<()> {
    let ping = WorkerPing {
        worker: worker.clone(),
        slots,
        token: None,
    };
    let mut last_analysis = None;
    let mut last_verification = None;
    loop {
        if analyze
            && last_analysis.is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(3))
        {
            if let Err(error) = try_gap_report(&client, &ping).await {
                eprintln!("Gap report: {error}");
            }
            last_analysis = Some(Instant::now());
        }
        if verify_contributions
            && last_verification
                .is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(3))
        {
            if let Err(error) = verify_contribution(&client, &worker, engine.clone()).await {
                eprintln!("Volunteer verification: {error}");
            }
            last_verification = Some(Instant::now());
        }
        match client
            .post::<_, Option<Lease>>("/worker/claim", &ping)
            .await
        {
            Ok(Some(lease)) => {
                let renew = WorkerPing {
                    worker: worker.clone(),
                    slots,
                    token: Some(lease.token.clone()),
                };
                let renewal_client = client.clone();
                let heartbeat = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        if let Err(error) = renewal_client
                            .post::<_, Value>("/worker/heartbeat", &renew)
                            .await
                        {
                            eprintln!("heartbeat: {error}");
                        }
                    }
                });
                let result = process(
                    &client,
                    &worker,
                    lease,
                    engine.clone(),
                    &mut batch_upload,
                    evidence,
                )
                .await;
                heartbeat.abort();
                if let Err(error) = result {
                    eprintln!("batch: {error}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            Ok(None) => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(error) => {
                if error
                    .downcast_ref::<HttpFailure>()
                    .is_some_and(|e| e.status.is_client_error())
                {
                    return Err(error);
                }
                eprintln!("claim: {error}");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

#[derive(Deserialize)]
struct VerificationLease {
    lease: String,
    #[serde(flatten)]
    span: Span,
}

async fn verify_contribution(client: &Client, worker: &str, engine: Arc<Engine>) -> Result<()> {
    let result: Option<VerificationLease> = match client
        .post(
            "/worker/contribute/claim",
            &serde_json::json!({"worker": worker}),
        )
        .await
    {
        Ok(result) => result,
        Err(error)
            if error
                .downcast_ref::<HttpFailure>()
                .is_some_and(|failure| failure.status == StatusCode::NOT_FOUND) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let Some(VerificationLease { lease, span }) = result else {
        return Ok(());
    };
    let (count, checksum) = tokio::task::spawn_blocking(move || -> Result<_> {
        let primes = engine.enumerate(span.lo, span.hi);
        let checksum = format!("{:x}", Sha256::digest(encode(span, &primes)?));
        Ok((primes.len(), checksum))
    })
    .await??;
    let response: Value = client
        .post(
            "/worker/contribute/complete",
            &serde_json::json!({
                "worker":worker, "lease":lease, "checksum":checksum, "count":count,
            }),
        )
        .await?;
    eprintln!(
        "Verified volunteer range {}..={} match={}",
        span.lo, span.hi, response["verified"]
    );
    Ok(())
}

async fn process(
    client: &Client,
    worker: &str,
    lease: Lease,
    engine: Arc<Engine>,
    batch_upload: &mut bool,
    mode: EvidenceMode,
) -> Result<()> {
    let first = lease.blocks.first().ok_or("Empty lease")?;
    let last = lease.blocks.last().ok_or("Empty lease")?;
    let (lo, hi) = (first.lo, last.hi);
    let start = Instant::now();
    let (primes, used, retire) =
        if mode == EvidenceMode::Off || !lease.blocks.iter().any(|&span| evidence_eligible(span)) {
            (
                tokio::task::spawn_blocking(move || engine.enumerate(lo, hi)).await?,
                Vec::new(),
                None,
            )
        } else {
            supported_lease(client, engine, &lease.blocks, mode).await?
        };
    let compute = start.elapsed().as_secs_f64();
    let upload_start = Instant::now();
    let mut offset = 0;
    let mut bytes = 0;
    let mut uploads = Vec::with_capacity(lease.blocks.len());
    let mut group_bytes = 2;
    let mut requests = 0;
    for span in lease.blocks {
        let end = offset + primes[offset..].partition_point(|&p| p <= span.hi);
        let data = encode(span, &primes[offset..end])?;
        let checksum = format!("{:x}", Sha256::digest(&data));
        bytes += data.len();
        let upload = Upload {
            token: lease.token.clone(),
            worker: worker.to_owned(),
            span,
            count: (end - offset) as u64,
            payload: STANDARD.encode(&data),
            engine: concat!("prime-finder/", env!("CARGO_PKG_VERSION")).into(),
            checksum,
        };
        if *batch_upload {
            let encoded_len = serde_json::to_vec(&upload)?.len();
            if group_bytes + encoded_len + 1 > UPLOAD_BODY_LIMIT {
                requests += flush_uploads(client, &mut uploads, batch_upload).await?;
                group_bytes = 2;
            }
            if encoded_len + 2 <= UPLOAD_BODY_LIMIT && *batch_upload {
                group_bytes += encoded_len + 1;
                uploads.push(upload);
                offset = end;
                continue;
            }
        }
        send_upload(client, "/worker/commit", &upload).await?;
        requests += 1;
        offset = end;
    }
    requests += flush_uploads(client, &mut uploads, batch_upload).await?;
    if let Some(span) = retire {
        if let Err(error) = client
            .post::<_, Value>(
                "/worker/contribute/evidence/retire",
                &serde_json::json!({
                    "lo":span.lo.to_string(), "hi":span.hi.to_string(), "used":used,
                }),
            )
            .await
        {
            eprintln!("Could not retire volunteer evidence: {error}");
        }
    }
    eprintln!(
        "worker={worker} range={lo}..={hi} primes={} payload_bytes={bytes} upload_requests={requests} compute_seconds={compute:.6} upload_seconds={:.6} evidence_blocks={}",
        primes.len(),
        upload_start.elapsed().as_secs_f64(),
        used.len(),
    );
    Ok(())
}

fn evidence_eligible(span: Span) -> bool {
    span.lo >= evidence::MIN_LO
        && span.lo.is_multiple_of(BLOCK_WIDTH)
        && span.hi.checked_sub(span.lo) == Some(BLOCK_WIDTH - 1)
}

#[derive(Deserialize)]
struct EvidencePayload {
    kind: String,
    lease: String,
    checksum: String,
    payload: String,
}

async fn supported_lease(
    client: &Client,
    engine: Arc<Engine>,
    blocks: &[Span],
    mode: EvidenceMode,
) -> Result<(Vec<u64>, Vec<UsedEvidence>, Option<Span>)> {
    let mut inputs = Vec::with_capacity(blocks.len());
    let mut retire: Option<Span> = None;
    for &span in blocks {
        let mut evidence = None;
        if mode == EvidenceMode::Volunteer && evidence_eligible(span) {
            let covered = retire.get_or_insert(span);
            covered.hi = span.hi;
            match client.post("/worker/contribute/evidence", &span).await {
                Ok(payload) => evidence = payload,
                Err(error) => eprintln!("Could not fetch volunteer evidence: {error}"),
            }
        }
        inputs.push((span, evidence));
    }
    Ok(tokio::task::spawn_blocking(move || {
        let mut primes = Vec::new();
        let mut used = Vec::new();
        for (span, payload) in inputs {
            if let Some(EvidencePayload {
                kind,
                lease,
                checksum,
                payload,
            }) = payload
            {
                if kind == evidence::KIND
                    && payload.len() <= evidence::MAX_BYTES.div_ceil(3) * 4
                    && let Ok(bytes) = STANDARD.decode(payload)
                    && format!("{:x}", Sha256::digest(&bytes)) == checksum
                    && let Ok(mut accepted) = evidence::verify(span.lo, span.hi, &bytes)
                {
                    primes.append(&mut accepted);
                    used.push(UsedEvidence {
                        span,
                        checksum,
                        lease,
                    });
                    continue;
                }
            }
            let mut exact = if evidence_eligible(span) {
                evidence::local_presieve(&engine, span.lo, span.hi)
                    .unwrap_or_else(|_| engine.enumerate(span.lo, span.hi))
            } else {
                engine.enumerate(span.lo, span.hi)
            };
            primes.append(&mut exact);
        }
        (primes, used, retire)
    })
    .await?)
}

async fn send_upload<T: Serialize + ?Sized>(client: &Client, path: &str, body: &T) -> Result<()> {
    for retry in 0..=2 {
        match client.post::<_, Value>(path, body).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                let transient = error.downcast_ref::<HttpFailure>().is_none_or(|http| {
                    matches!(
                        http.status,
                        StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
                    )
                });
                if !transient || retry == 2 {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_millis(200 * (1 << retry))).await;
            }
        }
    }
    unreachable!("upload returns on the last attempt")
}

async fn flush_uploads(
    client: &Client,
    uploads: &mut Vec<Upload>,
    batch_upload: &mut bool,
) -> Result<usize> {
    if uploads.is_empty() {
        return Ok(0);
    }
    match send_upload(client, "/worker/commit-batch", &uploads[..]).await {
        Ok(()) => {
            uploads.clear();
            Ok(1)
        }
        Err(error) => {
            let Some(failure) = error.downcast_ref::<HttpFailure>() else {
                return Err(error);
            };
            match failure.status {
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => *batch_upload = false,
                StatusCode::PAYLOAD_TOO_LARGE => {}
                _ => return Err(error),
            }
            let requests = uploads.len() + 1;
            for upload in uploads.drain(..) {
                send_upload(client, "/worker/commit", &upload).await?;
            }
            Ok(requests)
        }
    }
}

async fn try_gap_report(client: &Client, ping: &WorkerPing) -> Result<()> {
    let lease: Option<GapLease> = match client.post("/worker/gaps/claim", ping).await {
        Ok(result) => result,
        Err(error)
            if error
                .downcast_ref::<HttpFailure>()
                .is_some_and(|e| e.status == StatusCode::NOT_FOUND) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let Some(lease) = lease else {
        return Ok(());
    };
    let renew = WorkerPing {
        worker: ping.worker.clone(),
        slots: ping.slots,
        token: Some(lease.token.clone()),
    };
    let renewal_client = client.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(20)).await;
            if let Err(error) = renewal_client
                .post::<_, Value>("/worker/gaps/heartbeat", &renew)
                .await
            {
                eprintln!("gap heartbeat: {error}");
            }
        }
    });
    let result: Result<()> = async {
        let report = gap_pages(client, &lease, &ping.worker).await?;
        let _: Value = client
            .post(
                "/worker/gaps/commit",
                &serde_json::json!({
                    "worker":ping.worker,"token":lease.token,"report":report,
                }),
            )
            .await?;
        eprintln!(
            "Gap report complete for {}..={} at revision {}",
            lease.lo, lease.hi, lease.revision
        );
        Ok(())
    }
    .await;
    heartbeat.abort();
    if let Err(error) = &result {
        let message: String = error.to_string().chars().take(240).collect();
        let _ = client
            .post::<_, Value>(
                "/worker/gaps/fail",
                &serde_json::json!({
                    "worker":ping.worker, "token":lease.token, "error":message,
                }),
            )
            .await;
    }
    result
}

async fn gap_pages(client: &Client, lease: &GapLease, worker: &str) -> Result<GapReport> {
    let mut acc = GapAccumulator::new(Span {
        lo: lease.lo,
        hi: lease.hi,
    })?;
    let mut after = None;
    loop {
        let mut url = format!("/worker/gaps/blocks?token={}&worker={worker}", lease.token);
        if let Some(cursor) = after {
            url.push_str(&format!("&after={cursor}"));
        }
        let page: GapPage = client.get(&url).await?;
        acc = tokio::task::spawn_blocking(move || -> Result<GapAccumulator> {
            for block in page.blocks {
                acc.push(block)?;
            }
            Ok(acc)
        })
        .await??;
        let Some(next) = page.next_after else { break };
        let next: u64 = next.parse()?;
        if after.is_some_and(|previous| next <= previous) {
            return Err("Archive gap page cursor did not advance".into());
        }
        after = Some(next);
    }
    acc.finish(lease.prime_count.parse()?)
}
