use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fmt,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const BLOCK: u128 = crate::block_codec::BLOCK_WIDTH as u128;
const LEASE_SECONDS: i64 = 120;
const LEASE_BLOCKS: usize = 16;
const SUMMARY_ROWS: usize = 4096;
const SUMMARY_DECODES: usize = 32;

pub type Result<T> = std::result::Result<T, ArchiveError>;

#[derive(Debug)]
pub enum ArchiveError {
    Invalid(String),
    NotFound,
    Conflict(String),
    Uncovered,
    StoragePaused,
    Database(rusqlite::Error),
    Corrupt(String),
    Random(String),
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message)
            | Self::Conflict(message)
            | Self::Corrupt(message)
            | Self::Random(message) => f.write_str(message),
            Self::NotFound => f.write_str("job not found"),
            Self::Uncovered => f.write_str("archive does not cover the entire requested range"),
            Self::StoragePaused => f.write_str("archive storage is paused"),
            Self::Database(error) => error.fmt(f),
        }
    }
}

impl Error for ArchiveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for ArchiveError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

pub(crate) fn invalid(message: &str) -> ArchiveError {
    ArchiveError::Invalid(message.into())
}

pub(crate) fn conflict(message: &str) -> ArchiveError {
    ArchiveError::Conflict(message.into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    #[serde(with = "decimal")]
    pub lo: u64,
    #[serde(with = "decimal")]
    pub hi: u64,
}

pub(crate) mod decimal {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(D::Error::custom("expected an unsigned decimal integer"));
        }
        value.parse().map_err(D::Error::custom)
    }
}

impl Span {
    pub fn validate(self) -> Result<Self> {
        if self.lo > self.hi {
            return Err(invalid("lower bound must be no greater than upper bound"));
        }
        Ok(self)
    }

    pub fn width(self) -> u128 {
        u128::from(self.hi) + 1 - u128::from(self.lo)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: i64,
    pub span: Span,
    pub state: String,
    pub width: String,
    pub covered: String,
    pub count: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Lease {
    pub token: String,
    pub worker: String,
    pub blocks: Vec<Span>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Commit {
    pub token: String,
    pub worker: String,
    pub span: Span,
    pub count: u64,
    pub payload: Vec<u8>,
    pub engine: String,
    pub checksum: Option<String>,
}

struct PreparedBlock {
    request: Commit,
    primes: Vec<u64>,
    digest: [u8; 32],
}

pub struct PreparedBatch(Vec<PreparedBlock>);

impl PreparedBatch {
    /// Check the batch shape and verify the block payloads before acquiring the write lock.
    pub fn prepare(requests: Vec<Commit>) -> Result<Self> {
        if requests.is_empty() || requests.len() > LEASE_BLOCKS {
            return Err(invalid("batch must contain 1 to 16 blocks"));
        }
        for pair in requests.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            if a.token != b.token || a.worker != b.worker {
                return Err(invalid(
                    "batch blocks must belong to the same worker and lease",
                ));
            }
            if a.span.hi.checked_add(1) != Some(b.span.lo) {
                return Err(invalid("batch blocks must be contiguous and ascending"));
            }
        }
        let blocks = requests
            .into_iter()
            .map(|request| {
                request.span.validate()?;
                if request.span.width() > BLOCK || request.payload.len() as u128 > BLOCK + 16 {
                    return Err(invalid("block payload exceeds archive limit"));
                }
                if request.engine.is_empty() || request.engine.len() > 128 {
                    return Err(invalid("engine version must contain 1 to 128 bytes"));
                }
                let primes = decode(request.span, request.count, &request.payload)?;
                let digest = Sha256::digest(&request.payload);
                if request
                    .checksum
                    .as_deref()
                    .is_some_and(|value| value != format!("{digest:x}"))
                {
                    return Err(invalid("prime block checksum mismatch"));
                }
                Ok(PreparedBlock {
                    request,
                    primes,
                    digest: digest.into(),
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self(blocks))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub snapshot: i64,
    pub primes: Vec<String>,
    pub next_after: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Summary {
    pub snapshot: i64,
    pub bins: Vec<Bucket>,
    pub next_after: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Bucket {
    pub lo: String,
    pub hi: String,
    pub covered: String,
    pub count: String,
}

pub(crate) fn key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn read_key(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    row.get::<_, [u8; 8]>(index).map(u64::from_be_bytes)
}

pub(crate) fn read_span(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<Span> {
    Ok(Span {
        lo: read_key(row, offset)?,
        hi: read_key(row, offset + 1)?,
    })
}

fn read_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let span = read_span(row, 1)?;
    Ok(Job {
        id: row.get(0)?,
        span,
        state: row.get(3)?,
        width: span.width().to_string(),
        covered: row.get(4)?,
        count: row.get(5)?,
    })
}

fn read_total(value: &str) -> Result<u128> {
    value
        .parse()
        .map_err(|_| ArchiveError::Corrupt("invalid archive count".into()))
}

fn count_within(primes: &[u64], span: Span) -> u128 {
    let start = primes.partition_point(|value| *value < span.lo);
    let end = primes.partition_point(|value| *value <= span.hi);
    (end - start) as u128
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn token() -> Result<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes).map_err(|error| ArchiveError::Random(error.to_string()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn encode(span: Span, primes: &[u64]) -> Result<Vec<u8>> {
    crate::block_codec::encode(span.lo, span.hi, primes).map_err(|error| invalid(error.message()))
}

pub fn decode(span: Span, count: u64, payload: &[u8]) -> Result<Vec<u64>> {
    crate::block_codec::decode(span.lo, span.hi, count, payload)
        .map_err(|error| invalid(error.message()))
}

pub struct Archive {
    pub(crate) db: Connection,
}

impl Archive {
    pub fn open(path: &Path) -> Result<Self> {
        if rusqlite::version_number() < 3_051_003 {
            return Err(ArchiveError::Corrupt(format!(
                "archive requires SQLite 3.51.3 or later; linked SQLite is {}",
                rusqlite::version()
            )));
        }
        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(5))?;
        let mode: String = db.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if mode != "wal" && path != Path::new(":memory:") {
            return Err(ArchiveError::Corrupt(
                "archive requires SQLite WAL mode".into(),
            ));
        }
        db.execute_batch(
            "PRAGMA synchronous=FULL;
             PRAGMA temp_store=MEMORY;
             PRAGMA wal_autocheckpoint=1000;
             PRAGMA journal_size_limit=16777216;",
        )?;

        let version: i64 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        match version {
            0 => db.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY,
                    lo BLOB NOT NULL CHECK(length(lo)=8),
                    hi BLOB NOT NULL CHECK(length(hi)=8 AND hi>=lo),
                    state TEXT NOT NULL CHECK(state IN ('queued', 'paused', 'cancelled', 'complete')),
                    covered TEXT NOT NULL,
                    count TEXT NOT NULL,
                    scan BLOB CHECK(scan IS NULL OR length(scan)=8)
                 ) STRICT;
                 CREATE INDEX jobs_dispatch ON jobs(state,id) WHERE scan IS NOT NULL;
                 CREATE INDEX jobs_overlap ON jobs(lo,hi) WHERE state!='complete';
                 CREATE TABLE segments (
                    lo BLOB PRIMARY KEY CHECK(length(lo)=8),
                    hi BLOB NOT NULL CHECK(length(hi)=8 AND hi>=lo),
                    token TEXT NOT NULL,
                    worker TEXT NOT NULL,
                    expires INTEGER,
                    rev INTEGER UNIQUE,
                    count INTEGER CHECK(count BETWEEN 0 AND 1048576),
                    digest BLOB,
                    engine TEXT,
                    payload BLOB,
                    CHECK ((rev IS NULL AND expires IS NOT NULL AND payload IS NULL)
                      OR (rev IS NOT NULL AND count IS NOT NULL AND digest IS NOT NULL
                      AND engine IS NOT NULL AND payload IS NOT NULL AND expires IS NULL))
                 ) STRICT, WITHOUT ROWID;
                 CREATE INDEX lease_expiry ON segments(expires) WHERE rev IS NULL;
                 CREATE INDEX lease_token ON segments(token) WHERE rev IS NULL;
                 PRAGMA user_version=1;
                 COMMIT;",
            )?,
            1 | 2 => {}
            _ => {
                return Err(ArchiveError::Corrupt(
                    "unsupported archive schema version".into(),
                ));
            }
        }

        if version < 2 {
            crate::gap_store::migrate(&mut db)?;
        }

        // One coordinator owns the database. Only uncommitted work loses its lease on restart.
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM segments WHERE rev IS NULL", [])?;
        crate::gap_store::release_running(&tx)?;
        tx.commit()?;
        Ok(Self { db })
    }

    pub fn snapshot(&self) -> Result<i64> {
        Ok(self
            .db
            .query_row("SELECT COALESCE(MAX(rev),0) FROM segments", [], |row| {
                row.get(0)
            })?)
    }

    pub(crate) fn read_snapshot(&self, snapshot: Option<i64>) -> Result<i64> {
        let current = self.snapshot()?;
        match snapshot {
            Some(value) if !(0..=current).contains(&value) => {
                Err(invalid("archive snapshot does not exist"))
            }
            Some(value) => Ok(value),
            None => Ok(current),
        }
    }

    pub fn create_job(&mut self, span: Span) -> Result<Job> {
        span.validate()?;
        let (covered, count) = self.totals(span, self.snapshot()?)?;
        let complete = covered == span.width();
        self.db.execute(
            "INSERT INTO jobs(lo,hi,state,scan,covered,count) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                key(span.lo),
                key(span.hi),
                if complete { "complete" } else { "queued" },
                (!complete).then(|| key(span.lo)),
                covered.to_string(),
                count.to_string(),
            ],
        )?;
        self.job(self.db.last_insert_rowid())
    }

    pub fn job(&self, id: i64) -> Result<Job> {
        let job = self
            .db
            .query_row(
                "SELECT id,lo,hi,state,covered,count FROM jobs WHERE id=?",
                [id],
                read_job,
            )
            .optional()?
            .ok_or(ArchiveError::NotFound)?;
        self.with_running_state(job)
    }

    pub fn jobs(&self) -> Result<Vec<Job>> {
        let mut stmt = self
            .db
            .prepare("SELECT id,lo,hi,state,covered,count FROM jobs ORDER BY state IN ('queued','paused') DESC,id DESC LIMIT 100")?;
        stmt.query_map([], read_job)?
            .map(|row| self.with_running_state(row?))
            .collect()
    }

    fn with_running_state(&self, mut job: Job) -> Result<Job> {
        if job.state == "queued" {
            let running = self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM segments INDEXED BY lease_expiry WHERE rev IS NULL AND expires>?1 AND lo<=?2 AND hi>=?3)",
                params![now(), key(job.span.hi), key(job.span.lo)],
                |row| row.get::<_, bool>(0),
            )?;
            if running {
                job.state = "running".into();
            }
        }
        Ok(job)
    }

    pub fn update_job(&mut self, id: i64, action: &str) -> Result<Job> {
        let state = match action {
            "pause" => "paused",
            "resume" => "queued",
            "cancel" => "cancelled",
            _ => return Err(invalid("expected pause, resume, or cancel")),
        };
        let job = self.job(id)?;
        if job.state == "cancelled" && state != "cancelled" {
            return Err(conflict("cancelled jobs cannot resume"));
        }
        if job.state != "complete" {
            self.db
                .execute("UPDATE jobs SET state=?1 WHERE id=?2", params![state, id])?;
        }
        self.job(id)
    }

    pub fn claim(&mut self, worker: &str) -> Result<Option<Lease>> {
        if worker.is_empty() || worker.len() > 128 {
            return Err(invalid("worker identity must contain 1 to 128 bytes"));
        }
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM segments WHERE rev IS NULL AND expires<=?",
            [current],
        )?;

        let jobs = {
            let mut stmt = tx.prepare(
                "SELECT id,scan,hi FROM jobs WHERE state='queued' AND scan IS NOT NULL ORDER BY id",
            )?;
            stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, read_span(row, 1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (id, span) in jobs {
            let mut cursor = u128::from(span.lo);
            let end = u128::from(span.hi) + 1;
            let mut committed_prefix = cursor;
            while cursor < end {
                let at = cursor as u64;
                let previous = tx.query_row(
                    "SELECT hi,rev IS NOT NULL FROM segments WHERE lo<=? ORDER BY lo DESC LIMIT 1",
                    [key(at)],
                    |row| Ok((read_key(row, 0)?, row.get::<_, bool>(1)?)),
                ).optional()?;
                if let Some((hi, committed)) = previous.filter(|(hi, _)| *hi >= at) {
                    let next = (u128::from(hi) + 1).min(end);
                    if committed && committed_prefix == cursor {
                        committed_prefix = next;
                    }
                    cursor = next;
                    continue;
                }

                let following = tx
                    .query_row(
                        "SELECT lo FROM segments WHERE lo>? ORDER BY lo LIMIT 1",
                        [key(at)],
                        |row| read_key(row, 0),
                    )
                    .optional()?;
                let gap_end = following.map_or(end, |lo| u128::from(lo).min(end));
                let token = token()?;
                let mut blocks = Vec::new();
                while cursor < gap_end && blocks.len() < LEASE_BLOCKS {
                    let next = ((cursor / BLOCK + 1) * BLOCK).min(gap_end);
                    let block = Span {
                        lo: cursor as u64,
                        hi: (next - 1) as u64,
                    };
                    tx.execute(
                        "INSERT INTO segments(lo,hi,token,worker,expires) VALUES (?1,?2,?3,?4,?5)",
                        params![
                            key(block.lo),
                            key(block.hi),
                            token,
                            worker,
                            current + LEASE_SECONDS
                        ],
                    )?;
                    blocks.push(block);
                    cursor = next;
                }
                tx.execute(
                    "UPDATE jobs SET scan=?1 WHERE id=?2",
                    params![key(committed_prefix as u64), id],
                )?;
                tx.commit()?;
                return Ok(Some(Lease {
                    token,
                    worker: worker.into(),
                    blocks,
                }));
            }

            // An active lease may cover the tail, but expiry can make it dispatchable again.
            let scan = (committed_prefix < end).then(|| key(committed_prefix as u64));
            tx.execute("UPDATE jobs SET scan=?1 WHERE id=?2", params![scan, id])?;
        }
        tx.commit()?;
        Ok(None)
    }

    pub fn heartbeat(&mut self, token: &str, worker: &str) -> Result<()> {
        let current = now();
        let rows = self.db.execute(
            "UPDATE segments SET expires=?1 WHERE token=?2 AND worker=?3 AND rev IS NULL AND expires>?4",
            params![current + LEASE_SECONDS, token, worker, current],
        )?;
        if rows == 0 {
            return Err(conflict("lease expired or unknown"));
        }
        Ok(())
    }

    pub fn commit(&mut self, request: Commit) -> Result<()> {
        self.commit_with_guard(request, || Ok(()))
    }

    pub fn commit_with_guard(
        &mut self,
        request: Commit,
        before_write: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.commit_batch_with_guard(vec![request], |_| before_write())
    }

    pub fn commit_batch_with_guard(
        &mut self,
        requests: Vec<Commit>,
        before_write: impl FnOnce(usize) -> Result<()>,
    ) -> Result<()> {
        self.commit_prepared_batch_with_guard(PreparedBatch::prepare(requests)?, before_write)
    }

    /// Commit all new blocks together. Identical accepted blocks are safe to replay.
    pub fn commit_prepared_batch_with_guard(
        &mut self,
        batch: PreparedBatch,
        before_write: impl FnOnce(usize) -> Result<()>,
    ) -> Result<()> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_time = now();
        let pending = {
            let mut lease_row = tx.prepare_cached(
                "SELECT hi,token,worker,expires,rev,count,digest FROM segments WHERE lo=?",
            )?;
            let mut pending = Vec::with_capacity(batch.0.len());
            for block in batch.0 {
                let request = &block.request;
                let current = lease_row
                    .query_row([key(request.span.lo)], |row| {
                        Ok((
                            read_key(row, 0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Option<u32>>(5)?.map(u64::from),
                            row.get::<_, Option<Vec<u8>>>(6)?,
                        ))
                    })
                    .optional()?;
                let Some((hi, token, worker, expiry, revision, count, saved_hash)) = current else {
                    return Err(conflict("lease expired or unknown"));
                };
                if hi != request.span.hi || token != request.token || worker != request.worker {
                    return Err(conflict("block does not match the current lease"));
                }
                if revision.is_some() {
                    if count != Some(request.count)
                        || saved_hash.as_deref() != Some(block.digest.as_slice())
                    {
                        return Err(conflict("block was committed with a different result"));
                    }
                    continue;
                }
                if expiry.is_none_or(|expiry| expiry <= current_time) {
                    return Err(conflict("lease expired or unknown"));
                }
                pending.push(block);
            }
            pending
        };
        let [first, ..] = pending.as_slice() else {
            return Ok(());
        };
        before_write(
            pending
                .iter()
                .map(|block| block.request.payload.len())
                .sum(),
        )?;
        let last = pending.last().unwrap();

        {
            let mut save_block = tx.prepare_cached(
                "UPDATE segments SET expires=NULL,count=?1,digest=?2,engine=?3,payload=?4,
                 rev=(SELECT COALESCE(MAX(rev),0)+1 FROM segments) WHERE lo=?5",
            )?;
            for block in &pending {
                let request = &block.request;
                save_block.execute(params![
                    request.count as u32,
                    &block.digest[..],
                    request.engine,
                    request.payload,
                    key(request.span.lo),
                ])?;
            }
        }

        let jobs = {
            let mut overlapping_jobs = tx.prepare_cached(
                "SELECT id,lo,hi,state,covered,count FROM jobs
                 WHERE state!='complete' AND lo<=?1 AND hi>=?2",
            )?;
            overlapping_jobs
                .query_map(
                    params![key(last.request.span.hi), key(first.request.span.lo)],
                    read_job,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };

        {
            let mut save_job = tx.prepare_cached(
                "UPDATE jobs SET state=?1,covered=?2,count=?3,scan=CASE WHEN ?4 THEN NULL ELSE scan END
                 WHERE id=?5",
            )?;
            for job in jobs {
                let mut added_coverage = 0;
                let mut added_primes = 0;
                for block in &pending {
                    let intersection = Span {
                        lo: job.span.lo.max(block.request.span.lo),
                        hi: job.span.hi.min(block.request.span.hi),
                    };
                    if intersection.lo <= intersection.hi {
                        added_coverage += intersection.width();
                        added_primes += count_within(&block.primes, intersection);
                    }
                }
                // Completed replays may leave a hole in the range occupied by pending blocks.
                if added_coverage == 0 {
                    continue;
                }
                let covered = read_total(&job.covered)? + added_coverage;
                let count = read_total(&job.count)? + added_primes;
                let complete = job.state != "cancelled" && covered == job.span.width();
                let state = if complete { "complete" } else { &job.state };
                save_job.execute(params![
                    state,
                    covered.to_string(),
                    count.to_string(),
                    complete,
                    job.id
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn first_key(&self, lo: u64) -> Result<u64> {
        let previous = self
            .db
            .query_row(
                "SELECT lo,hi FROM segments WHERE lo<=? ORDER BY lo DESC LIMIT 1",
                [key(lo)],
                |row| read_span(row, 0),
            )
            .optional()?;
        Ok(previous
            .filter(|span| span.hi >= lo)
            .map_or(lo, |span| span.lo))
    }

    fn read_payload(&self, span: Span, count: u64) -> Result<Vec<u64>> {
        let (data, digest): (Vec<u8>, Vec<u8>) = self.db.query_row(
            "SELECT payload,digest FROM segments WHERE lo=?",
            [key(span.lo)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if Sha256::digest(&data)[..] != digest {
            return Err(ArchiveError::Corrupt(
                "saved prime block checksum mismatch".into(),
            ));
        }
        decode(span, count, &data).map_err(|error| ArchiveError::Corrupt(error.to_string()))
    }

    pub fn page(
        &self,
        span: Span,
        after: Option<u64>,
        limit: usize,
        snapshot: Option<i64>,
    ) -> Result<Page> {
        span.validate()?;
        if !(1..=10_000).contains(&limit) {
            return Err(invalid("page limit must be between 1 and 10000"));
        }
        let snapshot = self.read_snapshot(snapshot)?;
        let mut page = Page {
            snapshot,
            primes: Vec::new(),
            next_after: None,
        };
        if after.is_some_and(|last| last >= span.hi) {
            return Ok(page);
        }
        let lo = after.map_or(span.lo, |last| span.lo.max(last + 1));
        let first = self.first_key(lo)?;
        let mut stmt = self.db.prepare(
            "SELECT lo,hi,count FROM segments WHERE lo>=?1 AND lo<=?2 AND rev<=?3 AND count>0 ORDER BY lo",
        )?;
        let mut rows = stmt.query(params![key(first), key(span.hi), snapshot])?;
        while let Some(row) = rows.next()? {
            let block = read_span(row, 0)?;
            let primes = self.read_payload(block, u64::from(row.get::<_, u32>(2)?))?;
            let start = primes.partition_point(|value| *value < lo);
            let end = primes.partition_point(|value| *value <= span.hi);
            for value in &primes[start..end] {
                if page.primes.len() == limit {
                    page.next_after = page.primes.last().cloned();
                    return Ok(page);
                }
                page.primes.push(value.to_string());
            }
        }
        Ok(page)
    }

    pub fn summary(
        &self,
        span: Span,
        bins: usize,
        snapshot: Option<i64>,
        after: Option<u64>,
    ) -> Result<Summary> {
        span.validate()?;
        if !(1..=2048).contains(&bins) {
            return Err(invalid("bucket count must be between 1 and 2048"));
        }
        let snapshot = self.read_snapshot(snapshot)?;
        let bins = (bins as u128).min(span.width());
        let spans: Vec<_> = (0..bins)
            .map(|index| Span {
                lo: (u128::from(span.lo) + span.width() * index / bins) as u64,
                hi: (u128::from(span.lo) + span.width() * (index + 1) / bins - 1) as u64,
            })
            .collect();
        let mut counts = vec![(0_u128, 0_u128); spans.len()];

        let load = after.map_or_else(
            || self.first_key(span.lo).map(Some),
            |key| Ok(key.checked_add(1)),
        )?;
        let mut next_after = None;
        if let Some(first) = load {
            let mut stmt = self.db.prepare(
                "SELECT lo,hi,count FROM segments WHERE lo>=?1 AND lo<=?2 AND rev<=?3 AND hi>=?4 ORDER BY lo",
            )?;
            let mut rows = stmt.query(params![key(first), key(span.hi), snapshot, key(span.lo)])?;
            let mut scanned = 0;
            let mut decoded = 0;
            let mut last = None;
            while let Some(row) = rows.next()? {
                let block = read_span(row, 0)?;
                let count = u64::from(row.get::<_, u32>(2)?);
                let left = spans.partition_point(|bucket| bucket.hi < block.lo);
                let right = spans.partition_point(|bucket| bucket.lo <= block.hi);
                let contained =
                    right == left + 1 && spans[left].lo <= block.lo && block.hi <= spans[left].hi;
                let needs_data = !contained && count > 0;
                if scanned == SUMMARY_ROWS || (needs_data && decoded == SUMMARY_DECODES) {
                    next_after = last;
                    break;
                }
                let values = if needs_data {
                    decoded += 1;
                    self.read_payload(block, count)?
                } else {
                    Vec::new()
                };
                for index in left..right {
                    let intersection = Span {
                        lo: spans[index].lo.max(block.lo),
                        hi: spans[index].hi.min(block.hi),
                    };
                    counts[index].0 += intersection.width();
                    counts[index].1 += if contained {
                        u128::from(count)
                    } else {
                        count_within(&values, intersection)
                    };
                }
                scanned += 1;
                last = Some(block.lo.to_string());
            }
        }
        Ok(Summary {
            snapshot,
            next_after,
            bins: spans
                .iter()
                .zip(counts)
                .map(|(span, (covered, count))| Bucket {
                    lo: span.lo.to_string(),
                    hi: span.hi.to_string(),
                    covered: covered.to_string(),
                    count: count.to_string(),
                })
                .collect(),
        })
    }

    pub(crate) fn totals(&self, span: Span, snapshot: i64) -> Result<(u128, u128)> {
        let mut after = None;
        let mut covered = 0;
        let mut count = 0;
        loop {
            let result = self.summary(span, 1, Some(snapshot), after)?;
            let bin = &result.bins[0];
            covered += bin.covered.parse::<u128>().expect("summary formats u128");
            count += bin.count.parse::<u128>().expect("summary formats u128");
            let Some(next_after) = result.next_after else {
                break;
            };
            after = Some(next_after.parse().expect("summary formats u64"));
        }
        Ok((covered, count))
    }

    pub fn verify_full(&self, span: Span, snapshot: Option<i64>) -> Result<i64> {
        span.validate()?;
        let snapshot = self.read_snapshot(snapshot)?;
        if self.totals(span, snapshot)?.0 != span.width() {
            return Err(ArchiveError::Uncovered);
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(lo: u64, hi: u64) -> Span {
        Span { lo, hi }
    }

    fn memory() -> Archive {
        Archive::open(Path::new(":memory:")).unwrap()
    }

    fn result(lease: &Lease, block: Span, primes: &[u64]) -> Commit {
        let payload = encode(block, primes).unwrap();
        Commit {
            token: lease.token.clone(),
            worker: lease.worker.clone(),
            span: block,
            count: primes.len() as u64,
            checksum: Some(format!("{:x}", Sha256::digest(&payload))),
            payload,
            engine: "test-engine".into(),
        }
    }

    #[test]
    fn overlaps_share_leases_and_commits() {
        let mut archive = memory();
        let first = archive.create_job(span(0, 10)).unwrap();
        let overlap = archive.create_job(span(7, 17)).unwrap();
        let a = archive.claim("a").unwrap().unwrap();
        let b = archive.claim("b").unwrap().unwrap();
        assert_eq!(a.blocks, [span(0, 10)]);
        assert_eq!(b.blocks, [span(11, 17)]);
        assert!(archive.claim("c").unwrap().is_none());
        assert!(matches!(
            archive.verify_full(span(0, 17), None),
            Err(ArchiveError::Uncovered)
        ));
        assert_eq!(archive.job(first.id).unwrap().state, "running");

        archive
            .commit(result(&b, b.blocks[0], &[11, 13, 17]))
            .unwrap();
        let partial = archive.job(overlap.id).unwrap();
        assert_eq!(
            (partial.covered.as_str(), partial.count.as_str()),
            ("7", "3")
        );
        let snapshot = archive.snapshot().unwrap();

        let mut bad = result(&a, a.blocks[0], &[2, 3, 5, 7]);
        bad.checksum = Some(String::new());
        assert!(matches!(archive.commit(bad), Err(ArchiveError::Invalid(_))));
        assert_eq!(archive.snapshot().unwrap(), snapshot);

        archive
            .commit(result(&a, a.blocks[0], &[2, 3, 5, 7]))
            .unwrap();
        archive
            .commit(result(&a, a.blocks[0], &[2, 3, 5, 7]))
            .unwrap();
        assert_eq!(archive.snapshot().unwrap(), snapshot + 1);
        assert!(matches!(
            archive.commit(result(&a, a.blocks[0], &[2, 3, 5])),
            Err(ArchiveError::Conflict(_))
        ));
        let completed = archive.job(overlap.id).unwrap();
        assert_eq!(
            (
                completed.state.as_str(),
                completed.covered.as_str(),
                completed.count.as_str()
            ),
            ("complete", "11", "4")
        );

        let page = archive.page(span(5, 17), None, 2, None).unwrap();
        assert_eq!(page.primes, ["5", "7"]);
        let rest = archive
            .page(span(5, 17), Some(7), 10, Some(page.snapshot))
            .unwrap();
        assert_eq!(rest.primes, ["11", "13", "17"]);
        assert_eq!(rest.next_after, None);
        assert_eq!(
            archive
                .page(span(0, 17), None, 10, Some(snapshot))
                .unwrap()
                .primes,
            ["11", "13", "17"]
        );
        let cached = archive.create_job(span(6, 13)).unwrap();
        assert_eq!(
            (
                cached.state.as_str(),
                cached.covered.as_str(),
                cached.count.as_str()
            ),
            ("complete", "8", "3")
        );
    }

    #[test]
    fn batch_replays_committed_prefix_and_updates_clipped_job_counts() {
        let mut archive = memory();
        let edge = BLOCK as u64;
        let scope = span(edge - 1, 2 * edge + 7);
        let overlap = span(edge + 1, 2 * edge + 2);
        let job = archive.create_job(scope).unwrap();
        let intersecting = archive.create_job(overlap).unwrap();
        let lease = archive.claim("batch").unwrap().unwrap();
        assert_eq!(
            lease.blocks,
            [
                span(edge - 1, edge - 1),
                span(edge, 2 * edge - 1),
                span(2 * edge, scope.hi)
            ]
        );
        let engine = crate::engine::Engine::new(100_000_000);
        let batch = || {
            lease
                .blocks
                .iter()
                .map(|&block| result(&lease, block, &engine.enumerate(block.lo, block.hi)))
                .collect::<Vec<_>>()
        };

        archive.commit(batch().remove(0)).unwrap();
        let requests = batch();
        let pending_bytes = requests[1..]
            .iter()
            .map(|block| block.payload.len())
            .sum::<usize>();
        archive
            .commit_batch_with_guard(requests, |bytes| {
                assert_eq!(bytes, pending_bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(archive.snapshot().unwrap(), 3);
        assert!(matches!(
            archive.verify_full(scope, Some(2)),
            Err(ArchiveError::Uncovered)
        ));
        assert_eq!(archive.verify_full(scope, None).unwrap(), 3);
        let done = archive.job(job.id).unwrap();
        assert_eq!(done.state, "complete");
        assert_eq!(done.covered, scope.width().to_string());
        assert_eq!(
            done.count,
            engine.enumerate(scope.lo, scope.hi).len().to_string()
        );
        let done = archive.job(intersecting.id).unwrap();
        assert_eq!(done.state, "complete");
        assert_eq!(done.covered, overlap.width().to_string());
        assert_eq!(
            done.count,
            engine.enumerate(overlap.lo, overlap.hi).len().to_string()
        );

        archive
            .commit_batch_with_guard(batch(), |_| panic!("replay must bypass write guard"))
            .unwrap();
        assert_eq!(archive.snapshot().unwrap(), 3);
        assert_eq!(archive.job(intersecting.id).unwrap().count, done.count);
    }

    #[test]
    fn batch_rejects_bad_blocks_and_quota_failure_without_committing() {
        let mut archive = memory();
        let edge = BLOCK as u64;
        let job = archive.create_job(span(edge - 1, edge + 2)).unwrap();
        let lease = archive.claim("batch").unwrap().unwrap();
        let batch = || {
            lease
                .blocks
                .iter()
                .map(|&block| result(&lease, block, &[]))
                .collect::<Vec<_>>()
        };

        let mut wrong_checksum = batch();
        wrong_checksum[1].checksum = Some("bad checksum".into());
        assert!(matches!(
            archive.commit_batch_with_guard(wrong_checksum, |_| panic!(
                "invalid batch must not allocate"
            )),
            Err(ArchiveError::Invalid(_))
        ));
        let out_of_order = batch().into_iter().rev().collect();
        assert!(matches!(
            PreparedBatch::prepare(out_of_order),
            Err(ArchiveError::Invalid(_))
        ));
        assert!(matches!(
            archive.commit_batch_with_guard(batch(), |bytes| {
                assert_eq!(bytes, 0);
                Err(ArchiveError::StoragePaused)
            }),
            Err(ArchiveError::StoragePaused)
        ));

        archive
            .db
            .execute(
                "UPDATE segments SET expires=0 WHERE lo=?1",
                [key(lease.blocks[1].lo)],
            )
            .unwrap();
        assert!(matches!(
            archive.commit_batch_with_guard(batch(), |_| panic!("expired batch must not allocate")),
            Err(ArchiveError::Conflict(_))
        ));
        assert_eq!(archive.snapshot().unwrap(), 0);
        assert_eq!(archive.job(job.id).unwrap().covered, "0");
    }

    #[test]
    fn batch_handles_a_full_lease_through_u64_max() {
        let mut archive = memory();
        let scope = span(
            u64::MAX - (BLOCK as u64) * LEASE_BLOCKS as u64 + 1,
            u64::MAX,
        );
        let job = archive.create_job(scope).unwrap();
        let lease = archive.claim("batch").unwrap().unwrap();
        assert_eq!(lease.blocks.len(), LEASE_BLOCKS);
        let requests = lease
            .blocks
            .iter()
            .map(|&block| result(&lease, block, &[]))
            .collect();
        archive
            .commit_batch_with_guard(requests, |_| Ok(()))
            .unwrap();
        assert_eq!(archive.snapshot().unwrap(), LEASE_BLOCKS as i64);
        let done = archive.job(job.id).unwrap();
        assert_eq!(done.covered, scope.width().to_string());
        assert_eq!(done.state, "complete");
        assert_eq!(done.count, "0");
    }

    #[test]
    fn batch_rolls_back_first_write_when_a_later_write_fails() {
        let mut archive = memory();
        let edge = BLOCK as u64;
        let scope = span(edge - 1, edge + 1);
        let job = archive.create_job(scope).unwrap();
        let lease = archive.claim("batch").unwrap().unwrap();
        let batch = || {
            lease
                .blocks
                .iter()
                .map(|&block| result(&lease, block, &[]))
                .collect::<Vec<_>>()
        };
        archive
            .db
            .execute_batch(
                "CREATE TEMP TRIGGER fail_batch_write BEFORE UPDATE OF payload ON segments
             WHEN OLD.lo = (SELECT MAX(lo) FROM segments)
             BEGIN SELECT RAISE(ABORT, 'forced batch failure'); END;",
            )
            .unwrap();
        assert!(matches!(
            archive.commit_batch_with_guard(batch(), |_| Ok(())),
            Err(ArchiveError::Database(_))
        ));
        assert_eq!(archive.snapshot().unwrap(), 0);
        assert_eq!(archive.job(job.id).unwrap().covered, "0");
        archive
            .db
            .execute("DROP TRIGGER fail_batch_write", [])
            .unwrap();
        archive
            .commit_batch_with_guard(batch(), |_| Ok(()))
            .unwrap();
        assert_eq!(archive.snapshot().unwrap(), 2);
        assert_eq!(archive.job(job.id).unwrap().covered, "3");
    }

    #[test]
    fn unsigned_bounds_and_empty_coverage_are_exact() {
        let mut archive = memory();
        let last_prime = 18_446_744_073_709_551_557;
        let tail = span(u64::MAX - 59, u64::MAX);
        archive.create_job(tail).unwrap();
        let lease = archive.claim("a").unwrap().unwrap();
        assert_eq!(lease.blocks, [tail]);
        archive.commit(result(&lease, tail, &[last_prime])).unwrap();
        assert_eq!(archive.verify_full(tail, None).unwrap(), 1);
        assert_eq!(
            archive.page(tail, None, 10, None).unwrap().primes,
            [last_prime.to_string()]
        );
        assert!(
            archive
                .page(tail, Some(u64::MAX), 10, None)
                .unwrap()
                .primes
                .is_empty()
        );

        let empty = archive.create_job(span(0, 1)).unwrap();
        let lease = archive.claim("a").unwrap().unwrap();
        archive
            .commit(result(&lease, lease.blocks[0], &[]))
            .unwrap();
        let done = archive.job(empty.id).unwrap();
        assert_eq!(
            (
                done.covered.as_str(),
                done.count.as_str(),
                done.state.as_str()
            ),
            ("2", "0", "complete")
        );
        let full = archive.create_job(span(0, u64::MAX)).unwrap();
        assert_eq!(full.width, "18446744073709551616");
        assert_eq!(full.covered, "62");
        let slices = archive.summary(tail, 17, None, None).unwrap();
        assert_eq!(slices.bins.last().unwrap().hi, u64::MAX.to_string());
        assert_eq!(
            slices
                .bins
                .iter()
                .map(|bin| read_total(&bin.covered).unwrap())
                .sum::<u128>(),
            60
        );
        assert_eq!(
            slices
                .bins
                .iter()
                .map(|bin| read_total(&bin.count).unwrap())
                .sum::<u128>(),
            1
        );
    }

    #[test]
    fn expired_work_reuses_committed_prefix_and_respects_job_controls() {
        let mut archive = memory();
        let edge = BLOCK as u64;
        let span = span(edge - 1, edge + 3);
        let job = archive.create_job(span).unwrap();
        let first = archive.claim("first").unwrap().unwrap();
        assert_eq!(first.blocks.len(), 2);
        archive.heartbeat(&first.token, &first.worker).unwrap();
        archive
            .commit(result(&first, first.blocks[0], &[]))
            .unwrap();
        archive
            .db
            .execute("UPDATE segments SET expires=0 WHERE rev IS NULL", [])
            .unwrap();
        archive.update_job(job.id, "pause").unwrap();
        assert!(archive.claim("second").unwrap().is_none());
        archive.update_job(job.id, "resume").unwrap();
        let replacement = archive.claim("second").unwrap().unwrap();
        assert_eq!(replacement.blocks, first.blocks[1..]);
        assert!(matches!(
            archive.commit(result(&first, first.blocks[1], &[])),
            Err(ArchiveError::Conflict(_))
        ));
        archive.update_job(job.id, "cancel").unwrap();
        archive
            .commit(result(&replacement, replacement.blocks[0], &[edge + 3]))
            .unwrap();
        let job = archive.job(job.id).unwrap();
        assert_eq!(
            (job.state.as_str(), job.covered.as_str()),
            ("cancelled", "5")
        );
        assert!(archive.verify_full(span, None).is_ok());
        assert!(matches!(
            archive.update_job(job.id, "resume"),
            Err(ArchiveError::Conflict(_))
        ));
    }

    #[test]
    fn summary_pages_preserve_snapshot_and_coverage() {
        let mut archive = memory();
        for n in 0..70_u64 {
            let block = span(4 * n + 2, 4 * n + 3);
            archive.create_job(block).unwrap();
            let lease = archive.claim("a").unwrap().unwrap();
            archive.commit(result(&lease, block, &[block.lo])).unwrap();
        }
        let scope = span(3, 280);
        let first = archive.summary(scope, 278, None, None).unwrap();
        let snapshot = first.snapshot;
        let after = first.next_after.as_deref().unwrap().parse().unwrap();
        // A newly filled hole prior to the cursor must stay excluded from the original view.
        archive.create_job(span(4, 5)).unwrap();
        let lease = archive.claim("a").unwrap().unwrap();
        archive.commit(result(&lease, span(4, 5), &[5])).unwrap();
        let second = archive
            .summary(scope, 278, Some(snapshot), Some(after))
            .unwrap();
        let after = second.next_after.as_deref().unwrap().parse().unwrap();
        let third = archive
            .summary(scope, 278, Some(snapshot), Some(after))
            .unwrap();
        assert!(third.next_after.is_none());
        let covered: u128 = [first, second, third]
            .into_iter()
            .flat_map(|page| page.bins)
            .map(|bin| bin.covered.parse::<u128>().unwrap())
            .sum();
        assert_eq!(covered, 139);
    }

    #[test]
    fn reopen_recovers_commits_and_releases_leases() {
        let path =
            std::env::temp_dir().join(format!("prime-archive-test-{}.sqlite", token().unwrap()));
        let mut archive = Archive::open(&path).unwrap();
        let edge = BLOCK as u64;
        let job = archive.create_job(span(edge - 1, edge)).unwrap();
        let first = archive.claim("first").unwrap().unwrap();
        archive
            .commit(result(&first, first.blocks[0], &[]))
            .unwrap();
        drop(archive);

        let mut archive = Archive::open(&path).unwrap();
        assert_eq!(archive.job(job.id).unwrap().covered, "1");
        let second = archive.claim("second").unwrap().unwrap();
        assert_eq!(second.blocks, first.blocks[1..]);
        archive
            .commit(result(&second, second.blocks[0], &[]))
            .unwrap();
        assert_eq!(archive.job(job.id).unwrap().state, "complete");
        drop(archive);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn v1_archive_migration_preserves_revision_payload_and_jobs() {
        let path =
            std::env::temp_dir().join(format!("prime-archive-v1-{}.sqlite", token().unwrap()));
        let mut archive = Archive::open(&path).unwrap();
        let job = archive.create_job(span(0, 10)).unwrap();
        let lease = archive.claim("worker").unwrap().unwrap();
        archive
            .commit(result(&lease, lease.blocks[0], &[2, 3, 5, 7]))
            .unwrap();
        archive
            .db
            .execute_batch("DROP TABLE gap_reports; PRAGMA user_version=1;")
            .unwrap();
        drop(archive);

        let mut archive = Archive::open(&path).unwrap();
        assert_eq!(archive.snapshot().unwrap(), 1);
        assert_eq!(archive.job(job.id).unwrap().count, "4");
        assert_eq!(
            archive.page(span(0, 10), None, 10, None).unwrap().primes,
            ["2", "3", "5", "7"]
        );
        assert_eq!(
            archive.queue_gaps(span(1, 8), 1, || Ok(())).unwrap().state,
            "queued"
        );
        drop(archive);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn codec_rejects_truncated_noncanonical_and_overflowing_gaps() {
        let full = span(0, u64::MAX);
        for input in [&[0x80][..], &[0x80, 0], &[0xff; 10]] {
            assert!(decode(full, 1, input).is_err());
        }
        assert!(decode(span(2, 3), 2, &[0, 0]).is_err());
        assert!(decode(span(2, 3), 1, &[0, 0]).is_err());
        assert!(encode(full, &[13, 13]).is_err());
        let primes = [2, 3, 5, 18_446_744_073_709_551_557];
        assert_eq!(
            decode(full, 4, &encode(full, &primes).unwrap()).unwrap(),
            primes
        );
    }
}
