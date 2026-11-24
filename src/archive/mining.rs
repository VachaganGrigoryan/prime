use super::*;
use serde_json::{from_str, to_string};

const MINING_PREFIX: &str = "prime-device/";
const LOOKAHEAD: u128 = 64 * BLOCK;
const ONLINE_SECONDS: i64 = 120;
const RATE_SECONDS: i64 = 60;
const ENROLLMENT_SECONDS: i64 = 600;
const SOURCE_LIMIT: u32 = 10;
const ENROLLMENT_LIMIT: u32 = 2048;
const ADMIN_PAGE_SIZE: i64 = 50;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MiningBenchmark {
    pub version: String,
    pub count: u64,
    pub digest: String,
    pub elapsed_ms: u64,
    pub platform: String,
    pub client_version: String,
    pub passed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MiningWorker {
    pub id: i64,
    pub name: String,
    pub state: String,
    pub last_seen: i64,
    pub benchmark: Option<MiningBenchmark>,
    pub benchmark_at: Option<i64>,
    pub accepted: u64,
    pub invalid: u64,
    pub expired: u64,
    pub verification_ms: Option<u64>,
    pub assignment: Option<Span>,
}

#[derive(Debug, Serialize)]
pub struct MiningStatus {
    pub state: String,
    pub verified_through: String,
    pub verified_primes: String,
    pub revision: String,
    pub latest_primes: Vec<String>,
    pub next_candidates: Option<Span>,
    pub active: Vec<Span>,
    pub online_workers: u64,
    pub recent_primes_per_second: f64,
}

#[derive(Debug, Serialize)]
pub struct MiningAdminOverview {
    pub state: String,
    pub paused: bool,
    pub verified_through: String,
    pub verified_primes: String,
    pub recent_primes_per_second: f64,
    pub active_blocks: i64,
    pub server_time: i64,
    pub total_devices: i64,
    pub pending_devices: i64,
    pub online_approved_devices: i64,
    pub paused_devices: i64,
}

#[derive(Debug, Serialize)]
pub struct MiningAdminDevice {
    #[serde(flatten)]
    pub worker: MiningWorker,
    pub benchmark_prime_count: Option<String>,
    pub paused: bool,
    pub online: bool,
    pub lease_expires_at: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct MiningAdminDevices {
    pub devices: Vec<MiningAdminDevice>,
    pub total: i64,
    pub server_time: i64,
}

#[derive(Debug, Serialize)]
pub struct MiningAdminDeviceDetail {
    #[serde(flatten)]
    pub device: MiningAdminDevice,
    pub decisions: Vec<MiningDecision>,
}

#[derive(Debug, Serialize)]
pub struct MiningDecision {
    pub action: String,
    pub actor: String,
    pub decided_at: i64,
}

#[derive(Debug)]
pub struct MiningSubmission {
    pub span: Span,
    pub committed_digest: Option<String>,
}

pub(super) fn migrate(db: &mut Connection) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS mining_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            started INTEGER NOT NULL DEFAULT 0 CHECK(started IN (0,1)),
            paused INTEGER NOT NULL DEFAULT 1 CHECK(paused IN (0,1)),
            verified BLOB NOT NULL CHECK(length(verified)=8),
            count TEXT NOT NULL
         ) STRICT;
         INSERT OR IGNORE INTO mining_state(singleton,verified,count)
            VALUES (1,x'0000000000000001','0');
         CREATE TABLE IF NOT EXISTS mining_workers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending'
              CHECK(state IN ('pending','approved','rejected','revoked')),
            token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
            last_seen INTEGER NOT NULL,
            benchmark TEXT,
            benchmark_at INTEGER,
            accepted INTEGER NOT NULL DEFAULT 0,
            invalid INTEGER NOT NULL DEFAULT 0,
            expired INTEGER NOT NULL DEFAULT 0,
            verification_ms INTEGER
         ) STRICT;
         CREATE INDEX IF NOT EXISTS mining_workers_online ON mining_workers(state,last_seen);
         CREATE TABLE IF NOT EXISTS mining_enrollments (
            code_hash BLOB PRIMARY KEY CHECK(length(code_hash)=32),
            challenge_hash BLOB NOT NULL UNIQUE CHECK(length(challenge_hash)=32),
            source_hash BLOB NOT NULL CHECK(length(source_hash)=32),
            name TEXT NOT NULL,
            created INTEGER NOT NULL,
            expires INTEGER NOT NULL,
            used INTEGER REFERENCES mining_workers(id)
         ) STRICT, WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS mining_enrollment_source ON mining_enrollments(source_hash,created);
         CREATE INDEX IF NOT EXISTS mining_enrollment_expiry ON mining_enrollments(expires);
         CREATE TABLE IF NOT EXISTS mining_decisions (
            id INTEGER PRIMARY KEY,
            worker_id INTEGER NOT NULL REFERENCES mining_workers(id),
            action TEXT NOT NULL,
            actor TEXT NOT NULL,
            decided_at INTEGER NOT NULL
         ) STRICT;
         CREATE TABLE IF NOT EXISTS mining_receipts (
            token TEXT PRIMARY KEY,
            digest TEXT NOT NULL CHECK(length(digest)=64)
         ) STRICT, WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS mining_segments_owner ON segments(worker) WHERE rev IS NULL;
         CREATE INDEX IF NOT EXISTS mining_segments_token ON segments(token);
         CREATE TABLE IF NOT EXISTS mining_rates (
            second INTEGER PRIMARY KEY,
            count INTEGER NOT NULL
         ) STRICT;
         PRAGMA user_version=3;",
    )?;
    // Existing archive segments already contain exact, trusted results. Scan metadata
    // once during migration and advance transactionally on future commits.
    advance_frontier(&tx)?;
    tx.commit()?;
    Ok(())
}

pub(super) fn migrate_admin(db: &mut Connection) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "ALTER TABLE mining_workers ADD COLUMN paused INTEGER NOT NULL DEFAULT 0
           CHECK(paused IN (0,1) AND (state='approved' OR paused=0));
         CREATE INDEX IF NOT EXISTS mining_decisions_worker
           ON mining_decisions(worker_id, decided_at DESC, id DESC);
         PRAGMA user_version=4;",
    )?;
    tx.commit()?;
    Ok(())
}

pub(super) fn require_legacy(db: &Connection) -> Result<()> {
    let started = db.query_row("SELECT started FROM mining_state", [], |row| {
        row.get::<_, bool>(0)
    })?;
    if started {
        return Err(conflict("continuous mining owns archive dispatch"));
    }
    Ok(())
}

pub(super) fn expire_leases(db: &Connection, before: i64) -> Result<()> {
    db.execute(
        "UPDATE mining_workers SET expired=expired+1
         WHERE id IN (SELECT CAST(substr(worker,length(?2)+1) AS INTEGER) FROM segments
           WHERE rev IS NULL AND expires<=?1 AND substr(worker,1,length(?2))=?2)",
        params![before, MINING_PREFIX],
    )?;
    db.execute(
        "DELETE FROM segments WHERE rev IS NULL AND expires<=?1",
        [before],
    )?;
    Ok(())
}

pub(super) fn advance_frontier(db: &Connection) -> Result<()> {
    let (mut frontier, count): (u64, String) =
        db.query_row("SELECT verified,count FROM mining_state", [], |row| {
            Ok((read_key(row, 0)?, row.get(1)?))
        })?;
    let original = frontier;
    let mut added = 0_u128;
    let mut scan =
        db.prepare_cached("SELECT hi,count FROM segments WHERE lo<=?1 ORDER BY lo DESC LIMIT 1")?;
    while let Some(next) = frontier.checked_add(1) {
        let preceding = scan
            .query_row([key(next)], |row| {
                Ok((read_key(row, 0)?, row.get::<_, Option<u32>>(1)?))
            })
            .optional()?;
        let Some((hi, Some(count))) = preceding.filter(|(hi, _)| *hi >= next) else {
            break;
        };
        frontier = hi;
        added += u128::from(count);
    }
    if frontier != original {
        db.execute(
            "UPDATE mining_state SET verified=?1,count=?2 WHERE singleton=1",
            params![key(frontier), (read_total(&count)? + added).to_string()],
        )?;
    }
    Ok(())
}

const WORKER_COLUMNS: &str = "w.id,w.name,w.state,w.last_seen,w.benchmark,w.benchmark_at,
     w.accepted,w.invalid,w.expired,w.verification_ms,s.lo,s.hi";
const WORKER_JOIN: &str = "FROM mining_workers w LEFT JOIN segments s ON s.worker=?1||w.id
       AND s.rev IS NULL AND s.expires>?2";
const ADMIN_DEVICE_FILTER: &str = "
    WHERE (?3='' OR instr(lower(w.name),lower(?3))>0 OR CAST(w.id AS TEXT)=?3)
      AND (?4='' OR w.state=?4)
      AND CASE ?5
        WHEN '' THEN 1
        WHEN 'enabled' THEN w.state='approved' AND w.paused=0
        WHEN 'finishing' THEN w.paused=1 AND s.expires IS NOT NULL
        WHEN 'paused' THEN w.paused=1 AND s.expires IS NULL
      END";

fn unsigned(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
        })
        .transpose()
}

fn read_worker(row: &rusqlite::Row<'_>) -> rusqlite::Result<MiningWorker> {
    let benchmark = row
        .get::<_, Option<String>>(4)?
        .map(|json| {
            from_str(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()?;
    let assignment = if row.get_ref(10)?.data_type() == rusqlite::types::Type::Null {
        None
    } else {
        Some(read_span(row, 10)?)
    };
    Ok(MiningWorker {
        id: row.get(0)?,
        name: row.get(1)?,
        state: row.get(2)?,
        last_seen: row.get(3)?,
        benchmark,
        benchmark_at: row.get(5)?,
        accepted: unsigned(row, 6)?.unwrap_or_default(),
        invalid: unsigned(row, 7)?.unwrap_or_default(),
        expired: unsigned(row, 8)?.unwrap_or_default(),
        verification_ms: unsigned(row, 9)?,
        assignment,
    })
}

fn read_admin_device(row: &rusqlite::Row<'_>, current: i64) -> rusqlite::Result<MiningAdminDevice> {
    let worker = read_worker(row)?;
    Ok(MiningAdminDevice {
        online: worker.last_seen > current - ONLINE_SECONDS,
        benchmark_prime_count: worker
            .benchmark
            .as_ref()
            .map(|report| report.count.to_string()),
        worker,
        paused: row.get(12)?,
        lease_expires_at: row.get(13)?,
    })
}

fn worker_by_id(db: &Connection, id: i64) -> Result<MiningWorker> {
    db.query_row(
        &format!("SELECT {WORKER_COLUMNS} {WORKER_JOIN} WHERE w.id=?3"),
        params![MINING_PREFIX, now(), id],
        read_worker,
    )
    .optional()?
    .ok_or(ArchiveError::NotFound)
}

fn mining_checkpoint(db: &Connection) -> Result<(bool, u64, String)> {
    Ok(db.query_row(
        "SELECT paused,verified,count FROM mining_state",
        [],
        |row| Ok((row.get(0)?, read_key(row, 1)?, row.get(2)?)),
    )?)
}

fn mining_state_label(
    paused: bool,
    frontier: u64,
    has_active: bool,
    has_eligible: bool,
) -> &'static str {
    if frontier == u64::MAX {
        "exhausted"
    } else if paused {
        "paused"
    } else if !has_active && !has_eligible {
        "waiting"
    } else {
        "running"
    }
}

fn verified_rate(db: &Connection, current: i64) -> Result<f64> {
    let count: f64 = db.query_row(
        "SELECT COALESCE(sum(CAST(count AS REAL)),0.0) FROM mining_rates WHERE second>?1",
        [current - RATE_SECONDS],
        |row| row.get(0),
    )?;
    Ok(count / RATE_SECONDS as f64)
}

fn worker_paused(db: &Connection, id: i64) -> Result<bool> {
    db.prepare_cached("SELECT paused FROM mining_workers WHERE id=?1")?
        .query_row([id], |row| row.get(0))
        .optional()?
        .ok_or(ArchiveError::NotFound)
}

fn approved_id(db: &Connection, token_hash: &[u8; 32]) -> Result<i64> {
    db.query_row(
        "SELECT id FROM mining_workers WHERE token_hash=?1 AND state='approved'",
        [token_hash],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| conflict("worker approval is required"))
}

fn worker_key(id: i64) -> String {
    format!("{MINING_PREFIX}{id}")
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|digit| digit.is_ascii_digit() || (b'a'..=b'f').contains(&digit))
}

fn touch(db: &Connection, token_hash: &[u8; 32], current: i64) -> Result<()> {
    db.execute(
        "UPDATE mining_workers SET last_seen=?2 WHERE token_hash=?1 AND last_seen<=?2-10",
        params![token_hash, current],
    )?;
    Ok(())
}

fn find_work(db: &Connection, frontier: u64, current: i64) -> Result<Option<Span>> {
    let mut next = u128::from(frontier) + 1;
    let limit = (next + LOOKAHEAD).min(u128::from(u64::MAX) + 1);
    // Segments cannot overlap. The only way a block can begin below the
    // candidate cursor is if it already comprises part of the verified prefix.
    let mut following = db.prepare_cached(
        "SELECT lo,hi FROM segments WHERE lo>=?1 AND (rev IS NOT NULL OR expires>?2)
         ORDER BY lo LIMIT 1",
    )?;
    while next < limit {
        let covered = following
            .query_row(params![key(next as u64), current], |row| read_span(row, 0))
            .optional()?;
        if let Some(span) = covered.filter(|span| u128::from(span.lo) == next) {
            next = u128::from(span.hi) + 1;
            continue;
        }
        let block_end = ((next / BLOCK + 1) * BLOCK).min(limit);
        let end = covered.map_or(block_end, |span| u128::from(span.lo).min(block_end));
        return Ok(Some(Span {
            lo: next as u64,
            hi: (end - 1) as u64,
        }));
    }
    Ok(None)
}

fn current_submission(
    db: &Connection,
    id: i64,
    lease: &str,
    current: i64,
) -> Result<MiningSubmission> {
    let result = db
        .query_row(
            "SELECT s.lo,s.hi,r.digest FROM segments s
             LEFT JOIN mining_receipts r ON s.token=r.token
             WHERE s.token=?1 AND s.worker=?2 AND (r.digest IS NOT NULL OR
                   (s.rev IS NULL AND s.expires>?3))",
            params![lease, worker_key(id), current],
            |row| {
                Ok(MiningSubmission {
                    span: read_span(row, 0)?,
                    committed_digest: row.get(2)?,
                })
            },
        )
        .optional()?;
    result.ok_or_else(|| conflict("lease expired or unknown"))
}

impl Archive {
    /// Check a legacy snapshot before using its committed segments as Prime's starting coverage.
    pub fn audit_prime_segments(path: &Path, engine: &crate::engine::Engine) -> Result<u64> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut statement = db.prepare(
            "SELECT lo,hi,count,digest,payload FROM segments WHERE rev IS NOT NULL ORDER BY lo",
        )?;
        let mut rows = statement.query([])?;
        let mut checked = 0;
        while let Some(row) = rows.next()? {
            let span = read_span(row, 0)?;
            let count = u64::from(row.get::<_, u32>(2)?);
            let digest: Vec<u8> = row.get(3)?;
            let payload: Vec<u8> = row.get(4)?;
            let exact = engine.enumerate(span.lo, span.hi);
            if digest != Sha256::digest(&payload).as_slice()
                || decode(span, count, &payload)? != exact
            {
                return Err(ArchiveError::Corrupt(format!(
                    "Legacy archive segment {} through {} failed exact verification",
                    span.lo, span.hi
                )));
            }
            checked += 1;
        }
        Ok(checked)
    }

    pub fn mining_enroll(
        &mut self,
        name: &str,
        code_hash: &[u8; 32],
        challenge_hash: &[u8; 32],
        source_hash: &[u8; 32],
        expires_at: i64,
    ) -> Result<()> {
        let name = name.trim();
        let current = now();
        if name.is_empty()
            || name.len() > 80
            || name.chars().any(char::is_control)
            || !(current + 1..=current + ENROLLMENT_SECONDS).contains(&expires_at)
        {
            return Err(invalid("invalid name or enrollment expiration"));
        }
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM mining_enrollments WHERE expires<?1",
            [current - ENROLLMENT_SECONDS],
        )?;
        let (source_count, replay): (u32, bool) = tx.query_row(
            "SELECT
               (SELECT count(*) FROM mining_enrollments WHERE source_hash=?1 AND created>?2),
               EXISTS(SELECT 1 FROM mining_enrollments WHERE challenge_hash=?3)",
            params![source_hash, current - 60, challenge_hash],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if replay {
            return Err(conflict("enrollment challenge was already redeemed"));
        }
        let total: u32 = tx.query_row("SELECT count(*) FROM mining_enrollments", [], |row| {
            row.get(0)
        })?;
        let pending: u32 = tx.query_row(
            "SELECT count(*) FROM mining_workers WHERE state='pending'",
            [],
            |row| row.get(0),
        )?;
        if source_count >= SOURCE_LIMIT || total >= ENROLLMENT_LIMIT || pending >= ENROLLMENT_LIMIT
        {
            return Err(conflict("enrollment limit reached, try again later"));
        }
        tx.execute(
            "INSERT INTO mining_enrollments(code_hash,challenge_hash,source_hash,name,created,expires)
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![code_hash, challenge_hash, source_hash, name, current, expires_at],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn mining_exchange(
        &mut self,
        code_hash: &[u8; 32],
        token_hash: &[u8; 32],
    ) -> Result<Option<MiningWorker>> {
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enrollment: Option<(String, Option<i64>)> = tx
            .query_row(
                "SELECT name,used FROM mining_enrollments WHERE code_hash=?1 AND expires>?2",
                params![code_hash, current],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((name, used)) = enrollment else {
            return Ok(None);
        };
        if let Some(id) = used {
            let same: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM mining_workers WHERE id=?1 AND token_hash=?2)",
                params![id, token_hash],
                |row| row.get(0),
            )?;
            return if same {
                Ok(Some(worker_by_id(&tx, id)?))
            } else {
                Ok(None)
            };
        }
        let pending: u32 = tx.query_row(
            "SELECT count(*) FROM mining_workers WHERE state='pending'",
            [],
            |row| row.get(0),
        )?;
        if pending >= ENROLLMENT_LIMIT {
            return Err(conflict("enrollment limit reached, try again later"));
        }
        tx.execute(
            "INSERT INTO mining_workers(name,token_hash,last_seen) VALUES (?1,?2,?3)",
            params![name, token_hash, current],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "UPDATE mining_enrollments SET used=?1 WHERE code_hash=?2",
            params![id, code_hash],
        )?;
        let worker = worker_by_id(&tx, id)?;
        tx.commit()?;
        Ok(Some(worker))
    }

    pub fn mining_worker(&mut self, token_hash: &[u8; 32]) -> Result<Option<MiningWorker>> {
        let current = now();
        touch(&self.db, token_hash, current)?;
        Ok(self
            .db
            .query_row(
                &format!("SELECT {WORKER_COLUMNS} {WORKER_JOIN} WHERE w.token_hash=?3"),
                params![MINING_PREFIX, current, token_hash],
                read_worker,
            )
            .optional()?)
    }

    pub fn mining_benchmark(
        &mut self,
        token_hash: &[u8; 32],
        result: MiningBenchmark,
    ) -> Result<MiningWorker> {
        if result.digest.len() != 64
            || !result.digest.bytes().all(|digit| digit.is_ascii_hexdigit())
            || result.elapsed_ms == 0
            || result.elapsed_ms > 3_600_000
            || [&result.version, &result.platform, &result.client_version]
                .into_iter()
                .any(|text| {
                    text.is_empty() || text.len() > 128 || text.chars().any(char::is_control)
                })
        {
            return Err(invalid("invalid benchmark metadata"));
        }
        let worker = self
            .mining_worker(token_hash)?
            .ok_or(ArchiveError::NotFound)?;
        if worker.benchmark.as_ref() == Some(&result) {
            return Ok(worker);
        }
        if worker.state != "pending"
            || worker
                .benchmark
                .as_ref()
                .is_some_and(|previous| previous.passed)
        {
            return Err(conflict("worker is outside benchmark enrollment"));
        }
        let encoded =
            to_string(&result).map_err(|error| ArchiveError::Corrupt(error.to_string()))?;
        self.db.execute(
            "UPDATE mining_workers SET benchmark=?1,benchmark_at=?2 WHERE id=?3",
            params![encoded, now(), worker.id],
        )?;
        worker_by_id(&self.db, worker.id)
    }

    pub fn mining_workers(&self) -> Result<Vec<MiningWorker>> {
        let mut statement = self.db.prepare(&format!(
            "SELECT {WORKER_COLUMNS} {WORKER_JOIN}
                 ORDER BY CASE w.state WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
                          w.last_seen DESC,w.id DESC LIMIT 4096"
        ))?;
        Ok(statement
            .query_map(params![MINING_PREFIX, now()], read_worker)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn mining_admin_devices(
        &self,
        q: &str,
        approval: &str,
        work: &str,
        page: u32,
    ) -> Result<MiningAdminDevices> {
        if page == 0
            || q.len() > 320
            || !matches!(
                approval,
                "" | "pending" | "approved" | "rejected" | "revoked"
            )
            || !matches!(work, "" | "enabled" | "finishing" | "paused")
        {
            return Err(invalid("invalid device search or filters"));
        }
        let current = now();
        let q = q.trim();
        let total = self
            .db
            .prepare_cached(&format!(
                "SELECT count(*) {WORKER_JOIN} {ADMIN_DEVICE_FILTER}"
            ))?
            .query_row(params![MINING_PREFIX, current, q, approval, work], |row| {
                row.get(0)
            })?;
        let offset = i64::from(page - 1) * ADMIN_PAGE_SIZE;
        let mut statement = self.db.prepare_cached(&format!(
            "SELECT {WORKER_COLUMNS},w.paused,s.expires {WORKER_JOIN} {ADMIN_DEVICE_FILTER}
             ORDER BY (w.state='pending') DESC,w.id DESC LIMIT ?6 OFFSET ?7"
        ))?;
        let devices = statement
            .query_map(
                params![
                    MINING_PREFIX,
                    current,
                    q,
                    approval,
                    work,
                    ADMIN_PAGE_SIZE,
                    offset
                ],
                |row| read_admin_device(row, current),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(MiningAdminDevices {
            devices,
            total,
            server_time: current,
        })
    }

    pub fn mining_admin_device(&self, id: i64) -> Result<MiningAdminDeviceDetail> {
        let current = now();
        let device = self
            .db
            .prepare_cached(&format!(
                "SELECT {WORKER_COLUMNS},w.paused,s.expires {WORKER_JOIN} WHERE w.id=?3"
            ))?
            .query_row(params![MINING_PREFIX, current, id], |row| {
                read_admin_device(row, current)
            })
            .optional()?
            .ok_or(ArchiveError::NotFound)?;
        let mut statement = self.db.prepare_cached(
            "SELECT action,actor,decided_at FROM mining_decisions
             WHERE worker_id=?1 ORDER BY decided_at DESC,id DESC LIMIT 20",
        )?;
        let decisions = statement
            .query_map([id], |row| {
                Ok(MiningDecision {
                    action: row.get(0)?,
                    actor: row.get(1)?,
                    decided_at: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(MiningAdminDeviceDetail { device, decisions })
    }

    pub fn mining_worker_paused(&self, id: i64) -> Result<bool> {
        worker_paused(&self.db, id)
    }

    pub fn mining_admin_overview(&self) -> Result<MiningAdminOverview> {
        let current = now();
        let (paused, frontier, count) = mining_checkpoint(&self.db)?;
        let active_blocks = self.db.query_row(
            "SELECT count(*) FROM segments WHERE rev IS NULL AND expires>?1
             AND substr(worker,1,length(?2))=?2",
            params![current, MINING_PREFIX],
            |row| row.get(0),
        )?;
        let (total_devices, pending_devices, online_approved_devices, paused_devices, eligible) =
            self.db.query_row(
                "SELECT count(*),
                        count(*) FILTER (WHERE state='pending'),
                        count(*) FILTER (WHERE state='approved' AND last_seen>?1),
                        count(*) FILTER (WHERE paused=1),
                        count(*) FILTER (WHERE state='approved' AND last_seen>?1 AND paused=0)>0
                 FROM mining_workers",
                [current - ONLINE_SECONDS],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?;
        Ok(MiningAdminOverview {
            state: mining_state_label(paused, frontier, active_blocks != 0, eligible).into(),
            paused,
            verified_through: frontier.to_string(),
            verified_primes: count,
            recent_primes_per_second: verified_rate(&self.db, current)?,
            active_blocks,
            server_time: current,
            total_devices,
            pending_devices,
            online_approved_devices,
            paused_devices,
        })
    }

    pub fn mining_decide(&mut self, id: i64, action: &str, actor: &str) -> Result<MiningWorker> {
        if actor.is_empty() || actor.len() > 320 || actor.chars().any(char::is_control) {
            return Err(invalid("invalid administrator identity"));
        }
        let approval = match action {
            "approve" => Some(("pending", "approved")),
            "reject" => Some(("pending", "rejected")),
            "revoke" => Some(("approved", "revoked")),
            "pause" | "resume" => None,
            _ => return Err(invalid("unknown worker action")),
        };
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let worker = worker_by_id(&tx, id)?;
        if let Some((required, decision)) = approval {
            if worker.state == decision {
                return Ok(worker);
            }
            if worker.state != required {
                return Err(conflict("worker state does not allow this action"));
            }
            if action == "approve" && !worker.benchmark.is_some_and(|benchmark| benchmark.passed) {
                return Err(conflict("worker needs a passing benchmark"));
            }
            tx.execute(
                "UPDATE mining_workers SET state=?1,paused=0 WHERE id=?2",
                params![decision, id],
            )?;
        } else {
            if worker.state != "approved" {
                return Err(conflict("worker approval is required"));
            }
            let paused = action == "pause";
            if worker_paused(&tx, id)? == paused {
                return Ok(worker);
            }
            tx.execute(
                "UPDATE mining_workers SET paused=?1 WHERE id=?2",
                params![paused, id],
            )?;
        }
        tx.execute(
            "INSERT INTO mining_decisions(worker_id,action,actor,decided_at) VALUES (?1,?2,?3,?4)",
            params![id, action, actor, now()],
        )?;
        if action == "revoke" {
            tx.execute(
                "DELETE FROM segments WHERE worker=?1 AND rev IS NULL",
                [worker_key(id)],
            )?;
        }
        let worker = worker_by_id(&tx, id)?;
        tx.commit()?;
        Ok(worker)
    }

    pub fn mining_set_paused(&mut self, paused: bool) -> Result<()> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !paused {
            expire_leases(&tx, now())?;
            let legacy: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM segments
                  WHERE rev IS NULL AND substr(worker,1,length(?1))!=?1)",
                [MINING_PREFIX],
                |row| row.get(0),
            )?;
            if legacy {
                return Err(conflict(
                    "wait for legacy leases to finish before starting mining",
                ));
            }
        }
        tx.execute(
            "UPDATE mining_state SET paused=?1,started=(started OR NOT ?1) WHERE singleton=1",
            [paused],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn mining_claim(&mut self, token_hash: &[u8; 32]) -> Result<Option<Lease>> {
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = approved_id(&tx, token_hash)?;
        touch(&tx, token_hash, current)?;
        expire_leases(&tx, current)?;
        let worker = worker_key(id);
        let previous = tx
            .query_row(
                "SELECT token,lo,hi FROM segments WHERE worker=?1 AND rev IS NULL",
                [&worker],
                |row| Ok((row.get::<_, String>(0)?, read_span(row, 1)?)),
            )
            .optional()?;
        if let Some((token, span)) = previous {
            tx.commit()?;
            return Ok(Some(Lease {
                token,
                worker,
                blocks: vec![span],
            }));
        }
        let (paused, frontier) =
            tx.query_row("SELECT paused,verified FROM mining_state", [], |row| {
                Ok((row.get::<_, bool>(0)?, read_key(row, 1)?))
            })?;
        let span = if paused || worker_paused(&tx, id)? {
            None
        } else {
            find_work(&tx, frontier, current)?
        };
        let Some(span) = span else {
            tx.commit()?;
            return Ok(None);
        };
        let token = token()?;
        tx.execute(
            "INSERT INTO segments(lo,hi,token,worker,expires) VALUES (?1,?2,?3,?4,?5)",
            params![
                key(span.lo),
                key(span.hi),
                &token,
                &worker,
                current + LEASE_SECONDS
            ],
        )?;
        tx.commit()?;
        Ok(Some(Lease {
            token,
            worker,
            blocks: vec![span],
        }))
    }

    pub fn mining_renew(&mut self, token_hash: &[u8; 32], lease: &str) -> Result<()> {
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = approved_id(&tx, token_hash)?;
        let updated = tx.execute(
            "UPDATE segments SET expires=?1 WHERE token=?2 AND worker=?3
             AND rev IS NULL AND expires>?4",
            params![current + LEASE_SECONDS, lease, worker_key(id), current],
        )?;
        if updated == 0 {
            return Err(conflict("lease expired or unknown"));
        }
        touch(&tx, token_hash, current)?;
        tx.commit()?;
        Ok(())
    }

    pub fn mining_submission(
        &self,
        token_hash: &[u8; 32],
        lease: &str,
    ) -> Result<MiningSubmission> {
        let id = approved_id(&self.db, token_hash)?;
        current_submission(&self.db, id, lease, now())
    }

    pub fn mining_reject_result(
        &mut self,
        token_hash: &[u8; 32],
        lease: &str,
        verification_ms: u64,
    ) -> Result<()> {
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = approved_id(&tx, token_hash)?;
        let deleted = tx.execute(
            "DELETE FROM segments WHERE worker=?1 AND token=?2 AND rev IS NULL AND expires>?3",
            params![worker_key(id), lease, current],
        )?;
        if deleted == 0 {
            return Err(conflict("lease expired or unknown"));
        }
        tx.execute(
            "UPDATE mining_workers SET invalid=invalid+1,verification_ms=?1,last_seen=?2 WHERE id=?3",
            params![i64::try_from(verification_ms).unwrap_or(i64::MAX), current, id],
        )?;
        tx.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mining_commit(
        &mut self,
        token_hash: &[u8; 32],
        lease: &str,
        span: Span,
        primes: &[u64],
        engine: &str,
        submission_digest: &str,
        verification_ms: u64,
        space: impl FnOnce(u64) -> Result<()>,
    ) -> Result<()> {
        if !valid_digest(submission_digest) {
            return Err(invalid("invalid submission checksum"));
        }
        let id = approved_id(&self.db, token_hash)?;
        let batch = PreparedBatch::prepare(vec![Commit {
            token: lease.into(),
            worker: worker_key(id),
            span,
            count: primes.len() as u64,
            payload: encode(span, primes)?,
            engine: engine.into(),
            checksum: None,
        }])?;
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = approved_id(&tx, token_hash)?;
        let submission = current_submission(&tx, id, lease, current)?;
        if span != submission.span {
            return Err(conflict("block does not match the current lease"));
        }
        if let Some(committed) = submission.committed_digest {
            return if committed == submission_digest {
                Ok(())
            } else {
                Err(conflict("block was committed with a different result"))
            };
        }
        Self::commit_prepared(&tx, batch, |bytes| space(bytes as u64))?;
        tx.execute(
            "INSERT INTO mining_receipts(token,digest) VALUES (?1,?2)",
            params![lease, submission_digest],
        )?;
        tx.execute(
            "UPDATE mining_workers SET accepted=accepted+1,verification_ms=?1,last_seen=?2 WHERE id=?3",
            params![i64::try_from(verification_ms).unwrap_or(i64::MAX), current, id],
        )?;
        tx.execute(
            "INSERT INTO mining_rates(second,count) VALUES (?1,?2)
             ON CONFLICT(second) DO UPDATE SET count=count+excluded.count",
            params![current, primes.len() as i64],
        )?;
        tx.execute(
            "DELETE FROM mining_rates WHERE second<=?1",
            [current - RATE_SECONDS],
        )?;
        advance_frontier(&tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn mining_status(&self) -> Result<MiningStatus> {
        let current = now();
        let (paused, frontier, count) = mining_checkpoint(&self.db)?;
        let active = self
            .db
            .prepare_cached(
                "SELECT lo,hi FROM segments WHERE rev IS NULL AND expires>?1
             AND substr(worker,1,length(?2))=?2 ORDER BY lo",
            )?
            .query_map(params![current, MINING_PREFIX], |row| read_span(row, 0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let (online_workers, eligible) = self.db.query_row(
            "SELECT count(*),count(*) FILTER (WHERE paused=0)>0
             FROM mining_workers WHERE state='approved' AND last_seen>?1",
            [current - ONLINE_SECONDS],
            |row| Ok((u64::from(row.get::<_, u32>(0)?), row.get(1)?)),
        )?;
        let state = mining_state_label(paused, frontier, !active.is_empty(), eligible);
        let mut latest_primes = Vec::new();
        let mut tail = self.db.prepare_cached(
            "SELECT lo,hi,count FROM segments WHERE lo<=?1 AND rev IS NOT NULL AND count>0
             ORDER BY lo DESC",
        )?;
        let mut rows = tail.query([key(frontier)])?;
        while latest_primes.len() < 12
            && let Some(row) = rows.next()?
        {
            let span = read_span(row, 0)?;
            let count = u64::from(row.get::<_, u32>(2)?);
            latest_primes.extend(
                self.read_payload(span, count)?
                    .iter()
                    .rev()
                    .take(12 - latest_primes.len())
                    .map(u64::to_string),
            );
        }
        Ok(MiningStatus {
            state: state.into(),
            verified_through: frontier.to_string(),
            verified_primes: count,
            revision: self.snapshot()?.to_string(),
            latest_primes,
            next_candidates: find_work(&self.db, frontier, current)?,
            active,
            online_workers,
            recent_primes_per_second: verified_rate(&self.db, current)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;

    fn archive() -> Archive {
        Archive::open(Path::new(":memory:")).unwrap()
    }

    fn enroll(archive: &mut Archive, byte: u8) -> (i64, [u8; 32]) {
        let credential = [byte; 32];
        archive
            .mining_enroll("a worker", &credential, &credential, &[99; 32], now() + 30)
            .unwrap();
        let worker = archive
            .mining_exchange(&credential, &credential)
            .unwrap()
            .unwrap();
        assert!(
            archive
                .mining_exchange(&credential, &[0; 32])
                .unwrap()
                .is_none()
        );
        (worker.id, credential)
    }

    fn benchmark(passed: bool) -> MiningBenchmark {
        MiningBenchmark {
            version: "v1".into(),
            count: 4,
            digest: "0".repeat(64),
            elapsed_ms: 20,
            platform: "macos-arm64".into(),
            client_version: "0.2.0".into(),
            passed,
        }
    }

    fn approved(archive: &mut Archive, byte: u8) -> (i64, [u8; 32]) {
        let (id, credential) = enroll(archive, byte);
        archive
            .mining_benchmark(&credential, benchmark(true))
            .unwrap();
        archive.mining_decide(id, "approve", "owner").unwrap();
        (id, credential)
    }

    fn commit(archive: &mut Archive, credential: &[u8; 32], lease: &Lease) -> Result<()> {
        let block = lease.blocks[0];
        let values = Engine::new(10_000).enumerate(block.lo, block.hi);
        let digest = Sha256::digest(encode(block, &values)?);
        archive.mining_commit(
            credential,
            &lease.token,
            block,
            &values,
            "test",
            &format!("{digest:x}"),
            10,
            |_| Ok(()),
        )
    }

    #[test]
    fn enrollment_requires_benchmark_and_rejects_turnstile_replay() {
        let mut archive = archive();
        let (id, credential) = enroll(&mut archive, 1);
        assert!(
            archive
                .mining_enroll("replay", &[2; 32], &credential, &[3; 32], now() + 30)
                .is_err()
        );
        archive.mining_set_paused(false).unwrap();
        assert!(archive.mining_claim(&credential).is_err());
        assert!(archive.mining_decide(id, "approve", "owner").is_err());
        archive
            .mining_benchmark(&credential, benchmark(false))
            .unwrap();
        assert!(archive.mining_decide(id, "approve", "owner").is_err());
        archive
            .mining_benchmark(&credential, benchmark(true))
            .unwrap();
        archive.mining_decide(id, "approve", "owner").unwrap();
        assert!(archive.mining_claim(&credential).unwrap().is_some());
        assert!(archive.claim("legacy").is_err());
    }

    #[test]
    fn out_of_order_results_cannot_skip_the_frontier() {
        let mut archive = archive();
        let (_, a) = approved(&mut archive, 1);
        let (_, b) = approved(&mut archive, 2);
        archive.mining_set_paused(false).unwrap();
        let a_lease = archive.mining_claim(&a).unwrap().unwrap();
        let b_lease = archive.mining_claim(&b).unwrap().unwrap();
        assert_eq!(a_lease.blocks[0].lo, 2);
        assert_eq!(a_lease.blocks[0].hi + 1, b_lease.blocks[0].lo);
        assert!(archive.mining_submission(&a, &b_lease.token).is_err());
        commit(&mut archive, &b, &b_lease).unwrap();
        assert_eq!(archive.mining_status().unwrap().verified_through, "1");
        assert!(archive.mining_status().unwrap().latest_primes.is_empty());
        commit(&mut archive, &a, &a_lease).unwrap();
        commit(&mut archive, &a, &a_lease).unwrap();
        let status = archive.mining_status().unwrap();
        assert_eq!(status.verified_through, b_lease.blocks[0].hi.to_string());
        assert_eq!(status.revision, "2");
        assert_eq!(archive.mining_worker(&a).unwrap().unwrap().accepted, 1);
        let expected = Engine::new(10_000).enumerate(2, b_lease.blocks[0].hi);
        assert_eq!(status.verified_primes, expected.len().to_string());
        assert_eq!(
            status.latest_primes[0],
            expected.last().unwrap().to_string()
        );
    }

    #[test]
    fn rejection_revocation_and_expiry_requeue_the_same_block() {
        let mut archive = archive();
        let (first_id, first) = approved(&mut archive, 1);
        let (_, second) = approved(&mut archive, 2);
        archive.mining_set_paused(false).unwrap();
        let lease = archive.mining_claim(&first).unwrap().unwrap();
        archive
            .mining_reject_result(&first, &lease.token, 12)
            .unwrap();
        assert_eq!(archive.mining_worker(&first).unwrap().unwrap().invalid, 1);
        let lease = archive.mining_claim(&first).unwrap().unwrap();
        archive.mining_decide(first_id, "revoke", "owner").unwrap();
        assert!(commit(&mut archive, &first, &lease).is_err());
        let second_lease = archive.mining_claim(&second).unwrap().unwrap();
        assert_eq!(lease.blocks, second_lease.blocks);
        assert_ne!(lease.token, second_lease.token);
        archive
            .db
            .execute(
                "UPDATE segments SET expires=?1 WHERE token=?2",
                params![now(), second_lease.token],
            )
            .unwrap();
        assert!(archive.mining_renew(&second, &second_lease.token).is_err());
        archive.mining_set_paused(true).unwrap();
        assert!(archive.mining_claim(&second).unwrap().is_none());
        assert_eq!(archive.mining_worker(&second).unwrap().unwrap().expired, 1);
        archive.mining_set_paused(false).unwrap();
        let retry = archive.mining_claim(&second).unwrap().unwrap();
        assert_eq!(retry.blocks, second_lease.blocks);
    }

    #[test]
    fn device_pause_drains_work_and_resumes_without_changing_approval() {
        let mut archive = archive();
        let (first_id, first) = approved(&mut archive, 1);
        let (second_id, second) = approved(&mut archive, 2);
        let (pending_id, _) = enroll(&mut archive, 3);
        assert!(archive.mining_decide(pending_id, "pause", "owner").is_err());
        assert!(
            archive
                .mining_decide(pending_id, "resume", "owner")
                .is_err()
        );
        archive
            .db
            .execute(
                "UPDATE mining_workers SET last_seen=0 WHERE id=?1",
                [second_id],
            )
            .unwrap();
        archive.mining_set_paused(false).unwrap();
        let lease = archive.mining_claim(&first).unwrap().unwrap();
        for action in ["pause", "pause"] {
            assert_eq!(
                archive
                    .mining_decide(first_id, action, "owner")
                    .unwrap()
                    .state,
                "approved"
            );
        }
        let detail = archive.mining_admin_device(first_id).unwrap();
        assert!(detail.device.paused && detail.device.online);
        assert_eq!(detail.device.worker.assignment, Some(lease.blocks[0]));
        assert!(detail.device.lease_expires_at.is_some());
        assert_eq!(detail.decisions.len(), 2);
        assert_eq!(detail.decisions[0].action, "pause");
        assert_eq!(detail.decisions[0].actor, "owner");
        assert_eq!(
            archive
                .mining_admin_devices("", "", "finishing", 1)
                .unwrap()
                .total,
            1
        );
        assert_eq!(
            archive
                .mining_admin_devices("", "", "paused", 1)
                .unwrap()
                .total,
            0
        );
        let overview = archive.mining_admin_overview().unwrap();
        assert_eq!(overview.state, "running");
        assert_eq!(overview.active_blocks, 1);
        assert_eq!(overview.online_approved_devices, 1);
        assert_eq!(overview.paused_devices, 1);
        assert_eq!(overview.pending_devices, 1);

        let recovered = archive.mining_claim(&first).unwrap().unwrap();
        assert_eq!(recovered.token, lease.token);
        archive.mining_renew(&first, &lease.token).unwrap();
        commit(&mut archive, &first, &lease).unwrap();
        assert!(archive.mining_claim(&first).unwrap().is_none());
        assert_eq!(
            archive
                .mining_admin_devices("", "", "paused", 1)
                .unwrap()
                .total,
            1
        );
        assert_eq!(archive.mining_status().unwrap().state, "waiting");
        let overview = archive.mining_admin_overview().unwrap();
        assert_eq!(overview.state, "waiting");
        assert_eq!(overview.active_blocks, 0);
        assert_eq!(overview.online_approved_devices, 1);
        assert!(overview.recent_primes_per_second > 0.0);

        for action in ["resume", "resume"] {
            archive.mining_decide(first_id, action, "owner").unwrap();
        }
        assert_eq!(
            archive
                .mining_admin_device(first_id)
                .unwrap()
                .decisions
                .len(),
            3
        );
        let next = archive.mining_claim(&first).unwrap().unwrap();
        assert_ne!(next.token, lease.token);
        archive.mining_decide(first_id, "pause", "owner").unwrap();
        archive.mining_decide(first_id, "revoke", "owner").unwrap();
        assert!(archive.mining_renew(&first, &next.token).is_err());
        assert!(commit(&mut archive, &first, &next).is_err());
        assert!(archive.mining_decide(first_id, "resume", "owner").is_err());
        assert_eq!(
            archive.mining_claim(&second).unwrap().unwrap().blocks,
            next.blocks
        );
        let detail = archive.mining_admin_device(first_id).unwrap();
        assert_eq!(detail.device.worker.accepted, 1);
        assert!(!detail.device.paused);
        assert!(detail.device.worker.assignment.is_none());
    }

    #[test]
    fn v3_worker_migration_defaults_to_enabled_and_retains_pause_after_reopen() {
        let path =
            std::env::temp_dir().join(format!("prime-mining-v3-{}.sqlite", token().unwrap()));
        let mut archive = Archive::open(&path).unwrap();
        let (id, credential) = approved(&mut archive, 1);
        archive.mining_set_paused(false).unwrap();
        archive
            .db
            .execute_batch(
                "DROP INDEX mining_decisions_worker;
                 ALTER TABLE mining_workers DROP COLUMN paused;
                 PRAGMA user_version=3;",
            )
            .unwrap();
        drop(archive);

        let mut archive = Archive::open(&path).unwrap();
        let detail = archive.mining_admin_device(id).unwrap();
        assert!(!detail.device.paused);
        assert_eq!(detail.device.worker.state, "approved");
        assert_eq!(detail.decisions[0].action, "approve");
        assert!(archive.mining_claim(&credential).unwrap().is_some());
        archive.mining_decide(id, "pause", "owner").unwrap();
        drop(archive);

        let mut archive = Archive::open(&path).unwrap();
        assert!(archive.mining_worker_paused(id).unwrap());
        assert!(archive.mining_claim(&credential).unwrap().is_none());
        assert_eq!(
            archive.mining_admin_device(id).unwrap().decisions[0].action,
            "pause"
        );
        drop(archive);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn admin_device_search_filters_and_pages_the_fleet() {
        let mut archive = archive();
        let (approved_id, _) = approved(&mut archive, 1);
        archive
            .db
            .execute(
                "WITH RECURSIVE devices(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM devices WHERE n<51)
                 INSERT INTO mining_workers(name,token_hash,last_seen)
                 SELECT 'Atlas-'||n,CAST(printf('%032d',n) AS BLOB),?1 FROM devices",
                [now()],
            )
            .unwrap();
        let first = archive.mining_admin_devices("", "", "", 1).unwrap();
        assert_eq!(first.total, 52);
        assert_eq!(first.devices.len(), 50);
        assert_eq!(first.devices[0].worker.name, "Atlas-51");
        let second = archive.mining_admin_devices("", "", "", 2).unwrap();
        assert_eq!(second.devices[0].worker.name, "Atlas-1");
        assert_eq!(second.devices[1].worker.id, approved_id);
        assert_eq!(
            archive
                .mining_admin_devices("aTLAs", "pending", "", 1)
                .unwrap()
                .total,
            51
        );
        let id = approved_id.to_string();
        assert_eq!(
            archive
                .mining_admin_devices(&id, "approved", "enabled", 1)
                .unwrap()
                .total,
            1
        );
        assert_eq!(
            archive
                .mining_admin_devices("", "approved", "paused", 1)
                .unwrap()
                .total,
            0
        );
        assert!(archive.mining_admin_devices("", "", "", 0).is_err());
        assert_eq!(archive.mining_admin_overview().unwrap().total_devices, 52);
        assert!(matches!(
            archive.mining_admin_device(999),
            Err(ArchiveError::NotFound)
        ));
    }

    #[test]
    fn restart_keeps_commits_and_requeues_only_uncommitted_work() {
        let root = std::env::temp_dir().join(format!("prime-mining-test-{}", token().unwrap()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("archive.sqlite");
        let mut archive = Archive::open(&path).unwrap();
        let (_, a) = approved(&mut archive, 1);
        let (_, b) = approved(&mut archive, 2);
        archive.mining_set_paused(false).unwrap();
        let earlier = archive.mining_claim(&a).unwrap().unwrap();
        let later = archive.mining_claim(&b).unwrap().unwrap();
        commit(&mut archive, &b, &later).unwrap();
        drop(archive);

        let mut archive = Archive::open(&path).unwrap();
        assert!(archive.mining_submission(&a, &earlier.token).is_err());
        assert!(
            archive
                .mining_submission(&b, &later.token)
                .unwrap()
                .committed_digest
                .is_some()
        );
        assert_eq!(archive.mining_worker(&a).unwrap().unwrap().expired, 1);
        let retry = archive.mining_claim(&b).unwrap().unwrap();
        assert_eq!(retry.blocks, earlier.blocks);
        commit(&mut archive, &b, &retry).unwrap();
        assert_eq!(
            archive.mining_status().unwrap().verified_through,
            later.blocks[0].hi.to_string()
        );
        drop(archive);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_coverage_is_reused_and_boundary_does_not_overflow() {
        let mut archive = archive();
        let (_, credential) = approved(&mut archive, 1);
        let span = Span { lo: 0, hi: 11 };
        archive.create_job(span).unwrap();
        let legacy = archive.claim("legacy").unwrap().unwrap();
        assert!(archive.mining_set_paused(false).is_err());
        archive
            .commit(Commit {
                token: legacy.token,
                worker: legacy.worker,
                span,
                count: 5,
                payload: encode(span, &[2, 3, 5, 7, 11]).unwrap(),
                engine: "test".into(),
                checksum: None,
            })
            .unwrap();
        assert_eq!(archive.mining_status().unwrap().verified_primes, "5");
        archive.mining_set_paused(false).unwrap();
        let lease = archive.mining_claim(&credential).unwrap().unwrap();
        assert_eq!(lease.blocks[0].lo, 12);
        // Seed the checkpoint to exercise the last integer without traversing u64.
        archive
            .db
            .execute("DELETE FROM segments WHERE rev IS NULL", [])
            .unwrap();
        archive
            .db
            .execute("UPDATE mining_state SET verified=?1", [key(u64::MAX - 1)])
            .unwrap();
        let lease = archive.mining_claim(&credential).unwrap().unwrap();
        assert_eq!(
            lease.blocks,
            [Span {
                lo: u64::MAX,
                hi: u64::MAX
            }]
        );
        commit(&mut archive, &credential, &lease).unwrap();
        let status = archive.mining_status().unwrap();
        assert_eq!(status.state, "exhausted");
        assert_eq!(status.verified_through, u64::MAX.to_string());
        assert!(archive.mining_claim(&credential).unwrap().is_none());
    }
}
