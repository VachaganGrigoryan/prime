use crate::archive::{
    Archive, ArchiveError, Result, Span, conflict, decimal, invalid, key, now, read_span, token,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

const LEASE_SECONDS: i64 = 120;
const BLOCKS_PER_PAGE: usize = 4;
const MAX_ERROR_BYTES: usize = 512;
pub const MAX_GAP_RECORDS: usize = 20;

pub(crate) fn migrate(db: &mut Connection) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "CREATE TABLE gap_reports (
            id INTEGER PRIMARY KEY,
            lo BLOB NOT NULL CHECK(length(lo)=8),
            hi BLOB NOT NULL CHECK(length(hi)=8 AND hi>=lo),
            revision INTEGER NOT NULL CHECK(revision>=0),
            prime_count TEXT NOT NULL,
            state TEXT NOT NULL,
            worker TEXT,
            token TEXT,
            expires INTEGER,
            error TEXT,
            report TEXT,
            UNIQUE(lo,hi,revision),
            CHECK(
                (state='queued' AND worker IS NULL AND token IS NULL AND expires IS NULL
                    AND error IS NULL AND report IS NULL)
                OR (worker IS NOT NULL AND token IS NOT NULL AND (
                    (state='running' AND expires IS NOT NULL AND error IS NULL AND report IS NULL)
                    OR (state='failed' AND expires IS NULL AND error IS NOT NULL AND report IS NULL)
                    OR (state='complete' AND expires IS NULL AND error IS NULL AND report IS NOT NULL)
                ))
            )
        ) STRICT;
        CREATE INDEX gap_pending ON gap_reports(id) WHERE state IN ('queued','running');
        CREATE INDEX gap_worker ON gap_reports(worker) WHERE state='running';
        CREATE UNIQUE INDEX gap_token ON gap_reports(token) WHERE token IS NOT NULL;
        PRAGMA user_version=2;",
    )?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn release_running(db: &Connection) -> Result<()> {
    db.execute(
        "UPDATE gap_reports SET state='queued',worker=NULL,token=NULL,expires=NULL
         WHERE state='running'",
        [],
    )?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GapLease {
    #[serde(with = "decimal")]
    pub lo: u64,
    #[serde(with = "decimal")]
    pub hi: u64,
    pub revision: String,
    pub token: String,
    pub prime_count: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GapBlock {
    #[serde(with = "decimal")]
    pub lo: u64,
    #[serde(with = "decimal")]
    pub hi: u64,
    pub count: u64,
    pub payload: String,
    pub checksum: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GapPage {
    pub blocks: Vec<GapBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapReport {
    pub prime_count: String,
    pub gap_count: String,
    pub twin_count: String,
    pub histogram: Vec<GapFrequency>,
    pub largest: Vec<GapRecord>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapFrequency {
    pub gap: String,
    pub count: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapRecord {
    pub gap: String,
    pub left: String,
    pub right: String,
}

#[derive(Debug, Serialize)]
pub struct GapStatus {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<GapReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl GapStatus {
    fn new(state: &'static str) -> Self {
        Self {
            state,
            report: None,
            error: None,
        }
    }
}

fn unsigned(value: &str) -> Result<u64> {
    let number = value
        .parse::<u64>()
        .map_err(|_| invalid("gap report values must be unsigned decimal strings"))?;
    if value != number.to_string() {
        return Err(invalid(
            "gap report values must be canonical decimal strings",
        ));
    }
    Ok(number)
}

fn validate_report(span: Span, expected_count: &str, report: &GapReport) -> Result<()> {
    let prime_count = unsigned(&report.prime_count)?;
    let gap_count = unsigned(&report.gap_count)?;
    let twin_count = unsigned(&report.twin_count)?;
    if report.prime_count != expected_count || gap_count != prime_count.saturating_sub(1) {
        return Err(invalid(
            "gap report count does not match the archived range",
        ));
    }
    let mut total = 0_u64;
    let mut distance = 0_u128;
    let mut previous_gap = 0;
    let mut twins = 0;
    let mut frequencies = Vec::with_capacity(report.histogram.len());
    for entry in &report.histogram {
        let gap = unsigned(&entry.gap)?;
        let count = unsigned(&entry.count)?;
        if gap <= previous_gap || count == 0 {
            return Err(invalid(
                "gap histogram must have positive sorted gaps and counts",
            ));
        }
        total = total
            .checked_add(count)
            .filter(|sum| *sum <= gap_count)
            .ok_or_else(|| invalid("gap histogram count exceeds the reported number of gaps"))?;
        distance = distance
            .checked_add(u128::from(gap) * u128::from(count))
            .filter(|sum| *sum <= u128::from(span.hi - span.lo))
            .ok_or_else(|| invalid("gap histogram distances exceed the archived span"))?;
        if gap == 2 {
            twins = count;
        }
        frequencies.push((gap, count));
        previous_gap = gap;
    }
    if total != gap_count || twins != twin_count {
        return Err(invalid("gap histogram totals do not match the report"));
    }

    let records = gap_count.min(MAX_GAP_RECORDS as u64) as usize;
    if report.largest.len() != records {
        return Err(invalid("incorrect number of largest gaps"));
    }
    let expected_gaps = frequencies
        .into_iter()
        .rev()
        .flat_map(|(gap, count)| std::iter::repeat_n(gap, count.min(records as u64) as usize));
    let mut previous = None;
    let mut spans = Vec::<Span>::with_capacity(records);
    for (record, expected_gap) in report.largest.iter().zip(expected_gaps) {
        let gap = unsigned(&record.gap)?;
        let left = unsigned(&record.left)?;
        let right = unsigned(&record.right)?;
        if gap != expected_gap
            || left < span.lo.max(2)
            || right > span.hi
            || right.checked_sub(left) != Some(gap)
            || previous.is_some_and(|(g, l)| gap == g && left <= l)
            || spans
                .iter()
                .any(|saved| left < saved.hi && right > saved.lo)
        {
            return Err(invalid(
                "largest gaps do not match the range, histogram, or order",
            ));
        }
        spans.push(Span {
            lo: left,
            hi: right,
        });
        previous = Some((gap, left));
    }
    Ok(())
}

fn lease_conflict() -> ArchiveError {
    conflict("gap report lease has expired or belongs to another worker")
}

impl Archive {
    pub fn gap_status(&self, span: Span, revision: i64) -> Result<GapStatus> {
        span.validate()?;
        self.read_snapshot(Some(revision))?;
        let row = self
            .db
            .query_row(
                "SELECT state,expires,error,report FROM gap_reports
                 WHERE lo=?1 AND hi=?2 AND revision=?3",
                params![key(span.lo), key(span.hi), revision],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((state, expires, error, report)) = row else {
            return Ok(GapStatus::new("missing"));
        };
        match state.as_str() {
            "queued" => Ok(GapStatus::new("queued")),
            "running" if expires.is_some_and(|deadline| deadline > now()) => {
                Ok(GapStatus::new("running"))
            }
            "running" => Ok(GapStatus::new("queued")),
            "failed" => Ok(GapStatus {
                error,
                ..GapStatus::new("failed")
            }),
            "complete" => {
                let report = report
                    .and_then(|value| serde_json::from_str(&value).ok())
                    .ok_or_else(|| ArchiveError::Corrupt("saved gap report is invalid".into()))?;
                Ok(GapStatus {
                    report: Some(report),
                    ..GapStatus::new("complete")
                })
            }
            _ => Err(ArchiveError::Corrupt("gap report state is invalid".into())),
        }
    }

    pub fn queue_gaps(
        &mut self,
        span: Span,
        revision: i64,
        guard: impl FnOnce() -> Result<()>,
    ) -> Result<GapStatus> {
        let existing = self.gap_status(span, revision)?;
        if existing.state == "missing" {
            let (covered, count) = self.totals(span, revision)?;
            if covered != span.width() {
                return Err(ArchiveError::Uncovered);
            }
            guard()?;
            self.db.execute(
                "INSERT INTO gap_reports(lo,hi,revision,prime_count,state)
                 VALUES (?1,?2,?3,?4,'queued')
                 ON CONFLICT(lo,hi,revision) DO NOTHING",
                params![key(span.lo), key(span.hi), revision, count.to_string()],
            )?;
        } else if existing.state == "failed" {
            guard()?;
            self.db.execute(
                "UPDATE gap_reports SET state='queued',worker=NULL,token=NULL,error=NULL
                 WHERE lo=?1 AND hi=?2 AND revision=?3 AND state='failed'",
                params![key(span.lo), key(span.hi), revision],
            )?;
        } else {
            return Ok(existing);
        }
        self.gap_status(span, revision)
    }

    pub fn claim_gaps(&mut self, worker: &str) -> Result<Option<GapLease>> {
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = tx
            .query_row(
                "SELECT lo,hi,revision,token,prime_count FROM gap_reports
                 WHERE state='running' AND worker=?1 AND expires>?2",
                params![worker, current],
                |row| {
                    let span = read_span(row, 0)?;
                    Ok(GapLease {
                        lo: span.lo,
                        hi: span.hi,
                        revision: row.get::<_, i64>(2)?.to_string(),
                        token: row.get(3)?,
                        prime_count: row.get(4)?,
                    })
                },
            )
            .optional()?;
        if active.is_some() {
            return Ok(active);
        }
        let pending = tx
            .query_row(
                "SELECT id,lo,hi,revision,prime_count FROM gap_reports
                 WHERE state IN ('queued','running') AND (expires IS NULL OR expires<=?)
                 ORDER BY id LIMIT 1",
                [current],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        read_span(row, 1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, span, revision, prime_count)) = pending else {
            return Ok(None);
        };
        let token = token()?;
        tx.execute(
            "UPDATE gap_reports SET state='running',worker=?1,token=?2,expires=?3 WHERE id=?4",
            params![worker, token, current + LEASE_SECONDS, id],
        )?;
        tx.commit()?;
        Ok(Some(GapLease {
            lo: span.lo,
            hi: span.hi,
            revision: revision.to_string(),
            token,
            prime_count,
        }))
    }

    pub fn heartbeat_gaps(&mut self, worker: &str, token: &str) -> Result<()> {
        let current = now();
        let changed = self.db.execute(
            "UPDATE gap_reports SET expires=?1
             WHERE token=?2 AND worker=?3 AND state='running' AND expires>?4",
            params![current + LEASE_SECONDS, token, worker, current],
        )?;
        if changed == 0 {
            return Err(lease_conflict());
        }
        Ok(())
    }

    pub fn gap_blocks(&self, worker: &str, token: &str, after: Option<u64>) -> Result<GapPage> {
        let (span, snapshot) = self
            .db
            .query_row(
                "SELECT lo,hi,revision FROM gap_reports
                 WHERE token=?1 AND worker=?2 AND state='running' AND expires>?3",
                params![token, worker, now()],
                |row| Ok((read_span(row, 0)?, row.get::<_, i64>(2)?)),
            )
            .optional()?
            .ok_or_else(lease_conflict)?;
        let mut page = GapPage {
            blocks: Vec::new(),
            next_after: None,
        };
        if after.is_some_and(|last| last >= span.hi) {
            return Ok(page);
        }

        let first = self
            .first_key(span.lo)?
            .max(after.map_or(0, |last| last + 1));
        let mut statement = self.db.prepare(
            "SELECT lo,hi,count,digest,payload FROM segments
             WHERE lo>=?1 AND lo<=?2 AND rev<=?3 AND hi>=?4 ORDER BY lo LIMIT ?5",
        )?;
        let mut rows = statement.query(params![
            key(first),
            key(span.hi),
            snapshot,
            key(span.lo),
            (BLOCKS_PER_PAGE + 1) as i64
        ])?;
        while let Some(row) = rows.next()? {
            if page.blocks.len() == BLOCKS_PER_PAGE {
                page.next_after = page.blocks.last().map(|block| block.lo.to_string());
                break;
            }
            let block = read_span(row, 0)?;
            let digest = row.get_ref(3)?.as_blob().map_err(|error| {
                ArchiveError::Corrupt(format!("saved block checksum could not be read: {error}"))
            })?;
            if digest.len() != 32 {
                return Err(ArchiveError::Corrupt(
                    "saved block checksum is invalid".into(),
                ));
            }
            let payload = row.get_ref(4)?.as_blob().map_err(|error| {
                ArchiveError::Corrupt(format!("saved block payload could not be read: {error}"))
            })?;
            page.blocks.push(GapBlock {
                lo: block.lo,
                hi: block.hi,
                count: u64::from(row.get::<_, u32>(2)?),
                payload: STANDARD.encode(payload),
                checksum: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
            });
        }
        Ok(page)
    }

    pub fn commit_gaps(
        &mut self,
        worker: &str,
        token: &str,
        report: &GapReport,
        guard: impl FnOnce(usize) -> Result<()>,
    ) -> Result<()> {
        let stored = serde_json::to_string(report)
            .map_err(|error| invalid(&format!("gap report could not be encoded: {error}")))?;
        let current = now();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (span, count, state, expires, prior) = tx
            .query_row(
                "SELECT lo,hi,prime_count,state,expires,report FROM gap_reports
                 WHERE token=?1 AND worker=?2",
                params![token, worker],
                |row| {
                    Ok((
                        read_span(row, 0)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(lease_conflict)?;
        if state == "complete" {
            return if prior.as_deref() == Some(stored.as_str()) {
                Ok(())
            } else {
                Err(conflict(
                    "gap report lease was committed with different results",
                ))
            };
        }
        if state != "running" || expires.is_none_or(|expiry| expiry <= current) {
            return Err(lease_conflict());
        }
        validate_report(span, &count, report)?;
        guard(stored.len())?;
        tx.execute(
            "UPDATE gap_reports SET state='complete',report=?1,expires=NULL WHERE token=?2",
            params![stored, token],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn fail_gaps(&mut self, worker: &str, token: &str, error: &str) -> Result<()> {
        let error = error.trim();
        if error.is_empty() || error.len() > MAX_ERROR_BYTES {
            return Err(invalid("gap report error must be between 1 and 512 bytes"));
        }
        let changed = self.db.execute(
            "UPDATE gap_reports SET state='failed',error=?1,expires=NULL
             WHERE token=?2 AND worker=?3
               AND ((state='running' AND expires>?4) OR (state='failed' AND error=?1))",
            params![error, token, worker, now()],
        )?;
        if changed == 0 {
            return Err(lease_conflict());
        }
        Ok(())
    }
}
