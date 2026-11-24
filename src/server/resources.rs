//! Coordinator process readings and a rolling, minute-resolution history.
use super::App;
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const DB_NAME: &str = "resource-history.sqlite";
const MINUTES: i64 = 1440;
// Allow for SQLite page growth, a WAL checkpoint, and the shared-memory index.
const HISTORY_WRITE_HEADROOM: u64 = 1 << 20;
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// File and filesystem measurements collected independently by the coordinator.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Storage {
    pub archive_bytes: Option<u64>,
    pub quota_bytes: Option<u64>,
    pub volume_used_bytes: Option<u64>,
    pub volume_total_bytes: Option<u64>,
    pub writable_bytes: Option<u64>,
    pub volume_shared: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Sample {
    pub sampled_at: i64,
    pub cpu_percent: Option<f64>,
    pub memory_bytes: Option<String>,
    pub archive_bytes: Option<String>,
    pub quota_bytes: Option<String>,
    pub volume_used_bytes: Option<String>,
    pub volume_total_bytes: Option<String>,
    pub writable_bytes: Option<String>,
    pub volume_shared: bool,
}

#[derive(Default)]
pub(super) struct Monitor(Mutex<Option<Sample>>);

impl Monitor {
    pub(super) fn latest(&self) -> Option<Sample> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

fn process_cpu() -> Option<Duration> {
    let mut ts = std::mem::MaybeUninit::uninit();
    // SAFETY: on success clock_gettime initializes the supplied timespec.
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, ts.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: clock_gettime returned success above.
    let ts = unsafe { ts.assume_init() };
    Some(Duration::new(
        ts.tv_sec.try_into().ok()?,
        ts.tv_nsec.try_into().ok()?,
    ))
}

#[cfg(any(target_os = "linux", test))]
fn rss_from_status(status: &str) -> Option<u64> {
    let mut fields = status
        .lines()
        .find_map(|s| s.strip_prefix("VmRSS:"))?
        .split_whitespace();
    let kib = fields.next()?.parse::<u64>().ok()?;
    (fields.next()? == "kB")
        .then(|| kib.checked_mul(1024))
        .flatten()
}

fn resident_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        rss_from_status(&std::fs::read_to_string("/proc/self/status").ok()?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct CpuDelta {
    used: f64,
    elapsed: f64,
    // Portion of the interval in the minute before the current sample.
    previous_fraction: f64,
}

#[derive(Default)]
struct CpuMeter(Option<(Instant, Duration, Duration)>);

impl CpuMeter {
    fn delta(&mut self, now: Instant, utc: Duration, cpu: Option<Duration>) -> Option<CpuDelta> {
        let previous = std::mem::replace(&mut self.0, cpu.map(|value| (now, utc, value)));
        let (last, last_utc, last_cpu) = previous?;
        let elapsed = now.checked_duration_since(last)?;
        let utc_elapsed = utc.checked_sub(last_utc)?;
        // Discard suspended or skipped sampling intervals and discontinuities in UTC.
        if elapsed.is_zero()
            || utc_elapsed.is_zero()
            || elapsed > 2 * SAMPLE_INTERVAL
            || elapsed.abs_diff(utc_elapsed) > Duration::from_secs(1)
        {
            return None;
        }
        let boundary = Duration::from_secs(utc.as_secs() / 60 * 60);
        let previous_fraction =
            boundary.saturating_sub(last_utc).as_secs_f64() / utc_elapsed.as_secs_f64();
        Some(CpuDelta {
            used: cpu?.checked_sub(last_cpu)?.as_secs_f64(),
            elapsed: elapsed.as_secs_f64(),
            previous_fraction,
        })
    }
}

fn writer(directory: &Path) -> rusqlite::Result<Connection> {
    let db = Connection::open(directory.join(DB_NAME))?;
    db.busy_timeout(Duration::from_millis(200))?;
    db.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA wal_autocheckpoint=64;
         PRAGMA journal_size_limit=262144;
         CREATE TABLE IF NOT EXISTS samples(
             minute INTEGER PRIMARY KEY,
             sample TEXT NOT NULL,
             cpu_seconds REAL NOT NULL,
             wall_seconds REAL NOT NULL
         );",
    )?;
    Ok(db)
}

fn epoch_seconds() -> i64 {
    i64::try_from(super::now()).unwrap_or(i64::MAX)
}

fn minute_start(at: i64) -> i64 {
    at - at % 60
}

fn earliest(minute: i64) -> i64 {
    minute.saturating_sub((MINUTES - 1) * 60)
}

fn record(db: &mut Connection, sample: &Sample, delta: Option<CpuDelta>) -> rusqlite::Result<()> {
    let minute = minute_start(sample.sampled_at);
    let json = serde_json::to_string(sample)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let tx = db.transaction()?;
    tx.execute(
        "DELETE FROM samples WHERE minute < ?1 OR minute > ?2",
        params![earliest(minute), minute],
    )?;
    let mut current_cpu = 0.0;
    let mut current_wall = 0.0;
    if let Some(d) = delta {
        if d.previous_fraction > 0.0 {
            // Update CPU only. Memory and storage belong to the minute of observation.
            // A missing observation in the previous minute stays a gap.
            tx.execute(
                "UPDATE samples SET cpu_seconds=cpu_seconds+?1, wall_seconds=wall_seconds+?2 WHERE minute=?3",
                params![d.used*d.previous_fraction, d.elapsed*d.previous_fraction, minute-60],
            )?;
        }
        current_cpu = d.used * (1.0 - d.previous_fraction);
        current_wall = d.elapsed * (1.0 - d.previous_fraction);
    }
    tx.execute(
        "INSERT INTO samples(minute,sample,cpu_seconds,wall_seconds) VALUES (?1,?2,?3,?4)
         ON CONFLICT(minute) DO UPDATE SET sample=excluded.sample,
         cpu_seconds=cpu_seconds+excluded.cpu_seconds, wall_seconds=wall_seconds+excluded.wall_seconds",
        params![minute,json,current_cpu,current_wall],
    )?;
    tx.commit()
}

fn read_history(db: &Connection, now: i64) -> rusqlite::Result<Vec<Sample>> {
    let end = minute_start(now);
    let mut query = db.prepare("SELECT minute, sample, 100.0*cpu_seconds/NULLIF(wall_seconds,0) FROM samples WHERE minute BETWEEN ?1 AND ?2 ORDER BY minute")?;
    query
        .query_map(params![earliest(end), end], |r| {
            let json: String = r.get(1)?;
            let mut sample: Sample = serde_json::from_str(&json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            sample.sampled_at = r.get(0)?;
            sample.cpu_percent = r.get(2)?;
            Ok(sample)
        })?
        .collect()
}

pub(super) fn history(directory: &Path) -> rusqlite::Result<Vec<Sample>> {
    let db =
        Connection::open_with_flags(directory.join(DB_NAME), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_millis(200))?;
    read_history(&db, epoch_seconds())
}

#[derive(Default)]
struct Collector {
    cpu: CpuMeter,
    db: Option<Connection>,
    history_failed: bool,
}

impl Collector {
    fn tick(&mut self, app: &App) {
        let utc = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let delta = self.cpu.delta(Instant::now(), utc, process_cpu());
        let storage = app.resource_storage();
        let sample = Sample {
            sampled_at: i64::try_from(utc.as_secs()).unwrap_or(i64::MAX),
            cpu_percent: delta.map(|d| 100.0 * d.used / d.elapsed),
            memory_bytes: resident_bytes().map(|b| b.to_string()),
            archive_bytes: storage.archive_bytes.map(|b| b.to_string()),
            quota_bytes: storage.quota_bytes.map(|b| b.to_string()),
            volume_used_bytes: storage.volume_used_bytes.map(|b| b.to_string()),
            volume_total_bytes: storage.volume_total_bytes.map(|b| b.to_string()),
            writable_bytes: storage.writable_bytes.map(|b| b.to_string()),
            volume_shared: storage.volume_shared,
        };
        *app.shared
            .resources
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(sample.clone());
        if !storage
            .writable_bytes
            .is_some_and(|bytes| bytes >= HISTORY_WRITE_HEADROOM)
        {
            return;
        }
        let result = (|| {
            if self.db.is_none() {
                self.db = Some(writer(&app.shared.config.archive_dir)?);
            }
            record(self.db.as_mut().expect("writer was opened"), &sample, delta)
        })();
        if let Err(error) = result {
            self.db = None;
            if !self.history_failed {
                eprintln!("coordinator resource history unavailable: {error}");
            }
            self.history_failed = true;
        } else {
            self.history_failed = false;
        }
    }
}

pub(super) async fn run(app: App) {
    let mut collector = Collector::default();
    let mut timer = tokio::time::interval(SAMPLE_INTERVAL);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        let app = app.clone();
        let result = tokio::task::spawn_blocking(move || {
            collector.tick(&app);
            collector
        })
        .await;
        collector = result.unwrap_or_else(|e| {
            eprintln!("coordinator resource sampler failed: {e}");
            Collector::default()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::{TestDir, test_app};

    #[test]
    fn cpu_uses_monotonic_elapsed_and_resets_after_missing_measurements() {
        let mut meter = CpuMeter::default();
        let start = Instant::now();
        let s = Duration::from_secs;
        assert_eq!(meter.delta(start, s(0), Some(s(0))), None);
        let d = meter.delta(start + s(5), s(5), Some(s(10))).unwrap();
        assert_eq!((d.used, d.elapsed, d.previous_fraction), (10.0, 5.0, 0.0));
        assert_eq!(meter.delta(start + s(10), s(10), None), None);
        assert_eq!(meter.delta(start + s(15), s(15), Some(s(14))), None);
        assert_eq!(meter.delta(start + s(35), s(35), Some(s(24))), None);
        assert_eq!(meter.delta(start + s(40), s(65), Some(s(30))), None);
        assert_eq!(meter.delta(start + s(45), s(40), Some(s(35))), None);
        assert!(process_cpu().is_some());
        assert_eq!(rss_from_status("VmHWM:  6 kB\nVmRSS:\t5 kB\n"), Some(5120));
        assert_eq!(rss_from_status("VmRSS:  5 MB"), None);
    }

    #[test]
    fn minute_history_survives_restart_weights_cpu_and_prunes_with_gaps() {
        let dir = TestDir::new();
        let app = test_app(&dir, 1_000_000_000, 1_000_000);
        let path = &app.shared.config.archive_dir;
        let mut collector = Collector::default();
        collector.tick(&app);
        let mut sample = app.shared.resources.latest().unwrap();
        assert!(sample.cpu_percent.is_none());
        let minute = minute_start(sample.sampled_at);
        let mut db = collector.db.take().unwrap();
        sample.sampled_at = minute - (MINUTES + 1) * 60;
        record(&mut db, &sample, None).unwrap();
        sample.sampled_at = minute - 180;
        record(&mut db, &sample, None).unwrap();
        sample.sampled_at = minute;
        sample.memory_bytes = Some("12".into());
        record(
            &mut db,
            &sample,
            Some(CpuDelta {
                used: 1.,
                elapsed: 5.,
                previous_fraction: 0.,
            }),
        )
        .unwrap();
        drop(db);
        let mut reopened = writer(path).unwrap();
        sample.sampled_at += 4;
        sample.memory_bytes = Some("30".into());
        record(
            &mut reopened,
            &sample,
            Some(CpuDelta {
                used: 5.,
                elapsed: 10.,
                previous_fraction: 0.,
            }),
        )
        .unwrap();
        let rows = history(path).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].sampled_at, minute - 180);
        assert_eq!(rows[1].sampled_at, minute);
        assert_eq!(rows[1].cpu_percent, Some(40.0));
        assert_eq!(rows[1].memory_bytes.as_deref(), Some("30"));
        assert!(
            read_history(&reopened, minute + MINUTES * 60)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn cpu_crossing_minute_boundary_updates_both_buckets() {
        let dir = TestDir::new();
        let app = test_app(&dir, 1_000_000_000, 1_000_000);
        let mut collector = Collector::default();
        collector.tick(&app);
        let mut db = collector.db.take().unwrap();
        let mut sample = app.shared.resources.latest().unwrap();
        let start = Instant::now();
        let s = Duration::from_secs;
        let mut meter = CpuMeter::default();
        meter.delta(start, s(54), Some(s(0)));
        sample.sampled_at = 58;
        sample.memory_bytes = Some("123".into());
        record(
            &mut db,
            &sample,
            meter.delta(start + s(4), s(58), Some(s(0))),
        )
        .unwrap();
        sample.sampled_at = 63;
        sample.memory_bytes = Some("321".into());
        record(
            &mut db,
            &sample,
            meter.delta(start + s(9), s(63), Some(s(5))),
        )
        .unwrap();
        let rows = read_history(&db, 65).unwrap();
        assert_eq!(rows.len(), 2);
        assert!((rows[0].cpu_percent.unwrap() - 100. / 3.).abs() < 1e-10);
        assert_eq!(rows[1].cpu_percent, Some(100.));
        assert_eq!(rows[0].memory_bytes.as_deref(), Some("123"));
        assert_eq!(rows[1].memory_bytes.as_deref(), Some("321"));
    }

    #[test]
    fn telemetry_yields_to_mining_when_headroom_is_tight() {
        let dir = TestDir::new();
        let app = test_app(&dir, 1_000_000, 10_000);
        let headroom = app.resource_storage().writable_bytes.unwrap();
        assert!(headroom > 0 && headroom < HISTORY_WRITE_HEADROOM);
        let mut collector = Collector::default();
        collector.tick(&app);
        assert!(app.shared.resources.latest().is_some());
        assert!(!app.shared.config.archive_dir.join(DB_NAME).exists());
        assert!(app.gap_space(10_000 + headroom).is_ok());
    }

    #[test]
    fn history_io_failure_preserves_live_sample_and_recovers() {
        let dir = TestDir::new();
        let app = test_app(&dir, 1_000_000_000, 1_000_000);
        let path = app.shared.config.archive_dir.join(DB_NAME);
        std::fs::create_dir(&path).unwrap();
        let mut collector = Collector::default();
        collector.tick(&app);
        assert!(app.shared.resources.latest().is_some());
        assert!(collector.history_failed);
        assert!(
            !app.shared
                .storage_fault
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        std::fs::remove_dir(&path).unwrap();
        collector.tick(&app);
        assert!(!collector.history_failed);
        assert!(app.shared.resources.latest().unwrap().cpu_percent.is_some());
        assert_eq!(
            history(&app.shared.config.archive_dir)
                .unwrap()
                .last()
                .unwrap()
                .sampled_at,
            minute_start(app.shared.resources.latest().unwrap().sampled_at)
        );
    }
}
