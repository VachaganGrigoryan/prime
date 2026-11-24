mod contribute;
mod gap_compute;
mod server;
mod worker;

use clap::{Parser, Subcommand};
use prime_finder::archive::Span;
use std::{
    io::{self, Write},
    net::SocketAddr,
    path::PathBuf,
    time::Instant,
};
use worker::Client;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Parser)]
#[command(about = "Enumerate and archive all primes in inclusive u64 ranges")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8787", global = true)]
    coordinator: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
        /// Optional listener for the public website and read-only archive API.
        #[arg(long)]
        public_bind: Option<SocketAddr>,
        /// Optional listener for public volunteer enrollment and submissions.
        #[arg(long)]
        contribute_bind: Option<SocketAddr>,
        #[arg(long, default_value = "prime-data")]
        archive_dir: PathBuf,
        #[arg(long, default_value = "dist")]
        web_root: PathBuf,
        #[arg(long, default_value_t = 100_000_000_000)]
        quota_bytes: u64,
        #[arg(long, default_value_t = 5_000_000_000)]
        reserve_bytes: u64,
        #[arg(long)]
        mount_id: Option<String>,
    },
    Worker {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..=32))]
        threads: u16,
        #[arg(long, default_value_t = 100_000_000, value_parser = clap::value_parser!(u32).range(2..=100_000_000))]
        base_limit: u32,
        /// Send several leased blocks together for one durable archive commit.
        #[arg(long)]
        batch_upload: bool,
        /// Recompute staged volunteer blocks on one trusted worker slot.
        #[arg(long)]
        verify_contributions: bool,
        /// Use volunteer evidence for eligible blocks before committing the exact results.
        #[arg(long, conflicts_with = "local_presieve")]
        use_volunteer_evidence: bool,
        /// Compute a local pre-sieve for the high-range benchmark control.
        #[arg(long)]
        local_presieve: bool,
    },
    Submit {
        lo: u64,
        hi: u64,
    },
    Status,
    Pause {
        id: i64,
    },
    Resume {
        id: i64,
    },
    Cancel {
        id: i64,
    },
    Page {
        lo: u64,
        hi: u64,
        #[arg(long)]
        after: Option<u64>,
        #[arg(long)]
        revision: Option<i64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    Summary {
        lo: u64,
        hi: u64,
        #[arg(long, default_value_t = 64)]
        buckets: usize,
        #[arg(long)]
        revision: Option<i64>,
        #[arg(long)]
        cursor: Option<u64>,
    },
    Export {
        lo: u64,
        hi: u64,
    },
    /// Compute a gap report on the Mac from NDJSON pages of archived prime blocks on stdin.
    Analyze {
        lo: u64,
        hi: u64,
        prime_count: u64,
    },
    Bench {
        lo: u64,
        hi: u64,
        #[arg(long, default_value_t = 100_000_000, value_parser = clap::value_parser!(u32).range(2..=100_000_000))]
        base_limit: u32,
    },
}

fn secret(name: &str) -> Result<String> {
    let value = std::env::var(name)?;
    if value.len() < 16 || value.bytes().any(|b| !b.is_ascii_graphic()) {
        return Err(format!(
            "{name} must contain at least 16 printable non-space ASCII characters"
        )
        .into());
    }
    Ok(value)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            bind,
            public_bind,
            contribute_bind,
            archive_dir,
            web_root,
            quota_bytes,
            reserve_bytes,
            mount_id,
        } => {
            server::serve(server::Config {
                bind,
                public_bind,
                contribute_bind,
                archive_dir,
                web_root,
                quota_bytes,
                reserve_bytes,
                mount_id,
            })
            .await
        }
        Command::Worker {
            id,
            threads,
            base_limit,
            batch_upload,
            verify_contributions,
            use_volunteer_evidence,
            local_presieve,
        } => {
            let evidence = if use_volunteer_evidence {
                worker::EvidenceMode::Volunteer
            } else if local_presieve {
                worker::EvidenceMode::Local
            } else {
                worker::EvidenceMode::Off
            };
            worker::run(
                cli.coordinator,
                id,
                threads,
                base_limit,
                batch_upload,
                verify_contributions,
                evidence,
            )
            .await
        }
        Command::Analyze {
            lo,
            hi,
            prime_count,
        } => {
            if !cfg!(target_os = "macos") {
                return Err("Run gap analysis on the Mac".into());
            }
            let report =
                gap_compute::analyze_archive(Span { lo, hi }, prime_count, io::stdin().lock())?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Command::Bench { lo, hi, base_limit } => {
            if lo > hi || u128::from(hi) - u128::from(lo) + 1 > 16 * (1 << 20) {
                return Err("bench requires an ascending interval of at most 2^24 integers".into());
            }
            let engine = prime_finder::engine::Engine::new(base_limit);
            for phase in ["cold", "warm"] {
                let start = Instant::now();
                let primes = engine.enumerate(lo, hi);
                let elapsed = start.elapsed().as_secs_f64();
                let payload = prime_finder::archive::encode(Span { lo, hi }, &primes)?;
                println!(
                    "{}",
                    serde_json::json!({
                        "phase": phase, "count": primes.len(), "seconds": elapsed,
                        "primes_per_second": primes.len() as f64 / elapsed, "payload_bytes": payload.len(),
                    })
                );
            }
            Ok(())
        }
        command => {
            let client = Client::admin(cli.coordinator)?;
            let value = match command {
                Command::Submit { lo, hi } => client.post("/jobs", &Span { lo, hi }).await?,
                Command::Status => client.get("/status").await?,
                Command::Pause { id } => client.action(id, "pause").await?,
                Command::Resume { id } => client.action(id, "resume").await?,
                Command::Cancel { id } => client.action(id, "cancel").await?,
                Command::Page {
                    lo,
                    hi,
                    after,
                    revision,
                    limit,
                } => {
                    let mut path = format!("/primes?lo={lo}&hi={hi}&limit={limit}");
                    if let Some(after) = after {
                        path.push_str(&format!("&after={after}"));
                    }
                    if let Some(revision) = revision {
                        path.push_str(&format!("&revision={revision}"));
                    }
                    client.get(&path).await?
                }
                Command::Summary {
                    lo,
                    hi,
                    buckets,
                    revision,
                    cursor,
                } => {
                    let mut path = format!("/summary?lo={lo}&hi={hi}&buckets={buckets}");
                    if let Some(revision) = revision {
                        path.push_str(&format!("&revision={revision}"));
                    }
                    if let Some(cursor) = cursor {
                        path.push_str(&format!("&cursor={cursor}"));
                    }
                    client.get(&path).await?
                }
                Command::Export { lo, hi } => {
                    let mut response = client.download(&format!("/export?lo={lo}&hi={hi}")).await?;
                    let mut stdout = io::stdout().lock();
                    while let Some(chunk) = response.chunk().await? {
                        stdout.write_all(&chunk)?;
                    }
                    return Ok(());
                }
                _ => unreachable!("commands handled before creating the client"),
            };
            println!("{}", serde_json::to_string_pretty(&value)?);
            Ok(())
        }
    }
}
