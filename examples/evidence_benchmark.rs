//! Local durable-write comparison with precomputed evidence. It excludes transport and queueing.

use prime_finder::{
    archive::{Archive, Commit, Span, encode},
    block_codec::BLOCK_WIDTH,
    engine::Engine,
    evidence,
};
use sha2::{Digest, Sha256};
use std::{env, error::Error, fs, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn run(
    path: &Path,
    spans: &[Span],
    supplied: &[Vec<u8>],
    engine: &Engine,
    mode: usize,
) -> Result<(f64, u64, [u8; 32])> {
    let range = Span {
        lo: spans[0].lo,
        hi: spans[spans.len() - 1].hi,
    };
    let mut archive = Archive::open(path)?;
    let job = archive.create_job(range)?;
    let start = Instant::now();
    let mut remainders = supplied.iter();
    while let Some(lease) = archive.claim("benchmark")? {
        for span in lease.blocks {
            let witness = remainders.next().ok_or("unexpected block")?;
            let primes = match mode {
                0 => engine.enumerate(span.lo, span.hi),
                1 => evidence::local_presieve(engine, span.lo, span.hi)?,
                _ => evidence::verify(span.lo, span.hi, witness)?,
            };
            archive.commit(Commit {
                token: lease.token.clone(),
                worker: lease.worker.clone(),
                span,
                count: primes.len() as u64,
                payload: encode(span, &primes)?,
                engine: "evidence-benchmark".into(),
                checksum: None,
            })?;
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    drop(archive);

    let archive = Archive::open(path)?;
    let revision = archive.verify_full(range, None)?;
    let mut after = None;
    let mut hash = Sha256::new();
    let mut count = 0;
    loop {
        let page = archive.page(range, after, 10_000, Some(revision))?;
        for prime in page.primes {
            hash.update(prime.parse::<u64>()?.to_be_bytes());
            count += 1;
        }
        match page.next_after {
            Some(cursor) => after = Some(cursor.parse()?),
            None => break,
        }
    }
    assert_eq!(count, archive.job(job.id)?.count.parse::<u64>()?);
    Ok((elapsed, count, hash.finalize().into()))
}

fn main() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let directory = args.next().ok_or("pass a new output directory")?;
    let generate = match args.next().as_deref() {
        None => evidence::generate,
        Some(flag) if flag == "--certificates" => evidence::generate_with_certificates,
        _ => return Err("expected an optional --certificates flag".into()),
    };
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let root = Path::new(&directory);
    fs::create_dir(root)?;
    let engine = Engine::new(100_000_000);
    for target in [100_000_000_000_000_000_u64, 1_000_000_000_000_000_000_u64] {
        let lo = target / BLOCK_WIDTH * BLOCK_WIDTH;
        let spans = (0..4)
            .map(|i| Span {
                lo: lo + i * BLOCK_WIDTH,
                hi: lo + (i + 1) * BLOCK_WIDTH - 1,
            })
            .collect::<Vec<_>>();
        let start = Instant::now();
        let supplied = spans
            .iter()
            .map(|s| generate(&engine, s.lo, s.hi))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let bytes: usize = supplied.iter().map(Vec::len).sum();
        println!(
            "near={target} blocks=4 evidence_bytes={bytes} generation_seconds={:.6}",
            start.elapsed().as_secs_f64()
        );
        let mut rates = [Vec::new(), Vec::new(), Vec::new()];
        let mut expected = None;
        for round in 0..3 {
            for index in 0..3 {
                let mode = (round + index) % 3;
                let path = root.join(format!("{target}-{round}-{mode}.sqlite"));
                let (seconds, count, digest) = run(&path, &spans, &supplied, &engine, mode)?;
                assert_eq!(*expected.get_or_insert((count, digest)), (count, digest));
                rates[mode].push(count as f64 / seconds);
            }
        }
        for samples in &mut rates {
            samples.sort_by(f64::total_cmp);
        }
        let control = rates[0][1].max(rates[1][1]);
        let gain = (rates[2][1] / control - 1.0) * 100.0;
        let (count, digest) = expected.ok_or("no results")?;
        let digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        println!(
            "near={target} prime_count={count} sha256={digest} native={:.0} local_presieve={:.0} precomputed_evidence={:.0} gain_percent={gain:.2}",
            rates[0][1], rates[1][1], rates[2][1]
        );
    }
    Ok(())
}
