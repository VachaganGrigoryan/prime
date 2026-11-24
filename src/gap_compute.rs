use crate::Result;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prime_finder::{
    archive::{Span, decode},
    gap_store::{GapBlock, GapFrequency, GapPage, GapRecord, GapReport},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::BufRead};

pub struct GapAccumulator {
    span: Span,
    next: u128,
    previous: Option<u64>,
    count: u64,
    histogram: BTreeMap<u64, u64>,
    largest: Vec<(u64, u64, u64)>,
}

impl GapAccumulator {
    pub fn new(span: Span) -> Result<Self> {
        span.validate()?;
        Ok(Self {
            span,
            next: u128::from(span.lo),
            previous: None,
            count: 0,
            histogram: BTreeMap::new(),
            largest: Vec::with_capacity(20),
        })
    }

    pub fn push(&mut self, block: GapBlock) -> Result<()> {
        let payload = STANDARD.decode(block.payload)?;
        if format!("{:x}", Sha256::digest(&payload)) != block.checksum {
            return Err("Archived prime block checksum mismatch".into());
        }
        let span = Span {
            lo: block.lo,
            hi: block.hi,
        };
        self.push_primes(span, &decode(span, block.count, &payload)?)
    }

    fn push_primes(&mut self, block: Span, values: &[u64]) -> Result<()> {
        let lo = block.lo.max(self.span.lo);
        let hi = block.hi.min(self.span.hi);
        if lo > hi || u128::from(lo) != self.next {
            return Err("Gap analysis requires consecutive, fully checked blocks".into());
        }
        let from = values.partition_point(|&value| value < lo);
        let to = values.partition_point(|&value| value <= hi);
        for &prime in &values[from..to] {
            if let Some(left) = self.previous {
                let gap = prime
                    .checked_sub(left)
                    .filter(|&gap| gap > 0)
                    .ok_or("Archive primes must be strictly increasing")?;
                *self.histogram.entry(gap).or_default() += 1;
                if self.largest.len() < 20 || self.largest.last().is_some_and(|entry| gap > entry.0)
                {
                    let at = self.largest.partition_point(|&(value, earlier, _)| {
                        value > gap || (value == gap && earlier < left)
                    });
                    self.largest.insert(at, (gap, left, prime));
                    self.largest.truncate(20);
                }
            }
            self.previous = Some(prime);
            self.count += 1;
        }
        self.next = u128::from(hi) + 1;
        Ok(())
    }

    pub fn finish(self, expected: u64) -> Result<GapReport> {
        if self.next != u128::from(self.span.hi) + 1 || self.count != expected {
            return Err("Gap report did not cover the requested archive snapshot".into());
        }
        Ok(GapReport {
            prime_count: self.count.to_string(),
            gap_count: self.count.saturating_sub(1).to_string(),
            twin_count: self.histogram.get(&2).copied().unwrap_or(0).to_string(),
            histogram: self
                .histogram
                .into_iter()
                .map(|(gap, count)| GapFrequency {
                    gap: gap.to_string(),
                    count: count.to_string(),
                })
                .collect(),
            largest: self
                .largest
                .into_iter()
                .map(|(gap, left, right)| GapRecord {
                    gap: gap.to_string(),
                    left: left.to_string(),
                    right: right.to_string(),
                })
                .collect(),
        })
    }
}

pub fn analyze_archive(span: Span, count: u64, input: impl BufRead) -> Result<GapReport> {
    let mut acc = GapAccumulator::new(span)?;
    for line in input.lines() {
        let page: GapPage = serde_json::from_str(&line?)?;
        for block in page.blocks {
            acc.push(block)?;
        }
    }
    acc.finish(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn clipped_blocks_carry_adjacency_across_an_empty_block() {
        let mut report = GapAccumulator::new(Span { lo: 3, hi: 12 }).unwrap();
        report
            .push_primes(Span { lo: 0, hi: 7 }, &[2, 3, 5, 7])
            .unwrap();
        report.push_primes(Span { lo: 8, hi: 10 }, &[]).unwrap();
        report
            .push_primes(Span { lo: 11, hi: 20 }, &[11, 13, 17, 19])
            .unwrap();
        assert_eq!(
            serde_json::to_value(report.finish(4).unwrap()).unwrap(),
            json!({
                "prime_count":"4","gap_count":"3","twin_count":"2",
                "histogram":[{"gap":"2","count":"2"},{"gap":"4","count":"1"}],
                "largest":[{"gap":"4","left":"7","right":"11"},{"gap":"2","left":"3","right":"5"},{"gap":"2","left":"5","right":"7"}],
            })
        );
    }

    #[test]
    fn rejects_holes_and_supports_max_endpoint() {
        let mut report = GapAccumulator::new(Span {
            lo: u64::MAX - 59,
            hi: u64::MAX,
        })
        .unwrap();
        assert!(
            report
                .push_primes(
                    Span {
                        lo: u64::MAX - 58,
                        hi: u64::MAX
                    },
                    &[]
                )
                .is_err()
        );
        report
            .push_primes(
                Span {
                    lo: u64::MAX - 59,
                    hi: u64::MAX,
                },
                &[u64::MAX - 58],
            )
            .unwrap();
        assert_eq!(report.finish(1).unwrap().gap_count, "0");
    }
}
