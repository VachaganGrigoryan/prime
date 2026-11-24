//! Prime's versioned worker benchmark.

use crate::{block_codec::BLOCK_WIDTH, engine::Engine};
use sha2::{Digest, Sha256};

pub const BENCHMARK_VERSION: u32 = 1;
pub const BENCHMARK_COUNT: u64 = 377_952;
pub const BENCHMARK_DIGEST: &str =
    "1a13d69bef85794b4ad202ad1706695a5e0872ad2af015aae24a6e1978d0facd";

const BENCHMARK_START: u64 = 1 << 32;
const BENCHMARK_BLOCKS: u64 = 8;

/// Hash every enumerated prime in increasing order, encoded as an unsigned
/// 64-bit big-endian integer. Each benchmark block uses the production engine.
pub fn benchmark(engine: &Engine) -> (u64, String) {
    let mut count = 0;
    let mut digest = Sha256::new();
    for index in 0..BENCHMARK_BLOCKS {
        let lo = BENCHMARK_START + index * BLOCK_WIDTH;
        for prime in engine.enumerate(lo, lo + BLOCK_WIDTH - 1) {
            count += 1;
            digest.update(prime.to_be_bytes());
        }
    }
    (count, format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_matches_independent_sieve_fixture() {
        let (count, digest) = benchmark(&Engine::new(100_000_000));
        assert_eq!(count, BENCHMARK_COUNT);
        assert_eq!(digest, BENCHMARK_DIGEST);
    }
}
