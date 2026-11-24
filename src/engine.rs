//! Exact enumeration of unsigned 64-bit primes.

use std::sync::{Arc, RwLock};

const SEGMENT_BYTES: usize = 32 * 1024;
const SEGMENT_WIDTH: u64 = SEGMENT_BYTES as u64 * 30;
pub(crate) const RESIDUES: [u64; 8] = [1, 7, 11, 13, 17, 19, 23, 29];
pub(crate) const STEPS: [u64; 8] = [6, 4, 2, 4, 2, 4, 6, 2];
const RESIDUE_BITS: [u8; 30] = {
    let mut masks = [0; 30];
    let mut i = 0;
    while i < RESIDUES.len() {
        masks[RESIDUES[i] as usize] = 1 << i;
        i += 1;
    }
    masks
};

const SMALL_PRIMES: [u64; 12] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37];
// These bases detect every odd composite below 2^64.
// Forišek and Jančina, §3: https://ceur-ws.org/Vol-1326/020-Forisek.pdf
const WITNESSES: [u64; 7] = [2, 325, 9_375, 28_178, 450_775, 9_780_504, 1_795_265_022];

struct BasePrimes {
    limit: u32,
    values: Arc<Vec<u32>>,
}

/// A thread-safe prime enumerator with an in-process base-prime cache.
pub struct Engine {
    base_limit: u32,
    base: RwLock<BasePrimes>,
}

impl Engine {
    /// Sets the maximum square root eligible for the segmented sieve.
    /// The cache starts empty and grows only when a sieve request needs it.
    pub fn new(base_limit: u32) -> Self {
        Self {
            base_limit,
            base: RwLock::new(BasePrimes {
                limit: 1,
                values: Arc::new(Vec::new()),
            }),
        }
    }

    /// Enumerates primes in a single inclusive work batch in ascending order.
    /// Workers divide large jobs into bounded batches before calling this.
    pub fn enumerate(&self, lo: u64, hi: u64) -> Vec<u64> {
        if lo > hi || hi < 2 {
            return Vec::new();
        }
        let root = hi.isqrt() as u32;
        let width = u128::from(hi) - u128::from(lo) + 1;
        let mut primes = Vec::new();
        if root <= self.base_limit && width >= u128::from(100_000_u32.max(root / 8)) {
            let base = self.base_primes(root);
            segmented(lo, hi, &base, |prime| primes.push(prime));
        } else {
            wheel_candidates(lo, hi, |candidate| {
                if is_prime(candidate) {
                    primes.push(candidate);
                }
            });
        }
        primes
    }

    pub(crate) fn base_primes(&self, needed: u32) -> Arc<Vec<u32>> {
        {
            let base = self.base.read().expect("base-prime cache poisoned");
            if base.limit >= needed {
                return Arc::clone(&base.values);
            }
        }

        let mut base = self.base.write().expect("base-prime cache poisoned");
        if base.limit < needed {
            // Geometric growth amortizes repeated calls with similar upper bounds.
            let limit = needed.max(base.limit.saturating_mul(2).min(self.base_limit));
            let start = u64::from(base.limit) + 1;
            let seeds = small_sieve(limit.isqrt());
            let values = Arc::make_mut(&mut base.values);
            segmented(start, u64::from(limit), &seeds, |p| {
                values.push(p as u32);
            });
            base.limit = limit;
        }
        Arc::clone(&base.values)
    }
}

/// Determines primality exactly for all unsigned 64-bit inputs.
pub fn is_prime(n: u64) -> bool {
    is_prime_with_bases(n, &WITNESSES, mod_mul)
}

fn is_prime_with_bases(n: u64, witnesses: &[u64], multiply: impl Fn(u64, u64, u64) -> u64) -> bool {
    if n < 2 {
        return false;
    }
    for p in SMALL_PRIMES {
        if n.is_multiple_of(p) {
            return n == p;
        }
    }
    strong_prime_tests(n, witnesses, multiply)
}

pub(crate) fn is_certificate_factor_prime(n: u64) -> bool {
    // Jaeschke, §5 provides the strict lower bounds 4,759,123,141 and
    // 10^12 for composites passing these respective witness sets.
    // https://cr.yp.to/bib/1993/jaeschke.pdf
    if n <= u64::from(u32::MAX) {
        is_prime_with_bases(n, &[2, 7, 61], |a, b, modulus| a * b % modulus)
    } else if n <= 1_000_000_000_000 {
        is_prime_with_bases(n, &[2, 13, 23, 1_662_803], mod_mul)
    } else {
        is_prime(n)
    }
}

fn strong_prime_tests(n: u64, witnesses: &[u64], multiply: impl Fn(u64, u64, u64) -> u64) -> bool {
    let rounds = (n - 1).trailing_zeros();
    let odd_part = (n - 1) >> rounds;
    'witness: for witness in witnesses {
        let base = witness % n;
        if base == 0 {
            continue;
        }
        let mut residue = pow_with(base, odd_part, n, &multiply);
        if residue == 1 || residue == n - 1 {
            continue;
        }
        for _ in 1..rounds {
            residue = multiply(residue, residue, n);
            if residue == n - 1 {
                continue 'witness;
            }
        }
        return false;
    }
    true
}

fn mod_mul(a: u64, b: u64, modulus: u64) -> u64 {
    (u128::from(a) * u128::from(b) % u128::from(modulus)) as u64
}

pub(crate) fn mod_pow(base: u64, exponent: u64, modulus: u64) -> u64 {
    pow_with(base, exponent, modulus, &mod_mul)
}

fn pow_with(
    mut base: u64,
    mut exponent: u64,
    modulus: u64,
    multiply: &impl Fn(u64, u64, u64) -> u64,
) -> u64 {
    let mut result = 1;
    while exponent != 0 {
        if exponent & 1 != 0 {
            result = multiply(result, base, modulus);
        }
        exponent >>= 1;
        if exponent != 0 {
            base = multiply(base, base, modulus);
        }
    }
    result
}

fn small_sieve(limit: u32) -> Vec<u32> {
    let mut composite = vec![false; limit as usize + 1];
    for p in 2..=limit.isqrt() {
        if !composite[p as usize] {
            for multiple in (p * p..=limit).step_by(p as usize) {
                composite[multiple as usize] = true;
            }
        }
    }
    (2..=limit).filter(|&p| !composite[p as usize]).collect()
}

pub(crate) fn first_wheel_value(value: u64) -> Option<(u64, usize)> {
    let rem = value % 30;
    let index = RESIDUES.partition_point(|&residue| residue < rem);
    Some(((value - rem).checked_add(RESIDUES[index])?, index))
}

pub(crate) fn wheel_candidates(lo: u64, hi: u64, mut emit: impl FnMut(u64)) {
    [2, 3, 5]
        .into_iter()
        .filter(|&p| lo <= p && p <= hi)
        .for_each(&mut emit);
    let Some((mut candidate, mut index)) = first_wheel_value(lo.max(7)) else {
        return;
    };
    while candidate <= hi {
        emit(candidate);
        let Some(next) = candidate.checked_add(STEPS[index]) else {
            break;
        };
        candidate = next;
        index = (index + 1) % STEPS.len();
    }
}

pub(crate) fn segmented(lo: u64, hi: u64, base: &[u32], mut emit: impl FnMut(u64)) {
    [2, 3, 5]
        .into_iter()
        .filter(|&p| lo <= p && p <= hi)
        .for_each(&mut emit);
    if hi < 7 {
        return;
    }
    let lo = lo.max(7);
    let mut start = lo - lo % 30;
    let mut composite = [0_u8; SEGMENT_BYTES];
    loop {
        let end = start.saturating_add(SEGMENT_WIDTH - 1).min(hi);
        let active = &mut composite[..((end - start) / 30 + 1) as usize];
        active.fill(0);
        for p in base.iter().copied().skip_while(|&p| p < 7).map(u64::from) {
            if p * p > end {
                break;
            }
            let multiplier = start.div_ceil(p).max(p);
            let Some((factor, mut index)) = first_wheel_value(multiplier) else {
                continue;
            };
            let Some(mut multiple) = factor.checked_mul(p) else {
                continue;
            };
            while multiple <= end {
                let offset = multiple - start;
                active[(offset / 30) as usize] |= RESIDUE_BITS[(offset % 30) as usize];
                let Some(next) = multiple.checked_add(p * STEPS[index]) else {
                    break;
                };
                multiple = next;
                index = (index + 1) % STEPS.len();
            }
        }
        for (index, &bits) in active.iter().enumerate() {
            let block = start + index as u64 * 30;
            let mut remaining = !bits;
            while remaining != 0 {
                let residue = RESIDUES[remaining.trailing_zeros() as usize];
                if residue > end - block {
                    break;
                }
                let prime = block + residue;
                if lo <= prime {
                    emit(prime);
                }
                remaining &= remaining - 1;
            }
        }
        if end == hi {
            break;
        }
        start = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleton_and_zero_prime_intervals() {
        let engine = Engine::new(100_000_000);
        assert!(engine.enumerate(0, 1).is_empty());
        assert!(engine.enumerate(14, 16).is_empty());
        assert!(engine.enumerate(4, 3).is_empty());
        assert_eq!(engine.enumerate(2, 2), vec![2]);
        assert_eq!(engine.enumerate(5, 7), vec![5, 7]);
    }

    #[test]
    fn large_unsigned_values_and_pseudoprimes() {
        let engine = Engine::new(100_000_000);
        assert_eq!(
            engine.enumerate(u64::MAX - 59, u64::MAX),
            vec![18_446_744_073_709_551_557]
        );
        assert!(engine.enumerate(u64::MAX, u64::MAX).is_empty());
        assert_eq!(
            engine.enumerate((1_u64 << 63) - 20, (1_u64 << 63) + 39),
            vec![9_223_372_036_854_775_837]
        );
        assert!(!is_prime(3_825_123_056_546_413_051));
        assert!(!is_prime(4_294_967_291_u64.pow(2)));
    }

    #[test]
    fn certificate_factor_primality_matches_the_full_test() {
        for (lo, hi) in [
            (0, 128),
            (u64::from(u32::MAX) - 128, u64::from(u32::MAX) + 128),
            (4_759_123_141 - 128, 4_759_123_141 + 128),
            (1_000_000_000_000 - 128, 1_000_000_000_000 + 128),
        ] {
            for n in lo..=hi {
                assert_eq!(is_certificate_factor_prime(n), is_prime(n), "{n}");
            }
        }
    }

    #[test]
    fn both_paths_agree_including_segment_edge() {
        let sieve = Engine::new(100_000_000);
        let miller_rabin = Engine::new(0);
        let stop = 1_000_000;
        let expected: Vec<_> = small_sieve(stop).into_iter().map(u64::from).collect();
        assert_eq!(expected.len(), 78_498);
        assert_eq!(sieve.enumerate(0, u64::from(stop)), expected);
        assert_eq!(miller_rabin.enumerate(0, u64::from(stop)), expected);

        let square = 1_009_u64.pow(2);
        let lo = square - SEGMENT_WIDTH - 1;
        let hi = square + 100;
        let expected = miller_rabin.enumerate(lo, hi);
        assert_eq!(sieve.enumerate(lo, hi), expected);
        assert!(!expected.contains(&square));
    }

    #[test]
    fn cache_growth_preserves_in_flight_readers() {
        let engine = Engine::new(100_000);
        let snapshot = engine.base_primes(100);
        let grown = engine.base_primes(200);
        assert_eq!(snapshot.as_ref(), &small_sieve(100));
        assert_eq!(grown.as_ref(), &small_sieve(200));
        assert!(Arc::ptr_eq(&grown, &engine.base_primes(197)));
    }
}
