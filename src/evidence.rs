//! Exact classification of high-range prime blocks from volunteered evidence.

use crate::block_codec::BLOCK_WIDTH;
use crate::engine::{
    Engine, RESIDUES, STEPS, first_wheel_value, is_certificate_factor_prime, is_prime, mod_pow,
    segmented, wheel_candidates,
};
use std::fmt;
use std::sync::{Arc, OnceLock};

/// Wheel-ordered little-endian u32 divisors, followed by optional certificates
/// ordered by u32 block offset. Each certificate encodes a base and factor
/// count as bytes, then a little-endian u64 and u8 exponent per prime factor.
pub const KIND: &str = "divisors-v1";
pub const MIN_LO: u64 = 10_000_000_000_000_000;
pub const MAX_BYTES: usize = 2 * 1024 * 1024;
const SIEVE_LIMIT: u32 = 1_000_000;
const CERT_TRIAL_LIMIT: u32 = 997;
const MAX_FACTORS: usize = 8;
const MAX_CERT_PRIME: u64 = 1_000_000_000_000;
const FACTOR_BYTES: usize = size_of::<u64>() + 1;
const CERT_HEADER: usize = size_of::<u32>() + 2;
const BEFORE: [u64; 31] = {
    let mut counts = [0; 31];
    let mut end = 1;
    let mut index = 0;
    while end < counts.len() {
        if index < RESIDUES.len() && RESIDUES[index] < end as u64 {
            index += 1;
        }
        counts[end] = index as u64;
        end += 1;
    }
    counts
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvidenceError(&'static str);

impl fmt::Display for EvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for EvidenceError {}

fn check_span(lo: u64, hi: u64) -> Result<(), EvidenceError> {
    if lo < MIN_LO
        || hi
            .checked_sub(lo)
            .is_none_or(|distance| distance >= BLOCK_WIDTH)
    {
        return Err(EvidenceError("span is not a high-range archive block"));
    }
    Ok(())
}

// Count wheel candidates strictly below the value without incrementing the
// bound, which may already be u64::MAX.
fn ordinal(value: u64) -> u64 {
    value / 30 * 8 + BEFORE[(value % 30) as usize]
}

fn slots(lo: u64, hi: u64) -> usize {
    let last = hi / 30 * 8 + BEFORE[(hi % 30) as usize + 1];
    (last - ordinal(lo)) as usize
}

/// Produces one little-endian u32 per candidate in ascending wheel order.
/// A zero leaves the classification to the trusted worker.
pub fn generate(engine: &Engine, lo: u64, hi: u64) -> Result<Vec<u8>, EvidenceError> {
    check_span(lo, hi)?;
    let first = ordinal(lo);
    let mut divisors = vec![0_u32; slots(lo, hi)];
    let primes = engine.base_primes(SIEVE_LIMIT);
    for divisor in primes
        .iter()
        .copied()
        .skip_while(|&p| p < 7)
        .take_while(|&p| p <= SIEVE_LIMIT)
    {
        let p = u64::from(divisor);
        let Some((factor, mut index)) = first_wheel_value(lo.div_ceil(p)) else {
            continue;
        };
        let Some(mut n) = factor.checked_mul(p) else {
            continue;
        };
        while n <= hi {
            divisors[(ordinal(n) - first) as usize] = divisor;
            let Some(next) = n.checked_add(p * STEPS[index]) else {
                break;
            };
            n = next;
            index = (index + 1) % STEPS.len();
        }
    }
    Ok(divisors.into_iter().flat_map(u32::to_le_bytes).collect())
}

/// Appends sparse Pocklington certificates to the divisor evidence. The
/// generator spends a fixed budget on factoring each candidate's predecessor.
/// Unsuccessful candidates retain their local Miller–Rabin fallback.
pub fn generate_with_certificates(
    engine: &Engine,
    lo: u64,
    hi: u64,
) -> Result<Vec<u8>, EvidenceError> {
    let mut proof = generate(engine, lo, hi)?;
    let factors = proof.clone();
    let trial = engine.base_primes(CERT_TRIAL_LIMIT);
    let mut divisors = factors.as_chunks::<4>().0.iter();
    wheel_candidates(lo, hi, |n| {
        let divisor = divisors.next().expect("generated every wheel slot");
        if *divisor != [0; 4] || proof.len() + CERT_HEADER + MAX_FACTORS * FACTOR_BYTES > MAX_BYTES
        {
            return;
        }
        if is_prime(n)
            && let Some((base, factors)) = make_certificate(n, &trial)
        {
            proof.extend_from_slice(&((n - lo) as u32).to_le_bytes());
            proof.push(base);
            proof.push(factors.len() as u8);
            for (prime, exponent) in factors {
                proof.extend_from_slice(&prime.to_le_bytes());
                proof.push(exponent);
            }
        }
    });
    Ok(proof)
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

fn bounded_factor(n: u64) -> Option<u64> {
    // Pollard rho with Brent's cycle schedule. Batch gcds keep the cost
    // bounded even when the cofactor has no easy-to-find divisor.
    let modulus = u128::from(n);
    let multiply = |a, b| ((u128::from(a) * u128::from(b)) % modulus) as u64;
    let step = |x| ((u128::from(x) * u128::from(x) + 1) % modulus) as u64;
    let mut y = 2_u64;
    let mut span: usize = 1;
    while span <= 1_024 {
        let x = y;
        for _ in 0..span {
            y = step(y);
        }
        for offset in (0..span).step_by(32) {
            let size = (span - offset).min(32);
            let mut start = y;
            let mut product = 1;
            for _ in 0..size {
                y = step(y);
                product = multiply(product, x.abs_diff(y));
            }
            let divisor = gcd(product, n);
            if (2..n).contains(&divisor) {
                return Some(divisor);
            }
            if divisor == n {
                for _ in 0..size {
                    start = step(start);
                    let divisor = gcd(x.abs_diff(start), n);
                    if (2..n).contains(&divisor) {
                        return Some(divisor);
                    }
                }
                return None;
            }
        }
        span *= 2;
    }
    None
}

fn make_certificate(n: u64, trial: &[u32]) -> Option<(u8, Vec<(u64, u8)>)> {
    let mut remainder = n - 1;
    let mut primes = Vec::new();
    for q in trial
        .iter()
        .copied()
        .take_while(|&q| q <= CERT_TRIAL_LIMIT)
        .map(u64::from)
    {
        while remainder.is_multiple_of(q) {
            primes.push(q);
            remainder /= q;
        }
    }
    let mut pending = vec![remainder];
    // The known prime powers can suffice without finding every factor.
    for _ in 0..9 {
        let Some(value) = pending.pop() else { break };
        if value == 1 {
            continue;
        }
        if is_prime(value) {
            primes.push(value);
        } else if let Some(divisor) = bounded_factor(value) {
            let other = value / divisor;
            pending.push(divisor.max(other));
            pending.push(divisor.min(other));
        }
    }
    primes.retain(|&q| q <= MAX_CERT_PRIME);
    primes.sort_unstable();
    let mut powers: Vec<(u64, u8, u64)> = Vec::new();
    for q in primes {
        if let Some((previous, exponent, power)) = powers.last_mut()
            && *previous == q
        {
            *exponent += 1;
            *power *= q;
        } else {
            powers.push((q, 1, q));
        }
    }
    powers.sort_unstable_by_key(|&(_, _, power)| std::cmp::Reverse(power));
    let mut factors = Vec::new();
    let (root, mut product) = (n.isqrt(), 1);
    for (q, exponent, power) in powers.into_iter().take(MAX_FACTORS) {
        factors.push((q, exponent));
        product *= power;
        if product > root {
            factors.sort_unstable_by_key(|&(q, _)| q);
            return (2..=32)
                .find(|&base| pocklington_checks(n, base, factors.iter().map(|&(q, _)| q)))
                .map(|base| (base, factors));
        }
    }
    None
}

fn pocklington_checks(n: u64, base: u8, factors: impl Iterator<Item = u64>) -> bool {
    let mut divisors = [0; MAX_FACTORS];
    let mut used = 0;
    let mut radical = 1;
    for q in factors {
        divisors[used] = q;
        used += 1;
        radical *= q;
    }
    // The radical divides n - 1. The Fermat and per-factor checks each
    // share the same base a^((n - 1) / radical) and its repeated squares.
    let a = mod_pow(u64::from(base), (n - 1) / radical, n);
    let multiply = |v, w| ((u128::from(v) * u128::from(w)) % u128::from(n)) as u64;
    let mut squares = [0; u64::BITS as usize];
    squares[0] = a;
    for i in 1..=radical.ilog2() as usize {
        squares[i] = multiply(squares[i - 1], squares[i - 1]);
    }
    let power = |mut exponent: u64| {
        let mut result = 1;
        while exponent != 0 {
            result = multiply(result, squares[exponent.trailing_zeros() as usize]);
            exponent &= exponent - 1;
        }
        result
    };
    power(radical) == 1
        && divisors[..used]
            .iter()
            .all(|&q| gcd(power(radical / q).abs_diff(1), n) == 1)
}

fn is_certificate_prime(q: u64) -> bool {
    static SMALL: OnceLock<Arc<Vec<u32>>> = OnceLock::new();
    if q <= u64::from(SIEVE_LIMIT) {
        SMALL
            .get_or_init(|| Engine::new(SIEVE_LIMIT).base_primes(SIEVE_LIMIT))
            .binary_search(&(q as u32))
            .is_ok()
    } else {
        is_certificate_factor_prime(q)
    }
}

struct Certificate<'a> {
    offset: u32,
    base: u8,
    powers: &'a [[u8; FACTOR_BYTES]],
}

fn next_certificate<'a>(tail: &mut &'a [u8]) -> Result<Option<Certificate<'a>>, EvidenceError> {
    if tail.is_empty() {
        return Ok(None);
    }
    let Some((header, rest)) = tail.split_first_chunk::<CERT_HEADER>() else {
        return Err(EvidenceError("truncated certificate header"));
    };
    let offset = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let base = header[4];
    let count = usize::from(header[5]);
    if !(2..=32).contains(&base) || !(1..=MAX_FACTORS).contains(&count) {
        return Err(EvidenceError("invalid certificate fields"));
    }
    let Some((encoded, rest)) = rest.split_at_checked(count * FACTOR_BYTES) else {
        return Err(EvidenceError("truncated certificate factors"));
    };
    *tail = rest;
    Ok(Some(Certificate {
        offset,
        base,
        powers: encoded.as_chunks().0,
    }))
}

impl Certificate<'_> {
    fn proves(&self, n: u64) -> bool {
        let mut previous = 1;
        let mut remainder = n - 1;
        let mut factors = [0; MAX_FACTORS];
        for (bytes, factor) in self.powers.iter().zip(&mut factors) {
            let (encoded, exponent) = bytes.split_first_chunk::<8>().expect("fixed factor width");
            let q = u64::from_le_bytes(*encoded);
            if q <= previous
                || q > MAX_CERT_PRIME
                || exponent[0] == 0
                || !remainder.is_multiple_of(q)
                || !is_certificate_prime(q)
            {
                return false;
            }
            previous = q;
            *factor = q;
            for _ in 0..exponent[0] {
                if !remainder.is_multiple_of(q) {
                    return false;
                }
                remainder /= q;
            }
        }
        // Each certified prime power divides n - 1, so division recovers the
        // product without overflow. F > floor(sqrt(n)) is equivalent to F² > n.
        (n - 1) / remainder > n.isqrt()
            && pocklington_checks(n, self.base, factors.into_iter().take(self.powers.len()))
    }
}

/// Reconstructs the exact, sorted prime list for the trusted span. An invalid
/// divisor cannot suppress a candidate, and the result is rejected if any slot
/// is malformed so callers can fall back to local generation.
pub fn verify(lo: u64, hi: u64, bytes: &[u8]) -> Result<Vec<u64>, EvidenceError> {
    check_span(lo, hi)?;
    let factor_bytes = slots(lo, hi) * size_of::<u32>();
    if !(factor_bytes..=MAX_BYTES).contains(&bytes.len()) {
        return Err(EvidenceError("unexpected divisor evidence length"));
    }
    let (raw_factors, mut tail) = bytes.split_at(factor_bytes);
    let mut factors = raw_factors.as_chunks::<4>().0.iter();
    let mut cert = next_certificate(&mut tail)?;
    let mut primes = Vec::new();
    let mut valid = Ok(());
    wheel_candidates(lo, hi, |n| {
        if valid.is_err() {
            return;
        }
        let Some(bytes) = factors.next() else {
            valid = Err(EvidenceError("invalid wheel slot"));
            return;
        };
        let factor = u64::from(u32::from_le_bytes(*bytes));
        let offset = (n - lo) as u32;
        if let Some(certificate) = cert.as_ref().filter(|proof| proof.offset <= offset) {
            if certificate.offset != offset || factor != 0 || !certificate.proves(n) {
                valid = Err(EvidenceError("invalid prime certificate"));
                return;
            }
            primes.push(n);
            match next_certificate(&mut tail) {
                Ok(next) => cert = next,
                Err(error) => valid = Err(error),
            }
        } else if factor == 0 {
            if is_prime(n) {
                primes.push(n);
            }
        } else if factor <= 1 || factor >= n || !n.is_multiple_of(factor) {
            valid = Err(EvidenceError("invalid divisor in evidence"));
        }
    });
    valid?;
    if cert.is_some() || factors.next().is_some() {
        return Err(EvidenceError("evidence has no matching wheel slot"));
    }
    Ok(primes)
}

/// Runs the same pre-sieve locally with the normal packed wheel bitset.
pub fn local_presieve(engine: &Engine, lo: u64, hi: u64) -> Result<Vec<u64>, EvidenceError> {
    check_span(lo, hi)?;
    let base = engine.base_primes(SIEVE_LIMIT);
    let end = base.partition_point(|&p| p <= SIEVE_LIMIT);
    let mut primes = Vec::new();
    segmented(lo, hi, &base[..end], |n| {
        if is_prime(n) {
            primes.push(n);
        }
    });
    Ok(primes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supplied_and_local_evidence_match_exact_engine() {
        let engine = Engine::new(100_000_000);
        for (lo, hi) in [
            (10_u64.pow(18), 10_u64.pow(18) + 32_768),
            (u64::MAX - 333, u64::MAX),
            (u64::MAX, u64::MAX),
        ] {
            let expected = engine.enumerate(lo, hi);
            let bytes = generate(&engine, lo, hi).unwrap();
            assert!(bytes.len() <= MAX_BYTES);
            assert_eq!(verify(lo, hi, &bytes).unwrap(), expected);
            assert_eq!(verify(lo, hi, &vec![0; bytes.len()]).unwrap(), expected);
            assert_eq!(local_presieve(&engine, lo, hi).unwrap(), expected);
        }
    }

    #[test]
    fn invalid_evidence_and_spans_are_rejected() {
        let n = 18_446_744_073_709_551_557;
        assert!(verify(n, n, &1_u32.to_le_bytes()).is_err());
        assert!(verify(n, n, &23_u32.to_le_bytes()).is_err());
        assert!(verify(n, n, &[]).is_err());
        assert!(verify(n, n, &[0; 5]).is_err());
        assert!(verify(n, n - 1, &[]).is_err());
        assert!(verify(MIN_LO - 1, MIN_LO, &[]).is_err());
        assert!(verify(MIN_LO, MIN_LO + BLOCK_WIDTH, &[]).is_err());
    }

    #[test]
    fn certificates_match_exact_engine_and_reject_tampering() {
        let engine = Engine::new(SIEVE_LIMIT);
        let lo = 10_u64.pow(18);
        let hi = lo + 32_767;
        let expected = engine.enumerate(lo, hi);
        let bytes = generate_with_certificates(&engine, lo, hi).unwrap();
        let base_len = slots(lo, hi) * size_of::<u32>();
        assert!(bytes.len() > base_len);
        assert_eq!(verify(lo, hi, &bytes).unwrap(), expected);
        assert_eq!(verify(lo, hi, &bytes[..base_len]).unwrap(), expected);

        // Corrupt the first certificate's factor to 1.
        let mut invalid = bytes.clone();
        invalid[base_len + CERT_HEADER..base_len + CERT_HEADER + 8]
            .copy_from_slice(&1_u64.to_le_bytes());
        assert!(verify(lo, hi, &invalid).is_err());
        assert!(verify(lo, hi, &bytes[..bytes.len() - 1]).is_err());
        // An offset off the wheel cannot silently suppress or insert a value.
        let mut invalid = bytes;
        invalid[base_len..base_len + 4].copy_from_slice(&0_u32.to_le_bytes());
        assert!(verify(lo, hi, &invalid).is_err());
    }
}
