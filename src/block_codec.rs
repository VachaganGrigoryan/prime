//! Canonical unsigned variable-length prime gaps used by the archive and workers.

use std::fmt;

/// Largest inclusive integer range assigned as a single archive block.
pub const BLOCK_WIDTH: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodecError(&'static str);

impl CodecError {
    pub fn message(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for CodecError {}

type Result<T> = std::result::Result<T, CodecError>;

fn bounds(lo: u64, hi: u64) -> Result<u128> {
    if lo > hi {
        return Err(CodecError(
            "lower bound must be no greater than upper bound",
        ));
    }
    Ok(u128::from(hi) + 1 - u128::from(lo))
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_varint(input: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *input
            .get(*offset)
            .ok_or(CodecError("truncated prime gap"))?;
        *offset += 1;
        if shift == 63 && byte > 1 {
            return Err(CodecError("prime gap overflows u64"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            if shift != 0 && byte == 0 {
                return Err(CodecError("non-canonical prime gap"));
            }
            return Ok(value);
        }
    }
    Err(CodecError("invalid prime gap"))
}

/// Encodes an ascending prime list with the first gap measured from `lo`.
///
/// # Errors
///
/// Returns an error for invalid bounds or unordered, duplicate, or out-of-range values.
pub fn encode(lo: u64, hi: u64, primes: &[u64]) -> Result<Vec<u8>> {
    bounds(lo, hi)?;
    let mut out = Vec::with_capacity(primes.len());
    let mut previous = lo;
    for (index, &prime) in primes.iter().enumerate() {
        if prime < 2 || prime > hi || prime < previous || (index != 0 && prime == previous) {
            return Err(CodecError(
                "primes must be strictly increasing and within the block",
            ));
        }
        put_varint(&mut out, prime - previous);
        previous = prime;
    }
    Ok(out)
}

/// Decodes exactly `count` values inside the supplied range.
///
/// # Errors
///
/// Rejects invalid bounds, malformed encodings, impossible counts, and invalid values.
pub fn decode(lo: u64, hi: u64, count: u64, payload: &[u8]) -> Result<Vec<u64>> {
    let width = bounds(lo, hi)?;
    if u128::from(count) > width || u128::from(count) > payload.len() as u128 {
        return Err(CodecError("prime count exceeds payload or interval"));
    }
    let mut values = Vec::with_capacity(count as usize);
    let mut offset = 0;
    let mut previous = lo;
    for index in 0..count {
        let gap = take_varint(payload, &mut offset)?;
        if index != 0 && gap == 0 {
            return Err(CodecError("duplicate prime in block"));
        }
        let prime = previous
            .checked_add(gap)
            .filter(|value| (2..=hi).contains(value))
            .ok_or(CodecError("prime outside block"))?;
        values.push(prime);
        previous = prime;
    }
    if offset != payload.len() {
        return Err(CodecError("prime count does not match encoded payload"));
    }
    Ok(values)
}
