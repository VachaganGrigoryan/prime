"""Prime estimate models v1, evaluated with mpmath 1.3.0 at 50 decimal digits.

The zero table is the unmodified LMFDB text export of David Platt's ordinates.
https://www.lmfdb.org/zeros/zeta/list?N=1&limit=64
https://www.lmfdb.org/zeros/zeta/Source
"""

from functools import lru_cache
from hashlib import sha256
from math import isqrt
from pathlib import Path

from mpmath import mp

MODEL_VERSION = "atlas-prime-models-v1"
MAX_INTEGER = 2**64 - 1
ZERO_FILE = Path(__file__).with_name("zeta-zeros-64.txt")
ZERO_SHA256 = "ab0def6e153e5601b230ef139c94438c1e43bc884a5baf62754ba39d917923f3"
PAIR_COUNTS = (0, 16, 64)
DIGITS = 50


@lru_cache(maxsize=1)
def _zero_data():
    raw = ZERO_FILE.read_bytes()
    if sha256(raw).hexdigest() != ZERO_SHA256:
        raise ValueError("The zeta zero table does not match the model checksum")
    with mp.workdps(DIGITS):
        return tuple(mp.mpc("0.5", row.split()[1].decode("ascii"))
                     for row in raw.splitlines())


def zero_checksum():
    _zero_data()
    return ZERO_SHA256


def _check_integer(n):
    if type(n) is not int or not 0 <= n <= MAX_INTEGER:
        raise ValueError("An endpoint must be an unsigned 64-bit integer")


def _check_window(lo, hi):
    _check_integer(lo)
    _check_integer(hi)
    if not 2 <= lo <= hi:
        raise ValueError("A prime window requires 2 <= lo <= hi")


@lru_cache(maxsize=4096)
def endpoint(n):
    """Cumulative model values through integer n. The empty prefix is zero.

    PNT, li and Gram R take n. The explicit formula takes n+1/2 to
    include all prime powers through n without evaluating at a jump.
    """
    _check_integer(n)
    with mp.workdps(DIGITS):
        if n < 2:
            return dict.fromkeys(("pnt", "corrected", "li", "r", "psi0",
                                  "psi16", "psi64", "rpsi0", "rpsi16", "rpsi64"), mp.mpf(0))
        x = mp.mpf(n)
        lx = mp.log(x)
        result = {"pnt": x / lx, "corrected": x / (lx - 1),
                  "li": mp.li(x), "r": mp.riemannr(x)}
        t = x + mp.mpf("0.5")
        log_t = mp.log(t)
        smooth = t - mp.log(2 * mp.pi) - mp.log1p(-1 / t**2) / 2
        terms = []
        zeros = _zero_data()
        for count in PAIR_COUNTS:
            for rho in zeros[len(terms):count]:
                terms.append(2 * mp.re(mp.exp(rho * log_t) / rho))
            psi = smooth - mp.fsum(terms)
            result[f"psi{count}"] = psi
            result[f"rpsi{count}"] = mp.riemannr(psi)
        return result


def window(lo, hi):
    """Model differences on an inclusive interval, retaining u64 resolution."""
    _check_window(lo, hi)
    with mp.workdps(DIGITS):
        before, after = endpoint(lo - 1), endpoint(hi)
        return {name: value - before[name] for name, value in after.items()}


def phase(lo, hi, pairs):
    """Change in the highest retained zero's phase across half-step endpoints."""
    _check_window(lo, hi)
    if pairs not in PAIR_COUNTS:
        raise ValueError("Expected 0, 16 or 64 zero pairs")
    with mp.workdps(DIGITS):
        if not pairs:
            return mp.mpf(0)
        start = mp.mpf(lo) - mp.mpf("0.5")
        return _zero_data()[pairs - 1].imag * mp.log1p((hi - lo + 1) / start)


def _floor_root(n, k):
    if k == 2:
        return isqrt(n)
    left, right = 0, 1 << ((n.bit_length() + k - 1) // k)
    while right - left > 1:
        mid = (left + right) // 2
        if mid**k <= n:
            left = mid
        else:
            right = mid
    return left


def _prime_base(n):
    # Every higher-power base in the u64 domain is <= 2^32-1.
    # Trial division suffices on the short root intervals for these windows.
    if n % 2 == 0:
        return n == 2
    return all(n % p for p in range(3, isqrt(n) + 1, 2))


def observed_psi(lo, hi, primes):
    """Sum log(p) for each p^k in [lo,hi], from verified primes in that window.

    The prime stream must be complete and ordered. Higher powers can have
    bases below lo, so enumerate possible base intervals independently.
    """
    _check_window(lo, hi)

    def weights():
        previous = lo - 1
        for p in primes:
            if type(p) is not int or not previous < p <= hi:
                raise ValueError("Expected strictly ascending in-window integer primes")
            yield mp.log(p)
            previous = p
        for k in range(2, hi.bit_length()):
            first = max(2, _floor_root(lo - 1, k) + 1)
            last = _floor_root(hi, k)
            for p in range(first, last + 1):
                if _prime_base(p):
                    yield mp.log(p)

    with mp.workdps(DIGITS):
        return mp.fsum(weights())
