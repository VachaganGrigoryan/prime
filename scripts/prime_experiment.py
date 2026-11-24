#!/usr/bin/env python3
"""Generate public Prime research data, freeze forecasts and replay their scores."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from time import sleep
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from mpmath import mp

from prime_models import MODEL_VERSION, endpoint, observed_psi, phase, window, zero_checksum


mp.dps = 50
ORIGIN = "https://prime-api.notallow.cc"
FRONTIER = 148_607_336_447
REVISION = "141726"
U64_MAX = 2**64 - 1
PAIRS = (0, 16, 64)
MODELS = {"pnt": "pnt", "corrected": "corrected", "li": "li", "smooth": "r",
          **{f"z{k}": f"rpsi{k}" for k in PAIRS}}


def decimal(value):
    return mp.nstr(value, 34, min_fixed=-100, max_fixed=100)


def estimates(values):
    return {name: decimal(values[key]) for name, key in MODELS.items()}


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def digest(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def save(path, data, *, replace=False):
    """Publish a complete file atomically; freeze forecasts and score snapshots once."""
    f = tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8")
    temporary = Path(f.name)
    try:
        with f:
            json.dump(data, f, ensure_ascii=False, indent=2, allow_nan=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        temporary.chmod(0o644)
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)



class Archive:
    def __init__(self, origin):
        self.origin = origin.rstrip("/")

    def read(self, path, **params):
        url = self.origin + "/api/v1/" + path
        if params:
            url += "?" + urlencode(params)
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "Prime-Research/1.0"})
        for attempt in range(3):
            try:
                with urlopen(request, timeout=30) as response:
                    return json.load(response)
            except HTTPError as error:
                if error.code not in (502, 503, 504) or attempt == 2:
                    raise
                error.close()
                sleep(2**attempt)

    def pages(self, path, revision, cursor, next_cursor, **params):
        last = None
        while True:
            page = self.read(path, revision=revision, **params)
            if str(page["revision"]) != revision:
                raise ValueError("archive response changed revision")
            yield page
            cursor_value = page[next_cursor]
            if cursor_value is None:
                return
            position = int(cursor_value)
            if last is not None and position <= last:
                raise ValueError("archive pagination failed to advance")
            params[cursor] = last = position

    def summary(self, lo, hi, revision, bins=1):
        """Return inclusive bounds, coverage and count across every pinned page."""
        spans = [(lo + (hi-lo+1)*i//bins, lo + (hi-lo+1)*(i+1)//bins - 1)
                 for i in range(bins)]
        totals = [[0, 0] for _ in spans]
        for page in self.pages("summary", revision, "cursor", "next_cursor", lo=lo, hi=hi, buckets=bins):
            if [(int(b["lo"]), int(b["hi"])) for b in page["buckets"]] != spans:
                raise ValueError("archive returned unexpected bucket bounds")
            for total, bucket in zip(totals, page["buckets"]):
                count, covered = int(bucket["count"]), int(bucket["covered"])
                if not 0 <= count <= covered:
                    raise ValueError("invalid archive coverage or count")
                total[0] += covered
                total[1] += count
        if any(total[0] > b-a+1 for (a,b), total in zip(spans, totals)):
            raise ValueError("archive coverage exceeds interval length")
        return totals

    def primes(self, lo, hi, revision):
        last = lo - 1
        for page in self.pages("primes", revision, "after", "next_after", lo=lo, hi=hi, limit=4096):
            for text in page["primes"]:
                p = int(text)
                if not last < p <= hi:
                    raise ValueError("archive returned unordered or out-of-range primes")
                last = p
                yield p
            if page["next_after"] is not None and int(page["next_after"]) != last:
                raise ValueError("prime cursor does not match the last prime")


def metadata(kind, archive, revision):
    return {"schema": 1, "kind": kind, "origin": archive.origin, "revision": revision,
            "model_version": MODEL_VERSION, "zero_sha256": zero_checksum()}


def prediction_cell(location, lo, width, n):
    if n < 1 or width < 1 or lo < 2 or lo + n*width - 1 > U64_MAX:
        raise ValueError("windows must be nonempty inclusive u64 ranges starting at 2 or above")
    rows = []
    for a in range(lo, lo+n*width, width):
        b = a+width-1
        values = window(a, b)
        rows.append({"lo": str(a), "hi": str(b), "estimates": estimates(values),
                     "psi_estimates": {"smooth": str(width),
                                       **{f"z{k}": decimal(values[f"psi{k}"]) for k in PAIRS}},
                     "phase": {f"z{k}": decimal(phase(a, b, k)) for k in PAIRS}})
    return {"location": location, "lo": str(lo), "hi": str(lo+n*width-1),
            "width": str(width) if width > 2**53-1 else width, "windows": rows}


def archive_psi(archive, revision, lo, hi, expected_count):
    """Count the pinned prime stream as its log weights are accumulated."""
    def verified_stream():
        count = 0
        for prime in archive.primes(lo, hi, revision):
            count += 1
            yield prime
        if count != expected_count:
            raise ValueError("prime list disagrees with pinned summary count")

    return observed_psi(lo, hi, verified_stream())


def score_cell(archive, revision, cell):
    """Read one revision. Incomplete rows retain their forecasts but receive no score."""
    complete, pending = [], []
    all_rows = cell["windows"]
    for start in range(0, len(all_rows), 160):
        rows = all_rows[start:start+160]
        totals = archive.summary(int(rows[0]["lo"]), int(rows[-1]["hi"]), revision, len(rows))
        for row, (covered, count) in zip(rows, totals):
            a, b = int(row["lo"]), int(row["hi"])
            if covered != b-a+1:
                pending.append(dict(row, covered=str(covered)))
                continue
            complete.append(dict(row, count=str(count), covered=str(covered),
                                 psi=decimal(archive_psi(archive, revision, a, b, count))))
    mae = {}
    for observed, models in (("count", "estimates"), ("psi", "psi_estimates")):
        mae[observed] = {key: float(mp.fsum(abs(mp.mpf(row[observed]) - mp.mpf(row[models][key]))
                                           for row in complete) / len(complete)) if complete else None
                         for key in ("smooth", "z0", "z16", "z64")}
    return dict(cell, windows=complete, pending=pending, mae=mae)


def snapshot(archive):
    result = metadata("retrospective", archive, REVISION)
    result["observation_source"] = "Atlas complete summary and prime pages at the stated revision"
    checkpoints = []
    for x, expected in ((1_000_000, 78_498), (FRONTIER, 6_021_313_753)):
        (covered, count), = archive.summary(2, x, REVISION)
        if (covered, count) != (x-1, expected):
            raise ValueError(f"checkpoint mismatch at {x}")
        checkpoints.append({"x": str(x), "count": str(count), "estimates": estimates(endpoint(x))})
    result["checkpoints"], result["frontier"] = checkpoints, checkpoints[-1]
    result["cells"] = []
    for name, end, widths in (("Near one million", 2**20, (256, 16_384)),
                              ("Near 100 million", 100_000_000, (16_384,)),
                              ("Archive frontier", FRONTIER, (256, 16_384, 2**20))):
        for width in widths:
            cell = prediction_cell(name, end+1-8*width, width, 8)
            scored = score_cell(archive, REVISION, cell)
            if scored["pending"]:
                raise ValueError("retrospective contains incomplete windows")
            result["cells"].append(scored)
    pinned_counts = [40659, 40792, 40569, 40808, 40782, 40710, 40817, 40829]
    if [int(w["count"]) for w in result["cells"][-1]["windows"]] != pinned_counts:
        raise ValueError("pinned block counts changed")
    return result


def pending_state(archive, cells):
    progress = archive.read("mining/progress")
    revision = str(progress["revision"])
    ranges = {(int(c["lo"]), int(c["hi"])) for c in cells}
    for lo, hi in ranges:
        if int(progress["verified_through"]) >= lo or any(
            int(span["lo"]) <= hi and lo <= int(span["hi"]) for span in progress["active"]
        ):
            raise ValueError("forecast overlaps the verified frontier or an active lease")
        if archive.summary(lo, hi, revision)[0][0]:
            raise ValueError("forecast contains integers already archived at the capture revision")
    return {"revision": revision, "checked_at": timestamp(), "verified_through": progress["verified_through"]}


def predict(archive, lo, widths, windows):
    if len(set(widths)) != len(widths):
        raise ValueError("forecast widths must be distinct")
    cells = [prediction_cell("Prospective range", lo, width, windows) for width in sorted(widths)]
    capture = pending_state(archive, cells)
    result = dict(metadata("prediction", archive, capture["revision"]), created_at=capture["checked_at"],
                  verified_through=capture["verified_through"], cells=cells)
    result["sha256"] = digest(result)
    return result


def check_manifest(manifest):
    data = {k: v for k, v in manifest.items() if k != "sha256"}
    if manifest["kind"] != "prediction" or digest(data) != manifest["sha256"]:
        raise ValueError("manifest checksum or type mismatch")
    if manifest["model_version"] != MODEL_VERSION or manifest["zero_sha256"] != zero_checksum():
        raise ValueError("replay requires the manifest's model version and zero table")


def verify_pending(manifest):
    """Call after independently reading the published manifest to witness outstanding coverage."""
    check_manifest(manifest)
    archive = Archive(manifest["origin"])
    state = pending_state(archive, manifest["cells"])
    if int(state["revision"]) < int(manifest["revision"]):
        raise ValueError("archive revision predates forecast")
    return dict(metadata("publication-witness", archive, state["revision"]), observed_at=state["checked_at"],
                verified_through=state["verified_through"], manifest_sha256=manifest["sha256"])


def score(manifest, revision):
    check_manifest(manifest)
    archive = Archive(manifest["origin"])
    if revision is None:
        revision = str(archive.read("mining/progress")["revision"])
    if int(revision) < int(manifest["revision"]):
        raise ValueError("scoring revision predates forecast")
    return dict(metadata("score", archive, revision), manifest_sha256=manifest["sha256"],
                prediction_revision=manifest["revision"], created_at=timestamp(),
                cells=[score_cell(archive, revision, c) for c in manifest["cells"]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("snapshot", "predict", "score", "witness"):
        sub = commands.add_parser(name)
        sub.add_argument("--out", type=Path, required=True)
        if name in {"score", "witness"}:
            sub.add_argument("--manifest", type=Path, required=True)
            if name == "score":
                sub.add_argument("--revision")
        else:
            sub.add_argument("--origin", default=ORIGIN)
        if name == "predict":
            sub.add_argument("--lo", type=int, required=True)
            sub.add_argument("--width", type=int, action="append", required=True,
                             help="inclusive window width; repeat to compare scales at the same location")
            sub.add_argument("--windows", type=int, required=True)
    args = parser.parse_args()
    if args.command == "score":
        data = score(json.loads(args.manifest.read_text()), args.revision)
    elif args.command == "witness":
        data = verify_pending(json.loads(args.manifest.read_text()))
    elif args.command == "predict":
        data = predict(Archive(args.origin), args.lo, args.width, args.windows)
    else:
        data = snapshot(Archive(args.origin))
    save(args.out, data, replace=args.command == "snapshot")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError) as error:
        raise SystemExit(str(error))
