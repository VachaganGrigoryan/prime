#!/usr/bin/env python3
"""Compare worker upload modes on disposable, empty Prime Finder archives."""

import argparse
import base64
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import re
import secrets
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Api:
    def __init__(self, origin, token=None, *, worker=False):
        self.origin = origin
        self.opener = build_opener(ProxyHandler({}), NoRedirect())
        self.headers = {}
        if worker:
            self.headers["Authorization"] = f"Bearer {token}"
        elif token is not None:
            credentials = base64.b64encode(f"admin:{token}".encode()).decode("ascii")
            self.headers["Authorization"] = f"Basic {credentials}"

    def open(self, path, body=None, timeout=10):
        headers = self.headers.copy()
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(body).encode()
        request = Request(self.origin + path, data=body, headers=headers)
        return self.opener.open(request, timeout=timeout)

    def json(self, path, body=None):
        with self.open("/api/v1" + path, body) as response:
            return json.load(response)

    def export(self, lo, hi, revision):
        query = urlencode({"lo": lo, "hi": hi, "revision": revision})
        digest = hashlib.sha256()
        lines = 0
        with self.open("/api/v1/export?" + query, timeout=60) as response:
            for chunk in iter(lambda: response.read(128 << 10), b""):
                digest.update(chunk)
                lines += chunk.count(b"\n")
        return digest.hexdigest(), lines


def check_origin(value, *, atlas):
    parsed = urlsplit(value)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
            or parsed.port is None or parsed.username is not None
            or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise ValueError("Expected an HTTP origin on 127.0.0.1")
    if atlas and not 19000 <= parsed.port <= 19999:
        raise ValueError("Benchmark tunnels must use ports 19000–19999")
    return f"http://127.0.0.1:{parsed.port}"


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextmanager
def child(command, env, output):
    with output.open("wb") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def hook_json(hook, *args):
    result = subprocess.run([hook, *args], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout)


def wait_ready(api, coordinator, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if coordinator is not None and coordinator.poll() is not None:
            raise RuntimeError(f"Coordinator exited with status {coordinator.returncode}")
        try:
            return api.json("/status")
        except HTTPError:
            raise
        except (URLError, TimeoutError):
            time.sleep(0.1)
    raise RuntimeError("Isolated coordinator did not become ready")


@contextmanager
def trial_endpoint(args, root, trial):
    mode, slots, repeat = trial
    identity = [mode, str(slots), str(repeat), str(args.lo), str(args.hi)]
    env = os.environ.copy()
    if args.prepare is not None:
        env["PRIME_ADMIN_TOKEN"] = os.environ["PRIME_BENCH_ADMIN_TOKEN"]
        env["PRIME_WORKER_TOKEN"] = os.environ["PRIME_BENCH_WORKER_TOKEN"]
        try:
            endpoints = hook_json(args.prepare, *identity)
            origin = check_origin(endpoints["origin"], atlas=True)
            measure = (lambda when: hook_json(args.metrics, origin, when)) if args.metrics else None
            yield (Api(origin, env["PRIME_ADMIN_TOKEN"]),
                   Api(check_origin(endpoints["read_origin"], atlas=True)), env, None, measure)
        finally:
            subprocess.run([args.cleanup, *identity], check=True, timeout=120)
        return
    env["PRIME_ADMIN_TOKEN"] = secrets.token_hex(32)
    env["PRIME_WORKER_TOKEN"] = secrets.token_hex(32)
    if args.atlas_image:
        from atlas_benchmark import AtlasTrial, PRIVATE_PORT, READ_PORT

        stage = AtlasTrial(args.atlas_image, args.atlas_memory, direct=args.atlas_direct)
        with stage.endpoints(env, root / "tunnel.log") as tunnel:
            yield (Api(f"http://127.0.0.1:{PRIVATE_PORT}", env["PRIME_ADMIN_TOKEN"]),
                   Api(f"http://127.0.0.1:{READ_PORT}"), env, tunnel, lambda _: stage.metrics())
        return
    port, read_port = free_port(), free_port()
    while read_port == port:
        read_port = free_port()
    origin = f"http://127.0.0.1:{port}"
    command = [
        args.binary, "serve", "--bind", f"127.0.0.1:{port}",
        "--public-bind", f"127.0.0.1:{read_port}", "--archive-dir", str(root / "archive"),
        "--web-root", str(root / "web"), "--reserve-bytes", str(64 << 20),
    ]
    with child(command, env, root / "coordinator.log") as coordinator:
        yield (Api(origin, env["PRIME_ADMIN_TOKEN"]),
               Api(f"http://127.0.0.1:{read_port}"), env, coordinator, None)


def public_reads(api, lo, hi, stop, interval, samples):
    path = "/api/v1/summary?" + urlencode({"lo": lo, "hi": hi, "buckets": 64})
    while not stop.is_set():
        start = time.monotonic()
        try:
            with api.open(path, timeout=5) as response:
                response.read()
                status = response.status
        except HTTPError as error:
            status = error.code
            error.close()
        except (URLError, TimeoutError, OSError):
            status = 0
        samples.append((status, (time.monotonic() - start) * 1000))
        stop.wait(interval)


def phase_times(log):
    phases = {"compute_seconds": [], "upload_seconds": [], "upload_requests": []}
    with log.open(errors="replace") as source:
        for line in source:
            for phase, samples in phases.items():
                value = re.search(rf"\b{phase}=([0-9.]+)", line)
                if value:
                    samples.append(float(value[1]))
    return {f"median_lease_{phase}": statistics.median(samples)
            for phase, samples in phases.items() if samples}


def run_trial(args, trial, root):
    mode, slots, repeat = trial
    print(f"{mode}, {slots} slots, run {repeat}", file=sys.stderr, flush=True)
    with trial_endpoint(args, root, trial) as (api, public, env, coordinator, measure):
        initial = wait_ready(api, coordinator)
        if initial["revision"] != "0" or initial["jobs"] or initial["workers"]:
            raise RuntimeError("Refusing to benchmark an archive with existing data, jobs, or workers")
        if initial["storage"]["paused"]:
            raise RuntimeError("Isolated archive is paused")
        if public.json("/capabilities") != {"mode": "public"}:
            raise RuntimeError("Expected the read-only coordinator on the read origin")
        if mode == "batch":
            try:
                Api(api.origin, env["PRIME_WORKER_TOKEN"], worker=True).json("/worker/commit-batch", [])
            except HTTPError as error:
                supported = error.code == 400
                error.close()
                if not supported:
                    raise RuntimeError("The isolated coordinator did not accept batch upload probing") from None
            else:
                raise RuntimeError("The isolated coordinator unexpectedly accepted an empty batch")
        before = measure("before") if measure else None
        args_range = {"lo": str(args.lo), "hi": str(args.hi)}
        job = api.json("/jobs", args_range)
        if job["state"] != "queued" or job["covered"] != "0" or job["count"] != "0":
            raise RuntimeError("Benchmark job has prior coverage or is unavailable")
        command = [args.binary, "--coordinator", api.origin, "worker",
                   "--id", f"bench-{mode}-{slots}", "--threads", str(slots)]
        if mode == "batch":
            command += ["--batch-upload"]
        worker_log = root / "worker.log"
        samples = []
        progress = []
        stop = threading.Event()
        reads = threading.Thread(target=public_reads, daemon=True, args=(
            public, args.lo, args.hi, stop, args.read_interval, samples,
        ))
        reads.start()
        try:
            start = time.monotonic()
            with child(command, env, worker_log) as worker:
                while True:
                    current = api.json("/jobs/" + job["id"])
                    duration = time.monotonic() - start
                    covered = int(current["covered"])
                    count = int(current["count"])
                    if not progress or progress[-1]["covered"] != covered:
                        progress.append({"seconds": round(duration, 3), "covered": covered, "primes": count})
                    if current["state"] == "complete":
                        break
                    if worker.poll() is not None:
                        raise RuntimeError(f"Worker exited with status {worker.returncode}")
                    if current["state"] not in ("queued", "running"):
                        raise RuntimeError(f"Benchmark job entered state {current['state']}")
                    if duration > args.timeout:
                        raise RuntimeError(f"Benchmark job timed out after {duration:.1f} seconds")
                    time.sleep(args.poll_interval)
        finally:
            stop.set()
            reads.join(timeout=6)
        if current["lo"] != args_range["lo"] or current["hi"] != args_range["hi"] or covered != args.hi - args.lo + 1:
            raise RuntimeError("Completed archive job reported unexpected bounds or coverage")
        revision = api.json("/status")["revision"]
        digest, exported_count = api.export(args.lo, args.hi, revision)
        if count != exported_count:
            raise RuntimeError(f"Expected {count} exported primes, received {exported_count}")
        after = measure("after") if measure else None
        memory_gate = False
        if before is not None and after is not None:
            memory_gate = all(type(before.get(key)) is int and before[key] == after.get(key)
                              for key in ("restart_count", "oom_kill"))
        latencies = sorted(elapsed for status, elapsed in samples if status == 200)
        p95 = latencies[math.ceil(0.95 * len(latencies)) - 1] if latencies else None
        return {
            "mode": mode, "slots": slots, "repeat": repeat, "seconds": duration,
            "first_and_last_progress": [progress[0], progress[-1]],
            "durable_samples": len(progress), "primes": count, "covered": covered,
            "primes_per_second": count / duration, "revision": revision, "sha256": digest,
            "public_reads": len(samples), "public_errors": dict(sorted(
                (str(status), sum(code == status for code, _ in samples))
                for status in {code for code, _ in samples if code != 200}
            )),
            "public_p95_ms": p95, "metrics_before": before, "metrics_after": after,
            "measured_no_oom_or_restart": memory_gate, **phase_times(worker_log),
        }


def compare(trials, slots):
    outputs = {(trial["covered"], trial["primes"], trial["sha256"]) for trial in trials}
    if len(outputs) != 1:
        raise RuntimeError("Exact job totals or export SHA-256 differ between trials")
    rates = {}
    for mode, threads in itertools.product(("single", "batch"), slots):
        samples = [run["primes_per_second"] for run in trials
                   if run["mode"] == mode and run["slots"] == threads]
        if samples:
            rates.setdefault(mode, {})[threads] = statistics.median(samples)
    if len(rates) != 2:
        return {"median_primes_per_second": rates, "comparison_available": False}
    best_single = max(rates["single"].values())
    best_slots = max(rates["batch"], key=rates["batch"].get)
    gain = (rates["batch"][best_slots] / best_single - 1) * 100 if best_single else 0.0
    return {"median_primes_per_second": rates, "comparison_available": True,
            "exact_outputs_match": True, "best_batch_slots": best_slots,
            "improvement_percent_vs_best_single": gain, "meets_20_percent_gate": gain >= 20}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/release/prime-finder")
    parser.add_argument("--lo", type=int, default=99_731_564_545)
    parser.add_argument("--hi", type=int, default=100_000_000_000)
    parser.add_argument("--slots", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--mode", choices=("both", "single", "batch"), default="both")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=3600, help="Maximum seconds per job")
    parser.add_argument("--poll-interval", type=float, default=0.05)
    parser.add_argument("--read-interval", type=float, default=0.25)
    parser.add_argument("--atlas-image", help="Benchmark an already imported image on Atlas")
    parser.add_argument("--atlas-memory", choices=("512Mi", "1Gi"), default="512Mi")
    parser.add_argument("--atlas-direct", action="store_true", help="Use the Atlas LAN connection directly")
    parser.add_argument("--prepare", help="Start a disposable Atlas archive and print its endpoints")
    parser.add_argument("--cleanup", help="Stop and remove the disposable Atlas archive")
    parser.add_argument("--metrics", help="Report restart_count, oom_kill and other staging metrics")
    parser.add_argument("--log-dir", type=Path, help="Keep coordinator and worker process logs here")
    args = parser.parse_args()
    if not 0 <= args.lo <= args.hi < 1 << 64:
        parser.error("range bounds must be ascending u64 values")
    if len(set(args.slots)) != len(args.slots) or any(not 1 <= n <= 32 for n in args.slots):
        parser.error("slot counts must be distinct integers between 1 and 32")
    if bool(args.prepare) != bool(args.cleanup):
        parser.error("--prepare and --cleanup must be supplied together")
    if args.atlas_image and (args.prepare or args.metrics):
        parser.error("--atlas-image manages the staging pod and metrics without external hooks")
    if args.atlas_direct and not args.atlas_image:
        parser.error("--atlas-direct requires --atlas-image")
    if args.metrics and not args.prepare:
        parser.error("--metrics requires --prepare")
    if args.prepare and not all(os.environ.get(key) for key in ("PRIME_BENCH_ADMIN_TOKEN", "PRIME_BENCH_WORKER_TOKEN")):
        parser.error("set the dedicated PRIME_BENCH_ADMIN_TOKEN and PRIME_BENCH_WORKER_TOKEN")
    if args.prepare and any(
            os.environ.get(f"PRIME_BENCH_{role}_TOKEN") == os.environ.get(f"PRIME_{role}_TOKEN")
            for role in ("ADMIN", "WORKER")
    ):
        parser.error("benchmark tokens must be separate from existing coordinator credentials")
    if args.repeats < 1 or any(not math.isfinite(value) or value <= 0 for value in (
            args.timeout, args.poll_interval, args.read_interval)):
        parser.error("repeats, timeout, and polling intervals must be positive")
    args.binary = str(Path(args.binary).resolve(strict=True))
    if args.log_dir:
        args.log_dir.mkdir(parents=True, exist_ok=True)
    return args


def main():
    args = parse_args()
    trials = []
    with tempfile.TemporaryDirectory(prefix="prime-benchmark-", dir=args.log_dir) as directory:
        logs = Path(directory)
        for repeat in range(1, args.repeats + 1):
            modes = ("single", "batch") if repeat % 2 else ("batch", "single")
            for slots, mode in itertools.product(args.slots, modes):
                if args.mode != "both" and mode != args.mode:
                    continue
                root = logs / f"{mode}-{slots}-{repeat}"
                root.mkdir()
                try:
                    trials.append(run_trial(args, (mode, slots, repeat), root))
                finally:
                    if args.log_dir:
                        for log in root.glob("*.log"):
                            (args.log_dir / f"{logs.name}-{root.name}-{log.name}").write_bytes(log.read_bytes())
        report = compare(trials, args.slots)
        report["trials"] = trials
        report["range"] = {"lo": str(args.lo), "hi": str(args.hi)}
        report["mixed_workload_30_minute_soak_passed"] = bool((args.prepare or args.atlas_image) and any(
            run["mode"] == "batch" and run["seconds"] >= 1800
            and run["public_reads"] > sum(run["public_errors"].values())
            and run["measured_no_oom_or_restart"]
            for run in trials
        ))
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(f"benchmark: {error}") from None
