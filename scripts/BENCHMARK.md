# Measure durable prime throughput

Build the current Prime Finder binary on the Mac and run the harness from the Rust repository.

```sh
cargo build --locked --release
python3 scripts/benchmark.py --slots 1 --repeats 1 --lo 0 --hi 1048575
```

This smoke test starts separate disposable loopback coordinators for single and batch uploads. It requires permission to open loopback sockets. For a local A/B run, omit the range and slot arguments. The defaults cover 268,435,456 integers ending at 100 billion and compare 1, 2, 4, and 8 slots. Each trial starts from an empty archive and repeats the same range. The benchmark reverses single and batch order on alternating rounds. Its stdout is JSON.

## Atlas measurement

Run the worker harness on the Mac with an image already imported into Atlas k3s. For example, if the image was imported as `prime-finder:benchmark`:

```sh
python3 scripts/benchmark.py --atlas-image prime-finder:benchmark \
  --atlas-memory 512Mi --log-dir /tmp/prime-benchmark-logs \
  > /tmp/prime-benchmark-512m.json
python3 scripts/benchmark.py --atlas-image prime-finder:benchmark \
  --atlas-memory 1Gi --log-dir /tmp/prime-benchmark-logs \
  > /tmp/prime-benchmark-1g.json
```

Each trial creates a pod and a credential secret in `dev-prime-finder`, with its own eight-GB archive quota and scratch directory under `/NAS/dev/prime-finder-benchmarks`. The Mac connects through loopback SSH forwards on ports 19087 and 19088. Both must be free on the Mac and Atlas. The harness collects cgroup v2 counters, then removes the per-trial pod, secret, and scratch data. The production archive stays on its existing mount. Account for its filesystem differences when interpreting storage and memory results.

Add `--atlas-direct` when the Mac can reach the Atlas LAN address and the SSH alias normally uses the Cloudflare proxy.

For another staging layout, provide executable hooks.

- **Prepare** receives `MODE SLOTS REPEAT LO HI`. It creates the disposable coordinator and tunnels and prints one JSON object such as `{"origin":"http://127.0.0.1:19087","read_origin":"http://127.0.0.1:19088"}`. Both origins must use local ports 19000 through 19999.
- **Cleanup** receives the same arguments. It stops that staging instance and removes its scratch state. It must also work after a partial prepare.
- **Metrics** receives `ORIGIN before|after`. It reports integer `restart_count` and `oom_kill` counters for the staging pod. Include cgroup memory limits and `memory.stat` fields for anonymous and file-backed memory when available.

Export `PRIME_BENCH_ADMIN_TOKEN` and `PRIME_BENCH_WORKER_TOKEN` using the staging secret values. The hooks inherit these variables. Keep token values out of command lines and logs.

```sh
python3 scripts/benchmark.py \
  --prepare ./prepare-prime-benchmark --cleanup ./cleanup-prime-benchmark \
  --metrics ./prime-benchmark-metrics --log-dir /tmp/prime-benchmark-logs \
  > /tmp/prime-benchmark-matrix.json
```

The harness polls counts confirmed by the coordinator while requesting summaries from its public listener. It measures elapsed time through complete coverage, then independently checks the exported prime count and SHA-256 at the winning revision. Public error counts and successful-request p95 are included. Worker timing medians appear when the release binary logs them. Producing the exact export can take time after the measured write interval.

Compare the lowest memory setting that sustains representative throughput and acceptable read latency. To measure SQLite lock time or WAL checkpoint behavior, collect the staging coordinator's instrumentation separately.

## Promotion gate

The report tests whether the best batch slot configuration improves median durable throughput by at least 20% against the best single configuration. Every result must match the control's exact count and export digest.

Run a separate batch soak at the chosen slot count. Pick a fresh range large enough to sustain writes for at least 1,800 seconds at the measured rate. Set `--mode batch --slots <selected> --repeats 1 --timeout 7200` and set the range with `--lo` and `--hi`. Use the same Atlas image and memory setting as the winning matrix run. Review public latency and errors against the single-path baseline. Promote only when the matrix and the 30-minute soak pass.

## Volunteer evidence

Use the Mac diagnostic to compare native enumeration, local pre-sieving, and checking precomputed volunteer evidence. It reports median durable SQLite writes per second and verifies the reopened archive's exact coverage, count, and SHA-256 of its ordered big-endian `u64` values. Pass a new scratch directory that does not exist yet.

```sh
cargo run --locked --release --example evidence_benchmark -- /tmp/prime-evidence-baseline
cargo run --locked --release --example evidence_benchmark -- /tmp/prime-evidence-certificates --certificates
```

Use `--atlas-image IMAGE --atlas-direct --volunteer-slots 6` for a smoke test with a disposable Atlas pod and both roles on this Mac. The harness seeds devices in its scratch database and wraps the loopback volunteer listener in temporary TLS. This checks HTTPS work submission and durable archive writes. Test browser enrollment separately with the staging Turnstile widget.

For a representative launch comparison with separate worker machines, prepare disposable Atlas archives through the hooks above. Enable the third, volunteer-only listener and put it behind a staging HTTPS hostname with its own Turnstile widget. Return its HTTPS origin as `volunteer_origin` and fresh enrolled 64-digit hexadecimal credentials as `volunteer_tokens` in the prepare hook's JSON for `evidence` trials. The harness keeps credentials out of logs and passes one on standard input to each volunteer. The `--volunteer-run` hook receives the zero-based worker index and HTTPS origin as arguments and connects to a worker on a separate Mac, forwarding its standard input.

```sh
python3 scripts/benchmark.py --mode evidence-compare --slots 4 --repeats 3 \
  --lo 999999999999737856 --hi 1000000000268173311 \
  --prepare ./prepare-prime-benchmark --cleanup ./cleanup-prime-benchmark \
  --metrics ./prime-benchmark-metrics --volunteer-run ./run-volunteer-on-mac \
  --log-dir /tmp/prime-evidence-logs
```

Use the fastest same-slot trusted control. Include startup with an empty evidence queue and compare identical exports. After at least 20% improvement, choose enough new aligned blocks for a 30-minute run. Export the same range from a fresh trusted-only control and supply its digest with `--expected-export-sha256` when running `--mode evidence --repeats 1 --timeout 7200`. Require stable public reads and unchanged OOM and restart counters. The harness records how many blocks used volunteer evidence.
