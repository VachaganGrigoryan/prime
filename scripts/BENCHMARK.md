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
