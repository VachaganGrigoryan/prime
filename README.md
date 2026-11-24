# Prime Finder

Prime Finder enumerates unsigned 64-bit primes on worker computers and keeps an exact, durable archive on Atlas. The Test Math website uses its read-only APIs.

Use SQLite 3.51.3 or later for the coordinator. Build the Rust tools with:

```sh
cargo test --locked
cargo build --locked --release
```

[Atlas deployment](deploy/atlas/README.md) covers coordinator and archive operations. [Benchmarking](scripts/BENCHMARK.md) describes the batch promotion gate. [Prime Worker](WORKER.md) covers portable volunteer builds and releases.

Prime Finder is [MIT-licensed](LICENSE). Copyright 2026 Vachagan Grigoryan.
