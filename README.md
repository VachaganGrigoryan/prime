# Prime

Prime is a continuous, distributed search for unsigned 64-bit primes. Atlas owns
the archive, assigns the earliest unchecked blocks to approved workers, and
verifies results before advancing the contiguous search. The public dashboard
and local dashboard read the same Atlas API.

```sh
brew install VachaganGrigoryan/tap/prime
prime worker
prime ui
```

On first run, complete the enrollment challenge in a browser and enter the
one-time code in the terminal. The worker runs its benchmark and waits for an
operator to review it. An approved worker starts on the next poll.

- [Public dashboard](https://prime.notallow.cc/)
- [Atlas Admin](https://prime-admin.notallow.cc/)
- [Install and run a worker](WORKER.md)
- [Deploy and operate Atlas](deploy/atlas/README.md)

To build and test locally, use Rust 1.99.1 and SQLite 3.51.3 or newer for the
coordinator. The portable build omits SQLite.

```sh
cargo test --locked --all-features
cargo build --locked --release --bin prime-coordinator
cargo build --locked --release --no-default-features --features volunteer-worker --bin prime
```

Prime is [MIT-licensed](LICENSE). Copyright 2026 Vachagan Grigoryan.
