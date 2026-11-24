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

## Prime estimates

Open **Estimates** in the dashboard or in a source build of `prime ui`. The
retrospective compares formulas with counts and log-weighted prime powers at
a fixed Atlas revision. A separate prediction manifest freezes the model,
zero-table checksum, interval boundaries and numerical predictions. Each
download includes its archive origin and revision.

The numerical tools run independently of mining. They require Python 3.9+
and `mpmath==1.3.0`. The 64 zeta ordinates are pinned in the repository.
From the repository root, the following commands reproduce the retrospective
and replay the bundled prediction at an archive revision of your choice.

```sh
python3 -m venv /tmp/prime-estimates-env
/tmp/prime-estimates-env/bin/pip install mpmath==1.3.0
/tmp/prime-estimates-env/bin/python scripts/prime_experiment.py snapshot --out /tmp/prime-experiment.json
/tmp/prime-estimates-env/bin/python scripts/prime_experiment.py score --manifest src/client_assets/prime-prediction.json --out /tmp/prime-score.json
```

The score command uses a single current revision by default. Pass
`--revision REV` to reproduce historical results. Open the resulting score
file on the Estimates page. Fully covered windows contribute to the scores.
Each window with missing coverage remains pending. Pick a fresh output
filename for each replay to retain earlier results.

To run another prospective experiment, choose a range ahead of mining.
For example, `predict --lo LO --width 16384 --width 1048576 --windows 8 --out FILE`
freezes eight disjoint windows at each width. Windows at different widths
can overlap. Publish the immutable forecast through the existing Pages
release process before coverage starts. Preserve previous forecast/score
pairs when publishing a new experiment.

The **Score published prime predictions** workflow runs hourly and on demand.
After the source release and Pages both serve the same manifest, it reads
the public file and verifies the range is still unchecked. It records that
archive revision, the public URL and the Pages commit as a publication
witness. Later runs update the downloadable score when additional windows
are fully verified. When updating other site assets on the Pages branch,
preserve its witnessed `prime-score.json` for the same manifest. The score
bundled in the source tree is the initial offline snapshot.
