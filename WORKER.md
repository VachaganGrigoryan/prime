# Run a Prime worker

## Install

On Apple Silicon and Intel Macs, install from the project’s Homebrew tap.

```sh
brew install VachaganGrigoryan/tap/prime
```

On Linux or Windows, download the matching binary archive and `SHA256SUMS` from
[Prime releases](https://github.com/VachaganGrigoryan/prime/releases). Verify the
archive’s SHA-256 against the matching entry before extracting it. Linux
archives target x86-64 and ARM64 with glibc 2.35 or newer. Windows uses x86-64.

## Join Atlas

Run `prime worker` or `prime.exe worker` on Windows. Complete the browser
challenge and enter the displayed one-time code at the terminal prompt. The CLI
stores its per-device credential, runs the benchmark, and shows the pending
approval state. Leave it running or rerun the command later. Following approval
the worker automatically requests a block, renews its lease while computing,
and sends the result to Atlas. Stop the foreground worker with Ctrl+C.

The default API is `https://prime-api.notallow.cc`. To use the isolated staging
site, run `prime --server https://prime-dev-api.notallow.cc worker`. Complete a
separate enrollment for that origin.

Run `prime ui` to serve the dashboard on this device. Open the loopback address
printed by the command. The dashboard works while the worker is stopped and can
also be used without enrolling. For the staging dashboard run
`prime --server https://prime-dev-api.notallow.cc ui`.

## Build and verify a release

```sh
cargo test --locked --no-default-features --features volunteer-worker --lib --bin prime
cargo build --locked --release --no-default-features --features volunteer-worker --bin prime
```

The Prime GitHub workflow checks the coordinator with the Atlas SQLite build,
runs Homebrew install and tests on both Mac architectures, and runs portable
tests on the Linux and Windows release targets. Tag releases `vMAJOR.MINOR.PATCH`
to match the Cargo package version. The workflow publishes the source archive,
`prime.rb`, native Linux and Windows archives, and `SHA256SUMS`. Verify the
released files with `shasum -a 256 -c SHA256SUMS`, then publish the verified
`prime.rb` as `Formula/prime.rb` in the existing
[Homebrew tap](https://github.com/VachaganGrigoryan/homebrew-tap).

The legacy `prime-worker` and preview releases use the previous protocol.
Install `prime` and enroll again to join continuous mining.
