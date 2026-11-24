# Prime Worker

Prime Worker helps Test Math fill the prime archive on Atlas. It runs one leased block at a time in the foreground and sends evidence over HTTPS. A trusted Mac checks the evidence, classifies the remaining candidates, and saves the complete prime block.

## Install and run

Once public enrollment opens, install on an Apple Silicon or Intel Mac with the [Test Math Homebrew tap](https://github.com/VachaganGrigoryan/homebrew-tap). Homebrew builds the CLI from a checksummed release archive.

```sh
brew tap VachaganGrigoryan/tap
brew trust --formula VachaganGrigoryan/tap/prime-worker
brew install VachaganGrigoryan/tap/prime-worker
prime-worker join
prime-worker run
```

For Linux or Windows, use the archive for your system from the [Prime Finder releases](https://github.com/VachaganGrigoryan/prime-finder/releases). Verify its SHA-256 checksum against `SHA256SUMS` in the same release, then extract it and run the executable with `join` and `run`. The Linux archives target x86-64 and ARM64 with glibc 2.35 or newer. Windows uses x86-64.

`join` prints the enrollment URL. Complete the form there to get your worker credential. Paste it at the hidden prompt when you run `prime-worker run`. For automation, redirect a private credential file to standard input, such as `prime-worker run < worker-credential` on Unix or `Get-Content worker-credential | .\prime-worker.exe run` in PowerShell. The worker starts when you run it and stops on Ctrl+C.

The command defaults to `https://test-math-workers.notallow.cc`. A test deployment can use `--server` with a different HTTPS origin.

## Test another device on the LAN

Install the preview on macOS with `brew install VachaganGrigoryan/tap/prime-worker-preview`. On Linux or Windows, use the matching binary from the [GitHub preview release](https://github.com/VachaganGrigoryan/prime-finder/releases/tag/worker-preview-v0.1.0), or build the source with the command below. The operator shares the preview HTTPS address, its CA certificate, and a separate one-device credential. Keep that credential private.

```sh
prime-worker --server "$PREVIEW_ORIGIN" --ca-certificate ./prime-preview-ca.pem run
```

Paste your device credential when prompted, or supply it over standard input for automation. On Windows, use `prime-worker.exe`. Preview jobs run on an isolated Atlas archive, and their credentials work only for the active preview. The operator runs `python3 scripts/device_preview.py --help` for setup.

## Build and verify a release

From the Prime Finder checkout, build the portable executable with the volunteer-worker feature.

```sh
cargo test --locked --no-default-features --features volunteer-worker --lib --bin prime-worker
cargo build --locked --release --no-default-features --features volunteer-worker --bin prime-worker
```

The GitHub workflow builds and runs tests on both Mac architectures, both Linux architectures, and Windows x86-64. It also runs `brew install` and `brew test` against the generated source formula on both Mac architectures. Worker tags use `worker-vMAJOR.MINOR.PATCH` and match the package version. Release assets include the checksummed source archive, the generated `prime-worker.rb`, and the Linux and Windows binaries. Download and verify the tagged release assets with `shasum -a 256 -c SHA256SUMS`, then copy `prime-worker.rb` into `Formula/prime-worker.rb` in the existing tap.

Exercise enrollment, claim, submission, and the trusted archive commit on staging using the released worker. Public enrollment opens after exact archive parity, the 20% improvement in durable primes per second against the fastest trusted-only baseline, and the 30-minute soak.

Prime Finder is MIT-licensed. Copyright 2026 Vachagan Grigoryan. See [LICENSE](LICENSE).
