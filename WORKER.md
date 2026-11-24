# Prime Worker

Prime Worker enumerates primes for Test Math on volunteer computers. It runs one block at a time in the foreground and sends each result to Atlas over HTTPS. A trusted worker independently recomputes each submission to confirm contribution credit. A matching trusted search fills the published archive.

## Install and run

On macOS, the cask goes in the [Test Math Homebrew tap](https://github.com/VachaganGrigoryan/homebrew-tap). Once the signed release and its cask are published, install and enroll with these commands.

```sh
brew install --cask VachaganGrigoryan/tap/prime-worker
prime-worker join
prime-worker run
```

For Linux or Windows, use the archive for your system from the [Prime Finder releases](https://github.com/VachaganGrigoryan/prime-finder/releases). Verify its SHA-256 checksum against `SHA256SUMS` in the same release, then extract it and run the executable with `join` and `run`. The Linux archives target x86-64 and ARM64 with glibc 2.35 or newer. Windows uses x86-64.

`join` prints the enrollment URL. Complete the form there to get your worker credential. Paste it at the hidden prompt when you run `prime-worker run`. For automation, redirect a private credential file to standard input, such as `prime-worker run < worker-credential` on Unix or `Get-Content worker-credential | .\prime-worker.exe run` in PowerShell. The worker starts when you run it and stops on Ctrl+C.

The command defaults to `https://test-math-workers.notallow.cc`. A test deployment can use `--server` with a different HTTPS origin.

## Build and verify a release

From the Prime Finder checkout, build the portable executable with the volunteer-worker feature.

```sh
cargo test --locked --no-default-features --features volunteer-worker --lib --bin prime-worker
cargo build --locked --release --no-default-features --features volunteer-worker --bin prime-worker
```

The GitHub workflow builds and runs tests on both Mac architectures, both Linux architectures, and Windows x86-64. Worker tags use `worker-vMAJOR.MINOR.PATCH` and match the package version. The workflow signs and notarizes both Mac binaries before publishing the archives, checksums, and generated `prime-worker.rb`. Review that cask and add it to `Casks/prime-worker.rb` in the existing tap after release assets are available.

The Mac signing job uses the GitHub secrets `MACOS_CERTIFICATE_P12` and `MACOS_CERTIFICATE_PASSWORD` for the Developer ID identity. It uses `MACOS_NOTARY_KEY_P8`, `MACOS_NOTARY_KEY_ID`, and `MACOS_NOTARY_ISSUER` for notarization. The certificate and key secrets contain base64 of their respective files. Before announcing availability, run the released worker through enrollment, claim, submission, and verification on each supported platform.
