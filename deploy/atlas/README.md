# Prime Finder on Atlas

The coordinator runs in `dev-prime-finder` on Atlas. The Northbound API on loopback 8787 handles jobs and trusted workers. The archive API on ClusterIP 8788 supplies the independently deployed Test Math frontend. The coordinator stores its archive under `/NAS/dev/prime-finder/storage` and retains the existing sparse 100 GB filesystem and SQLite WAL with full synchronization.

## Build and roll out the coordinator

Run the Linux image build in the Atlas build workspace. Transfer this repository's `Cargo.toml`, `Cargo.lock`, `src/`, and `deploy/atlas/` to its source directory. Build and import a unique tag using the existing Atlas workflow.

```sh
atlas build new prime-finder lab-v1 --mem 3Gi --empty
atlas build exec prime-finder lab-v1 -- 'cd "$WS/src" && docker build -t prime-finder:lab-v6 -f deploy/atlas/Dockerfile . && docker save -o "$WS/prime-finder-v6.tar" prime-finder:lab-v6'
atlas ssh 'sudo k3s ctr -n k8s.io images import /build/prime-finder/lab-v1/prime-finder-v6.tar'
```

On a first install, provision the mounted storage with `provision-storage.sh` and create `prime-finder-env` with the usual Atlas env-to-secret workflow. Its keys are `PRIME_ADMIN_TOKEN` and `PRIME_WORKER_TOKEN`. For an existing service use the current volume and secret. Dry-run the matching kustomization, then apply it and check the rollout.

```sh
atlas kube apply --dry-run=server -k deploy/atlas
atlas kube apply -k deploy/atlas
atlas kube -n dev-prime-finder rollout status deploy/dev-prime-finder-coordinator
```

## Private operation

Forward 8787 for the existing worker and CLI. Add 8791 for the private browser UI when the independent Test Math web service is installed.

```sh
ssh -o ExitOnForwardFailure=yes -N -L 127.0.0.1:8787:127.0.0.1:8787 -L 127.0.0.1:8791:127.0.0.1:8791 atlas
```

Supply the private worker token through the environment and start `prime-finder worker --id mac --threads 8`. Use private browser URL `http://127.0.0.1:8791/primes/`. Set `--batch-upload` on the staging worker when comparing the transport. The default sends one block per request.

### Add another trusted Mac

Set up the `atlas` SSH alias on the second Mac using the Atlas repository's `mac/install-mac.sh` SSH instructions. Give this Mac its own SSH key. Check that `ssh -o BatchMode=yes atlas true` succeeds before starting the worker. The launcher uses the existing Atlas administrator account to retrieve only the worker token into memory.

```sh
cargo build --locked --release --bin prime-finder
bash scripts/run-atlas-mac-worker.sh --check
bash scripts/run-atlas-mac-worker.sh mac2 4
```

With the private Mac bundle, use `bash run.command` in the extracted directory and skip the build. Choose a worker ID unique to that Mac. The worker claims blocks from the same Atlas queue, so leave any existing search running. The launcher uses local port 28787 and closes its tunnel when the worker exits. On the home LAN, prefix the command with `PRIME_ATLAS_DIRECT=1` if the alias is trying to use the remote proxy.

## Volunteer rollout

Use a separate staging hostname and scratch archive for enrollment tests and benchmarks. Provision `prime-finder-turnstile` with `PRIME_TURNSTILE_SITE_KEY` and `PRIME_TURNSTILE_SECRET`. Set `PRIME_TURNSTILE_HOSTNAME` to the staging hostname and configure the widget for the same hostname. The `deploy/atlas/volunteers` overlay enables the dedicated loopback listener on 8789. Attach the production hostname `test-math-workers.notallow.cc` only after the platform, throughput, and soak checks pass. Keep 8787 and the private frontend out of public ingress.

Submit a private job of complete aligned blocks at or above `10^16`. Register it with `POST /api/v1/contribute/admin/evidence/campaign` and `{"job_id":123}`. Run a trusted Mac worker with `--use-volunteer-evidence` to consume staged evidence as part of that job. The worker still classifies every candidate and commits through the archive's trusted endpoint. On the private listener, `GET /api/v1/contribute/admin/evidence/status` reports staged and used blocks, `POST /api/v1/contribute/admin/evidence/pause` with `{"paused":true}` pauses evidence leases, and `POST /api/v1/contribute/admin/revoke` revokes a device by ID.

For the earlier redundant verification protocol, submit the decimal-string `lo` and `hi` to `/api/v1/contribute/admin/campaign` and run the trusted worker with `--verify-contributions`. Its counters are available at `/api/v1/contribute/admin/status`.
