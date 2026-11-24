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

## Volunteer rollout

After staging and platform verification, provision `prime-finder-turnstile` with `PRIME_TURNSTILE_SITE_KEY` and `PRIME_TURNSTILE_SECRET`, configure the widget for `test-math-workers.notallow.cc`, and apply `deploy/atlas/volunteers`. This enables the dedicated loopback listener on 8789. Attach that port to the volunteer hostname after checking the listener in isolation. Keep 8787 and the private frontend out of public ingress.

For each campaign, privately submit a canonical search for the range and POST its decimal-string `lo` and `hi` to `/api/v1/contribute/admin/campaign`. Run a trusted Mac worker with `--verify-contributions` to compare independent recomputations against staged reports. Volunteer receipts record matches and rejections. Canonical results come from the trusted search. Private `GET /api/v1/contribute/admin/status` shows campaign and verification totals, and `POST /api/v1/contribute/admin/revoke` revokes a device by ID.
