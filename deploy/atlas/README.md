# Prime on Atlas

Atlas runs the coordinator, archive API, and Admin. Cloudflare Pages serves the
public dashboard from `src/client_assets`. The production overlay upgrades
`dev-prime-finder-coordinator` in place, retaining its archive volume, archive
mount identity, and the read-only ClusterIP port 8788 used by Test Math.

| Host | Atlas origin | Access |
| --- | --- | --- |
| `prime.notallow.cc` | Cloudflare Pages | Public stats |
| `prime-api.notallow.cc` | `127.0.0.1:8789` | Public reads, Turnstile enrollment, per-device worker bearer |
| `prime-admin.notallow.cc` | `127.0.0.1:8792` | Owner-only Cloudflare Access |
| `prime-dev-api.notallow.cc` | `127.0.0.1:28889` | Isolated staging API and enrollment |
| `prime-dev-admin.notallow.cc` | `127.0.0.1:28892` | Owner-only staging Access |

The loopback ports 8787 (production) and 28887 (staging) are for private
operator access through SSH. The coordinator checks the Access token issuer,
audience, signature, and lifetime for browser Admin and checks the configured
origin on Admin writes. Keep public API responses involving enrollment or
workers out of edge caches.

## Build and stage

Build a unique image tag in a clean Atlas build workspace and import it into
k3s. These commands use the release image tag selected in
`continuous/workload.patch.yaml`. For iterations, use a fresh tag and update
that patch before rendering staging.

```sh
atlas build new prime v020 --mem 3Gi --empty
atlas ssh 'sudo mkdir -p /build/prime/v020/src'
tar -cf - Cargo.toml Cargo.lock src deploy/atlas | atlas ssh 'sudo tar -xf - -C /build/prime/v020/src'
atlas build exec prime v020 -- 'cd "$WS/src" && docker build -t prime:0.2.0 -f deploy/atlas/Dockerfile . && docker save -o "$WS/prime-0.2.0.tar" prime:0.2.0'
atlas ssh 'sudo k3s ctr -n k8s.io images import /build/prime/v020/prime-0.2.0.tar'
```

Create separate HTTPS staging and production Turnstile widgets restricted to
their API hostnames. Create a separate owner-only Cloudflare Access application
for each Admin hostname before publishing its tunnel route. Set the following
values in a mode-0600 env file on Atlas for each environment, using the
environment’s hostname, widget, and Access application audience. Use
`https://vachagan.cloudflareaccess.com` for the Atlas Access issuer.

- `PRIME_TURNSTILE_SITE_KEY`, `PRIME_TURNSTILE_SECRET`, and `PRIME_TURNSTILE_HOSTNAME`
- `PRIME_ADMIN_ORIGIN`, `PRIME_ACCESS_ISSUER`, and `PRIME_ACCESS_AUDIENCE`
- `PRIME_UI_ORIGIN`, which defaults to `https://prime.notallow.cc`

Use the existing Atlas env-to-secret tool to store each environment’s file as
`prime-continuous-env` in its namespace. The staging namespace also needs a
separate `prime-finder-env` with random `PRIME_ADMIN_TOKEN` and
`PRIME_WORKER_TOKEN`. Reuse the existing `prime-finder-env` only in production.
Check the staging and production host ports against existing pod `hostPort`
settings and host processes before applying either overlay.

```sh
atlas ssh 'findmnt -M /NAS && sudo install -d -m 0700 -o 10001 -g 10001 /NAS/dev/prime-staging/storage'
atlas ssh 'sudo ~/atlas/k8s/bin/env-secret.sh dev-prime-staging prime-finder-env /DATA/dev/prime-staging/base.env'
atlas ssh 'sudo ~/atlas/k8s/bin/env-secret.sh dev-prime-staging prime-continuous-env /DATA/dev/prime-staging/prime.env'
kubectl kustomize deploy/atlas/staging | atlas kube apply --dry-run=server -f -
kubectl kustomize deploy/atlas/staging | atlas kube apply -f -
atlas kube -n dev-prime-staging rollout status deploy/dev-prime-finder-coordinator
atlas ssh 'curl -fsS http://127.0.0.1:28889/healthz'
atlas publish add prime-dev-api.notallow.cc 28889 --yes
atlas publish add prime-dev-admin.notallow.cc 28892 --yes
```

Check that an unauthenticated browser is sent to Access at the staging Admin
host and that the owner can sign in. Run two staged workers with separate
enrollments. Verify each appears with its benchmark while pending and that no
production leases are issued before approval. Approve both, resume mining, and
verify distinct assignments and exact archive output. Exercise a lease expiry,
worker revocation, and a coordinator restart. Use the local dashboard against
the staging API and measure verified primes per second and verification queue
pressure with one worker and with two.

```sh
prime --server https://prime-dev-api.notallow.cc worker
prime --server https://prime-dev-api.notallow.cc ui
curl -fsS https://prime-dev-api.notallow.cc/api/v1/mining/progress
```

To enroll two staging workers on a single Mac, run each worker in a separate
terminal with its own absolute `XDG_STATE_HOME`, for example a directory from
`mktemp -d`. Each terminal prompts for a separate enrollment code.

## Back up and cut over

Drain the existing range workers and pause their queued jobs. Capture the
running image, archive revision, and the Test Math archive response for a
covered interval. Use the existing mount at `/NAS/dev/prime-finder/storage`.
Make a verified backup of the SQLite databases with the coordinator stopped.
Leave the coordinator stopped until the production overlay is applied.

```sh
curl -fsS 'https://test-math.notallow.cc/api/v1/summary?lo=2&hi=1000000&buckets=4' > /tmp/prime-test-math-before.json
atlas kube -n dev-prime-finder get deployment dev-prime-finder-coordinator -o yaml > /tmp/prime-deployment-before.yaml
atlas kube -n dev-prime-finder scale deployment/dev-prime-finder-coordinator --replicas=0
atlas kube -n dev-prime-finder wait --for=delete pod -l app.kubernetes.io/component=coordinator,app.kubernetes.io/name=prime-finder --timeout=120s
atlas ssh 'sudo bash -se' <<'ATLAS'
set -euo pipefail
mountpoint=/NAS/dev/prime-finder/storage
[[ $(findmnt -n -M "$mountpoint" -o FSTYPE) == ext4 ]]
[[ $(cat "$mountpoint/.prime-archive-id") == prime-finder-atlas-v1 ]]
destination=/NAS/dev/prime-finder/backups/pre-prime-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 0700 "$destination"
python3 - "$mountpoint" "$destination" <<'PY'
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

mountpoint, destination = map(Path, sys.argv[1:])
for filename in ('archive.sqlite', 'contributions.sqlite'):
    source = mountpoint / filename
    if not source.exists():
        continue
    with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as original:
        with closing(sqlite3.connect(destination / filename)) as backup:
            original.backup(backup)
            if backup.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                raise SystemExit(f'Invalid backup of {filename}')
PY
test -s "$destination/archive.sqlite"
printf 'Verified backup: %s\n' "$destination"
ATLAS
# Audit the verified backup with prime-coordinator audit-archive before applying.
atlas ssh 'sudo ~/atlas/k8s/bin/env-secret.sh dev-prime-finder prime-continuous-env /DATA/dev/prime/prime.env'
kubectl kustomize deploy/atlas/continuous | atlas kube apply --dry-run=server -f -
kubectl kustomize deploy/atlas/continuous | atlas kube apply -f -
atlas kube -n dev-prime-finder rollout status deploy/dev-prime-finder-coordinator
atlas ssh 'curl -fsS http://127.0.0.1:8789/api/v1/mining/progress'
curl -fsS 'https://test-math.notallow.cc/api/v1/summary?lo=2&hi=1000000&buckets=4' > /tmp/prime-test-math-after.json
cmp /tmp/prime-test-math-before.json /tmp/prime-test-math-after.json
```

Run the new image's `prime-coordinator audit-archive /path/to/backup/archive.sqlite` against the stopped archive snapshot before applying the production overlay. Any failed segment is a cutover blocker.

Confirm that Prime starts paused with the first gap after the verified
contiguous prefix. Verify the Access application before adding the Admin
route. Publish the API and Admin tunnel routes, then upload
`src/client_assets` to the Cloudflare Pages project `prime` and attach
`prime.notallow.cc` as its custom domain.

```sh
atlas publish add prime-api.notallow.cc 8789 --yes
atlas publish add prime-admin.notallow.cc 8792 --yes
curl -fsS https://prime-api.notallow.cc/healthz
curl -fsS https://prime-api.notallow.cc/api/v1/mining/progress
curl -sS -o /dev/null -w '%{http_code}\n' https://prime-api.notallow.cc/api/v1/mining/admin/workers
curl -sS -D - -o /dev/null https://prime-admin.notallow.cc/
```

The public API must return 404 for Admin routes. Confirm the public UI loads
progress and archive queries over HTTPS and that Admin approval works after
signing in. Enroll the Mac, review its benchmark in Admin, approve it, and
resume mining. Confirm the frontier advances and the worker’s accepted result
count rises. Publish the verified Homebrew formula after the release workflow
passes.

If a production issue occurs, pause dispatch in Admin and preserve the new
database and pre-cutover backup. Rolling back to a pre-Prime image requires
the compatible pre-cutover snapshot. Keep the newer archive for recovery of
any work completed after cutover.

The base kustomization and `volunteers` overlay remain available for legacy
maintenance and isolated benchmarks. Continuous mining and legacy volunteer
dispatch are mutually exclusive.
