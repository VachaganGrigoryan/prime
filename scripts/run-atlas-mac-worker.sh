#!/bin/bash
set +x
set -euo pipefail

usage() {
    echo "Usage: $0 [worker-id [slots]] | --check"
}

check=0
case ${1:-} in
    -h|--help) usage; exit 0 ;;
    --check) check=1; shift ;;
esac
if (( $# > 2 || (check && $# != 0) )); then
    usage >&2
    exit 2
fi

worker_id=${1:-mac2}
slots=${2:-4}
port=28787
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
binary="$script_dir/prime-finder"
if [[ ! -x "$binary" ]]; then
    binary="$script_dir/../target/release/prime-finder"
fi
if [[ ! -x "$binary" ]]; then
    echo "Build the worker with cargo build --locked --release --bin prime-finder." >&2
    exit 1
fi

ssh_options=(-o BatchMode=yes -o ConnectTimeout=10)
if [[ ${PRIME_ATLAS_DIRECT:-0} == 1 ]]; then
    ssh_options+=(-o ProxyCommand=none)
fi

temp_dir=$(mktemp -d /tmp/prime-ssh.XXXXXXXX)
socket="$temp_dir/control"
cleanup() {
    if [[ -S "$socket" ]]; then
        ssh -S "$socket" -O exit atlas >/dev/null 2>&1 || true
    fi
    rmdir -- "$temp_dir" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ssh "${ssh_options[@]}" -M -S "$socket" -o ControlPersist=no \
    -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
    -fN -L "127.0.0.1:$port:127.0.0.1:8787" atlas

unset PRIME_WORKER_TOKEN
if ! PRIME_WORKER_TOKEN=$(ssh "${ssh_options[@]}" -S "$socket" atlas \
    'sudo -n kubectl -n dev-prime-finder get secret prime-finder-env -o jsonpath="{.data.PRIME_WORKER_TOKEN}" | base64 -d') \
    || [[ -z "$PRIME_WORKER_TOKEN" ]]; then
    echo "Could not read the worker credential. The Atlas SSH account needs access to prime-finder-env." >&2
    exit 1
fi

origin="http://127.0.0.1:$port"
curl --noproxy '*' --fail --silent --show-error --max-time 10 "$origin/healthz" >/dev/null
if (( check )); then
    echo "Atlas coordinator reachable and worker credential retrieved."
    exit 0
fi

echo "Starting Atlas worker $worker_id with $slots slots. Press Ctrl-C to stop."
PRIME_WORKER_TOKEN="$PRIME_WORKER_TOKEN" "$binary" --coordinator "$origin" \
    worker --id "$worker_id" --threads "$slots"
