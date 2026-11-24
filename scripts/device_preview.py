#!/usr/bin/env python3
"""Run an invite-only Prime Worker preview with a disposable Atlas archive."""

import argparse
from contextlib import ExitStack
from ipaddress import IPv4Address, IPv4Network
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
import time

from atlas_benchmark import AtlasTrial, PRIVATE_PORT, volunteer_tls
from benchmark import Api, child, wait_ready

BLOCK_WIDTH = 1 << 20
START = 10**18 // BLOCK_WIDTH * BLOCK_WIDTH
PRIVATE_LAN = tuple(map(IPv4Network, ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")))
STATUS = "/contribute/admin/evidence/status"


def lan_address(value):
    try:
        address = IPv4Address(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("enter this Mac's private IPv4 address") from error
    if not any(address in network and address not in (network.network_address, network.broadcast_address)
               for network in PRIVATE_LAN):
        raise argparse.ArgumentTypeError("the preview binds only to a private LAN IPv4 address")
    return str(address)


def bounded_int(minimum, maximum):
    def parse(value):
        number = int(value)
        if not minimum <= number <= maximum:
            raise argparse.ArgumentTypeError(f"enter a number from {minimum} to {maximum}")
        return number

    return parse


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", required=True, type=lan_address, help="this Mac's private IPv4 address")
    parser.add_argument("--output", required=True, type=Path, help="new directory for credentials, CA, and logs")
    parser.add_argument("--atlas-image", default="prime-finder:volunteer-v2")
    parser.add_argument("--atlas-direct", action="store_true")
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "target/release/prime-finder")
    parser.add_argument("--port", type=bounded_int(1024, 65535), default=19443)
    parser.add_argument("--devices", type=bounded_int(1, 16), default=4)
    parser.add_argument("--minutes", type=bounded_int(1, 360), default=120)
    parser.add_argument("--blocks", type=bounded_int(1, 32768), default=32768)
    return parser.parse_args()


def write_token(path, token):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="ascii") as file:
        file.write(f"{token}\n")


def hold_until_ready(api, tunnel, deadline):
    while time.monotonic() < deadline:
        if tunnel.poll() is not None:
            raise RuntimeError("The Atlas tunnel exited. See tunnel.log.")
        state = api.json(STATUS)
        if state["ready"] > 0:
            return True
        time.sleep(1)
    return False


def serve_job(api, tunnel, env, binary, root, job, deadline):
    command = [str(binary), "--coordinator", api.origin, "worker", "--id", "atlas-device-preview",
               "--threads", "1", "--use-volunteer-evidence"]
    with child(command, env, root / "worker.log") as worker:
        promoted = 0
        complete = False
        while time.monotonic() < deadline:
            if tunnel.poll() is not None or worker.poll() is not None:
                raise RuntimeError("The preview worker or tunnel exited. See the preview logs.")
            used = api.json(STATUS)["used_blocks"]
            if used > promoted:
                promoted = used
                print(f"Saved {used} blocks using volunteer evidence on Atlas.", flush=True)
            state = api.json(f"/jobs/{job['id']}")["state"]
            if state == "complete" and not complete:
                print("The preview range is complete.", flush=True)
                complete = True
            if state not in ("queued", "running", "complete"):
                raise RuntimeError(f"The preview job entered state {state!r}.")
            time.sleep(5)


def run(args):
    binary = args.binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f"Build the trusted coordinator binary first. Expected {binary}.")
    root = args.output.resolve()
    root.mkdir(mode=0o700)
    env = {**os.environ, "PRIME_ADMIN_TOKEN": secrets.token_hex(32), "PRIME_WORKER_TOKEN": secrets.token_hex(32)}
    stage = AtlasTrial(args.atlas_image, "512Mi", direct=args.atlas_direct, volunteer_slots=args.devices)
    with ExitStack() as stack:
        tls_root = Path(stack.enter_context(TemporaryDirectory(prefix="prime-preview-tls-")))
        origin, certificate = stack.enter_context(volunteer_tls(tls_root, args.listen, args.port))
        tunnel = stack.enter_context(stage.endpoints(env, root / "tunnel.log"))
        api = Api(f"http://127.0.0.1:{PRIVATE_PORT}", env["PRIME_ADMIN_TOKEN"])
        initial = wait_ready(api, tunnel)
        if initial["revision"] != "0" or initial["jobs"] or initial["workers"] or initial["storage"]["paused"]:
            raise RuntimeError("Expected a clean, available preview archive.")
        job = api.json("/jobs", {"lo": str(START), "hi": str(START + args.blocks * BLOCK_WIDTH - 1)})
        api.json("/contribute/admin/evidence/campaign", {"job_id": int(job["id"])})
        tokens = stage.seed_devices()
        ca = root / "prime-preview-ca.pem"
        shutil.copyfile(certificate, ca)
        for index, token in enumerate(tokens, 1):
            credential = root / f"device-{index}.token"
            write_token(credential, token)
            stack.callback(credential.unlink, missing_ok=True)
        print(f"Preview listening on {origin} for up to {args.minutes} minutes.", flush=True)
        print(f"Transfer {ca} and one device token per tester from {root}.", flush=True)
        print(f"Run prime-worker --server {origin} --ca-certificate prime-preview-ca.pem run", flush=True)
        print("Paste the assigned device token when prompted. Waiting for the first submission.", flush=True)
        deadline = time.monotonic() + args.minutes * 60
        if hold_until_ready(api, tunnel, deadline):
            serve_job(api, tunnel, env, binary, root, job, deadline)


def stop(_signal, _frame):
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, stop)
    try:
        run(parse_args())
    except KeyboardInterrupt:
        print("Preview stopped.", flush=True)
        return 130
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Preview failed. {error}", file=sys.stderr)
        return 1
    print("Preview ended and Atlas scratch resources were cleaned up.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
