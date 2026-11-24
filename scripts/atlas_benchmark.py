"""Disposable Atlas benchmark pods and their Mac loopback forwards."""

from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import secrets
import shlex
import ssl
import subprocess
import sys
import threading
import uuid

NAMESPACE = "dev-prime-finder"
SCRATCH_ROOT = "/NAS/dev/prime-finder-benchmarks"
PRIVATE_PORT = 19087
READ_PORT = 19088
VOLUNTEER_PORT = 19089


class VolunteerProxy(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        allowed = (self.path.startswith("/api/v1/contribute/evidence/")
                   or self.path == "/api/v1/contribute/claim")
        if not allowed or not 0 <= length <= 3 << 20:
            self.send_error(400)
            return
        upstream = HTTPConnection("127.0.0.1", VOLUNTEER_PORT, timeout=15)
        try:
            headers = {name: self.headers[name] for name in ("Authorization", "Content-Type")
                       if name in self.headers}
            upstream.request("POST", self.path, self.rfile.read(length), headers)
            response = upstream.getresponse()
            payload = response.read()
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            upstream.close()

    def log_message(self, *args):
        pass


@contextmanager
def volunteer_tls(root):
    certificate, key = root / "volunteer.pem", root / "volunteer.key"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                    "-keyout", str(key), "-out", str(certificate)], check=True, capture_output=True)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    with ThreadingHTTPServer(("127.0.0.1", 0), VolunteerProxy) as server:
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"https://127.0.0.1:{server.server_port}", str(certificate)
        finally:
            server.shutdown()
            thread.join()


class AtlasTrial:
    def __init__(self, image, memory, direct=False, volunteer_slots=0):
        self.image = image
        self.memory = memory
        self.volunteer_slots = volunteer_slots
        self.name = "prime-bench-" + uuid.uuid4().hex[:20]
        self.scratch = f"{SCRATCH_ROOT}/{self.name}"
        self.ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]
        if direct:
            self.ssh.extend(["-o", "ProxyCommand=none"])
        self.ssh.append("atlas")

    def remote(self, command, *, body=None, timeout=45):
        result = subprocess.run(
            [*self.ssh, shlex.join(command)], input=body, check=True, text=True,
            stdout=subprocess.PIPE, timeout=timeout,
        )
        return result.stdout

    def kube(self, *args, body=None, timeout=45):
        return self.remote(["sudo", "-n", "kubectl", "-n", NAMESPACE, *args], body=body, timeout=timeout)

    def manifest(self, env):
        metadata = {"name": self.name, "labels": {"app": "prime-bench"}}
        secret = {
            "apiVersion": "v1", "kind": "Secret", "metadata": metadata,
            "stringData": {key: env[key] for key in ("PRIME_ADMIN_TOKEN", "PRIME_WORKER_TOKEN")},
        }
        container = {
            "name": "coordinator", "image": self.image, "imagePullPolicy": "Never",
            "args": ["serve", "--bind", "0.0.0.0:8787", "--public-bind", "0.0.0.0:8788",
                     "--archive-dir", "/archive", "--quota-bytes", "8000000000",
                     "--reserve-bytes", "200000000"],
            "envFrom": [{"secretRef": {"name": self.name}}],
            "ports": [
                {"containerPort": 8787, "hostPort": PRIVATE_PORT, "hostIP": "127.0.0.1"},
                {"containerPort": 8788, "hostPort": READ_PORT, "hostIP": "127.0.0.1"},
            ],
            "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8787}},
            "resources": {"requests": {"memory": "128Mi", "cpu": "250m"},
                          "limits": {"memory": self.memory, "cpu": "1"}},
            "volumeMounts": [{"name": "archive", "mountPath": "/archive"}],
            "securityContext": {"allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]}},
        }
        if self.volunteer_slots:
            container["args"] += ["--contribute-bind", "0.0.0.0:8789"]
            container["ports"].append({"containerPort": 8789, "hostPort": VOLUNTEER_PORT, "hostIP": "127.0.0.1"})
            secret["stringData"].update(PRIME_TURNSTILE_SITE_KEY="benchmark-only-site-key",
                                        PRIME_TURNSTILE_SECRET=secrets.token_hex(32))
        pod = {
            "apiVersion": "v1", "kind": "Pod", "metadata": metadata,
            "spec": {
                "restartPolicy": "Never", "automountServiceAccountToken": False,
                "terminationGracePeriodSeconds": 5, "containers": [container],
                "securityContext": {"runAsNonRoot": True, "runAsUser": 10001,
                                    "runAsGroup": 10001, "seccompProfile": {"type": "RuntimeDefault"}},
                "volumes": [{"name": "archive",
                             "hostPath": {"path": self.scratch, "type": "Directory"}}],
            },
        }
        return json.dumps({"apiVersion": "v1", "kind": "List", "items": [secret, pod]})

    def seed_devices(self):
        # The ephemeral pod has no enrollment hostname. Seed only its disposable credential DB.
        tokens = [secrets.token_hex(32) for _ in range(self.volunteer_slots)]
        seed = """import hashlib, json, os, sqlite3, sys
os.setgid(10001)
os.setuid(10001)
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    db.executemany('INSERT INTO devices(name,digest) VALUES(?,?)',
                   [(f'benchmark-{i}', hashlib.sha256(token.encode()).digest())
                    for i, token in enumerate(json.load(sys.stdin))])
"""
        self.remote(["sudo", "-n", "python3", "-c", seed, f"{self.scratch}/contributions.sqlite"],
                    body=json.dumps(tokens))
        return tokens

    @contextmanager
    def endpoints(self, env, log):
        scratch_created = False
        print(f"Atlas trial {self.name} on {self.scratch}", file=sys.stderr, flush=True)
        try:
            self.remote(["sudo", "-n", "install", "-d", "-m", "0755", SCRATCH_ROOT])
            self.remote(["sudo", "-n", "install", "-d", "-m", "0700", "-o", "10001", "-g", "10001",
                         self.scratch])
            scratch_created = True
            self.kube("create", "-f", "-", body=self.manifest(env))
            self.kube("wait", "--for=condition=Ready", f"pod/{self.name}", "--timeout=80s", timeout=95)
            forward = []
            ports = (PRIVATE_PORT, READ_PORT, VOLUNTEER_PORT) if self.volunteer_slots else (PRIVATE_PORT, READ_PORT)
            for port in ports:
                forward += ["-L", f"127.0.0.1:{port}:127.0.0.1:{port}"]
            command = [*self.ssh[:-1], "-o", "ExitOnForwardFailure=yes", "-o", "ControlPath=none",
                       "-N", *forward, self.ssh[-1]]
            with log.open("wb") as output:
                tunnel = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
                try:
                    yield tunnel
                finally:
                    if tunnel.poll() is None:
                        tunnel.terminate()
                        tunnel.wait(timeout=8)
        except Exception:
            if scratch_created:
                try:
                    state = self.kube("get", "pod", self.name,
                                      "-o", "jsonpath={.status.containerStatuses[0].state}")
                    print(f"Atlas trial {self.name} state {state}", file=sys.stderr)
                except (OSError, subprocess.SubprocessError):
                    pass
            raise
        finally:
            if scratch_created:
                self.kube("delete", f"pod/{self.name}", f"secret/{self.name}", "--ignore-not-found",
                          "--wait=true", "--timeout=35s")
                self.remote(["sudo", "-n", "rm", "-rf", "--", self.scratch])

    def metrics(self):
        pod = json.loads(self.kube("get", "pod", self.name, "-o", "json"))
        status = pod["status"]["containerStatuses"][0]
        expected_limit = {"512Mi": 512 << 20, "1Gi": 1 << 30}[self.memory]
        limit = self.kube("exec", self.name, "--", "cat", "/sys/fs/cgroup/memory.max").strip()
        if limit != str(expected_limit):
            raise RuntimeError(f"Expected the staging container cgroup limit, found {limit!r}")
        events = self.cgroup_fields("memory.events")
        accounting = self.cgroup_fields("memory.stat")
        return {
            "restart_count": status["restartCount"], "oom_kill": events["oom_kill"],
            "anon_bytes": accounting["anon"], "file_bytes": accounting["file"],
            "file_dirty_bytes": accounting["file_dirty"],
            "file_writeback_bytes": accounting["file_writeback"],
            "memory_limit_bytes": expected_limit,
        }

    def cgroup_fields(self, filename):
        output = self.kube("exec", self.name, "--", "cat", f"/sys/fs/cgroup/{filename}")
        return {key: int(value) for key, value in (line.split() for line in output.splitlines() if line)}
