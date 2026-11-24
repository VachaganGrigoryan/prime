"""Disposable Atlas benchmark pods and their Mac loopback forwards."""

from contextlib import contextmanager
import json
import shlex
import subprocess
import sys
import uuid

NAMESPACE = "dev-prime-finder"
SCRATCH_ROOT = "/NAS/dev/prime-finder-benchmarks"
PRIVATE_PORT = 19087
READ_PORT = 19088


class AtlasTrial:
    def __init__(self, image, memory, direct=False):
        self.image = image
        self.memory = memory
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
            for port in (PRIVATE_PORT, READ_PORT):
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
