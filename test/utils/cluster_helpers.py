# Copyright 2019 The Vearch Authors.
# Licensed under the Apache License, Version 2.0.

# -*- coding: UTF-8 -*-

"""
Cluster fault-injection helpers for chaos tests.

Two supported cluster deployment modes:
  1. **bare-metal** — local bare-process cluster launched by scripts/cluster.sh.
     PID files live under .cluster_pids/; kill / start is done via os.kill +
     subprocess.Popen.
  2. **docker-compose** — container cluster brought up by cloud/docker-compose.yml
     (the path CI takes, via the set_cluster_env composite action). kill /
     start is done via `docker kill / docker start vearch-{role}{idx}`, and
     logs are read through `docker logs` rather than the host filesystem.

Mode selection:
  The CLUSTER_MODE environment variable specifies the mode explicitly
  ("bare" / "docker"); otherwise auto-detect:
    - .cluster_pids/ has *.pid files ⇒ bare-metal
    - otherwise `docker ps` shows vearch-* containers ⇒ docker
    - neither ⇒ default to bare-metal

Port mapping (exposed on the host in docker mode):
  master1: 8817 (other masters reachable only inside containers)
  router1: 9001 (other routers reachable only inside containers)
  PS:      no ports exposed to the host

Cross-host fault injection needs different infrastructure (Toxiproxy / Chaos Mesh / SSH).
"""

import os
import signal
import subprocess
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Topology — must match config/*.toml + scripts/cluster.sh + cloud/docker-compose.yml
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
CLUSTER_SCRIPT = REPO_ROOT / "scripts" / "cluster.sh"
PID_DIR = REPO_ROOT / ".cluster_pids"
CONF_DIR = REPO_ROOT / "config"
LOG_DIR = REPO_ROOT / "logs"
VEARCH_BIN = os.environ.get("VEARCH_BIN", str(REPO_ROOT / "build/bin/vearch"))
AUTH = ("root", os.environ.get("PASSWORD", "secret"))

# Where the vearch binary's CGO dependency `libgamma.so` lives.
# Only relevant in bare-metal mode (in docker mode the binary lives under the
# image's /vearch/lib/, with LD_LIBRARY_PATH already configured at image build).
VEARCH_LIB_DIR = os.environ.get(
    "VEARCH_LIB_DIR", str(REPO_ROOT / "build/gamma_build"))


def _subprocess_env():
    """Build an env dict for vearch subprocesses with LD_LIBRARY_PATH
    augmented so that `libgamma.so` (and any other CGO deps under the same
    dir) resolve at startup. Without this every bare-process start_*
    would 'died within 2s' with a misleading liveness-gate error.
    """
    env = os.environ.copy()
    existing = env.get("LD_LIBRARY_PATH", "")
    if existing:
        env["LD_LIBRARY_PATH"] = f"{VEARCH_LIB_DIR}:{existing}"
    else:
        env["LD_LIBRARY_PATH"] = VEARCH_LIB_DIR
    return env


# ---------------------------------------------------------------------------
# Mode detection — bare vs docker
# ---------------------------------------------------------------------------


def _detect_mode():
    """Auto-detect the cluster deployment mode. The env var VEARCH_CLUSTER_MODE
    takes precedence over auto-detection.

    Auto-detection rules:
      1. any *.pid file under .cluster_pids/ ⇒ bare (scripts/cluster.sh running)
      2. `docker ps` shows vearch-* containers ⇒ docker
      3. neither ⇒ default to bare (assume the user is about to use cluster.sh)
    """
    forced = os.environ.get("VEARCH_CLUSTER_MODE", "").strip().lower()
    if forced in ("bare", "docker"):
        return forced

    # 1. PID files take precedence
    try:
        if PID_DIR.exists() and any(PID_DIR.glob("*.pid")):
            return "bare"
    except OSError:
        pass

    # 2. docker container probe
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", "name=vearch-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        if out.returncode == 0 and out.stdout.strip():
            return "docker"
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass

    # 3. default bare
    return "bare"


CLUSTER_MODE = _detect_mode()


# Mode-specific port / name tables.
# bare mode corresponds to scripts/cluster.sh + config/*.toml;
# docker mode corresponds to the container names in cloud/docker-compose.yml
# plus the host port mapping.

_BARE_MASTERS = {
    "m1": {"api": 28817, "etcd_client": 22370, "monitor": 28821},
    "m2": {"api": 28827, "etcd_client": 22371, "monitor": 28831},
    "m3": {"api": 28837, "etcd_client": 22372, "monitor": 28841},
}
_BARE_PSES = {
    1: {"rpc": 28081},
    2: {"rpc": 28082},
    3: {"rpc": 28083},
}
_BARE_ROUTERS = {
    1: {"http": 29001},
    2: {"http": 29002},
}

# docker mode: only master1 / router1 expose their ports to the host. Other
# nodes' ports are reachable only inside containers. Here the `api`/`http` field
# value = host port (when exposed) or None (when not exposed); the PS rpc port is
# 8081 inside the container but not visible on the host, so the PSES table no
# longer carries rpc. The container_name field is the container name used in
# docker mode.
_DOCKER_MASTERS = {
    "m1": {"api": 8817, "container_name": "vearch-master1"},
    "m2": {"api": None, "container_name": "vearch-master2"},
    "m3": {"api": None, "container_name": "vearch-master3"},
}
_DOCKER_PSES = {
    1: {"container_name": "vearch-ps1"},
    2: {"container_name": "vearch-ps2"},
    3: {"container_name": "vearch-ps3"},
}
_DOCKER_ROUTERS = {
    1: {"http": 9001, "container_name": "vearch-router1"},
    2: {"http": None, "container_name": "vearch-router2"},
}

if CLUSTER_MODE == "docker":
    MASTERS = _DOCKER_MASTERS
    PSES = _DOCKER_PSES
    ROUTERS = _DOCKER_ROUTERS
else:
    MASTERS = _BARE_MASTERS
    PSES = _BARE_PSES
    ROUTERS = _BARE_ROUTERS



# ---------------------------------------------------------------------------
# Pid file utilities
# ---------------------------------------------------------------------------


def _read_pid(role, instance):
    """role in {master,ps,router}; instance like 'm1' or 1."""
    pid_file = PID_DIR / f"{role}{instance}.pid"
    if not pid_file.exists():
        return None
    try:
        return int(pid_file.read_text().strip())
    except (ValueError, OSError):
        return None


def _is_alive(pid):
    """True only if `pid` is a real running process, NOT a zombie.

    Subtle: os.kill(pid, 0) returns success on zombie (defunct) processes
    too — they retain a valid pid until the parent reaps them. When we
    Popen a child and SIGKILL it, the kernel marks it Z(ombie) but the
    pid stays valid until we waitpid(). Without this distinction
    kill_master would loop forever waiting for the pid to "vanish".

    We disambiguate by reading /proc/<pid>/status — Linux only, but
    the cluster scripts already assume Linux.
    """
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    # The pid exists; check whether it's a zombie.
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("State:"):
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].upper() == "Z":
                        return False  # zombie = effectively dead
                    return True
    except (FileNotFoundError, PermissionError, OSError):
        # /proc unreadable or already vanished — best-effort fallback.
        return False
    return True


def _wait_until(predicate, timeout=30, interval=0.5, desc=""):
    """Poll predicate() until True or timeout. Raises TimeoutError on miss."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for: {desc}")


# ---------------------------------------------------------------------------
# Docker-mode primitives (only used when CLUSTER_MODE == "docker")
# ---------------------------------------------------------------------------


def _docker_inspect_running(container_name):
    """True if docker container is in 'running' state. False if stopped /
    paused / doesn't exist. Used in place of pid-based _is_alive() for
    docker mode."""
    try:
        out = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}",
             container_name],
            capture_output=True, text=True, timeout=5)
        return out.returncode == 0 and out.stdout.strip() == "true"
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


def _docker_kill_container(container_name, hard=True, timeout=10):
    """Kill (or stop) a docker container.
    hard=True  → `docker kill --signal=SIGKILL` (immediate, not graceful)
    hard=False → `docker stop -t 5` (SIGTERM, then SIGKILL after 5s grace)
    Both are idempotent: calling again on an already-stopped container is a no-op.
    """
    if not _docker_inspect_running(container_name):
        return
    sig_args = ["--signal=SIGKILL"] if hard else ["--signal=SIGTERM"]
    cmd = ["docker", "kill"] + sig_args + [container_name] if hard else \
          ["docker", "stop", "-t", "5", container_name]
    subprocess.run(cmd, capture_output=True, timeout=timeout)
    _wait_until(lambda: not _docker_inspect_running(container_name),
                timeout=timeout,
                desc=f"docker container {container_name} to stop")


def _docker_start_container(container_name, wait_timeout=30):
    """`docker start` an existing but stopped container. Idempotent."""
    if _docker_inspect_running(container_name):
        return
    res = subprocess.run(
        ["docker", "start", container_name],
        capture_output=True, text=True, timeout=wait_timeout)
    if res.returncode != 0:
        raise RuntimeError(
            f"docker start {container_name} failed: stderr={res.stderr[:300]}")
    _wait_until(lambda: _docker_inspect_running(container_name),
                timeout=wait_timeout,
                desc=f"docker container {container_name} to start")


def _docker_logs_tail(container_name, n=30):
    """`docker logs --tail N`. Returns an empty string on failure (used for
    error diagnostics, must not raise)."""
    try:
        out = subprocess.run(
            ["docker", "logs", "--tail", str(n), container_name],
            capture_output=True, text=True, timeout=5)
        return (out.stdout or "") + (out.stderr or "")
    except Exception:
        return ""


def _docker_exec_cat_logs(container_name, timeout=15):
    """In docker mode vearch writes logs to files (config_cluster.toml sets
    `log = "logs/"`, and with toConsole=false nothing goes to stdout). That path
    is *relative* — the final runtime image (the centos stage of cloud/Dockerfile)
    sets no WORKDIR, so the container CWD is `/` and logs actually land under
    `/logs/` (not `/vearch/logs/`; the latter is only the builder stage's
    WORKDIR). Here we cat both candidate paths and take whichever has content.

    `docker logs` gives stdout, whereas these are file logs, so we must exec into
    the container to read the files. The container must be running (a killed node
    is unreadable). Any failure returns "", because log scanning is best-effort
    and must not let diagnostic logic take the test down.
    """
    if not _docker_inspect_running(container_name):
        return ""
    try:
        out = subprocess.run(
            ["docker", "exec", container_name, "sh", "-c",
             "cat /logs/*.log /vearch/logs/*.log 2>/dev/null"],
            capture_output=True, text=True, timeout=timeout)
        return out.stdout or ""
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""


def _docker_exec_master_healthy(container_name, timeout=8):
    """In docker mode, when a master's api port is not exposed to the host
    (m2/m3), exec into the container and curl localhost:8817/servers to check
    liveness (every master listens on 8817 inside its container; see the
    docker-compose healthcheck). The container must be running; any failure
    returns False.

    Problem this solves: wait_for_master_quorum originally only probed masters
    whose api port was host-exposed (only m1 in docker). Once a chaos test kills
    m1, no master is probeable on the host, so even a healthy m2+m3 quorum would
    be misjudged as "quorum lost".
    """
    if not _docker_inspect_running(container_name):
        return False
    try:
        out = subprocess.run(
            ["docker", "exec", container_name, "sh", "-c",
             "curl -fs -u root:secret http://localhost:8817/servers"],
            capture_output=True, text=True, timeout=timeout)
        if out.returncode != 0 or not out.stdout:
            return False
        import json as _json
        return _json.loads(out.stdout).get("code") == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError, ValueError):
        return False


# bare mode: host log directory name per role (subdirectory under LOG_DIR).
_BARE_LOG_DIRNAME = {
    "ps": lambda idx: f"ps{idx}",
    "router": lambda idx: f"router{idx}",
    "master": lambda name: f"master_{name}",
}


def read_node_logs(role, idx):
    """Return the full log text of a node, mode-aware; returns "" if
    unavailable (never raises).

    role: 'ps' | 'router' | 'master'
    idx:  int (1/2/3) for ps/router; 'm1'/'m2'/'m3' for master

    docker: `docker exec <container> cat /vearch/logs/*.log` (container must be running).
    bare:   concatenate the contents of LOG_DIR/<dir>/*.log.

    Chaos tests use this to scan logs for characteristic lines (e.g. the router's
    fallback line, the PS's dispatched line). In docker mode the logs live inside
    the container and the old hard-coded host path can't read them — use this
    unified entry point.
    """
    if CLUSTER_MODE == "docker":
        table = {"ps": PSES, "router": ROUTERS, "master": MASTERS}.get(role)
        if not table or idx not in table:
            return ""
        container = table[idx].get("container_name")
        if not container:
            return ""
        return _docker_exec_cat_logs(container)

    # bare mode: read host log files.
    namer = _BARE_LOG_DIRNAME.get(role)
    if namer is None:
        return ""
    log_dir = LOG_DIR / namer(idx)
    if not log_dir.exists():
        return ""
    chunks = []
    for f in log_dir.glob("*.log"):
        try:
            chunks.append(f.read_text(errors="ignore"))
        except OSError:
            continue
    return "\n".join(chunks)



# ---------------------------------------------------------------------------
# PS fault injection
# ---------------------------------------------------------------------------


def _reap_if_child(pid):
    """Best-effort waitpid to remove zombie entry. Safe to call even if
    pid is not our child (raises ChildProcessError, which we swallow)."""
    try:
        os.waitpid(pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        pass


def kill_ps(idx, hard=True):
    """Kill PS instance `idx` (1/2/3). hard=True uses SIGKILL, else SIGTERM.
    Dispatches by CLUSTER_MODE — bare uses os.kill on the PID, docker uses
    `docker kill`.
    """
    if CLUSTER_MODE == "docker":
        return _docker_kill_container(
            PSES[idx]["container_name"], hard=hard, timeout=10)

    pid = _read_pid("ps", idx)
    if pid is None:
        raise RuntimeError(f"no pid file for ps{idx}; is cluster started?")
    sig = signal.SIGKILL if hard else signal.SIGTERM
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        return  # already dead
    _reap_if_child(pid)
    # _is_alive() now distinguishes zombie from running, so this exits
    # promptly even if waitpid above didn't reap (e.g. pid not our child).
    _wait_until(lambda: not _is_alive(pid), timeout=10,
                desc=f"ps{idx} (pid {pid}) to die")
    pid_file = PID_DIR / f"ps{idx}.pid"
    if pid_file.exists():
        pid_file.unlink()


def start_ps(idx, wait_ready=True, timeout=30):
    """(Re)start PS instance `idx`. Idempotent: if already running, no-op.
    Dispatches by CLUSTER_MODE — bare uses subprocess.Popen, docker uses
    `docker start`.
    """
    if CLUSTER_MODE == "docker":
        container = PSES[idx]["container_name"]
        _docker_start_container(container, wait_timeout=timeout)
        # docker mode: 'ready' = container running. The PS rpc port is not
        # exposed to the host, so we can't poll the port like in bare mode; nor
        # do we need to — when docker-compose starts the PS, depends_on router is
        # already healthy and the PS inside the container initializes itself.
        # docker_inspect_running == true is sufficient.
        return container

    existing = _read_pid("ps", idx)
    if _is_alive(existing):
        return existing
    log_dir = LOG_DIR / f"ps{idx}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "startup.log"
    conf = CONF_DIR / f"ps{idx}.toml"
    proc = subprocess.Popen(
        [VEARCH_BIN, "-conf", str(conf), "ps"],
        stdout=open(log_file, "ab"),
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
        env=_subprocess_env(),
        start_new_session=True,
    )
    PID_DIR.mkdir(exist_ok=True)
    (PID_DIR / f"ps{idx}.pid").write_text(str(proc.pid))

    # Liveness gate: verify the process didn't die immediately. PS can
    # crash early on port conflict (e.g. raft heartbeat port still held
    # by zombie) — without this check, wait_for_ps_ready below would
    # then time out with a misleading "port not listening" message.
    time.sleep(2)
    if not _is_alive(proc.pid):
        tail = ""
        try:
            data = log_file.read_text(errors="ignore")
            tail = "\n".join(data.splitlines()[-30:])
        except OSError:
            pass
        raise RuntimeError(
            f"ps{idx} (pid {proc.pid}) died within 2s after launch.\n"
            f"--- tail of {log_file} ---\n{tail}")

    if wait_ready:
        wait_for_ps_ready(idx, timeout=timeout)
    return proc.pid


def wait_for_ps_ready(idx, timeout=30):
    """Poll PS rpc port until accepting TCP connections.

    docker mode: the PS rpc port is not exposed to the host, so this function
    becomes a no-op (the container running is enough, and start_ps already
    verified it).
    """
    if CLUSTER_MODE == "docker":
        return
    import socket
    port = PSES[idx]["rpc"]

    def _connect():
        with socket.socket() as s:
            s.settimeout(1)
            try:
                s.connect(("127.0.0.1", port))
                return True
            except (ConnectionRefusedError, socket.timeout):
                return False
    _wait_until(_connect, timeout=timeout, desc=f"ps{idx}:{port} reachable")


# ---------------------------------------------------------------------------
# Master fault injection
# ---------------------------------------------------------------------------


def kill_master(name, hard=True):
    """name in {'m1','m2','m3'}. Dispatches by CLUSTER_MODE."""
    if CLUSTER_MODE == "docker":
        return _docker_kill_container(
            MASTERS[name]["container_name"], hard=hard, timeout=15)

    pid = _read_pid("master_", name) or _read_pid("master", name)
    if pid is None:
        raise RuntimeError(f"no pid file for master {name}; is cluster started?")
    sig = signal.SIGKILL if hard else signal.SIGTERM
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        return
    _reap_if_child(pid)
    _wait_until(lambda: not _is_alive(pid), timeout=10,
                desc=f"master {name} (pid {pid}) to die")
    for fn in (f"master_{name}.pid", f"master{name}.pid"):
        f = PID_DIR / fn
        if f.exists():
            f.unlink()


def start_master(name, wait_quorum=True, timeout=120, strict=False):
    """Restart a master that was previously killed. Dispatches by CLUSTER_MODE.

    timeout=120 (was 30): embedded etcd recovery from disk + raft re-sync
    with the surviving quorum members commonly takes 30-90s, especially
    when the master was the leader (forces re-election after kill). 30s
    was too aggressive for chaos tests.

    strict=False (default): wait until ANY master responds (quorum check),
    not specifically the one we just restarted. For chaos test cleanup
    we usually only need the cluster as a whole to be queryable; we
    don't need to verify this particular master fully caught up.

    strict=True: wait until THIS master itself answers /servers. Use
    when test logic depends on the specific master being a quorum
    member (e.g. testing leader stickiness). In docker mode, strict=True
    requires that master's host api port to be exposed (by default only m1
    exposes 8817); otherwise it falls back to the lenient quorum check.
    """
    if CLUSTER_MODE == "docker":
        container = MASTERS[name]["container_name"]
        _docker_start_container(container, wait_timeout=timeout)
        if wait_quorum:
            if strict and MASTERS[name].get("api"):
                wait_for_master_ready(name, timeout=timeout)
            else:
                wait_for_master_quorum(timeout=timeout)
        return container

    existing = _read_pid("master_", name) or _read_pid("master", name)
    if _is_alive(existing):
        return existing
    log_dir = LOG_DIR / f"master_{name}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "startup.log"
    conf = CONF_DIR / f"master_{name}.toml"
    proc = subprocess.Popen(
        [VEARCH_BIN, "-conf", str(conf), "-master", name, "master"],
        stdout=open(log_file, "ab"),
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
        env=_subprocess_env(),
        start_new_session=True,
    )
    PID_DIR.mkdir(exist_ok=True)
    (PID_DIR / f"master_{name}.pid").write_text(str(proc.pid))

    # Liveness gate: verify the process didn't die immediately after launch.
    # Without this, a master that crashes during etcd init (port conflict /
    # state mismatch / config error) goes unnoticed because lenient
    # wait_for_master_quorum is satisfied by the surviving masters.
    # Sleep briefly, then check the pid is still alive.
    time.sleep(3)
    if not _is_alive(proc.pid):
        # Capture the last few lines of the log so the caller knows why.
        tail = ""
        try:
            data = log_file.read_text(errors="ignore")
            tail = "\n".join(data.splitlines()[-30:])
        except OSError:
            pass
        raise RuntimeError(
            f"master {name} (pid {proc.pid}) died within 3s after launch.\n"
            f"Common causes: port conflict, etcd state/config mismatch, "
            f"missing data dir.\n"
            f"--- tail of {log_file} ---\n{tail}")

    if wait_quorum:
        if strict:
            # Strict mode: this specific master must answer /servers.
            wait_for_master_ready(name, timeout=timeout)
        else:
            # Lenient (default): any master answering = quorum healthy,
            # which is what most chaos test cleanup actually needs.
            wait_for_master_quorum(timeout=timeout)
    return proc.pid


def wait_for_master_ready(name, timeout=30):
    """Poll until the specific master responds to /servers.
    In docker mode, that master must have an api port exposed to the host
    (by default only m1); if none is exposed, raise — the caller should use
    wait_for_master_quorum instead.
    """
    api_port = MASTERS[name].get("api")
    if api_port is None:
        raise RuntimeError(
            f"master {name} has no host-exposed api port (docker mode); "
            f"use wait_for_master_quorum() instead")

    def _ok():
        try:
            r = requests.get(
                f"http://127.0.0.1:{api_port}/servers",
                auth=AUTH,
                timeout=2,
            )
            return r.status_code == 200 and r.json().get("code") == 0
        except Exception:
            return False

    _wait_until(_ok, timeout=timeout, desc=f"master {name} to be reachable")


def wait_for_master_quorum(timeout=30):
    """Poll until at least one master is reachable / serving.

    In docker mode only master1 (api=8817) is reachable on the host; the other
    masters' api ports are not exposed, but we can `docker exec` into the
    container and curl localhost:8817 to check liveness. Otherwise, once m1 is
    killed, no master would be probeable on the host and quorum would be
    misjudged as lost (even with a healthy m2+m3 quorum).
    """
    def _ok():
        for name, ports in MASTERS.items():
            api_port = ports.get("api")
            if api_port is not None:
                try:
                    r = requests.get(
                        f"http://127.0.0.1:{api_port}/servers",
                        auth=AUTH,
                        timeout=2,
                    )
                    if r.status_code == 200 and r.json().get("code") == 0:
                        return True
                except Exception:
                    pass
            elif CLUSTER_MODE == "docker":
                if _docker_exec_master_healthy(ports.get("container_name")):
                    return True
        return False
    _wait_until(_ok, timeout=timeout, desc="any master to be reachable")


def find_master_leader():
    """Find the etcd raft leader among the masters.

    Strategy 1: etcdctl endpoint status (bare mode only — docker mode doesn't
        expose the etcd_client port to the host).
    Strategy 2: scan master logs for recent scheduler tick markers
        (bare mode only — in docker mode logs are in `docker logs`, not host files).
    In docker mode neither is available, so fall through to returning None and
    let the caller handle it.

    Returns master name ('m1'/'m2'/'m3') or None when undetermined.
    """
    if CLUSTER_MODE == "docker":
        # In docker mode logs must be pulled from docker logs. Try the
        # docker-logs-based fallback.
        return _find_leader_via_docker_logs()

    leader = _find_leader_via_etcdctl()
    if leader:
        return leader
    return _find_leader_via_logs()


def _find_leader_via_etcdctl():
    # bare mode has the etcd_client field; docker mode doesn't expose that port.
    endpoints_list = []
    for m in MASTERS.values():
        if "etcd_client" in m:
            endpoints_list.append(f"http://127.0.0.1:{m['etcd_client']}")
    if not endpoints_list:
        return None
    endpoints = ",".join(endpoints_list)
    try:
        out = subprocess.run(
            ["etcdctl", "--endpoints", endpoints, "endpoint", "status",
             "-w", "json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=5)
        if out.returncode != 0:
            return None
        import json
        items = json.loads(out.stdout)
        for it in items:
            status = it.get("Status") or it
            mid = status.get("header", {}).get("member_id")
            leader_id = status.get("leader")
            if mid and leader_id and mid == leader_id:
                ep = it.get("Endpoint", "")
                # Match endpoint port to master name.
                for name, ports in MASTERS.items():
                    if "etcd_client" in ports and ep.endswith(f":{ports['etcd_client']}"):
                        return name
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return None


def _find_leader_via_docker_logs():
    """In docker mode, use `docker logs vearch-masterN` to find scheduler tick traces."""
    candidates = []
    for name, info in MASTERS.items():
        container = info.get("container_name")
        if not container:
            continue
        txt = _docker_logs_tail(container, n=2000)
        if any(m in txt for m in (
                "rebuild dispatched", "space ", "admit", "tick")):
            # docker logs has no mtime concept; use txt length as a proxy
            # (an active leader logs more).
            candidates.append((name, len(txt)))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def _find_leader_via_logs():
    """Heuristic: master leader logs 'space ... admitted' or
    'rebuild dispatched' during scheduler ticks. Only the leader does.
    Look at the most recently modified relevant log among the 3 masters.
    """
    candidates = []
    for name in MASTERS:
        log_path = LOG_DIR / f"master_{name}"
        if not log_path.exists():
            continue
        # Find any log file with rebuild scheduler activity.
        latest_mtime = 0
        for f in log_path.glob("*.log"):
            try:
                # grep for scheduler markers
                txt = f.read_text(errors="ignore")[-50000:]  # tail
                if any(marker in txt for marker in (
                        "rebuild dispatched", "space ", "admit", "tick")):
                    latest_mtime = max(latest_mtime, f.stat().st_mtime)
            except OSError:
                continue
        if latest_mtime:
            candidates.append((name, latest_mtime))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


# ---------------------------------------------------------------------------
# Router fault injection (rarely needed but provided for completeness)
# ---------------------------------------------------------------------------


def kill_router(idx, hard=True):
    """Kill router instance `idx`. Dispatches by CLUSTER_MODE."""
    if CLUSTER_MODE == "docker":
        return _docker_kill_container(
            ROUTERS[idx]["container_name"], hard=hard, timeout=10)

    pid = _read_pid("router", idx)
    if pid is None:
        return
    sig = signal.SIGKILL if hard else signal.SIGTERM
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        pass
    _reap_if_child(pid)
    _wait_until(lambda: not _is_alive(pid), timeout=10,
                desc=f"router{idx} (pid {pid}) to die")
    pid_file = PID_DIR / f"router{idx}.pid"
    if pid_file.exists():
        pid_file.unlink()


def start_router(idx, wait_ready=True, timeout=15):
    """(Re)start router instance `idx`. Dispatches by CLUSTER_MODE."""
    if CLUSTER_MODE == "docker":
        container = ROUTERS[idx]["container_name"]
        _docker_start_container(container, wait_timeout=timeout)
        if wait_ready and ROUTERS[idx].get("http"):
            # Only when this router's http port is exposed to the host can we
            # poll to verify.
            port = ROUTERS[idx]["http"]
            _wait_until(lambda: requests.get(
                f"http://127.0.0.1:{port}/dbs",
                auth=AUTH, timeout=1).status_code == 200,
                timeout=timeout, desc=f"router{idx}:{port} reachable")
        return container

    existing = _read_pid("router", idx)
    if _is_alive(existing):
        return existing
    log_dir = LOG_DIR / f"router{idx}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "startup.log"
    conf = CONF_DIR / f"router{idx}.toml"
    proc = subprocess.Popen(
        [VEARCH_BIN, "-conf", str(conf), "router"],
        stdout=open(log_file, "ab"),
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
        env=_subprocess_env(),
        start_new_session=True,
    )
    (PID_DIR / f"router{idx}.pid").write_text(str(proc.pid))
    if wait_ready:
        port = ROUTERS[idx]["http"]
        _wait_until(lambda: requests.get(
            f"http://127.0.0.1:{port}/dbs",
            auth=AUTH, timeout=1).status_code == 200,
            timeout=timeout, desc=f"router{idx}:{port} reachable")
    return proc.pid


# ---------------------------------------------------------------------------
# Cluster sanity checks
# ---------------------------------------------------------------------------


def cluster_is_healthy():
    """Quick check: at least 1 master + 1 router + ≥1 PS responsive.
    Iterate over all MASTERS entries, skipping those whose api port is not
    exposed (m2/m3 in docker mode).
    """
    try:
        for ports in MASTERS.values():
            api_port = ports.get("api")
            if api_port is None:
                continue
            r = requests.get(
                f"http://127.0.0.1:{api_port}/servers",
                auth=AUTH, timeout=2)
            if r.status_code == 200 and r.json().get("code") == 0:
                data = r.json().get("data") or {}
                servers = data.get("servers") or []
                return len(servers) >= 1
    except Exception:
        pass
    return False


def list_registered_pses():
    """Return list of PS rpc_ports currently registered with master.
    In docker mode the rpc_port is also 8081 inside the container (/servers
    reports the container's view of the port), so this function returns "the list
    of rpc_ports in the master-side server cache", which may not match the ports
    in our PSES dict (especially since PSES has no rpc field in docker mode) —
    callers should use this return value only for *count* or *existence* checks,
    not to compare against PSES[idx]["rpc"].
    """
    for ports in MASTERS.values():
        api_port = ports.get("api")
        if api_port is None:
            continue
        try:
            r = requests.get(
                f"http://127.0.0.1:{api_port}/servers",
                auth=AUTH, timeout=2)
            if r.status_code == 200 and r.json().get("code") == 0:
                data = r.json().get("data") or {}
                servers = data.get("servers") or []
                out = []
                for item in servers:
                    server = item.get("server") if isinstance(item, dict) else item
                    if isinstance(server, dict) and server.get("rpc_port"):
                        out.append(server.get("rpc_port"))
                return out
        except Exception:
            continue
    return []


def _query_servers():
    """Return the list of server entries in the master's /servers (each item
    shaped like {"server": {...}} or a bare server dict); returns [] if not
    found. Iterate over MASTERS to find the first api port reachable on the host
    (only m1 is exposed in docker mode)."""
    for ports in MASTERS.values():
        api_port = ports.get("api")
        if api_port is None:
            continue
        try:
            r = requests.get(
                f"http://127.0.0.1:{api_port}/servers",
                auth=AUTH, timeout=2)
            if r.status_code == 200 and r.json().get("code") == 0:
                data = r.json().get("data") or {}
                return data.get("servers") or []
        except Exception:
            continue
    return []


def _docker_container_ip(container_name):
    """The container's IP on the docker network; returns "" on failure."""
    try:
        out = subprocess.run(
            ["docker", "inspect", "--format",
             "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
             container_name],
            capture_output=True, text=True, timeout=5)
        if out.returncode == 0:
            return out.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    return ""


def ps_idx_for_node(node_id):
    """Map a PS nodeID from the master's /servers to the local PS instance
    number (idx). Returns None if it can't be mapped — the caller should
    degrade gracefully accordingly (skip / weak mode).

    bare:   match by rpc_port (server.rpc_port == PSES[idx]['rpc']).
    docker: PS containers all have rpc_port 8081 inside, which can't be told
            apart, so match by container IP instead (server.ip == that PS
            container's IP on the compose network).
    """
    target = None
    for item in _query_servers():
        server = item.get("server") if isinstance(item, dict) and "server" in item else item
        if not isinstance(server, dict):
            continue
        try:
            if int(server.get("name", -1)) == int(node_id):
                target = server
                break
        except (TypeError, ValueError):
            continue
    if target is None:
        return None

    if CLUSTER_MODE == "docker":
        ip = target.get("ip", "")
        if not ip:
            return None
        for idx, info in PSES.items():
            if _docker_container_ip(info["container_name"]) == ip:
                return idx
        return None

    rpc = target.get("rpc_port")
    for idx, info in PSES.items():
        if info.get("rpc") == rpc:
            return idx
    return None

