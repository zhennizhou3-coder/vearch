# Copyright 2019 The Vearch Authors.
# Licensed under the Apache License, Version 2.0.

# -*- coding: UTF-8 -*-

"""
Cluster fault-injection helpers for chaos tests.

Assumes the local cluster is started by scripts/cluster.sh, which writes
PID files under .cluster_pids/ named master_m{1,2,3}.pid, ps{1,2,3}.pid,
router{1,2}.pid.

All operations target processes on 127.0.0.1; cross-host fault injection
needs different infrastructure (Toxiproxy / Chaos Mesh / SSH).
"""

import os
import signal
import subprocess
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Topology — must match config/*.toml + scripts/cluster.sh.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
CLUSTER_SCRIPT = REPO_ROOT / "scripts" / "cluster.sh"
PID_DIR = REPO_ROOT / ".cluster_pids"
CONF_DIR = REPO_ROOT / "config"
LOG_DIR = REPO_ROOT / "logs"
VEARCH_BIN = os.environ.get("VEARCH_BIN", str(REPO_ROOT / "build/bin/vearch"))
AUTH = ("root", os.environ.get("PASSWORD", "secret"))

# Where the vearch binary's CGO dependency `libgamma.so` lives.
# vearch makefile drops it under build/gamma_build/. Tests start the binary
# directly via subprocess; without this, ld.so can't resolve libgamma.so on
# the bare-process path (docker-compose path is fine because the image
# pre-installs the lib under /usr/local/lib).
# Override with VEARCH_LIB_DIR=... if your local layout differs.
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

MASTERS = {
    "m1": {"api": 28817, "etcd_client": 22370, "monitor": 28821},
    "m2": {"api": 28827, "etcd_client": 22371, "monitor": 28831},
    "m3": {"api": 28837, "etcd_client": 22372, "monitor": 28841},
}
PSES = {
    1: {"rpc": 18081},
    2: {"rpc": 18082},
    3: {"rpc": 18083},
}
ROUTERS = {
    1: {"http": 19001},
    2: {"http": 19002},
}


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
    """Kill PS instance `idx` (1/2/3). hard=True uses SIGKILL, else SIGTERM."""
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
    """(Re)start PS instance `idx`. Idempotent: if already running, no-op."""
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
    """Poll PS rpc port until accepting TCP connections."""
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
    """name in {'m1','m2','m3'}."""
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
    """Restart a master that was previously killed.

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
    member (e.g. testing leader stickiness).
    """
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
    """Poll until the specific master responds to /servers."""
    ports = MASTERS[name]

    def _ok():
        try:
            r = requests.get(
                f"http://127.0.0.1:{ports['api']}/servers",
                auth=AUTH,
                timeout=2,
            )
            return r.status_code == 200 and r.json().get("code") == 0
        except Exception:
            return False

    _wait_until(_ok, timeout=timeout, desc=f"master {name} to be reachable")


def wait_for_master_quorum(timeout=30):
    """Poll until at least one master responds to /servers."""
    def _ok():
        for name, ports in MASTERS.items():
            try:
                r = requests.get(
                    f"http://127.0.0.1:{ports['api']}/servers",
                    auth=AUTH,
                    timeout=2,
                )
                if r.status_code == 200 and r.json().get("code") == 0:
                    return True
            except Exception:
                continue
        return False
    _wait_until(_ok, timeout=timeout, desc="any master to be reachable")


def find_master_leader():
    """Find the etcd raft leader among the 3 masters.

    Strategy 1: etcdctl endpoint status (if installed).
    Strategy 2: scan master logs for recent scheduler tick markers.
    Returns master name ('m1'/'m2'/'m3') or None when undetermined.
    """
    leader = _find_leader_via_etcdctl()
    if leader:
        return leader
    return _find_leader_via_logs()


def _find_leader_via_etcdctl():
    endpoints = ",".join(
        f"http://127.0.0.1:{m['etcd_client']}" for m in MASTERS.values())
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
                    if ep.endswith(f":{ports['etcd_client']}"):
                        return name
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return None


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
    """Quick check: at least 1 master + 1 router + ≥1 PS responsive."""
    try:
        for ports in MASTERS.values():
            r = requests.get(
                f"http://127.0.0.1:{ports['api']}/servers",
                auth=AUTH, timeout=2)
            if r.status_code == 200 and r.json().get("code") == 0:
                data = r.json().get("data") or {}
                servers = data.get("servers") or []
                return len(servers) >= 1
    except Exception:
        pass
    return False


def list_registered_pses():
    """Return list of PS rpc_ports currently registered with master."""
    for ports in MASTERS.values():
        try:
            r = requests.get(
                f"http://127.0.0.1:{ports['api']}/servers",
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
