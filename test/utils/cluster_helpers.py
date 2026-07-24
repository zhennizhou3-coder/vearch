# Copyright 2019 The Vearch Authors.
# Licensed under the Apache License, Version 2.0.

# -*- coding: UTF-8 -*-

"""
Cluster fault-injection helpers for chaos tests (docker-compose mode).

The cluster is the container set brought up by cloud/docker-compose.yml
(the CI path, via the set_cluster_env composite action). kill / start go
through `docker kill / docker start vearch-{role}{idx}`; logs are read via
`docker logs` or `docker exec cat`, not the host filesystem.

Host-exposed ports:
  master1: 8817 (other masters reachable only inside the container network)
  router1: 9001 (other routers reachable only inside the network)
  PS:      no ports exposed to the host

Cross-host fault injection needs different infrastructure
(Toxiproxy / Chaos Mesh / SSH).
"""

import subprocess
import time

import requests

# ---------------------------------------------------------------------------
# Topology — must match cloud/docker-compose.yml
# ---------------------------------------------------------------------------

AUTH = ("root", "secret")

# Only master1 / router1 expose a port to the host; the others are reachable
# only inside the container network. `api`/`http` = host port, or None when
# not exposed. PS rpc (8081) is never exposed, so PSES carries no rpc field.
MASTERS = {
    "m1": {"api": 8817, "container_name": "vearch-master1"},
    "m2": {"api": None, "container_name": "vearch-master2"},
    "m3": {"api": None, "container_name": "vearch-master3"},
}
PSES = {
    1: {"container_name": "vearch-ps1"},
    2: {"container_name": "vearch-ps2"},
    3: {"container_name": "vearch-ps3"},
}
ROUTERS = {
    1: {"http": 9001, "container_name": "vearch-router1"},
    2: {"http": None, "container_name": "vearch-router2"},
}


def _wait_until(predicate, timeout=30, interval=0.5, desc=""):
    """Poll predicate() until True or timeout. Raises TimeoutError on miss."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for: {desc}")


# ---------------------------------------------------------------------------
# Docker primitives
# ---------------------------------------------------------------------------


def _docker_inspect_running(container_name):
    """True iff the container is in 'running' state."""
    try:
        out = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}",
             container_name],
            capture_output=True, text=True, timeout=5)
        return out.returncode == 0 and out.stdout.strip() == "true"
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


def _docker_kill_container(container_name, hard=True, timeout=10):
    """Kill (hard=True, SIGKILL) or stop (hard=False, SIGTERM + 5s grace) a
    container. Idempotent: a no-op if already stopped."""
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
    """Start an existing but stopped container. Idempotent."""
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
    """`docker logs --tail N`. Returns "" on failure (diagnostic-only)."""
    try:
        out = subprocess.run(
            ["docker", "logs", "--tail", str(n), container_name],
            capture_output=True, text=True, timeout=5)
        return (out.stdout or "") + (out.stderr or "")
    except Exception:
        return ""


def _docker_exec_cat_logs(container_name, timeout=15):
    """Read vearch file logs from inside the container.

    vearch logs to files (config_cluster.toml `log = "logs/"`, toConsole=false
    so nothing goes to stdout). The path is relative and the runtime image sets
    no WORKDIR, so the container CWD is `/` and logs land in `/logs/` — we cat
    both `/logs/` and `/vearch/logs/` and take whichever has content. `docker
    logs` only sees stdout, so we must exec in to read the files. The container
    must be running; any failure returns "" since log scanning is best-effort.
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
    """Liveness check for a master whose api port is not host-exposed (m2/m3):
    exec in and curl localhost:8817/servers (every master listens on 8817
    internally). Without this, wait_for_master_quorum probes only host-exposed
    masters (m1), so killing m1 looks like quorum loss even when m2+m3 are
    healthy. Container must be running; any failure returns False."""
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


def read_node_logs(role, idx):
    """Return a node's full log text via `docker exec cat`; "" if unavailable
    (never raises). Container must be running.

    role: 'ps' | 'router' | 'master'
    idx:  int (1/2/3) for ps/router; 'm1'/'m2'/'m3' for master

    Chaos tests use this to scan for marker lines (router fallback lines, PS
    dispatched lines). Logs live inside the container, so this is the single
    entry point for reading them.
    """
    table = {"ps": PSES, "router": ROUTERS, "master": MASTERS}.get(role)
    if not table or idx not in table:
        return ""
    container = table[idx].get("container_name")
    if not container:
        return ""
    return _docker_exec_cat_logs(container)


# ---------------------------------------------------------------------------
# PS fault injection
# ---------------------------------------------------------------------------


def kill_ps(idx, hard=True):
    """Kill PS instance `idx` (1/2/3). hard=True uses SIGKILL, else SIGTERM."""
    return _docker_kill_container(
        PSES[idx]["container_name"], hard=hard, timeout=10)


def start_ps(idx, wait_ready=True, timeout=30):
    """(Re)start PS instance `idx`. Idempotent.

    'ready' == container running: the PS rpc port is not host-exposed so we
    cannot poll it, and docker-compose only starts PS once router is healthy,
    after which the PS initializes itself. A running container is enough.
    """
    container = PSES[idx]["container_name"]
    _docker_start_container(container, wait_timeout=timeout)
    return container


def wait_for_ps_ready(idx, timeout=30):
    """No-op: the PS rpc port is not host-exposed, and start_ps already
    verified the container is running. Kept for interface parity."""
    return


# ---------------------------------------------------------------------------
# Master fault injection
# ---------------------------------------------------------------------------


def kill_master(name, hard=True):
    """name in {'m1','m2','m3'}."""
    return _docker_kill_container(
        MASTERS[name]["container_name"], hard=hard, timeout=15)


def start_master(name, wait_quorum=True, timeout=120, strict=False):
    """Restart a previously killed master.

    strict=False (default): wait until ANY master responds (quorum check) —
    what chaos-test cleanup usually needs.
    strict=True: wait until THIS master answers /servers. Requires its api port
    to be host-exposed (only m1 by default); otherwise falls back to the
    lenient quorum check.
    """
    container = MASTERS[name]["container_name"]
    _docker_start_container(container, wait_timeout=timeout)
    if wait_quorum:
        if strict and MASTERS[name].get("api"):
            wait_for_master_ready(name, timeout=timeout)
        else:
            wait_for_master_quorum(timeout=timeout)
    return container


def wait_for_master_ready(name, timeout=30):
    """Poll until the specific master responds to /servers. Requires its api
    port to be host-exposed (only m1 by default); raises otherwise — caller
    should use wait_for_master_quorum instead."""
    api_port = MASTERS[name].get("api")
    if api_port is None:
        raise RuntimeError(
            f"master {name} has no host-exposed api port; "
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

    Only master1 (api=8817) is reachable on the host; for the others we exec
    into the container and curl localhost:8817. Without this, killing m1 would
    look like quorum loss even when m2+m3 are healthy.
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
            elif _docker_exec_master_healthy(ports.get("container_name")):
                return True
        return False
    _wait_until(_ok, timeout=timeout, desc="any master to be reachable")


def find_master_leader():
    """Find the etcd raft leader among the masters.

    The etcd client port is not host-exposed and logs live inside containers,
    so we scan `docker logs vearch-masterN` for scheduler-tick markers as a
    heuristic. Returns 'm1'/'m2'/'m3', or None when undetermined.
    """
    candidates = []
    for name, info in MASTERS.items():
        container = info.get("container_name")
        if not container:
            continue
        txt = _docker_logs_tail(container, n=2000)
        if any(m in txt for m in (
                "rebuild dispatched", "space ", "admit", "tick")):
            # docker logs has no mtime; use text length as a proxy (an active
            # leader logs more).
            candidates.append((name, len(txt)))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


# ---------------------------------------------------------------------------
# Router fault injection (rarely needed but provided for completeness)
# ---------------------------------------------------------------------------


def kill_router(idx, hard=True):
    """Kill router instance `idx`."""
    return _docker_kill_container(
        ROUTERS[idx]["container_name"], hard=hard, timeout=10)


def start_router(idx, wait_ready=True, timeout=15):
    """(Re)start router instance `idx`."""
    container = ROUTERS[idx]["container_name"]
    _docker_start_container(container, wait_timeout=timeout)
    if wait_ready and ROUTERS[idx].get("http"):
        # Can only poll when this router's http port is host-exposed.
        port = ROUTERS[idx]["http"]
        _wait_until(lambda: requests.get(
            f"http://127.0.0.1:{port}/dbs",
            auth=AUTH, timeout=1).status_code == 200,
            timeout=timeout, desc=f"router{idx}:{port} reachable")
    return container


# ---------------------------------------------------------------------------
# Cluster sanity checks
# ---------------------------------------------------------------------------


def cluster_is_healthy():
    """Quick check: at least 1 master + 1 router + >=1 PS responsive. Skips
    masters whose api port is not host-exposed (m2/m3)."""
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
    """Return the PS rpc_ports currently registered with master, as reported by
    /servers (container-side ports). Use the result only for count/existence
    checks — it will not match the PSES dict (which carries no rpc field)."""
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
    """Return the server entries from master /servers (each {"server": {...}}
    or a bare server dict), or [] if unreachable. Uses the first host-reachable
    api port (only m1 is exposed)."""
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
    """Container IP within the docker network; "" on failure."""
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
    """Map a PS nodeID from master /servers to a local PS instance idx, or None
    if it can't be resolved (caller should degrade gracefully). PS containers
    all use rpc_port 8081, so we match on container IP (server.ip) instead."""
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

    ip = target.get("ip", "")
    if not ip:
        return None
    for idx, info in PSES.items():
        if _docker_container_ip(info["container_name"]) == ip:
            return idx
    return None
