# Copyright 2019 The Vearch Authors.
# Licensed under the Apache License, Version 2.0.

# -*- coding: UTF-8 -*-

"""
Cluster fault-injection helpers for chaos tests.

支持两种集群部署模式:
  1. **bare-metal** — 由 scripts/cluster.sh 启动的本地裸进程集群,
     PID 文件在 .cluster_pids/ 下,kill / start 通过 os.kill +
     subprocess.Popen 完成。
  2. **docker-compose** — 由 cloud/docker-compose.yml 起的容器集群
     (CI 走的路径,via set_cluster_env composite action)。kill /
     start 通过 `docker kill / docker start vearch-{role}{idx}` 完成,
     日志通过 `docker logs` 而非 host 文件系统。

模式选择:
  CLUSTER_MODE 环境变量显式指定 ("bare" / "docker"),
  否则 auto-detect:
    - .cluster_pids/ 有 *.pid 文件 ⇒ bare-metal
    - 否则 `docker ps` 显示 vearch-* 容器 ⇒ docker
    - 都没有 ⇒ 默认 bare-metal

端口映射(docker mode host 暴露):
  master1: 8817 (其它 master 仅容器内可达)
  router1: 9001 (其它 router 仅容器内可达)
  PS:      不暴露任何端口到 host

跨 host fault injection 需要不同基础设施 (Toxiproxy / Chaos Mesh / SSH)。
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
# Only relevant in bare-metal mode (docker mode 的 binary 在镜像 /vearch/lib/
# 下,镜像 build 时已经 LD_LIBRARY_PATH 配好)。
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
    """Auto-detect 集群部署模式。env var VEARCH_CLUSTER_MODE 优先于自动检测。

    自动检测规则:
      1. .cluster_pids/ 下有任意 *.pid 文件 ⇒ bare(scripts/cluster.sh 在跑)
      2. `docker ps` 看到 vearch-* 容器 ⇒ docker
      3. 都没看到 ⇒ 默认 bare(假设用户即将用 cluster.sh 起)
    """
    forced = os.environ.get("VEARCH_CLUSTER_MODE", "").strip().lower()
    if forced in ("bare", "docker"):
        return forced

    # 1. PID 文件优先
    try:
        if PID_DIR.exists() and any(PID_DIR.glob("*.pid")):
            return "bare"
    except OSError:
        pass

    # 2. docker container 探测
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", "name=vearch-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        if out.returncode == 0 and out.stdout.strip():
            return "docker"
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass

    # 3. 默认 bare
    return "bare"


CLUSTER_MODE = _detect_mode()


# Mode-specific port / name tables.
# bare 模式对应 scripts/cluster.sh + config/*.toml;
# docker 模式对应 cloud/docker-compose.yml 的容器名 + host 端口映射。

_BARE_MASTERS = {
    "m1": {"api": 28817, "etcd_client": 22370, "monitor": 28821},
    "m2": {"api": 28827, "etcd_client": 22371, "monitor": 28831},
    "m3": {"api": 28837, "etcd_client": 22372, "monitor": 28841},
}
_BARE_PSES = {
    1: {"rpc": 18081},
    2: {"rpc": 18082},
    3: {"rpc": 18083},
}
_BARE_ROUTERS = {
    1: {"http": 19001},
    2: {"http": 19002},
}

# docker mode: 只有 master1 / router1 把端口暴露到 host。其它节点的端口
# 仅容器内可达。这里 `api`/`http` 字段值 = host 端口(暴露的)或 None(没
# 暴露的);PS rpc 端口在容器内是 8081,但 host 不可见,所以 PSES 表里
# 不再带 rpc。container_name 字段是 docker mode 用的容器名。
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
    hard=True  → `docker kill --signal=SIGKILL` (即时,不 graceful)
    hard=False → `docker stop -t 5` (SIGTERM 5s grace 后 SIGKILL)
    都是幂等的:容器已经停了再调一次不报错。
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
    """`docker start` 一个已经存在但 stopped 的容器。幂等。"""
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
    """`docker logs --tail N`。失败返回空字符串(用于错误诊断,不应 raise)。"""
    try:
        out = subprocess.run(
            ["docker", "logs", "--tail", str(n), container_name],
            capture_output=True, text=True, timeout=5)
        return (out.stdout or "") + (out.stderr or "")
    except Exception:
        return ""


def _docker_exec_cat_logs(container_name, timeout=15):
    """docker 模式下 vearch 把日志写在容器内 /vearch/logs/*.log(见
    config_cluster.toml `log = "logs/"` + 容器 WORKDIR /vearch);日志目录没
    挂载到 host,所以 host 文件系统读不到,`docker logs` 拿到的是 stdout 而非
    文件日志。这里用 `docker exec ... cat` 把容器内全部日志文件读出来。

    容器必须 running(被 kill 掉的节点读不到)。任何失败都返回 "",因为
    日志扫描是 best-effort,不应让诊断逻辑把测试带挂。
    """
    if not _docker_inspect_running(container_name):
        return ""
    try:
        out = subprocess.run(
            ["docker", "exec", container_name, "sh", "-c",
             "cat /vearch/logs/*.log 2>/dev/null"],
            capture_output=True, text=True, timeout=timeout)
        return out.stdout or ""
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""


# bare 模式下各角色的 host 日志目录名(LOG_DIR 下的子目录)。
_BARE_LOG_DIRNAME = {
    "ps": lambda idx: f"ps{idx}",
    "router": lambda idx: f"router{idx}",
    "master": lambda name: f"master_{name}",
}


def read_node_logs(role, idx):
    """返回某个节点的全部日志文本,mode-aware;拿不到返回 ""(永不 raise)。

    role: 'ps' | 'router' | 'master'
    idx:  ps/router 用 int (1/2/3);master 用 'm1'/'m2'/'m3'

    docker: `docker exec <container> cat /vearch/logs/*.log`(容器须 running)。
    bare:   拼接 LOG_DIR/<dir>/*.log 的内容。

    chaos 测试用它扫日志找特征行(如 router 的 fallback 行、PS 的 dispatched
    行)。docker 模式下日志在容器内,host 读不到旧的硬编码路径 —— 用这个统一入口。
    """
    if CLUSTER_MODE == "docker":
        table = {"ps": PSES, "router": ROUTERS, "master": MASTERS}.get(role)
        if not table or idx not in table:
            return ""
        container = table[idx].get("container_name")
        if not container:
            return ""
        return _docker_exec_cat_logs(container)

    # bare mode: 读 host 日志文件。
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
    Dispatches by CLUSTER_MODE — bare 走 os.kill PID,docker 走 `docker kill`。
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
    Dispatches by CLUSTER_MODE — bare 走 subprocess.Popen,docker 走
    `docker start`。
    """
    if CLUSTER_MODE == "docker":
        container = PSES[idx]["container_name"]
        _docker_start_container(container, wait_timeout=timeout)
        # docker mode: 'ready' = container running. PS rpc 端口不暴露到 host,
        # 没法像 bare mode 那样 polling 端口;我们也没必要 — docker-compose
        # 起 PS 时 depends_on router 已经 healthy,容器内 PS 会自己初始化
        # 完成。docker_inspect_running == true 已经够用。
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

    docker mode: PS rpc 端口不暴露到 host,这条函数变成 no-op (container
    running 就够了,start_ps 已经验过)。
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
    member (e.g. testing leader stickiness). docker mode 下,strict=True
    要求该 master 的 host api 端口被暴露(默认只 m1 暴露 8817),否则
    fallback 到 lenient quorum check。
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
    docker mode 下,该 master 必须有 api 端口暴露到 host(默认只 m1);
    没暴露 api 的话 raise — caller 用 wait_for_master_quorum 代替。
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
    """Poll until at least one master with a host-exposed api port responds
    to /servers. docker mode 下只 master1 (api=8817) 在 host 上可达。
    """
    def _ok():
        for name, ports in MASTERS.items():
            api_port = ports.get("api")
            if api_port is None:
                continue
            try:
                r = requests.get(
                    f"http://127.0.0.1:{api_port}/servers",
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
    """Find the etcd raft leader among the masters.

    Strategy 1: etcdctl endpoint status (bare mode only — docker mode 没把
        etcd_client 端口暴露到 host)。
    Strategy 2: scan master logs for recent scheduler tick markers
        (bare mode only — docker mode 日志在 `docker logs` 不在 host 文件)。
    docker mode 下两条都不可用,fall through 到返回 None,caller 自行处理。

    Returns master name ('m1'/'m2'/'m3') or None when undetermined.
    """
    if CLUSTER_MODE == "docker":
        # docker 模式下日志要从 docker logs 拉。尝试基于 docker logs 的 fallback。
        return _find_leader_via_docker_logs()

    leader = _find_leader_via_etcdctl()
    if leader:
        return leader
    return _find_leader_via_logs()


def _find_leader_via_etcdctl():
    # bare-mode 才有 etcd_client 字段;docker mode 没暴露这个端口。
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
    """docker mode 下用 `docker logs vearch-masterN` 找 scheduler tick 痕迹。"""
    candidates = []
    for name, info in MASTERS.items():
        container = info.get("container_name")
        if not container:
            continue
        txt = _docker_logs_tail(container, n=2000)
        if any(m in txt for m in (
                "rebuild dispatched", "space ", "admit", "tick")):
            # docker logs 没 mtime 概念,用 txt 长度作 proxy(活跃 leader 日志多)
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
            # 仅当该 router 的 http 端口被暴露到 host 时才能 polling 验证。
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
    遍历所有 MASTERS 项,跳过 api 端口没暴露的(docker mode m2/m3)。
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
    docker mode 下 rpc_port 也是容器内 8081(/servers 报告的是容器视角的端口),
    所以这条函数返回的是「master 端 server cache 里的 rpc_port 列表」,
    跟我们 PSES dict 里的端口可能不一致(尤其是 docker mode 下 PSES 没存
    rpc 字段)— caller 用这个返回值仅做 *数量* 或 *存在性* 判断,不要拿
    来跟 PSES[idx]["rpc"] 比较。
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
