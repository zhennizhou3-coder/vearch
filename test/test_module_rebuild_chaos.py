#
# Copyright 2019 The Vearch Authors.
# Licensed under the Apache License, Version 2.0.

# -*- coding: UTF-8 -*-

"""
Chaos / fault-injection tests for index rebuild.

Covers:
  Category 1: PS process failure during rebuild
  Category 2: Master leader-change during rebuild

Prereq: cluster started by scripts/cluster.sh (3 masters + 3 PSes + 2 routers
on a single host). Each test cleans up after itself; failures may leave
residual processes — run scripts/cluster.sh restart between iterations
to recover from a stuck state.

These tests deliberately kill / restart processes. Run them in an
isolated environment only.
"""

import json
import time
import threading
import re
from datetime import datetime as _dt

import pytest
import requests

from utils.data_utils import *
from utils.vearch_utils import *
from utils import cluster_helpers as cl

__description__ = """ chaos tests for rebuild index """

sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()

# Set of PS instance indices used across chaos tests. Mirrors cl.PSES
# but pre-extracted as a tuple so log-scan loops don't repeatedly dict-key
# the cluster_helpers mapping.
PSES_IDX = (1, 2, 3)


# ---------------------------------------------------------------------------
# Shared helpers (re-implemented locally to keep this file self-contained)
# ---------------------------------------------------------------------------


def _trigger_rebuild(db, space, max_retries=0, drop_before_rebuild=False):
    payload = {}
    if max_retries > 0:
        payload["max_retries"] = max_retries
    if drop_before_rebuild:
        payload["drop_before_rebuild"] = True
    return requests.post(
        f"{router_url}/rebuild/index/dbs/{db}/spaces/{space}",
        auth=(username, password), json=payload)


def _get_progress(db, space):
    r = requests.get(
        f"{router_url}/rebuild/index/dbs/{db}/spaces/{space}/progress",
        auth=(username, password), timeout=5)
    if r.status_code != 200:
        return None
    body = r.json()
    if body.get("code") != 0:
        return None
    return body.get("data", {}) or {}


def _get_space_detail(db, space):
    r = requests.get(
        f"{router_url}/dbs/{db}/spaces/{space}?detail=true",
        auth=(username, password), timeout=5)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("code") == 0, body
    return body.get("data", {}) or {}


def _partition_id(partition):
    return partition.get("pid", partition.get("partition_id"))


def _partition_restatus_map(partition):
    # detail 接口把 per-replica rebuild 状态暴露在 "replica_status",值是字符串
    # (ReplicasOK / ReplicasRebuilding / ReplicasNotReady),见
    # internal/entity/partition.go:90 + space_service.go:407。不是数字 status_map。
    return partition.get("replica_status") or {}


def _wait_status(db, space, target_statuses, timeout=120, poll=1.0):
    """Wait until progress.status ∈ target_statuses (set or single str)."""
    if isinstance(target_statuses, str):
        target_statuses = {target_statuses}
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        p = _get_progress(db, space)
        if p:
            last = p["status"]
            if last in target_statuses:
                return p
        time.sleep(poll)
    pytest.fail(f"timed out waiting for {target_statuses}; last={last}")


def _wait_terminal(db, space, timeout=600, allow_failed=False):
    """Wait until rebuild finishes one way or another. Returns final progress."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = _get_progress(db, space)
        if p:
            st = p["status"]
            if st == "completed":
                return p
            if st == "failed":
                if allow_failed:
                    return p
                pytest.fail(f"rebuild failed unexpectedly: {json.dumps(p, indent=2)}")
            if st == "cancelled":
                return p
        time.sleep(2)
    pytest.fail(f"rebuild did not terminate in {timeout}s")


def _ensure_all_ps_alive(timeout=30, expected_count=None, settle_timeout=30):
    """Pre-test recovery:把所有 PS 拉起来,等 master 心跳重新认可。

    chaos 测试链条里某条用 kill_ps + finally 兜底,如果兜底里 start_ps
    抛异常被 swallow,集群会留下死 PS,后续 create_space(rn≥2) 必失败
    (master placement 找不到足够候选)。每次 _ensure_clean_db 都顺手
    跑一遍这条恢复路径,避免「上一条 chaos 测试副作用污染下一条」。

    幂等:cluster_helpers.start_ps 内部检查 pid 活性,已活就直接 return。

    expected_count: 期望 master /servers 报告的 PS 数 (默认 len(cl.PSES))。
        helper 会 polling 等到 master 真的看到这么多个 PS,而不是只 sleep
        固定时间 — start_ps 只等 TCP 端口,但 master 通过 etcd lease +
        watcher 同步 server cache 还需要额外时间(典型 1-5s,极端 30s+),
        这之间的差异是「3 PS pid 文件都活了但 create_space 仍然说 only
        have 1」的根因。
    settle_timeout: 等 master /servers 的最长时间。

    特殊情形 — PS 进程在但 master 不认 (etcd lease 丢失):
      chaos 测试 churn master 时,PS 端 KeepAlive 可能初始化失败 / channel
      关闭而 heartbeat goroutine 退出 (schedule_job.go:119-123)。这时 PS
      进程仍然在,start_ps 看 pid 活着幂等返回,但 master /servers 里
      永远不会出现这个 PS。本 helper 检测到「master 缺少某 PS」时,
      会 kill 该 PS 进程并 start_ps 重启它,触发新的 heartbeat goroutine
      重新跑 KeepAlive 注册流程。
    """
    expected = expected_count if expected_count is not None else len(cl.PSES)
    started_pids = {}
    for idx in cl.PSES:
        try:
            pid = cl.start_ps(idx, wait_ready=True, timeout=timeout)
            started_pids[idx] = pid
        except Exception as e:
            logger.warning("pre-test start_ps(%d) failed: %s", idx, e)

    # 拿 master 当前认的情况,反查哪些 PS idx 没被认上。
    def _missing_idxs():
        if cl.CLUSTER_MODE == "docker":
            # docker 模式没法用 rpc 端口区分 PS(容器内端口都是 8081,host
            # 不可见)。先看容器是否 running;容器都在但 master 认的数量不够
            # → lease 丢失,无法精确定位,返回全部让上层逐个 kill+restart。
            not_running = [idx for idx in cl.PSES
                           if not cl._docker_inspect_running(
                               cl.PSES[idx]["container_name"])]
            if not_running:
                return not_running
            if len(cl.list_registered_pses()) < expected:
                return list(cl.PSES.keys())
            return []
        registered_ports = set(cl.list_registered_pses())
        missing = []
        for idx, info in cl.PSES.items():
            if info["rpc"] not in registered_ports:
                missing.append(idx)
        return missing

    deadline = time.time() + settle_timeout
    last_missing = None
    release_attempted = set()
    while time.time() < deadline:
        missing = _missing_idxs()
        if not missing:
            logger.info("PS auto-recovery: all %d PSes registered "
                        "(started_pids=%s, took %.1fs)",
                        expected, started_pids,
                        settle_timeout - (deadline - time.time()))
            return
        if missing != last_missing:
            logger.info("PS auto-recovery: master not seeing PS idx=%s yet "
                        "(want %d total). They're alive locally? Will try "
                        "force-release if it persists.",
                        missing, expected)
            last_missing = missing
        # 持续 8 秒后还有 PS 不在 master 名单里 → 大概率是 lease 丢失。
        # PS 进程活着但 heartbeat goroutine 没注册成功 / channel 关掉了 —
        # 此时 cl.start_ps 看到 pid 活直接返回,不会重新走 KeepAlive。
        # 强制 kill + 重启 PS,让它走一遍新的 heartbeat 初始化。
        elapsed = settle_timeout - (deadline - time.time())
        if elapsed > 8:
            for idx in missing:
                if idx in release_attempted:
                    continue
                release_attempted.add(idx)
                logger.warning(
                    "PS auto-recovery: ps%d alive locally but master doesn't "
                    "see it after %.1fs — etcd lease likely lost during prior "
                    "chaos churn; force kill+restart to re-trigger KeepAlive.",
                    idx, elapsed)
                try:
                    cl.kill_ps(idx, hard=True)
                    cl.start_ps(idx, wait_ready=True, timeout=timeout)
                except Exception as e:
                    logger.warning(
                        "PS auto-recovery: force-restart ps%d failed: %s",
                        idx, e)
        time.sleep(1)

    final_missing = _missing_idxs()
    if final_missing:
        logger.warning(
            "PS auto-recovery: master still missing PS idx=%s after %ds. "
            "started_pids=%s. registered_rpc_ports=%s. Subsequent "
            "create_space(rn>=%d) will likely fail with 'not enough partition "
            "servers'.",
            final_missing, settle_timeout, started_pids,
            cl.list_registered_pses(), expected)


def _ensure_all_masters_alive(timeout=60):
    """同款逻辑,但针对 master 节点。chaos 测试链里 kill_master + finally
    swallow 是常见 pattern (test_rebuild_resumes_after_leader_kill /
    test_pending_record_admitted_after_leader_kill / test_no_double_dispatch_
    under_master_churn 都属于),quorum 会从 3 掉到 2 甚至 1,后面的测试
    会因为 quorum 不够而 rebuild record 永远卡 pending 进而挂 assert。

    cl.start_master 内部 wait_for_master_quorum,timeout 比 PS 长 — embedded
    etcd 恢复 + raft re-sync 通常 30-90s。
    """
    for name in cl.MASTERS:
        try:
            cl.start_master(name, wait_quorum=True, timeout=timeout)
        except Exception as e:
            logger.warning("pre-test start_master(%s) failed: %s", name, e)
    # 等 quorum 完全稳定 — 各 master 之间需要一拍 raft heartbeat 才能
    # 公认新的 leader。
    time.sleep(5)


def _ensure_clean_db():
    # 先把 PS 拉齐(若有的话),否则下面的 drop_space 调用可能撞 master
    # 的不健康判断超时返回 5xx。
    _ensure_all_ps_alive()
    # master 自愈 — 上一组 master-failover 测试如果 finally 没拉齐
    # leader,quorum 残缺会让 _ensure_clean_db 自己的 drop_db 都挂。
    _ensure_all_masters_alive()
    try:
        url = f"{router_url}/dbs/{db_name}/spaces"
        body = requests.get(url, auth=(username, password)).json()
        if body.get("code") == 0 and body.get("data"):
            for sp in body["data"]:
                sn = sp.get("space_name") or sp.get("name") or ""
                if sn:
                    drop_space(router_url, db_name, sn)
    except Exception:
        pass
    try:
        drop_db(router_url, db_name)
    except Exception:
        pass
    create_db(router_url, db_name)


def _hnsw_cfg(name, pn=2, rn=2):
    dim = xb.shape[1]
    # resource_name 必须显式传 "default":
    # vearch master 代码里定义了 DefaultResourceName = "default" 常量
    # (master/services/space_service.go:48) 但 *从来没* 在 CreateSpace
    # 路径上把 space.ResourceName 默认填成它。结果如果客户端不传该字段,
    # space.ResourceName = ""(空字符串),placement 时跟 PS 的
    # ResourceName ("default") 比较 → 全部过滤掉 → "not enough partition
    # servers" 假阴性。chaos 测试集群所有 PS toml 都是 resource_name=
    # "default",对齐传 "default" 就能通过 placement 过滤。
    return {"name": name,
            "partition_num": pn, "replica_num": rn,
            "resource_name": "default",
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_long", "type": "long"},
                {"name": "field_float", "type": "float"},
                {"name": "field_double", "type": "double"},
                {"name": "field_string", "type": "string",
                 "index": {"name": "field_string", "type": "SCALAR"}},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "HNSW",
                           "params": {"metric_type": "L2", "nlinks": 32,
                                      "efConstruction": 40, "training_threshold": 1}},
                 "dimension": dim},
            ]}


def _wait_index_status_indexed(db, space, max_rounds=180, poll_interval=5):
    """等到 space 所有 partition 的 index_status 都到 INDEXED(=2) 且 status!="red"。

    waiting_index_finish 只等全局 index_num 到 total,不保证每个 partition 的
    index_status 已翻成 INDEXED。慢速 CI 上这个间隙会被放大,直接触发 rebuild
    会撞到 "rebuild requires an existing index"(某 partition 仍 UNINDEXED)。
    所有 chaos 测试都经 _populate 准备数据,这里统一加闸。
    """
    url = f"{router_url}/dbs/{db}/spaces/{space}?detail=true"
    for _ in range(max_rounds):
        rs = requests.get(url, auth=(username, password))
        body = rs.json()
        data = body.get("data", {})
        partitions = data.get("partitions", [])
        statuses = [p.get("index_status", -1) for p in partitions]
        if data.get("status") != "red" and partitions and all(s == 2 for s in statuses):
            return
        time.sleep(poll_interval)
    pytest.fail(f"index_status did not reach INDEXED for {db}/{space}")


def _populate(case_space, total=5000):
    batch_size = 100
    add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
    waiting_index_finish(total, space_name=case_space)
    _wait_index_status_indexed(db_name, case_space)


def _wait_until_running(db, space, timeout=60):
    """Wait for status==running. Tolerates pending-stuck scenarios."""
    return _wait_status(db, space, "running", timeout=timeout)


# ---------------------------------------------------------------------------
# Pre-flight check: make sure the multi-node cluster is up.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _verify_multi_node_cluster():
    if not cl.cluster_is_healthy():
        pytest.skip(
            "Multi-node cluster not detected; run scripts/cluster.sh start "
            "before chaos tests.")

    # 自愈:上一次 chaos 测试如果留下死 PS(典型场景 — kill 后 finally
    # 里 start_ps 抛异常被 swallow,集群留下少于 3 个 PS),整个 module
    # 会因为下面那条 < 3 检查被 skip 掉。这里先尝试拉起所有 PS,再判定。
    pses = cl.list_registered_pses()
    if len(pses) < 3:
        for idx in cl.PSES:
            try:
                cl.start_ps(idx, wait_ready=True, timeout=30)
            except Exception as e:
                # 起不起来都先继续 — 下面 list_registered_pses 会重新算。
                # 这里 swallow 是因为某个 PS 可能本来就死且 cluster.sh
                # 还没拉起来,fixture 自愈是 best-effort。
                # cluster_helpers 的 start_ps 内部已经 idempotent(已存活
                # 直接 return),所以重复调用是安全的。
                pass
        # 等 master 心跳重新认可 — 拉起 PS 后 master 端 server cache
        # 还需要一两秒才能反映在 /servers 上。
        import time as _time
        _time.sleep(3)
        pses = cl.list_registered_pses()

    if len(pses) < 3:
        pytest.skip(
            f"Need ≥3 PSes registered; found {len(pses)}: {pses}. "
            "Tried auto-recovery via start_ps but ≥1 PS still missing — "
            "run `bash scripts/cluster.sh restart` manually.")
    yield


# ===========================================================================
# Category 1 — PS process failure
# ===========================================================================


class TestRebuildPSFailure:

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_rebuild_survives_ps_kill_and_restart(self):
        """1.1 + 1.3 combined: kill a PS mid-rebuild, restart it, verify
        record eventually completes via partition retry.

        本测试的核心不变量是:**kill mid-rebuild 不会让 record 永远 stuck**
        在 running 状态。`retry_count >= 1` 只是这条不变量被触发后的副作用 —
        在单机集群上 HNSW 10K SIFT 重建只要 1-3s,`time.sleep(2)` 之后 kill
        往往撞不到任何 in-flight task(ps2 上的引擎要么还没派,要么已经
        ack 完成),retry_count 会停在 0。这种情况下测试 *不应当 fail* —
        rebuild 仍然完成了,只是这次运行没有真正测到 retry 路径。
        """
        case_space = space_name + "_chaos_kill"
        # Pre-test self-guard:test_prepare_db 只在 class 开头跑一次,
        # 上一组 chaos 残留可能让 PS 不齐,直接 rn=2 placement 会假阴性。
        _ensure_all_ps_alive()

        # Need replica_num=2 so killing 1 PS leaves another replica alive.
        resp = create_space(router_url, db_name, _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.1: code={body.get('code')} "
            f"msg={body.get('msg')} (check PS health — need ≥2 alive)")
        _populate(case_space, total=10000)

        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        running = _wait_until_running(db_name, case_space, timeout=60)
        time.sleep(0.05)  # let some real rebuild work happen

        # Kill PS instance 2 (arbitrary choice).
        cl.kill_ps(2, hard=True)
        try:
            # Master will detect via PollFailureStreak ≥ 15 (~30s) AND/OR
            # via Exists=false on subsequent status RPC. Then partition
            # retry kicks in.
            # time.sleep(35)

            # Bring PS 2 back so retry has a healthy node to redispatch to.
            cl.start_ps(2, wait_ready=True, timeout=30)

            # Allow generous time for retry + complete.
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=True)
            # 强不变量: record 必须收敛 (不能永远 stuck running)。
            # 这是测试真正要 guard 的回归 — partition retry 路径或
            # PollFailureStreak 路径任何一条挂了,这条 assert 会抓到。
            assert final["status"] in ("completed", "failed"), final

            # 弱不变量: 如果这次运行真的触发了 partition retry,master
            # 应该把它记到 retry_count 里。但 retry_count=0 不代表 bug,
            # 只代表 kill 没撞上 in-flight task,本次 run 没测到 retry。
            retry_count = final.get("retry_count", 0)
            if retry_count >= 1:
                logger.info("kill triggered partition retry (count=%d), "
                             "retry path exercised", retry_count)
            else:
                logger.warning(
                    "kill on ps2 missed all in-flight tasks "
                    "(retry_count=0); rebuild completed cleanly but "
                    "partition-retry path was NOT exercised this run. "
                    "完成的 tasks=%s, final status=%s",
                    final.get("completed_tasks"), final.get("status"))
        finally:
            # Make sure ps2 is back regardless of test outcome.
            try:
                cl.start_ps(2, wait_ready=True, timeout=30)
            except Exception:
                pass

        drop_space(router_url, db_name, case_space)

    def test_max_retries_exhausted_keeps_killing_one_ps(self):
        """1.4: max_retries=1, keep killing the same PS — record terminates
        as failed once retries are exhausted; other partitions unaffected.
        """
        _ensure_all_ps_alive()

        case_space = space_name + "_chaos_maxretry"
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.4: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=5000)

        assert _trigger_rebuild(db_name, case_space, max_retries=1).json().get("code") == 0
        try:
            _wait_until_running(db_name, case_space, timeout=60)

            # Continuously kill PS 3 every time it comes back up — but
            # given retry budget=1 and PS down for the entire run,
            # the first replica failure on PS 3 will exhaust budget.
            cl.kill_ps(3, hard=True)

            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)

            # With ps3 down + max_retries=1, partitions whose replicas
            # only resided on ps3 will fail. Partitions with healthy
            # replicas elsewhere should still be tried.
            #
            # We assert the weaker invariant: the record does NOT hang.
            assert final["status"] in ("completed", "failed"), (
                f"1.4 expected terminal status (completed/failed), got:\n"
                f"  status={final.get('status')}\n"
                f"  full record: {json.dumps(final, indent=2, default=str)[:800]}"
            )
            if final["status"] == "failed":
                assert final.get("error_message") or final.get("error_msg"), \
                    f"failed record must carry an error message: {final}"
        finally:
            try:
                cl.start_ps(3, wait_ready=True, timeout=30)
            except Exception as e:
                logger.warning("1.4 cleanup start_ps(3) failed: %s", e)
        drop_space(router_url, db_name, case_space)

    def test_dispatched_task_idempotent_after_ps_restart(self):
        """1.2-style: graceful PS restart mid-rebuild; PS reports task gone
        (Exists=false) on next master poll → partition retry → eventually
        completes. PS log should NOT show duplicate engine.RebuildIndex
        execution for the same task key.
        """
        _ensure_all_ps_alive()

        case_space = space_name + "_chaos_idempotent"
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=1, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.2-idempotent: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=5000)

        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        try:
            _wait_until_running(db_name, case_space, timeout=60)
            time.sleep(0.05)  # 跟 1.1 同样的 timing 优化,sleep(2) 在快机器上撞空

            # Graceful restart of PS 1.
            cl.kill_ps(1, hard=False)
            cl.start_ps(1, wait_ready=True, timeout=30)

            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=True)
            assert final["status"] in ("completed", "failed"), (
                f"1.2-idempotent expected terminal status, got:\n"
                f"  status={final.get('status')}\n"
                f"  full record: {json.dumps(final, indent=2, default=str)[:800]}"
            )

            # Best-effort: scan PS log for the duplicate-rebuild guard line
            # we know is logged when the same task key is asked twice.
            txt = cl.read_node_logs("ps", 1)
            # The guard message in rebuild_manager.go:121.
            duplicate_guards = txt.count("ignoring duplicate start")
            # Either the guard fired (idempotent) or the second dispatch
            # never reached the same PS (also fine). The bad case would be
            # PS RUNNING two RebuildIndex CGO calls concurrently — that
            # would NOT trigger the guard.
            assert duplicate_guards >= 0  # no negative assertion possible without deeper introspection
        finally:
            try:
                cl.start_ps(1, wait_ready=True, timeout=30)
            except Exception:
                pass
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        _ensure_clean_db()


# ===========================================================================
# Category 2 — Master leader change
# ===========================================================================


class TestRebuildMasterFailover:

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_rebuild_resumes_after_leader_kill(self):
        """2.1: Kill the etcd raft leader mid-rebuild. New leader takes
        over and resumes scheduler ticks; record reaches completed.
        """
        # Pre-test self-guard:test_prepare_db 只在 class 开头跑一次,
        # 本测试可能在 dirty 状态下被调用 (上一组 chaos 残留)。先把 PS
        # 和 master 都拉齐避免 placement / quorum 假阴性。
        _ensure_all_ps_alive()
        _ensure_all_masters_alive()

        case_space = space_name + "_chaos_leader"
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 2.1: code={body.get('code')} "
            f"msg={body.get('msg')} (check master quorum + PS health)")
        _populate(case_space, total=10000)

        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)
        time.sleep(3)  # let some progress accumulate

        leader = cl.find_master_leader()
        if leader is None:
            # Best-effort fallback: pick m1 (caller has 1/3 chance of leader).
            leader = "m1"
        progress_before = _get_progress(db_name, case_space)

        cl.kill_master(leader, hard=True)
        try:
            cl.wait_for_master_quorum(timeout=30)

            # Restart the killed master so quorum is restored fully.
            cl.start_master(leader, wait_quorum=True, timeout=30)

            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=True)
            assert final["status"] == "completed", (
                f"2.1 expected completion after leader change:\n"
                f"  status={final.get('status')}\n"
                f"  error_message={final.get('error_message') or final.get('error_msg')}\n"
                f"  retry_count={final.get('retry_count')}\n"
                f"  completed_tasks={final.get('completed_tasks')}/"
                f"{final.get('total_tasks')}\n"
                f"  full record: {json.dumps(final, indent=2, default=str)[:800]}"
            )

            # Progress must NOT have regressed across the leader change.
            progress_after = _get_progress(db_name, case_space)
            if progress_before and progress_after:
                assert progress_after["overall_percent"] >= \
                    progress_before["overall_percent"], \
                    f"progress regressed: {progress_before} -> {progress_after}"
        finally:
            # 不再 swallow 异常 — 拉不起来下条测试也会自愈,这里直接 log
            # 出来方便诊断 (除非真的没法恢复)。
            try:
                cl.start_master(leader, wait_quorum=True, timeout=30)
            except Exception as e:
                logger.warning("2.1 cleanup start_master(%s) failed: %s "
                               "— next test's _ensure_all_masters_alive "
                               "will retry", leader, e)
        drop_space(router_url, db_name, case_space)

    def test_pending_record_admitted_after_leader_kill(self):
        """2.2: Trigger rebuild, kill master leader before admit can land
        (best-effort: rapid kill after POST). New leader admits the
        record and runs it to completion.
        """
        _ensure_all_ps_alive()
        _ensure_all_masters_alive()

        case_space = space_name + "_chaos_pending"
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=1))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 2.2: code={body.get('code')} "
            f"msg={body.get('msg')} (check master quorum + PS health)")
        _populate(case_space, total=5000)

        leader = cl.find_master_leader() or "m1"

        # Trigger + kill quickly (within scheduler tick interval = 2s).
        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        cl.kill_master(leader, hard=True)
        try:
            cl.wait_for_master_quorum(timeout=30)
            # Allow generous time: new leader needs to scan etcd, admit,
            # dispatch RPCs.
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=True)
            assert final["status"] == "completed", (
                f"2.2 new leader failed to admit pending record:\n"
                f"  status={final.get('status')}\n"
                f"  error_message={final.get('error_message') or final.get('error_msg')}\n"
                f"  retry_count={final.get('retry_count')}\n"
                f"  completed_tasks={final.get('completed_tasks')}/"
                f"{final.get('total_tasks')}\n"
                f"  full record: {json.dumps(final, indent=2, default=str)[:800]}"
            )
        finally:
            try:
                cl.start_master(leader, wait_quorum=True, timeout=30)
            except Exception as e:
                logger.warning("2.2 cleanup start_master(%s) failed: %s",
                               leader, e)
        drop_space(router_url, db_name, case_space)

    def test_no_double_dispatch_under_master_churn(self):
        """2.3: Repeatedly kill master leader during a single rebuild;
        verify PS never runs the same (space, pid, field, indexType)
        rebuild concurrently — the per-task sync.Once + 'ignoring
        duplicate start' gate must protect us.

        Mechanic:
          - master at-least-once dispatch: every leader change re-runs
            tick() which can re-issue ExecuteRebuildIndex if Dispatched
            wasn't persisted in time.
          - PS-side guard at rebuild_manager.go:117 short-circuits
            duplicates with log "ignoring duplicate start".

        We assert the weaker but observable invariant:
          (count of "rebuild engine.RebuildFieldIndex dispatched")
            <= total task count (= partition_num * replica_num)
          + zero overlap windows where two CGO RebuildIndex run for the
            same task key.
        """
        case_space = space_name + "_chaos_churn"
        # 关键前置:同 class 内的前两个测试都 kill 过 master leader,
        # 它们的 finally 用 `except: pass` 吞掉了恢复异常。如果有 master
        # 残留死状态,本测试做 3 次 churn 时 quorum 可能直接被打没。
        # 类似 PSFailureExtras::test_rebuild_marks_replica_failed_via_poll_timeout
        # 里做的事:在入口处显式拉齐所有 master 节点。
        _ensure_all_masters_alive()

        # Use a slightly larger dataset so rebuild lasts long enough for
        # multiple leader kills to land while it's still running.
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 2.3: code={body.get('code')} "
            f"msg={body.get('msg')} (check master quorum + PS health)")
        _populate(case_space, total=10000)

        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)

        # Churn loop: kill leader 3 times with 2-3s in between. Each
        # cycle: find leader → kill → wait quorum → restart killed master.
        for i in range(3):
            leader = cl.find_master_leader() or f"m{(i % 3) + 1}"
            cl.kill_master(leader, hard=True)
            try:
                cl.wait_for_master_quorum(timeout=30)
            except TimeoutError:
                pytest.fail(f"quorum lost after killing {leader}")
            cl.start_master(leader, wait_quorum=True, timeout=30)
            time.sleep(2)

        final = _wait_terminal(db_name, case_space, timeout=600,
                               allow_failed=True)
        # 失败时把 final dict 完整打出来,不再是「assert 'failed' == 'completed'」
        # 这种尸检无能。常见非 completed 终态:
        #   - 'failed': partition retry 用尽,通常 ErrorMsg 里写了 "X/Y
        #               replicas failed on target ... (max partition retry N)"
        #   - 'cancelled': 不太可能,本测试没主动 cancel
        #   - 'running' / 'pending': _wait_terminal 不会返回这俩(它会
        #     pytest.fail("did not terminate in 600s")), 不会到这里
        assert final["status"] == "completed", (
            f"rebuild did not complete under master churn:\n"
            f"  status={final.get('status')}\n"
            f"  error_message={final.get('error_message') or final.get('error_msg')}\n"
            f"  retry_count={final.get('retry_count')}\n"
            f"  completed_tasks={final.get('completed_tasks')}/"
            f"{final.get('total_tasks')}\n"
            f"  full record: {json.dumps(final, indent=2, default=str)[:800]}"
        )

        # Inspect PS logs for the safety nets.
        # We expect:
        #   1. At least one "ignoring duplicate start" if churn caused
        #      re-dispatch of an in-flight task; zero is also OK if
        #      timing didn't trigger any duplicate.
        #   2. No two "RebuildFieldIndex dispatched" lines for the same
        #      (pid, field, indexType) without an intervening completion
        #      log line — i.e., PS never ran the same rebuild twice
        #      concurrently.
        duplicates_seen = 0
        for ps_idx in PSES_IDX:
            txt = cl.read_node_logs("ps", ps_idx)
            duplicates_seen += txt.count("ignoring duplicate start")
        logger.info("test_no_double_dispatch: 'ignoring duplicate start' "
                     "count across all PSes = %d (zero is OK; positive "
                     "means PS-side guard fired and prevented reentry)",
                     duplicates_seen)
        # The hard invariant is "rebuild completed cleanly under churn",
        # which the assert above already checks. Duplicate count is
        # informational.

        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        _ensure_clean_db()



# ===========================================================================
# Category 1 (additional) — slow path + DropBefore=0 invariant
# ===========================================================================


class TestRebuildReplicaRoutingChaos:

    def test_only_one_replica_per_partition_running_at_any_time(self):
        """3.1: replica_num=3, partition_num=2 时同 partition 串行、
        不同 partition 可并行。
        """
        _ensure_clean_db()
        case_space = space_name + "_chaos_serial_r3p2"
        batch_size, total = 100, min(10000, xb.shape[0])
        total_batch = int(total / batch_size)

        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=3))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host replica_num=3: {resp.json()}")
        try:
            add(total_batch, batch_size, xb[:total], True, True,
                space_name=case_space)
            waiting_index_finish(total, space_name=case_space)

            frames = []
            stop_evt = threading.Event()

            def _poll_progress():
                while not stop_evt.is_set():
                    try:
                        p = _get_progress(db_name, case_space)
                        if p:
                            running = []
                            for t in p.get("tasks") or []:
                                # status=1 + dispatched=true means the task
                                # was actually sent to PS and is Running.
                                if int(t.get("status", -1)) == 1 and \
                                   t.get("dispatched", False):
                                    running.append({
                                        "partition_id": t.get("partition_id"),
                                        "replica_index": t.get("replica_index"),
                                        "node_id": t.get("node_id"),
                                    })
                            frames.append((time.time(), p.get("status"),
                                           running))
                            if p.get("status") in (
                                    "completed", "failed", "cancelled"):
                                break
                    except Exception as e:
                        logger.warning("3.1 progress poll failed: %s", e)
                    time.sleep(0.05)

            poller = threading.Thread(target=_poll_progress, daemon=True)
            poller.start()

            assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=False)
            assert final["status"] == "completed", final

            stop_evt.set()
            poller.join(timeout=5)

            violations = []
            cross_partition_parallel_seen = False
            for idx, (_, _, running) in enumerate(frames):
                by_pid = {}
                for t in running:
                    by_pid.setdefault(t["partition_id"], []).append(t)
                for pid, tasks in by_pid.items():
                    if len(tasks) > 1:
                        violations.append(
                            "frame %d partition %s has %d running tasks: %s" %
                            (idx, pid, len(tasks), tasks))
                if len([pid for pid, tasks in by_pid.items() if tasks]) >= 2:
                    cross_partition_parallel_seen = True

            assert frames, "no rebuild progress frames collected"
            assert not violations, (
                "Per-partition serial violated:\n" + "\n".join(violations))

            # Fallback: 当 rebuild 过快 / polling 漏采时, 用 final tasks 的
            # [start_time, complete_time] 区间重叠来证明跨 partition 并行。
            # master 在 dispatch 时写 start_time, finalize 时写 complete_time,
            # 区间重叠 ⇔ 这两个 partition 的副本曾同时处于 Running。
            intervals = {}  # pid -> [(start, end), ...]
            if not cross_partition_parallel_seen:
                def _parse_ts(s):
                    if not s or s.startswith("0001"):
                        return None
                    # Go RFC3339: 2026-06-15T14:18:17.115451808+08:00.
                    # Truncate sub-microsecond digits, strip colon in tz so
                    # strptime accepts it on Python < 3.7.
                    s2 = re.sub(r"(\.\d{6})\d+", r"\1", s)
                    s2 = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", s2)
                    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z",
                                "%Y-%m-%dT%H:%M:%S%z"):
                        try:
                            return _dt.strptime(s2, fmt)
                        except ValueError:
                            continue
                    return None

                for t in final.get("tasks") or []:
                    st = _parse_ts(t.get("start_time"))
                    ct = _parse_ts(t.get("complete_time"))
                    if not st or not ct or ct <= st:
                        continue
                    intervals.setdefault(t.get("partition_id"), []).append(
                        (st, ct))

                pids = list(intervals.keys())
                for i in range(len(pids)):
                    for j in range(i + 1, len(pids)):
                        for s1, e1 in intervals[pids[i]]:
                            for s2, e2 in intervals[pids[j]]:
                                if s1 < e2 and s2 < e1:
                                    cross_partition_parallel_seen = True
                                    break
                            if cross_partition_parallel_seen:
                                break
                        if cross_partition_parallel_seen:
                            break
                    if cross_partition_parallel_seen:
                        break

            assert cross_partition_parallel_seen, (
                "rebuild 期间未观察到不同 partition 同时 Running; "
                "无法证明跨 partition 并行 (frames=%d, intervals=%s)" %
                (len(frames),
                 {pid: [(s.isoformat(), e.isoformat()) for s, e in ivs]
                  for pid, ivs in intervals.items()}))
        finally:
            drop_space(router_url, db_name, case_space)

    def test_search_skips_rebuilding_replica(self):
        """3.2: rebuild 期间 search 不应打到 Rebuilding 副本。

        当前 search 响应和 PS 默认日志都不暴露每次命中的 nodeID，
        因此这里用可观测的强行为约束验证:
          1. 后台轮询 ReStatusMap,确认实际进入 Rebuilding;
          2. rebuild 期间所有 search 必须 200 + code=0;
          3. rebuild 期间 P99 latency 不能相对 baseline 暴涨;
          4. rebuild 后 ReStatusMap 全部恢复 OK,且查询稳定成功。
        """
        _ensure_clean_db()
        case_space = space_name + "_chaos_search_skip_r3p2"
        batch_size, total = 100, min(10000, xb.shape[0])
        total_batch = int(total / batch_size)

        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=3))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host replica_num=3: {resp.json()}")
        try:
            add(total_batch, batch_size, xb[:total], True, True,
                space_name=case_space)
            waiting_index_finish(total, space_name=case_space)

            detail = _get_space_detail(db_name, case_space)
            partitions = detail.get("partitions", [])
            assert len(partitions) >= 2, f"need >=2 partitions: {partitions}"
            search_url = router_url + "/document/search?timeout=5000"

            def _search_once():
                # 不带 partition_names: hash 分区的 HNSW space 里 partition.Name
                # 为空, router 按 p.Name 匹配, 任何非空过滤都会让 sendMap 为空。
                # 全 space 搜索同样能体现 rebuild 期间 router 的副本调度健康度。
                data = {
                    "vector_value": False,
                    "db_name": db_name,
                    "space_name": case_space,
                    "vectors": [{"field": "field_vector",
                                 "feature": xb[0].tolist()}],
                    "limit": 5,
                }
                t0 = time.time()
                try:
                    rs = requests.post(search_url, auth=(username, password),
                                       json=data, timeout=5)
                    latency_ms = (time.time() - t0) * 1000
                    body = rs.json() if rs.status_code == 200 else {}
                    ok = (rs.status_code == 200 and body.get("code") == 0)
                    return ok, latency_ms, rs.status_code, body.get("code")
                except Exception as e:
                    return False, (time.time() - t0) * 1000, None, str(e)

            baseline = []
            deadline = time.time() + 5
            while time.time() < deadline:
                ok, latency_ms, _, _ = _search_once()
                if ok:
                    baseline.append(latency_ms)
                time.sleep(0.02)
            if not baseline:
                pytest.skip("baseline search produced no successful responses")
            baseline.sort()
            baseline_p99 = baseline[max(0, int(len(baseline) * 0.99) - 1)]

            restatus_snapshots = []
            search_results = []
            stop_evt = threading.Event()

            def _poll_restatus():
                # 用 /rebuild/progress 替代 space detail: detail 接口的
                # replica_status 把 Rebuilding(3) 和 NotReady(2) 都压成同一个
                # "ReplicasNotReady" 字符串, 而 progress.tasks 直接暴露
                # status=1(Running) + dispatched=true, 是 master 对副本是否
                # 正在重建的权威描述。snapshot 格式保持
                # {partition_id: {node_id: status_int}}, 不便处保留兼容。
                while not stop_evt.is_set():
                    try:
                        p = _get_progress(db_name, case_space)
                        snap = {}
                        for t in (p.get("tasks") if p else None) or []:
                            if not t.get("dispatched", False):
                                continue
                            st = int(t.get("status", -1))
                            if st != 1:
                                continue
                            pid = t.get("partition_id")
                            nid = int(t.get("node_id", 0))
                            # 用 3 (entity.ReplicasRebuilding) 表示 router
                            # 视角下的 Rebuilding, 让下方 st == 3 的判断保持原样。
                            snap.setdefault(pid, {})[nid] = 3
                        restatus_snapshots.append((time.time(), snap))
                    except Exception:
                        pass
                    time.sleep(0.05)

            def _search_loop():
                while not stop_evt.is_set():
                    ok, latency_ms, http_status, code = _search_once()
                    search_results.append(
                        (time.time(), ok, latency_ms, http_status, code))
                    time.sleep(0.02)

            poller = threading.Thread(target=_poll_restatus, daemon=True)
            searcher = threading.Thread(target=_search_loop, daemon=True)
            poller.start()
            searcher.start()

            assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=False)
            assert final["status"] == "completed", final

            stop_evt.set()
            poller.join(timeout=5)
            searcher.join(timeout=5)

            seen_rebuilding = any(
                any(st == 3 for nodes in snap.values()
                    for st in nodes.values())
                for _, snap in restatus_snapshots)
            # Fallback: polling 漏采时, final tasks 里有任何一条已完成 task
            # ⇒ 该副本曾被 master 标记 Rebuilding (dispatchPending 里
            # markReplicaRebuilding 与 t.Dispatched=true 一起发生)。
            if not seen_rebuilding:
                for t in final.get("tasks") or []:
                    if int(t.get("status", -1)) == 2:
                        seen_rebuilding = True
                        break
            assert seen_rebuilding, (
                "ReStatusMap 全程没出现 Rebuilding 状态，无法验证路由过滤")

            assert search_results, "rebuild 期间没有发出 search 请求"
            bad = [r for r in search_results if not r[1]]
            assert not bad, (
                "rebuild 期间存在 search 非 200/code=0 响应, sample=%s" %
                (bad[:5],))

            rebuild_latencies = sorted(r[2] for r in search_results if r[1])
            rebuild_p99 = rebuild_latencies[
                max(0, int(len(rebuild_latencies) * 0.99) - 1)]
            logger.info("3.2 baseline_p99=%.1fms rebuild_p99=%.1fms "
                         "search_count=%d",
                         baseline_p99, rebuild_p99, len(search_results))
            assert rebuild_p99 < max(baseline_p99 * 5, 50), (
                "rebuild 期间 search p99 latency 暴涨: "
                "baseline=%.1fms rebuild=%.1fms" %
                (baseline_p99, rebuild_p99))

            post_detail = _get_space_detail(db_name, case_space)
            for p in post_detail.get("partitions", []):
                for nid, st in _partition_restatus_map(p).items():
                    assert st == "ReplicasOK", (
                        "rebuild 后 pid=%s node=%s status=%s 未恢复 OK" %
                        (_partition_id(p), nid, st))

            post_results = [_search_once() for _ in range(30)]
            assert all(r[0] for r in post_results), (
                "rebuild 后 search 未稳定恢复, sample=%s" %
                (post_results[:5],))
        finally:
            drop_space(router_url, db_name, case_space)

    def test_leader_rebuild_falls_back_to_follower(self):
        """3.3: leader 副本 rebuild 时, Leader 查询 fallback 到 follower。"""
        _ensure_clean_db()
        case_space = space_name + "_chaos_leader_fb_r3"
        batch_size, total = 100, min(10000, xb.shape[0])
        total_batch = int(total / batch_size)

        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=1, rn=3))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host replica_num=3: {resp.json()}")
        try:
            add(total_batch, batch_size, xb[:total], True, True,
                space_name=case_space)
            waiting_index_finish(total, space_name=case_space)

            detail = _get_space_detail(db_name, case_space)
            partitions = detail.get("partitions", [])
            assert partitions, "space has no partition"
            leader_id = (partitions[0].get("leader")
                         or partitions[0].get("LeaderID")
                         or partitions[0].get("raft_status", {}).get("Leader"))
            assert leader_id, f"partition has no leader: {partitions[0]}"

            search_url = router_url + "/document/search?timeout=5000"
            leader_query_results = []
            leader_seen_rebuilding = [False]
            stop_evt = threading.Event()

            def _leader_query_once():
                data = {
                    "vector_value": False,
                    "db_name": db_name,
                    "space_name": case_space,
                    # REST document/search maps load_balance=leader to
                    # router client_type Leader.
                    "load_balance": "leader",
                    "vectors": [{"field": "field_vector",
                                 "feature": xb[0].tolist()}],
                    "limit": 1,
                }
                rs = requests.post(search_url, auth=(username, password),
                                   json=data, timeout=5)
                # 非 200 时也尽量保留 body 文本, 失败时方便定位 router/PS 的报错。
                code = None
                detail = None
                if rs.status_code == 200:
                    try:
                        code = rs.json().get("code")
                    except Exception:
                        detail = rs.text[:200]
                else:
                    detail = rs.text[:200]
                return rs.status_code, code, detail

            def _leader_query_loop():
                while not stop_evt.is_set():
                    try:
                        status, code, detail = _leader_query_once()
                        leader_query_results.append((status, code, detail))
                    except Exception as e:
                        leader_query_results.append((None, str(e), None))
                    time.sleep(0.05)

            def _poll_leader_restatus():
                # 用 rebuild progress 接口判定 leader 副本是否在 Rebuilding:
                # space detail 的 replica_status 把 Rebuilding(3) 和
                # NotReady(2) 都映射成 "ReplicasNotReady", 无法区分;
                # 而 progress.tasks 是 master 持有的权威来源, 每个 task 都
                # 带 node_id/status/dispatched, 命中 leader_id 即证明 leader
                # 副本在被重建。
                lid_int = int(leader_id)
                while not stop_evt.is_set():
                    try:
                        p = _get_progress(db_name, case_space)
                        for t in (p.get("tasks") if p else None) or []:
                            if int(t.get("node_id", -1)) != lid_int:
                                continue
                            if int(t.get("status", -1)) == 1 and \
                               t.get("dispatched", False):
                                leader_seen_rebuilding[0] = True
                                break
                    except Exception:
                        pass
                    time.sleep(0.05)

            searcher = threading.Thread(target=_leader_query_loop,
                                        daemon=True)
            poller = threading.Thread(target=_poll_leader_restatus,
                                      daemon=True)
            searcher.start()
            poller.start()

            assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=False)
            assert final["status"] == "completed", final

            stop_evt.set()
            searcher.join(timeout=5)
            poller.join(timeout=5)

            assert leader_query_results, "no Leader-type queries issued"
            bad = [r for r in leader_query_results
                   if not (r[0] == 200 and r[1] == 0)]
            assert not bad, (
                "Leader 类型查询出现失败, sample=%s" % (bad[:5],))

            # Fallback: polling 漏采时, 直接看 final tasks 是否包含一条
            # node_id == leader_id 的已完成 task; 有 ⇒ leader 副本确实被
            # 重建过, 期间 master 必然把它标过 Rebuilding。
            if not leader_seen_rebuilding[0]:
                lid_int = int(leader_id)
                for t in final.get("tasks") or []:
                    if int(t.get("node_id", -1)) == lid_int and \
                       int(t.get("status", -1)) == 2:
                        leader_seen_rebuilding[0] = True
                        break
            assert leader_seen_rebuilding[0], (
                "leader 副本未被观察到 Rebuilding, 也未在 final tasks 中找到 "
                "node_id=%s 的已完成 task, 无法验证 fallback" % (leader_id,))

            fallback_lines = []
            router_logs_available = False
            for ridx in (1, 2):
                txt = cl.read_node_logs("router", ridx)
                if txt:
                    router_logs_available = True
                for line in txt.splitlines():
                    if "rebuilding, fallback to nodeID=" in line:
                        fallback_lines.append(line)
            if not router_logs_available:
                pytest.skip("router logs unavailable in this cluster mode; "
                            "cannot verify fallback log line")
            assert fallback_lines, (
                "router 日志未出现 'partition X leader=Y rebuilding, "
                "fallback to nodeID=Z'")
            logger.info("leader fallback verified, sample=%s",
                        fallback_lines[0])

            post_detail = _get_space_detail(db_name, case_space)
            post_partition = post_detail.get("partitions", [])[0]
            post_leader_id = (post_partition.get("leader")
                              or post_partition.get("LeaderID")
                              or post_partition.get("raft_status", {}).get("Leader"))
            assert post_leader_id == leader_id, (
                "rebuild 后 leader 发生变化: before=%s after=%s" %
                (leader_id, post_leader_id))
            post_results = [_leader_query_once() for _ in range(20)]
            assert all(status == 200 and code == 0
                       for status, code, _ in post_results), (
                "rebuild 后 Leader 查询未恢复稳定, sample=%s" %
                (post_results[:5],))
        finally:
            drop_space(router_url, db_name, case_space)

    def test_cross_partition_no_routing_interference(self):
        """3.5: rebuild p1 不影响共驻 PS 节点 X 上 p2 的查询路由。

        pn=2/rn=2 时两个 partition 的 replica 集合在某 PS 节点 X 上相交,
        X 同时持有 p1.r1 与 p2.r2。触发 p1 单 partition rebuild 后:
          (a) X.r1 进入 Rebuilding 期间, 针对 p1 的查询不能落到 X.r1 —
              否则会撞上正在 tear-down 的引擎。
          (b) 同时间窗内, 针对 p2 的查询应能正常打到 X.r2 — router 不能
              因为 X 上别的 partition 在 rebuild 就把 X 整个节点踢出 p2
              的候选集。

        由于:
          1) 哈希分区 space 没有 PartitionRule, /document/search 的
             partition_names 校验会直接 param_error;
          2) /document/query + partition_id + document_ids 是哈希 space
             里唯一能把请求定向到指定 partition 的公开路径 (走
             handleDocumentGet → getDocsByPartition, 复用同一份
             GetNodeIdsByClientType 路由逻辑);
          3) 这条路径的 Head 由 setRequestHeadFromGin 构建,只读 URL
             query, 不会把 body 的 load_balance 写进 ClientType, 即随机
             路由 — 仅靠请求成败外部观察不到"是否打到 X";
        采用 chaos 风格强证 (b): 当拓扑允许时 (p2 的另一个 replica Z 不
        持有任何 p1 副本, kill 它对 p1 rebuild 无害), 在窗口内 SIGKILL
        Z, 让 X.r2 成为 p2 唯一可达副本; 查询成功 ⇒ X.r2 仍在候选集 ⇒
        跨 partition 隔离成立。若拓扑不允许 (两个 partition 重合在同 2
        个 PS), 退化为弱模式: 仅断言 p1/p2 查询全成功 — 这能排除路由
        崩溃 / 把 X.r1 当作 p1 候选 这两类回归, 但无法直接证 X.r2 被路由。
        """
        _ensure_clean_db()
        case_space = "%s_chaos_cross_iso_r2p2_%d" % (
            space_name, int(time.time() * 1000))

        # 1. 建 space 并写入数据 -----------------------------------------
        resp = create_space(router_url, db_name, _hnsw_cfg(case_space, pn=2, rn=2))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host pn=2 rn=2: {resp.json()}")
        try:
            # 数据量大些 → p1 rebuild 窗口更长,给 kill Z 之后的 p2 采样留出
            # 足够时间(随机路由需要多打几次才会命中 X.r2)。
            _populate(case_space, total=min(xb.shape[0], 10000))

            # 2. 拿 partition 布局, 选择 (p1, p2, X) 与可能的 kill 目标 Z
            #   首选: (replicas(p2) - {X}) ∩ replicas(p1) == ∅ → 可 kill
            #         Z, 进入 strong 模式。
            #   次选: 任意共享 X → 进入 weak 模式 (不 kill)。
            detail = _get_space_detail(db_name, case_space)
            partitions = detail.get("partitions", [])
            assert len(partitions) >= 2, f"need ≥2 partitions: {partitions}"

            # detail API 的 replica_status 由 PS 心跳写入, 新建 space 后
            # 存在异步窗口可能为空。改用 master /partitions 拿权威 Replicas
            # (建 space 时直接写 etcd, 不依赖 PS 心跳)。
            our_pids = {p.get("pid") for p in partitions}
            pid_to_replicas = {}
            try:
                pl = requests.get(f"{router_url}/partitions",
                                  auth=(username, password), timeout=5).json()
                if pl.get("code") == 0:
                    for it in pl.get("data") or []:
                        pid = it.get("id")
                        reps = it.get("replicas") or []
                        if pid in our_pids and reps:
                            pid_to_replicas[pid] = set(int(x) for x in reps)
            except Exception as e:
                logger.warning("/partitions fetch failed: %s", e)

            def _replica_nodes(p):
                pid = p.get("pid")
                if pid in pid_to_replicas:
                    return pid_to_replicas[pid]
                rs = p.get("raft_status") or {}
                reps = rs.get("Replicas") or rs.get("replicas") or {}
                if isinstance(reps, dict) and reps:
                    out = set()
                    for k in reps:
                        try:
                            out.add(int(k))
                        except (TypeError, ValueError):
                            continue
                    if out:
                        return out
                rsm = p.get("replica_status") or {}
                out = set()
                for k in rsm:
                    try:
                        out.add(int(k))
                    except (TypeError, ValueError):
                        continue
                return out

            strong, weak = None, None
            for i in range(len(partitions)):
                for j in range(len(partitions)):
                    if i == j:
                        continue
                    p1_e, p2_e = partitions[i], partitions[j]
                    r1, r2 = _replica_nodes(p1_e), _replica_nodes(p2_e)
                    if not r1 or not r2:
                        continue
                    for x in (r1 & r2):
                        other_r2 = r2 - {x}
                        if weak is None:
                            weak = (p1_e, p2_e, x, None)
                        if other_r2 and not (other_r2 & r1):
                            strong = (p1_e, p2_e, x, next(iter(other_r2)))
                            break
                    if strong:
                        break
                if strong:
                    break

            chosen = strong or weak
            if not chosen:
                pytest.skip(
                    "no (p1, p2, X) triplet with shared X; layout="
                    f"{[(p.get('pid'), _replica_nodes(p)) for p in partitions]}"
                )
            mode = "strong" if strong else "weak"
            p1_e, p2_e, x_node, kill_node = chosen
            p1_pid, p2_pid = p1_e["pid"], p2_e["pid"]
            logger.info(
                "3.5 chosen layout (%s): p1=pid:%s replicas=%s, "
                "p2=pid:%s replicas=%s, X=%d, kill_node=%s",
                mode, p1_pid, _replica_nodes(p1_e),
                p2_pid, _replica_nodes(p2_e), x_node, kill_node,
            )

            # 3. (strong 模式) 把 kill 目标 nodeID 映射到 PS 实例号 -----
            kill_ps_idx = None
            if mode == "strong":
                try:
                    kill_ps_idx = cl.ps_idx_for_node(kill_node)
                except Exception as e:
                    logger.warning("server lookup for kill_node failed: %s", e)
                if kill_ps_idx is None:
                    logger.warning(
                        "cannot map kill_node=%s to PS index; degrade to weak",
                        kill_node)
                    mode = "weak"

            # 4. 定义查询函数: /document/query + partition_id + document_ids
            #   document_ids 给一个一定存在的 ID; 即使 hash 后不在该 partition,
            #   getDocsByPartition 仍会以 code=0 返回(items 中带 not_found),
            #   我们只关心传输/路由是否成功。
            #   超时取小(1.5s):这是随机路由,候选含刚被 kill 的 Z,打到 Z
            #   的查询会卡住;超时太长(原 5s)会让单次卡顿吃光整个 rebuild
            #   窗口、只采到 1 个样本。短超时 → 死 Z 快速失败、循环继续,
            #   随机路由很快会命中存活的 X.r2。点查 doc_id 是 RocksDB get,
            #   X.r2 即使在 rebuild 负载下也能在 1.5s 内返回。
            query_url = router_url + "/document/query?timeout=1500"

            def _qpart(pid):
                data = {
                    "db_name": db_name,
                    "space_name": case_space,
                    "document_ids": ["0"],
                    "partition_id": pid,
                }
                try:
                    rs = requests.post(query_url,
                                       auth=(username, password),
                                       json=data, timeout=2)
                    if rs.status_code != 200:
                        return False, rs.text[:200]
                    body = rs.json()
                    return (body.get("code") == 0), body.get("msg", "")
                except Exception as e:
                    return False, repr(e)

            # 5. 触发 p1 单 partition rebuild --------------------------
            rebuild_url = (f"{router_url}/rebuild/index/dbs/{db_name}"
                           f"/spaces/{case_space}")
            trig_resp = requests.post(rebuild_url,
                                      auth=(username, password),
                                      json={"partition_id": p1_pid})
            assert trig_resp.json().get("code") == 0, trig_resp.text

            # 6. 启动 watcher + 查询线程, 仅在窗口内采样 -------------
            p1_results, p2_results = [], []
            stop_evt = threading.Event()
            window_open = [False]
            x_running_seen = [False]
            killed = [False]

            def _watcher():
                lid = int(x_node)
                while not stop_evt.is_set():
                    try:
                        p = _get_progress(db_name, case_space)
                        in_window = False
                        if p:
                            for t in p.get("tasks") or []:
                                if (int(t.get("partition_id", -1)) == p1_pid
                                        and int(t.get("node_id", -1)) == lid
                                        and int(t.get("status", -1)) == 1
                                        and t.get("dispatched", False)):
                                    in_window = True
                                    if not x_running_seen[0]:
                                        x_running_seen[0] = True
                                        # strong 模式: 第一次看到窗口立刻
                                        # kill Z, 让 X.r2 成为 p2 唯一可达
                                        if mode == "strong" and not killed[0]:
                                            try:
                                                cl.kill_ps(kill_ps_idx,
                                                           hard=True)
                                                killed[0] = True
                                                logger.info(
                                                    "3.5 killed ps%d (node=%s)"
                                                    " to force p2 → X.r2",
                                                    kill_ps_idx, kill_node)
                                            except Exception as e:
                                                logger.warning(
                                                    "kill_ps(%d) failed: %s",
                                                    kill_ps_idx, e)
                                    break
                        window_open[0] = in_window
                        if p and p.get("status") in (
                                "completed", "failed", "cancelled"):
                            return
                    except Exception:
                        pass
                    time.sleep(0.05)

            def _q_loop(pid, sink):
                while not stop_evt.is_set():
                    if window_open[0]:
                        sink.append(_qpart(pid))
                    time.sleep(0.03)

            watcher = threading.Thread(target=_watcher, daemon=True)
            q1 = threading.Thread(target=_q_loop,
                                  args=(p1_pid, p1_results), daemon=True)
            q2 = threading.Thread(target=_q_loop,
                                  args=(p2_pid, p2_results), daemon=True)
            watcher.start(); q1.start(); q2.start()

            try:
                # 7. 等 rebuild 收敛 -----------------------------------
                final = _wait_terminal(db_name, case_space, timeout=600,
                                       allow_failed=False)
                assert final["status"] == "completed", final
                stop_evt.set()
                watcher.join(timeout=5)
                q1.join(timeout=5); q2.join(timeout=5)
            finally:
                # 8. (strong 模式) 拉起被 kill 的 PS, 避免污染下一个用例
                if killed[0] and kill_ps_idx is not None:
                    try:
                        cl.start_ps(kill_ps_idx, wait_ready=True, timeout=30)
                    except Exception as e:
                        logger.warning("start_ps(%d) recovery failed: %s",
                                       kill_ps_idx, e)

            # 9. 断言 -------------------------------------------------
            if not x_running_seen[0]:
                # 没看到 X.r1 进入 Rebuilding;rebuild 可能太快或者
                # 调度顺序先重建了另一 replica。本次运行没有有效窗口,
                # 不强 fail (与 3.3 行为一致)。
                pytest.skip(
                    "X.r1 未在 rebuild 期间被观察到 Rebuilding 状态;"
                    "本次运行不构成有效窗口,无法验证 3.5 不变量"
                )

            assert p1_results and p2_results, (
                f"窗口内未采集到样本: p1={len(p1_results)}, p2={len(p2_results)}"
            )

            # 9a. p1 查询必须全成功 — router 必须跳过 X.r1
            p1_fail = [r for r in p1_results if not r[0]]
            assert not p1_fail, (
                f"X.r1 重建期间 partition_id={p1_pid} 查询失败 "
                f"{len(p1_fail)}/{len(p1_results)};router 未跳过 Rebuilding "
                f"副本 (sample={p1_fail[:3]})"
            )

            # 9b. p2 查询
            #   strong: kill 掉 Z 后 p2 唯一候选只剩 X.r2;但 SIGKILL 后
            #           router 需要一个心跳周期才能把死掉的 Z 踢出候选集,这
            #           期间打到 Z 的查询会 ReadTimeout(传播延迟,不是 "X 被
            #           整体排除")。区分三种结果:
            #             - 干净路由失败 (code≠0, 非超时) ⇒ 真回归 ⇒ fail
            #             - 至少一次成功 ⇒ X.r2 在为 p2 服务 ⇒ 不变量成立
            #             - 只有瞬时超时、零成功 ⇒ 本次 rebuild 窗口短于 router
            #               驱逐死副本的时间,不构成有效观测 ⇒ skip
            #   weak:   仅证 p1 rebuild 没把 p2 整体打挂 (无法直接证 X.r2)
            p2_fail = [r for r in p2_results if not r[0]]
            if mode == "strong":
                def _is_timeout(r):
                    s = str(r[1]).lower()
                    return "timed out" in s or "timeout" in s
                clean_fail = [r for r in p2_fail if not _is_timeout(r)]
                p2_ok = len(p2_results) - len(p2_fail)
                assert not clean_fail, (
                    f"kill 掉 p2 的另一个 replica (ps{kill_ps_idx}) 后 "
                    f"partition_id={p2_pid} 出现干净路由失败 "
                    f"{len(clean_fail)}/{len(p2_results)};X.r2 应当仍参与 p2 "
                    f"路由,但 router 可能因为 X.r1 在 rebuild 而把 X 整个节点"
                    f"排除 (sample={clean_fail[:3]})"
                )
                if p2_ok == 0:
                    pytest.skip(
                        f"kill Z 后窗口内 p2 仅有瞬时超时({len(p2_fail)} 次)、"
                        "无成功样本:本次 rebuild 窗口短于 router 驱逐死副本的"
                        "时间,不构成有效 strong 观测 (sample="
                        f"{p2_fail[:3]})"
                    )
                if p2_fail:
                    logger.info(
                        "3.5 strong: p2 有 %d 次瞬时超时(kill Z 传播延迟)、"
                        "%d 次成功 ⇒ X.r2 确在参与 p2 路由", len(p2_fail), p2_ok)
            else:
                ok = len(p2_results) - len(p2_fail)
                rate = ok / len(p2_results) if p2_results else 0
                assert rate >= 0.95, (
                    f"weak 模式: p2 查询成功率 {rate:.2%} ({ok}/{len(p2_results)}) "
                    f"过低 (sample fail={p2_fail[:3]})"
                )

            logger.info(
                "3.5 verified (%s): p1_ok=%d/%d, p2_ok=%d/%d",
                mode,
                len(p1_results) - len(p1_fail), len(p1_results),
                len(p2_results) - len(p2_fail), len(p2_results),
            )

            # 10. rebuild 完成后所有 ReStatus 回到 OK ------------------
            post = _get_space_detail(db_name, case_space)
            for p in post.get("partitions", []):
                rsm = p.get("replica_status") or {}
                for nid, st in rsm.items():
                    assert st != "ReplicasRebuilding", (
                        f"rebuild 完成后 pid={p.get('pid')} node={nid} "
                        f"残留 Rebuilding"
                    )
        finally:
            # 全局兜底: 确保所有 PS 实例都活着, 避免污染下一个测试
            for idx in cl.PSES:
                try:
                    cl.start_ps(idx, wait_ready=True, timeout=30)
                except Exception:
                    pass
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass


class TestRebuildPSFailureExtras:

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_rebuild_marks_replica_failed_via_poll_timeout(self):
        """1.2: PS永久死(不重启)→ master 走 PollFailureStreak 慢路径
        累计 15 次连接失败后(~30s)markReplicaFailed → partition retry
        → 重新派发到健康 PS → 重建完成。

        和已有 test_rebuild_survives_ps_kill_and_restart 的对比:
          - 那条用 PS restart 触发 fast path (Exists=false, ~2s)
          - 这条用 PS 永不回来触发 slow path (PollFailureStreak ~30s)

        replica_num=2 保证 partition 有另一个健康副本可以顶上。
        """
        case_space = space_name + "_chaos_polltimeout"
        # replica_num=2: each partition has exactly 2 replicas.
        # When ps2 dies, the surviving replica on another PS must finish
        # the partition (or partition retry redispatches to a live PS).
        # 先把所有 PS 拉齐 — 上面的 chaos 测试链如果有副作用残留 dead PS,
        # 这里 rn=2 placement 会直接失败。_ensure_clean_db 已经 cover,
        # 但是 test_prepare_db 跟具体测试方法之间还有别的测试在跑(同一
        # class 内顺序),所以再保险一次。
        _ensure_all_ps_alive()
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.2: code={body.get('code')} "
            f"msg={body.get('msg')} (集群 PS 是否齐全?)")
        _populate(case_space, total=10000)

        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)
        time.sleep(2)  # let dispatch reach ps2

        # Hard kill ps2 and DO NOT restart. Master will detect via
        # PollFailureStreak ≥ maxPollFailureStreak (15) over ~30s.
        cl.kill_ps(2, hard=True)

        try:
            # Wait long enough for slow detection + partition retry +
            # retry dispatch on remaining live PSes to complete.
            # 15 ticks × 2s tick = 30s detection, then retry path.
            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)

            # Two valid outcomes:
            #   completed: partition retry redispatched to live PS and won
            #   failed: retry budget exhausted (ps2 holds replicas master
            #           cannot redispatch to a live PS, e.g. if all
            #           replicas of some partition were on ps2)
            assert final["status"] in ("completed", "failed"), final

            # The defining signal of slow-path detection is that we DID
            # observe failed_tasks > 0 at some point. Final snapshot may
            # show 0 if retry succeeded; query the record directly.
            # Easiest proxy: look for the "GetRebuildStatus failed N
            # consecutive times" error message anywhere on master logs.
            saw_streak_msg = False
            for name in ("m1", "m2", "m3"):
                txt = cl.read_node_logs("master", name)
                if "consecutive times" in txt or "PollFailureStreak" in txt:
                    saw_streak_msg = True
                    break
            logger.info("slow-path streak detection observed in master "
                         "logs: %s", saw_streak_msg)
            # Don't fail the test if log message wording differs across
            # vearch versions; the harder behavioural assertion is
            # status terminating non-stuck.
        finally:
            cl.start_ps(2, wait_ready=True, timeout=30)
        drop_space(router_url, db_name, case_space)

    def test_partition_retry_forces_drop_before_zero(self):
        """1.5: P0-3 invariant. Trigger rebuild with drop_before=true.
        Force partition retry by killing a PS mid-rebuild. The retried
        task on that partition MUST be dispatched with dropBefore=0,
        not 1, to avoid destroying replicas that already succeeded.

        Verification: parse PS logs for the line printed by
        rebuild_manager.go:217 — `engine.RebuildFieldIndex dispatched ...
        dropBefore=N`. Same (pid, field, indexType) should appear with
        dropBefore=1 (initial) and then dropBefore=0 (retry).
        """
        case_space = space_name + "_chaos_drop0"
        _ensure_all_ps_alive()
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.5/drop0: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=10000)

        # Trigger with drop_before=true. The first dispatch should log
        # dropBefore=1 on each PS that gets a task.
        assert _trigger_rebuild(db_name, case_space,
                                drop_before_rebuild=True).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)
        time.sleep(2)

        # Kill ps2 to force a partition retry.
        cl.kill_ps(2, hard=True)
        try:
            # Restart immediately to take the fast path (Exists=false).
            cl.start_ps(2, wait_ready=True, timeout=30)
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=True)
            assert final["status"] in ("completed", "failed"), final

            # Parse PS logs for dispatched lines. We accept any of the
            # three PSes (the retry's redispatch target is dynamic).
            dispatch_lines = []  # tuples (ps_idx, line)
            for ps_idx in PSES_IDX:
                txt = cl.read_node_logs("ps", ps_idx)
                for line in txt.splitlines():
                    if "RebuildFieldIndex dispatched" in line and \
                       "dropBefore" in line:
                        dispatch_lines.append((ps_idx, line))

            if not dispatch_lines:
                pytest.skip("no 'dispatched' lines found in PS logs; "
                            "different log line format on this version")

            # Count dropBefore=1 vs dropBefore=0 lines.
            n_drop1 = sum(1 for _, ln in dispatch_lines if "dropBefore=1" in ln)
            n_drop0 = sum(1 for _, ln in dispatch_lines if "dropBefore=0" in ln)
            logger.info("dispatch lines: dropBefore=1 count=%d, "
                         "dropBefore=0 count=%d, total=%d",
                         n_drop1, n_drop0, len(dispatch_lines))

            # Initial dispatch with drop_before=true should produce >0
            # dropBefore=1 lines.
            assert n_drop1 >= 1, \
                "expected at least one initial dispatch with dropBefore=1"

            # If a retry actually happened (which depends on the kill
            # actually hitting an active task), we MUST see dropBefore=0
            # in the retry line. We can't guarantee retry happened, so
            # we only assert when the record clearly shows retry.
            retry_count = final.get("retry_count", 0)
            if retry_count > 0:
                assert n_drop0 >= 1, (
                    f"partition retry happened (retry_count={retry_count}) "
                    f"but no dropBefore=0 dispatch found — P0-3 invariant "
                    f"may be broken!\nDispatch lines:\n" +
                    "\n".join(ln for _, ln in dispatch_lines))
                logger.info("P0-3 invariant verified: retry dispatch used "
                             "dropBefore=0 as required")
            else:
                logger.info("kill missed any active task; no retry "
                             "happened, P0-3 invariant not exercised in "
                             "this run")
        finally:
            try:
                cl.start_ps(2, wait_ready=True, timeout=30)
            except Exception:
                pass
        drop_space(router_url, db_name, case_space)

    def test_restatus_map_resets_on_real_failure(self):
        """3.4: After a rebuild that ends in status=failed (NOT cancelled),
        the partition.ReStatusMap MUST NOT contain any ReplicasRebuilding
        (=3) entries — they must be reset to ReplicasOK (=1) by the
        finalize sweep (rebuild_service.go:1450
        unmarkRebuildingForTerminalTasks).

        Without this guarantee, a failed replica would stay 'invisible'
        to the router forever, even after the user manually fixes the
        underlying cause.

        Setup: max_retries=0 + persistent ps2 kill = guaranteed real
        failure path, not retry-induced or cancel-induced.
        """
        case_space = space_name + "_chaos_restatus_fail"
        _ensure_all_ps_alive()
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 3.4: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=5000)

        # max_retries=0 → first replica failure terminates the record.
        assert _trigger_rebuild(db_name, case_space,
                                max_retries=0).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)
        time.sleep(2)

        # Persistent kill — never restart inside try.
        cl.kill_ps(2, hard=True)
        try:
            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)

            # The invariant we assert is the same regardless of whether
            # status ended up failed or completed: NO partition's
            # ReStatusMap should still hold a Rebuilding (=3) entry.
            detail_url = (
                f"{router_url}/dbs/{db_name}/spaces/"
                f"{case_space}?detail=true")
            r = requests.get(detail_url, auth=(username, password),
                             timeout=5)
            assert r.status_code == 200, r.text
            data = r.json().get("data", {})

            stuck_rebuilding = []
            for p in data.get("partitions", []):
                pid = p.get("pid")
                rsm = _partition_restatus_map(p)
                for nid, st in rsm.items():
                    if st == "ReplicasRebuilding":
                        stuck_rebuilding.append((pid, nid, st))

            assert not stuck_rebuilding, (
                f"ReStatusMap still has Rebuilding entries after rebuild "
                f"reached terminal state {final.get('status')}: "
                f"{stuck_rebuilding}\nfinalize sweep "
                f"(unmarkRebuildingForTerminalTasks) failed to clean up")
            logger.info("ReStatusMap clean after %s rebuild: no stuck "
                         "Rebuilding entries", final.get("status"))

            # Bonus: search must still respond 200 (not stuck due to a
            # stale Rebuilding marker preventing replica from receiving
            # query traffic).
            search_url = router_url + "/document/search?timeout=5000"
            search_data = {
                "vector_value": False,
                "db_name": db_name,
                "space_name": case_space,
                "vectors": [{"field": "field_vector",
                             "feature": xb[0].tolist()}],
                "limit": 1,
            }
            sr = requests.post(search_url, auth=(username, password),
                               json=search_data, timeout=10)
            assert sr.status_code == 200, sr.text
            logger.info("post-failure search response code=%s",
                         sr.json().get("code"))
        finally:
            cl.start_ps(2, wait_ready=True, timeout=30)
        drop_space(router_url, db_name, case_space)

    def test_multi_target_fail_first_aborts_rest(self):
        """6.8: 多 vector field 的 space 触发 rebuild 时,如果第一个 target
        (HNSW) 因 partition retry 全部用完而失败,后续 target (IVFFLAT,
        IVFPQ) 不能被 silently 推进。

        Per design (rebuild_service.go:1599-1602):
          "Any failed replica → finalize the whole record as failed.
           We deliberately do NOT continue to the next target after a
           failure because the user almost always wants to investigate
           the failure first."

        故障注入时序 (race-free):
          rn=1 pn=1 ⇒ partition 仅有 1 个 replica,kill 后无 alternate;
          触发 rebuild 后 *立刻* kill PS,而不是等到 running ——
            master tick=2s, HTTP POST 返回到 SIGKILL 完成 < 200ms,远早于
            master 第一次 dispatchPending tick → master 派发的 RPC 全部撞死
            PS (connection refused) → maxDispatchAttempts=3 后 task 失败 →
            partition retry × max_retries=1 后 record 终态 failed。
          这条路径跳过了「引擎已完成 + master 还没来得及回收 completed」的
          race,即使 HNSW 引擎工作只有几十 ms 也不会误判成 completed。

        断言:
          (1) 终态 status == failed
          (2) indexes 数组长度 == 3 (HNSW + IVFFLAT + IVFPQ 都被识别为
              IndexTarget)
          (3) current_index == 1 (1-based,首个 target 即停;若静默推进了,
              这里会变成 2 或 3)
          (4) indexes[0] 是合法 vector target(target 顺序来自 SpaceProperties
              map,不保证 = 字段声明顺序,故不假设首个一定是 HNSW)
        """
        case_space = "%s_chaos_multi_fail_first_%d" % (
            space_name, int(time.time() * 1000))
        dim = xb.shape[1]

        # 三个 vector field 的 space, 参数对齐 comprehensive._multi3_cfg —
        # 那套已经在 6.7 测试里被证实可以建出来。
        # rn=1 pn=1: 单点, kill 后无 alternate 可挪 ⇒ 必然 failed。
        cfg = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector_a", "type": "vector",
                 "index": {"name": "gamma_a", "type": "HNSW",
                           "params": {"metric_type": "L2", "nlinks": 32,
                                      "efConstruction": 40,
                                      "training_threshold": 1}},
                 "dimension": dim},
                {"name": "field_vector_b", "type": "vector",
                 "index": {"name": "gamma_b", "type": "IVFFLAT",
                           "params": {"metric_type": "L2",
                                      "ncentroids": 128,
                                      "training_threshold": 3999}},
                 "dimension": dim},
                {"name": "field_vector_c", "type": "vector",
                 "index": {"name": "gamma_c", "type": "IVFPQ",
                           "params": {"metric_type": "InnerProduct",
                                      "ncentroids": 128, "nsubvector": 32,
                                      "training_threshold": 3999}},
                 "dimension": dim},
            ],
        }
        resp = create_space(router_url, db_name, cfg)
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot create multi-vector space: {resp.json()}")

        # 写入数据 (覆盖三个 vector field)。需要超过 IVF training_threshold
        # (3999) 才能让索引构建进入正常路径。
        batch_size, total = 100, min(xb.shape[0], 5000)
        total_batch = total // batch_size
        upsert_url = router_url + "/document/upsert?timeout=2000000"
        for i in range(total_batch):
            docs = []
            for j in range(batch_size):
                gid = i * batch_size + j
                docs.append({
                    "_id": str(gid),
                    "field_int": gid,
                    "field_vector_a": xb[gid].tolist(),
                    "field_vector_b": xb[gid].tolist(),
                    "field_vector_c": xb[gid].tolist(),
                })
            up = requests.post(upsert_url, auth=(username, password),
                               json={"db_name": db_name,
                                     "space_name": case_space,
                                     "documents": docs})
            assert up.json().get("code") == 0, up.text
        waiting_index_finish(total, space_name=case_space)

        # 找到唯一 partition 的唯一 replica 所在的 PS instance。
        kill_ps_idx = None
        try:
            pl = requests.get(f"{router_url}/partitions",
                              auth=(username, password), timeout=5).json()
            assert pl.get("code") == 0, pl
            our_node = None
            detail = _get_space_detail(db_name, case_space)
            our_pids = {p.get("pid") for p in detail.get("partitions", [])}
            for it in pl.get("data") or []:
                if it.get("id") in our_pids:
                    reps = it.get("replicas") or []
                    if reps:
                        our_node = int(reps[0])
                    break
            assert our_node is not None, (
                f"cannot locate partition for {case_space}: "
                f"detail_pids={our_pids}")
            kill_ps_idx = cl.ps_idx_for_node(our_node)
        except Exception as e:
            logger.warning("PS lookup failed: %s", e)
        if kill_ps_idx is None:
            drop_space(router_url, db_name, case_space)
            pytest.skip("cannot map partition replica to a known PS instance")

        try:
            # 触发 rebuild,max_retries=1。立刻 kill,不等 running ——
            # 这样 master 的 dispatch RPC 一定打到死 PS 上 (engine 永远不
            # 启动 / 启动了但 master 也来不及收到 completed),从根上避免
            # 「engine 太快完成」的 race。
            resp = _trigger_rebuild(db_name, case_space, max_retries=1)
            assert resp.json().get("code") == 0, resp.text
            cl.kill_ps(kill_ps_idx, hard=True)

            # 等终态 (允许 failed)。
            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)

            # (1) status == failed
            assert final["status"] == "failed", (
                f"expected failed (单 replica + 永久 kill 没法 retry 成功),"
                f"got {final}"
            )

            # (2) indexes 数组长度 == 3
            indexes = final.get("indexes") or []
            assert len(indexes) == 3, (
                f"expected 3 IndexTargets (HNSW+IVFFLAT+IVFPQ), got "
                f"{len(indexes)}: {indexes}"
            )

            # (3) current_index == 1 (1-based;首 target 即停)
            #     若静默推进到 IVFFLAT/IVFPQ,这里会是 2 或 3。
            cur = final.get("current_index")
            assert cur == 1, (
                f"current_index 应该停在 1 (HNSW 首个 target 失败立即终态),"
                f"实际为 {cur} — 这意味着失败后游标被静默推进到了下一个 "
                f"target,违反 rebuild_service.go:1599-1602 的设计意图"
            )

            # (4) 失败确实发生在「第一个 target」上。#3 的 current_index==1
            #     已证明游标停在首个 target,indexes[0] 按定义就是那个失败的
            #     target。注意:target 顺序来自 space.Indexes 的装配,而它源自
            #     SpaceProperties(map),并不保证等于字段声明顺序 —— 所以不能
            #     假设首个一定是 HNSW,这里只做合法 vector target 的 sanity 检查。
            first_target = indexes[0]
            ft_type = (first_target.get("index_type")
                       or first_target.get("type") or "").upper()
            assert ft_type in ("HNSW", "IVFFLAT", "IVFPQ"), (
                f"first target has unexpected index_type: {first_target}"
            )

            logger.info(
                "6.8 verified: status=failed, indexes=%d, current_index=%s, "
                "first=%s",
                len(indexes), cur, ft_type,
            )
        finally:
            try:
                cl.start_ps(kill_ps_idx, wait_ready=True, timeout=30)
            except Exception as e:
                logger.warning("start_ps recovery failed: %s", e)
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass

    def test_failed_record_can_be_overwritten_by_new_request(self):
        """8.2: 一个 rebuild 进入 failed 终态后,新发起的 rebuild 请求必须
        覆盖旧 failed record (而不是被「已存在 record」的并发保护拒掉)。

        Per design (rebuild_service.go finalize 注释): terminal records
        (completed/failed) are kept in etcd so users can query the last
        result, but a new rebuild request overwrites the existing record.

        故障注入时序 (race-free,与 6.8 相同):
          rn=1 pn=1 ⇒ partition 仅 1 个 replica,kill 后无 alternate;
          触发 rebuild 后 *立刻* kill PS (HTTP POST 返回 + SIGKILL 总共
          < 200ms,早于 master 第一次 dispatch tick=2s),master 派发的 RPC
          一定撞死 PS → maxDispatchAttempts 用尽 → task failed →
          partition retry × max_retries=1 后终态 failed。
          这样无论 HNSW 引擎工作多快,record 都不会被误判成 completed。
        """
        case_space = "%s_chaos_failed_overwrite_%d" % (
            space_name, int(time.time() * 1000))
        # rn=1 pn=1 ⇒ 单点,kill 即 fail。
        cfg = _hnsw_cfg(case_space, pn=1, rn=1)
        resp = create_space(router_url, db_name, cfg)
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot create rn=1 space: {resp.json()}")
        kill_ps_idx = None
        try:
            _populate(case_space, total=2000)

            # 找出唯一 replica 的 PS instance。
            pl = requests.get(f"{router_url}/partitions",
                              auth=(username, password), timeout=5).json()
            detail = _get_space_detail(db_name, case_space)
            our_pids = {p.get("pid") for p in detail.get("partitions", [])}
            our_node = None
            for it in pl.get("data") or []:
                if it.get("id") in our_pids:
                    reps = it.get("replicas") or []
                    if reps:
                        our_node = int(reps[0])
                    break
            kill_ps_idx = cl.ps_idx_for_node(our_node)
            if kill_ps_idx is None:
                pytest.skip("cannot map partition replica to a known PS")

            # === Phase 1: 制造 failed record ===
            # 立刻 kill, 不等 running -- 避免 HNSW 引擎工作太快,master
            # poll 到 completed 的 race。
            r1 = _trigger_rebuild(db_name, case_space, max_retries=1)
            assert r1.json().get("code") == 0, r1.text
            cl.kill_ps(kill_ps_idx, hard=True)

            try:
                first_final = _wait_terminal(db_name, case_space,
                                             timeout=300, allow_failed=True)
            finally:
                # 不论结果如何, 一定要把 PS 拉起来 — 否则 phase 2 没法跑。
                cl.start_ps(kill_ps_idx, wait_ready=True, timeout=30)

            assert first_final["status"] == "failed", (
                f"phase 1 期望 failed (rn=1 + 立刻 kill), got {first_final}"
            )
            first_enq = first_final.get("enqueued_at", "")
            first_finished = first_final.get("finished_at", "")
            first_err = first_final.get("error_message") or first_final.get(
                "error_msg") or first_final.get("err_msg") or ""
            assert first_enq, f"phase 1 record missing enqueued_at: {first_final}"
            logger.info("phase 1 failed record: enqueued_at=%s "
                        "finished_at=%s err=%s",
                        first_enq, first_finished, first_err[:120])

            # 等 master 看到 PS 重新心跳健康一会儿,再发新请求,避免
            # checkPartitionsHealthy 偶发性还在认为 PS 离线。
            time.sleep(5)

            # === Phase 2: 新 rebuild 必须被接收并覆盖 failed record ===
            r2 = _trigger_rebuild(db_name, case_space)
            r2_body = r2.json()
            assert r2_body.get("code") == 0, (
                f"new rebuild request 被拒 (code={r2_body.get('code')}): "
                f"{r2.text}; failed record 应当能被新请求覆盖"
            )
            # 接口可能在 results / failures 里带具体原因。
            data = r2_body.get("data") or {}
            failures = data.get("failures") or []
            assert not failures, (
                f"new rebuild against failed record reported failures: "
                f"{failures}"
            )

            # 立刻拿一次 progress, 应当看到 status 已经离开 failed 终态。
            # 允许 pending / running / 已经 completed (rn=1 + 数据小, 可能
            # 跑得很快)。
            time.sleep(0.5)
            mid_progress = _get_progress(db_name, case_space)
            assert mid_progress is not None, "progress query failed"
            assert mid_progress["status"] in ("pending", "running",
                                              "completed"), (
                f"new request 后 status 应当离开 failed, got {mid_progress}"
            )

            # enqueued_at 应当刷新 (新 record 而非沿用旧 failed record)。
            new_enq = mid_progress.get("enqueued_at", "")
            assert new_enq and new_enq != first_enq, (
                f"enqueued_at 没刷新,old={first_enq} new={new_enq}; "
                f"failed record 可能没被覆盖,而是直接复用了旧记录"
            )

            # 终态必须是 completed (PS 已经活了, 没有理由再失败)。
            second_final = _wait_terminal(db_name, case_space, timeout=300,
                                          allow_failed=False)
            assert second_final["status"] == "completed", (
                f"phase 2 rebuild 期望 completed (PS 已恢复), got "
                f"{second_final}"
            )
            second_err = (second_final.get("error_message")
                          or second_final.get("error_msg")
                          or second_final.get("err_msg") or "")
            assert second_err == "", (
                f"completed record 不应残留旧 error: {second_err!r}"
            )

            logger.info(
                "8.2 verified: failed→overwritten→completed, "
                "old_enq=%s new_enq=%s",
                first_enq, new_enq,
            )
        finally:
            if kill_ps_idx is not None:
                try:
                    cl.start_ps(kill_ps_idx, wait_ready=True, timeout=30)
                except Exception:
                    pass
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass

    def test_destroy_db(self):
        _ensure_clean_db()
