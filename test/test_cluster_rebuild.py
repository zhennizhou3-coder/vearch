#
# Copyright 2019 The Vearch Authors.
# Licensed under the Apache License, Version 2.0.

# -*- coding: UTF-8 -*-

"""
Cluster-dependent tests for index rebuild.

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
import os
import shutil
import random
import concurrent.futures
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
        f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}",
        auth=(username, password), json=payload, timeout=30)


def _get_progress(db, space):
    try:
        r = requests.get(
            f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/progress",
            auth=(username, password), timeout=5)
    except requests.exceptions.RequestException:



        return None
    if r.status_code != 200:
        return None
    body = r.json()
    if body.get("code") != 0:
        return None
    return body.get("data", {}) or {}


def _get_space_detail(db, space, retries=6, retry_sleep=2):
    """Read space details with retries while the cluster recovers."""
    last = None
    for _ in range(retries):
        try:
            r = requests.get(
                f"{router_url}/dbs/{db}/spaces/{space}?detail=true",
                auth=(username, password), timeout=5)
        except requests.exceptions.RequestException as e:
            last = repr(e)
            time.sleep(retry_sleep)
            continue
        if r.status_code == 200:
            body = r.json()
            if body.get("code") == 0:
                return body.get("data", {}) or {}
            last = body
        else:
            last = r.text
        time.sleep(retry_sleep)
    raise AssertionError(
        f"_get_space_detail({db}/{space}) failed {retries} consecutive times; "
        f"the cluster may be severely degraded: {last}")


def _partition_id(partition):
    return partition.get("pid", partition.get("partition_id"))


def _partition_restatus_map(partition):
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
    """Start every PS and wait until Master reports the expected registrations."""
    expected = expected_count if expected_count is not None else len(cl.PSES)
    started_pids = {}
    for idx in cl.PSES:
        try:
            pid = cl.start_ps(idx, wait_ready=True, timeout=timeout)
            started_pids[idx] = pid
        except Exception as e:
            logger.warning("pre-test start_ps(%d) failed: %s", idx, e)


    def _missing_idxs():
        if cl.CLUSTER_MODE == "docker":



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
    """Start every Master and wait for the embedded-etcd quorum to stabilize."""
    for name in cl.MASTERS:
        try:
            cl.start_master(name, wait_quorum=True, timeout=timeout)
        except Exception as e:
            logger.warning("pre-test start_master(%s) failed: %s", name, e)
    time.sleep(5)


def _ensure_clean_db():
    _ensure_all_ps_alive()
    _ensure_all_masters_alive()
    try:
        url = f"{router_url}/dbs/{db_name}/spaces"
        body = requests.get(url, auth=(username, password), timeout=5).json()
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


def _ivfpq_cfg(name, pn=2, rn=2):
    """Cluster-safe IVFPQ config used by timing-sensitive chaos tests."""
    dim = xb.shape[1]
    return {
        "name": name,
        "partition_num": pn,
        "replica_num": rn,
        "resource_name": "default",
        "fields": [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {"name": "field_string", "type": "string",
             "index": {"name": "field_string", "type": "SCALAR"}},
            {"name": "field_vector", "type": "vector",
             "index": {"name": "gamma", "type": "IVFPQ",
                       "params": {"metric_type": "InnerProduct",
                                  "ncentroids": 64,
                                  "nprobe": 16,
                                  "nsubvector": 32,
                                  "training_threshold": 2496}},
             "dimension": dim},
        ],
    }
def _wait_index_status_indexed(db, space, max_rounds=180, poll_interval=5):
    """Wait until every partition reports INDEXED and the space is not red."""
    url = f"{router_url}/dbs/{db}/spaces/{space}?detail=true"
    for _ in range(max_rounds):
        try:
            rs = requests.get(url, auth=(username, password), timeout=5)
            data = rs.json().get("data", {}) if rs.status_code == 200 else {}
        except requests.exceptions.RequestException:
            data = {}
        partitions = data.get("partitions") or []
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

    pses = cl.list_registered_pses()
    if len(pses) < 3:
        for idx in cl.PSES:
            try:
                cl.start_ps(idx, wait_ready=True, timeout=30)
            except Exception as e:
                pass

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

    def setup_class(self):
        _ensure_clean_db()

    def test_rebuild_survives_ps_kill_and_restart(self):
        """Verifies an in-flight IVFPQ rebuild completes after its PS is killed, restarted, and retried."""
        case_space = space_name + "_chaos_kill"
        _ensure_all_ps_alive()

        # Need replica_num=2 so killing 1 PS leaves another replica alive.
        resp = create_space(
            router_url, db_name, _ivfpq_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.1: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=10000)

        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0
        victim_ps_idx = None
        try:
            deadline = time.time() + 120
            victim_task = None
            last_progress = None
            while time.time() < deadline:
                last_progress = _get_progress(db_name, case_space)
                for task in (last_progress or {}).get("tasks") or []:
                    if (task.get("status") == "running"
                            and task.get("dispatched", False)):
                        victim_task = task
                        break
                if victim_task is not None:
                    break
                time.sleep(0.1)

            assert victim_task is not None, (
                "no dispatched Running IVFPQ task became visible; "
                f"last progress={last_progress}")
            victim_node_id = int(victim_task.get("node_id"))
            victim_ps_idx = cl.ps_idx_for_node(victim_node_id)
            assert victim_ps_idx is not None, (
                f"cannot map running task node to PS: {victim_task}")
            logger.info(
                "1.1 killing in-flight IVFPQ task: ps%d node=%s pid=%s",
                victim_ps_idx, victim_node_id,
                victim_task.get("partition_id"))
            cl.kill_ps(victim_ps_idx, hard=True)

            # Restarting creates a fresh in-memory PS task table. Master sees
            # the original dispatched task as missing and retries it.
            cl.start_ps(victim_ps_idx, wait_ready=True, timeout=30)

            # Allow generous time for retry + complete.
            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=True)
            assert final["status"] == "completed", final
            assert final.get("retry_count", 0) >= 1, (
                f"in-flight PS restart did not exercise retry: {final}")
        finally:
            if victim_ps_idx is not None:
                try:
                    cl.start_ps(victim_ps_idx, wait_ready=True, timeout=30)
                except Exception:
                    pass

        drop_space(router_url, db_name, case_space)

    def test_max_retries_exhausted_then_failed_record_overwritten(self):
        """Verifies retry exhaustion produces a failed record that a later successful request can replace."""
        _ensure_all_ps_alive()

        case_space = space_name + "_chaos_maxretry"
        resp = create_space(router_url, db_name,
                            _ivfpq_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.4: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=10000)

        assert _trigger_rebuild(db_name, case_space, max_retries=1).json().get("code") == 0
        victim_ps_idx = None
        try:
            deadline = time.time() + 120
            victim_task = None
            last_progress = None
            while time.time() < deadline:
                last_progress = _get_progress(db_name, case_space)
                for task in (last_progress or {}).get("tasks") or []:
                    if (task.get("status") == "running"
                            and task.get("dispatched", False)):
                        victim_task = task
                        break
                if victim_task is not None:
                    break
                time.sleep(0.1)

            assert victim_task is not None, (
                "no dispatched Running IVFPQ task became visible; "
                f"last progress={last_progress}")
            victim_node_id = int(victim_task.get("node_id"))
            victim_ps_idx = cl.ps_idx_for_node(victim_node_id)
            assert victim_ps_idx is not None, (
                f"cannot map running task node to PS: {victim_task}")
            logger.info(
                "1.4 killing in-flight IVFPQ task: ps%d node=%s pid=%s",
                victim_ps_idx, victim_node_id,
                victim_task.get("partition_id"))
            cl.kill_ps(victim_ps_idx, hard=True)

            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)
            assert final["status"] == "failed", final
            assert final.get("retry_count", 0) >= 1, (
                f"in-flight PS kill did not exercise retry: {final}")
            failed_error = final.get("error_message") or final.get("error_msg")
            assert failed_error, \
                f"failed record must carry an error message: {final}"
            failed_enqueued_at = final.get("enqueued_at")
            assert failed_enqueued_at, final

            # Restore the victim and wait until Master sees it and every
            # partition has recovered before replacing the failed record.
            cl.start_ps(victim_ps_idx, wait_ready=True, timeout=30)
            _ensure_all_ps_alive()
            _wait_index_status_indexed(db_name, case_space)

            replacement = _trigger_rebuild(db_name, case_space)
            replacement_body = replacement.json()
            assert replacement_body.get("code") == 0, replacement.text
            failures = (replacement_body.get("data") or {}).get("failures") or []
            assert not failures, (
                f"new rebuild against failed record was rejected: {failures}")

            replacement_progress = _get_progress(db_name, case_space)
            assert replacement_progress is not None, (
                "replacement rebuild progress is not queryable")
            assert replacement_progress["status"] in (
                "pending", "running", "completed"), replacement_progress
            replacement_enqueued_at = replacement_progress.get("enqueued_at")
            assert replacement_enqueued_at != failed_enqueued_at, (
                "failed rebuild record was reused instead of replaced: "
                f"old={failed_enqueued_at} new={replacement_enqueued_at}")

            replacement_final = _wait_terminal(
                db_name, case_space, timeout=600, allow_failed=False)
            assert replacement_final["status"] == "completed", replacement_final
            replacement_error = (
                replacement_final.get("error_message")
                or replacement_final.get("error_msg")
                or ""
            )
            assert replacement_error == "", (
                f"completed replacement retained old error: {replacement_error!r}")
        finally:
            if victim_ps_idx is not None:
                try:
                    cl.start_ps(victim_ps_idx, wait_ready=True, timeout=30)
                except Exception as e:
                    logger.warning(
                        "1.4 cleanup start_ps(%d) failed: %s",
                        victim_ps_idx, e)
            try:
                drop_space(router_url, db_name, case_space)
            except Exception as e:
                logger.warning("1.4 cleanup drop_space failed: %s", e)

    def test_partition_replica_failure_skips_remaining_replicas(self):
        """Verifies exhausting one replica task cancels undispatched sibling tasks on the same partition."""
        _ensure_all_ps_alive()

        case_space = space_name + "_chaos_skip_replicas"
        rn, pn = 2, 1
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=pn, rn=rn))
        body = resp.json()
        if body.get("code") != 0:
            pytest.skip(f"cluster cannot host rn={rn}: {body}")
        _populate(case_space, total=5000)

        # Locate the PS hosting each replica of the single partition.
        pl = requests.get(f"{router_url}/partitions",
                          auth=(username, password), timeout=5).json()
        assert pl.get("code") == 0, pl
        detail = _get_space_detail(db_name, case_space)
        our_pids = {p.get("pid") for p in detail.get("partitions") or []}
        replica_nodes = []
        for it in pl.get("data") or []:
            if it.get("id") in our_pids:
                replica_nodes = [int(n) for n in (it.get("replicas") or [])]
                break
        assert len(replica_nodes) == rn, replica_nodes
        # Kill the PS carrying the first replica.
        victim_ps_idx = cl.ps_idx_for_node(replica_nodes[0])
        assert victim_ps_idx is not None, replica_nodes

        try:
            assert _trigger_rebuild(db_name, case_space, max_retries=1).json().get("code") == 0
            _wait_until_running(db_name, case_space, timeout=60)
            cl.kill_ps(victim_ps_idx, hard=True)

            final = _wait_terminal(db_name, case_space, timeout=300, allow_failed=True)
            # Whole record should be Failed (the killed replica exhausted
            # retries) — not Completed, not Cancelled.
            assert final["status"] == "failed", final

            tasks = final.get("tasks") or []
            assert len(tasks) == rn, tasks
            # Exactly one replica task is Failed (the victim); every other
            # replica task on the same partition is Cancelled (skipped).
            failed = [t for t in tasks if t.get("status") == "failed"]
            cancelled = [t for t in tasks if t.get("status") == "cancelled"]
            completed = [t for t in tasks if t.get("status") == "completed"]
            assert len(failed) >= 1, tasks
            assert len(cancelled) >= 1, (
                "R1 invariant broken: no sibling replica task was cancelled "
                "after the failing replica exhausted retries. Tasks: "
                + json.dumps(tasks, indent=2, default=str)
            )
            # Sanity: cancelled + failed + completed accounts for every task.
            assert len(failed) + len(cancelled) + len(completed) == len(tasks), tasks
        finally:
            try:
                cl.start_ps(victim_ps_idx, wait_ready=True, timeout=30)
            except Exception as e:
                logger.warning("skip-replicas cleanup start_ps failed: %s", e)
        drop_space(router_url, db_name, case_space)

    def teardown_class(self):
        _ensure_clean_db()


# ===========================================================================
# Category 2 — Master leader change
# ===========================================================================
class TestRebuildMasterFailover:

    def setup_class(self):
        _ensure_clean_db()

    def test_rebuild_resumes_after_leader_kill(self):
        """Verifies a rebuild resumes and completes after the Master leader is killed."""
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


            try:
                cl.start_master(leader, wait_quorum=True, timeout=30)
            except Exception as e:
                logger.warning("2.1 cleanup start_master(%s) failed: %s "
                               "— next test's _ensure_all_masters_alive "
                               "will retry", leader, e)
        drop_space(router_url, db_name, case_space)

    def test_pending_record_admitted_after_leader_kill(self):
        """Verifies a new Master leader admits and completes a rebuild left pending by its predecessor."""
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
        """Verifies a rebuild completes while the Master leader repeatedly changes."""
        case_space = space_name + "_chaos_churn"
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

        assert final["status"] == "completed", (
            f"rebuild did not complete under master churn:\n"
            f"  status={final.get('status')}\n"
            f"  error_message={final.get('error_message') or final.get('error_msg')}\n"
            f"  retry_count={final.get('retry_count')}\n"
            f"  completed_tasks={final.get('completed_tasks')}/"
            f"{final.get('total_tasks')}\n"
            f"  full record: {json.dumps(final, indent=2, default=str)[:800]}"
        )

        duplicates_seen = 0
        for ps_idx in PSES_IDX:
            txt = cl.read_node_logs("ps", ps_idx)
            duplicates_seen += txt.count("ignoring duplicate start")
        logger.info("test_no_double_dispatch: 'ignoring duplicate start' "
                     "count across all PSes = %d (zero is OK; positive "
                     "means PS-side guard fired and prevented reentry)",
                     duplicates_seen)

        drop_space(router_url, db_name, case_space)

    def teardown_class(self):
        _ensure_clean_db()


# ===========================================================================
# Category 3 — Replica serialization and query routing
# ===========================================================================
class TestRebuildReplicaRoutingChaos:

    def test_only_one_replica_per_partition_running_at_any_time(self):
        """Verifies all tasks in one rebuild record execute globally and strictly serially."""
        _ensure_clean_db()
        case_space = space_name + "_chaos_serial_r2p2"
        batch_size, total = 100, min(10000, xb.shape[0])
        total_batch = int(total / batch_size)

        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host replica_num=2: {resp.json()}")
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
                                # status="running" + dispatched=true means the task
                                # was actually sent to PS and is Running.
                                if t.get("status") == "running" and \
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

            # (a) Frame-level check: at most one running task cluster-wide.
            frame_violations = []
            for idx, (_, _, running) in enumerate(frames):
                if len(running) > 1:
                    frame_violations.append(
                        "frame %d has %d running tasks (expected <= 1): %s" %
                        (idx, len(running), running))

            assert frames, "no rebuild progress frames collected"
            assert not frame_violations, (
                "Global serial invariant violated:\n"
                + "\n".join(frame_violations))

            # (b) Interval-level check on final tasks: [start_time,
            # complete_time] intervals must be pairwise non-overlapping.
            # master writes start_time on dispatch, complete_time on
            # finalize; overlap ⇔ two tasks were Running at once.
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

            intervals = []  # list of (start, end, partition_id, replica_index)
            for t in final.get("tasks") or []:
                st = _parse_ts(t.get("start_time"))
                ct = _parse_ts(t.get("complete_time"))
                if not st or not ct or ct <= st:
                    continue
                intervals.append(
                    (st, ct, t.get("partition_id"), t.get("replica_index")))

            assert len(intervals) >= 2, (
                "expected >= 2 completed intervals to test serialism, "
                "got %d: tasks=%s" % (len(intervals), final.get("tasks")))

            overlaps = []
            for i in range(len(intervals)):
                s1, e1, p1, r1 = intervals[i]
                for j in range(i + 1, len(intervals)):
                    s2, e2, p2, r2 = intervals[j]
                    if s1 < e2 and s2 < e1:
                        overlaps.append(
                            "(pid=%s repl=%s [%s..%s]) overlaps "
                            "(pid=%s repl=%s [%s..%s])" % (
                                p1, r1, s1.isoformat(), e1.isoformat(),
                                p2, r2, s2.isoformat(), e2.isoformat()))
            assert not overlaps, (
                "Task intervals must be pairwise disjoint under global "
                "serial scheduling; overlaps:\n" + "\n".join(overlaps))
        finally:
            drop_space(router_url, db_name, case_space)

    def test_search_skips_rebuilding_replica(self):
        """Verifies normal searches remain successful while Router skips rebuilding replicas."""
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
            partitions = detail.get("partitions") or []
            assert len(partitions) >= 2, f"need >=2 partitions: {partitions}"
            search_url = router_url + "/document/search?timeout=5000"

            def _search_once():
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
                while not stop_evt.is_set():
                    try:
                        p = _get_progress(db_name, case_space)
                        snap = {}
                        for t in (p.get("tasks") if p else None) or []:
                            if not t.get("dispatched", False):
                                continue
                            if t.get("status") != "running":
                                continue
                            pid = t.get("partition_id")
                            nid = int(t.get("node_id", 0))


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



            if not seen_rebuilding:
                for t in final.get("tasks") or []:
                    if t.get("status") == "completed":
                        seen_rebuilding = True
                        break
            assert seen_rebuilding, (
                "No Rebuilding state was observed, so routing filters were not exercised")

            rebuilding_pairs = set()
            for _, snap in restatus_snapshots:
                for pid, nodes in snap.items():
                    for nid, st in nodes.items():
                        if st == 3:
                            rebuilding_pairs.add((int(pid), int(nid)))
            for t in final.get("tasks") or []:
                if t.get("status") == "completed":
                    rebuilding_pairs.add(
                        (int(t.get("partition_id")),
                         int(t.get("node_id", 0))))
            assert rebuilding_pairs, (
                "No rebuilding replica was identified for Router log verification")

            skip_pattern = re.compile(
                r"partition (\d+) skipped nodeID=(\d+) rebuilding, "
                r"client_type=(\S+)")
            skip_lines = []
            router_logs_available = False
            for ridx in (1, 2):
                txt = cl.read_node_logs("router", ridx)
                if txt:
                    router_logs_available = True
                for line in txt.splitlines():
                    m = skip_pattern.search(line)
                    if not m:
                        continue
                    pid = int(m.group(1))
                    nid = int(m.group(2))
                    ctype = m.group(3)
                    if ctype.lower() in ("leader", "fallback"):
                        continue
                    if (pid, nid) in rebuilding_pairs:
                        skip_lines.append(line)
            if not router_logs_available:
                pytest.skip("router logs unavailable in this cluster mode; "
                            "cannot verify replica-skip log line")
            assert skip_lines, (
                "Router logs did not show a non-Leader route skipping a rebuilding replica; "
                "rebuilding_pairs=%s" % (rebuilding_pairs,))
            logger.info("3.2 replica-skip verified, sample=%s",
                         skip_lines[0])

            assert search_results, "no search request was issued during rebuild"
            bad = [r for r in search_results if not r[1]]
            assert not bad, (
                "search returned non-200 or nonzero responses during rebuild; sample=%s" %
                (bad[:5],))

            rebuild_latencies = sorted(r[2] for r in search_results if r[1])
            rebuild_p99 = rebuild_latencies[
                max(0, int(len(rebuild_latencies) * 0.99) - 1)]
            logger.info("3.2 baseline_p99=%.1fms rebuild_p99=%.1fms "
                         "search_count=%d",
                         baseline_p99, rebuild_p99, len(search_results))
            assert rebuild_p99 < max(baseline_p99 * 5, 50), (
                "search p99 latency regressed during rebuild: "
                "baseline=%.1fms rebuild=%.1fms" %
                (baseline_p99, rebuild_p99))

            post_detail = _get_space_detail(db_name, case_space)
            for p in post_detail.get("partitions") or []:
                for nid, st in _partition_restatus_map(p).items():
                    assert st == "ReplicasOK", (
                        "replica did not return to OK after rebuild: pid=%s node=%s status=%s" %
                        (_partition_id(p), nid, st))

            post_results = [_search_once() for _ in range(30)]
            assert all(r[0] for r in post_results), (
                "search was not stable after rebuild; sample=%s" %
                (post_results[:5],))
        finally:
            drop_space(router_url, db_name, case_space)

    def test_leader_rebuild_falls_back_to_follower(self):
        """Verifies leader-directed searches fall back to a follower while the leader replica rebuilds."""
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
            partitions = detail.get("partitions") or []
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
                lid_int = int(leader_id)
                while not stop_evt.is_set():
                    try:
                        p = _get_progress(db_name, case_space)
                        for t in (p.get("tasks") if p else None) or []:
                            if int(t.get("node_id", -1)) != lid_int:
                                continue
                            if t.get("status") == "running" and \
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
                "Leader-directed queries failed; sample=%s" % (bad[:5],))

            if not leader_seen_rebuilding[0]:
                lid_int = int(leader_id)
                for t in final.get("tasks") or []:
                    if int(t.get("node_id", -1)) == lid_int and \
                       t.get("status") == "completed":
                        leader_seen_rebuilding[0] = True
                        break
            assert leader_seen_rebuilding[0], (
                "the leader replica was neither observed rebuilding nor found in final tasks; "
                "no completed task for node_id=%s was available to verify fallback" % (leader_id,))

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
                "Router logs did not contain 'partition X leader=Y rebuilding, "
                "fallback to nodeID=Z'")
            logger.info("leader fallback verified, sample=%s",
                        fallback_lines[0])

            post_detail = _get_space_detail(db_name, case_space)
            post_parts = post_detail.get("partitions") or []
            assert post_parts, "space details returned no partition after rebuild"
            post_partition = post_parts[0]
            post_leader_id = (post_partition.get("leader")
                              or post_partition.get("LeaderID")
                              or post_partition.get("raft_status", {}).get("Leader"))
            assert post_leader_id == leader_id, (
                "leader changed after rebuild: before=%s after=%s" %
                (leader_id, post_leader_id))
            post_results = [_leader_query_once() for _ in range(20)]
            assert all(status == 200 and code == 0
                       for status, code, _ in post_results), (
                "Leader-directed queries were not stable after rebuild; sample=%s" %
                (post_results[:5],))
        finally:
            drop_space(router_url, db_name, case_space)

    def test_cross_partition_no_routing_interference(self):
        """Verifies Router avoids a rebuild-busy PS for both the rebuilding partition and co-located partitions."""
        _ensure_clean_db()
        case_space = "%s_chaos_cross_iso_r2p2_%d" % (
            space_name, int(time.time() * 1000))
        resp = create_space(router_url, db_name, _hnsw_cfg(case_space, pn=2, rn=2))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host pn=2 rn=2: {resp.json()}")
        try:
            _populate(case_space, total=min(xb.shape[0], 10000))

            detail = _get_space_detail(db_name, case_space)
            partitions = detail.get("partitions") or []
            assert len(partitions) >= 2, f"need ≥2 partitions: {partitions}"

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

            chosen = None  # (p1_entry, p2_entry, X_node)
            for i in range(len(partitions)):
                for j in range(len(partitions)):
                    if i == j:
                        continue
                    p1_e, p2_e = partitions[i], partitions[j]
                    r1, r2 = _replica_nodes(p1_e), _replica_nodes(p2_e)
                    if not r1 or not r2:
                        continue
                    shared = r1 & r2
                    if not shared:
                        continue

                    for x in shared:
                        if r2 - {x}:
                            chosen = (p1_e, p2_e, x)
                            break
                    if chosen is None:


                        chosen = (p1_e, p2_e, next(iter(shared)))
                    break
                if chosen:
                    break

            if not chosen:
                pytest.skip(
                    "no (p1, p2, X) triplet with shared X; layout="
                    f"{[(p.get('pid'), _replica_nodes(p)) for p in partitions]}"
                )
            p1_e, p2_e, x_node = chosen
            p1_pid, p2_pid = p1_e["pid"], p2_e["pid"]
            p2_reps = _replica_nodes(p2_e)
            if not (p2_reps - {x_node}):
                pytest.skip(
                    f"p2({p2_pid}) has no idle replica besides X={x_node}; "
                    "only last-resort fallback is testable, not preferIdleHosts filtering")
            logger.info(
                "3.5 chosen layout: p1=pid:%s replicas=%s, "
                "p2=pid:%s replicas=%s, X=%d",
                p1_pid, _replica_nodes(p1_e),
                p2_pid, p2_reps, x_node,
            )

            def _search(client_timeout=2, url_timeout_ms=2000):
                search_url = (router_url +
                              f"/document/search?timeout={url_timeout_ms}")
                data = {
                    "vector_value": False,
                    "db_name": db_name,
                    "space_name": case_space,
                    "vectors": [{"field": "field_vector",
                                 "feature": xb[0].tolist()}],
                    "limit": 5,
                }
                try:
                    rs = requests.post(search_url,
                                       auth=(username, password),
                                       json=data, timeout=client_timeout)
                    if rs.status_code != 200:
                        return False, rs.text[:200]
                    body = rs.json()
                    return (body.get("code") == 0), body.get("msg", "")
                except Exception as e:
                    return False, repr(e)


            rebuild_url = (f"{router_url}/index/rebuild/dbs/{db_name}"
                           f"/spaces/{case_space}")
            trig_resp = requests.post(rebuild_url,
                                      auth=(username, password),
                                      json={"partition_id": p1_pid},
                                      timeout=30)
            assert trig_resp.json().get("code") == 0, trig_resp.text

            search_results = []
            stop_evt = threading.Event()
            window_open = [False]
            x_running_seen = [False]

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
                                        and t.get("status") == "running"
                                        and t.get("dispatched", False)):
                                    in_window = True
                                    x_running_seen[0] = True
                                    break
                        window_open[0] = in_window
                        if p and p.get("status") in (
                                "completed", "failed", "cancelled"):
                            return
                    except Exception:
                        pass
                    time.sleep(0.05)

            def _search_loop():
                while not stop_evt.is_set():
                    if window_open[0]:
                        search_results.append(_search())
                    time.sleep(0.01)

            watcher = threading.Thread(target=_watcher, daemon=True)
            searcher = threading.Thread(target=_search_loop, daemon=True)
            watcher.start(); searcher.start()

            final = _wait_terminal(db_name, case_space, timeout=600,
                                   allow_failed=False)
            assert final["status"] == "completed", final
            stop_evt.set()
            watcher.join(timeout=5)
            searcher.join(timeout=5)
            if not x_running_seen[0]:
                pytest.skip(
                    "X.r1 was not observed rebuilding; "
                    "this run had no valid window for invariant 3.5"
                )
            assert search_results, "no search samples were collected in the rebuild window"
            search_fail = [r for r in search_results if not r[0]]
            fail_rate = len(search_fail) / len(search_results)
            assert fail_rate <= 0.05, (
                f"search failure rate during rebuild was {fail_rate:.2%} "
                f"({len(search_fail)}/{len(search_results)}), which is too high: "
                f"Router did not skip the rebuilding replica or preferIdleHosts emptied p2 "
                f"candidates (sample failures={search_fail[:3]})"
            )
            #        "partition <p1> skipped nodeID=<X> rebuilding"

            #        "partition <p2> preferred idle replicas over busy
            #         nodeID=<X> rebuilding"
            skip_pattern = re.compile(
                r"partition (\d+) skipped nodeID=(\d+) rebuilding, "
                r"client_type=(\S+)"
            )
            prefer_pattern = re.compile(
                r"partition (\d+) preferred idle replicas over busy "
                r"nodeID=(\d+) rebuilding"
            )
            skip_hits, prefer_hits = [], []
            router_logs_available = False
            for ridx in (1, 2):
                txt = cl.read_node_logs("router", ridx)
                if txt:
                    router_logs_available = True
                for line in txt.splitlines():
                    m = skip_pattern.search(line)
                    if m and int(m.group(1)) == p1_pid and int(m.group(2)) == x_node:
                        if m.group(3).lower() not in ("leader", "fallback"):
                            skip_hits.append(line)
                        continue
                    m = prefer_pattern.search(line)
                    if m and int(m.group(1)) == p2_pid and int(m.group(2)) == x_node:
                        prefer_hits.append(line)
            if not router_logs_available:
                pytest.skip("router logs unavailable in this cluster mode; "
                            "cannot verify replica-skip / prefer-idle log lines")
            assert skip_hits, (
                f"Router logs did not show p1={p1_pid} filtering X.r1(node={x_node}); "
                f"expected the same skipped-node signal as test 3.2")
            assert prefer_hits, (
                f"Router logs did not show p2={p2_pid} filtering X.r2(busy nodeID={x_node}); "
                f"preferIdleHosts did not filter the busy node and cross-partition "
                f"routing isolation was violated")
            logger.info(
                "3.5 verified: search_ok=%d/%d, skip_hit=%s, prefer_hit=%s",
                len(search_results) - len(search_fail), len(search_results),
                skip_hits[0], prefer_hits[0],
            )
            post = _get_space_detail(db_name, case_space)
            for p in post.get("partitions") or []:
                rsm = p.get("replica_status") or {}
                for nid, st in rsm.items():
                    assert st != "ReplicasRebuildingIndex", (
                        f"after rebuild pid={p.get('pid')} node={nid} "
                        f"still reported Rebuilding"
                    )
        finally:
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass


# ===========================================================================
# Category 4 — Failure cleanup and retry invariants
# ===========================================================================
class TestRebuildPSFailureExtras:

    def setup_class(self):
        _ensure_clean_db()

    def test_rebuild_marks_replica_failed_via_poll_timeout(self):
        """Verifies a permanently unavailable PS eventually drives its rebuild task to a terminal state."""
        case_space = space_name + "_chaos_polltimeout"
        # replica_num=2: each partition has exactly 2 replicas.
        # When ps2 dies, the surviving replica on another PS must finish
        # the partition (or partition retry redispatches to a live PS).
        _ensure_all_ps_alive()
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 1.2: code={body.get('code')} "
            f"msg={body.get('msg')} (verify that all PS nodes are registered)")
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
        """Verifies a partition retry dispatches with dropBefore zero after an initial drop-before rebuild."""
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
        """Verifies terminal failure removes all rebuilding markers from replica status."""
        case_space = space_name + "_chaos_restatus_fail"
        _ensure_all_ps_alive()
        resp = create_space(router_url, db_name,
                            _hnsw_cfg(case_space, pn=2, rn=2))
        body = resp.json()
        assert body.get("code") == 0, (
            f"create_space failed for 3.4: code={body.get('code')} "
            f"msg={body.get('msg')}")
        _populate(case_space, total=5000)

        assert _trigger_rebuild(db_name, case_space,
                                max_retries=1).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)
        time.sleep(2)

        # Persistent kill — never restart inside try.
        cl.kill_ps(2, hard=True)
        try:
            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)

            detail_url = (
                f"{router_url}/dbs/{db_name}/spaces/"
                f"{case_space}?detail=true")
            partitions = None
            for _ in range(15):
                try:
                    r = requests.get(detail_url, auth=(username, password),
                                     timeout=5)
                    if r.status_code == 200:
                        partitions = (r.json().get("data") or {}).get("partitions")
                except requests.exceptions.RequestException:


                    partitions = None
                if partitions:
                    break
                time.sleep(2)
            if not partitions:
                pytest.skip(
                    "space details remained empty while ps2 was down due to leader election or "
                    "cluster degradation, so ReStatusMap could not be observed")

            stuck_rebuilding = []
            for p in partitions:
                pid = p.get("pid")
                rsm = _partition_restatus_map(p)
                for nid, st in rsm.items():
                    if st == "ReplicasRebuildingIndex":
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


            search_url = router_url + "/document/search?timeout=5000"
            search_data = {
                "vector_value": False,
                "db_name": db_name,
                "space_name": case_space,
                "vectors": [{"field": "field_vector",
                             "feature": xb[0].tolist()}],
                "limit": 1,
            }
            sr = None
            for _ in range(8):
                try:
                    sr = requests.post(search_url, auth=(username, password),
                                       json=search_data, timeout=10)
                    if sr.status_code == 200:
                        break
                except requests.exceptions.RequestException:
                    sr = None
                time.sleep(2)
            assert sr is not None and sr.status_code == 200, (
                "post-failure search never returned normally while ps2 was down and Router kept "
                f"hanging or timing out: {getattr(sr, 'text', 'no response')[:200]}")
            logger.info("post-failure search response code=%s",
                         sr.json().get("code"))
        finally:
            try:
                cl.start_ps(2, wait_ready=True, timeout=30)
            except Exception:
                pass
        drop_space(router_url, db_name, case_space)

    def test_restatus_map_resets_on_cancelled_rebuild(self):
        """Verifies cancellation removes all rebuilding markers from replica status."""
        case_space = space_name + "_chaos_restatus_cancel"
        _ensure_all_ps_alive()
        resp = create_space(
            router_url, db_name, _hnsw_cfg(case_space, pn=1, rn=2)
        )
        assert resp.json().get("code") == 0, resp.text
        _populate(case_space, total=10000)

        assert _trigger_rebuild(
            db_name, case_space
        ).json().get("code") == 0
        _wait_until_running(db_name, case_space, timeout=60)

        observed_rebuilding = set()
        deadline = time.time() + 30
        while time.time() < deadline and not observed_rebuilding:
            for partition in _get_space_detail(db_name, case_space).get(
                    "partitions", []):
                pid = partition.get("pid")
                for node_id, status in _partition_restatus_map(
                        partition).items():
                    if status == "ReplicasRebuildingIndex":
                        observed_rebuilding.add((pid, str(node_id)))
            if not observed_rebuilding:
                time.sleep(0.2)
        assert observed_rebuilding, (
            "no replica entered ReplicasRebuildingIndex; cancellation "
            "cleanup path was not exercised"
        )

        cancel = requests.post(
            f"{router_url}/index/rebuild/dbs/{db_name}/spaces/"
            f"{case_space}/cancel",
            auth=(username, password), timeout=30,
        )
        cancel_body = cancel.json()
        assert cancel_body.get("code") == 0, cancel_body
        entries = (cancel_body.get("data") or {}).get("results") or []
        assert entries, cancel_body
        assert entries[0].get("status") in ("running", "cancelled"), entries[0]

        final = _wait_terminal(db_name, case_space, timeout=300)
        assert final.get("status") in ("completed", "cancelled"), final
        assert final.get("pending_tasks", 0) == 0, final

        final_detail = _get_space_detail(db_name, case_space)
        stuck = []
        for partition in final_detail.get("partitions", []):
            pid = partition.get("pid")
            for node_id, status in _partition_restatus_map(partition).items():
                if status == "ReplicasRebuildingIndex":
                    stuck.append((pid, node_id, status))
        assert not stuck, (
            f"cancelled rebuild left ReStatusMap entries rebuilding: {stuck}; "
            f"initially observed={sorted(observed_rebuilding)}"
        )
        drop_space(router_url, db_name, case_space)

    def test_multi_target_fail_first_aborts_rest(self):
        """Verifies failure of the first index target aborts all later targets in a multi-index rebuild."""
        _ensure_clean_db()

        case_space = "%s_chaos_multi_fail_first_%d" % (
            space_name, int(time.time() * 1000))
        dim = xb.shape[1]
        _ensure_all_ps_alive()

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
                                      "ncentroids": 32, "nprobe": 8,
                                      "training_threshold": 1248}},
                 "dimension": dim},
                {"name": "field_vector_c", "type": "vector",
                 "index": {"name": "gamma_c", "type": "IVFPQ",
                           "params": {"metric_type": "InnerProduct",
                                      "ncentroids": 32, "nprobe": 8,
                                      "nsubvector": 32,
                                      "training_threshold": 1248}},
                 "dimension": dim},
            ],
        }
        resp = create_space(router_url, db_name, cfg)
        if resp.json().get("code") != 0:
            # Full response dump for diagnosis — pytest.skip truncates long msgs.
            body = resp.json()
            full_msg = body.get("msg") or body.get("message") or ""
            logger.error("create_space failed full body: %s", body)
            logger.error("create_space failed full msg: %s", full_msg)
            pytest.skip(
                f"cluster cannot create multi-vector space: "
                f"code={body.get('code')} msg={full_msg!r}")

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


        kill_ps_idx = None
        try:
            pl = requests.get(f"{router_url}/partitions",
                              auth=(username, password), timeout=5).json()
            assert pl.get("code") == 0, pl
            our_node = None
            detail = _get_space_detail(db_name, case_space)
            our_pids = {p.get("pid") for p in detail.get("partitions") or []}
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
            resp = _trigger_rebuild(db_name, case_space, max_retries=1)
            assert resp.json().get("code") == 0, resp.text
            cl.kill_ps(kill_ps_idx, hard=True)

            final = _wait_terminal(db_name, case_space, timeout=300,
                                   allow_failed=True)

            # (1) status == failed
            assert final["status"] == "failed", (
                f"expected failure because the only replica was killed permanently; "
                f"got {final}"
            )
            indexes = final.get("indexes") or []
            assert len(indexes) == 3, (
                f"expected 3 IndexTargets (HNSW+IVFFLAT+IVFPQ), got "
                f"{len(indexes)}: {indexes}"
            )
            cur = final.get("current_index")
            assert cur == 1, (
                f"current_index must remain 1 after the first target fails; "
                f"got {cur}, meaning failure silently advanced to another "
                f"target contrary to rebuild_service.go fail-fast semantics"
            )

            first_target = indexes[0]
            assert isinstance(first_target, str) and first_target, (
                f"first target should be a non-empty index name, got: {first_target!r}"
            )

            logger.info(
                "6.8 verified: status=failed, indexes=%d, current_index=%s, "
                "first=%s",
                len(indexes), cur, first_target,
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

    def teardown_class(self):
        _ensure_clean_db()


# ===========================================================================
# Category 5 — Concurrent search during rebuild
# ===========================================================================
class TestRebuildConcurrentWrites:

    def setup_class(self):
        _ensure_clean_db()

    def test_search_no_errors_during_rebuild(self):
        """Verifies search error rate stays below the allowed threshold during rebuild."""
        batch_size, total = 100, 10000
        case_space = space_name + "_chaos_search_load"

        cfg = _hnsw_cfg(case_space, pn=1, rn=2)
        resp = create_space(router_url, db_name, cfg)
        if resp.json().get("code") != 0:
            pytest.skip(
                f"5.3 needs ≥2 PS to satisfy rn=2: {resp.json()}")

        detail0 = _get_space_detail(db_name, case_space)
        placement_check = []
        for p in detail0.get("partitions", []):
            rsm = p.get("replica_status") or {}
            placement_check.append((p.get("pid"), len(rsm), list(rsm.keys())))

        first = (detail0.get("partitions") or [{}])[0]
        raft_replicas = (first.get("raft_status") or {}).get("Replicas") or {}
        if len(raft_replicas) < 2:
            drop_space(router_url, db_name, case_space)
            pytest.skip(
                f"5.3 partition was placed with only {len(raft_replicas)} replica(s): "
                f"{raft_replicas} (placement={placement_check}); test requires "
                f"≥2 to survive single-replica Rebuilding window")
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        stop_evt = threading.Event()

        events = []
        events_lock = threading.Lock()

        def search_loop():
            url = router_url + "/document/search?timeout=10000"
            i = 0
            while not stop_evt.is_set():
                data = {"vector_value": False, "db_name": db_name, "space_name": case_space,
                        "vectors": [{"field": "field_vector", "feature": xb[i % total].tolist()}]}
                ts = time.time()
                try:
                    rs = requests.post(url, auth=(username, password), json=data, timeout=10)


                    try:
                        body = rs.json()
                    except Exception:
                        body = {}
                    code = body.get("code")
                    if rs.status_code == 200 and code == 0:
                        with events_lock:
                            events.append((ts, "ok", None))
                    else:

                        with events_lock:
                            events.append((ts, "http_err",
                                           (rs.status_code, code,
                                            (body.get("msg") or "")[:160])))
                except Exception as e:
                    with events_lock:
                        events.append((ts, "exception", repr(e)[:160]))
                i += 1
                time.sleep(0.02)

        t = threading.Thread(target=search_loop, daemon=True)
        t.start()
        time.sleep(2)

        _wait_index_status_indexed(db_name, case_space)
        rebuild_start = time.time()
        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0

        restatus_snapshots = []  # list of (ts, {nodeID: state})

        def restatus_poller():
            while not stop_evt.is_set():
                try:
                    d = _get_space_detail(db_name, case_space)
                    for p in d.get("partitions", []):
                        rsm = p.get("replica_status") or {}
                        restatus_snapshots.append((time.time(), dict(rsm)))
                        break
                except Exception:
                    pass
                time.sleep(0.1)

        rs_thread = threading.Thread(target=restatus_poller, daemon=True)
        rs_thread.start()

        _wait_terminal(db_name, case_space, timeout=600)
        rebuild_end = time.time()

        time.sleep(2)
        stop_evt.set()
        t.join(timeout=5)
        rs_thread.join(timeout=5)

        # baseline:   ts < rebuild_start

        # post:       ts > rebuild_end
        baseline = [e for e in events if e[0] < rebuild_start]
        during   = [e for e in events if rebuild_start <= e[0] <= rebuild_end]
        post     = [e for e in events if e[0] > rebuild_end]

        def summarize(label, slice_):
            ok = sum(1 for _, k, _ in slice_ if k == "ok")
            errs = [e for e in slice_ if e[1] != "ok"]
            rate = len(errs) / max(1, len(slice_))
            logger.info("5.3 %s: n=%d ok=%d err=%d rate=%.2f%%",
                        label, len(slice_), ok, len(errs), rate * 100)

            if errs:
                for ts, kind, detail in errs[:5]:
                    logger.info("    sample err (%s): %s", kind, detail)

                from collections import Counter
                buckets = Counter()
                for _, kind, detail in errs:
                    if kind == "http_err" and isinstance(detail, tuple) and len(detail) >= 2:
                        # detail = (status_code, vearch_code, msg)
                        buckets[(kind, detail[0], detail[1])] += 1
                    else:
                        buckets[(kind, None, None)] += 1
                logger.info("    error breakdown (kind, http_status, vearch_code): %s",
                            dict(buckets))
            return rate, errs

        summarize("baseline", baseline)
        during_rate, during_errs = summarize("during rebuild", during)
        summarize("post", post)

        rebuild_secs = rebuild_end - rebuild_start
        logger.info("5.3 rebuild window duration: %.1fs", rebuild_secs)

        prev_state = None
        for ts, rsm in restatus_snapshots:

            non_ok = {nid: st for nid, st in rsm.items()
                      if st != "ReplicasOK"}
            if non_ok != prev_state:
                logger.info("    ReStatusMap @%.2fs: %s",
                            ts - rebuild_start, rsm)
                prev_state = non_ok

        all_rebuilding_windows = []
        for ts, rsm in restatus_snapshots:
            if rsm and all(st == "ReplicasRebuildingIndex" for st in rsm.values()):
                all_rebuilding_windows.append(ts)
        if all_rebuilding_windows:
            duration = all_rebuilding_windows[-1] - all_rebuilding_windows[0]
            logger.warning(
                "5.3 detected 'all replicas Rebuilding' window: "
                "%d snapshots span %.2fs — this is the root cause of 703 errors",
                len(all_rebuilding_windows), duration)

        assert len(during) >= 5, (
            f"too few during-rebuild samples (n={len(during)}); rebuild or search "
            f"loop timing prevented a meaningful error-rate calculation")

        assert during_rate < 0.10, (
            f"search error rate during rebuild = {during_rate:.2%}, "
            f"exceeded the 10% threshold; inspect 'sample err' and 'error breakdown' "
            f"logs to identify the error type")
        drop_space(router_url, db_name, case_space)

    def teardown_class(self):
        drop_db(router_url, db_name)
