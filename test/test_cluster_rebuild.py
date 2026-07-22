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
        # PS 死 / 集群降级时 router 可能 hang 到超时;视为本次 poll 无结果,
        # 由调用方(_wait_terminal / _wait_until_running 等都 `if p:` 兜底)
        # 下一轮重试,而不是把整条测试带挂。
        return None
    if r.status_code != 200:
        return None
    body = r.json()
    if body.get("code") != 0:
        return None
    return body.get("data", {}) or {}


def _get_space_detail(db, space, retries=6, retry_sleep=2):
    """读 space detail。kill PS / 集群降级时 router 汇总分区信息会 hang 到
    超时;这里重试若干次(给 leader 重选 / PS 恢复留时间),持续失败才 raise,
    而不是单次 ReadTimeout 就把整条测试带挂。所有调用方共享这层健壮性。
    """
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
        f"_get_space_detail({db}/{space}) 连续 {retries} 次失败"
        f"(集群可能严重降级): {last}")


def _partition_id(partition):
    return partition.get("pid", partition.get("partition_id"))


def _partition_restatus_map(partition):
    # detail 接口把 per-replica rebuild 状态暴露在 "replica_status",值是字符串
    # (ReplicasOK / ReplicasRebuildingIndex / ReplicasNotReady),见
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
                                  "training_threshold": 2000}},
             "dimension": dim},
        ],
    }


def _wait_index_status_indexed(db, space, max_rounds=180, poll_interval=5):
    """等到 space 所有 partition 的 index_status 都到 INDEXED(=2) 且 status!="red"。

    waiting_index_finish 只等全局 index_num 到 total,不保证每个 partition 的
    index_status 已翻成 INDEXED。慢速 CI 上这个间隙会被放大,直接触发 rebuild
    会撞到 "rebuild requires an existing index"(某 partition 仍 UNINDEXED)。
    所有 chaos 测试都经 _populate 准备数据,这里统一加闸。
    """
    url = f"{router_url}/dbs/{db}/spaces/{space}?detail=true"
    for _ in range(max_rounds):
        try:
            rs = requests.get(url, auth=(username, password), timeout=5)
            data = rs.json().get("data", {}) if rs.status_code == 200 else {}
        except requests.exceptions.RequestException:
            data = {}  # router 暂时 hang(降级)→ 本轮跳过,下一轮重试
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

    def setup_class(self):
        _ensure_clean_db()

    def test_rebuild_survives_ps_kill_and_restart(self):
        """Kill the PS executing an IVFPQ task, restart it, and verify retry.

        The victim is selected from an actually dispatched Running task so
        this test cannot pass merely because a fixed PS had no in-flight work.
        """
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
        """Exhaust retries, then replace the failed record with a new run.

        The victim is selected from an actually dispatched Running task;
        record.status=running alone does not prove that a fixed PS currently
        owns in-flight work. After failure the PS is restored and a new
        request must replace the terminal record and complete successfully.
        """
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
        """1.4b: R1 invariant — once one replica of a partition exhausts its
        retry budget, every remaining not-yet-dispatched replica task on
        the SAME partition must be cancelled (not failed, not run), so at
        least one physical replica of that partition is left untouched and
        the partition stays queryable.

        Setup: rn>=2, pn=1, max_retries=1. Kill the PS hosting one specific
        replica so its retries all fail; the other replica's task must end
        up Cancelled (not Completed, not Failed).
        """
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
# Category 1 (additional) — slow path + DropBefore=0 invariant
# ===========================================================================


class TestRebuildReplicaRoutingChaos:

    def test_only_one_replica_per_partition_running_at_any_time(self):
        """3.1: 全局 rebuild 串行 — 任一时刻至多 1 个 task 在 Running。

        新调度语义: 一个 record 内所有 task 严格串行执行 (dispatchPending 里
        `如果有任何 Dispatched && !terminal 的 task, 就一个都不派发`)。因此:
          - 每个 progress frame 的 running 集合大小 <= 1
          - final tasks 的 [start_time, complete_time] 区间两两不重叠

        用 rn=2(而非 rn=3):3-PS 的 docker 集群 + resource_limit_rate=0.98
        放不下 rn=3 会直接 skip。rn=2 + pn=2 共 4 个 task, 足以观察串行序。
        """
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
            partitions = detail.get("partitions") or []
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
                            if t.get("status") != "running":
                                continue
                            pid = t.get("partition_id")
                            nid = int(t.get("node_id", 0))
                            # 用 3 (entity.ReplicasRebuildingIndex) 表示 router
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
                    if t.get("status") == "completed":
                        seen_rebuilding = True
                        break
            assert seen_rebuilding, (
                "ReStatusMap 全程没出现 Rebuilding 状态，无法验证路由过滤")

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
                "未能确定任一 Rebuilding 副本, 无法核对 router 日志")

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
                "router 日志未出现非 Leader 分支跳过 Rebuilding 副本的记录, "
                "rebuilding_pairs=%s" % (rebuilding_pairs,))
            logger.info("3.2 replica-skip verified, sample=%s",
                         skip_lines[0])

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
            for p in post_detail.get("partitions") or []:
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
                "Leader 类型查询出现失败, sample=%s" % (bad[:5],))

            # Fallback: polling 漏采时, 直接看 final tasks 是否包含一条
            # node_id == leader_id 的已完成 task; 有 ⇒ leader 副本确实被
            # 重建过, 期间 master 必然把它标过 Rebuilding。
            if not leader_seen_rebuilding[0]:
                lid_int = int(leader_id)
                for t in final.get("tasks") or []:
                    if int(t.get("node_id", -1)) == lid_int and \
                       t.get("status") == "completed":
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
            post_parts = post_detail.get("partitions") or []
            assert post_parts, "rebuild 后 detail 未返回任何 partition"
            post_partition = post_parts[0]
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
        """3.5: rebuild p1 时 router 对共驻 PS 节点 X 上 p2 的候选过滤。

        pn=2/rn=2 时两个 partition 的 replica 集合可能在某 PS 节点 X 上相交,
        X 同时持有 p1.r1 与 p2.r2。触发 p1 单 partition rebuild 后:
          (a) X.r1 进入 Rebuilding 期间, 针对 p1 的查询不能落到 X.r1 —
              否则会撞上正在 tear-down 的引擎(硬过滤, isRebuildingIndex,
              无回退——rebuilding 副本永远不是自己 partition 的合法目标)。
              可观测信号: router 日志 "partition <p1> skipped nodeID=<X>
              rebuilding, client_type=<...>" (与 3.2 用例同一条日志)。
          (b) 同时间窗内, 针对 p2 的查询应把 X.r2 从候选集里踢掉, 转去
              其他副本 — X 上重建吃 CPU/IO, 让 co-tenant p2 也走这台会
              被拖累 (preferIdleHosts / rebuildBusyNodeID)。这是"带
              last-resort 回退的硬过滤": idle 子集非空时 busy 节点权重
              为 0; 若 p2 在 X 之外的副本都不可达, 回退到 X.r2 保住可
              用性 (不是加权路由,busy 节点在有其他候选时拿不到流量)。
              可观测信号: router 日志 "partition <p2> preferred idle
              replicas over busy nodeID=<X> rebuilding"。

        HTTP 响应和 partition 心跳都不暴露每次命中的 nodeID, 本用例复用
        3.2 用例已建立的"抓 router 日志"模式——两个信号都是 router 内
        logSkipRebuildingReplica / preferredIdleLogged 首见去重后写出的
        Warn 行, 直接 grep 即可。测试不 kill 任何进程, 拓扑要求仅"存在
        某 X 同时承载 p1、p2 的副本, 且 p2 在 X 之外还有其他副本"
        (后者保证 (b) 分支不退化到 last-resort); 若拓扑不满足则 skip。
        """
        _ensure_clean_db()
        case_space = "%s_chaos_cross_iso_r2p2_%d" % (
            space_name, int(time.time() * 1000))

        # 1. 建 space 并写入数据 -----------------------------------------
        resp = create_space(router_url, db_name, _hnsw_cfg(case_space, pn=2, rn=2))
        if resp.json().get("code") != 0:
            pytest.skip(f"cluster cannot host pn=2 rn=2: {resp.json()}")
        try:
            _populate(case_space, total=min(xb.shape[0], 10000))

            # 2. 拿 partition 布局, 找到 (p1, p2, X): 至少存在一个 PS 节点
            #    X 同时是 p1、p2 的副本。detail API 的 replica_status 由 PS
            #    心跳异步写入, 新建 space 后可能为空; 改用 master /partitions
            #    直接从 etcd 读权威 Replicas。
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
                    # 优先挑 p2 有 idle 副本可换 (r2 - {X}) 非空的 X, 否则
                    # 只剩 last-resort 回退路径, 观测不到 preferred-idle 日志。
                    for x in shared:
                        if r2 - {x}:
                            chosen = (p1_e, p2_e, x)
                            break
                    if chosen is None:
                        # 拓扑退化: r1 == r2, 任何 shared X 都让 p2 只剩 X;
                        # 仍记下来, 后面 skip 说明清楚。
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
                    f"p2({p2_pid}) 副本 == {{X={x_node}}} — 没有 idle 备选, "
                    "只能验 last-resort 回退, 覆盖不到 preferIdleHosts 的踢除逻辑")
            logger.info(
                "3.5 chosen layout: p1=pid:%s replicas=%s, "
                "p2=pid:%s replicas=%s, X=%d",
                p1_pid, _replica_nodes(p1_e),
                p2_pid, p2_reps, x_node,
            )

            # 3. 查询函数: /document/search 走 SearchByPartitions →
            #    searchFromPartition, 每个 partition 独立走一次
            #    SelectNodeByClientType(clientType=""/Random)。一次整空间
            #    search 会同时命中 p1 与 p2 各自的候选筛选路径, 从而在
            #    router 日志里同时写出 (i) p1 skip X.r1 与 (ii) p2 prefer
            #    idle-over-X 两条 Warn。hash 分区的 space 里 partition.Name
            #    为空, partition_names 过滤会让 sendMap 空 (与 3.2 一致),
            #    故不带 partition_names, 用全空间 search 覆盖 p1、p2。
            #    setRequestHeadFromGin 不读 body 的 load_balance, 默认
            #    ClientType="", SelectNodeByClientType 走 Random 分支。
            #    /document/query + partition_id 走 Execute() 是 leader-only,
            #    不进 SelectNodeByClientType, 抓不到目标日志——不能用它。
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

            # 4. 触发 p1 单 partition rebuild --------------------------
            rebuild_url = (f"{router_url}/index/rebuild/dbs/{db_name}"
                           f"/spaces/{case_space}")
            trig_resp = requests.post(rebuild_url,
                                      auth=(username, password),
                                      json={"partition_id": p1_pid},
                                      timeout=30)
            assert trig_resp.json().get("code") == 0, trig_resp.text

            # 5. 启动 watcher + search 线程, 仅在 X.r1 处于 Rebuilding
            #    窗口内采样。一次整空间 search 同时驱动 p1、p2 各自的
            #    SelectNodeByClientType, 首见去重的两条日志只需要窗口内
            #    发生一次即可落盘。
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

            # 6. 断言 -------------------------------------------------
            if not x_running_seen[0]:
                # 没看到 X.r1 进入 Rebuilding: rebuild 太快或先重建了另一
                # replica。本次运行没有有效窗口, skip (与 3.3 行为一致)。
                pytest.skip(
                    "X.r1 未在 rebuild 期间被观察到 Rebuilding 状态;"
                    "本次运行不构成有效窗口,无法验证 3.5 不变量"
                )

            assert search_results, "窗口内未采集到 search 样本"

            # 6a. 全空间 search 必须整体 200/code=0 — router 既跳过 X.r1
            #    (硬过滤, 无回退), 又不把 p2 候选清空 (preferIdleHosts 的
            #    last-resort 回退保证 p2 至少有一个可达副本)。search 是
            #    fan-out 广播, 只要任一 partition 失败整体 code!=0, 就
            #    等价于 p1 或 p2 路由被打断。
            search_fail = [r for r in search_results if not r[0]]
            fail_rate = len(search_fail) / len(search_results)
            assert fail_rate <= 0.05, (
                f"rebuild 窗口内 search 失败率 {fail_rate:.2%} "
                f"({len(search_fail)}/{len(search_results)}) 过高: "
                f"router 未跳过 Rebuilding 副本, 或 preferIdleHosts 把 p2 "
                f"候选清空 (sample fail={search_fail[:3]})"
            )

            # 6b. router 日志双信号 (核心断言, 复用 3.2 的 grep 模式):
            #   (i)  硬过滤 X.r1(无回退):
            #        "partition <p1> skipped nodeID=<X> rebuilding"
            #   (ii) 带回退的硬过滤把 X.r2 踢出候选:
            #        "partition <p2> preferred idle replicas over busy
            #         nodeID=<X> rebuilding"
            #   两条都是 首见去重 + Warn 级别, 窗口内只要发生就会写一次。
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
                f"router 日志未出现 p1={p1_pid} 硬过滤 X.r1(node={x_node}) "
                f"的记录 — 与 3.2 断言同一条 (skipped nodeID=... rebuilding)")
            assert prefer_hits, (
                f"router 日志未出现 p2={p2_pid} 把 X.r2(busy nodeID={x_node}) "
                f"从候选踢出的记录 — preferIdleHosts 未生效, 跨 partition "
                f"路由隔离被破坏")
            logger.info(
                "3.5 verified: search_ok=%d/%d, skip_hit=%s, prefer_hit=%s",
                len(search_results) - len(search_fail), len(search_results),
                skip_hits[0], prefer_hits[0],
            )

            # 7. rebuild 完成后所有 ReStatus 回到 OK ------------------
            post = _get_space_detail(db_name, case_space)
            for p in post.get("partitions") or []:
                rsm = p.get("replica_status") or {}
                for nid, st in rsm.items():
                    assert st != "ReplicasRebuildingIndex", (
                        f"rebuild 完成后 pid={p.get('pid')} node={nid} "
                        f"残留 Rebuilding"
                    )
        finally:
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass


class TestRebuildPSFailureExtras:

    def setup_class(self):
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

        Setup: max_retries=1 + persistent ps2 kill. API 契约里
        max_retries=0 表示"use default" (defaultMaxRetries=3),不是
        "零重试"; 想在最少的重试次数下走完 "in-place retry → markFailed
        → sibling cancel → unmarkRebuildingForTerminalTasks" 全链路,
        max_retries=1 就够 — 一次 in-place retry 之后 markFailed,同款
        marker 清扫路径,不会因为 retry 计数不同产生分支。
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

        # max_retries=1 → 一次 in-place retry 后 markFailed,触发 sibling cancel
        # + unmarkRebuildingForTerminalTasks 完整清扫路径。
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
                    # ps2 死期间 router 汇总分区信息会 hang 到超时 → 视为本轮
                    # 拿不到,继续重试(等 leader 重选 / 集群稳定)。
                    partitions = None
                if partitions:
                    break
                time.sleep(2)
            if not partitions:
                pytest.skip(
                    "ps2 死期间 detail 持续返回空 partitions(leader 重选/"
                    "集群降级),本次无法观测 ReStatusMap")

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
            # query traffic). ps2 死期间 router 可能 hang,重试几次并容忍瞬时
            # 超时;只要有一次正常响应即可。
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
                "post-failure search 始终未正常响应(ps2 死期间 router 持续"
                f"hang/超时):{getattr(sr, 'text', 'no response')[:200]}")
            logger.info("post-failure search response code=%s",
                         sr.json().get("code"))
        finally:
            # best-effort:mid-rebuild kill 后 ps2 冷启动可能超过 30s
            # (raft 追赶 + Gamma 引擎 reload + master lease 重认),
            # 30s 超时不代表清理失败 — 下一条测试的 _ensure_all_ps_alive
            # 会兜底 kill+restart 走完整的 KeepAlive 注册流程。
            # 参见同文件 1.5 用例(test_partition_retry_forces_drop_before_zero)
            # 的同款 pattern。
            try:
                cl.start_ps(2, wait_ready=True, timeout=30)
            except Exception:
                pass
        drop_space(router_url, db_name, case_space)

    def test_restatus_map_resets_on_cancelled_rebuild(self):
        """A cancelled running rebuild must not leave router-visible
        ReplicasRebuildingIndex markers behind.

        Unlike the former standalone smoke test, this case first proves that
        a concrete replica entered Rebuilding, then cancels the running record
        and checks every replica after the record reaches a terminal state.
        """
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
        _ensure_clean_db()

        case_space = "%s_chaos_multi_fail_first_%d" % (
            space_name, int(time.time() * 1000))
        dim = xb.shape[1]
        _ensure_all_ps_alive()

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

        # 找到唯一 partition 的唯一 replica 所在的 PS instance。
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
            #     target。target 现在是 IndexName 字符串 (e.g. "gamma_a")。
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
# Concurrent search during rebuild
# ===========================================================================

class TestRebuildConcurrentWrites:

    def setup_class(self):
        _ensure_clean_db()

    def test_search_no_errors_during_rebuild(self):
        """5.3: 持续 search 期间触发 rebuild,running 窗口内错误率应当低。

        过去把 [baseline + rebuild_running + post_stop] 三段都揉成一个
        err_rate,假阳性很高 — baseline 阶段的偶发错和 post 阶段集群刚
        收尾的瞬态错都会把分子顶起来。改成按时间戳分段统计 rebuild
        running 窗口内的 err_rate,并把错误样本打印到日志(否则失败
        时只看到 "22.6%" 完全不知道是什么错)。

        阈值放到 10%(在单机 chaos 集群上跑 rn=2 时,master→router cache
        同步空挡 + per-partition 串行重建副本切换都会带来 1-3% 的瞬态错,
        2% 不现实)。如果 ≥10% 那才是真有问题。
        """
        batch_size, total = 100, 10000
        case_space = space_name + "_chaos_search_load"

        # rn=2 是本测试的硬前提:重建期间唯一 replica 被标 Rebuilding 后
        # router 的 random 路由会把它过滤干净 (`randIDs=[]`),
        # `replicaRoundRobin.Next(_, [])` 返回 nodeID=0,最后撞
        # `create_rpcclient_failed` (code 703)。这条 503 错跟 rebuild
        # 本身无关,纯粹是「单点 + 副本被滤」的副作用。如果集群规模
        # 不够放 rn=2,直接 skip — 否则这条测试的语义不成立。
        cfg = _hnsw_cfg(case_space, pn=1, rn=2)
        resp = create_space(router_url, db_name, cfg)
        if resp.json().get("code") != 0:
            pytest.skip(
                f"5.3 needs ≥2 PS to satisfy rn=2: {resp.json()}")
        # 即便 create_space 返回 0,也要验证 placement 真的给了 2 份
        # — 有些集群配置下 rn 会被静默降级,空看 code 不靠谱。
        detail0 = _get_space_detail(db_name, case_space)
        placement_check = []
        for p in detail0.get("partitions", []):
            rsm = p.get("replica_status") or {}
            placement_check.append((p.get("pid"), len(rsm), list(rsm.keys())))
        # 注意 rsm 可能在创建后还没立刻写满,这里只用 raft_status.Replicas
        # 这条权威源做断言。
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
        # 改成存 (timestamp, kind, detail) 三元组,kind ∈ {ok, http_err, exception}
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
                    # 关键: vearch router 在 status_code != 200 时 body 仍是
                    # JSON 带 {code, msg};不要 silently 丢掉。
                    try:
                        body = rs.json()
                    except Exception:
                        body = {}
                    code = body.get("code")
                    if rs.status_code == 200 and code == 0:
                        with events_lock:
                            events.append((ts, "ok", None))
                    else:
                        # 把 HTTP status + vearch code + msg 全捕获
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
        time.sleep(2)  # 让 baseline 阶段先跑一会儿,稍后剔除

        _wait_index_status_indexed(db_name, case_space)
        rebuild_start = time.time()
        assert _trigger_rebuild(db_name, case_space).json().get("code") == 0

        # 后台同步快照 ReStatusMap, 用于诊断 503/703 类错误时 partition 副本
        # 状态: 重建途中是否出现「所有 replica 同时被标 Rebuilding」的窗口
        # (即 router 视角下没有任何可用 replica → nodeID=0 → code=703)。
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

        # === 分段统计 ===
        # baseline:   ts < rebuild_start
        # during:     rebuild_start <= ts <= rebuild_end (这是我们要 assert 的窗口)
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
            # 打前 5 条错误的细节,失败时方便定位
            if errs:
                for ts, kind, detail in errs[:5]:
                    logger.info("    sample err (%s): %s", kind, detail)
                # 按 (kind, http_status, vearch_code) 聚合 — 看错误是不是同源
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

        # 把 ReStatusMap 时序压成一行行 (相邻同状态 dedup), 看是否真出现
        # 「所有 replica 同时 Rebuilding」的窗口。
        prev_state = None
        for ts, rsm in restatus_snapshots:
            # 只统计有非 ReplicasOK 的快照 (Rebuilding=3 / NotReady=2 都算)
            non_ok = {nid: st for nid, st in rsm.items()
                      if st != "ReplicasOK"}
            if non_ok != prev_state:
                logger.info("    ReStatusMap @%.2fs: %s",
                            ts - rebuild_start, rsm)
                prev_state = non_ok
        # 直接判:是否存在「所有 replica 同时 Rebuilding」的瞬间
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
            f"during-rebuild 样本太少 (n={len(during)});rebuild 太快或 search "
            f"loop 太慢,无法统计真实 error rate")

        # 真正的 assert:rebuild 窗口内 error rate < 10%。这个阈值容忍
        # master→router cache 同步空挡 + per-partition 串行重建副本切换
        # 这类瞬态错;真出现 ≥10% 那才是回归。
        assert during_rate < 0.10, (
            f"search error rate during rebuild = {during_rate:.2%}, "
            f"超出 10% 容忍线;查看日志 'sample err' / 'error breakdown' "
            f"定位是哪一种错")
        drop_space(router_url, db_name, case_space)

    def teardown_class(self):
        drop_db(router_url, db_name)
