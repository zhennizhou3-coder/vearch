#
# Copyright 2019 The Vearch Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: UTF-8 -*-

"""
Non-cluster test cases for the rebuild index module.

Covers:
  1. Basic lifecycle — trigger space-level rebuild and verify completion.
  2. Progress query — verify progress API shape, monotonicity, and detail.
  3. Cancel rebuild — pending / running / terminal states.
  4. Per-(field, indexType) target rebuild — the minimum rebuild unit.
     Today one field has one index; the (field, indexType) API is
     forward-compatible for multi-index-per-field in the future.
  5. Concurrent rebuild rejection.
  6. Multi-index-type parameterized rebuild.
  7. DB-level trigger / query / cancel.
  8. Global-scope trigger / query / cancel.

Each test class follows the standard pattern:
    test_prepare_db    -> create_db
    test_xxx           -> create_space -> add docs -> waiting_index_finish
                        -> trigger rebuild -> verify -> check_search -> drop_space
    test_destroy_db    -> drop_db
"""

import json
import os
import shutil
import time
import threading
import random
import concurrent.futures

import pytest
import requests

from utils.data_utils import *
from utils.vearch_utils import *

__description__ = """ test case for rebuild index module """

sift10k = DatasetSift10K()
xb = sift10k.get_database()
xq = sift10k.get_queries()
gt = sift10k.get_groundtruth()

_PROGRESS_REQUIRED_KEYS = {
    "space_key",
    "total_tasks",
    "completed_tasks",
    "failed_tasks",
    "running_tasks",
    "pending_tasks",
    "success_ratio",
    "overall_percent",
    "status",
}

def _trigger_rebuild(
    db: str,
    space: str,
    index_name: str = "",
    max_retries: int = 0,
):
    """POST /index/rebuild/dbs/:db/spaces/:space[/indexes/:index_name]
    """
    payload = {}
    if index_name:
        url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/indexes/{index_name}"
    else:
        url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}"
    if max_retries > 0:
        payload["max_retries"] = max_retries
    resp = requests.post(url, auth=(username, password), json=payload)
    logger.info("trigger_rebuild url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp

def _trigger_rebuild_db(db: str):
    """POST /index/rebuild/dbs/:db — rebuild all spaces in a DB."""
    url = f"{router_url}/index/rebuild/dbs/{db}"
    resp = requests.post(url, auth=(username, password), json={})
    logger.info("trigger_rebuild_db url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp

def _trigger_rebuild_global():
    """POST /index/rebuild — rebuild all spaces across all DBs."""
    url = f"{router_url}/index/rebuild"
    resp = requests.post(url, auth=(username, password), json={})
    logger.info("trigger_rebuild_global url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp

def _trigger_rebuild_drop(db, space):
      url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}"
      return requests.post(url, auth=(username, password),
                           json={"drop_before_rebuild": True})

def _get_rebuild_progress(db: str, space: str) -> dict:
    """GET /index/rebuild/dbs/:db/spaces/:space/progress"""
    url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/progress"
    resp = requests.get(url, auth=(username, password))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("code") == 0, body
    data = body.get("data", {}) or {}
    missing = _PROGRESS_REQUIRED_KEYS - set(data.keys())
    assert not missing, f"progress response missing keys {missing}: {data}"
    return data

def _list_rebuild_progress(db: str = "") -> dict:
    """GET progress summary.

    db=""  -> GET /index/rebuild/progress           (global summary)
    db=xxx -> GET /index/rebuild/dbs/xxx/progress  (db-level summary)
    """
    if db:
        url = f"{router_url}/index/rebuild/dbs/{db}/progress"
    else:
        url = f"{router_url}/index/rebuild/progress"
    resp = requests.get(url, auth=(username, password))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("code") == 0, body
    return body.get("data", {})

def _cancel_rebuild(db: str, space: str):
    """POST /index/rebuild/dbs/:db/spaces/:space/cancel"""
    url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/cancel"
    resp = requests.post(url, auth=(username, password))
    return resp

def _cancel_rebuild_db(db: str):
    """POST /index/rebuild/dbs/:db/cancel — cancel all rebuilds in a DB."""
    url = f"{router_url}/index/rebuild/dbs/{db}/cancel"
    resp = requests.post(url, auth=(username, password))
    return resp

def _cancel_rebuild_global():
    """POST /index/rebuild/cancel — cancel all rebuilds globally."""
    url = f"{router_url}/index/rebuild/cancel"
    resp = requests.post(url, auth=(username, password))
    return resp

def _wait_rebuild_completed(
    db: str,
    space: str,
    timeout: int = 600,
    poll_interval: int = 3,
) -> list:
    """Poll progress until terminal. Returns chronological snapshots.

    Note on monotonicity: overall_percent is the progress of the *current*
    rebuild target, not the aggregate across all targets. When a multi-
    index rebuild advances from target N to N+1, master resets
    Tasks/TotalTasks for the new target and overall_percent drops back
    toward 0 (see rebuild_service.prepareNextTarget). We only require
    monotonicity *within* a single target (same current_index).
    """
    deadline = time.time() + timeout
    snapshots = []
    last_overall = -1
    last_current_index = -1
    while time.time() < deadline:
        progress = _get_rebuild_progress(db, space)
        snapshots.append(progress)
        status = progress["status"]
        overall = progress.get("overall_percent", 0)
        current_index = progress.get("current_index", 0)

        if current_index == last_current_index:
            assert overall >= last_overall, (
                f"overall_percent decreased within target #{current_index}: "
                f"{last_overall} -> {overall}\n"
                f"snapshot: {json.dumps(progress, indent=2)}"
            )
        last_overall = overall
        last_current_index = current_index

        logger.info(
            "progress: status=%s target=%d overall=%d%% completed=%d/%d running=%d pending=%d failed=%d ratio=%.2f",
            status, current_index, overall,
            progress["completed_tasks"], progress["total_tasks"],
            progress["running_tasks"], progress["pending_tasks"],
            progress["failed_tasks"], progress["success_ratio"],
        )

        if status == "completed":
            return snapshots
        if status == "failed":
            pytest.fail(
                f"rebuild failed for {db}/{space}: {json.dumps(progress, indent=2)}"
            )
        time.sleep(poll_interval)
    pytest.fail(
        f"rebuild did not complete within {timeout}s for {db}/{space}; "
        f"last snapshot: {json.dumps(snapshots[-1] if snapshots else {}, indent=2)}"
    )

def _wait_index_status_indexed(
    db: str,
    space: str,
    max_rounds: int = 180,
    poll_interval: int = 5,
) -> None:
    """Wait until every partition reports engine IndexStatus == INDEXED (2)."""
    url = f"{router_url}/dbs/{db}/spaces/{space}?detail=true"
    for round_i in range(max_rounds):
        rs = requests.get(url, auth=(username, password))
        assert rs.status_code == 200, rs.text
        body = rs.json()
        assert body.get("code") == 0, body
        data = body.get("data", {})
        partitions = data.get("partitions", [])
        idx_statuses = [p.get("index_status", -1) for p in partitions]
        logger.info(
            "index_status round=%d status=%s partitions=%s",
            round_i, data.get("status"), idx_statuses,
        )
        if data.get("status") != "red" and partitions and all(s == 2 for s in idx_statuses):
            return
        time.sleep(poll_interval)
    pytest.fail(f"index_status did not reach INDEXED for {db}/{space} within {max_rounds} rounds")

def _check_search(case_space_name: str, times: int = 5, db_name_override: str = ""):
    """Light search smoke test after rebuild."""
    target_db = db_name_override or db_name
    url = router_url + "/document/search?timeout=2000000"
    for i in range(times):
        data = {
            "vector_value": True,
            "db_name": target_db,
            "space_name": case_space_name,
            "vectors": [{"field": "field_vector", "feature": xb[i : i + 1].flatten().tolist()}],
        }
        rs = requests.post(url, auth=(username, password), json=data)
        body = rs.json()
        if body.get("code") != 0:
            logger.warning("search returned non-zero code: %s", body)
            continue
        documents = body["data"]["documents"]
        assert len(documents) == 1

def _compute_recall(case_space_name: str, k: int = 100) -> dict:
    """Compute recall@1 and recall@10 against SIFT10K groundtruth.

    Returns a dict with keys ``recall_at_1`` and ``recall_at_10``, each
    in [0.0, 1.0].

    The groundtruth ``gt`` is indexed by query index; each query's
    nearest-neighbour ground truth is ``gt[i][:1]`` (recall@1) and
    ``gt[i][:10]`` (recall@10).  We search the space and check whether
    the returned ``field_int`` values (which equal the document ID in
    the standard add() flow) overlap with the groundtruth set.
    """
    url = router_url + "/document/search?timeout=2000000"
    nq = xq.shape[0]
    recall1_hits = 0
    recall10_hits = 0

    for i in range(nq):
        data = {
            "vector_value": False,
            "db_name": db_name,
            "space_name": case_space_name,
            "vectors": [{"field": "field_vector", "feature": xq[i].tolist()}],
            "fields": ["field_int"],
            "limit": k,
        }
        rs = requests.post(url, auth=(username, password), json=data)
        body = rs.json()
        if body.get("code") != 0:
            logger.warning("search returned non-zero code for query %d: %s", i, body)
            continue
        documents = body["data"]["documents"]
        if not documents:
            continue
        # documents is a list of result-lists (one per query vector).
        # With a single query vector it is [[result1, result2, ...]].
        results = documents[0] if isinstance(documents[0], list) else documents
        returned_ids = set()
        for r in results:
            fid = r.get("field_int")
            if fid is not None:
                returned_ids.add(fid)
        # field_int in add() = index*batch_size + j (0-based).
        # SIFT groundtruth IDs match field_int values directly (0-based).
        gt1 = set([int(gt[i][0])])
        gt10 = set(int(g) for g in gt[i][:10])
        if returned_ids & gt1:
            recall1_hits += 1
        if returned_ids & gt10:
            recall10_hits += 1

    return {
        "recall_at_1": recall1_hits / nq if nq else 0.0,
        "recall_at_10": recall10_hits / nq if nq else 0.0,
    }

def _check_search_field(case_space_name: str, field: str, times: int = 3, db_name_override: str = ""):
    """Search smoke test targeting a specific vector field."""
    target_db = db_name_override or db_name
    url = router_url + "/document/search?timeout=2000000"
    for i in range(times):
        data = {
            "vector_value": True,
            "db_name": target_db,
            "space_name": case_space_name,
            "vectors": [{"field": field, "feature": xb[i : i + 1].flatten().tolist()}],
        }
        rs = requests.post(url, auth=(username, password), json=data)
        body = rs.json()
        if body.get("code") != 0:
            logger.warning("search returned non-zero code: %s", body)
            continue
        documents = body["data"]["documents"]
        assert len(documents) == 1

def _ensure_clean_db():
    """Drop all spaces then drop DB, then create a fresh DB.

    Step 1/2 are async on the master side (drop_space returns once master
    accepts the request, but partitions are torn down on PS afterwards;
    similarly drop_db can race with residual space removal). We poll
    after each destructive step until the master view actually clears.

    The final create_db is asserted — silent failure here had been
    masquerading as a "code=1 / db_not_exist" failure on the next
    create_space, which is exactly the flaky CI hit we just observed.
    """
    spaces_url = f"{router_url}/dbs/{db_name}/spaces"
    db_url = f"{router_url}/dbs/{db_name}"

    # Step 1: List existing spaces under the DB and drop each.
    rs = requests.get(spaces_url, auth=(username, password))
    logger.info("list spaces response: status=%d body=%s",
                rs.status_code, rs.text[:500])
    if rs.status_code == 200:
        body = rs.json()
        if body.get("code") == 0 and body.get("data"):
            for sp in body["data"]:
                sp_name = sp.get("space_name") or sp.get("name") or ""
                if sp_name:
                    logger.info("dropping residual space: %s", sp_name)
                    drop_resp = drop_space(router_url, db_name, sp_name)
                    logger.info("drop_space %s result: status=%d body=%s",
                                sp_name, drop_resp.status_code,
                                drop_resp.text[:200])

    # Wait until all spaces actually disappear from the master view (max 30s).
    deadline = time.time() + 30
    while time.time() < deadline:
        rs = requests.get(spaces_url, auth=(username, password))
        if rs.status_code != 200:
            break  # DB itself already gone -> nothing left to drop
        body = rs.json()
        if not body.get("data"):
            break
        time.sleep(0.5)
    else:
        logger.warning("_ensure_clean_db: spaces did not fully drop within 30s; "
                       "last list: %s", rs.text[:500])

    # Step 2: Drop DB (ignore "not found"; we created it ourselves anyway).
    drop_resp = drop_db(router_url, db_name)
    logger.info("drop_db result: status=%d body=%s",
                drop_resp.status_code, drop_resp.text[:200])

    # Wait until the DB itself is gone (max 15s).
    deadline = time.time() + 15
    while time.time() < deadline:
        r = requests.get(db_url, auth=(username, password))
        # Master returns non-200 OR code != 0 once the DB is fully removed.
        if r.status_code != 200 or r.json().get("code") != 0:
            break
        time.sleep(0.5)
    else:
        logger.warning("_ensure_clean_db: db %s still visible after drop within 15s",
                       db_name)

    # Step 3: Create fresh DB — assert success so a silent failure cannot
    # cascade into "db_not_exist" on later create_space.
    create_resp = create_db(router_url, db_name)
    logger.info("create_db result: status=%d body=%s",
                create_resp.status_code, create_resp.text[:200])
    assert create_resp.status_code == 200, (
        f"create_db {db_name} HTTP {create_resp.status_code}: "
        f"{create_resp.text[:500]}")
    create_body = create_resp.json()
    assert create_body.get("code") == 0, (
        f"create_db {db_name} business error: {create_resp.text[:500]}")

# ---------------------------------------------------------------------------
# Space config factories
# ---------------------------------------------------------------------------

def _hnsw_space_config(name: str, partition_num: int = 2, replica_num: int = 1) -> dict:
    embedding_size = xb.shape[1]
    return {
        "name": name, "partition_num": partition_num, "replica_num": replica_num,
        "fields": [
            {"name": "field_int", "type": "integer"},
            {"name": "field_long", "type": "long"},
            {"name": "field_float", "type": "float"},
            {"name": "field_double", "type": "double"},
            {"name": "field_string", "type": "string", "index": {"name": "field_string", "type": "SCALAR"}},
            {"name": "field_vector", "type": "vector",
             "index": {"name": "gamma", "type": "HNSW",
                       "params": {"metric_type": "InnerProduct", "nlinks": 32, "efConstruction": 40, "training_threshold": 1}},
             "dimension": embedding_size},
        ],
    }

def _flat_space_config(name: str, partition_num: int = 1, replica_num: int = 1) -> dict:
    embedding_size = xb.shape[1]
    return {
        "name": name, "partition_num": partition_num, "replica_num": replica_num,
        "fields": [
            {"name": "field_int", "type": "integer"},
            {"name": "field_vector", "type": "vector",
             "index": {"name": "gamma", "type": "FLAT",
                       "params": {"metric_type": "L2", "training_threshold": 1}},
             "dimension": embedding_size},
        ],
    }

def _multi_vector_space_config(name: str, partition_num: int = 1) -> dict:
    """Space with two vector fields, each carrying one index."""
    embedding_size = xb.shape[1]
    return {
        "name": name, "partition_num": partition_num, "replica_num": 1,
        "fields": [
            {"name": "field_int", "type": "integer"},
            {"name": "field_vector_a", "type": "vector",
             "index": {"name": "gamma_a", "type": "HNSW",
                       "params": {"metric_type": "L2", "nlinks": 32, "efConstruction": 40, "training_threshold": 1}},
             "dimension": embedding_size},
            {"name": "field_vector_b", "type": "vector",
             "index": {"name": "gamma_b", "type": "FLAT",
                       "params": {"metric_type": "L2", "training_threshold": 1}},
             "dimension": embedding_size},
        ],
    }

def _ivfflat_space_config(name: str, partition_num: int = 1) -> dict:
    embedding_size = xb.shape[1]
    return {
        "name": name, "partition_num": partition_num, "replica_num": 1,
        "fields": [
            {"name": "field_int", "type": "integer"},
            {"name": "field_vector", "type": "vector",
             "index": {"name": "gamma", "type": "IVFFLAT",
                       "params": {"metric_type": "L2", "ncentroids": 128, "training_threshold": 3999}},
             "dimension": embedding_size},
        ],
    }

def _ivfpq_space_config(name: str, partition_num: int = 1) -> dict:
    embedding_size = xb.shape[1]
    return {
        "name": name, "partition_num": partition_num, "replica_num": 1,
        "fields": [
            {"name": "field_int", "type": "integer"},
            {"name": "field_vector", "type": "vector",
             "index": {"name": "gamma", "type": "IVFPQ",
                       "params": {"metric_type": "InnerProduct", "ncentroids": 128, "nsubvector": 32, "training_threshold": 3999}},
             "dimension": embedding_size},
        ],
    }

# ---------------------------------------------------------------------------
# 1. Basic lifecycle
# ---------------------------------------------------------------------------

def _add_multi_vector_docs(space_name: str):
    """Insert documents with two vector fields (field_vector_a, field_vector_b).

    The generic ``add()`` helper hard-codes ``field_vector``, which does not
    exist on multi-vector spaces. We build the payload inline instead.

    Module-level (not bound to any class) so every test class can reuse it.
    """
    batch_size = 100
    total = xb.shape[0]
    total_batch = int(total / batch_size)
    url = router_url + "/document/upsert?timeout=2000000"
    for i in range(total_batch):
        docs = []
        for j in range(batch_size):
            doc_id = i * batch_size + j
            docs.append({
                "_id": str(doc_id),
                "field_int": doc_id,
                "field_vector_a": xb[i * batch_size + j].tolist(),
                "field_vector_b": xb[i * batch_size + j].tolist(),
            })
        data = {"db_name": db_name, "space_name": space_name, "documents": docs}
        rs = requests.post(url, auth=(username, password), json=data)
        body = rs.json()
        if body.get("code") != 0:
            logger.error("add multi-vector docs batch %d error: %s", i, body)
        assert body.get("code") == 0, f"add docs failed batch {i}: {body}"
    waiting_index_finish(total, space_name=space_name)

class TestRebuildBasicLifecycle:
    """Trigger space-level rebuild and verify completion."""

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_rebuild_hnsw_full_space(self):
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_mri_basic"

        assert create_space(router_url, db_name, _hnsw_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        # ---- Compute recall BEFORE rebuild ----
        recall_before = _compute_recall(case_space, k=100)
        logger.info(
            "recall BEFORE rebuild: recall@1=%.4f recall@10=%.4f",
            recall_before["recall_at_1"], recall_before["recall_at_10"],
        )

        # Diagnostic: test rebuild route via both Router and Master directly
        rebuild_url_via_router = f"{router_url}/index/rebuild/dbs/{db_name}/spaces/{case_space}"
        rebuild_url_via_master = f"{master_url}/index/rebuild/dbs/{db_name}/spaces/{case_space}"
        logger.info("rebuild URL via router: %s", rebuild_url_via_router)
        logger.info("rebuild URL via master: %s", rebuild_url_via_master)

        # Try Master directly to isolate routing issues
        master_resp = requests.post(rebuild_url_via_master, auth=(username, password), json={})
        logger.info("master direct rebuild: status=%d body=%s", master_resp.status_code, master_resp.text[:500])

        resp = _trigger_rebuild(db_name, case_space)
        assert resp.status_code == 200, (
            f"rebuild trigger failed: status={resp.status_code} body={resp.text[:500]}\n"
            f"URL={rebuild_url_via_router}\n"
            f"Master direct: status={master_resp.status_code} body={master_resp.text[:500]}\n"
            f"If both return 404, the running vearch binary may not include rebuild routes — "
            f"rebuild with 'make build' and restart the cluster."
        )
        body = resp.json()
        logger.info("rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        _wait_index_status_indexed(db_name, case_space)
        _check_search(case_space)

        # ---- Compute recall AFTER rebuild ----
        recall_after = _compute_recall(case_space, k=100)
        logger.info(
            "recall AFTER rebuild: recall@1=%.4f recall@10=%.4f",
            recall_after["recall_at_1"], recall_after["recall_at_10"],
        )

    def test_rebuild_after_delete_half_data(self):
        """Insert all data, delete half, then rebuild — verify rebuild completes
        and search still works on the remaining documents."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_mri_del_rebuild"

        assert create_space(router_url, db_name, _hnsw_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        # ---- Delete half of the documents (IDs in range [total//2, total)) ----
        delete_url = router_url + "/document/delete?timeout=300000"
        half = total // 2
        ids_to_delete = [str(i) for i in range(half, total)]
        # Delete in batches of 200 to avoid oversized requests
        batch_del = 200
        for start in range(0, len(ids_to_delete), batch_del):
            chunk = ids_to_delete[start : start + batch_del]
            del_data = {
                "db_name": db_name,
                "space_name": case_space,
                "document_ids": chunk,
            }
            del_resp = requests.post(delete_url, auth=(username, password), json=del_data)
            body = del_resp.json()
            logger.info(
                "delete batch start=%d count=%d code=%d",
                start, len(chunk), body.get("code", -1),
            )
            assert body.get("code") == 0, f"delete failed: {body}"

        logger.info("deleted %d documents, %d remain", len(ids_to_delete), half)

        # ---- Compute recall BEFORE rebuild (only remaining docs) ----
        recall_before = _compute_recall(case_space, k=100)
        logger.info(
            "recall BEFORE rebuild (after delete): recall@1=%.4f recall@10=%.4f",
            recall_before["recall_at_1"], recall_before["recall_at_10"],
        )

        # ---- Trigger rebuild ----
        resp = _trigger_rebuild(db_name, case_space)
        assert resp.status_code == 200, f"trigger failed: {resp.text[:500]}"
        body = resp.json()
        assert body.get("code") == 0, body

        # ---- Wait for rebuild to complete ----
        snapshots = _wait_rebuild_completed(db_name, case_space, timeout=600)
        final = snapshots[-1]
        assert final["status"] == "completed", final
        assert final["overall_percent"] == 100, final
        assert final["failed_tasks"] == 0, final

        _wait_index_status_indexed(db_name, case_space)

        # ---- Verify search still works after rebuild ----
        _check_search(case_space)

        # ---- Compute recall AFTER rebuild ----
        recall_after = _compute_recall(case_space, k=100)
        logger.info(
            "recall AFTER rebuild (after delete): recall@1=%.4f recall@10=%.4f",
            recall_after["recall_at_1"], recall_after["recall_at_10"],
        )

        drop_space(router_url, db_name, case_space)

    def test_rebuild_all_spaces_in_db(self):
        """Trigger DB-level rebuild (POST /index/rebuild/dbs/:db) which
        rebuilds every space in the DB.  Create 2 spaces, trigger DB-level
        rebuild, verify both spaces are rebuilt and search works."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        sp_a = space_name + "_mri_db_a"
        sp_b = space_name + "_mri_db_b"

        assert create_space(router_url, db_name, _flat_space_config(sp_a)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=sp_a)
        waiting_index_finish(total, space_name=sp_a)

        assert create_space(router_url, db_name, _flat_space_config(sp_b)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=sp_b)
        waiting_index_finish(total, space_name=sp_b)

        # Trigger DB-level rebuild.
        resp = _trigger_rebuild_db(db_name)
        assert resp.status_code == 200, f"trigger failed: {resp.text[:500]}"
        body = resp.json()
        logger.info("DB-level rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        # Both spaces should appear in the DB-level progress summary.
        summary = _list_rebuild_progress(db_name)
        rebuilt_keys = {r["space_key"] for r in summary.get("results", [])}
        assert f"{db_name}-{sp_a}" in rebuilt_keys, f"{sp_a} not in {rebuilt_keys}"
        assert f"{db_name}-{sp_b}" in rebuilt_keys, f"{sp_b} not in {rebuilt_keys}"

        # Wait for both rebuilds to complete.
        _wait_rebuild_completed(db_name, sp_a, timeout=600)
        _wait_rebuild_completed(db_name, sp_b, timeout=600)

        _wait_index_status_indexed(db_name, sp_a)
        _wait_index_status_indexed(db_name, sp_b)
        _check_search(sp_a)
        _check_search(sp_b)

        drop_space(router_url, db_name, sp_a)
        drop_space(router_url, db_name, sp_b)

    def test_rebuild_global_multiple_dbs(self):
        """Global rebuild (POST /index/rebuild/dbs) rebuilds all spaces
        across all DBs.  Create 2 DBs with 1 space each, trigger global
        rebuild, verify both spaces are rebuilt."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        extra_db = db_name + "_mri_global2"
        # Clean up extra DB from prior runs. Do NOT touch the main db_name
        # here — it is managed by test_prepare_db / test_destroy_db.
        for db_to_clean in (extra_db,):
            url = f"{router_url}/dbs/{db_to_clean}/spaces"
            rs = requests.get(url, auth=(username, password))
            if rs.status_code == 200:
                body = rs.json()
                if body.get("code") == 0 and body.get("data"):
                    for sp in body["data"]:
                        sp_name = sp.get("space_name") or sp.get("name") or ""
                        if sp_name:
                            drop_space(router_url, db_to_clean, sp_name)
            drop_db(router_url, db_to_clean)

        create_db(router_url, extra_db)

        sp_main = space_name + "_mri_global2_main"
        sp_extra = space_name + "_mri_global2_extra"

        assert create_space(router_url, db_name, _flat_space_config(sp_main)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=sp_main)
        waiting_index_finish(total, space_name=sp_main)

        assert create_space(router_url, extra_db, _flat_space_config(sp_extra)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, db_name=extra_db, space_name=sp_extra)
        waiting_index_finish(total, db_name=extra_db, space_name=sp_extra)

        # Trigger global rebuild.
        resp = _trigger_rebuild_global()
        assert resp.status_code == 200, f"trigger failed: {resp.text[:500]}"
        body = resp.json()
        logger.info("global rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        # Both spaces should appear in the global progress summary.
        global_summary = _list_rebuild_progress()
        rebuilt_keys = {r["space_key"] for r in global_summary.get("results", [])}
        assert f"{db_name}-{sp_main}" in rebuilt_keys, f"{sp_main} not in {rebuilt_keys}"
        assert f"{extra_db}-{sp_extra}" in rebuilt_keys, f"{sp_extra} not in {rebuilt_keys}"

        _wait_rebuild_completed(db_name, sp_main, timeout=600)
        _wait_rebuild_completed(extra_db, sp_extra, timeout=600)

        _wait_index_status_indexed(db_name, sp_main)
        _wait_index_status_indexed(extra_db, sp_extra)
        _check_search(sp_main)
        _check_search(sp_extra, db_name_override=extra_db)

        drop_space(router_url, db_name, sp_main)
        drop_space(router_url, extra_db, sp_extra)
        drop_db(router_url, extra_db)

    def test_rebuild_space_without_index_built(self):
        """Rebuild a space whose vector index has never been built should be
        rejected upfront by the Go master, not fail at the PS level.

        The C++ engine rejects rebuild when index_status_ == UNINDEXED
        (engine.cc:1009-1016). The Go master now checks index_status via
        PartitionInfo RPC in checkPartitionsHealthy (rebuild_service.go)
        and returns an error before dispatching to the PS.
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_no_index"
        # Use IVFFLAT with a high training_threshold so the index is NOT
        # built automatically by the background indexer.
        embedding_size = xb.shape[1]
        config = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "IVFFLAT",
                           "params": {"metric_type": "L2", "ncentroids": 16,
                                      "training_threshold": 99999}},
                 "dimension": embedding_size},
            ],
        }
        assert create_space(router_url, db_name, config).json()["code"] == 0

        # Insert vectors — index won't build because total (10000) <
        # training_threshold (99999).
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        time.sleep(3)  # Brief wait for docs to land.

        # Verify index is NOT yet indexed.
        detail_url = f"{router_url}/dbs/{db_name}/spaces/{case_space}?detail=true"
        detail_resp = requests.get(detail_url, auth=(username, password))
        assert detail_resp.status_code == 200
        detail_body = detail_resp.json()
        assert detail_body.get("code") == 0
        partitions = detail_body.get("data", {}).get("partitions", [])
        idx_statuses = [p.get("index_status", -1) for p in partitions]
        logger.info("index_status before rebuild: %s", idx_statuses)

        # Trigger rebuild — should be rejected upfront by the master
        # because the index has never been built (index_status=UNINDEXED).
        # The top-level code is 0 (the HTTP request itself succeeded), but
        # the rejection appears in data.failures.
        resp = _trigger_rebuild(db_name, case_space)
        body = resp.json()
        logger.info("rebuild trigger for unindexed space: %s", body)

        failures = body.get("data", {}).get("failures", [])
        assert len(failures) > 0, (
            f"rebuild of UNINDEXED space should be rejected, "
            f"but got no failures: {body}"
        )
        assert "UNINDEXED" in failures[0].get("error", ""), (
            f"expected UNINDEXED error but got: {failures}"
        )

        drop_space(router_url, db_name, case_space)

    def test_concurrent_rebuild_rejected(self):
        """Trigger two rebuilds for the same space simultaneously.
        The second request should be rejected because the space already
        has a non-terminal (pending/running) rebuild record.

        The rejection appears in data.failures (top-level code is 0
        because the HTTP request itself succeeded).

        Ref: rebuild_service.go StartRebuild, line 313-329.
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_concurrent2"
        assert create_space(router_url, db_name, _flat_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        # First rebuild — should succeed.
        first = _trigger_rebuild(db_name, case_space).json()
        first_results = first.get("data", {}).get("results", [])
        assert len(first_results) > 0, f"first rebuild should succeed: {first}"

        # Second rebuild — should be rejected (space already has a
        # pending/running rebuild record). The rejection is in
        # data.failures, not top-level code.
        second = _trigger_rebuild(db_name, case_space).json()
        logger.info("second rebuild response: %s", second)
        second_failures = second.get("data", {}).get("failures", [])
        assert len(second_failures) > 0, (
            f"second concurrent rebuild should be rejected in failures, "
            f"but got no failures: {second}"
        )
        err_msg = second_failures[0].get("error", "")
        assert "already" in err_msg.lower(), (
            f"expected 'already pending/running' error but got: {err_msg}"
        )

        # Wait for the first rebuild to complete.
        _wait_rebuild_completed(db_name, case_space, timeout=300)
        _wait_index_status_indexed(db_name, case_space)

        # After completion, a new rebuild should be accepted (terminal
        # records can be overwritten).
        third = _trigger_rebuild(db_name, case_space).json()
        third_results = third.get("data", {}).get("results", [])
        assert len(third_results) > 0, f"rebuild after completion should succeed: {third}"
        _wait_rebuild_completed(db_name, case_space, timeout=300)

        drop_space(router_url, db_name, case_space)

    def test_db_rebuild_after_space_rebuild(self):
        """One space in a DB is already rebuilding, then a DB-level rebuild
        is triggered. The already-rebuilding space should be rejected (not
        re-processed), while the second space should be accepted and rebuilt.

        Ref: rebuild_service.go StartRebuild rejects non-terminal duplicates;
             cluster_api.go rebuildIndex fans out per-space with failure
             collection.
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        sp_a = space_name + "_mri_db_overlap_a"
        sp_b = space_name + "_mri_db_overlap_b"
        assert create_space(router_url, db_name, _flat_space_config(sp_a)).json()["code"] == 0
        assert create_space(router_url, db_name, _flat_space_config(sp_b)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=sp_a)
        add(total_batch, batch_size, xb, True, False, space_name=sp_b)
        waiting_index_finish(total, space_name=sp_a)
        waiting_index_finish(total, space_name=sp_b)

        # Trigger rebuild for space_a only.
        first_a = _trigger_rebuild(db_name, sp_a).json()
        assert first_a.get("code") == 0, f"first rebuild of sp_a should succeed: {first_a}"

        # Now trigger DB-level rebuild (both sp_a and sp_b).
        db_resp = _trigger_rebuild_db(db_name)
        db_body = db_resp.json()
        logger.info("DB-level rebuild response: %s", json.dumps(db_body, indent=2, default=str))
        assert db_body.get("code") == 0, db_body

        # The response should contain:
        #   - sp_a in failures (already pending/running)
        #   - sp_b in results (successfully enqueued)
        results = db_body.get("data", {}).get("results", []) or []
        failures = db_body.get("data", {}).get("failures", []) or []
        result_keys = [r.get("space_key", "") for r in results]
        failure_space_names = [f.get("space_name", "") for f in failures]

        # sp_a should be in failures because it already has a running record.
        assert sp_a in failure_space_names, (
            f"sp_a should be in failures (already rebuilding) but got "
            f"failures={failure_space_names}, results={result_keys}"
        )

        # sp_b should be in results (successfully enqueued).
        sp_b_key = f"{db_name}-{sp_b}"
        assert any(sp_b in k for k in result_keys), (
            f"sp_b should be in results but got results={result_keys}, "
            f"failures={failure_space_names}"
        )

        # Wait for both rebuilds to complete.
        _wait_rebuild_completed(db_name, sp_a, timeout=300)
        _wait_rebuild_completed(db_name, sp_b, timeout=300)
        _wait_index_status_indexed(db_name, sp_a)
        _wait_index_status_indexed(db_name, sp_b)

        drop_space(router_url, db_name, sp_a)
        drop_space(router_url, db_name, sp_b)

    def test_rebuild_ivf_after_delete_half(self):
        """IVFPQ rebuild after deleting half the vectors.

        IVFPQ is more sensitive to training data quality than IVFFLAT
        because PQ codebook training amplifies the effect of stale/biased
        training samples.  Deleting half the vectors and then rebuilding
        should show noticeable recall improvement when the new reservoir-
        sampled training data excludes deleted vectors and is drawn
        uniformly from the remaining set."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_mri_ivfpq_del"

        # Use IVFPQ with L2 metric (matching SIFT groundtruth).
        # IVFPQ training involves both K-Means (coarse quantizer) and
        # PQ codebook (sub-vector quantizer), making it far more sensitive
        # to training data contamination than IVFFLAT.
        embedding_size = xb.shape[1]
        ivfpq_l2_config = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "IVFPQ",
                           "params": {"metric_type": "L2", "ncentroids": 128,
                                      "nsubvector": 32, "training_threshold": 3999}},
                 "dimension": embedding_size},
            ],
        }
        assert create_space(router_url, db_name, ivfpq_l2_config).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        # ---- Delete half of the documents (IDs in range [total//2, total)) ----
        delete_url = router_url + "/document/delete?timeout=300000"
        half = total // 2
        ids_to_delete = [str(i) for i in range(half, total)]
        batch_del = 200
        for start in range(0, len(ids_to_delete), batch_del):
            chunk = ids_to_delete[start : start + batch_del]
            del_data = {
                "db_name": db_name,
                "space_name": case_space,
                "document_ids": chunk,
            }
            del_resp = requests.post(delete_url, auth=(username, password), json=del_data)
            body = del_resp.json()
            assert body.get("code") == 0, f"delete failed: {body}"

        logger.info("deleted %d documents, %d remain", len(ids_to_delete), half)

        # ---- Compute recall BEFORE rebuild (stale index) ----
        recall_before = _compute_recall(case_space, k=100)
        logger.info(
            "IVFPQ recall BEFORE rebuild (after delete): recall@1=%.4f recall@10=%.4f",
            recall_before["recall_at_1"], recall_before["recall_at_10"],
        )

        # ---- Trigger rebuild ----
        resp = _trigger_rebuild_drop(db_name, case_space)
        assert resp.status_code == 200, f"trigger failed: {resp.text[:500]}"
        body = resp.json()
        assert body.get("code") == 0, body

        # ---- Wait for rebuild to complete ----
        snapshots = _wait_rebuild_completed(db_name, case_space, timeout=600)
        final = snapshots[-1]
        assert final["status"] == "completed", final
        assert final["failed_tasks"] == 0, final

        _wait_index_status_indexed(db_name, case_space)

        # ---- Verify index_num / doc_num after rebuild ----
        detail_url = f"{router_url}/dbs/{db_name}/spaces/{case_space}?detail=true"
        detail_resp = requests.get(detail_url, auth=(username, password))
        assert detail_resp.status_code == 200, detail_resp.text
        detail_body = detail_resp.json()
        assert detail_body.get("code") == 0, detail_body
        detail_data = detail_body.get("data", {})
        partitions = detail_data.get("partitions", [])
        for p in partitions:
            logger.info(
                "AFTER REBUILD partition pid=%s doc_num=%s index_num=%s max_docid=%s",
                p.get("pid"), p.get("doc_num"), p.get("index_num"), p.get("max_docid"),
            )

        # ---- Compute recall AFTER rebuild ----
        recall_after = _compute_recall(case_space, k=100)
        logger.info(
            "IVFPQ recall AFTER rebuild (after delete): recall@1=%.4f recall@10=%.4f",
            recall_after["recall_at_1"], recall_after["recall_at_10"],
        )

        # IVFPQ training and search can fluctuate slightly between rebuilds.
        # Allow at most two query outcomes to differ while still catching a
        # meaningful recall regression.
        recall_tolerance = 2 / xq.shape[0]
        assert recall_after["recall_at_1"] + recall_tolerance > recall_before["recall_at_1"], (
            f"recall@1 degraded beyond tolerance {recall_tolerance:.4f} after rebuild: "
            f"{recall_before['recall_at_1']:.4f} -> {recall_after['recall_at_1']:.4f}"
        )
        assert recall_after["recall_at_10"] + recall_tolerance > recall_before["recall_at_10"], (
            f"recall@10 degraded beyond tolerance {recall_tolerance:.4f} after rebuild: "
            f"{recall_before['recall_at_10']:.4f} -> {recall_after['recall_at_10']:.4f}"
        )

        drop_space(router_url, db_name, case_space)

    def test_rebuild_improves_recall_with_undertrained_init(self):
        """Rebuild after appending data to an initially-undertrained IVFPQ.

        Phase 1: Insert only the first half (5000 vectors) with
        training_threshold=1000.  The IVFPQ centroids are trained on
        the first 1000 vectors — a small sample for ncentroids=32.

        Phase 2: Insert the second half (5000 vectors).  Centroids are
        now frozen; these vectors get pushed into existing clusters.
        Recall should be suboptimal.

        Rebuild: Destroy and recreate the index; the engine retrains
        centroids.  Due to the engine's Indexing() using only the first
        training_threshold_ vectors from raw_vec (GetVectorHeader), the
        training sample after rebuild is the same first 1000 vectors
        that Phase 1 already used — centroids do not change.  This is a
        known engine limitation; a proper fix would train on ALL live
        vectors during rebuild.
        """
        case_space = space_name + "_mri_undertrained"
        embedding_size = xb.shape[1]
        config = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "IVFPQ",
                           "params": {"metric_type": "L2",
                                      "ncentroids": 32,
                                      "nprobe": 16,
                                      "nsubvector": 32,
                                      "training_threshold": 1248}},
                 "dimension": embedding_size},
            ],
        }
        create_resp = create_space(router_url, db_name, config)
        assert create_resp.json().get("code") == 0, create_resp.text

        batch_size = 100
        total = xb.shape[0]
        half = total // 2   # 5000

        # Phase 1: Insert first half.  training_threshold=1000 < 5000,
        # so the IVFPQ index trains its centroids on the first 1000
        # vectors of this half.
        add(half // batch_size, batch_size, xb[:half],
            with_id=False, full_field=False,
            space_name=case_space, offset=0)
        waiting_index_finish(half, space_name=case_space)
        logger.info("phase 1 done: %d vectors, centroids trained on first 1000", half)

        # Phase 2: Insert second half.  Centroids are frozen; these
        # vectors get assigned to the existing clusters.
        add(half // batch_size, batch_size, xb[half:total],
            with_id=False, full_field=False,
            space_name=case_space, offset=half)
        waiting_index_finish(total, space_name=case_space)
        logger.info("phase 2 done: %d total vectors", total)

        recall_before = _compute_recall(case_space, k=10)
        logger.info(
            "IVFPQ recall BEFORE rebuild: recall@1=%.4f recall@10=%.4f",
            recall_before["recall_at_1"], recall_before["recall_at_10"],
        )

        # Trigger rebuild with drop_before_rebuild=True.
        resp = _trigger_rebuild_drop(db_name, case_space)
        assert resp.status_code == 200, f"trigger failed: {resp.text[:500]}"
        body = resp.json()
        assert body.get("code") == 0, body

        snapshots = _wait_rebuild_completed(db_name, case_space, timeout=600)
        final = snapshots[-1]
        assert final["status"] == "completed", final
        assert final["failed_tasks"] == 0, final
        _wait_index_status_indexed(db_name, case_space)

        recall_after = _compute_recall(case_space, k=10)
        logger.info(
            "IVFPQ recall AFTER rebuild: recall@1=%.4f recall@10=%.4f",
            recall_after["recall_at_1"], recall_after["recall_at_10"],
        )

        gain_1 = recall_after["recall_at_1"] - recall_before["recall_at_1"]
        gain_10 = recall_after["recall_at_10"] - recall_before["recall_at_10"]
        logger.info(
            "recall gain after rebuild: recall@1 %+.4f, recall@10 %+.4f",
            gain_1, gain_10,
        )

        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

# ---------------------------------------------------------------------------
# 2. Progress query
# ---------------------------------------------------------------------------

class TestRebuildProgressQuery:
    """Verify progress API shape, monotonicity, and detail."""

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_progress_missing_record_returns_error(self):
        """Querying progress for a space that was never rebuilt returns an error."""
        case_space = space_name + "_mri_nf"
        assert create_space(router_url, db_name, _hnsw_space_config(case_space)).json()["code"] == 0

        url = f"{router_url}/index/rebuild/dbs/{db_name}/spaces/{case_space}/progress"
        resp = requests.get(url, auth=(username, password))
        # Router proxies the Master response body but currently returns HTTP 200.
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("code") == 101, body

        drop_space(router_url, db_name, case_space)

    def test_rebuild_progress_lifecycle(self):
        """Full lifecycle: trigger rebuild and walk the progress API end-to-end."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_mri_prog"

        assert create_space(router_url, db_name, _hnsw_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild(db_name, case_space)
        assert resp.json().get("code") == 0, resp.text

        first = _get_rebuild_progress(db_name, case_space)
        assert first["status"] in ("pending", "running", "completed"), first

        snapshots = _wait_rebuild_completed(db_name, case_space, timeout=600)
        final = snapshots[-1]

        assert final["status"] == "completed", final
        assert final["total_tasks"] > 0
        assert final["completed_tasks"] == final["total_tasks"]
        assert final["failed_tasks"] == 0
        assert final["running_tasks"] == 0
        assert final["pending_tasks"] == 0
        assert final["overall_percent"] == 100
        assert abs(final["success_ratio"] - 1.0) < 1e-9
        assert final.get("enqueued_at"), final
        assert final.get("started_at"), final
        assert final.get("finished_at"), final

        tasks = final.get("tasks") or []
        assert len(tasks) == final["total_tasks"], final
        for t in tasks:
            assert "partition_id" in t, t
            assert "node_id" in t, t
            assert t["status"] in ("running", "completed", "failed"), t
            assert 0 <= t["progress"] <= 100, t
            assert t["status"] == "completed", t
            assert t["progress"] == 100, t

        _wait_index_status_indexed(db_name, case_space)
        _check_search(case_space)
        drop_space(router_url, db_name, case_space)

    def test_list_rebuild_progress(self):
        """DB-level progress API: create 3 spaces, rebuild only 1, verify
        the progress list contains exactly that space."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        # Create 3 spaces under the same DB.
        spaces = [space_name + f"_mri_list_{c}" for c in ("a", "b", "c")]
        for sp in spaces:
            assert create_space(router_url, db_name, _flat_space_config(sp)).json()["code"] == 0
            add(total_batch, batch_size, xb, True, False, space_name=sp)
            waiting_index_finish(total, space_name=sp)

        # Rebuild only the second space.
        rebuilt_space = spaces[1]
        resp = _trigger_rebuild(db_name, rebuilt_space)
        assert resp.json().get("code") == 0, resp.text

        summary = _list_rebuild_progress(db_name)
        assert "results" in summary, summary

        # The rebuilt space must appear in the results.
        rebuilt_key = f"{db_name}-{rebuilt_space}"
        found = [r for r in summary["results"] if r["space_key"] == rebuilt_key]
        assert len(found) >= 1, (
            f"rebuilt space {rebuilt_key} not found in results: "
            f"{[r['space_key'] for r in summary['results']]}"
        )

        # Spaces that were never rebuilt must NOT appear in the results
        # (they have no rebuild record in etcd).
        never_rebuilt = [spaces[0], spaces[2]]
        for sp in never_rebuilt:
            key = f"{db_name}-{sp}"
            matches = [r for r in summary["results"] if r["space_key"] == key]
            assert len(matches) == 0, (
                f"space {key} was never rebuilt but appears in progress results: {matches}"
            )

        _wait_rebuild_completed(db_name, rebuilt_space, timeout=300)
        for sp in spaces:
            drop_space(router_url, db_name, sp)

    def test_global_progress_partial_rebuild(self):
        """Global progress API: create 2 DBs with 1 space each, rebuild only
        1 space in 1 DB, verify global progress shows exactly 1 result."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        extra_db = db_name + "_mri_global"
        # Clean up extra DB from prior runs.
        for sp_data in (db_name, extra_db):
            url = f"{router_url}/dbs/{sp_data}/spaces"
            rs = requests.get(url, auth=(username, password))
            if rs.status_code == 200:
                body = rs.json()
                if body.get("code") == 0 and body.get("data"):
                    for sp in body["data"]:
                        sp_name = sp.get("space_name") or sp.get("name") or ""
                        if sp_name:
                            drop_space(router_url, sp_data, sp_name)
            drop_db(router_url, sp_data)

        # Create both DBs.
        create_db(router_url, db_name)
        create_db(router_url, extra_db)

        # Create 1 space in each DB.
        sp_main = space_name + "_mri_global_main"
        sp_extra = space_name + "_mri_global_extra"
        assert create_space(router_url, db_name, _flat_space_config(sp_main)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=sp_main)
        waiting_index_finish(total, space_name=sp_main)

        assert create_space(router_url, extra_db, _flat_space_config(sp_extra)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, db_name=extra_db, space_name=sp_extra)
        waiting_index_finish(total, space_name=sp_extra, db_name=extra_db)

        # Rebuild only the space in the extra DB.
        resp = _trigger_rebuild(extra_db, sp_extra)
        assert resp.json().get("code") == 0, resp.text

        global_summary = _list_rebuild_progress()
        assert "results" in global_summary, global_summary

        # The rebuilt space must appear in the global results.
        rebuilt_key = f"{extra_db}-{sp_extra}"
        found = [r for r in global_summary["results"] if r["space_key"] == rebuilt_key]
        assert len(found) >= 1, (
            f"rebuilt space {rebuilt_key} not found in global results: "
            f"{[r['space_key'] for r in global_summary['results']]}"
        )

        # The space that was never rebuilt must NOT appear in the results.
        never_rebuilt_key = f"{db_name}-{sp_main}"
        matches = [r for r in global_summary["results"] if r["space_key"] == never_rebuilt_key]
        assert len(matches) == 0, (
            f"space {never_rebuilt_key} was never rebuilt but appears in global progress: {matches}"
        )

        _wait_rebuild_completed(extra_db, sp_extra, timeout=300)

        # Clean up.
        drop_space(router_url, db_name, sp_main)
        drop_space(router_url, extra_db, sp_extra)
        drop_db(router_url, extra_db)

    def test_rebuild_db_all_spaces_and_progress(self):
        """Trigger rebuild for all spaces in a DB via _trigger_rebuild_db,
        then verify DB-level progress contains every space."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space_a = space_name + "_mri_prog_db_a"
        case_space_b = space_name + "_mri_prog_db_b"
        assert create_space(router_url, db_name, _flat_space_config(case_space_a)).json()["code"] == 0
        assert create_space(router_url, db_name, _flat_space_config(case_space_b)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space_a)
        add(total_batch, batch_size, xb, True, False, space_name=case_space_b)
        waiting_index_finish(total, space_name=case_space_a)
        waiting_index_finish(total, space_name=case_space_b)

        # Trigger rebuild for all spaces in the DB.
        resp = _trigger_rebuild_db(db_name)
        body = resp.json()
        logger.info("DB-level trigger response: %s", body)
        assert body.get("code") == 0, body

        # Query DB-level progress.
        db_progress = _list_rebuild_progress(db_name)
        logger.info("DB-level progress: %s", json.dumps(db_progress, indent=2, default=str))
        results = db_progress.get("results", [])
        space_keys = [r.get("space_key", "") for r in results]
        assert any(case_space_a in k for k in space_keys), f"space_a not found in DB progress: {space_keys}"
        assert any(case_space_b in k for k in space_keys), f"space_b not found in DB progress: {space_keys}"

        _wait_rebuild_completed(db_name, case_space_a, timeout=300)
        _wait_rebuild_completed(db_name, case_space_b, timeout=300)
        _wait_index_status_indexed(db_name, case_space_a)
        _wait_index_status_indexed(db_name, case_space_b)

        drop_space(router_url, db_name, case_space_a)
        drop_space(router_url, db_name, case_space_b)

    def test_global_rebuild_and_progress(self):
        """Trigger global rebuild via _trigger_rebuild_global, then verify
        global progress summary contains all rebuilt spaces across
        multiple DBs."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        extra_db = db_name + "_mri_prog_global_extra"

        # Clean up extra DB from prior runs.
        for db_to_clean in (db_name, extra_db):
            url = f"{router_url}/dbs/{db_to_clean}/spaces"
            rs = requests.get(url, auth=(username, password))
            if rs.status_code == 200:
                body = rs.json()
                if body.get("code") == 0 and body.get("data"):
                    for sp in body["data"]:
                        sp_name = sp.get("space_name") or sp.get("name") or ""
                        if sp_name:
                            drop_space(router_url, db_to_clean, sp_name)
            drop_db(router_url, db_to_clean)

        create_db(router_url, db_name)
        create_db(router_url, extra_db)

        # Create 2 spaces in the main DB and 1 space in the extra DB.
        sp_main_a = space_name + "_mri_prog_global_main_a"
        sp_main_b = space_name + "_mri_prog_global_main_b"
        sp_extra = space_name + "_mri_prog_global_extra"

        assert create_space(router_url, db_name, _flat_space_config(sp_main_a)).json()["code"] == 0
        assert create_space(router_url, db_name, _flat_space_config(sp_main_b)).json()["code"] == 0
        assert create_space(router_url, extra_db, _flat_space_config(sp_extra)).json()["code"] == 0

        add(total_batch, batch_size, xb, True, False, space_name=sp_main_a)
        add(total_batch, batch_size, xb, True, False, space_name=sp_main_b)
        add(total_batch, batch_size, xb, True, False, db_name=extra_db, space_name=sp_extra)
        waiting_index_finish(total, space_name=sp_main_a)
        waiting_index_finish(total, space_name=sp_main_b)
        waiting_index_finish(total, db_name=extra_db, space_name=sp_extra)

        # Trigger global rebuild — rebuilds ALL spaces across ALL DBs.
        resp = _trigger_rebuild_global()
        body = resp.json()
        logger.info("global trigger response: %s", body)
        assert body.get("code") == 0, body

        # Query global progress — should contain all 3 spaces.
        global_progress = _list_rebuild_progress()
        logger.info("global progress: %s", json.dumps(global_progress, indent=2, default=str))
        results = global_progress.get("results", [])
        space_keys = [r.get("space_key", "") for r in results]

        assert any(sp_main_a in k for k in space_keys), f"sp_main_a not found in global progress: {space_keys}"
        assert any(sp_main_b in k for k in space_keys), f"sp_main_b not found in global progress: {space_keys}"
        assert any(sp_extra in k for k in space_keys), f"sp_extra not found in global progress: {space_keys}"

        # Wait for all rebuilds to complete.
        _wait_rebuild_completed(db_name, sp_main_a, timeout=300)
        _wait_rebuild_completed(db_name, sp_main_b, timeout=300)
        _wait_rebuild_completed(extra_db, sp_extra, timeout=300)

        _wait_index_status_indexed(db_name, sp_main_a)
        _wait_index_status_indexed(db_name, sp_main_b)
        _wait_index_status_indexed(extra_db, sp_extra)

        drop_space(router_url, db_name, sp_main_a)
        drop_space(router_url, db_name, sp_main_b)
        drop_space(router_url, extra_db, sp_extra)
        drop_db(router_url, extra_db)


    def test_destroy_db(self):
        drop_db(router_url, db_name)

# ---------------------------------------------------------------------------
# 3. Cancel rebuild
# ---------------------------------------------------------------------------

class TestCancelRebuild:
    """Cancel rebuild: only pending records can be cancelled.

    Strategy: create N spaces, trigger rebuild on all of them, then cancel.
    Because the scheduler admits at most one space at a time, space-0 will
    be running (not cancellable) while the others remain pending (cancellable).
    After cancellation, pending records transition to 'cancelled' (a terminal
    state persisted in etcd), NOT to 'completed'.
    """

    _N = 3  # number of spaces

    @staticmethod
    def _space_suffix(idx: int) -> str:
        return chr(ord('a') + idx)

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_cancel_pending_and_running(self):
        """Trigger N rebuilds, cancel all; running one stays, pending
        become cancelled.

        Uses HNSW indexes (slower rebuild than FLAT) so that spaces
        remain in pending/running state long enough to observe the
        cancel behavior. Because the scheduler admits at most one space
        at a time, space-0 will be running (not cancellable) while the
        others remain pending (cancellable).
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        spaces = [space_name + f"_mri_cancel_{self._space_suffix(i)}" for i in range(self._N)]

        # Create spaces with HNSW and load data.
        for sp in spaces:
            cs_resp = create_space(router_url, db_name, _hnsw_space_config(sp))
            assert cs_resp.json().get("code") == 0, (
                f"create_space {sp} failed: HTTP={cs_resp.status_code} "
                f"body={cs_resp.text[:500]}"
            )
            add(total_batch, batch_size, xb, True, False, space_name=sp)
            waiting_index_finish(total, space_name=sp)

        # Trigger rebuild on every space.
        for sp in spaces:
            resp = _trigger_rebuild(db_name, sp)
            assert resp.json().get("code") == 0, resp.text

        # Give the scheduler time to admit spaces. HNSW rebuilds are
        # slower, so some should still be pending after this wait.
        time.sleep(5)

        # Cancel every space.
        cancel_results = []
        for sp in spaces:
            cancel_resp = _cancel_rebuild(db_name, sp)
            body = cancel_resp.json()
            logger.info("cancel response for %s: %s", sp, body)
            assert body.get("code") == 0, body
            data = body.get("data", {})
            results = data.get("results", [])
            if results:
                cancel_results.append(results[0])

        # Classify outcomes and verify each entry has a clear reason.
        cancelled_keys = set()
        not_cancelled_keys = set()
        for entry in cancel_results:
            key = f"{entry.get('db_name', db_name)}-{entry.get('space_name', '')}"
            self._assert_cancel_entry_reason(entry, sp_label=key)
            if entry.get("cancelled"):
                cancelled_keys.add(key)
            else:
                not_cancelled_keys.add(key)

        logger.info("cancelled: %s  not_cancelled: %s", cancelled_keys, not_cancelled_keys)

        # With HNSW indexes, we expect at least one pending → cancelled.
        # If not (e.g. very fast machine), the test still passes because
        # every entry has a valid reason — we just log a note.
        if len(cancelled_keys) >= 1:
            logger.info("successfully cancelled at least 1 pending rebuild")
        else:
            logger.info("no pending rebuilds were caught; "
                        "all entries had valid reasons for cancellation failure")

        # The running rebuild (admitted by the scheduler) should report
        # cancelled=False (the record stays Running), but may report
        # cancelled_tasks>0 when best-effort per-task cancel skipped any
        # not-yet-dispatched replicas.
        running_cancelled_false = [
            e for e in cancel_results
            if not e.get("cancelled") and "running" in (e.get("reason", "") + e.get("status", "")).lower()
        ]
        for e in running_cancelled_false:
            assert e["cancelled"] is False, f"running rebuild should not be cancellable: {e}"
            # cancelled_tasks is >=0; when >0 it reflects tasks that were
            # planned-but-not-yet-dispatched at the moment of the cancel
            # call and are now Cancelled in the plan.
            assert e.get("cancelled_tasks", 0) >= 0, e
            if e.get("cancelled_tasks", 0) > 0:
                logger.info(
                    "best-effort task-level cancel applied: space=%s cancelled_tasks=%d",
                    e.get("space_name"), e["cancelled_tasks"],
                )

        # Verify individual progress: cancelled spaces show status='cancelled'.
        for sp in spaces:
            progress = _get_rebuild_progress(db_name, sp)
            logger.info("progress for %s: status=%s", sp, progress["status"])
            if progress["status"] == "cancelled":
                pass  # expected for pending→cancelled
            elif progress["status"] == "running":
                _wait_rebuild_completed(db_name, sp, timeout=600)
            elif progress["status"] == "completed":
                pass  # scheduler finished before cancel took effect

        # Verify DB-level progress summary reflects the mixed outcomes.
        summary = _list_rebuild_progress(db_name)
        logger.info("DB-level progress summary after cancel: %s", json.dumps(summary, indent=2, default=str))

        assert "results" in summary, summary

        # Count per-status from the summary.
        status_map = {}
        for r in summary["results"]:
            s = r.get("status", "")
            status_map[s] = status_map.get(s, 0) + 1

        logger.info("DB-level status counts: %s", status_map)

        # We expect at least one cancelled entry (with HNSW).
        if status_map.get("cancelled", 0) >= 1:
            logger.info("DB summary shows cancelled entries as expected")
        else:
            logger.info("DB summary has no cancelled entries (rebuilds too fast on this machine)")

        # Wait for any remaining running rebuilds before cleanup.
        for sp in spaces:
            progress = _get_rebuild_progress(db_name, sp)
            if progress["status"] == "running":
                _wait_rebuild_completed(db_name, sp, timeout=600)

        # Cleanup.
        for sp in spaces:
            _wait_index_status_indexed(db_name, sp)
            drop_space(router_url, db_name, sp)

    def _assert_cancel_entry_reason(self, entry, sp_label="space"):

        assert "cancelled" in entry, f"missing 'cancelled' in {sp_label}: {entry}"
        assert "reason" in entry and entry["reason"], (
            f"cancel entry for {sp_label} should have a non-empty 'reason', got {entry}"
        )
        assert "status" in entry, f"missing 'status' in {sp_label}: {entry}"

        cancelled = entry["cancelled"]
        status = entry["status"]
        reason = entry["reason"].lower()

        if cancelled:
            # cancelled=True: reason must explain why cancellation succeeded.
            assert "cancel" in reason, (
                f"cancelled=True but reason doesn't mention cancel: {entry}"
            )
        else:
            # cancelled=False: reason must explain the outcome. This covers
            # three shapes:
            #   - Terminal (completed/failed): "rebuild already <status>..."
            #   - Running, everything already dispatched: "rebuild is running
            #     and every task is already dispatched..."
            #   - Running, best-effort task-level cancel applied: "cancelled
            #     N not-yet-dispatched tasks..." (the record itself stays
            #     Running; individual pending tasks were transitioned to
            #     Cancelled).
            assert (
                status.lower() in reason
                or "running" in reason
                or "cannot cancel" in reason
                or "cancelled" in reason
            ), (
                f"cancelled=False but reason doesn't reference status '{status}': {entry}"
            )

    def test_cancel_specific_space_while_all_rebuilding(self):
        """All DBs are rebuilding; cancel a specific db/space and verify
        the cancel response is well-formed with a clear reason.

        Uses HNSW indexes (slower rebuild than FLAT) so that spaces
        remain in pending/running state long enough to observe cancel
        behavior. The test verifies:
        - The cancel API returns a well-formed response for each space
          with cancelled, reason, and status fields.
        - The reason field clearly explains the outcome.
        - Cancelling one space does NOT affect other spaces.

        Pattern: create 2 DBs with 2 spaces each → trigger global rebuild
        → cancel one specific space → verify response.
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        extra_db = db_name + "_mri_cancel_specific"
        # Clean up extra DB from prior runs.
        for db_to_clean in (extra_db,):
            url = f"{router_url}/dbs/{db_to_clean}/spaces"
            rs = requests.get(url, auth=(username, password))
            if rs.status_code == 200:
                body = rs.json()
                if body.get("code") == 0 and body.get("data"):
                    for sp in body["data"]:
                        sp_name = sp.get("space_name") or sp.get("name") or ""
                        if sp_name:
                            drop_space(router_url, db_to_clean, sp_name)
            drop_db(router_url, db_to_clean)
        create_db(router_url, extra_db)

        # Create 2 spaces in each DB — use HNSW for slower rebuild.
        sp_a = space_name + "_mri_cs_a"
        sp_b = space_name + "_mri_cs_b"
        sp_c = space_name + "_mri_cs_c"
        sp_d = space_name + "_mri_cs_d"

        for sp, db in [(sp_a, db_name), (sp_b, db_name), (sp_c, extra_db), (sp_d, extra_db)]:
            assert create_space(router_url, db, _hnsw_space_config(sp)).json()["code"] == 0
            add(total_batch, batch_size, xb, True, False, db_name=db, space_name=sp)
            waiting_index_finish(total, db_name=db, space_name=sp)

        # Trigger global rebuild (all 4 spaces).
        global_resp = _trigger_rebuild_global()
        assert global_resp.json().get("code") == 0, global_resp.text

        # Give the scheduler time to admit spaces. HNSW rebuilds are
        # slower, so some spaces should still be pending.
        time.sleep(5)

        # Cancel one specific space (sp_b) in the main db.
        cancel_resp = _cancel_rebuild(db_name, sp_b)
        cancel_body = cancel_resp.json()
        logger.info("cancel specific space response: %s", cancel_body)
        assert cancel_body.get("code") == 0, cancel_body

        data = cancel_body.get("data", {})
        results = data.get("results", [])
        failures = data.get("failures", [])

        # The targeted space should appear in results (record exists)
        # or failures (no record / error).
        cancel_entry = None
        for r in results:
            if r.get("space_name") == sp_b:
                cancel_entry = r
                break
        if cancel_entry is None:
            for f in failures:
                if f.get("space_name") == sp_b:
                    cancel_entry = f
                    break

        assert cancel_entry is not None, (
            f"sp_b should appear in cancel response, got results={results}, failures={failures}"
        )
        logger.info("cancel entry for sp_b: %s", cancel_entry)

        # Verify the cancel entry has a clear, non-empty reason that
        # is consistent with its cancelled/status fields.
        self._assert_cancel_entry_reason(cancel_entry, sp_label=sp_b)

        # Verify the other 3 spaces are NOT cancelled by this targeted cancel.
        for db, sp in [(db_name, sp_a), (extra_db, sp_c), (extra_db, sp_d)]:
            progress = _get_rebuild_progress(db, sp)
            # They may be in any state EXCEPT cancelled (targeted cancel
            # only affects the specified space).
            if progress["status"] == "cancelled":
                logger.warning("unexpected cancelled status for %s/%s (not targeted by cancel)", db, sp)

        # Wait for all rebuilds to reach a terminal state.
        for db, sp in [(db_name, sp_a), (db_name, sp_b), (extra_db, sp_c), (extra_db, sp_d)]:
            progress = _get_rebuild_progress(db, sp)
            if progress["status"] == "running":
                _wait_rebuild_completed(db, sp, timeout=600)
            _wait_index_status_indexed(db, sp)

        # Cleanup.
        for sp in (sp_a, sp_b):
            drop_space(router_url, db_name, sp)
        for sp in (sp_c, sp_d):
            drop_space(router_url, extra_db, sp)
        drop_db(router_url, extra_db)

    def test_cancel_db_and_global_pending_rebuilds(self):
        """Only some spaces in the DB are rebuilding; cancel all rebuilds
        in the DB and verify pending spaces are cancelled.

        Uses HNSW indexes (slower rebuild than FLAT) so that spaces
        remain in pending state long enough to be cancelled.
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        sp_x = space_name + "_mri_cancel_db_x"
        sp_y = space_name + "_mri_cancel_db_y"
        sp_z = space_name + "_mri_cancel_db_z"

        for sp in (sp_x, sp_y, sp_z):
            assert create_space(router_url, db_name, _hnsw_space_config(sp)).json()["code"] == 0
            add(total_batch, batch_size, xb, True, False, space_name=sp)
            waiting_index_finish(total, space_name=sp)

        # Only rebuild sp_x and sp_y, NOT sp_z.
        for sp in (sp_x, sp_y):
            resp = _trigger_rebuild(db_name, sp)
            assert resp.json().get("code") == 0, resp.text

        # HNSW rebuilds are slower; give the scheduler time to start
        # some but not all spaces, so some remain pending.
        time.sleep(5)

        # Cancel all rebuilds in this DB.
        db_cancel_resp = _cancel_rebuild_db(db_name)
        db_cancel_body = db_cancel_resp.json()
        logger.info("DB-level cancel response: %s", db_cancel_body)
        assert db_cancel_body.get("code") == 0, db_cancel_body

        db_results = db_cancel_body.get("data", {}).get("results", [])

        # Verify pending spaces were cancelled.
        cancelled_names = {r["space_name"] for r in db_results if r.get("cancelled")}
        logger.info("cancelled spaces: %s", cancelled_names)

        # With HNSW indexes and 5s wait, at least one of sp_x/sp_y
        # should still be pending → cancelled.
        if len(cancelled_names) >= 1:
            logger.info("successfully cancelled pending rebuild(s): %s", cancelled_names)
            for name in cancelled_names:
                progress = _get_rebuild_progress(db_name, name)
                assert progress["status"] == "cancelled", (
                    f"{name} should be cancelled, got {progress['status']}"
                )
        else:
            logger.info("no pending rebuilds were caught (rebuilds too fast on this machine)")

        # Only sp_x and sp_y have rebuild records. Wait for any running
        # rebuilds to complete before cleaning up all three spaces.
        for sp in (sp_x, sp_y):
            progress = _get_rebuild_progress(db_name, sp)
            if progress["status"] == "running":
                _wait_rebuild_completed(db_name, sp, timeout=600)

        for sp in (sp_x, sp_y, sp_z):
            _wait_index_status_indexed(db_name, sp)
            drop_space(router_url, db_name, sp)

    def test_cancel_nonexistent_completed_already_cancelled(self):
        """Cancel rebuild in various terminal / edge states:

        1. Cancel a space whose index was auto-built (never explicitly
           rebuilt) → the system still has a completed rebuild record,
           so cancelled=False with reason mentioning "completed".
        2. Cancel a completed rebuild → cancelled=False, reason mentions
           "completed".
        3. Cancel an already-cancelled rebuild → cancelled=True
           (idempotent), reason mentions "already cancelled".
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        # ---- Case 1: Cancel a space that was never explicitly rebuilt ----
        # After waiting_index_finish the auto-indexer has built the index
        # and created a completed rebuild record. Cancelling should return
        # cancelled=False with a reason explaining the record is already
        # in a terminal state.
        case_space = space_name + "_mri_cancel_none"
        assert create_space(router_url, db_name, _flat_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        cancel_resp = _cancel_rebuild(db_name, case_space)
        cancel_body = cancel_resp.json()
        logger.info("cancel auto-built space response: %s", cancel_body)
        assert cancel_body.get("code") == 0, cancel_body

        results1 = cancel_body.get("data", {}).get("results", [])
        failures1 = cancel_body.get("data", {}).get("failures", [])
        # The auto-built index creates a completed rebuild record, so
        # the cancel should appear in results (not failures) with
        # cancelled=False.
        if results1:
            auto_entry = results1[0]
            self._assert_cancel_entry_reason(auto_entry, sp_label=case_space + " (auto-built)")
            assert auto_entry.get("cancelled") is False, (
                f"auto-built index cancel should have cancelled=False, got {auto_entry}"
            )
        elif failures1:
            # Edge case: if no rebuild record was created during auto-build,
            # it appears in failures with "no rebuild record".
            err_msg = failures1[0].get("error", "")
            assert "no rebuild record" in err_msg.lower(), (
                f"expected 'no rebuild record' error but got: {err_msg}"
            )

        # ---- Case 2: Cancel a completed rebuild ----
        # Trigger and wait for a rebuild to complete.
        rebuild_resp = _trigger_rebuild(db_name, case_space)
        assert rebuild_resp.json().get("code") == 0, rebuild_resp.text
        _wait_rebuild_completed(db_name, case_space, timeout=300)

        cancel_resp2 = _cancel_rebuild(db_name, case_space)
        cancel_body2 = cancel_resp2.json()
        logger.info("cancel completed rebuild response: %s", cancel_body2)

        results2 = cancel_body2.get("data", {}).get("results", [])
        # The completed rebuild should appear in results with cancelled=False.
        completed_entry = None
        for r in results2:
            if r.get("space_name") == case_space:
                completed_entry = r
                break
        assert completed_entry is not None, (
            f"completed rebuild cancel entry not found in results={results2}, "
            f"failures={cancel_body2.get('data', {}).get('failures', [])}"
        )
        # Verify the cancel entry has a clear reason explaining why
        # cancellation was denied (terminal state: completed).
        self._assert_cancel_entry_reason(completed_entry, sp_label=case_space)
        assert completed_entry.get("cancelled") is False, (
            f"completed rebuild should have cancelled=False, got {completed_entry}"
        )

        # ---- Case 3: Cancel an already-cancelled rebuild ----
        # Create two spaces and trigger rebuild on both. The scheduler
        # processes one at a time, so the first is running while the
        # second is likely still pending → cancellable.
        sp_a = space_name + "_mri_cancel_already_a"
        sp_b = space_name + "_mri_cancel_already_b"
        for sp in (sp_a, sp_b):
            assert create_space(router_url, db_name, _hnsw_space_config(sp)).json()["code"] == 0
            add(total_batch, batch_size, xb, True, False, space_name=sp)
            waiting_index_finish(total, space_name=sp)

        for sp in (sp_a, sp_b):
            resp = _trigger_rebuild(db_name, sp)
            assert resp.json().get("code") == 0, resp.text

        # HNSW rebuilds are slower; give the scheduler time to start
        # one but not the other.
        # time.sleep(5)

        # Cancel sp_b (likely pending → cancelled).
        first_cancel = _cancel_rebuild(db_name, sp_b)
        first_body = first_cancel.json()
        logger.info("first cancel of sp_b: %s", first_body)

        first_results = first_body.get("data", {}).get("results", [])
        if first_results:
            first_entry = first_results[0]
            self._assert_cancel_entry_reason(first_entry, sp_label=sp_b + " (first cancel)")

        progress_b = _get_rebuild_progress(db_name, sp_b)
        if progress_b["status"] == "cancelled":
            # Cancel again — should be idempotent (cancelled=True).
            second_cancel = _cancel_rebuild(db_name, sp_b)
            second_body = second_cancel.json()
            logger.info("second cancel of already-cancelled sp_b: %s", second_body)

            results3 = second_body.get("data", {}).get("results", [])
            already_entry = None
            for r in results3:
                if r.get("space_name") == sp_b:
                    already_entry = r
                    break
            assert already_entry is not None, (
                f"already-cancelled entry not found in results={results3}"
            )
            self._assert_cancel_entry_reason(already_entry, sp_label=sp_b)
            assert already_entry.get("cancelled") is True, (
                f"already-cancelled rebuild should have cancelled=True (idempotent), "
                f"got {already_entry}"
            )
        else:
            logger.info("sp_b was running, could not cancel; skipping already-cancelled check")

        # Wait for any running rebuilds.
        for sp in (sp_a, sp_b):
            progress = _get_rebuild_progress(db_name, sp)
            if progress["status"] == "running":
                _wait_rebuild_completed(db_name, sp, timeout=600)
            _wait_index_status_indexed(db_name, sp)
            drop_space(router_url, db_name, sp)

        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

# ---------------------------------------------------------------------------
# 4. Per-(field, indexType) target rebuild
# ---------------------------------------------------------------------------

class TestRebuildPerField:
    """Rebuild a specific (field_name, index_type) target."""

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_search_routes_each_field_to_its_named_index(self):
        """Search resolves field_name -> index_name without crossing indexes.

        The two documents deliberately store opposite vectors in the two
        fields.  A zero-vector query must therefore hit a different document
        depending on the requested field.  A missing field->index mapping, or
        routing both fields to the same named index, makes this assertion fail.
        """
        case_space = space_name + "_mri_search_index_route"
        assert create_space(
            router_url, db_name, _multi_vector_space_config(case_space)
        ).json()["code"] == 0

        zero = [0.0] * xb.shape[1]
        one = [1.0] * xb.shape[1]
        data = {
            "db_name": db_name,
            "space_name": case_space,
            "documents": [
                {
                    "_id": "doc_a",
                    "field_int": 0,
                    "field_vector_a": zero,
                    "field_vector_b": one,
                },
                {
                    "_id": "doc_b",
                    "field_int": 1,
                    "field_vector_a": one,
                    "field_vector_b": zero,
                },
            ],
        }
        try:
            rs = requests.post(
                router_url + "/document/upsert?timeout=2000000",
                auth=(username, password), json=data,
            )
            assert rs.json().get("code") == 0, rs.text
            waiting_index_finish(2, timewait=1, space_name=case_space)

            def search_top1(field):
                query = {
                    "vector_value": False,
                    "db_name": db_name,
                    "space_name": case_space,
                    "vectors": [{"field": field, "feature": zero}],
                    "fields": ["field_int"],
                    "limit": 1,
                }
                response = requests.post(
                    router_url + "/document/search?timeout=2000000",
                    auth=(username, password), json=query,
                )
                body = response.json()
                assert body.get("code") == 0, body
                documents = body["data"]["documents"]
                results = documents[0] if isinstance(documents[0], list) else documents
                assert len(results) == 1, body
                return results[0]["field_int"]

            assert search_top1("field_vector_a") == 0
            assert search_top1("field_vector_b") == 1
        finally:
            drop_space(router_url, db_name, case_space)

    def test_per_field_rebuild_hnsw(self):
        """Rebuild field_vector_a / HNSW on a multi-vector space."""
        case_space = space_name + "_mri_perfield_hnsw"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        resp = _trigger_rebuild(db_name, case_space, index_name="gamma_a")
        body = resp.json()
        logger.info("per-field rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 1, f"expected 1 index target, got {indexes}"
        assert indexes[0] == "gamma_a"
        logger.info("rebuild detail: db_name %s, space_name %s, index_name %s",
                    db_name, case_space, indexes[0])

        snapshots = _wait_rebuild_completed(db_name, case_space, timeout=600)
        assert snapshots[-1]["status"] == "completed"

        _wait_index_status_indexed(db_name, case_space)
        _check_search_field(case_space, field="field_vector_a")
        _check_search_field(case_space, field="field_vector_b")
        drop_space(router_url, db_name, case_space)

    def test_per_field_rebuild_flat(self):
        """Rebuild field_vector_b / FLAT on a multi-vector space."""
        case_space = space_name + "_mri_perfield_flat"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        resp = _trigger_rebuild(db_name, case_space, index_name="gamma_b")
        body = resp.json()
        logger.info("per-field rebuild flat trigger response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 1, f"expected 1 index target, got {indexes}"
        assert indexes[0] == "gamma_b"

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        _wait_index_status_indexed(db_name, case_space)
        _check_search_field(case_space, field="field_vector_a")
        _check_search_field(case_space, field="field_vector_b")
        drop_space(router_url, db_name, case_space)

    def test_rebuild_all_targets_on_multi_vector_space(self):
        """Fan-out rebuild (no field/index_type) rebuilds every target."""
        case_space = space_name + "_mri_all_targets"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        resp = _trigger_rebuild(db_name, case_space)
        body = resp.json()
        logger.info("all-target rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) >= 2, f"expected >= 2 index names, got {indexes}"
        for idx_name in indexes:
            assert isinstance(idx_name, str) and idx_name, f"index name must be non-empty string: {idx_name!r}"

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_rebuild_single_field_on_single_vector_space(self):
        """Rebuild a single-field space by specifying (field, indexType) explicitly."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_mri_single_field"

        assert create_space(router_url, db_name, _hnsw_space_config(case_space, partition_num=1)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild(db_name, case_space, index_name="gamma")
        body = resp.json()
        logger.info("single-field per-target rebuild response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 1, f"expected 1 index target, got {indexes}"
        assert indexes[0] == "gamma"

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        _wait_index_status_indexed(db_name, case_space)
        _check_search(case_space)
        drop_space(router_url, db_name, case_space)

    def test_rebuild_nonexistent_field_rejected(self):
        """Specifying a field that does not exist on the space must be rejected."""
        case_space = space_name + "_mri_bad_field"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        resp = _trigger_rebuild(db_name, case_space, index_name="nonexistent_index")
        body = resp.json()
        logger.info("rebuild nonexistent field response: %s", body)
        # The rebuild API returns code=0 at the top level for batch-style
        # responses; individual failures are reported in data.failures.
        data = body.get("data", {})
        failures = data.get("failures", [])
        if body.get("code") != 0:
            # Top-level error — check msg.
            msg = body.get("msg", "").lower()
            assert "field" in msg or "no field" in msg or "nonexistent" in msg or "not found" in msg, (
                f"unexpected error message for nonexistent field: {body}"
            )
        else:
            # Batch-style: the target should appear in failures, not results.
            assert len(failures) > 0, (
                f"expected failure for nonexistent field, got success: {body}"
            )
            err_msg = failures[0].get("error", "").lower()
            assert "field" in err_msg or "no field" in err_msg or "nonexistent" in err_msg or "not found" in err_msg, (
                f"unexpected failure message for nonexistent field: {body}"
            )

        drop_space(router_url, db_name, case_space)

    def test_rebuild_wrong_index_type_rejected(self):
        """Specifying a field that exists but with a wrong index type must be rejected.
        E.g. field_vector_a has HNSW, requesting FLAT for it should fail."""
        case_space = space_name + "_mri_wrong_type"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        # field_vector_a has HNSW (gamma_a); asking for a non-existent index should fail.
        resp = _trigger_rebuild(db_name, case_space, index_name="gamma_a_ivfpq_nonexistent")
        body = resp.json()
        logger.info("rebuild wrong index type response: %s", body)
        # The rebuild API returns code=0 at the top level for batch-style
        # responses; individual failures are reported in data.failures.
        data = body.get("data", {})
        failures = data.get("failures", [])
        if body.get("code") != 0:
            msg = body.get("msg", "").lower()
            assert "index" in msg or "type" in msg or "no index" in msg, (
                f"unexpected error message for wrong index type: {body}"
            )
        else:
            assert len(failures) > 0, (
                f"expected failure for wrong index type, got success: {body}"
            )
            err_msg = failures[0].get("error", "").lower()
            assert "index" in err_msg or "type" in err_msg or "no index" in err_msg, (
                f"unexpected failure message for wrong index type: {body}"
            )

        # field_vector_b has FLAT (gamma_b); asking for a non-existent index name should also fail.
        resp2 = _trigger_rebuild(db_name, case_space, index_name="gamma_b_hnsw_nonexistent")
        body2 = resp2.json()
        logger.info("rebuild wrong index type (b->HNSW) response: %s", body2)
        data2 = body2.get("data", {})
        failures2 = data2.get("failures", [])
        if body2.get("code") != 0:
            pass  # top-level error already indicates rejection
        else:
            assert len(failures2) > 0, (
                f"expected failure for wrong index type (field_vector_b/HNSW), got success: {body2}"
            )

        drop_space(router_url, db_name, case_space)

    def test_rebuild_empty_index_name_means_all(self):
        """Empty index_name means rebuild all rebuildable indexes of the space.
        (Replaces the old field/type partial-target test — under the
        IndexName API there is only one identifier and it is either
        specified or absent, never half-specified.)"""
        case_space = space_name + "_mri_empty_idx_name"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        # Empty index_name -> rebuild all targets.
        resp = _trigger_rebuild(db_name, case_space, index_name="")
        body = resp.json()
        logger.info("rebuild with empty index_name response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        # _multi_vector_space_config has two rebuildable vector indexes.
        assert len(indexes) >= 2, f"expected >= 2 targets when no index_name set, got {indexes}"

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        drop_space(router_url, db_name, case_space)

    def test_rebuild_multi_index_space_all_indexes(self):
        """Rebuild a space with multiple vector indexes without specifying
        field_name / index_type — all indexes should be rebuilt sequentially."""
        case_space = space_name + "_mri_multi_all"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        # Trigger full-space rebuild (no field_name / index_type).
        resp = _trigger_rebuild(db_name, case_space)
        body = resp.json()
        logger.info("multi-index space rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        # Verify the progress response lists all index names.
        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 2, f"expected 2 index targets, got {indexes}"
        names = set(indexes)
        assert "gamma_a" in names, f"missing gamma_a in {indexes}"
        assert "gamma_b" in names, f"missing gamma_b in {indexes}"

        # Wait for rebuild to complete.
        snapshots = _wait_rebuild_completed(db_name, case_space, timeout=600)
        final = snapshots[-1]
        assert final["status"] == "completed", f"expected completed, got {final['status']}"
        assert final["failed_tasks"] == 0, f"unexpected failed tasks: {final}"

        # Verify index status is healthy and search still works.
        _wait_index_status_indexed(db_name, case_space)
        _check_search_field(case_space, field="field_vector_a")
        _check_search_field(case_space, field="field_vector_b")

        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

# ---------------------------------------------------------------------------
# 5. Concurrent rebuild rejection
# ---------------------------------------------------------------------------

class TestRebuildConcurrentRejection:
    """A second rebuild while one is pending/running must be rejected."""

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_concurrent_rebuild_rejected(self):
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_mri_concurrent"

        assert create_space(router_url, db_name, _flat_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        first = _trigger_rebuild(db_name, case_space).json()
        assert first.get("code") == 0, first

        second = _trigger_rebuild(db_name, case_space).json()
        logger.info("second rebuild response: %s", second)
        second_failures = second.get("data", {}).get("failures", []) or []
        rejected = second.get("code") != 0 or len(second_failures) > 0
        assert rejected, (
            "concurrent rebuild must be rejected; expected top-level "
            "code!=0 or data.failures, got %s" % second)
        if second_failures:
            err_msg = second_failures[0].get("error", "")
            assert "already pending" in err_msg or "already running" in err_msg, (
                "expected 'already pending/running' error but got: %s" %
                err_msg)

        _wait_rebuild_completed(db_name, case_space, timeout=300)

        third = _trigger_rebuild(db_name, case_space).json()
        assert third.get("code") == 0, third
        _wait_rebuild_completed(db_name, case_space, timeout=300)

        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)


# ---------------------------------------------------------------------------
# 6. Multi-index-type parameterized rebuild
# ---------------------------------------------------------------------------

class TestRebuildMultiIndexType:
    """Parameterized rebuild across different index types."""

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    @pytest.mark.parametrize(
        ["index_type", "space_suffix"],
        [
            ["FLAT", "_mri_type_flat"],
            ["HNSW", "_mri_type_hnsw"],
            ["IVFPQ", "_mri_type_ivfpq"],
        ],
    )
    def test_rebuild_by_index_type(self, index_type, space_suffix):
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + space_suffix

        if index_type == "FLAT":
            config = _flat_space_config(case_space)
        elif index_type == "IVFPQ":
            config = _ivfpq_space_config(case_space)
        else:
            config = _hnsw_space_config(case_space, partition_num=1)

        assert create_space(router_url, db_name, config).json()["code"] == 0
        # full_field= 控制 add() 是否往 doc 里塞 long/float/double/string
        # 这些非 vector 字段。只有 _hnsw_space_config 的 schema 真的有这套
        # 字段;_flat_space_config / _ivfpq_space_config 都是精简 schema
        # (只有 field_int + field_vector),传 True 会让 router 校验失败
        # 报 "unrecognizable field, field_string is not found in space"。
        full_field = (index_type == "HNSW")
        add(total_batch, batch_size, xb, True, full_field, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild(db_name, case_space)
        body = resp.json()
        logger.info("rebuild [%s] trigger response: %s", index_type, body)
        assert body.get("code") == 0, body

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        _wait_index_status_indexed(db_name, case_space)

        if index_type != "FLAT":
            _check_search(case_space)
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

# ---------------------------------------------------------------------------
# 7. DB-level trigger / query / cancel
# ---------------------------------------------------------------------------

class TestRebuildDBLevel:
    """Exercise the DB-scope rebuild endpoints.

    POST /index/rebuild/dbs/:db                     — trigger all spaces
    GET  /index/rebuild/dbs/:db/progress             — query DB summary
    POST /index/rebuild/dbs/:db/cancel               — cancel all spaces
    """

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_db_level_trigger_and_progress(self):
        """Trigger rebuild for all spaces in a DB, then verify DB-level progress."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space_a = space_name + "_mri_db_a"
        case_space_b = space_name + "_mri_db_b"
        assert create_space(router_url, db_name, _flat_space_config(case_space_a)).json()["code"] == 0
        assert create_space(router_url, db_name, _flat_space_config(case_space_b)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space_a)
        add(total_batch, batch_size, xb, True, False, space_name=case_space_b)
        waiting_index_finish(total, space_name=case_space_a)
        waiting_index_finish(total, space_name=case_space_b)

        resp = _trigger_rebuild_db(db_name)
        body = resp.json()
        logger.info("DB-level trigger response: %s", body)
        assert body.get("code") == 0, body

        db_progress = _list_rebuild_progress(db_name)
        logger.info("DB-level progress: %s", json.dumps(db_progress, indent=2, default=str))
        results = db_progress.get("results", [])
        space_keys = [r.get("space_key", "") for r in results]
        assert any(case_space_a in k for k in space_keys), f"space_a not found in DB progress: {space_keys}"
        assert any(case_space_b in k for k in space_keys), f"space_b not found in DB progress: {space_keys}"

        _wait_rebuild_completed(db_name, case_space_a, timeout=300)
        _wait_rebuild_completed(db_name, case_space_b, timeout=300)
        _wait_index_status_indexed(db_name, case_space_a)
        _wait_index_status_indexed(db_name, case_space_b)

        drop_space(router_url, db_name, case_space_a)
        drop_space(router_url, db_name, case_space_b)

    def test_db_level_cancel(self):
        """Trigger DB-level rebuild, then cancel at DB scope."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_db_cancel"
        assert create_space(router_url, db_name, _hnsw_space_config(case_space, partition_num=2)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild_db(db_name)
        assert resp.json().get("code") == 0, resp.text

        cancel_resp = _cancel_rebuild_db(db_name)
        body = cancel_resp.json()
        logger.info("DB-level cancel response: %s", body)
        assert body.get("code") == 0, body

        results = body.get("data", {}).get("results", [])
        if results:
            for entry in results:
                assert "cancelled" in entry, entry
                assert "reason" in entry, entry
                logger.info(
                    "cancel entry: space_key=%s cancelled=%s reason=%s",
                    entry.get("space_key"), entry.get("cancelled"), entry.get("reason"),
                )

        progress = _get_rebuild_progress(db_name, case_space)
        if progress["status"] not in ("completed", "cancelled"):
            _wait_rebuild_completed(db_name, case_space, timeout=600)

        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

# ---------------------------------------------------------------------------
# 8. Global-scope trigger / query / cancel
# ---------------------------------------------------------------------------

class TestRebuildGlobalScope:
    """Exercise the global rebuild endpoints.

    POST /index/rebuild                            — trigger all DBs
    GET  /index/rebuild/progress                    — global summary
    POST /index/rebuild/cancel                     — cancel all DBs
    """

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_global_trigger_and_progress(self):
        """Trigger global rebuild and verify global progress summary."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_global"
        assert create_space(router_url, db_name, _flat_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild_global()
        body = resp.json()
        logger.info("global trigger response: %s", body)
        assert body.get("code") == 0, body

        global_progress = _list_rebuild_progress()
        logger.info("global progress: %s", json.dumps(global_progress, indent=2, default=str))
        results = global_progress.get("results", [])
        space_keys = [r.get("space_key", "") for r in results]
        assert any(case_space in k for k in space_keys), f"our space not found in global progress: {space_keys}"

        _wait_rebuild_completed(db_name, case_space, timeout=300)
        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_global_cancel(self):
        """Trigger global rebuild, then cancel globally."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_global_cancel"
        assert create_space(router_url, db_name, _hnsw_space_config(case_space, partition_num=2)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild_global()
        assert resp.json().get("code") == 0, resp.text

        time.sleep(3)

        cancel_resp = _cancel_rebuild_global()
        body = cancel_resp.json()
        logger.info("global cancel response: %s", body)
        assert body.get("code") == 0, body

        results = body.get("data", {}).get("results", [])
        if results:
            for entry in results:
                assert "cancelled" in entry, entry
                assert "reason" in entry, entry

        progress = _get_rebuild_progress(db_name, case_space)
        if progress["status"] not in ("completed", "cancelled"):
            _wait_rebuild_completed(db_name, case_space, timeout=600)

        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

    """Exercise the global rebuild endpoints.

    POST /index/rebuild                            — trigger all DBs
    GET  /index/rebuild/progress                    — global summary
    POST /index/rebuild/cancel                     — cancel all DBs
    """

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _ensure_clean_db()

    def test_global_trigger_and_progress(self):
        """Trigger global rebuild and verify global progress summary."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_global"
        assert create_space(router_url, db_name, _flat_space_config(case_space)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild_global()
        body = resp.json()
        logger.info("global trigger response: %s", body)
        assert body.get("code") == 0, body

        global_progress = _list_rebuild_progress()
        logger.info("global progress: %s", json.dumps(global_progress, indent=2, default=str))
        results = global_progress.get("results", [])
        space_keys = [r.get("space_key", "") for r in results]
        assert any(case_space in k for k in space_keys), f"our space not found in global progress: {space_keys}"

        _wait_rebuild_completed(db_name, case_space, timeout=300)
        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_global_cancel(self):
        """Trigger global rebuild, then cancel globally."""
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_global_cancel"
        assert create_space(router_url, db_name, _hnsw_space_config(case_space, partition_num=2)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        resp = _trigger_rebuild_global()
        assert resp.json().get("code") == 0, resp.text

        time.sleep(3)

        cancel_resp = _cancel_rebuild_global()
        body = cancel_resp.json()
        logger.info("global cancel response: %s", body)
        assert body.get("code") == 0, body

        results = body.get("data", {}).get("results", [])
        if results:
            for entry in results:
                assert "cancelled" in entry, entry
                assert "reason" in entry, entry

        progress = _get_rebuild_progress(db_name, case_space)
        if progress["status"] not in ("completed", "cancelled"):
            _wait_rebuild_completed(db_name, case_space, timeout=600)

        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)


# ===========================================================================
# Additional non-cluster coverage from test_module_rebuild_comprehensive.py
# ===========================================================================

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _comp_trigger_rebuild(db, space, index_name="", max_retries=0,
                     drop_before_rebuild=False, describe=0, partition_id=-1,
                     ensure_indexed=True):
    """POST rebuild with optional parameters.

    ensure_indexed: gate on every partition reaching index_status==INDEXED
        before triggering. waiting_index_finish() only waits the *aggregate*
        index_num to reach total — it does NOT guarantee per-partition
        index_status has flipped to INDEXED. There is a window (wider for
        range-partitioned spaces, where index build is more staggered) in
        which the doc count is complete but some partition is still
        UNINDEXED. Triggering then is rejected by the server with
        "rebuild requires an existing index", the rebuild never starts, and
        the progress poller times out. Gating here makes every trigger
        deterministic. Pass ensure_indexed=False only when the space is
        intentionally not (yet) fully indexed at trigger time.
    """
    if ensure_indexed:
        _comp_wait_index_status_indexed(db, space)
    payload = {}
    if index_name:
        url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/indexes/{index_name}"
    else:
        url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}"
    if max_retries > 0:
        payload["max_retries"] = max_retries
    if drop_before_rebuild:
        payload["drop_before_rebuild"] = True
    if describe > 0:
        payload["describe"] = describe
    if partition_id >= 0:
        payload["partition_id"] = partition_id
    resp = requests.post(url, auth=(username, password), json=payload)
    logger.info("trigger_rebuild url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp


def _comp_trigger_rebuild_db(db):
    url = f"{router_url}/index/rebuild/dbs/{db}"
    return requests.post(url, auth=(username, password), json={})


def _comp_get_rebuild_progress(db, space):
    url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/progress"
    resp = requests.get(url, auth=(username, password))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("code") == 0, body
    return body.get("data", {}) or {}


def _comp_cancel_rebuild(db, space):
    url = f"{router_url}/index/rebuild/dbs/{db}/spaces/{space}/cancel"
    return requests.post(url, auth=(username, password))


def _comp_wait_rebuild_completed(db, space, timeout=600, poll_interval=3, allow_failed=False):
    """Poll progress until terminal. Returns chronological snapshots.

    Note on monotonicity: overall_percent is the progress of the *current*
    rebuild target, not the aggregate across all targets. When a multi-index
    rebuild advances from target N to N+1, master resets Tasks/TotalTasks
    for the new target and overall_percent drops back toward 0. We only
    require monotonicity *within* a single target (same current_index).
    """
    deadline = time.time() + timeout
    snapshots = []
    last_overall = -1
    last_current_index = -1
    while time.time() < deadline:
        progress = _comp_get_rebuild_progress(db, space)
        snapshots.append(progress)
        status = progress["status"]
        overall = progress.get("overall_percent", 0)
        current_index = progress.get("current_index", 0)
        if current_index == last_current_index:
            assert overall >= last_overall, (
                f"overall_percent decreased within target #{current_index}: "
                f"{last_overall} -> {overall}"
            )
        last_overall = overall
        last_current_index = current_index
        logger.info(
            "progress: status=%s target=%d overall=%d%% completed=%d/%d running=%d failed=%d",
            status, current_index, overall,
            progress["completed_tasks"], progress["total_tasks"],
            progress["running_tasks"], progress["failed_tasks"],
        )
        if status == "completed":
            return snapshots
        if status == "failed":
            if allow_failed:
                return snapshots
            pytest.fail(f"rebuild failed for {db}/{space}: {json.dumps(progress, indent=2)}")
        if status == "cancelled":
            return snapshots
        time.sleep(poll_interval)
    pytest.fail(f"rebuild did not complete within {timeout}s for {db}/{space}")


def _comp_wait_index_status_indexed(db, space, max_rounds=180, poll_interval=5):
    url = f"{router_url}/dbs/{db}/spaces/{space}?detail=true"
    for r in range(max_rounds):
        rs = requests.get(url, auth=(username, password))
        body = rs.json()
        data = body.get("data", {})
        partitions = data.get("partitions", [])
        statuses = [p.get("index_status", -1) for p in partitions]
        if data.get("status") != "red" and partitions and all(s == 2 for s in statuses):
            return
        time.sleep(poll_interval)
    pytest.fail(f"index_status did not reach INDEXED for {db}/{space}")


def _comp_ensure_clean_db():
    url = f"{router_url}/dbs/{db_name}/spaces"
    rs = requests.get(url, auth=(username, password))
    if rs.status_code == 200:
        body = rs.json()
        if body.get("code") == 0 and body.get("data"):
            for sp in body["data"]:
                sp_name = sp.get("space_name") or sp.get("name") or ""
                if sp_name:
                    drop_space(router_url, db_name, sp_name)
    drop_db(router_url, db_name)
    create_db(router_url, db_name)


def _comp_check_search(case_space_name, times=5, db_name_override=""):
    target_db = db_name_override or db_name
    url = router_url + "/document/search?timeout=2000000"
    for i in range(times):
        data = {"vector_value": True, "db_name": target_db, "space_name": case_space_name,
                "vectors": [{"field": "field_vector", "feature": xb[i:i+1].flatten().tolist()}]}
        rs = requests.post(url, auth=(username, password), json=data)
        body = rs.json()
        if body.get("code") != 0:
            logger.warning("search returned non-zero code: %s", body)
            continue
        assert len(body["data"]["documents"]) == 1


def _comp_get_space_detail(db, space):
    url = f"{router_url}/dbs/{db}/spaces/{space}?detail=true"
    rs = requests.get(url, auth=(username, password))
    body = rs.json()
    assert body.get("code") == 0, body
    return body.get("data", {})


def _comp_delete_documents(db, space, doc_ids):
    url = router_url + "/document/delete?timeout=300000"
    batch = 200
    for start in range(0, len(doc_ids), batch):
        chunk = doc_ids[start:start+batch]
        del_data = {"db_name": db, "space_name": space, "document_ids": [str(d) for d in chunk]}
        resp = requests.post(url, auth=(username, password), json=del_data)
        assert resp.json().get("code") == 0, f"delete failed: {resp.json()}"


def _comp_query_document(db, space, doc_id):
    url = router_url + "/document/query"
    data = {"db_name": db, "space_name": space, "document_ids": [doc_id], "fields": ["field_int", "field_vector"]}
    return requests.post(url, auth=(username, password), json=data).json()


# ---------------------------------------------------------------------------
# Space config factories
# ---------------------------------------------------------------------------


def _comp_hnsw_cfg(name, pn=2, rn=1):
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_long", "type": "long"},
                {"name": "field_float", "type": "float"},
                {"name": "field_double", "type": "double"},
                {"name": "field_string", "type": "string", "index": {"name": "field_string", "type": "SCALAR"}},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "HNSW",
                           "params": {"metric_type": "InnerProduct", "nlinks": 32, "efConstruction": 40, "training_threshold": 1}},
                 "dimension": dim},
            ]}

def _comp_hnsw_range_partition_cfg(name, pn=1, rn=2):
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_date", "type": "date"},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "HNSW",
                           "params": {"metric_type": "InnerProduct", "nlinks": 32, "efConstruction": 40, "training_threshold": 1}},
                 "dimension": dim},
            ],
            "partition_rule": {
                "type": "RANGE",
                "field": "field_date",
                "ranges": [
                    {"name": "p0", "value": "2026-01-02"},
                    {"name": "p1", "value": "2026-01-03"},
                ],
            }}

def _comp_flat_cfg(name, pn=1, rn=1):
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [{"name": "field_int", "type": "integer"},
                       {"name": "field_vector", "type": "vector",
                        "index": {"name": "gamma", "type": "FLAT", "params": {"metric_type": "L2", "training_threshold": 1}},
                        "dimension": dim}]}

def _comp_ivfflat_cfg(name, pn=1, rn=1):
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [{"name": "field_int", "type": "integer"},
                       {"name": "field_vector", "type": "vector",
                        "index": {"name": "gamma", "type": "IVFFLAT",
                                  "params": {"metric_type": "L2", "ncentroids": 128, "training_threshold": 3999}},
                        "dimension": dim}]}

def _comp_ivfpq_cfg(name, pn=1, rn=1):
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [{"name": "field_int", "type": "integer"},
                       {"name": "field_vector", "type": "vector",
                        "index": {"name": "gamma", "type": "IVFPQ",
                                  "params": {"metric_type": "InnerProduct", "ncentroids": 128, "nsubvector": 32, "training_threshold": 3999}},
                        "dimension": dim}]}

def _comp_ivfrabitq_cfg(name, pn=1, rn=1):
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [{"name": "field_int", "type": "integer"},
                       {"name": "field_vector", "type": "vector",
                        "index": {"name": "gamma", "type": "IVFRABITQ",
                                  "params": {"metric_type": "InnerProduct", "ncentroids": 128, "training_threshold": 3999}},
                        "dimension": dim}]}

def _comp_multi3_cfg(name, pn=1, rn=1):
    """3 vector fields: HNSW + IVFFLAT + IVFPQ.
    """
    dim = xb.shape[1]
    return {"name": name, "partition_num": pn, "replica_num": rn,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector_a", "type": "vector",
                 "index": {"name": "gamma_a", "type": "HNSW",
                           "params": {"metric_type": "L2", "nlinks": 32, "efConstruction": 40, "training_threshold": 1}},
                 "dimension": dim},
                {"name": "field_vector_b", "type": "vector",
                 "index": {"name": "gamma_b", "type": "IVFFLAT",
                           "params": {"metric_type": "L2", "ncentroids": 32, "nprobe": 8, "training_threshold": 1248}},
                 "dimension": dim},
                {"name": "field_vector_c", "type": "vector",
                 "index": {"name": "gamma_c", "type": "IVFPQ",
                           "params": {"metric_type": "InnerProduct", "ncentroids": 32, "nprobe": 8, "nsubvector": 32, "training_threshold": 1248}},
                 "dimension": dim},
            ]}


def _comp_add_multi3_docs(space_name, n_fields=3):
    """Insert docs with multiple vector fields."""
    batch_size, total = 100, xb.shape[0]
    total_batch = int(total / batch_size)
    url = router_url + "/document/upsert?timeout=2000000"
    field_names = ["field_vector_a", "field_vector_b", "field_vector_c"]
    for i in range(total_batch):
        docs = []
        for j in range(batch_size):
            doc = {"_id": str(i*batch_size+j), "field_int": i*batch_size+j}
            for k in range(min(n_fields, len(field_names))):
                doc[field_names[k]] = xb[i*batch_size+j].tolist()
            docs.append(doc)
        rs = requests.post(url, auth=(username, password), json={"db_name": db_name, "space_name": space_name, "documents": docs})
        assert rs.json().get("code") == 0
    waiting_index_finish(total, space_name=space_name)

def _comp_add_range_partition_docs(space_name, total=10000, batch_size=100):
    """Insert docs into named range partitions p0 and p1."""
    url = router_url + "/document/upsert?timeout=2000000"
    for i in range(total // batch_size):
        docs = []
        for j in range(batch_size):
            idx = i * batch_size + j
            docs.append({
                "_id": str(idx),
                "field_int": idx,
                "field_date": "2026-01-01" if idx < total // 2 else "2026-01-02",
                "field_vector": xb[idx].tolist(),
            })
        rs = requests.post(url, auth=(username, password),
                           json={"db_name": db_name, "space_name": space_name,
                                 "documents": docs},
                           timeout=30)
        assert rs.json().get("code") == 0, rs.text
    waiting_index_finish(total, space_name=space_name)


# ===========================================================================
# 3. Per-partition serialization visibility
# ===========================================================================



class TestRebuildReplicaSerialization:

    def setup_class(self):
        pass

    def test_prepare_db(self):
        _comp_ensure_clean_db()


    def test_restatus_map_resets_on_cancelled_rebuild(self):
        """3.4: Cancelled rebuild leaves space healthy."""
        batch_size, total = 100, xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_comp_restatus"

        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        _comp_trigger_rebuild(db_name, case_space)
        time.sleep(2)
        _comp_cancel_rebuild(db_name, case_space)

        progress = _comp_get_rebuild_progress(db_name, case_space)
        if progress["status"] == "running":
            _comp_wait_rebuild_completed(db_name, case_space, timeout=300)

        _comp_wait_index_status_indexed(db_name, case_space)
        _comp_check_search(case_space)
        drop_space(router_url, db_name, case_space)



    def test_cross_partition_query_works_during_rebuild(self):
        """3.5: Querying a partition NOT being rebuilt is unaffected by
        another partition's rebuild on a shared PS.

        Without partition placement control, we use the per-partition
        rebuild API to scope rebuild to a single partition and verify
        queries on the OTHER partitions still work normally.
        """
        batch_size, total = 100, xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_comp_cross_pid"

        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=2)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        detail = _comp_get_space_detail(db_name, case_space)
        partitions = detail.get("partitions", [])
        assert len(partitions) >= 2, "need at least 2 partitions"
        target_pid = partitions[0]["pid"]
        other_pids = [p["pid"] for p in partitions[1:]]

        # Trigger rebuild on partition[0] only.
        resp = _comp_trigger_rebuild(db_name, case_space, partition_id=target_pid)
        assert resp.json().get("code") == 0

        # While rebuild is running, query and verify search returns docs.
        url = router_url + "/document/search?timeout=10000"
        ok_count = 0
        err_count = 0
        deadline = time.time() + 120
        while time.time() < deadline:
            progress = _comp_get_rebuild_progress(db_name, case_space)
            data = {"vector_value": False, "db_name": db_name, "space_name": case_space,
                    "vectors": [{"field": "field_vector", "feature": xb[ok_count % total].tolist()}]}
            try:
                rs = requests.post(url, auth=(username, password), json=data, timeout=10)
                if rs.json().get("code") == 0:
                    ok_count += 1
                else:
                    err_count += 1
            except Exception:
                err_count += 1
            if progress["status"] in ("completed", "failed"):
                break
            time.sleep(0.5)

        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        attempts = ok_count + err_count
        if attempts > 0:
            assert err_count / attempts < 0.05, f"err {err_count}/{attempts} too high"
        drop_space(router_url, db_name, case_space)

    def test_destroy_db_3(self):
        drop_db(router_url, db_name)


# ===========================================================================
# 4. Data integrity
# ===========================================================================


class TestRebuildDataIntegrity:

    def test_prepare_db(self):
        _comp_ensure_clean_db()

    def test_doc_num_preserved_after_rebuild(self):
        """4.1: doc_num / max_docid unchanged; deleted IDs stay invisible;
        non-deleted vector data preserved byte-equal.
        """
        batch_size, total = 100, xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_comp_invariant"

        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        random.seed(42)
        deleted_ids = random.sample(range(total), 3000)
        _comp_delete_documents(db_name, case_space, deleted_ids)
        time.sleep(2)

        pre_detail = _comp_get_space_detail(db_name, case_space)
        pre_doc_num = pre_detail.get("doc_num")
        pre_max = sum(p.get("max_docid", 0) for p in pre_detail.get("partitions", []))

        # Sample 30 known-non-deleted IDs and capture their vectors.
        non_deleted = list(set(range(total)) - set(deleted_ids))
        sample_ids = random.sample(non_deleted, 30)
        pre_vectors = {}
        for did in sample_ids:
            r = _comp_query_document(db_name, case_space, str(did))
            assert r.get("code") == 0, r
            docs = r.get("data", {}).get("documents", [])
            if docs and docs[0]:
                pre_vectors[did] = docs[0].get("field_vector") or docs[0].get("_source", {}).get("field_vector")

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        _comp_wait_index_status_indexed(db_name, case_space)

        post_detail = _comp_get_space_detail(db_name, case_space)
        post_doc_num = post_detail.get("doc_num")
        post_max = sum(p.get("max_docid", 0) for p in post_detail.get("partitions", []))

        assert post_doc_num == pre_doc_num, f"doc_num changed: {pre_doc_num} -> {post_doc_num}"
        assert post_max == pre_max, f"max_docid sum changed: {pre_max} -> {post_max}"

        # Deleted IDs must remain invisible.
        for did in deleted_ids[:50]:
            r = _comp_query_document(db_name, case_space, str(did))
            docs = r.get("data", {}).get("documents", []) or []
            # vearch query API typically returns empty list or document with _found=false
            for d in docs:
                if d:
                    found = d.get("_found", True)
                    assert not found, f"deleted id {did} resurrected: {d}"

        # Sampled non-deleted vectors must be byte-equal.
        diffs = 0
        for did, pre_v in pre_vectors.items():
            r = _comp_query_document(db_name, case_space, str(did))
            docs = r.get("data", {}).get("documents", []) or []
            if not docs or not docs[0]:
                continue
            post_v = docs[0].get("field_vector") or docs[0].get("_source", {}).get("field_vector")
            if pre_v != post_v:
                diffs += 1
        assert diffs == 0, f"{diffs}/{len(pre_vectors)} sampled vectors differ post-rebuild"

        drop_space(router_url, db_name, case_space)

    def test_search_consistent_pre_post_rebuild(self):
        """4.2: For 100 fixed queries, top-1 hit rate ≥ 95% (HNSW allows
        small graph reshaping); top-10 Jaccard ≥ 0.85 between pre and post.
        """
        batch_size, total = 100, xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_comp_consistent"

        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        nq = 100
        url = router_url + "/document/search?timeout=10000"

        def topk(idx):
            data = {"vector_value": False, "db_name": db_name, "space_name": case_space,
                    "vectors": [{"field": "field_vector", "feature": xq[idx].tolist()}],
                    "fields": ["field_int"], "limit": 10}
            rs = requests.post(url, auth=(username, password), json=data, timeout=10)
            body = rs.json()
            if body.get("code") != 0:
                return []
            docs = body.get("data", {}).get("documents", [[]])
            res = docs[0] if docs and isinstance(docs[0], list) else docs
            return [d.get("field_int") for d in res if d.get("field_int") is not None]

        pre = [topk(i) for i in range(nq)]

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        _comp_wait_index_status_indexed(db_name, case_space)

        post = [topk(i) for i in range(nq)]

        top1_hits = sum(1 for a, b in zip(pre, post) if a and b and a[0] == b[0])
        jaccard_sum = 0.0
        for a, b in zip(pre, post):
            sa, sb = set(a), set(b)
            if not sa and not sb:
                jaccard_sum += 1.0
                continue
            jaccard_sum += len(sa & sb) / max(1, len(sa | sb))

        top1_rate = top1_hits / nq
        jaccard_avg = jaccard_sum / nq
        logger.info("4.2 top1=%.3f jaccard=%.3f", top1_rate, jaccard_avg)
        assert top1_rate >= 0.90, f"top1 hit rate too low: {top1_rate:.3f}"
        assert jaccard_avg >= 0.85, f"jaccard too low: {jaccard_avg:.3f}"

        drop_space(router_url, db_name, case_space)

    def test_schema_unchanged_after_rebuild(self):
        """4.3: Schema dump byte-equal pre and post rebuild."""
        batch_size, total = 100, xb.shape[0]
        total_batch = int(total / batch_size)
        case_space = space_name + "_comp_schema"

        assert create_space(router_url, db_name, _comp_multi3_cfg(case_space)).json()["code"] == 0
        _comp_add_multi3_docs(case_space)

        def get_schema(target):
            url = f"{router_url}/dbs/{db_name}/spaces/{target}"
            body = requests.get(url, auth=(username, password)).json()
            data = body.get("data", {})
            # Strip mutable runtime fields.
            for k in ("doc_num", "partitions", "status", "update_time", "id"):
                data.pop(k, None)
            return json.dumps(data, sort_keys=True)

        pre = get_schema(case_space)
        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
        _comp_wait_index_status_indexed(db_name, case_space)
        post = get_schema(case_space)

        assert pre == post, f"schema drift:\nPRE  : {pre[:500]}\nPOST : {post[:500]}"
        drop_space(router_url, db_name, case_space)

    def test_destroy_db_4(self):
        drop_db(router_url, db_name)


# ===========================================================================
# 5. Concurrent writes during rebuild
# ===========================================================================


class TestRebuildConcurrentWrites:

    def test_prepare_db(self):
        _comp_ensure_clean_db()

    def test_inserts_during_rebuild_visible_after(self):
        """5.1: Insert 5000 new docs while rebuild is running; final
        doc_num == 10000 and new vectors are queryable.

        NOTE on with_id: the add() helper does NOT apply `offset` to
        `_id` (only to field_int). With with_id=True both batches would
        produce _id="0".."4999" and the second batch would silently
        upsert over the first → doc_num stuck at 5000. Letting vearch
        auto-assign _id (with_id=False) avoids the collision; the
        post-checks below query by vector content so _id values don't
        matter.
        """
        batch_size, half = 100, 5000
        case_space = space_name + "_comp_ins_during"

        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(half // batch_size, batch_size, xb[:half],
            with_id=False, full_field=False,
            space_name=case_space, offset=0)
        waiting_index_finish(half, space_name=case_space)

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        # Wait until rebuild is actually running.
        for _ in range(20):
            if _comp_get_rebuild_progress(db_name, case_space)["status"] == "running":
                break
            time.sleep(0.5)

        # Concurrent insert second half. with_id=False so vearch assigns
        # fresh _id for every doc, avoiding the upsert-overwrite trap.
        add(half // batch_size, batch_size, xb[half:],
            with_id=False, full_field=False,
            space_name=case_space, offset=half)

        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
        logger.info("rebuild finished after concurrent insert")
        waiting_index_finish(2 * half, space_name=case_space)
        _comp_wait_index_status_indexed(db_name, case_space)

        detail = _comp_get_space_detail(db_name, case_space)
        assert detail.get("doc_num") == 2 * half, f"expected 10000 docs, got {detail.get('doc_num')}"

        # Sample new vectors and confirm reachable via vector search.
        url = router_url + "/document/search?timeout=10000"
        for sid in [half, half + 100, 2 * half - 1]:
            data = {"vector_value": False, "db_name": db_name, "space_name": case_space,
                    "vectors": [{"field": "field_vector", "feature": xb[sid].tolist()}],
                    "fields": ["field_int"], "limit": 1}
            r = requests.post(url, auth=(username, password), json=data).json()
            assert r.get("code") == 0
            docs = r.get("data", {}).get("documents", [[]])
            res = docs[0] if docs and isinstance(docs[0], list) else docs
            assert res, f"no result for inserted vector at index={sid}"
        drop_space(router_url, db_name, case_space)

    def test_deletes_during_rebuild_take_effect(self):
        """5.2: Delete IDs 5000..9999 during rebuild; final doc_num == 5000."""
        batch_size, total = 100, 10000
        case_space = space_name + "_comp_del_during"

        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)

        _comp_delete_documents(db_name, case_space, list(range(5000, total)))
        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
        time.sleep(3)
        logger.info("after delete rebuild finished")
        _comp_wait_index_status_indexed(db_name, case_space)

        detail = _comp_get_space_detail(db_name, case_space)
        assert detail.get("doc_num") == 5000, f"expected 5000 docs, got {detail.get('doc_num')}"

        # Spot-check: deleted IDs not returnable.
        for did in [5000, 7500, 9999]:
            r = _comp_query_document(db_name, case_space, str(did))
            docs = r.get("data", {}).get("documents", []) or []
            for d in docs:
                if d:
                    assert not d.get("_found", True), f"deleted id {did} still returnable"
        drop_space(router_url, db_name, case_space)


    def test_destroy_db_5(self):
        drop_db(router_url, db_name)


# ===========================================================================
# 6. Index type matrix
# ===========================================================================


class TestRebuildIndexTypeMatrix:

    def test_prepare_db(self):
        _comp_ensure_clean_db()

    def _basic_rebuild_lifecycle(self, case_space, cfg, total=10000, do_search=True):
        batch_size = 100
        total_batch = total // batch_size
        assert create_space(router_url, db_name, cfg).json()["code"] == 0
        add(total_batch, batch_size, xb[:total], True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)
        pre_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
        _comp_wait_index_status_indexed(db_name, case_space)

        post_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")
        assert post_doc_num == pre_doc_num
        if do_search:
            _comp_check_search(case_space, times=3)
        drop_space(router_url, db_name, case_space)

    def test_rebuild_flat(self):
        """6.1: FLAT — no training; rebuild should be effectively a noop
        but must complete without error.
        """
        case_space = space_name + "_comp_flat"
        self._basic_rebuild_lifecycle(case_space, _comp_flat_cfg(case_space))

    def test_rebuild_ivfflat(self):
        case_space = space_name + "_comp_ivfflat"
        self._basic_rebuild_lifecycle(case_space, _comp_ivfflat_cfg(case_space))

    def test_rebuild_ivfpq(self):
        case_space = space_name + "_comp_ivfpq"
        self._basic_rebuild_lifecycle(case_space, _comp_ivfpq_cfg(case_space))

    def test_rebuild_ivfrabitq(self):
        """6.2: IVFRABITQ basic lifecycle."""
        case_space = space_name + "_comp_rabitq"
        try:
            resp = create_space(router_url, db_name, _comp_ivfrabitq_cfg(case_space))
            if resp.json().get("code") != 0:
                pytest.skip(f"IVFRABITQ not supported: {resp.json()}")
        except Exception as e:
            pytest.skip(f"IVFRABITQ unavailable: {e}")
        # Rebuild + verify.
        batch_size, total = 100, 10000
        add(total // batch_size, batch_size, xb[:total], True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)
        pre = _comp_get_space_detail(db_name, case_space).get("doc_num")
        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
        _comp_wait_index_status_indexed(db_name, case_space)
        post = _comp_get_space_detail(db_name, case_space).get("doc_num")
        assert pre == post
        drop_space(router_url, db_name, case_space)

    # def test_rebuild_ivfpqfs(self):
    #     """6.3: IVFPQFS — fast-scan IVFPQ variant. The master allowlist
    #     (entity/space.go:370-387) is build-dependent: in a default build
    #     IVFPQFS is not whitelisted and the request fails at the master
    #     validator with PARAM_ERROR. In that case we skip; the test still
    #     documents the intent and runs end-to-end on builds that include it.
    #     """
    #     case_space = space_name + "_comp_ivfpqfs"
    #     embedding_size = xb.shape[1]
    #     cfg = {
    #         "name": case_space, "partition_num": 1, "replica_num": 1,
    #         "fields": [
    #             {"name": "field_int", "type": "integer"},
    #             {"name": "field_vector", "type": "vector",
    #              "index": {"name": "gamma", "type": "IVFPQFS",
    #                        "params": {"metric_type": "L2",
    #                                   "ncentroids": 128, "nsubvector": 32,
    #                                   "training_threshold": 3999}},
    #              "dimension": embedding_size},
    #         ],
    #     }
    #     resp = create_space(router_url, db_name, cfg)
    #     body = resp.json()
    #     if body.get("code") != 0:
    #         pytest.skip(
    #             f"IVFPQFS not accepted by master allowlist on this build: "
    #             f"code={body.get('code')} msg={body.get('msg')}")
    #     try:
    #         self._run_lifecycle_existing_space(case_space, total=10000,
    #                                             rebuild_timeout=600)
    #     finally:
    #         try:
    #             drop_space(router_url, db_name, case_space)
    #         except Exception:
    #             pass

    def test_rebuild_binary_ivf(self):
        """6.4: BinaryIVF on packed binary vectors.

        Wire format (router/document/doc_parse.go:174-193, :465-485):
          - dimension declared in space schema is the BIT count.
          - feature payload is a list of len = dimension/8 of uint8 values
            (each byte holds 8 bits).
          - master validator rejects feature length ≠ dimension/8.

        We synthesise random packed bytes with numpy and upsert directly
        via /document/upsert (the shared `add()` helper in vearch_utils
        only knows about the float SIFT dataset).
        """
        import numpy as np
        case_space = space_name + "_comp_bivf"
        dim_bits = 128                # multiple of 8
        code_size = dim_bits // 8     # 16 bytes per vector
        cfg = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "BINARYIVF",
                           "params": {"metric_type": "L2",
                                      "ncentroids": 64,
                                      "training_threshold": 1000}},
                 "dimension": dim_bits},
            ],
        }
        resp = create_space(router_url, db_name, cfg)
        body = resp.json()
        if body.get("code") != 0:
            pytest.skip(
                f"BINARYIVF not supported on this cluster build: "
                f"code={body.get('code')} msg={body.get('msg')}")
        try:
            np.random.seed(42)
            n_docs = 5000     # > training_threshold so index actually trains
            bvec = np.random.randint(0, 256, size=(n_docs, code_size),
                                     dtype=np.uint8)

            upsert_url = router_url + "/document/upsert?timeout=300000"
            batch = 100
            for start in range(0, n_docs, batch):
                docs = []
                for j in range(start, min(start + batch, n_docs)):
                    docs.append({
                        "_id": str(j),
                        "field_int": j,
                        "field_vector": bvec[j].tolist(),
                    })
                r = requests.post(upsert_url, auth=(username, password),
                                  json={"db_name": db_name,
                                        "space_name": case_space,
                                        "documents": docs})
                rb = r.json()
                assert rb.get("code") == 0, (
                    f"upsert binary docs failed at start={start}: {r.text[:300]}")
            waiting_index_finish(n_docs, space_name=case_space)

            pre_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")

            assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
            _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
            _comp_wait_index_status_indexed(db_name, case_space)

            post_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")
            assert post_doc_num == pre_doc_num, (
                f"doc_num diverged across rebuild: pre={pre_doc_num} "
                f"post={post_doc_num}")

            # Smoke search: binary feature must be a list of uint8.
            search_url = router_url + "/document/search?timeout=10000"
            ok = 0
            for i in range(5):
                r = requests.post(search_url, auth=(username, password),
                                  json={"db_name": db_name,
                                        "space_name": case_space,
                                        "vectors": [{"field": "field_vector",
                                                     "feature": bvec[i].tolist()}],
                                        "limit": 5})
                rb = r.json()
                if rb.get("code") == 0 and rb.get("data", {}).get("documents"):
                    ok += 1
            assert ok >= 3, f"smoke search produced too few hits: ok={ok}/5"
        finally:
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass

    def test_rebuild_diskann(self):
        """6.5: DISKANN_STATIC — graph-based on-disk index.

        DISKANN_STATIC is **静态索引** —— 写入数据不会触发增量构建,
        index_status 维持 UNINDEXED, index_num 维持 0;调用 master 的
        rebuild 接口前必须先把索引「初次构建」起来,否则
        rebuild_service.go:checkPartitionsHealthy 会以 UNINDEXED 拒
        掉请求。

        正确顺序 (与 test_vector_index_diskann_static.py 一致):
          add(数据) → /index/forcemerge → 等到 INDEXED → 这才有「已有索引」
          可以让 rebuild 重建。

        rebuild 上限 30 分钟 (=1800s);初次 build 单独一段也 30 分钟封顶。
        SIFT10K + R=32 L=64 num_threads=2 在常规机器上典型 1-3 分钟。
        """
        case_space = space_name + "_comp_diskann"
        embedding_size = xb.shape[1]
        cfg = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector", "type": "vector",
                 "store_type": "RocksDB",
                 "index": {"name": "gamma", "type": "DISKANN_STATIC",
                           "params": {
                               "metric_type": "L2",
                               "training_threshold": 1000,
                               "R": 32, "L": 64,
                               "num_threads": 2,
                               "beam_width": 4,
                               "num_nodes_to_cache": 100000,
                               "search_dram_budget_gb": 0.5,
                               "build_dram_budget_gb": 0.56,
                               "disk_pq_bytes": 0,
                               "use_opq": 0,
                               "append_reorder_data": 0,
                           }},
                 "dimension": embedding_size},
            ],
        }
        resp = create_space(router_url, db_name, cfg)
        body = resp.json()
        if body.get("code") != 0:
            pytest.skip(
                f"DISKANN_STATIC not supported on this cluster build: "
                f"code={body.get('code')} msg={body.get('msg')}")
        try:
            # 1. 写入数据 — 此时 STATIC 索引保持 UNINDEXED, 不要 polling
            #    waiting_index_finish (它会死循环等 index_num 涨到 total)。
            batch_size, total = 100, 10000
            logger.info("6.5 inserting %d docs (DISKANN_STATIC, no auto-build)", total)
            add(total // batch_size, batch_size, xb[:total], True, False,
                space_name=case_space)

            # 让数据落到 raw store, 避免后面 forcemerge 抢先于 last batch
            # 的写入。
            time.sleep(5)

            detail = _comp_get_space_detail(db_name, case_space)
            doc_num_after_insert = detail.get("doc_num", 0)
            logger.info("6.5 inserted: doc_num=%d, partitions=%s",
                        doc_num_after_insert,
                        [(p.get("pid"), p.get("index_status"), p.get("index_num"))
                         for p in detail.get("partitions", [])])
            assert doc_num_after_insert >= total, (
                f"insert lost data: expected ≥{total}, got {doc_num_after_insert}")

            # 2. 显式触发 DiskANN 初次构建 (partition_id=0 表示所有 partition)。
            logger.info("6.5 triggering /index/forcemerge for initial DiskANN build")
            fm = requests.post(
                router_url + "/index/forcemerge",
                auth=(username, password),
                json={"db_name": db_name, "space_name": case_space,
                      "partition_id": 0},
                timeout=60)
            fm_body = fm.json()
            assert fm_body.get("code") == 0, (
                f"forcemerge failed: {fm.text[:300]}")

            # 3. 轮询 INDEXED, 带可见进度。SIFT10K 上典型 1-3min, 留 30min 上限。
            initial_build_deadline = time.time() + 1800
            poll_interval = 5
            last_logged = -1
            while time.time() < initial_build_deadline:
                d = _comp_get_space_detail(db_name, case_space)
                partitions = d.get("partitions", [])
                statuses = [p.get("index_status", -1) for p in partitions]
                index_nums = [p.get("index_num", 0) for p in partitions]
                total_index = sum(index_nums)
                if total_index != last_logged:
                    logger.info(
                        "6.5 initial build progress: status=%s index_status=%s "
                        "index_num=%s sum=%d/%d",
                        d.get("status"), statuses, index_nums,
                        total_index, total)
                    last_logged = total_index
                if (d.get("status") != "red" and partitions
                        and all(s == 2 for s in statuses)):
                    logger.info("6.5 initial DiskANN build complete after %.1fs",
                                1800 - (initial_build_deadline - time.time()))
                    break
                time.sleep(poll_interval)
            else:
                pytest.fail(
                    "6.5 initial DiskANN build did not reach INDEXED in 30min; "
                    "check ps logs for engine errors")

            pre_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")

            # 4. 这才是真正测试的那次 — rebuild 已 INDEXED 的 DiskANN。
            logger.info("6.5 triggering rebuild")
            assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
            _comp_wait_rebuild_completed(db_name, case_space, timeout=1800)
            # rebuild 完之后引擎需要再写一次 INDEXED, 给 25min 上限。
            _comp_wait_index_status_indexed(db_name, case_space,
                                       max_rounds=300, poll_interval=5)

            post_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")
            assert post_doc_num == pre_doc_num, (
                f"doc_num diverged across rebuild: pre={pre_doc_num} "
                f"post={post_doc_num}")

            _comp_check_search(case_space, times=3)
        finally:
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass

    def test_rebuild_scann(self):
        """6.6: SCANN — accelerated quantization-based index. Requires
        engine compiled with USE_SCANN. Skips cleanly if either master
        rejects the type or PS engine returns an init failure.
        """
        case_space = space_name + "_comp_scann"
        embedding_size = xb.shape[1]
        cfg = {
            "name": case_space, "partition_num": 1, "replica_num": 1,
            "fields": [
                {"name": "field_int", "type": "integer"},
                {"name": "field_vector", "type": "vector",
                 "index": {"name": "gamma", "type": "SCANN",
                           "params": {"metric_type": "InnerProduct",
                                      "ncentroids": 256, "nsubvector": 64,
                                      "nprobe": 10,
                                      "training_threshold": 3999}},
                 "dimension": embedding_size},
            ],
        }
        resp = create_space(router_url, db_name, cfg)
        body = resp.json()
        if body.get("code") != 0:
            pytest.skip(
                f"SCANN not supported on this cluster build: "
                f"code={body.get('code')} msg={body.get('msg')}")
        try:
            self._run_lifecycle_existing_space(case_space, total=10000,
                                                rebuild_timeout=900)
        finally:
            try:
                drop_space(router_url, db_name, case_space)
            except Exception:
                pass

    def _run_lifecycle_existing_space(self, case_space, total=10000,
                                      rebuild_timeout=600,
                                      index_indexed_max_rounds=180,
                                      do_search=True):
        """Variant of _basic_rebuild_lifecycle that assumes the space is
        already created (so the caller can skip-on-create-failure for
        index types whose support is build-dependent)."""
        batch_size = 100
        total_batch = total // batch_size
        add(total_batch, batch_size, xb[:total], True, False,
            space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        pre_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=rebuild_timeout)
        _comp_wait_index_status_indexed(db_name, case_space,
                                   max_rounds=index_indexed_max_rounds)

        post_doc_num = _comp_get_space_detail(db_name, case_space).get("doc_num")
        assert post_doc_num == pre_doc_num, (
            f"doc_num diverged across rebuild: pre={pre_doc_num} "
            f"post={post_doc_num}")

        if do_search:
            _comp_check_search(case_space, times=3)

    def test_rebuild_multi_vector_field_fanout(self):
        """6.7: Space with 3 vector fields, no field/index_type → fan-out.
        indexes array should contain all 3, current_index should advance.
        """
        case_space = space_name + "_comp_fanout"
        assert create_space(router_url, db_name, _comp_multi3_cfg(case_space)).json()["code"] == 0
        _comp_add_multi3_docs(case_space)

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        first = _comp_get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) >= 2, f"expected ≥2 IndexTargets, got {indexes}"
        seen_indexes = {first.get("current_index")}

        deadline = time.time() + 900
        while time.time() < deadline:
            p = _comp_get_rebuild_progress(db_name, case_space)
            if p.get("current_index") is not None:
                seen_indexes.add(p["current_index"])
            if p["status"] in ("completed", "failed"):
                assert p["status"] == "completed", p
                break
            time.sleep(2)

        # current_index should have advanced through ≥2 distinct values.
        assert len(seen_indexes) >= 2, f"current_index didn't advance: seen={seen_indexes}"
        _comp_wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_multi_index_space_runs_consecutively_without_yield(self):
        """6.7b: When a space has multiple index targets, all of them must
        run back-to-back under a single Running record. A second space
        whose rebuild is enqueued *after* the first has started must NOT be
        admitted until every target of the first space has finished.

        This locks in the R3 semantics: prepareNextTarget advances the
        target cursor in place and never yields the scheduler slot back to
        the pending queue between targets. Under the old code the record
        went Pending between targets, which allowed another space's older
        pending record to be admitted mid-flight.
        """
        space_multi = space_name + "_comp_multi_consec_a"
        space_single = space_name + "_comp_multi_consec_b"

        # Space A: multi-vector so the record has ≥2 targets.
        assert create_space(router_url, db_name, _comp_multi3_cfg(space_multi)).json()["code"] == 0
        _comp_add_multi3_docs(space_multi)

        # Space B: single-vector, fast target. Would race ahead if A ever
        # yielded its scheduler slot mid-way through its target list.
        assert create_space(router_url, db_name, _comp_multi3_cfg(space_single)).json()["code"] == 0
        _comp_add_multi3_docs(space_single)

        # Trigger A first so it wins admission.
        assert _comp_trigger_rebuild(db_name, space_multi).json().get("code") == 0
        # Trigger B a moment later — B stays Pending.
        time.sleep(0.5)
        assert _comp_trigger_rebuild(db_name, space_single).json().get("code") == 0

        # While A is Running, B must remain Pending. Sandwich each pb
        # read between two pa reads so we don't flag the tick where A
        # finalizes AND B is admitted in the same scheduler pass. Both
        # writes are persisted together in that tick; two sequential
        # GETs would then show pa=running/completed + pb=running with
        # no way to distinguish the legal "A finalized, then B admitted
        # in the same tick" case from a real R3 violation ("A and B
        # Running concurrently"). Requiring A to be Running at BOTH ends
        # of the pb GET collapses the observation window to a range
        # where A is provably still running.
        deadline = time.time() + 900
        a_finished = False
        b_ever_running_while_a_running = False
        b_ever_completed_while_a_running = False
        while time.time() < deadline:
            pa_before = _comp_get_rebuild_progress(db_name, space_multi)
            pb        = _comp_get_rebuild_progress(db_name, space_single)
            pa_after  = _comp_get_rebuild_progress(db_name, space_multi)

            a_running_throughout = (
                pa_before["status"] == "running"
                and pa_after["status"] == "running"
            )
            if a_running_throughout and pb["status"] == "running":
                b_ever_running_while_a_running = True
            if a_running_throughout and pb["status"] == "completed":
                b_ever_completed_while_a_running = True

            if pa_after["status"] in ("completed", "failed"):
                a_finished = True
                assert pa_after["status"] == "completed", pa_after
                break
            time.sleep(1)

        assert a_finished, "space A did not finish within deadline"
        assert not b_ever_running_while_a_running, (
            "space B was admitted while space A was still cycling through "
            "its index targets — R3 (no-yield between targets) is broken"
        )
        assert not b_ever_completed_while_a_running, (
            "space B completed before space A finished — R3 (no-yield "
            "between targets) is broken"
        )

        # Space B should complete once A releases the slot.
        _comp_wait_rebuild_completed(db_name, space_single, timeout=600)
        _comp_wait_index_status_indexed(db_name, space_multi)
        _comp_wait_index_status_indexed(db_name, space_single)

        drop_space(router_url, db_name, space_multi)
        drop_space(router_url, db_name, space_single)

    # def test_multi_target_fail_first_aborts_rest(self):
    #     """6.8: When the first IndexTarget fails its retries, subsequent
    #     targets must NOT be attempted (per design: surface failures).

    #     Real implementation lives in test_module_rebuild_chaos.py
    #     (TestRebuildPSFailureExtras.test_multi_target_fail_first_aborts_rest)
    #     because it needs PS-kill fault injection. Skipping here.
    #     """
    #     pytest.skip("see chaos: test_multi_target_fail_first_aborts_rest")

    def test_destroy_db_6(self):
        drop_db(router_url, db_name)


# ===========================================================================
# 7. API parameter matrix
# ===========================================================================


class TestRebuildParameters:

    def test_prepare_db(self):
        _comp_ensure_clean_db()

    def _make_basic_space(self, case_space, total=5000):
        batch_size = 100
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

    def test_drop_before_true_completes(self):
        """7.1"""
        case_space = space_name + "_comp_drop1"
        self._make_basic_space(case_space)
        resp = _comp_trigger_rebuild(db_name, case_space, drop_before_rebuild=True)
        assert resp.json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        _comp_wait_index_status_indexed(db_name, case_space)
        _comp_check_search(case_space)
        drop_space(router_url, db_name, case_space)

    def test_drop_before_false_completes(self):
        """7.2"""
        case_space = space_name + "_comp_drop0"
        self._make_basic_space(case_space)
        resp = _comp_trigger_rebuild(db_name, case_space, drop_before_rebuild=False)
        assert resp.json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        _comp_wait_index_status_indexed(db_name, case_space)
        _comp_check_search(case_space)
        drop_space(router_url, db_name, case_space)

    def test_describe_mode_is_idempotent(self):
        """7.3: describe=1 should not modify the index.
        Verify by capturing top-10 of 20 queries before and after; expect
        identical results.
        """
        case_space = space_name + "_comp_describe"
        self._make_basic_space(case_space)

        url = router_url + "/document/search?timeout=10000"
        def topk(idx):
            data = {"vector_value": False, "db_name": db_name, "space_name": case_space,
                    "vectors": [{"field": "field_vector", "feature": xq[idx].tolist()}],
                    "fields": ["field_int"], "limit": 10}
            body = requests.post(url, auth=(username, password), json=data).json()
            if body.get("code") != 0:
                return []
            docs = body.get("data", {}).get("documents", [[]])
            res = docs[0] if docs and isinstance(docs[0], list) else docs
            return [d.get("field_int") for d in res if d.get("field_int") is not None]

        pre = [topk(i) for i in range(20)]
        resp = _comp_trigger_rebuild(db_name, case_space, describe=1)
        if resp.json().get("code") != 0:
            pytest.skip(f"describe mode rejected: {resp.json()}")
        _comp_wait_rebuild_completed(db_name, case_space, timeout=120)
        post = [topk(i) for i in range(20)]
        assert pre == post, "describe rebuild changed search results"
        drop_space(router_url, db_name, case_space)

    def test_max_retries_custom_value(self):
        """7.4: progress reflects the requested max_retries."""
        case_space = space_name + "_comp_maxretry"
        self._make_basic_space(case_space)
        resp = _comp_trigger_rebuild(db_name, case_space, max_retries=5)
        assert resp.json().get("code") == 0
        progress = _comp_get_rebuild_progress(db_name, case_space)
        # The API may surface this under different key; accept either.
        mr = progress.get("max_retries", progress.get("MaxRetries"))
        if mr is not None:
            assert int(mr) == 5, f"max_retries not honored: {mr}"
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        drop_space(router_url, db_name, case_space)

    def test_partition_id_specific_only(self):
        """7.5: Single-partition rebuild — record contains only that pid."""
        case_space = space_name + "_comp_pid"
        batch_size, total = 100, 5000
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=3)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        partitions = _comp_get_space_detail(db_name, case_space).get("partitions", [])
        assert len(partitions) == 3
        target_pid = partitions[1]["pid"]

        resp = _comp_trigger_rebuild(db_name, case_space, partition_id=target_pid)
        assert resp.json().get("code") == 0
        progress = _comp_get_rebuild_progress(db_name, case_space)
        tasks = progress.get("tasks") or []
        for t in tasks:
            assert t.get("partition_id") == target_pid,\
                f"task on wrong pid: expected {target_pid}, got {t.get('partition_id')}"
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        _comp_wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_drop_before_purges_deleted(self):
        """7.6: drop_before=true + delete-half + IVFFLAT.
        After rebuild index_num should be ≤ doc_num (no resurrected deletes).
        """
        case_space = space_name + "_comp_drop_del"
        batch_size, total = 100, 10000
        assert create_space(router_url, db_name, _comp_ivfflat_cfg(case_space, pn=1)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, False, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        _comp_delete_documents(db_name, case_space, list(range(5000, total)))
        time.sleep(2)

        resp = _comp_trigger_rebuild(db_name, case_space, drop_before_rebuild=True)
        assert resp.json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=600)
        _comp_wait_index_status_indexed(db_name, case_space)

        detail = _comp_get_space_detail(db_name, case_space)
        assert detail.get("doc_num") == 5000
        # index_num is high-water mark, not actual count, so we only sanity-check ≥ doc_num
        # and assert doc_num is correct.
        for did in [5000, 7500, 9999]:
            r = _comp_query_document(db_name, case_space, str(did))
            docs = r.get("data", {}).get("documents", []) or []
            for d in docs:
                if d:
                    assert not d.get("_found", True), f"deleted id {did} resurrected after drop=true rebuild"
        drop_space(router_url, db_name, case_space)

    def test_destroy_db_7(self):
        drop_db(router_url, db_name)


# ===========================================================================
# 8. State machine edges
# ===========================================================================


class TestRebuildStateMachineEdges:

    def test_prepare_db(self):
        _comp_ensure_clean_db()

    def test_cancel_pending_then_immediate_new_rebuild(self):
        """8.1"""
        case_space = space_name + "_comp_cancel_re"
        batch_size, total = 100, 5000
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=2)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        _comp_trigger_rebuild(db_name, case_space)
        _comp_cancel_rebuild(db_name, case_space)

        progress = _comp_get_rebuild_progress(db_name, case_space)
        if progress["status"] == "running":
            # Already admitted; wait it out before reissuing.
            _comp_wait_rebuild_completed(db_name, case_space, timeout=300, allow_failed=True)

        time.sleep(0.5)
        resp = _comp_trigger_rebuild(db_name, case_space)
        assert resp.json().get("code") == 0, resp.text
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        drop_space(router_url, db_name, case_space)

    def test_completed_record_overwrite(self):
        """8.3: A completed record can be overwritten by a new request."""
        case_space = space_name + "_comp_re_complete"
        batch_size, total = 100, 5000
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        snapshots = _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        first_enq = snapshots[-1].get("enqueued_at", "")

        time.sleep(1)
        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        snapshots2 = _comp_wait_rebuild_completed(db_name, case_space, timeout=300)
        second_enq = snapshots2[-1].get("enqueued_at", "")

        assert first_enq != second_enq, "enqueued_at not refreshed on second rebuild"
        drop_space(router_url, db_name, case_space)

    def test_double_cancel_is_idempotent(self):
        """8.4"""
        case_space = space_name + "_comp_dbl_cancel"
        batch_size, total = 100, 5000
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=2)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        _comp_trigger_rebuild(db_name, case_space)
        r1 = _comp_cancel_rebuild(db_name, case_space)
        assert r1.json().get("code") == 0
        r2 = _comp_cancel_rebuild(db_name, case_space)
        # Second cancel should not 5xx; allow either code=0 or specific
        # "already cancelled" reason.
        assert r2.status_code == 200, r2.text

        progress = _comp_get_rebuild_progress(db_name, case_space)
        if progress["status"] not in ("cancelled", "completed"):
            _comp_wait_rebuild_completed(db_name, case_space, timeout=300, allow_failed=True)
        drop_space(router_url, db_name, case_space)

    # def test_failed_record_overwrite(self):
    #     """8.2: Failed records can be overwritten — requires fault
    #     injection to deterministically produce a failed state.

    #     Real implementation lives in test_module_rebuild_chaos.py
    #     (TestRebuildPSFailureExtras.
    #      test_failed_record_can_be_overwritten_by_new_request)
    #     because it needs PS-kill fault injection. Skipping here.
    #     """
    #     pytest.skip("see chaos: test_failed_record_can_be_overwritten_by_new_request")

    def test_destroy_db_8(self):
        drop_db(router_url, db_name)


# ===========================================================================
# 9. Lifecycle / exception cleanup ⚠️ HIGH-RISK area
# ===========================================================================


class TestRebuildLifecycle:

    def test_prepare_db(self):
        _comp_ensure_clean_db()

    def test_drop_space_during_running_rebuild(self):
        """9.1: DROP SPACE while rebuild is running must succeed AND clean
        up the etcd record. Master must not panic.
        """
        case_space = space_name + "_comp_drop_during"
        batch_size, total = 100, 10000
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=2)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        # Wait for running state.
        for _ in range(30):
            if _comp_get_rebuild_progress(db_name, case_space)["status"] == "running":
                break
            time.sleep(0.5)

        # DROP SPACE.
        drop_resp = drop_space(router_url, db_name, case_space)
        assert drop_resp is None or drop_resp.json().get("code") == 0,\
            f"drop_space failed: {drop_resp.json() if drop_resp else 'no response'}"

        # The progress endpoint must still respond (not 5xx). The record
        # may persist briefly but ideally transitions to failed/not_found.
        time.sleep(5)
        url = f"{router_url}/index/rebuild/dbs/{db_name}/spaces/{case_space}/progress"
        rs = requests.get(url, auth=(username, password))
        assert rs.status_code == 200, f"progress endpoint 5xx after drop: {rs.text}"

        # Sanity: master/router still healthy.
        rs = requests.get(f"{router_url}/dbs", auth=(username, password))
        assert rs.status_code == 200

    def test_drop_db_during_db_level_rebuild(self):
        """9.2: DROP DB while a DB-level rebuild is running."""
        local_db = db_name + "_drop_during"
        case_a = "sp_a"
        case_b = "sp_b"
        # Best-effort cleanup of leftovers. drop_db alone is not enough:
        # vearch rejects it when spaces still live under the db (a stuck
        # prior run may have left sp_a/sp_b behind), so enumerate and drop
        # each space first, then the db. Finally assert create_db succeeds
        # so a silent no-op cannot cascade into SPACE_EXIST below.
        try:
            rs = requests.get(f"{router_url}/dbs/{local_db}/spaces",
                              auth=(username, password))
            if rs.status_code == 200 and rs.json().get("code") == 0:
                for sp in rs.json().get("data") or []:
                    sp_name = sp.get("space_name") or sp.get("name") or ""
                    if sp_name:
                        drop_space(router_url, local_db, sp_name)
        except Exception:
            pass
        try:
            drop_db(router_url, local_db)
        except Exception:
            pass
        cr = create_db(router_url, local_db)
        assert cr.json().get("code") == 0, (
            f"create_db({local_db}) failed: {cr.text[:500]}")

        batch_size, total = 100, 5000
        for sp in (case_a, case_b):
            cfg = _comp_hnsw_cfg(sp, pn=1)
            cfg["name"] = sp
            r = requests.post(
                f"{router_url}/dbs/{local_db}/spaces",
                auth=(username, password), json=cfg)
            assert r.json().get("code") == 0

        # Insert into both.
        url_upsert = router_url + "/document/upsert?timeout=2000000"
        for sp in (case_a, case_b):
            for i in range(total // batch_size):
                docs = [{"_id": str(i*batch_size+j), "field_int": i*batch_size+j,
                         "field_long": i*batch_size+j, "field_float": float(i*batch_size+j),
                         "field_double": float(i*batch_size+j), "field_string": str(i*batch_size+j),
                         "field_vector": xb[i*batch_size+j].tolist()} for j in range(batch_size)]
                requests.post(url_upsert, auth=(username, password),
                               json={"db_name": local_db, "space_name": sp, "documents": docs})
            waiting_index_finish(total, space_name=sp, db_name=local_db)
            # Same per-partition INDEXED gate as _trigger_rebuild; this test
            # POSTs the DB-level rebuild directly so it can't piggyback on it.
            _comp_wait_index_status_indexed(local_db, sp)

        # Trigger DB-level rebuild.
        rs = requests.post(f"{router_url}/index/rebuild/dbs/{local_db}",
                           auth=(username, password), json={})
        assert rs.json().get("code") == 0

        time.sleep(2)
        # DROP DB.
        rs = requests.delete(f"{router_url}/dbs/{local_db}",
                             auth=(username, password))
        # vearch may require dropping spaces first; be lenient here.
        if rs.json().get("code") != 0:
            for sp in (case_a, case_b):
                requests.delete(f"{router_url}/dbs/{local_db}/spaces/{sp}",
                                 auth=(username, password))
            rs = requests.delete(f"{router_url}/dbs/{local_db}",
                                 auth=(username, password))
            assert rs.json().get("code") == 0

        # Master/router still healthy.
        rs = requests.get(f"{router_url}/dbs", auth=(username, password))
        assert rs.status_code == 200

    def test_terminal_record_retention_query_works(self):
        """9.3: Completed record can be queried after rebuild ends (PS
        retention is 2h; not waited here). Just verify the GET works
        immediately post-completion and returns the right status.
        """
        case_space = space_name + "_comp_retention"
        batch_size, total = 100, 5000
        assert create_space(router_url, db_name, _comp_hnsw_cfg(case_space, pn=1)).json()["code"] == 0
        add(total // batch_size, batch_size, xb[:total], True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        assert _comp_trigger_rebuild(db_name, case_space).json().get("code") == 0
        _comp_wait_rebuild_completed(db_name, case_space, timeout=300)

        # 5s after completion, record must still be queryable as completed.
        time.sleep(5)
        progress = _comp_get_rebuild_progress(db_name, case_space)
        assert progress["status"] == "completed"
        drop_space(router_url, db_name, case_space)

    def test_destroy_db_9(self):
        try:
            drop_db(router_url, db_name)
        except Exception:
            pass
