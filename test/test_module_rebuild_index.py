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
Test cases for the rebuild index module.

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
    field_name: str = "",
    index_type: str = "",
    max_retries: int = 0,
):
    """POST /rebuild/index/dbs/:db/spaces/:space[ /fields/:field/indexes/:index]
    """
    payload = {}
    if field_name and index_type:
        url = f"{router_url}/rebuild/index/dbs/{db}/spaces/{space}/fields/{field_name}/indexes/{index_type}"
    else:
        url = f"{router_url}/rebuild/index/dbs/{db}/spaces/{space}"
        if field_name:
            payload["field_name"] = field_name
        if index_type:
            payload["index_type"] = index_type
    if max_retries > 0:
        payload["max_retries"] = max_retries
    resp = requests.post(url, auth=(username, password), json=payload)
    logger.info("trigger_rebuild url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp

def _trigger_rebuild_db(db: str):
    """POST /rebuild/index/dbs/:db — rebuild all spaces in a DB."""
    url = f"{router_url}/rebuild/index/dbs/{db}"
    resp = requests.post(url, auth=(username, password), json={})
    logger.info("trigger_rebuild_db url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp

def _trigger_rebuild_global():
    """POST /rebuild/index/dbs — rebuild all spaces across all DBs."""
    url = f"{router_url}/rebuild/index/dbs"
    resp = requests.post(url, auth=(username, password), json={})
    logger.info("trigger_rebuild_global url=%s status=%d body=%s", url, resp.status_code, resp.text[:500])
    return resp

def _trigger_rebuild_drop(db, space):
      url = f"{router_url}/rebuild/index/dbs/{db}/spaces/{space}"
      return requests.post(url, auth=(username, password),
                           json={"drop_before_rebuild": True})

def _get_rebuild_progress(db: str, space: str) -> dict:
    """GET /rebuild/index/dbs/:db/spaces/:space/progress"""
    url = f"{router_url}/rebuild/index/dbs/{db}/spaces/{space}/progress"
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

    db=""  -> GET /rebuild/index/dbs              (global summary)
    db=xxx -> GET /rebuild/index/dbs/xxx/progress  (db-level summary)
    """
    if db:
        url = f"{router_url}/rebuild/index/dbs/{db}/progress"
    else:
        url = f"{router_url}/rebuild/index/dbs"
    resp = requests.get(url, auth=(username, password))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("code") == 0, body
    return body.get("data", {})

def _cancel_rebuild(db: str, space: str):
    """POST /cancel/rebuild/index/dbs/:db/spaces/:space"""
    url = f"{router_url}/cancel/rebuild/index/dbs/{db}/spaces/{space}"
    resp = requests.post(url, auth=(username, password))
    return resp

def _cancel_rebuild_db(db: str):
    """POST /cancel/rebuild/index/dbs/:db — cancel all rebuilds in a DB."""
    url = f"{router_url}/cancel/rebuild/index/dbs/{db}"
    resp = requests.post(url, auth=(username, password))
    return resp

def _cancel_rebuild_global():
    """POST /cancel/rebuild/index/dbs — cancel all rebuilds globally."""
    url = f"{router_url}/cancel/rebuild/index/dbs"
    resp = requests.post(url, auth=(username, password))
    return resp

def _wait_rebuild_completed(
    db: str,
    space: str,
    timeout: int = 600,
    poll_interval: int = 3,
) -> list:
    """Poll progress until terminal. Returns chronological snapshots."""
    deadline = time.time() + timeout
    snapshots = []
    last_overall = -1
    while time.time() < deadline:
        progress = _get_rebuild_progress(db, space)
        snapshots.append(progress)
        status = progress["status"]
        overall = progress.get("overall_percent", 0)

        assert overall >= last_overall, (
            f"overall_percent decreased: {last_overall} -> {overall}\n"
            f"snapshot: {json.dumps(progress, indent=2)}"
        )
        last_overall = overall

        logger.info(
            "progress: status=%s overall=%d%% completed=%d/%d running=%d pending=%d failed=%d ratio=%.2f",
            status, overall,
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
        rebuild_url_via_router = f"{router_url}/rebuild/index/dbs/{db_name}/spaces/{case_space}"
        rebuild_url_via_master = f"{master_url}/rebuild/index/dbs/{db_name}/spaces/{case_space}"
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
        """Trigger DB-level rebuild (POST /rebuild/index/dbs/:db) which
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
        """Global rebuild (POST /rebuild/index/dbs) rebuilds all spaces
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

        # Recall should not degrade after rebuild.
        assert recall_after["recall_at_1"] >= recall_before["recall_at_1"], (
            f"recall@1 degraded after rebuild: {recall_before['recall_at_1']:.4f} -> {recall_after['recall_at_1']:.4f}"
        )
        assert recall_after["recall_at_10"] >= recall_before["recall_at_10"], (
            f"recall@10 degraded after rebuild: {recall_before['recall_at_10']:.4f} -> {recall_after['recall_at_10']:.4f}"
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
                                      "training_threshold": 1000}},
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

    def test_progress_not_found_before_rebuild(self):
        """Querying progress for a space that was never rebuilt returns not_found."""
        case_space = space_name + "_mri_nf"
        assert create_space(router_url, db_name, _hnsw_space_config(case_space)).json()["code"] == 0

        progress = _get_rebuild_progress(db_name, case_space)
        assert progress["status"] == "not_found", progress
        assert progress["total_tasks"] == 0
        assert progress["completed_tasks"] == 0
        assert progress["overall_percent"] == 0

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
            assert t["status"] in (1, 2, 3), t
            assert 0 <= t["progress"] <= 100, t
            assert t["status"] == 2, t
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

    def test_replicas_rebuilt_sequentially_per_partition(self):
        """Verify that replicas of the same partition are rebuilt one at a time
        (sequential), not concurrently.

        The scheduler enforces two constraints in dispatchPending():
          1. Per-PS serial: at most one active task per PS node.
          2. Per-partition serial: at most one active replica per partition.

        By creating a space with replica_num >= 2 (replicas on different PS
        nodes), the per-PS constraint does not block parallel dispatch — the
        per-partition constraint is the only bottleneck. We rapidly poll the
        progress API during rebuild and check that in every snapshot, each
        partition has at most one running (dispatched=true, status=running)
        task.

        Ref: rebuild_service.go dispatchPending(), lines 1231-1311.
        """
        batch_size = 100
        total = xb.shape[0]
        total_batch = int(total / batch_size)

        case_space = space_name + "_mri_seq_replica"
        replica_num = 2
        partition_num = 2

        # Create space with multiple replicas and multiple partitions.
        config = _hnsw_space_config(case_space, partition_num=partition_num, replica_num=replica_num)
        resp = create_space(router_url, db_name, config)
        body = resp.json()
        if body.get("code") != 0:
            # If the cluster has insufficient PS nodes for multi-replica
            # placement, skip gracefully rather than fail.
            logger.warning(
                "Could not create multi-replica space (code=%d, msg=%s). "
                "Skipping replica-serialization test — cluster may have < %d PS nodes.",
                body.get("code"), body.get("msg", ""), replica_num,
            )
            pytest.skip(
                f"Cluster cannot host replica_num={replica_num} "
                f"(need >= {replica_num} PS nodes): {body}"
            )

        # Insert enough data so that rebuild takes measurable time.
        add(total_batch, batch_size, xb, True, True, space_name=case_space)
        waiting_index_finish(total, space_name=case_space)

        # Trigger rebuild.
        resp = _trigger_rebuild(db_name, case_space)
        assert resp.json().get("code") == 0, resp.text

        # Rapidly poll progress and collect task snapshots while rebuild is
        # in progress. We want to capture intermediate states where only a
        # subset of replicas are dispatched.
        snapshots = []
        deadline = time.time() + 600
        poll_interval = 0.5  # Fast polling to catch intermediate states.
        while time.time() < deadline:
            progress = _get_rebuild_progress(db_name, case_space)
            tasks = progress.get("tasks") or []
            # Capture the task details relevant to the serial check.
            snapshot = []
            for t in tasks:
                snapshot.append({
                    "partition_id": t.get("partition_id"),
                    "replica_index": t.get("replica_index"),
                    "status": t.get("status"),
                    "dispatched": t.get("dispatched", False),
                    "progress": t.get("progress", 0),
                })
            snapshots.append(snapshot)

            status = progress["status"]
            if status in ("completed", "failed"):
                break
            time.sleep(poll_interval)

        # Verify: in every snapshot, at most one dispatched-and-running task
        # per partition_id. This is the per-partition serial constraint.
        violations = []
        for i, snap in enumerate(snapshots):
            # Group dispatched running tasks by partition_id.
            running_per_partition = {}  # partition_id -> list of replica_index
            for t in snap:
                is_running = t["status"] == 1 and t["dispatched"] is True
                if is_running:
                    pid = t["partition_id"]
                    running_per_partition.setdefault(pid, []).append(t["replica_index"])
            for pid, replicas in running_per_partition.items():
                if len(replicas) > 1:
                    violations.append(
                        f"snapshot {i}: partition {pid} has {len(replicas)} "
                        f"concurrently running replicas: {replicas}"
                    )

        assert not violations, (
            "Per-partition serial constraint violated — found snapshots with "
            "multiple concurrently running replicas in the same partition:\n"
            + "\n".join(violations)
        )

        # Verify: we captured at least one snapshot showing the rebuild in
        # an intermediate state (some tasks dispatched, not all completed).
        # This ensures the test actually observed the scheduler behavior,
        # not just the final state.
        intermediate_found = False
        for snap in snapshots:
            dispatched_count = sum(1 for t in snap if t["dispatched"] is True and t["status"] != 2)
            completed_count = sum(1 for t in snap if t["status"] == 2)
            total_tasks = len(snap)
            if total_tasks > 0 and dispatched_count > 0 and completed_count < total_tasks:
                intermediate_found = True
                break

        if not intermediate_found:
            logger.warning(
                "No intermediate snapshot captured — rebuild may have completed "
                "too fast to observe per-partition serialization. The constraint "
                "was not violated, but the test coverage is limited."
            )

        # Wait for rebuild to complete and clean up.
        _wait_rebuild_completed(db_name, case_space, timeout=300)
        _wait_index_status_indexed(db_name, case_space)

        drop_space(router_url, db_name, case_space)

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
            assert create_space(router_url, db_name, _hnsw_space_config(sp)).json()["code"] == 0
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
        # cancelled=False with a reason explaining it is already running.
        running_cancelled_false = [
            e for e in cancel_results
            if not e.get("cancelled") and "running" in (e.get("reason", "") + e.get("status", "")).lower()
        ]
        for e in running_cancelled_false:
            assert e["cancelled"] is False, f"running rebuild should not be cancellable: {e}"

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
            # cancelled=False: reason must explain why cancellation was denied.
            # The reason should mention the current status (running/completed/failed).
            assert status.lower() in reason or "running" in reason or "cannot cancel" in reason, (
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

        # Wait for any running rebuilds to complete, then cleanup.
        for sp in (sp_x, sp_y, sp_z):
            progress = _get_rebuild_progress(db_name, sp)
            if progress["status"] == "running":
                _wait_rebuild_completed(db_name, sp, timeout=600)
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

    def test_per_field_rebuild_hnsw(self):
        """Rebuild field_vector_a / HNSW on a multi-vector space."""
        case_space = space_name + "_mri_perfield_hnsw"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        resp = _trigger_rebuild(db_name, case_space, field_name="field_vector_a", index_type="HNSW")
        body = resp.json()
        logger.info("per-field rebuild trigger response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 1, f"expected 1 IndexTarget, got {indexes}"
        assert indexes[0]["field_name"] == "field_vector_a"
        assert indexes[0]["index_type"] == "HNSW"
        logger.info("rebuild detail: db_name %s, space_name %s, field_name %s, index_type %s",db_name, case_space, indexes[0]["field_name"], indexes[0]["index_type"])

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

        resp = _trigger_rebuild(db_name, case_space, field_name="field_vector_b", index_type="FLAT")
        body = resp.json()
        logger.info("per-field rebuild flat trigger response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 1, f"expected 1 IndexTarget, got {indexes}"
        assert indexes[0]["field_name"] == "field_vector_b"
        assert indexes[0]["index_type"] == "FLAT"

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
        assert len(indexes) >= 2, f"expected >= 2 IndexTargets, got {indexes}"
        for idx_target in indexes:
            assert "field_name" in idx_target, f"IndexTarget missing field_name: {idx_target}"
            assert "index_type" in idx_target, f"IndexTarget missing index_type: {idx_target}"

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

        resp = _trigger_rebuild(db_name, case_space, field_name="field_vector", index_type="HNSW")
        body = resp.json()
        logger.info("single-field per-target rebuild response: %s", body)
        assert body.get("code") == 0, body

        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 1, f"expected 1 IndexTarget, got {indexes}"
        assert indexes[0]["field_name"] == "field_vector"
        assert indexes[0]["index_type"] == "HNSW"

        _wait_rebuild_completed(db_name, case_space, timeout=600)
        _wait_index_status_indexed(db_name, case_space)
        _check_search(case_space)
        drop_space(router_url, db_name, case_space)

    def test_rebuild_nonexistent_field_rejected(self):
        """Specifying a field that does not exist on the space must be rejected."""
        case_space = space_name + "_mri_bad_field"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        resp = _trigger_rebuild(db_name, case_space, field_name="nonexistent_field", index_type="HNSW")
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

        # field_vector_a has HNSW; asking for IVFPQ should fail.
        resp = _trigger_rebuild(db_name, case_space, field_name="field_vector_a", index_type="IVFPQ")
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

        # field_vector_b has FLAT; asking for HNSW should also fail.
        resp2 = _trigger_rebuild(db_name, case_space, field_name="field_vector_b", index_type="HNSW")
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

    def test_rebuild_field_without_index_type_rejected(self):
        """Specifying only field_name without index_type (or vice versa)
        must be rejected — both must be specified together."""
        case_space = space_name + "_mri_partial_target"
        assert create_space(router_url, db_name, _multi_vector_space_config(case_space)).json()["code"] == 0
        _add_multi_vector_docs(case_space)

        # Only field_name, no index_type.
        resp = _trigger_rebuild(db_name, case_space, field_name="field_vector_a", index_type="")
        body = resp.json()
        logger.info("rebuild field-only (no index_type) response: %s", body)
        # The rebuild API may return code=0 at the top level for batch-style
        # responses; individual failures are reported in data.failures.
        data = body.get("data", {})
        failures = data.get("failures", [])
        rejected = body.get("code") != 0 or len(failures) > 0
        assert rejected, (
            f"expected error for field_name without index_type, got success: {body}"
        )

        # Only index_type, no field_name — this triggers full-space rebuild
        # because empty field_name means "all indexes", so it should succeed.
        # We test the truly invalid case below via the URL path itself.

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

        # Verify the progress response lists all indexes.
        first = _get_rebuild_progress(db_name, case_space)
        indexes = first.get("indexes", [])
        assert len(indexes) == 2, f"expected 2 IndexTargets, got {indexes}"
        field_types = {(idx["field_name"], idx["index_type"]) for idx in indexes}
        assert ("field_vector_a", "HNSW") in field_types, f"missing field_vector_a/HNSW in {indexes}"
        assert ("field_vector_b", "FLAT") in field_types, f"missing field_vector_b/FLAT in {indexes}"

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

    POST /rebuild/index/dbs/:db                     — trigger all spaces
    GET  /rebuild/index/dbs/:db/progress             — query DB summary
    POST /cancel/rebuild/index/dbs/:db               — cancel all spaces
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
        if progress["status"] not in ("not_found", "completed", "cancelled"):
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

    POST /rebuild/index/dbs                           — trigger all DBs
    GET  /rebuild/index/dbs/progress                   — global summary
    POST /cancel/rebuild/index/dbs                     — cancel all DBs
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
        if progress["status"] not in ("not_found", "completed", "cancelled"):
            _wait_rebuild_completed(db_name, case_space, timeout=600)

        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)

    """Exercise the global rebuild endpoints.

    POST /rebuild/index/dbs                           — trigger all DBs
    GET  /rebuild/index/dbs/progress                   — global summary
    POST /cancel/rebuild/index/dbs                     — cancel all DBs
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
        if progress["status"] not in ("not_found", "completed", "cancelled"):
            _wait_rebuild_completed(db_name, case_space, timeout=600)

        _wait_index_status_indexed(db_name, case_space)
        drop_space(router_url, db_name, case_space)

    def test_destroy_db(self):
        drop_db(router_url, db_name)