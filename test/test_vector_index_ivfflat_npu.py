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

import pytest
from utils.vearch_utils import *
from utils.data_utils import *

__description__ = """ test case for index NPU_IVFFLAT (Ascend NPU) """


# AscendIndexIVFFlat supports a fixed set of dimensions/nlists, only
# InnerProduct metric, and requires nlist*39 vectors at minimum for training.
# nytimes-256-angular (d=256, IP, ~290k vectors) is the only bundled dataset
# that fits all three constraints.
nytimes = DatasetNytimes()
xb = nytimes.get_database()
xq = nytimes.get_queries()
gt = nytimes.get_groundtruth()


def create(router_url, embedding_size, store_type="RocksDB", index_params={}):
    properties = {}
    properties["fields"] = [
        {
            "name": "field_int",
            "type": "integer",
            "index": {
                "name": "field_int",
                "type": "SCALAR",
            },
        },
        {
            "name": "field_vector",
            "type": "vector",
            "dimension": embedding_size,
            "store_type": store_type,
            "index": {
                "name": "gamma",
                "type": "NPU_IVFFLAT",
                "params": index_params,
            },
        },
    ]

    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    response = create_db(router_url, db_name)
    logger.info(response.json())

    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())


def query(recall_num, nprobe, batch, xq, gt, k):
    query_dict = {
        "vectors": [],
        "index_params": {
            "nprobe": nprobe,
            "recall_num": recall_num,
        },
        "vector_value": False,
        "fields": ["field_int"],
        "limit": k,
        "db_name": db_name,
        "space_name": space_name,
    }

    avarage, recalls = evaluate(xq, gt, k, batch, query_dict)
    result = (
        "batch: %-3d, nprobe: %-3d, recall_num: %-4d, avg: %-3.2f ms, "
        % (batch, nprobe, recall_num, avarage)
    )
    for recall in recalls:
        result += "recall@%-3d = %-3.2f%% " % (recall, recalls[recall] * 100)
    logger.info(result)

    if nprobe >= 32 and recall_num >= 100:
        assert recalls[1] >= 0.5
        assert recalls[10] >= 0.7
        assert recalls[100] >= 0.85


def benchmark(store_type, index_params, xb, xq, gt):
    embedding_size = xb.shape[1]
    batch_size = 100
    k = 100

    total = xb.shape[0]
    total_batch = int(total / batch_size)
    logger.info(
        "dataset num: %d, total_batch: %d, dimension: %d, ncentroids %d, search num: %d, topK: %d"
        % (
            total,
            total_batch,
            embedding_size,
            index_params["ncentroids"],
            xq.shape[0],
            k,
        )
    )

    create(router_url, embedding_size, store_type, index_params)

    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(total, 15)

    for recall_num in [100, 200, 300]:
        for nprobe in [16, 32, 64]:
            for batch in [0, 1]:
                query(recall_num, nprobe, batch, xq, gt, k)

    destroy(router_url, db_name, space_name)


@pytest.mark.npu
@pytest.mark.parametrize(
    ["store_type", "ncentroids"],
    [
        ["RocksDB", 1024],
        ["RocksDB", 2048],
    ],
)
def test_vearch_index_npu_ivfflat(store_type: str, ncentroids: int):
    """Recall test on Nytimes-256 with the supported (d=256) dimensions."""
    index_params = {}
    index_params["metric_type"] = "InnerProduct"
    index_params["ncentroids"] = ncentroids
    index_params["nprobe"] = 64
    benchmark(store_type, index_params, xb, xq, gt)


@pytest.mark.npu
def test_vearch_index_npu_ivfflat_invalid_dim():
    """The Ascend backend only supports d in {64,128,256,...}; d=130 must fail."""
    embedding_size = 130
    index_params = {
        "metric_type": "InnerProduct",
        "ncentroids": 1024,
        "nprobe": 64,
    }
    properties = {
        "fields": [
            {
                "name": "field_int",
                "type": "integer",
            },
            {
                "name": "field_vector",
                "type": "vector",
                "dimension": embedding_size,
                "store_type": "RocksDB",
                "index": {
                    "name": "gamma",
                    "type": "NPU_IVFFLAT",
                    "params": index_params,
                },
            },
        ]
    }
    space_config = {
        "name": space_name,
        "partition_num": 1,
        "replica_num": 1,
        "fields": properties["fields"],
    }
    create_db(router_url, db_name)
    response = create_space(router_url, db_name, space_config)
    logger.info(response.json())
    if response.json().get("code") == 0:
        destroy(router_url, db_name, space_name)
        pytest.fail("expected NPU_IVFFLAT to reject dimension=130, but space was created")
    drop_db(router_url, db_name)


@pytest.mark.npu
def test_vearch_index_npu_ivfflat_force_ip():
    """metric_type other than InnerProduct should be ignored (forced to IP)."""
    embedding_size = xb.shape[1]
    index_params = {
        "metric_type": "L2",
        "ncentroids": 1024,
        "nprobe": 64,
    }
    create(router_url, embedding_size, "RocksDB", index_params)

    batch_size = 100
    total = xb.shape[0]
    total_batch = int(total / batch_size)
    add(total_batch, batch_size, xb)
    if total - total_batch * batch_size:
        add(total - total_batch * batch_size, 1, xb[total_batch * batch_size:])

    waiting_index_finish(total, 15)

    query_dict = {
        "vectors": [],
        "index_params": {"nprobe": 64, "recall_num": 100},
        "vector_value": False,
        "fields": ["field_int"],
        "limit": 10,
        "db_name": db_name,
        "space_name": space_name,
    }
    _, recalls = evaluate(xq, gt, 10, 0, query_dict)
    # Even with L2 requested, the index should still return reasonable IP recall
    # against nytimes-256 groundtruth (which is IP-based).
    assert recalls[10] >= 0.5, (
        "NPU_IVFFLAT should fall back to InnerProduct silently; recall@10=%f"
        % recalls[10]
    )
    destroy(router_url, db_name, space_name)
