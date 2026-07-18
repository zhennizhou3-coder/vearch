# Vearch RabitQ集成设计文档

# Vearch RabitQ集成设计文档

# 背景

rabitq是一种新的向量量化算法，目前在同等压缩比例的情况下有更好的召回效果和速度，以下是对应的两篇论文和介绍，分别为1bit版本和多bit版本

[https://arxiv.org/abs/2405.12497](https://arxiv.org/abs/2405.12497)

[https://arxiv.org/abs/2409.09913](https://arxiv.org/abs/2409.09913)

[http://dev.to/gaoj0017/quantization-in-the-counterintuitive-high-dimensional-space-4feg](http://dev.to/gaoj0017/quantization-in-the-counterintuitive-high-dimensional-space-4feg)

[https://github.com/VectorDB-NTU/RaBitQ-Library](https://github.com/VectorDB-NTU/RaBitQ-Library)

[https://vectordb-ntu.github.io/RaBitQ-Library/compact_code/](https://vectordb-ntu.github.io/RaBitQ-Library/compact_code/)

# 架构

结构和IVFPQ一致，只不过量化方式从PQ变成RABITQ

![](./assets/Vearch%20RabitQ集成设计文档.assets/0lOEwA8ODUWylO4ZefEe.png)
# 实现

支持增删改查，支持search指定精排（向量索引召回之后使用原始向量再次计算排序）

# 使用

向量字段索引类型设置为IVFRABITQ

```plaintext
{
  "name": "benchmark",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "field_vector",
      "index": {
        "name": "gamma",
        "type": "IVFRABITQ",
        "params": {
          "ncentroids": 4096,
          "nb_bits": 4, # 默认值
          "metric_type": "L2",
          "qb": 4 # 默认值，检索时用
        }
      },
      "type": "vector",
      "dimension": 128
    }
  ],
  "replica_num": 3,
  "partition_num": 1
}
```

# 测试

## 功能自测

代码分支：

[http://xingyun.jd.com/codingRoot/vearch/vearch/merges/714](http://xingyun.jd.com/codingRoot/vearch/vearch/merges/714)

测试用例可以参见对应pr中test部分的修改

部署分支：

[http://xingyun.jd.com/codingRoot/VDP/deploy/tree/feat_rabitq](http://xingyun.jd.com/codingRoot/VDP/deploy/tree/feat_rabitq)

测试应用：

[http://xingyun.jd.com/jdosCD/ls/group/vearch-compare](http://xingyun.jd.com/jdosCD/ls/group/vearch-compare)

## 性能压测

数据集


| 数据集 | 向量维度 | 数据量 | 下载链接 |
|---|---|---|---|
| ANN_SIFT1B | 128 | 10亿 | [http://corpus-texmex.irisa.fr/](http://corpus-texmex.irisa.fr/) |


数据保存地址

[https://taishan.jd.com/cfs/fileTabs?volumeId=1111&amp;volumeName=serverless_test3&amp;currPath=%2Fsift1B%2F](https://taishan.jd.com/cfs/fileTabs?volumeId=1111&volumeName=serverless_test3&currPath=%2Fsift1B%2F)

数据导入脚本：

sift1B/parallel_write_to_vearch.sh

召回计算脚本：

sift1B/cal_recall_vearch.py

压测数据：

IVFRABITQ sift1B/bigann_query_vearch_ivfrabitq.json

IVFPQ sift1B/bigann_query_vearch_ivfpq.json

压测使用forcebot平台进行压测

### 场景一

索引参数

```plaintext
{
  "name": "IVFRABITQ",
  "partition_num": 10,
  "replica_num": 1,
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "field_vector",
      "index": {
        "name": "gamma",
        "type": "IVFRABITQ",
        "params": {
          "nb_bits": 4,
          "qb": 4,
          "ncentroids": 40000,
          "metric_type": "L2",
          "training_threshold": 10000000
        }
      },
      "type": "vector",
      "dimension": 128
    }
  ]
}
```

索引构建性能

可以看到IVFRABITQ nb_bits为1且无精排召回较差，精排为检索参数不影响索引训练和构建


| 索引 | 参数 | 索引训练时间（小时） | 索引构建时间（小时） | 压缩比和内存占用 | 召回 |
|---|---|---|---|---|---|
| IVFRABITQ | nb_bits：4 | 1.68 | 2.61 | ![](./assets/Vearch%20RabitQ集成设计文档.assets/ksHnxsFy2iGJZeeMXlHU.png)<br>6倍压缩 | recall@1 = 70.21%<br>recall@10 = 98.38%<br>recall@100 = 98.65% |
| IVFRABITQ | nb_bits：1 | 1.78 | 2.28 | 21倍压缩 | recall@1 = 7.20%<br>recall@10 = 33.52%<br>recall@100 = 74.42% |
| IVFRABITQ | nb_bits：1<br>recall_num: 200 | 1.78 | 2.28 | 21倍压缩 | recall@1 = 97.38%<br>recall@10 = 97.44%<br>recall@100 = 97.44% |
| IVFPQ | nb_bits：8，nsubvector=64 | 1.48 | 2 | 8倍压缩 | recall@1 = 74.74%<br>recall@10 = 96.84%<br>recall@100 = 96.90% |


检索性能


| 索引 | 参数 | 事务名称 | 并发用户数 | 事务成功率(请求总次数) | TPS(最大/平均) | 响应时间(最大/最小/平均) | TP值(50/99/999) | 持续时长 |
|---|---|---|---|---|---|---|---|---|
| IVFRABITQ | nb_bits：4 | tran_0 | 10 | 100%(137457) | 473 / 228 | 1908 / 0 / 41 | 39 / 66 / 87 | 00:10:01 |
| IVFRABITQ | nb_bits：4 | tran_0 | 20 | 100%(151405) | 401 / 251 | 1638 / 0 / 75 | 80 / 122 / 238 | 00:10:01 |
| IVFRABITQ | nb_bits：4 | tran_0 | 30 | 100%(149782) | 323 / 248 | 3180 / 0 / 113 | 102 / 199 / 327 | 00:10:01 |
|   |   |   |   |   |   |   |   |   |
| IVFRABITQ | nb_bits：1<br>recall_num: 200 | tran_0 | 10 | 100%(169658) | 336 / 281 | 3016 / 0 / 34 | 31 / 56 / 246 | 00:10:01 |
| IVFRABITQ | nb_bits：1<br>recall_num: 200 | tran_0 | 20 | 100%(210515) | 607 / 349 | 2671 / 0 / 54 | 52 / 96 / 393 | 00:10:01 |
| IVFRABITQ | nb_bits：1<br>recall_num: 200 | tran_0 | 30 | 100%(208651) | 419 / 346 | 2740 / 0 / 81 | 89 / 177 / 662 | 00:10:01 |
|   |   |   |   |   |   |   |   |   |
| IVFPQ | nb_bits：8，nsubvector=64 | tran_0 | 10 | 100%(32278) | 101 / 53 | 2003 / 0 / 175 | 158 / 439 / 698 | 00:10:01 |
| IVFPQ | nb_bits：8，nsubvector=64 | tran_0 | 20 | 100%(18431) | 49 / 30 | 4551 / 0 / 609 | 437 / 2300 / 3261 | 00:10:01 |


IVFRABITQ

```plaintext
{
  "name": "IVFRABITQ",
  "partition_num": 10,
  "replica_num": 1,
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "field_vector",
      "index": {
        "name": "gamma",
        "type": "IVFRABITQ",
        "params": {
          "nb_bits": 4,
          "qb": 4,
          "ncentroids": 40000,
          "metric_type": "L2",
          "training_threshold": 10000000,
          "hnsw" : {
            "nlinks": 32,
            "efConstruction": 200,
            "efSearch": 64
          }
        }
      },
      "type": "vector",
      "dimension": 128
    }
  ]
}
```

IVFPQ

```plaintext
{
  "name": "IVFPQ",
  "partition_num": 10,
  "replica_num": 1,
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "field_vector",
      "index": {
        "name": "gamma",
        "type": "IVFPQ",
        "params": {
          "nsubvector": 64,
          "ncentroids": 40000,
          "metric_type": "L2",
          "training_threshold": 10000000,
          "hnsw" : {
            "nlinks": 32,
            "efConstruction": 200,
            "efSearch": 64
          }
        }
      },
      "type": "vector",
      "dimension": 128
    }
  ]
}
```

 


