# Vearch 实时查询功能设计

# Vearch 实时查询功能设计

# 背景

因为向量索引训练期间无法进行向量检索（需要训练完才能开始构建索引）以及

Vearch向量索引为异步构建，因此数据无法实时可见。对于一些实时性要求较高的场景比如交易等，需要保证数据插入即可见，因此需要实现Vearch实时查询功能。

# 架构

![](./assets/Vearch%20实时查询功能设计.assets/IBgBmFBfhrbdRYrpPNAB.png)
通过实时向量buffer实现实时功能：实时向量buffer分段存储，检索起点为index_count，终点为写入数据总量始终保持向量索引数据+实时向量buffer为一份全量数据。当index_count逐步增加，分段启动过期淘汰释放，以降低内存占用和重复计算。

# 实现

实时向量buffer支持增改查（无需支持删除，分段过期自动释放）

# 使用

建表时添加参数&quot;enable_realtime&quot;: true即开启实时功能。

```plaintext
建表参数示例
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
        "type": "HNSW", # IVFPQ
        "params": {
          "ncentroids": 4096,
          "nlinks": 32,
          "metric_type": "L2",
          "efConstruction": 160
        }
      },
      "type": "vector",
      "dimension": 128
    }
  ],
  "replica_num": 3,
  "partition_num": 1,
  "enable_realtime": true
}
```

测试应用

[http://xingyun.jd.com/jdosCD/ls/CI/publish-version](http://xingyun.jd.com/jdosCD/ls/CI/publish-version)

![](./assets/Vearch%20实时查询功能设计.assets/BAe7ZJ62wsERTqKfD7sy.png)
测试代码分支

[http://xingyun.jd.com/codingRoot/vearch/vearch/merges/712](http://xingyun.jd.com/codingRoot/vearch/vearch/merges/712)

部署代码分支

[http://xingyun.jd.com/codingRoot/VDP/deploy/tree/feat_realtime](http://xingyun.jd.com/codingRoot/VDP/deploy/tree/feat_realtime)

# 测试

可以看到实时查询解决的是向量查询实时性问题，即解决的是search接口实时性，因此测试需要通过search接口测试验证。

可以参考测试用例：[https://github.com/vearch/vearch/blob/master/test/test_module_realtime.py](https://github.com/vearch/vearch/blob/master/test/test_module_realtime.py)

