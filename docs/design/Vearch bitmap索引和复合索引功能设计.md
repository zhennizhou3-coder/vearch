# Vearch bitmap索引和复合索引功能设计

# Vearch bitmap索引和复合索引功能设计

# 背景

Vearch 当前只支持一种标量索引类型，即`SCALAR`类型（基于 RocksDB 的倒排索引）。在实际业务场景中，现有实现仍存在若干性能和功能上的瓶颈。

主要痛点：


1. 低基数字段查询效率不足：当字段值分布集中（如布尔类型、枚举类型、小基数整数字段）时，RocksDB 倒排索引需要遍历大量 postings list，I/O 开销显著



1. 多字段组合查询性能差：现有架构对多字段 AND/OR 组合过滤的处理是逐字段独立查询后再合并 bitmap，中间结果量大且内存占用高。而电商、推荐系统等场景中常见的&quot;品牌=xxx AND 价格区间=[100,500) AND 品类=手机&quot;这类精确多字段组合查询，缺乏专门的索引加速能力。


# 架构

# 实现

新增标量索引类型：INVERTED、BITMAP、COMPOSITE。其中INVERTED为之前版本的标量索引实现。为保持对之前版本的兼容，当指定SCALAR类型的标量索引时（之前版本只能指定这一种类型），会创建INVERTED类型的标量索引。

# 测试

建表语句

```plaintext
{
    "name": "space_bitmap",
    "fields": [
        {
            "name": "field_cardinality_1",
            "type": "int",
            "index": {
                "name": "field_cardinality_1",
                "type": "BITMAP"
            }
        },
        {
            "name": "field_cardinality_2",
            "type": "int",
            "index": {
                "name": "field_cardinality_2",
                "type": "BITMAP"
            }
        },
        {
            "name": "field_cardinality_10",
            "type": "int",
            "index": {
                "name": "field_cardinality_10",
                "type": "BITMAP"
            }
        },
        {
            "name": "field_cardinality_50",
            "type": "int",
            "index": {
                "name": "field_cardinality_50",
                "type": "BITMAP"
            }
        },
        {
            "name": "field_vector",
            "index": {
                "name": "gamma",
                "type": "FLAT",
                "params": {
                    "ncentroids": 4096,
                    "nlinks": 32,
                    "metric_type": "L2",
                    "efConstruction": 100
                }
            },
            "type": "vector",
            "dimension": 128
        }
    ],
    "replica_num": 1,
    "partition_num": 1
}
```

按照基数1、2、10、50创建四个字段，写入100万数据，检索请求为随机组合两个字段进行过滤

压测对比


| 索引类型 | 并发用户数 | 事务成功率(请求总次数) | TPS(最大/平均) | 响应时间(最大/最小/平均) | TP值(50/99/999) | 持续时长 |
|---|---|---|---|---|---|---|
| INVERTED | 10 | 99.97%(4960) | 28 / 16 | 4617 / 0 / 545 | 497 / 1714 / 3238 | 00:05:01 |
| BITMAP | 10 | 100%(71261) | 400 / 235 | 1063 / 0 / 38 | 29 / 118 / 251 | 00:05:01 |


## 商品数据测试

集群：[https://taishan.jd.com/vearch/cluster/nodeList?clusterId=11409&amp;clusterName=sc-same-test](https://taishan.jd.com/vearch/cluster/nodeList?clusterId=11409&clusterName=sc-same-test)

复合索引表

```plaintext
{
  "name": "test_composite_index",
  "partition_num": 19,
  "replica_num": 3,
  "fields": [
    {
      "name": "hr_bu_id",
      "type": "string"
    },
    {
      "name": "hr_dept_id_1",
      "type": "string"
    },
    {
      "name": "main_brand_code",
      "type": "string"
    },
    {
      "name": "item_third_cate_cd",
      "type": "string"
    },
    {
      "name": "is_global_flag",
      "type": "integer"
    },
    {
      "name": "emb",
      "index": {
        "name": "gamma",
        "type": "FLAT",
        "params": {
          "ncentroids": 4096,
          "nlinks": 32,
          "metric_type": "InnerProduct",
          "efConstruction": 100
        }
      },
      "type": "vector",
      "dimension": 128
    }
  ],
  "indexes": [
    {
      "name": "index_composite",
      "type": "COMPOSITE",
      "field_names": [
        "hr_bu_id",
        "hr_dept_id_1",
        "main_brand_code",
        "is_global_flag"
      ]
    }
  ]
}
```

单索引表

```plaintext
{
  "name": "test_scalar_index",
  "partition_num": 19,
  "replica_num": 3,
  "fields": [
          {
            "name": "hr_bu_id",
            "index": {
              "name": "hr_bu_id_idx",
              "type": "SCALAR"
            },
            "type": "string"
          },
          {
            "name": "hr_dept_id_1",
            "index": {
              "name": "hr_dept_id_1_idx",
              "type": "SCALAR"
            },
            "type": "string"
          },
          {
            "name": "main_brand_code",
            "index": {
              "name": "main_brand_code_idx",
              "type": "SCALAR"
            },
            "type": "string"
          },
          {
            "name": "item_third_cate_cd",
            "index": {
              "name": "item_third_cate_cd_idx",
              "type": "SCALAR"
            },
            "type": "string"
          },
          {
            "name": "is_global_flag",
            "index": {
              "name": "is_global_flag_idx",
              "type": "SCALAR"
            },
            "type": "integer"
          },
          {
            "name": "emb",
            "index": {
              "name": "gamma",
              "type": "FLAT",
              "params": {
                "ncentroids": 4096,
                "nlinks": 32,
                "metric_type": "InnerProduct",
                "efConstruction": 100
              }
            },
            "type": "vector",
            "dimension": 128
          }
        ]
}
```

查询语句

```plaintext
{
    "db_name": "db",
    "space_name": "test_composite_index", # "test_scalar_index"
    "limit": 100,
    "filters": {
        "conditions": [
            {
                "field": "hr_bu_id",
                "value": [
                    "00013807"
                ],
                "operator": "IN"
            },
            {
                "field": "hr_dept_id_1",
                "value": [
                    "00113128"
                ],
                "operator": "IN"
            },
            {
                "field": "main_brand_code",
                "value": [
                    "252165"
                ],
                "operator": "IN"
            },
            {
                "field": "is_global_flag",
                "value": 0,
                "operator": "="
            }
        ],
        "operator": "AND"
    }
}
```


| 查询索引类型 | 事务名称 | 并发用户数 | 事务成功率(请求总次数) | TPS(最大/平均) | 响应时间(最大/最小/平均) | TP值(50/99/999) | 持续时长 |
|---|---|---|---|---|---|---|---|
| Scalar | tran_0 | 10 | 99.95%(7034) | 23 / 11 | 4952 / 0 / 811 | 521 / 3371 / 4200 | 00:10:02 |
| Composite | tran_0 | 10 | 100%(406968) | 817 / 680 | 1061 / 0 / 14 | 13 / 26 / 194 | 00:09:58 |


