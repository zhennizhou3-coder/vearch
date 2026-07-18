# FAISS 与 Vearch 训练采样对比

## 背景

FAISS 的 `subsample_training_set` 面向已经准备好的连续训练数组。Vearch
的采样发生在 RawVector 存储层，需要先排除 bitmap 标记的已删除向量，再将
选中的向量组成连续训练 buffer 传给 FAISS。

## 流程对比

| 维度 | FAISS `subsample_training_set` | Vearch `SampleTrainingVectors` |
| --- | --- | --- |
| 输入 | 连续数组 `x[0..nx)` | Memory/RocksDB 中的 RawVector |
| 删除处理 | 不处理，调用方保证输入有效 | 扫描 bitmap，跳过 tombstone |
| 采样对象 | 连续数组行号 | 有效向量的 docid |
| 输出 | 新建连续 `x_new`，可选同步复制 weights | 新建连续训练 vector buffer |
| 样本数 | `k * max_points_per_centroid` | 最多为调用方传入的 `num` |
| 有效数量 | 不单独统计 | 返回 `valid_count` 供训练门槛判断 |
| 权重 | 支持采样 `weights` | 不涉及权重 |

## FAISS 采样

FAISS 根据聚类参数计算目标样本数：

```text
target = clus.k * clus.max_points_per_centroid
```

随后从已有训练数组中选取 `target` 行，并复制到新的连续 buffer。它不感知
文档删除状态；传入的 `x` 必须已经是可用于训练的有效数据。

### 标准模式

标准模式调用 `rand_perm` 生成 `[0, nx)` 的随机排列，再取前 `target` 个下标：

```cpp
std::vector<int> int_perm(nx);
rand_perm(int_perm.data(), nx, actual_seed);
```

特点：

- 均匀、无重复采样。
- 使用 `O(nx)` 内存保存完整排列。
- 要求 `nx <= INT_MAX`。因为 `int_perm` 的元素类型是 `int`，当
  `nx > INT_MAX` 时，FAISS 会直接抛异常，而不是自动切换模式：

  ```text
  Dataset too large (...) for standard subsampling;
  set use_faster_subsampling=true
  ```

  调用方必须显式将 `use_faster_subsampling` 设为 `true`，才能对这类超大
  数据集使用快速路径。

### 快速模式

开启 `use_faster_subsampling` 后，FAISS 重复生成随机下标：

```cpp
perm[i] = rng.rand_int64() % nx;
```

特点：

- 仅使用 `O(target)` 内存。
- 可能抽到重复样本，因为每次生成独立下标。
- 适用于无法承受完整 permutation，或 `nx > INT_MAX` 的超大训练集。

## Vearch 采样

Vearch 使用 Reservoir Sampling（Algorithm R）：

```text
遍历所有 docid
  -> 跳过 bitmap 标记的删除向量
  -> 维护最多 num 个有效 docid 的 reservoir
  -> 从存储层读取这些 docid 对应的向量
  -> 复制为连续训练 buffer
```

其关键性质：

- 每个存活向量被保留的概率相同。
- 不会重复选择同一个 docid。
- 额外 ID 内存为 `O(num)`，而非 `O(valid_count)`。
- 需要 `O(total)` 扫描，才能识别有效向量并得到 `valid_count`。

例如，若先收集 10 亿个有效 docid，`int64_t` ID 列表约需：

```text
1,000,000,000 * 8 bytes = 8 GB
```

reservoir 只保存训练所需的 `num` 个 ID。例如 100 万个 ID 约为 8 MB。

## 随机数生成器选择

### FAISS

FAISS 使用由 `clus.seed` 派生的 `actual_seed`：

```cpp
const uint64_t actual_seed = get_actual_rng_seed(clus.seed);
```

FAISS 根据采样模式使用两种不同的伪随机数生成器。两者同一 seed、同一调用顺序
下都可复现，适合调试和基准测试。

`clus.use_faster_subsampling` 默认值为 `false`，因此默认走标准的
`RandomGenerator + rand_perm` 无重复排列路径；只有显式设置为 `true` 才使用
SplitMix64 快速采样。

| 生成器 | 使用位置 | 核心状态 | 单次输出 | 主要用途 |
| --- | --- | --- | --- | --- |
| `RandomGenerator` | 标准 subsampling | `std::mt19937` | 32 位伪随机值 | 生成完整随机排列 |
| `SplitMix64RandomGenerator` | `use_faster_subsampling=true` | 一个 64 位整数 | 64 位混合后的值 | 低状态开销地生成大量独立下标 |

### `RandomGenerator` 和 Fisher-Yates 排列

`RandomGenerator` 将 `seed` 转为 `unsigned int` 后初始化 `std::mt19937`：

```cpp
RandomGenerator::RandomGenerator(int64_t seed) : mt((unsigned int)seed) {}
```

`rand_perm` 先构造顺序数组，再执行 Fisher-Yates shuffle：

```cpp
for (size_t i = 0; i < n; i++) {
    perm[i] = i;
}
for (size_t i = 0; i + 1 < n; i++) {
    int i2 = i + rng.rand_int(n - i);
    std::swap(perm[i], perm[i2]);
}
```

因此前 `target` 个元素构成无重复训练样本。这个路径的代价是必须保存长度为
`nx` 的 permutation。`RandomGenerator::rand_int64()` 通过两个 31 位结果拼出
更宽的整数；`rand_float()` 和 `rand_double()` 则将 `mt()` 归一化到 `[0, 1]`。

### `SplitMix64RandomGenerator`

快速采样使用 SplitMix64 的 64 位状态推进和位混合：

```cpp
uint64_t z = (state += 0x9e3779b97f4a7c15ULL);
z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
return z ^ (z >> 31);
```

它只维护一个 64 位 state，生成速度快、状态很小，适合快速模式直接产生：

```cpp
perm[i] = rng.rand_int64() % nx;
```

这不是排列：每次下标独立生成，所以允许重复样本。快速模式以少量状态和
`O(target)` 内存换取“可能重复”的近似采样。

### 取模偏差

FAISS 两个生成器的 `rand_int(max)` 都使用 `% max`。当随机数取值范围不是
`max` 的整数倍时，理论上会有极小的 modulo bias。对于训练下采样通常可以接受；
如果需要严格无偏的有界随机数，应使用 rejection sampling 或
`std::uniform_int_distribution`。

### Vearch

Vearch 当前使用：

```cpp
std::mt19937 rng(std::random_device{}());
```

`std::mt19937` 是 32 位 Mersenne Twister，和 FAISS 标准路径底层使用的
`std::mt19937` 属于同一类生成器；区别在于 Vearch 不暴露固定 seed，而是用
`std::random_device` 初始化。通常每次训练会获得不同样本，有利于避免固定前缀
或固定采样模式。

取舍如下：

| 选择 | 优点 | 代价 |
| --- | --- | --- |
| `random_device + mt19937` | 每次采样通常不同，无需暴露 seed 配置；无重复 reservoir 采样 | 不保证跨平台可复现；受限平台的 `random_device` 可能退化为确定性来源 |
| FAISS `RandomGenerator` | 固定 seed 可复现；适合 Fisher-Yates 无重复排列 | 标准 subsampling 需要 `O(nx)` permutation 内存 |
| FAISS `SplitMix64` | 64 位状态小，适合超大数据集快速生成下标 | 快速模式允许重复样本；使用 `% max` 有理论 modulo bias |
| 显式 seed 的 Vearch reservoir | 可复现，便于测试和问题复盘，同时保持 `O(num)` 内存 | 相同 seed 会重复同一采样结果，需要管理 seed 策略 |

当前 Vearch 的目标是在线训练时均匀随机抽取存活向量，因此选择
`random_device + mt19937` 是合理的。若未来需要稳定的 CI 结果、可复现的索引
构建或离线调试，可在训练参数中增加显式 seed，并将其传入 reservoir RNG。

## `num` 的来源与采样协同

Vearch caller 把样本数 `num` 控制在 FAISS Clustering 的 sweet spot
内，使得训练向量送入 FAISS 后**不再触发 FAISS 内部的二次采样**——两层
采样合并为一层：

```text
training_threshold (用户配置)
        ↓
IndexModel::ComputeIVFTrainingNum(nlist)        ← 在 IndexModel 基类中
        ↓ 三段式 clamp
num ∈ [nlist * min_points_per_centroid,         ← 39
       nlist * max_points_per_centroid]         ← 256
        ↓
IndexModel::GetTrainingVectors(num, ...)
        ↓ reservoir 采样 + bitmap 过滤
train_data (num 个 contiguous bytes)
        ↓
faiss::Index::train(num, train_data)
        ↓ FAISS 内部判断 nx > k * max_points_per_centroid?
        ↓ 我们传入的 num <= nlist * 256，不会触发 subsample_training_set
直接进 Clustering 主流程
```

设计意图：

- 39 / 256 这两个数取自 FAISS `Clustering` 默认值
  (`min_points_per_centroid` / `max_points_per_centroid`)，对应 FAISS
  在 `points/centroid < 39` 时告警 "training set too small"，在
  `points/centroid > 256` 时启用内部 subsample。
- 把 num 限制在区间内，FAISS 拿到的就是它想要的样本数：既不抱怨样本
  太少，也不需要内部再做一次随机选择。
- 由 caller 一次性完成 "选样本" 这件事——存储层（带 tombstone 过滤）
  + 训练层（FAISS 期待的样本量）一次解决，而不是 vearch 选完之后
  faiss 再选一次。

样本数三段式的具体行为：

| training_threshold (用户配置) | 采用的 num | 行为 |
| --- | --- | --- |
| `< nlist`              | `nlist * 39`  | clamp 上调到下限，warning |
| `[nlist*39, nlist*256]` | `training_threshold` | 直接采用 |
| `> nlist * 256`        | `nlist * 256` | clamp 下调到上限，warning |

如果实际 `valid_count`（去掉 tombstone 之后的存活向量数）小于该
`num`，`GetTrainingVectors` 返回 -1，caller 的 `Indexing()` 也返回
-1，partition 进入 UNINDEXED 状态，等待数据增长后下一次自动触发
BuildIndex。这是 vearch 既有的"宁缺毋滥"策略——不在样本不足时训练
低质量索引。

## 概率特性对比（精确版）

| 模型 | 单次采样某向量被选中的概率 | 是否可重复 |
| --- | --- | --- |
| FAISS 标准模式（rand_perm 取前 N） | 严格 `target / nx` | 否 |
| FAISS 快速模式（`rng % nx` × N 次有放回）| `1 - (1 - 1/nx)^target ≈ target/nx`（target<<nx 时） | 是，可重复 |
| Vearch reservoir（Algorithm R） | 严格 `num / valid_count` | 否 |

三者中只有 **vearch reservoir** 同时满足：
- 严格均匀概率（不像 faiss 快速模式 是近似）
- 不重复（不像 faiss 快速模式）
- 单 pass O(num) 内存（不像 faiss 标准模式 需要 O(nx) perm）
- 不需要预先知道 nx（faiss 两种模式都需要）

## 结论

FAISS 的采样解决的是连续训练数组的下采样问题；Vearch 的采样解决的是从带有
tombstone 的原始向量存储中构造有效训练集的问题。两者最终都输出连续 buffer，
但 Vearch 必须额外处理删除过滤、存储读取和超大有效 ID 集的内存约束。
