# 代码评审清单（数据库与存储专项）

通用项 + DB/存储专项。评审 subagent 与人工都应对照此清单出 finding。

## A. 正确性
- [ ] 边界条件：空、单元素、大对象、UINT_MAX、负数、NaN
- [ ] 错误路径：每个错误码都有处理与日志
- [ ] 资源释放：文件、句柄、锁、内存全部 RAII / defer
- [ ] 并发安全：共享状态是否受保护；原子操作的内存序是否正确
- [ ] 重入与可重启：函数能否在任意一行 crash 后从 WAL/Checkpoint 恢复

## B. 数据与持久化
- [ ] 落盘前是否 `fsync` / `fdatasync`，顺序是否正确
- [ ] WAL/Journal 写入顺序与 commit 标记
- [ ] Crash 在 fsync 前后的不变式
- [ ] 校验和：写入与读取路径都验证
- [ ] 字节序、对齐、版本号字段
- [ ] 文件 truncate / rename 的原子性
- [ ] 大端/小端兼容；不同 CPU 架构

## C. 事务与一致性
- [ ] 隔离级别是否符合声明
- [ ] 读视图 / 快照是否在事务期内稳定
- [ ] 锁顺序是否一致，是否存在死锁
- [ ] 副本一致性协议的边界（落后副本、网络分区）
- [ ] 时钟依赖：是否使用单调时钟，是否依赖 wall clock

## D. 性能与资源
- [ ] 是否在热路径分配内存 / 拷贝大对象
- [ ] 锁粒度是否过大；是否可用 RWLock / lock-free
- [ ] 是否有 O(N) 查找可改为 O(log N) / O(1)
- [ ] 是否有读放大、写放大、空间放大未评估
- [ ] 缓存命中率与失效策略
- [ ] 反压（backpressure）与限流

## E. 兼容性与升降级
- [ ] 新写入的数据老版本能否读
- [ ] 老版本数据新版本能否读，遇到未知字段如何处理
- [ ] Schema 版本号、Magic Number
- [ ] 滚动升级与回滚步骤是否被测试覆盖

## F. 可观测性
- [ ] 关键路径有计数器 / 直方图
- [ ] 错误日志包含足够上下文（请求 ID、key、offset）
- [ ] 日志级别合理，不在热路径打 info 日志
- [ ] Trace 关联 ID 是否贯穿

## G. 安全
- [ ] 用户输入 / 网络数据是否校验长度与格式
- [ ] 反序列化是否防御恶意构造
- [ ] 文件路径是否防穿越
- [ ] 鉴权与审计

## H. 测试
- [ ] 详设中的每个不变式都有对应测试
- [ ] 错误注入测试覆盖：磁盘满、写失败、读失败、网络分区、kill -9
- [ ] 性能回归基线已经写入

## I. 文档与命名
- [ ] 公开 API 有注释（不变式、错误码、复杂度）
- [ ] 命名一致，避免歧义缩写
- [ ] 关键文件头部说明文件格式版本

## J. Vearch 专项不变量（CLAUDE.md §5）

任意一项疑似违反 → **Blocking**。本段优先调用 `vearch-code-review` skill；本清单作为兜底自检。

- [ ] **写入必经 raft**：所有数据变更经 `internal/ps/storage/raftstore`；diff 中没有直接调用 engine 的写路径。
- [ ] **副本不可低于 `ReplicaNum`**：member 变更先 add 后 remove；不允许相反顺序。
- [ ] **anti-affinity 保持**：迁移目标保留 zone/rack/host 隔离。
- [ ] **不跨 `ResourceName` 迁移**：迁移与调度均限定在同一 resource pool。
- [ ] **etcd key 走 service 层**：业务路径不直接 mutate `internal/entity/meta.go` 中定义的 key；通过 `internal/master/services/*` 间接访问。
- [ ] **生成的 `.pb.go` 未被手改**：检查 diff 是否触及 `internal/proto/vearchpb/*.pb.go`；任何改动应来自 `.proto` 重新生成。
- [ ] **cgo 编译 tag**：涉及 cgo 引擎的测试与构建命令带 `-tags="vector"`。
- [ ] **C++ 引擎改动触发完整 relink**：修改 `internal/engine/` 后已运行 `make all` 或等价完整构建。
