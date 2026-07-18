# 人工确认节点清单（Confirmation Gates）

每个 ✅ 节点写完产物后**必须停下**，向用户输出请求确认的提示，收到明确"OK/继续"才推进。其余答复一律按反馈归档并修订。

| Gate | 阶段 | 产物文件 | 必答问题 |
| --- | --- | --- | --- |
| G1 | S1 需求 | `docs/01-requirement.md` | 1. 范围与边界是否完整？ 2. 非功能指标是否合理？ 3. 是否有遗漏的上下游影响？ |
| G2 | S2 概设 | `docs/02-high-level-design.md` | 1. 架构与备选方案是否合理？ 2. 一致性与恢复路径是否清晰？ 3. 容量预估是否可信？ |
| G3 | S3 拆解 | `docs/03-submodule-breakdown.md` | 1. 拆解粒度是否合适？ 2. 迭代顺序是否符合依赖？ |
| G4.1 | S4.1 黑盒用例（每模块） | `modules/<m>/04-blackbox-cases.md` | 1. 故障模式是否覆盖完整？ 2. 性能基线阈值是否合理？ |
| G4.2 | S4.2 详设（每模块） | `modules/<m>/05-detailed-design.md` | 1. 不变式是否完整？ 2. 并发与错误处理是否充分？ |
| G4.4 | S4.4 评审（每模块） | `modules/<m>/06-code-review.md` | 1. 是否同意 finding 处理结论？ 2. 是否需要二次评审？ |
| G4.7 | S4.7 压测（每模块） | `modules/<m>/09-stress-test.md` | 1. 关键指标是否达标？ 2. 是否需要扩大规模？ |
| G5 | S5 整体回归 | `docs/08-regression-report.md` | 1. 是否允许进入 PR 阶段？ |
| G6 | S6 PR | `docs/10-pr-summary.md` | 1. PR 是否可合入？ |
| G7 | S7 复盘 | `retrospective/verification.md` | 1. 是否同意复盘结论？ 2. 是否需要追加一轮改进？ |

## 通用确认输出格式
```
📋 [<Gate ID>] 产物已写入 <path>
关键决策：
  - <要点 1>
  - <要点 2>
  - <要点 3>
请确认：
  1. <问题 1>
  2. <问题 2>
回复 "OK / 继续" 推进，或直接给出反馈。
```
