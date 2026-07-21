---
name: db-dev-flow
description: 数据库与存储研发团队的端到端研发流程 skill。按"需求 → 概设 → 子模块拆解 → 子模块迭代（黑盒用例 → 详设 → 代码 → 评审 → 白盒 → 回归 → 压测） → 回归 → PR → 复盘"推进，每个关键节点停下等待人工反馈，所有产物（需求/设计/测试文档、代码分支、评审结果、缺陷、反馈、完整对话）沉淀到代码库。复盘阶段在 git worktree 中重新跑一遍改进后的 skill 验证缺陷率与反馈数下降。
---

# db-dev-flow — 数据库/存储研发流程 skill

本 skill 用于驱动**数据库与存储**方向的端到端研发任务。Skill 调用后，Claude 在仓库根下的 `docs/db-dev-flow/<feature-slug>/` 建立项目目录，按阶段推进，并在每个关键节点写完文档后**停下来等人工反馈**，反馈写入归档后再继续。Skill 本体目录（`.claude/skills/db-dev-flow/`）只读：模板/清单/提示词供加载，不被运行产物污染。

> **Skill 调用方式**：用户使用 `/db-dev-flow <feature 描述>` 或在对话中提到需要走这套流程时触发；可带 `--resume <feature-slug>` 续跑，带 `--stage <stage-id>` 跳到指定阶段。

---

## 0. 启动与项目目录建立

1. 解析 feature 主题，生成 kebab-case `<feature-slug>`（不超过 48 字符），告知用户并允许调整。
2. 在仓库根下的 `docs/db-dev-flow/<feature-slug>/` 创建目录骨架（不要写进 `skills/` 目录）：
   ```
   docs/  modules/  reviews/  feedback/  conversation/  retrospective/
   STATUS.md  branches.md
   ```
   并复制 `templates/` 中各模板到对应位置作为初始草稿（仅复制需要的阶段）。
3. 在 Vearch 仓库（即本仓库；skill 与业务代码同仓）创建 feature 主分支 `feature/<feature-slug>`；S6 拆 PR 时再按子模块切子分支 `feature/<feature-slug>/<module-id>`，所有分支名追加进 `branches.md`。skill 改动（仅 S7 复盘阶段触发）以 `chore(skill): ...` 前缀且 path 限定 `.claude/skills/db-dev-flow/`，与业务 commit 区分。
4. 初始化 `STATUS.md`：当前阶段 = `S1-requirement`，下一步 = "等待用户提供原始需求"。
5. **commit**（须用户授权，详见 §3.1）：`chore(<feature-slug>): bootstrap project skeleton`。

> commit 策略见 §3.1：每个 ✅ 节点产出汇总后**请求用户授权再 commit**，message 格式 `<stage>(<feature-slug>): <动词短语>`。中间草稿用 TaskCreate / 未追踪文件管理，**不自动 commit**。

---

## 0.5 Skill 协作约定

本 skill 是**编排层**，不重写通用能力。每个阶段开始前必须 invoke 下列 skill；本 skill 仅串联与归档。

| 阶段 | 必须前置 invoke |
|---|---|
| S1 需求 | `superpowers:brainstorming` |
| S2 概设 / S3 拆解 | `superpowers:writing-plans` |
| S4.1 黑盒 / S4.5 白盒 | `superpowers:test-driven-development`（**white-box 的"先红失败用例"步骤须前置到 S4.3 之前**：S4.1 黑盒固化外部契约 → S4.5 单元测试先写失败用例 → S4.3 写代码使其通过 → 回到 S4.5 补足分支与不变式覆盖。S4 节序号不变，但 TDD red→green→refactor 顺序优先于本 skill 章节顺序） |
| S4.3 代码生成 | `superpowers:executing-plans` 或 `superpowers:subagent-driven-development` |
| S4.4 评审 | **`vearch-code-review` 优先** + 本 skill 的 `prompts/code-review-prompt.md` 补充；finding 处理用 `superpowers:receiving-code-review` |
| 任意 ✅ 节点提交前 | `superpowers:verification-before-completion`（先跑构建/测试/grep 拿到证据，再向用户报告"已完成"） |
| S6 PR | `superpowers:finishing-a-development-branch` + `pr-review-single-purpose`（拆分自审） |
| S7.2.3 worktree 重生 | `superpowers:using-git-worktrees` |
| 子模块涉及 `internal/engine/`（任意阶段） | `architecture`（CLAUDE.md §10 强约束） |

未在表中列出的能力（commit/PR 创建命令、ETCD/raft 调试、压测数据采集）保留在本 skill 内。

---

## 1. 阶段定义与人工确认节点

每个阶段格式："**输入 → 产出文件 → 等待确认 → 反馈归档 → 继续**"。所有阶段产出 commit 后，在 `STATUS.md` 更新当前阶段。

### S1 需求文档（人工确认 ✅）
- **输入**：用户原始描述、参考资料链接。
- **产出**：`docs/01-requirement.md`（套用 `templates/01-requirement.md`），包含：背景、功能/非功能需求、数据模型变更、兼容性、SLA、上下游影响、明确的不做范围。
- **行动**：写完后向用户输出"📋 需求文档草稿已写入 `docs/01-requirement.md`，请确认或提出修订"，**停止推进**，等待用户反馈。
- **反馈归档**：用户反馈追加到 `feedback/S1-requirement.md`（含时间、原文、采纳/未采纳判断、修订摘要）。修订后再次确认，直至用户明确 OK。
- **commit**：`req(<feature-slug>): finalize requirement`。

### S2 概要设计（人工确认 ✅）
- **输入**：定稿需求。
- **产出**：`docs/02-high-level-design.md`（套用 `templates/02-high-level-design.md`），包含：架构图（文字/mermaid）、模块清单、关键接口、数据流、存储格式与索引、一致性/持久化策略、容量与性能预估、风险与备选方案。
- 重点强调数据库/存储侧的：**Schema 变更与迁移**、**回滚路径**、**容量增长**、**热点与分片**、**事务/一致性模型**、**故障域**、**备份恢复**。
- 等待人工确认；反馈归档到 `feedback/S2-high-level-design.md`。
- **commit**：`design(<feature-slug>): finalize high-level design`。

### S3 子模块拆解（人工确认 ✅）
- **输入**：定稿概设。
- **产出**：`docs/03-submodule-breakdown.md`：每个子模块一节，含：边界、依赖、输入输出、负责人/Owner、迭代顺序、风险等级。
- 同时为每个子模块在 `modules/<module-id>/` 下建空目录，放入该子模块用到的模板（黑盒/详设/白盒/评审/压测/回归各一份）。
- 等待人工确认拆解合理后再推进。反馈归档到 `feedback/S3-breakdown.md`。
- **commit**：`design(<feature-slug>): submodule breakdown`。

### S4 子模块迭代（每个子模块循环执行；每个内部节点都需要人工确认 ✅）

对 `modules/<module-id>` 中的**每一个**子模块按顺序：

#### S4.1 黑盒测试用例（人工确认 ✅）
- 产出：`modules/<module-id>/04-blackbox-cases.md`。
- 内容：等价类/边界、错误注入、并发、持久化/重启、扩缩容、迁移、性能基线、回归项、可观测性断言。
- 等待用户确认覆盖完整。反馈归档 `feedback/<module-id>-blackbox.md`。
- commit：`test(<feature-slug>/<module-id>): blackbox cases`。

#### S4.2 详细设计（人工确认 ✅）
- 产出：`modules/<module-id>/05-detailed-design.md`：类/函数清单、伪代码、状态机、磁盘格式/字节布局、锁与并发、错误码、关键不变式、与黑盒用例的对应表。
- 等待确认，反馈归档 `feedback/<module-id>-detail.md`。
- commit：`design(<feature-slug>/<module-id>): detailed design`。

#### S4.3 代码生成（写入 Vearch 仓库 feature 分支）
- **TDD 顺序**（参见 §0.5）：写代码前先把 S4.5 的"红失败用例"产出到 `modules/<module-id>/07-whitebox-tests.md`；再写代码让用例转绿。
- 严格按详设实现；每个有意义的逻辑单元独立 commit，commit message 引用详设小节编号。
- 同步把生成时的对话片段追加到 `conversation/<module-id>.jsonl`（一行一条 user/assistant 消息）。
- 完成后不要立刻继续，**先进入 S4.4 评审**。

#### S4.4 代码评审（人工确认 ✅）
- 调用 `prompts/code-review-prompt.md` 作为评审 subagent 的指令，对本模块 diff 评审。
- 评审结果写入 `modules/<module-id>/06-code-review.md`：每条 finding 含位置、严重度、建议、是否采纳。
- 将所有 finding 汇总进 `reviews/<module-id>-summary.md`。
- 等待人工确认评审结论，是否需要返工。反馈归档 `feedback/<module-id>-review.md`。
- 修改后再次评审，直到无 Blocking finding。
- commit：`review(<feature-slug>/<module-id>): address review findings`。

#### S4.5 白盒测试
- "红失败用例"已在 S4.3 之前写出（见 §0.5 TDD 顺序）；本步在代码完成后**补足分支与不变式覆盖**：未覆盖路径、错误注入、并发死锁、cgo 边界。
- cgo 包必须带 `-tags="vector"`。
- 跑测试，把覆盖率与失败用例写入 `modules/<module-id>/07-whitebox-tests.md`。
- 如失败，回到 S4.3/S4.4 修复。
- commit：`test(<feature-slug>/<module-id>): whitebox tests`。

#### S4.6 模块级回归
- 跑现有回归 suite，结果（pass/fail、耗时、差异）记录到 `modules/<module-id>/08-regression-report.md`。
- 失败 → 修复 → 再跑。

#### S4.7 压力测试（人工确认 ✅）
- 依据 `checklists/stress-test-checklist.md` 设计压测脚本，记录指标（吞吐、P50/P99、错误率、资源占用、磁盘 IO、内存峰值），结果写 `modules/<module-id>/09-stress-test.md`。
- 等待人工确认压测达标。反馈归档 `feedback/<module-id>-stress.md`。
- commit：`perf(<feature-slug>/<module-id>): stress test results`。

> 当前子模块全部 ✅ 后，切到下一个子模块从 S4.1 开始。

### S5 整体回归（人工确认 ✅）
- 全量回归（功能 + 性能基线 + 兼容性 + 升降级）。
- 报告写入 `docs/08-regression-report.md`，覆盖所有模块。
- 等待人工确认。反馈归档 `feedback/S5-regression.md`。
- commit：`test(<feature-slug>): full regression`。

### S6 提交 PR（人工确认 ✅）

**PR 拆分原则**（强制；对照 `pr-review-single-purpose` 与 CLAUDE.md §6 单一目的原则）：
- **一 PR 一目的**。子模块 a/b/c 之间相互独立（不属于同一最小可发布单元）时，按子模块分别建子分支并提交独立 PR。
- 仅当多个子模块构成不可分割的最小发布单元（缺一不可）时，才合并成一个 feature PR。
- 多 PR 场景下用**伞 PR 或 tracking issue** 串联，不要默认把 feature 视为单 PR。
- S6 启动前**必须** invoke `pr-review-single-purpose` 自审本次范围；越过 issue/feature 边界的均需拆分。
- 拆分场景下，**每个子 PR 须独立通过 S5 同等规模回归**（仅合入已回归通过的子 PR；未通过的保留在伞 issue 跟踪）。
- push 前 invoke `superpowers:finishing-a-development-branch` 走完成前自检。

- push `feature/<feature-slug>` 或子分支并 `gh pr create`（须用户授权）。
- PR 标题/正文使用 `templates/10-pr-summary.md`，含：背景、范围、风险、回滚步骤、测试矩阵、压测数据、迁移说明、Reviewer 关注点。
- 把 PR URL、reviewer 列表、关键讨论快照存入 `docs/10-pr-summary.md`。
- 等待人工 review 与 merge 决策。反馈归档 `feedback/S6-pr.md`。
- commit（须用户授权）：`docs(<feature-slug>): record PR submission`。

### S7 复盘分析（人工确认 ✅，含 worktree 重生验证）
见下方 §2 完整流程。

---

## 2. 复盘分析（S7）

### 2.1 缺陷与反馈聚合
- 扫描 `feedback/` 全部文件 + `reviews/` 全部 finding + 所有阶段的 STATUS 变更日志。
- 产出 `retrospective/analysis.md`（套用 `templates/11-retrospective.md`）：
  - **按阶段统计反馈条数、缺陷数、采纳率**；
  - **缺陷分类**：需求歧义 / 设计漏洞 / 实现错误 / 测试缺失 / 文档错误 / 流程缺陷；
  - **根因分析**：每类缺陷指向到 skill 流程的哪一环；
  - **改进项清单**：每项明确"改 skill 的哪个文件、改成什么、为什么"。

### 2.2 Skill 与文档改进
- 根据改进项清单，**直接修改本 skill 的源文件**：`SKILL.md`、`templates/*.md`、`checklists/*.md`、`prompts/*.md`。
- 改动日志写入 `retrospective/skill-improvements.md`（套用 `templates/12-skill-improvement.md`），逐条记录"改前 → 改后 → 期望解决的缺陷类型"。
- commit（须用户授权，path 限定 `.claude/skills/db-dev-flow/`）：`skill(<feature-slug>): improve skill from retro`。同时把改进前的 skill 快照保存到 `docs/db-dev-flow/<feature-slug>/retrospective/skill-snapshot-before/`，改进后到 `.../skill-snapshot-after/`，便于回看。

### 2.3 Worktree 重新生成验证（关键）
目的：在改进后的 skill 上**重新跑一遍同一个 feature**，对比缺陷率与反馈数是否下降。

步骤：
1. 先 invoke `superpowers:using-git-worktrees`，再 `EnterWorktree({name: "<feature-slug>-regen"})` 在 Vearch 仓库内开干净 worktree。skill 改动与业务代码副本均独立于 master；重生产物统一沉淀到 worktree 内 `docs/db-dev-flow/<feature-slug>-regen/`，**不要回写 master**。
2. 在 worktree 中：
   - 用改进后的 skill，输入与首次完全相同的原始需求；
   - **跳过人工确认环节**，改为：每个原本需要确认的节点，自动用首次的"已采纳反馈"作为隐式答复（来自 `feedback/` 归档），其余不修改；
   - 完整跑完 S1 → S6（不再跑复盘），产出沉淀到 worktree 内的同名项目目录。
3. 跑完后对比：
   - **对比基准**：首次跑 S5 通过时的 feature 主分支末端 commit（拆 PR 场景下取所有已合入子 PR 在 master 的合并点；记录在 `branches.md`）。
   - 用 `git diff --stat` + 逐文件对比基准代码 vs worktree 内重生代码；
   - 重生过程中每个阶段产生的**模拟反馈条数**（即如果 skill 改得好，模拟评审/模拟用例评估应该挑不出新问题）；
   - 重新对重生代码跑一次 `prompts/code-review-prompt.md` 评审，统计 finding 数与严重度分布。
4. 把对比结果写入 `retrospective/verification.md`（套用 `templates/13-regen-verification.md`）：
   - 首次 vs 重生：反馈条数、缺陷条数、评审 finding 数、压测关键指标；
   - 是否达到"缺陷与反馈最小化"目标；未达成则列出残余问题与下一轮改进项；
   - 把重生代码的 diff 摘要、评审结果文件链接进来。
5. 退出 worktree：`ExitWorktree({action: "keep"})` 保留供人工查看；将 worktree 路径写入 `retrospective/regen/worktree-path.txt`。

### 2.4 复盘确认
- 向用户汇报：缺陷与反馈下降比例、残余风险、skill 改动一览；
- 等待人工最终确认复盘结论。反馈归档 `feedback/S7-retro.md`。
- commit（须用户授权）：`retro(<feature-slug>): finalize retrospective`。

---

## 3. 通用规则

### 3.1 Commit 与沉淀
- **commit 须用户显式授权**：每个 ✅ 节点产出汇总后向用户请求授权再 commit，**不自动批量 commit**；中间过程用 TaskCreate / 未追踪文件草稿跟踪。
- message 前缀按阶段：`req/design/test/perf/review/docs/skill/retro/chore`。
- skill 与业务代码同仓（Vearch）：业务改动落 `feature/<feature-slug>` 分支；skill 改动（仅 S7）以 `chore(skill): ...` 前缀且 path 限定 `.claude/skills/db-dev-flow/`。`branches.md` 记录所有相关分支。
- 完整对话历史：每个阶段结束时，把本阶段的 user/assistant 关键往返追加到 `conversation/<stage>.md`（不必逐 token 全量，但要能复盘决策路径）。

### 3.2 人工确认机制
- 任何标记 ✅ 的节点：写完文档/产出后立即输出一段"待确认提示"，包含：
  1. 本节点产出文件路径；
  2. 三条以内的关键决策摘要；
  3. 明确问题列表（"以下点是否 OK？"）。
- 然后**停止推进**，不要自动进入下一阶段，等用户回复。
- 用户回复"OK / 继续"才推进；其他回复全部按反馈处理：先归档，再修订对应产物，再次请求确认。

### 3.3 STATUS.md 维护
每次阶段切换或确认收到必须更新 `STATUS.md`：
```
current_stage: S4.3
current_module: wal-writer
last_confirmed: S4.2 at 2026-06-15
pending_feedback: none
next_action: 生成 wal-writer 代码
```

### 3.4 失败与回滚
- 任何阶段失败（测试挂、评审 Blocking、压测不达标），不要跳过：回到上一节点修复，把失败原因写入 `feedback/<stage>.md` 的"自检"小节。
- 严禁 `--no-verify` / 强制 push / `git reset --hard` 等破坏性操作，除非用户明确要求。

### 3.5 工具约束
- 评审顺序：**(1) 先 invoke `vearch-code-review` skill 锚定 Vearch §5 不变量** → (2) 再用 `Agent({subagent_type: "code-reviewer"})` 加载 `prompts/code-review-prompt.md` 做通用 DB/存储深审 → (3) 两轮 finding 合并后用 `superpowers:receiving-code-review` 评估是否实施。**不要自审**。
- 复盘的 worktree 重生：先 invoke `superpowers:using-git-worktrees`，再 `EnterWorktree`；禁止在 master 分支或 feature 分支覆盖式重跑。
- 跨子模块/独立任务（多模块并行评审、并行压测脚本生成）可用 Workflow，但默认串行，避免 commit 顺序乱。

### 3.6 Vearch 构建/测试入口

S4.3/S4.5/S4.6/S4.7/S5 的命令必须严格匹配 CLAUDE.md §7：
- **Go 单元测试**：`go test ./internal/<package>/... -v`，cgo 包必须带 `-tags="vector"`。
- **Gamma 引擎测试**：`cd build && ./build.sh -t && cd gamma_build && ctest`。
- **集成测试**：`cd test && pytest test_document_search.py -x --log-cli-level=INFO`，需先启动本地 Vearch 实例。
- **完整构建**：`make all`（含引擎）或 `cd build && ./build.sh -g OFF`（仅 Go）。
- **PR 创建**：`gh pr create --title <title> --body <body>`（须用户授权）。

S4.7 压测优先复用 `test/` 下已有 pytest 框架做基线，不新建独立脚手架。

### 3.7 引擎工作前置

任何子模块涉及 `internal/engine/`（C++ Gamma 引擎、向量索引族、IndexModel、REGISTER_INDEX、反射器、原始向量存储、实时倒排）时：
- S2/S3/S4.2 必须先 invoke `architecture` skill；
- 详设需对照 `architecture` skill 的 `adding-an-index.md` 与 `docs/IndexLayer.md`；
- 写路径仍走 raft（CLAUDE.md §5 不变量 1），引擎索引仅在 raft apply 之后参与。

---

## 4. 模板与清单索引

- `templates/01-requirement.md` … `templates/13-regen-verification.md`：各阶段产物骨架。
- `checklists/confirmation-gates.md`：所有 ✅ 节点的检查项汇总。
- `checklists/review-checklist.md`：DB/存储评审专项清单（一致性、持久化、回滚、热点、迁移等）。
- `checklists/stress-test-checklist.md`：压测必测项。
- `prompts/code-review-prompt.md`：代码评审 subagent 提示词。
- `prompts/regen-prompt.md`：复盘重生时的 skill 自调用提示词。

---

## 5. 快速参考：阶段流转图

```
S1 需求 ✅ → S2 概设 ✅ → S3 拆解 ✅
   → for each module:
        S4.1 黑盒 ✅ → S4.2 详设 ✅ → S4.3 代码 → S4.4 评审 ✅
        → S4.5 白盒 → S4.6 模块回归 → S4.7 压测 ✅
   → S5 整体回归 ✅ → S6 PR ✅
   → S7 复盘 ✅（含 worktree 重生验证）
```

✅ 标记表示**必须等待人工反馈**才能继续。
