# 复盘重生提示词（Skill 改进效果验证用）

> 用法：S7.3 阶段，在 worktree 中重新跑一遍同一 feature，验证改进效果。

## 角色
你是 db-dev-flow skill 的执行者。你正在**重生模式**下运行：使用改进后的 skill，在隔离 worktree 中重新生成 `<feature-slug>` 的全部产物，用于与首次产物对比验证 skill 改进效果。

## 输入
- 原始需求：与首次完全一致，来自 `<原项目>/docs/01-requirement.md`
- 首次反馈归档目录：`<原项目>/feedback/`
- 改进后的 skill 路径：`.claude/skills/db-dev-flow/` （worktree 内已是改进版）

## 关键差异（与首次相比）
1. **跳过所有人工等待**：原本 ✅ 的节点不向真人发问。代之以**自动从 `<原项目>/feedback/<gate>.md` 中读取首次已采纳的反馈**作为隐式答复。
2. 若某个 gate 在首次中**没有反馈**（即一次通过），重生模式也默认通过。
3. 若首次的反馈在改进后的 skill 下已被自然消除（产物本来就符合反馈意图），跳过修订。
4. 不实际发起 PR，S6 阶段只生成 PR 文案到 `retrospective/regen/pr-draft.md`。

## 任务
1. 在 `retrospective/regen/<feature-slug>/` 下创建独立项目目录（不要污染首次项目）。
2. 按 SKILL.md 跑完 S1 → S6，每阶段产物落到重生项目目录。
3. 代码生成到 `retrospective/regen/<feature-slug>/code/` 子目录，**不要 push、不要建分支**。
4. 跑完后：
   - 对重生代码用同一份 `prompts/code-review-prompt.md` 评审一次，结果存 `retrospective/regen/review.md`。
   - 用 `git diff --no-index <首次代码目录> retrospective/regen/<feature-slug>/code/` 生成 diff，摘要存 `retrospective/regen/diff-summary.md`。
   - 把"如果运行真人评审，预计还会提的反馈"列在 `retrospective/regen/expected-feedback.md`（基于改进后的 skill 自检）。
5. 把以上输入到 `retrospective/verification.md` 的对比表。

## 完成标志
- 重生评审 finding 中 Blocking = 0
- 反馈条数较首次下降（具体阈值由 `retrospective/skill-improvements.md` 的期望效果给出）

## 边界
- 不要修改首次产物。
- 不要在重生过程中再次改 skill；本轮只验证。
- 如发现重生暴露了新缺陷，记录到 `retrospective/verification.md` §4，作为下一轮改进的输入，但本轮不修复。
