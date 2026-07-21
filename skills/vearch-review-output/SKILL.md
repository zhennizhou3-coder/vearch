---
name: vearch-review-output
description: Use when writing or saving a Vearch code-review report to a file — defines the output language, target directory, filename convention, and report header. Referenced by the vearch-code-review checklist skill.
---

# Vearch Code Review Output Format

How to persist a code-review report. This covers *where* and *in what shape* the report is written; the *content* of the review lives in the `vearch-code-review` checklist skill.

## 输出约定（Output）

- **默认用中文撰写评审报告**（除非用户明确要求其他语言）。
- **必须把评审结果写入文件**，不要只在对话里输出；对话里只给一段简短摘要并附上文件路径。
- 文件放在仓库的 `.claude/code-reviews/` 目录，格式为 Markdown（`.md`）。
- 文件名 = `<分支名>-<日期>.md`（日期 `YYYYMMDD`）。分支名中的 `/` 替换为 `-`。
- **同名冲突时追加更细的时间戳**（`-HHMMSS`），避免覆盖同一天的既有评审。

用下面的命令确定输出路径（路径里禁止用 `date` 之外的随机值，保证可复现）：

```bash
branch=$(git rev-parse --abbrev-ref HEAD | tr '/' '-')
day=$(date +%Y%m%d)
dir=.claude/code-reviews
file="$dir/${branch}-${day}.md"
[ -e "$file" ] && file="$dir/${branch}-${day}-$(date +%H%M%S).md"
echo "$file"
```

## 报告结构（Report header）

报告开头应包含：评审人、日期、分支、对比基线（一般是 `master`）、改动范围（文件数 / `+/-` 行数）、以及一行总体结论（通过 / 需修改 / 阻断）。

## 发现项聚合（Findings — REQUIRED）

**报告必须包含一个「发现项聚合」章节**，作为全篇发现项的唯一权威汇总，格式如下：

- **第一层按严重程度分类**，顺序固定为：阻断 → 高 → 中 → 低。每个级别单独成一个二级/三级小节；该级别无发现项时写「无」。
- **同一严重级别内，按文件聚合**：每个涉及的文件作为一个子分组（用文件路径作小标题或表格分组键），该文件在此级别下的所有发现项列在其下。
- 每条发现项给出：编号、一句话问题描述、`file:line` 锚点；如涉及行为变化或需人工确认，注明「需确认」。
- 建议每级用一张表格（列：编号 / 文件 / 问题 / `file:line`），或「文件小标题 + 条目列表」两种形式之一，全篇保持一致。

正文其余部分可按评审清单逐节展开分析，但所有发现项都要在上述聚合章节中出现，级别标注（阻断 / 高 / 中 / 低）与锚点保持一致。

示例骨架：

```markdown
## 发现项聚合

### 🔴 阻断
无。

### 🟠 高
#### internal/engine/xxx.cc
- H1 —— 一句话问题描述（`xxx.cc:123`）

### 🟡 中
#### internal/engine/yyy.cc
- M1 —— 一句话问题描述（`yyy.cc:45`，需确认）

### 🟢 低
无。
```
