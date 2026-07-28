---
name: pr-review-single-purpose
description: Use when reviewing PRs (via /review, /code-review, gh pr view) or preparing to create a PR in this project — flag PRs that bundle multiple unrelated changes
---

# PR Review: Single-Purpose Rule

## Overview

Each PR in this project must implement exactly **one** concern: a single feature, a single bugfix, a single refactor, or a single chore. PRs that bundle unrelated changes should be flagged and split.

## When to Use

- Reviewing a PR (`/review`, `/code-review`, `gh pr view`, ad-hoc diff inspection)
- Preparing to open a PR (`gh pr create`) from a working branch
- A user asks "is this PR good to merge?"

## The Rule

A PR is **single-purpose** when its diff can be summarized in one sentence without using "and" to join unrelated concerns.

| Diff summary | Single-purpose? |
|---|---|
| "Adds DiskANN index support" | ✅ |
| "Fixes memory leak in dump option" | ✅ |
| "Refactors partition server startup" | ✅ |
| "Adds DiskANN support **and** fixes router timeout bug" | ❌ split |
| "Cleans up unused imports **and** adds new metrics endpoint" | ❌ split |
| "Refactors auth **and** adds OAuth provider" | ⚠️  acceptable only if the refactor is required to enable OAuth |

## Review Checklist

Before commenting on individual changes, check the PR's overall scope:

1. **Read the PR title and description.** Does it claim to do one thing?
2. **Scan the file list.** Are changed files clustered around one concern, or scattered across unrelated modules?
3. **Check the commit history.** Multiple unrelated commit messages (`fix: X` + `feat: Y` + `refactor: Z`) is a strong signal.
4. If the PR is multi-purpose, **raise a CRITICAL severity finding: "split this PR"** — this is the top-level finding, listed before any other review comment. Enumerate the separable concerns and recommend one PR per concern. Do not bury this under line-level comments, and do not downgrade the severity even if individual changes look correct.

## Severity

A multi-purpose PR is a **CRITICAL** finding in this project, on par with correctness bugs and security issues. Rationale: a PR that bundles unrelated changes cannot be safely reverted, makes `git bisect` ambiguous, and hides defects from reviewers — these consequences affect production stability, not just code hygiene. Treat it accordingly:

- Report severity explicitly as `CRITICAL` in the review output (e.g. `[CRITICAL] Multi-purpose PR — split required`).
- Block merge approval until the PR is split or the bundling is justified under the "Acceptable Exceptions" section below.
- Do not lower severity because the individual changes are small, well-tested, or "obviously correct" — the issue is the bundling itself.

## Acceptable Exceptions

Only bundle changes when they are **genuinely interdependent**:

- A refactor required to enable the feature (refactor cannot land alone without breaking things, feature cannot land without the refactor).
- A bugfix discovered while implementing the feature, where the bug is in code the feature touches and isolating it would require artificial scaffolding.
- Trivial drive-by fixes (typo in a comment in a file you're already editing) — but never drive-by *behavior* changes.

When in doubt, split.

## When Creating PRs

If `gh pr create` is requested and the working branch contains mixed work (multiple unrelated commits, scattered files), surface this to the user **before** opening the PR. Suggest:

- Splitting the branch into multiple branches (`git cherry-pick` or interactive rebase, with user's explicit approval for the rebase).
- Or opening the PR scoped to one concern and leaving the rest for a follow-up PR.

Do not silently open a multi-purpose PR.

## Why

Single-purpose PRs are easier to review, easier to revert if something regresses in production, and produce a clean git history that makes `git bisect` and changelog generation tractable. Bundled PRs hide bugs because reviewers focus on the headline change and skim the rest.
