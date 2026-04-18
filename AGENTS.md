---
title: calculator AI Working Guide
docType: contract
scope: repo
status: active
authoritative: true
owner: calculator
language: en
whenToUse:
  - when a task may change solver runtime behavior, queue workers, snapshot building, package worker flows, or calculation-side validation scripts
  - when routing work from the workspace root into tiangong-lca-calculator
  - when deciding whether a change belongs here, in edge-functions, in database-engine, or in lca-workspace
whenToUpdate:
  - when runtime job families, validation gates, or ownership boundaries change
  - when the runtime SQL contract or package-flow contract changes
  - when the repo-local AI bootstrap docs under ai/ change
checkPaths:
  - AGENTS.md
  - README.md
  - docs/**
  - ai/**/*.md
  - ai/**/*.yaml
  - Cargo.toml
  - Makefile
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - .github/workflows/**
lastReviewedAt: 2026-04-18
lastReviewedCommit: a1d53203bd75a9c7b839e48d2a75fda5c8b4dc4d
related:
  - ai/repo.yaml
  - ai/task-router.md
  - ai/validation.md
  - ai/architecture.md
  - docs/lca-api-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/tidas-package-contract.md
---

## Repo Contract

`tiangong-lca-calculator` owns the TianGong LCA solver runtime: sparse-matrix build and solve logic, worker job execution, snapshot building, provider matching, package import or export worker flows, and calculation-side diagnostics. Start here when the task may change what the compute stack does.

## AI Load Order

Load docs in this order:

1. `AGENTS.md`
2. `ai/repo.yaml`
3. `ai/task-router.md`
4. `ai/validation.md`
5. `ai/architecture.md`
6. only then load the narrow contract doc that matches the task, such as:
   - `docs/lca-api-contract.md`
   - `docs/edge-function-integration.md`
   - `docs/frontend-integration.md`
   - `docs/tidas-package-contract.md`

Do not start from the root workspace or from the edge repo if the change is really about compute truth.

## Repo Ownership

This repo owns:

- solver crates under `crates/**`
- queue workers, package workers, and snapshot builder binaries
- runtime result persistence and diagnostics shaping
- calculation-side manual validation and debugging scripts under `scripts/**`
- runtime-facing contract docs under `docs/**`

This repo does not own:

- Edge request normalization, auth, enqueue API, or polling API behavior
- durable schema governance, branch config, or migration source of truth
- workspace integration state after merge

Route those tasks to:

- `edge-functions` for request, response, auth, enqueue, and polling API behavior
- `database-engine` for durable schema, migration, RPC, policy, and Supabase branch-governance truth
- `lca-workspace` for root integration after merge

## Branch And Validation Facts

- GitHub default branch: `main`
- True daily trunk: `main`
- Routine branch base: `main`
- Routine PR base: `main`
- Canonical repo-wide check: `make check`
- Hard post-edit gates:
  - `cargo clippy -p solver-worker --all-targets --all-features -- -D warnings`
  - `cargo fmt --all -- --check`

- Repo-local AI-doc maintenance is enforced by `.github/workflows/ai-doc-lint.yml` using the vendored `.github/scripts/ai-doc-lint.*` files.

## Operational Invariants

- Solve result persistence is S3-only; `lca_results` stores artifact metadata and diagnostics, not inline payloads.
- Queue enqueue and protected writes must stay on service-side paths; do not move them to frontend clients or authenticated direct table writes.
- Runtime write paths assume `service_role` ownership boundaries and existing RLS restrictions on `lca_*` tables.
- Worker and snapshot flows expect DB connectivity plus the required S3 env set: `DATABASE_URL` or `CONN`, `S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, and `S3_SECRET_ACCESS_KEY`.

## Hard Boundaries

- Do not move solver or worker behavior into `edge-functions`
- Do not treat local `supabase/migrations/**` as the workspace's durable schema governance source of truth
- Do not weaken the Clippy or format gates
- Do not treat a merged repo PR here as workspace-delivery complete if the root repo still needs a submodule bump

## Workspace Integration

A merged PR in `tiangong-lca-calculator` is repo-complete, not delivery-complete.

If the change must ship through the workspace:

1. merge the child PR into `tiangong-lca-calculator`
2. update the `lca-workspace` submodule pointer deliberately
3. complete any later workspace-level validation that depends on the updated solver snapshot
