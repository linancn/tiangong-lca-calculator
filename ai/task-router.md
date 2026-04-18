---
title: calculator Task Router
docType: router
scope: repo
status: active
authoritative: false
owner: calculator
language: en
whenToUse:
  - when you already know the task belongs in tiangong-lca-calculator but need the right next file or next doc
  - when deciding whether a change belongs in one solver layer, one worker flow, runtime SQL expectations, or another repo
  - when routing between calculator work and handoffs to edge-functions, database-engine, or lca-workspace
whenToUpdate:
  - when new job families or runtime hotspots appear
  - when cross-repo boundaries change
  - when validation routing becomes misleading
checkPaths:
  - AGENTS.md
  - ai/repo.yaml
  - ai/task-router.md
  - ai/validation.md
  - ai/architecture.md
  - crates/**
  - scripts/**
  - docs/**
  - supabase/migrations/**
lastReviewedAt: 2026-04-18
lastReviewedCommit: a1d53203bd75a9c7b839e48d2a75fda5c8b4dc4d
related:
  - ../AGENTS.md
  - ./repo.yaml
  - ./validation.md
  - ./architecture.md
  - ../docs/lca-api-contract.md
  - ../docs/edge-function-integration.md
  - ../docs/frontend-integration.md
  - ../docs/tidas-package-contract.md
---

## Repo Load Order

When working inside `tiangong-lca-calculator`, load docs in this order:

1. `AGENTS.md`
2. `ai/repo.yaml`
3. this file
4. `ai/validation.md` or `ai/architecture.md`
5. the narrow contract doc that matches the task

## High-Frequency Task Routing

| Task intent | First code paths to inspect | Next docs to load | Notes |
| --- | --- | --- | --- |
| Change sparse-matrix build, solve orchestration, or factorization cache behavior | `crates/solver-core/**` | `ai/validation.md`, `ai/architecture.md`, `docs/lca-api-contract.md` | Compute truth belongs here. |
| Change queue worker, HTTP worker runtime, or result persistence behavior | `crates/solver-worker/src/**` | `ai/validation.md`, `ai/architecture.md`, `docs/lca-api-contract.md` | This includes solve job execution and artifact shaping. |
| Change snapshot build or provider matching behavior | `crates/solver-worker/src/bin/snapshot_builder.rs`, nearby core modules | `ai/validation.md`, `ai/architecture.md` | Use snapshot and provider-diagnostics helpers when validating. |
| Change package import or export worker behavior | `crates/solver-worker/src/bin/package_worker.rs`, related worker modules | `ai/validation.md`, `ai/architecture.md`, `docs/tidas-package-contract.md` | Package-job semantics belong here. |
| Change contribution-path analysis behavior | `crates/solver-worker/src/types.rs` and nearby result handlers | `ai/validation.md`, `ai/architecture.md` | The current API docs understate this flow; rely on the architecture map first. |
| Change runtime SQL expectations referenced by the calculator | `supabase/migrations/**`, `scripts/validate_additive_migration.sh` | `ai/validation.md`, `docs/edge-function-integration.md`, `docs/frontend-integration.md` | Durable schema governance still belongs in `database-engine`. |
| Change manual debug or parity-validation helpers | `scripts/**`, `tools/bw25-validator/**` | `ai/validation.md`, `ai/architecture.md` | These scripts are part of the calculator operator surface. |
| Change request normalization, auth, enqueue API, or polling API behavior | `edge-functions`, not this repo | root `ai/task-router.md`, `tiangong-lca-edge-functions/AGENTS.md` | Edge owns the API runtime surface. |
| Change durable schema, migrations, RPCs, policies, or Supabase branch config | `database-engine`, not this repo | root `ai/task-router.md`, `database-engine/AGENTS.md` | Calculator depends on that truth but does not own it. |
| Decide whether work is delivery-complete after merge | root workspace docs, not repo code paths | root `AGENTS.md`, `_docs/workspace-branch-policy-contract.md` | Root integration remains a separate phase. |

## Wrong Turns To Avoid

### Fixing API-runtime behavior in the solver repo

If the bug is really request normalization, auth, enqueue, or polling API behavior, route it to `edge-functions`.

### Treating local migrations as workspace-wide schema governance

This repo still documents runtime SQL expectations, but durable schema truth belongs in `database-engine`.

### Ignoring the hard Rust gates

`cargo clippy -p solver-worker --all-targets --all-features -- -D warnings` and `cargo fmt --all -- --check` are hard post-edit gates.

## Cross-Repo Handoffs

Use these handoffs when work crosses boundaries:

1. solver/runtime change depends on new SQL or policy truth
   - start here for runtime code
   - coordinate with `database-engine`
2. solve or package runtime change affects API request or response flow
   - start here for runtime code
   - coordinate with `edge-functions`
3. merged repo PR still needs to ship through the workspace
   - return to `lca-workspace`
   - do the submodule pointer bump there
