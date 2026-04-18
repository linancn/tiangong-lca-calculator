---
title: calculator Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: calculator
language: en
whenToUse:
  - when a tiangong-lca-calculator change is ready for local validation
  - when deciding the minimum proof required for solver, worker, script, or runtime-contract changes
  - when writing PR validation notes for tiangong-lca-calculator work
whenToUpdate:
  - when the repo gains new canonical validation wrappers
  - when change categories require different proof
  - when runtime SQL or parity-validation expectations change
checkPaths:
  - ai/validation.md
  - ai/task-router.md
  - Cargo.toml
  - Makefile
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - docs/**
lastReviewedAt: 2026-04-18
lastReviewedCommit: a1d53203bd75a9c7b839e48d2a75fda5c8b4dc4d
related:
  - ../AGENTS.md
  - ./repo.yaml
  - ./task-router.md
  - ./architecture.md
  - ../docs/lca-api-contract.md
---

## Default Baseline

Unless the change is doc-only AI-contract work, the default baseline is:

```bash
make check
cargo clippy -p solver-worker --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Treat the last two commands as non-negotiable hard gates after code changes.

## Validation Matrix

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `crates/**` solver or worker code | `make check`; hard Clippy gate; hard format gate | run the narrow manual script that matches the touched area, such as snapshot build, full compute debug, or BW25 validation | Record which job family or worker path was exercised. |
| snapshot-builder or provider-matching behavior | baseline gates plus `./scripts/build_snapshot_from_ilcd.sh` when safe | run provider-link diagnostics export helpers when the task changes matching logic | Snapshot and provider diagnostics often need a task-specific proof. |
| package worker import or export flows | baseline gates | run the closest safe package-flow helper or record why live package proof is deferred | Package-job semantics are runtime-sensitive and may depend on storage or DB state. |
| runtime SQL expectation docs or local migration helpers | baseline gates plus `./scripts/validate_additive_migration.sh` when the task touches migration expectations | record separately when durable schema proof is required in `database-engine` | Local migration files here are not the workspace-wide source of truth. |
| manual debug, parity, or target-validation scripts | run the touched script with safe args or `--help` when available, plus baseline gates if code changed nearby | `./scripts/run_full_compute_debug.sh`, `./scripts/run_bw25_validation.sh`, or `./scripts/validate_lcia_targets.sh` as applicable | `bw25-validator` is manual-only and out-of-band. |
| AI docs only | run the root warning-only `ai-doc-lint` against touched files | do one scenario-based routing check from root into this repo | Refresh review metadata even when prose-only docs change. |

## Minimum PR Note Quality

A good PR note for this repo should say:

1. which baseline gates ran
2. which job family, script, or manual parity helper was exercised
3. whether any required database-engine or edge-functions proof lives elsewhere
