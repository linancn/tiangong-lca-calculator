# AGENTS.md

This file is for AI coding agents working in this repository.

## 0. Maintenance policy (mandatory)

`AGENTS.md` is a living contract.  
For every relevant code/schema/behavior change in this repo, update this file in the same task.

Always sync:

- architecture/module boundaries
- API/payload contracts
- schema assumptions
- dependency/runtime/tooling changes
- implementation status
- TODO priorities and known risks

Do not leave this file stale.

## 1. Project intent

Build a high-throughput LCA sparse solver stack with strict separation:

- Supabase: source data + orchestration + queue (`pgmq`) + job/result persistence.
- Rust solver-worker: sparse matrix build/validation/factorization/solve/writeback.
- SuiteSparse backend: UMFPACK via Rust FFI (current backend).

Core invariants:

- Always solve `M x = y` with `M = I - A`.
- Never compute explicit inverse.
- Heavy compute only in async worker path.
- Active scope: full-library process network solve (`lifecyclemodels` excluded from numeric solve path).

## 2. Current status (as of 2026-03-04)

### 2.1 Implemented

- Cargo workspace with 3 crates:
  - `crates/suitesparse-ffi`
  - `crates/solver-core`
  - `crates/solver-worker`
- UMFPACK minimal binding + safe wrappers:
  - `CscMatrix`
  - `UmfpackFactorization`
- Core pipeline:
  - build `M`, `B`, `C`
  - structure validation
  - in-memory factorization cache
  - `solve_one` / `solve_batch`
- Worker/API:
  - pgmq queue consume + archive
  - job execution for `prepare_factorization`, `solve_one`, `solve_batch`, `invalidate_factorization`, `rebuild_factorization`
  - internal HTTP endpoints (snapshot-first):
    - `POST /internal/snapshots/{snapshot_id}/prepare`
    - `GET /internal/snapshots/{snapshot_id}/factorization`
    - `POST /internal/snapshots/{snapshot_id}/solve`
    - `POST /internal/snapshots/{snapshot_id}/invalidate`
  - backward-compatible aliases:
    - `/internal/models/{snapshot_id}/...`
- Payload contract migrated to `snapshot_id` with backward alias support:
  - `model_version` is accepted as serde alias for queue payloads.

### 2.2 Supabase schema/migration status

Applied additive-only migration:

- `supabase/migrations/20260304073000_lca_snapshot_phase1.sql`

Created tables:

- `lca_network_snapshots`
- `lca_process_index`
- `lca_flow_index`
- `lca_technosphere_entries`
- `lca_biosphere_entries`
- `lca_characterization_factors`
- `lca_jobs`
- `lca_results`

Created queue:

- `pgmq.meta.queue_name = 'lca_jobs'`

Verification done at migration time:

- Existing source tables row counts unchanged (`processes`, `flows`, `lciamethods`, `lifecyclemodels`).
- New `lca_*` tables currently empty unless manually backfilled later.

### 2.3 Result storage policy (implemented)

Hybrid persistence is now active in `solver-worker`:

- Always encode result artifact as:
  - `MessagePack + zstd(level=3)`
  - format id: `msgpack+zstd:v1`
  - extension: `.msgpack.zst`
  - checksum: SHA-256 (hex)
- If encoded bytes `< RESULT_INLINE_MAX_BYTES` (default 256KB):
  - store JSON payload inline in `lca_results.payload`
- If encoded bytes `>= RESULT_INLINE_MAX_BYTES` and S3 config exists:
  - upload artifact to object storage
  - store metadata in `lca_results`:
    - `artifact_url`
    - `artifact_sha256`
    - `artifact_byte_size`
    - `artifact_format`
- If upload fails:
  - fallback to inline JSON payload and warn in logs

Object storage config keys:

- `S3_ENDPOINT`
- `S3_REGION`
- `S3_BUCKET`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- optional `S3_SESSION_TOKEN`
- optional `S3_PREFIX` (default `lca-results`)

Uploads are authenticated with AWS SigV4 (`Authorization` + `x-amz-date` + `x-amz-content-sha256`).

`DATABASE_URL` is preferred DB env var; `CONN` is accepted fallback.

### 2.4 Validation state

Latest checks passed:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 3. Architecture map

### 3.1 `crates/suitesparse-ffi`

- `src/matrix.rs`:
  - internal CSC representation
  - COO->CSC conversion + dedup + zero pruning
  - structural checks + sparse mat-vec
- `src/umfpack.rs`:
  - raw FFI declarations
  - symbolic/numeric solve wrappers
  - `Drop`-based C resource cleanup

### 3.2 `crates/solver-core`

- `src/data_builder.rs`: build `M/B/C` from sparse entries
- `src/validator.rs`: pre-factorization checks/warnings
- `src/cache.rs`: in-memory factorization cache/state
- `src/service.rs`: `prepare/solve/invalidate` orchestration

### 3.3 `crates/solver-worker`

- `src/config.rs`:
  - env/CLI config (`DATABASE_URL` + `CONN` fallback)
  - queue/http settings
  - object-storage settings
- `src/db.rs`:
  - reads snapshot sparse data from `lca_*` tables
  - updates `lca_jobs`
  - writes `lca_results` payload/metadata
- `src/artifacts.rs`:
  - artifact envelope encode (`msgpack+zstd:v1`)
  - SHA-256 checksum
- `src/storage.rs`:
  - S3-compatible upload client (path-style PUT)
- `src/queue.rs`:
  - queue polling + message lifecycle
- `src/http.rs`:
  - internal snapshot/model alias routes
- `src/types.rs`:
  - queue/API payload contracts

## 4. Schema assumptions

Current runtime expects snapshot-oriented tables:

- `lca_process_index(snapshot_id, process_idx, ...)`
- `lca_flow_index(snapshot_id, flow_idx, ...)`
- `lca_technosphere_entries(snapshot_id, row, col, value, ...)`
- `lca_biosphere_entries(snapshot_id, row, col, value, ...)`
- `lca_characterization_factors(snapshot_id, row, col, value, ...)`
- `lca_jobs`
- `lca_results`

Input source-of-truth upstream remains:

- `processes`
- `flows`
- `lciamethods`

`lifecyclemodels` is not part of numeric solve.

## 5. Known limitations / risks

- No snapshot-builder pipeline yet (no automated backfill from source tables to `lca_*` entries).
- Factorization cache is process-local memory only.
- No persisted factorization snapshots across restart.
- Internal HTTP endpoints do not enforce auth (assumed trusted internal network).
- Current uploader supports static key/secret (+ optional session token) credentials; key rotation and STS refresh are not yet automated.
- No advanced contribution/post-processing yet.
- Backend is UMFPACK-only; CHOLMOD/SPQR not exposed yet.

## 6. TODO backlog (priority)

### P0

- Implement snapshot builder job:
  - read `processes/flows/lciamethods`
  - materialize `lca_process_index/lca_flow_index/lca_*_entries`
  - write coverage diagnostics
- Add integration tests with real Postgres + `pgmq` (containerized).
- Add artifact reader/decoder utility for `msgpack+zstd:v1` outputs.
- Strengthen job/result diagnostics schema:
  - factorization stats
  - timing breakdown
  - failure code taxonomy

### P1

- Add retry/backoff/dead-letter flow for failed jobs.
- Add cache TTL/eviction and memory pressure controls.
- Add structured metrics and queue lag observability.
- Add signed S3 upload path (or managed storage SDK) for private bucket setups.

### P2

- Optional L2 cache for warm startup.
- Add CHOLMOD/SPQR backends behind unified trait.
- Add richer post-processing (top contributors/path analysis).

## 7. Safe change rules

- Keep solver invariant: factorize once, solve many.
- Keep heavy compute off synchronous request paths.
- Keep C FFI behind safe Rust wrappers.
- Always validate sparse matrix structure before numeric factorization.
- Never introduce explicit matrix inverse.
- For schema migration work: additive-first unless explicitly approved.
- If behavior/contracts/status changes, update this file in the same change set.

## 8. Local runbook (agent)

Install system deps (Ubuntu):

```bash
sudo apt-get update
sudo apt-get install -y libsuitesparse-dev libopenblas-dev liblapack-dev pkg-config
```

Run checks:

```bash
make check
```

Run worker:

```bash
set -a && source .env && set +a
cargo run -p solver-worker --release
```

## 9. Definition of done

A task is complete only when:

- code compiles
- `fmt/clippy/test` pass
- worker queue flow still works
- docs stay in sync (`AGENTS.md` + human-facing `README.md`)
