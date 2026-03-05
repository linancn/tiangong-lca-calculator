# LCA Schema Update Plan (Additive-only)

## 实施状态（2026-03-05）

已按本方案新增并应用 migration：

- `supabase/migrations/20260305052000_lca_request_cache_and_factorization_registry.sql`
- `supabase/migrations/20260305070000_lca_rls_lockdown.sql`

已落地对象：

- 新表：`lca_active_snapshots`、`lca_result_cache`、`lca_factorization_registry`
- 新列：`lca_jobs.request_key`、`lca_jobs.idempotency_key`
- 对应唯一约束与查询索引（缓存去重、幂等、状态查询）

说明：

- 本次实施保持 additive-only（未修改/删除源业务数据）。
- 已启用 `lca_*` 表 RLS，并收紧 `anon/authenticated` 的默认开放权限。

## 0. 文档目的

本文件是当前阶段的 **schema 更新说明**，只描述“如何新增”，不做实施命令。

边界：

- 仅设计 `lca_*` 命名空间下的增量表/列/索引。
- 明确禁止修改或删除现有业务源表数据（`processes/flows/lciamethods/lifecyclemodels`）。
- 与现有 artifact-first 架构保持一致。

## 1. 现状与约束

当前已存在并在用：

- `lca_network_snapshots`
- `lca_snapshot_artifacts`
- `lca_jobs`
- `lca_results`

当前流程：

- 管理员先构建 snapshot artifact（`snapshot-hdf5:v1`）。
- 用户计算主要走 `solve`，不重建全库。
- worker 从 snapshot artifact 读取矩阵并求解，结果写 `lca_results`（inline 或 S3 artifact）。

本次目标：

- 在不破坏现有流程的前提下，补齐“active snapshot 管理 + 结果缓存去重 + 分解注册协调 + job 幂等键”。

## 2. 命名规范

统一使用 `lca_*` 前缀：

- 新表：`lca_active_snapshots` / `lca_result_cache` / `lca_factorization_registry`
- 新列：放在既有 `lca_jobs` 上（`request_key` / `idempotency_key`）

## 3. Additive-only 变更清单

### 3.1 新增表：`lca_active_snapshots`

用途：记录每个 scope 当前生效的 snapshot 指针。

建议字段：

- `scope text pk`（如 `prod`）
- `snapshot_id uuid not null`（FK -> `lca_network_snapshots.id`）
- `source_hash text not null`
- `activated_at timestamptz not null default now()`
- `activated_by uuid null`
- `note text null`

建议索引：

- `index(snapshot_id)`

### 3.2 新增表：`lca_result_cache`

用途：按请求内容做命中缓存、去重和状态跟踪。

建议字段：

- `id uuid pk default gen_random_uuid()`
- `scope text not null default 'prod'`
- `snapshot_id uuid not null`（FK -> `lca_network_snapshots.id`）
- `request_key text not null`（规范化请求哈希）
- `request_payload jsonb not null`（规范化后的入参）
- `status text not null`（`pending/running/ready/failed/stale`）
- `job_id uuid null`（FK -> `lca_jobs.id`）
- `result_id uuid null`（FK -> `lca_results.id`）
- `error_code text null`
- `error_message text null`
- `hit_count bigint not null default 0`
- `last_accessed_at timestamptz not null default now()`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

约束与索引：

- `unique(scope, snapshot_id, request_key)`
- `unique(job_id) where job_id is not null`
- `unique(result_id) where result_id is not null`
- `index(scope, snapshot_id, status, updated_at desc)`
- `index(last_accessed_at desc)`

### 3.3 新增表：`lca_factorization_registry`

用途：跨 worker 协调 factorization 的生命周期；不存 C 指针。

建议字段：

- `id uuid pk default gen_random_uuid()`
- `scope text not null default 'prod'`
- `snapshot_id uuid not null`（FK -> `lca_network_snapshots.id`）
- `backend text not null default 'umfpack'`
- `numeric_options_hash text not null`
- `status text not null`（`pending/building/ready/failed/stale`）
- `owner_worker_id text null`
- `lease_until timestamptz null`
- `prepared_job_id uuid null`（FK -> `lca_jobs.id`）
- `diagnostics jsonb not null default '{}'::jsonb`
- `prepared_at timestamptz null`
- `last_used_at timestamptz null`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

约束与索引：

- `unique(scope, snapshot_id, backend, numeric_options_hash)`
- `index(status, lease_until)`

### 3.4 既有表增量：`lca_jobs` 新增列

新增字段：

- `request_key text null`
- `idempotency_key text null`

新增索引：

- `unique(idempotency_key) where idempotency_key is not null`
- `index(snapshot_id, job_type, status, created_at desc)`

> 注：以上均为 additive（新增列/索引），不改旧列语义。

## 4. 请求与缓存/队列协同（目标语义）

### 4.1 用户请求入口

1. 读取 `lca_active_snapshots(scope='prod')` 得到 `snapshot_id`。
2. 规范化请求并生成 `request_key`。
3. 查询/UPSERT `lca_result_cache(scope,snapshot_id,request_key)`：
- `ready` -> 直接返回 `result_id`。
- `pending/running` -> 返回现有 `job_id`（不重复算）。
- 缓存缺失 -> 创建 `lca_jobs` 并入队。

### 4.2 Worker 执行

- 按 `job_type` 消费 `lca_jobs`。
- `prepare/solve` 前通过 `lca_factorization_registry` 协调分解状态。
- 求解完成后写 `lca_results`，再回填 `lca_result_cache.result_id/status`。

## 5. 冲突与一致性策略

- Snapshot 不可变：每次新发布产生新 `snapshot_id`，不覆盖旧快照。
- 缓存键绑定 `snapshot_id`：避免旧版本结果污染新版本。
- 幂等：`idempotency_key` 唯一，重复请求只会复用已有 job。
- 去重：`lca_result_cache` 的唯一键防止同请求并发创建多个任务。
- 分解互斥：通过 `lca_factorization_registry` 唯一键 + lease 协调。

## 6. 上线顺序（仅方案）

1. 建表：`lca_active_snapshots` / `lca_result_cache` / `lca_factorization_registry`。
2. 增列：`lca_jobs.request_key` / `lca_jobs.idempotency_key`。
3. 建索引与唯一约束。
4. 应用层切换读写逻辑（先写后读，逐步灰度）。
5. 接入定时清理策略（`lca_result_cache` 旧数据归档或 TTL）。

## 7. 验收清单（实施后应验证）

- 不修改源数据：`processes/flows/lciamethods/lifecyclemodels` 行数与关键哈希不变。
- 同一请求并发 10 次仅产生 1 个新 `job_id`。
- 命中缓存时无新队列消息。
- 新 snapshot 激活后，旧缓存不会被误命中。
- worker 重启后，`ready` 结果仍可复用（DB/S3）。

## 8. 非目标（本阶段不做）

- 不新增 `lca_*_entries` 大表回写路径。
- 不引入 destructive migration（drop/alter type/rewrite 大表）。
- 不做跨机共享 factorization 实体序列化。

## 9. 迁移文件命名建议（预留）

- `supabase/migrations/<ts>_lca_active_snapshot_and_cache.sql`
- `supabase/migrations/<ts>_lca_factorization_registry.sql`
- `supabase/migrations/<ts>_lca_jobs_idempotency_columns.sql`

> 上述命名建议用于后续增量阶段；当前已执行的 migration 见“实施状态（2026-03-05）”。

## 10. 当前权限基线（已实施）

- `lca_*` 表统一启用 RLS。
- `anon`：无 `lca_*` 表级权限。
- `authenticated`：
  - 仅可读取自己的 `lca_jobs`（`requested_by = auth.uid()`）
  - 仅可读取关联自己 job 的 `lca_results`
- `service_role`：保留完整读写权限（Edge Functions / worker 内部路径）。
