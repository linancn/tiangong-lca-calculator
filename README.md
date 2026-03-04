# Tiangong LCA Calculator

面向 Supabase + Rust + SuiteSparse 的大规模 LCA 稀疏求解服务。

## 1. 架构定位

- Supabase: 业务数据、鉴权、Edge Functions 编排、`pgmq` 队列。
- Rust Solver Worker: 构建稀疏矩阵、缓存分解、重复回代、写回结果。
- SuiteSparse (UMFPACK): 稀疏线性系统求解核心。

核心不变：

- 只解 `M x = y`，其中 `M = I - A`
- 不求显式逆矩阵
- 重计算只走异步 worker，不走前端/同步请求

## 2. 当前实现状态

- 已接入 `snapshot_id` 语义（全库 process 网络）。
- 已支持作业类型：
  - `prepare_factorization`
  - `solve_one`
  - `solve_batch`
  - `invalidate_factorization`
  - `rebuild_factorization`
- 已完成 `lca_*` 新表 + `lca_jobs` 队列（additive migration）。
- 已支持结果混合存储：
  - 小结果：写 `lca_results.payload`（JSON）
  - 大结果：写对象存储，`lca_results` 仅存元数据

## 3. 结果文件格式（已选定）

对象存储中的大结果采用：

- 编码：`MessagePack`
- 压缩：`zstd`（level 3）
- 格式标识：`msgpack+zstd:v1`
- 文件后缀：`.msgpack.zst`
- 哈希：`SHA-256`

默认阈值：

- `RESULT_INLINE_MAX_BYTES=262144`（256KB）

当编码后字节数超过阈值时，worker 会上传 artifact 到 S3 兼容存储，并在 `lca_results` 中写入：

- `artifact_url`
- `artifact_sha256`
- `artifact_byte_size`
- `artifact_format`

## 4. 数据库迁移

已提供 migration：

- `supabase/migrations/20260304073000_lca_snapshot_phase1.sql`

该 migration 只新增 `lca_*` 表和队列，不改已有业务表数据。

可先做静态检查：

```bash
./scripts/validate_additive_migration.sh supabase/migrations/20260304073000_lca_snapshot_phase1.sql
```

执行迁移：

```bash
set -a && source .env && set +a
psql "$CONN" -v ON_ERROR_STOP=1 -f supabase/migrations/20260304073000_lca_snapshot_phase1.sql
```

## 5. 环境变量

最小必需：

- `DATABASE_URL` 或 `CONN`
- `PGMQ_QUEUE`（默认 `lca_jobs`）
- `SOLVER_MODE`（`worker` / `http` / `both`）
- `HTTP_ADDR`（默认 `0.0.0.0:8080`）

大结果对象存储（可选，但建议配置）：

- `S3_ENDPOINT`
- `S3_REGION`
- `S3_BUCKET`
- `S3_PREFIX`（默认 `lca-results`）
- `RESULT_INLINE_MAX_BYTES`（默认 `262144`）

说明：`S3_ENDPOINT/S3_REGION/S3_BUCKET` 需要同时提供，缺任何一项会启动失败。

## 6. 启动与检查

Ubuntu 依赖：

```bash
sudo apt-get update
sudo apt-get install -y libsuitesparse-dev libopenblas-dev liblapack-dev pkg-config
```

质量检查：

```bash
make check
```

启动服务：

```bash
set -a && source .env && set +a
cargo run -p solver-worker --release
```

## 7. 内部 API

推荐路径（snapshot 语义）：

- `POST /internal/snapshots/{snapshot_id}/prepare`
- `GET /internal/snapshots/{snapshot_id}/factorization`
- `POST /internal/snapshots/{snapshot_id}/solve`
- `POST /internal/snapshots/{snapshot_id}/invalidate`

兼容路径（旧命名别名）：

- `.../models/{snapshot_id}/...`

## 8. Queue Payload 契约

作业 payload 使用 `snapshot_id`。为兼容旧消息，worker 仍接受 `model_version` 字段别名。

## 9. 说明文档

- 面向 AI 的持续上下文：`AGENTS.md`
- 架构与建模方案：`LCA_SCHEMA_PLAN.md`
