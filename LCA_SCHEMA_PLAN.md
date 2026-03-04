# LCA 全库 Process 网络方案

## 1. 范围声明

本项目计算范围改为：

- 只做**全库 process 网络**的稀疏计算。
- 计算输入只依赖：`processes`、`flows`、`lciamethods`。
- `lifecyclemodels` 不参与求解链路（可保留给业务编排/UI，不进入数值核心）。

核心不变：

- 只解 `M x = y`，其中 `M = I - A`。
- 不求显式逆。
- prepare 阶段构建稀疏矩阵，solve 阶段只做回代。

---

## 2. 为什么仍然需要新增 LCA 表

现有 `public` 表是文档型 JSON 结构；求解器需要稳定的稀疏矩阵结构（COO/CSC）。

因此需要新增一组 `lca_*` 归一化表做：

- 全库快照（可复现）
- 索引映射（process/flow/impact -> row/col）
- 稀疏矩阵落表（A/B/C）
- 作业与结果（jobs/results）

---

## 3. 第一阶段（最小可跑）

### 3.1 `lca_network_snapshots`

用途：定义一次“全库计算快照”。

建议字段：

- `id uuid pk`
- `scope text`（固定 `full_library`）
- `process_filter jsonb`（例如 `state_code >= 100`、版本策略）
- `lcia_method_id uuid null`
- `lcia_method_version text null`
- `provider_matching_rule text`（见第 5 节）
- `source_hash text`（输入数据签名）
- `status text`（`draft/ready/stale/failed`）
- `created_by uuid`
- `created_at timestamptz`
- `updated_at timestamptz`

### 3.2 `lca_process_index`

用途：全库 process 节点编号。

建议字段：

- `snapshot_id uuid`
- `process_idx int`
- `process_id uuid`
- `process_version text`
- `state_code int`

约束/索引：

- `pk(snapshot_id, process_idx)`
- `unique(snapshot_id, process_id, process_version)`

### 3.3 `lca_flow_index`

用途：flow 编号（供 B/C 使用）。

建议字段：

- `snapshot_id uuid`
- `flow_idx int`
- `flow_id uuid`
- `flow_version text null`
- `flow_kind text`（`elementary/product/unknown`）

约束/索引：

- `pk(snapshot_id, flow_idx)`
- `index(snapshot_id, flow_id)`

### 3.4 `lca_technosphere_entries`

用途：A 矩阵条目。

建议字段：

- `snapshot_id uuid`
- `row int`
- `col int`
- `value double precision`
- `input_process_idx int`
- `provider_process_idx int`
- `flow_id uuid`

约束/索引：

- `pk(snapshot_id, row, col)`
- `index(snapshot_id, col)`

### 3.5 `lca_biosphere_entries`

用途：B 矩阵条目（`g = Bx`）。

建议字段：

- `snapshot_id uuid`
- `row int`
- `col int`
- `value double precision`
- `flow_id uuid`
- `process_idx int`

约束/索引：

- `pk(snapshot_id, row, col)`
- `index(snapshot_id, col)`
- `index(snapshot_id, row)`

### 3.6 `lca_characterization_factors`

用途：C 矩阵条目（`h = Cg`）。

建议字段：

- `snapshot_id uuid`
- `row int`
- `col int`
- `value double precision`
- `method_id uuid`
- `method_version text`
- `indicator_key text`

约束/索引：

- `pk(snapshot_id, row, col)`
- `index(snapshot_id, col)`

### 3.7 `lca_jobs`

建议字段：

- `id uuid pk`
- `job_type text`
- `snapshot_id uuid`
- `status text`（`queued/running/ready/completed/failed/stale`）
- `payload jsonb`
- `diagnostics jsonb`
- `attempt int`
- `max_attempt int`
- `requested_by uuid`
- `created_at timestamptz`
- `started_at timestamptz null`
- `finished_at timestamptz null`
- `updated_at timestamptz`

### 3.8 `lca_results`

建议字段：

- `id uuid pk`
- `job_id uuid`
- `snapshot_id uuid`
- `payload jsonb`
- `diagnostics jsonb`
- `artifact_url text`
- `artifact_sha256 text`
- `artifact_byte_size bigint`
- `artifact_format text`
- `created_at timestamptz`

---

## 4. 第二阶段（增强）

### 4.1 `lca_factorizations`

用途：记录分解元数据和状态（不保存 C 指针本体）。

建议字段：

- `id uuid pk`
- `snapshot_id uuid`
- `backend text`（`umfpack`）
- `options_hash text`
- `state text`（`pending/building/ready/failed/stale`）
- `matrix_n int`
- `matrix_nnz bigint`
- `validation_status text`
- `validation_messages jsonb`
- `symbolic_info jsonb`
- `numeric_info jsonb`
- `error_message text`
- `built_at timestamptz`
- `last_used_at timestamptz`

---

## 5. 全库网络的关键建模规则（必须先定）

### 5.1 供给匹配规则（最关键）

当前 `processes` 的 exchange 里通常没有稳定的 `referenceToProcess`，因此 input exchange 到 provider process 的映射不是天然唯一。必须选定规则：

- `strict_unique_provider`：仅当 flow 在快照内唯一 output provider 才建边。
- `equal_split_multi_provider`：多个 provider 等权分摊。
- `custom_weighted_provider`：按外部权重表分配（后续扩展）。

没有这个规则，A 矩阵不可确定。

### 5.2 Biosphere 判定规则

从 `flows` 分类识别 elementary flow（如 `Emissions`），决定哪些 exchange 进入 B。

### 5.3 LCIA 方法规则

`lca_characterization_factors` 从 `lciamethods` 提取，按选定 method 版本构造 C。

---

## 6. 计算链路

1. 创建 `snapshot`（全库范围 + 规则 + 方法）。
2. prepare：
- 从 `processes/flows/lciamethods` 抽取并写入 `lca_*_entries`。
- 构建 `M = I - A`，做结构校验。
- 分解并缓存。
3. solve：
- 输入 `y`（全库 process 维度或 process_id/value 稀疏输入）。
- 求 `x`，再算 `g = Bx`，`h = Cg`。
- 写 `lca_results`。

---

## 7. 对现有 Rust 服务的影响

当前代码已完成 `snapshot_id` 迁移：

- `prepare(model_version)` 已切为 `prepare(snapshot_id)`（HTTP 与 Queue）
- SQL 查询已切为 `... WHERE snapshot_id = ?`
- 为兼容旧消息，Queue payload 仍接受 `model_version` 字段别名
- API 路由已提供：
  - 首选：`/internal/snapshots/{snapshot_id}/...`
  - 兼容：`/internal/models/{snapshot_id}/...`

---

## 8. 上线顺序

1. 已完成第一阶段 8 张表 + `lca_jobs` 队列（additive-only）。
2. 下一步：跑一次全库 snapshot 构建，输出 coverage 诊断：
- process 覆盖率
- exchange -> provider 匹配率
- biosphere 提取率
- CF 匹配率
3. 已接入 `pgmq` 队列 `lca_jobs`。
4. 已打通 `prepare -> solve -> results`（基于 `lca_*` 快照表）。
5. 再补 `lca_factorizations` 等增强治理能力。

---

## 9. 结果存储策略（新增）

全库网络求解结果向量较大，当前采用“数据库 + 文件”的混合存储。

### 9.1 存数据库（`lca_results`）

保存轻量且高频查询的数据：

- `job_id / snapshot_id / created_at`
- 诊断信息（耗时、状态、告警、覆盖率）
- 统计摘要（总量、范数、top-k、分页预览）
- 文件元数据（`artifact_url`、`artifact_sha256`、`artifact_byte_size`、`artifact_format`）

### 9.2 存文件（对象存储）

保存大体量向量和批量结果：

- `x / g / h` 全量向量
- `solve_batch` 的每个 RHS 输出

当前实现格式：

- 位置：Supabase Storage（或 S3 等对象存储）
- 容器：`HDF5`
- 格式标识：`hdf5:v1`
- 后缀：`.h5`
- 校验：`SHA-256`

### 9.3 建议阈值

- 结果序列化后 `< 256KB`：直接写 `lca_results.payload`（JSON）
- `>= 256KB`：写对象存储，仅在 `lca_results` 保存摘要和文件引用

该策略可降低 Postgres 存储膨胀与 JSONB 解析成本，同时保留可检索性。

---

## 10. 非目标

- 不做 `lifecyclemodels` 子图求解。
- 不做浏览器端矩阵计算。
- 不做显式逆矩阵。
