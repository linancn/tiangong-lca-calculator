# tiangong-lca-calculator 问题分析记录

本文档用于跟踪方法学问题、当前实现状态与后续改造优先级。

更新时间：2026-03-08

## 0. 状态总览

| 编号 | 主题 | 状态 | 说明 |
|---|---|---|---|
| 问题1 | `B` 符号与 LCIA 负值解释 | ✅ 已实现（gross） | `B` 已切换为 gross 口径（不按方向翻转符号，保留原始 `amount`） |
| 问题2 | Quantitative reference 归一化 | ✅ 已实现 | 已接入 `strict/lenient` 归一化与诊断 |
| 问题3 | Allocation (`@allocatedFraction`) | ✅ 已实现 | 已接入 `strict/lenient` 分配比例解析与诊断 |
| 问题4 | 多 provider 处理 | 🟡 部分实现 | 已有自动链接主链路，证据明细与场景输出待补 |

## 1. 当前实现快照

- 技术矩阵 `A`：由 `Input` 边写入，`Output` 仅用于 provider 候选构建。
- 环境交换矩阵 `B`：仅对 elementary flow 生效，采用 `gross` 口径（`Input/Output` 均按原始 `amount` 入模，不做方向翻转）。
- 影响矩阵 `C`：按 flow 的 characterization factor 聚合。
- provider 规则已支持：
  - `strict_unique_provider`
  - `best_provider_strict`
  - `split_by_evidence`
  - `split_by_evidence_hybrid`
  - `split_equal`
- 多 provider 自动链接优先级：
  - 唯一 provider 直连
  - 同 `model_id` 过滤（若非空）
  - 地理+时间评分
- 已支持 quantitative reference 归一化：
  - `reference_normalization_mode=strict|lenient`（默认 `strict`）
- 已支持 allocation fraction 解析：
  - `allocation_fraction_mode=strict|lenient`（默认 `strict`）

代码位置：
- `crates/solver-worker/src/bin/snapshot_builder.rs`
- `crates/solver-worker/src/snapshot_artifacts.rs`
- `crates/solver-core/src/service.rs`

## 2. 问题一：`B` 中 `Input` 为负，LCIA 会不会出现负值？

状态：`✅ 已实现（gross）`

### 已实现内容

- 在 `snapshot_builder` 构建 `B` 时，对 elementary flow 不再按方向赋符号，统一采用：
  - `B_ij += amount_ij`
- 即 `Input` 与 `Output` 不再因为方向被强制加负号/正号；若源数据本身为负值则保留其负值语义。
- 实现位置：
  - `crates/solver-worker/src/bin/snapshot_builder.rs`
- 快照配置新增语义字段：
  - `biosphere_sign_mode = "gross"`
  - 持久化位置：`SnapshotBuildConfig`（artifact config + source fingerprint）

### 验收口径

- 同一批输入数据下，`gross` 结果不再因 `Input/Output` 方向翻转在 `B` 侧被机械抵消。
- 新构建快照与旧 `signed` 快照通过 `source_fingerprint` 自然区分，不会误复用。

## 3. 问题二：如何处理 Process 中的 Quantitative reference？

状态：`✅ 已实现`

### 已实现内容

- 构建期解析 `quantitativeReference.referenceToReferenceFlow`
- 定位 reference exchange，并将 exchange 统一换算到“每 1 reference unit”
- 模式开关：
  - `strict`：reference 缺失/非法直接失败
  - `lenient`：回退 `scale=1.0` 并计入诊断
- 覆盖诊断已输出：
  - `process_total`
  - `normalized_process_count`
  - `missing_reference_count`
  - `invalid_reference_count`

### 仍需关注

- `lenient` 仅建议用于调试，不进入生产结果链路

### 验收标准

- 同一 process 归一化后系数可重现
- 生产构建对缺失/非法 reference 有门禁（`strict`）

## 4. 问题三：如何处理 Process 中的 Allocation？

状态：`✅ 已实现`

### 已实现内容

- 构建期解析：
  - `processDataSet.exchanges.exchange[].allocations.allocation.@allocatedFraction`
- 支持 `"25"`、`"25%"`，统一为比例 `[0,1]`
- 入模值处理：
  - `amount_effective = amount_raw * reference_scale * allocation_fraction`
- 模式开关：
  - `strict`：缺失/非法 fraction 直接失败
  - `lenient`：回退 `fraction=1.0` 并计入诊断
- 覆盖诊断已输出：
  - `allocation_fraction_present_pct`
  - `allocation_fraction_missing_count`
  - `allocation_fraction_invalid_count`

### 验收标准

- 含 `@allocatedFraction` 样本与手工缩放一致
- `strict` 模式下缺失/非法 fraction 必须失败

## 5. 问题四：如何处理多个 provider？

状态：`🟡 部分实现`

### 已实现内容

- 多 provider 自动链接主链路已落地：
  - 第1层：唯一 provider 直连（`strict_unique_provider` 语义）
  - 第2层：优先同 `model_id` 候选子集
  - 第3层：地理+时间评分
- 支持策略：
  - `best_provider_strict`
  - `split_by_evidence`
  - `split_by_evidence_hybrid`
  - `split_equal`
- 已有 matching 覆盖指标：
  - `matched_unique_provider`
  - `matched_multi_provider`
  - `matched_multi_resolved`
  - `matched_multi_unresolved`
  - `matched_multi_fallback_equal`
  - `unmatched_no_provider`
  - `a_input_edges_written`

### 待实现内容

- provider 级证据明细落盘：
  - `evidence_vector`
  - `resolution_confidence`
  - `ambiguity_flag`
- 结果层不确定性场景输出：
  - `scenario_top1`
  - `scenario_evidence`
  - `scenario_equal`
- 与 Brightway 的系统性误差阈值验证与回归基准集

### Auto-link 规则（当前版本）

- 输入：
  - `flow_id`
  - `consumer_model_id`
  - `consumer_location`
  - `consumer_year`
  - `providers`
- 候选顺序：
  - 先唯一 provider 直连
  - 再同 `model_id` 过滤（若非空）
  - 最后地理+时间评分
- 地理评分：
  - 同国家同州：`1.00`
  - 同国家：`0.85`
  - 同区域：`0.60`
  - provider=`GLO`：`0.40`
  - 无法匹配：`0.10`
- 时间评分（`d=|provider_year-consumer_year|`）：
  - `d<=1`：`1.00`
  - `1<d<=3`：`0.85`
  - `3<d<=5`：`0.65`
  - `5<d<=10`：`0.40`
  - `d>10`：`0.20`
  - 缺失年份：`0.50`
- 综合分：
  - `score_i = 0.7 * geo_score + 0.3 * time_score`

## 6. 建议优先级

- P1：问题4证据明细与不确定性场景输出
- P1：持续扩展回归验证（含 Brightway 对照）
