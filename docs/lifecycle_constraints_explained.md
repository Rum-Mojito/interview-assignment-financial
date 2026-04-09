# `lifecycle_constraints` 说明文档

本文说明 `src/project_config/synth/compliance/lifecycle_constraints.json` 的用途、结构、执行方式与使用建议。

---

## 1. 这个文件是做什么的

`lifecycle_constraints.json` 是 Requirement2 的生命周期与时序约束“单一真相源”（single source of truth）：

- 用于**生成后修正**（generation post-process），把不合理状态/时间拉回到可接受范围；
- 用于**规则校验**（rule engine），输出 hard violation 与每条规则的 pass rate 指标；
- 也被**声明式 FSM 绑定**复用（`declarative_fsm` 从这里读取状态机定义）。

简单说：它不是“分布采样配置”，而是“业务一致性约束配置”。

---

## 2. 文件结构总览

顶层有 4 个规则组：

- `state_machine_rules`（8 条）：同一实体状态流转是否合法；
- `temporal_order_rules`（11 条）：同表行内时间先后是否合法；
- `cross_table_temporal_rules`（6 条）：跨表主从事件时间先后是否合法；
- `business_conservation_rules`（11 条）：数值守恒、状态-时间、父子一致性等业务约束是否合法。

---

## 3. 四大规则组详解

## `state_machine_rules`

### 作用
约束某个实体（如一个 `account_id`）在时间序列上的状态演进，只能走 `allowed_transitions` 定义的边。

### 关键字段
- `rule_id`: 规则唯一标识（建议稳定不变）  
- `table_name`: 作用表
- `entity_key_columns`: 实体主键（可组合键）
- `status_column`: 状态列
- `sequence_time_column`: 序列排序时间列
- `initial_states`: 多行实体时首状态允许集合
- `singleton_allowed_states`: 单行实体时允许状态集合
- `allowed_transitions`: 状态机边（from -> [to...]）

### 典型场景
- 账户状态：`open -> pending -> active -> dormant/frozen -> closed`
- 线索状态：`new/contacted -> qualified -> converted/lost`
- KYC case 生命周期、服务工单生命周期、预约状态生命周期。

---

## `temporal_order_rules`

### 作用
校验同一行内两个时间列的先后关系（`<=` 或 `<`）。

### 关键字段
- `table_name`
- `constraints`（数组）：
  - `left_column`
  - `operator`（`<=` 或 `<`）
  - `right_column`
  - `apply_when_column` + `apply_when_in`（可选，条件生效）

### 典型场景
- `opened_time <= event_time`
- `case_opened_time <= case_closed_time`
- 仅在机会关闭阶段时，要求 `expected_close_date <= actual_close_time`。

---

## `cross_table_temporal_rules`

### 作用
约束跨表父子/前后事件的时间先后关系（例如订单先于执行）。

### 关键字段
- `left_table_name` / `right_table_name`
- `left_key_columns` / `right_foreign_key_columns`（长度必须一致）
- `left_time_column` / `right_time_column`
- `operator`（`<=` 或 `<`）

### 典型场景
- `orders.order_time <= executions.execution_time`
- `accounts.opened_time <= transactions.transaction_time`
- `accounts.opened_time <= account_status_scd.status_time`。

---

## `business_conservation_rules`

### 作用
表达非纯时序的业务硬约束，当前支持 7 种 `type`：

- `intra_row_numeric_compare`  
  - 同行两数比较（如 `available_amount <= approved_amount`）
- `intra_row_numeric_range`  
  - 数值范围（如 `lead_score` 在 `[0,100]`）
- `state_requires_non_null_time`  
  - 某状态必须有非空时间（如 closed 必须有实际关闭时间）
- `aggregate_child_amount_le_parent_limit`  
  - 子表金额聚合不超过父表限额
- `current_state_matches_parent_status`  
  - 历史表 current 行状态必须与父表状态一致
- `current_flag_must_be_latest_time`  
  - current 标记必须落在该实体最新时间行
- `child_state_requires_parent_json_value_in`  
  - 子状态受父表 JSON 字段值约束（通过 `parent_json_path` 读取）

### 典型场景
- 授信额度与提款累计一致性；
- SCD current 行唯一性 + 最新性；
- “账户 open/active 需客户 KYC=VERIFIED”。

---

## 4. 运行时怎么生效（生成 vs 校验）

## 4.1 生成阶段（修正）
在 `src/synth/generator.py` 的 `_apply_lifecycle_constraints_for_generation` 里依次应用四类规则：

- temporal -> cross-table temporal -> state machine -> business conservation

但注意当前“生成修正”实现是**子集实现**：

- `temporal_order_rules`: 会把 `right_time` 修正到不早于 `left_time`
- `cross_table_temporal_rules`: 会把右表时间修正到不早于左表最早时间
- `state_machine_rules`: 会按状态机修正非法跳转
- `business_conservation_rules`: 目前仅实现了
  - `intra_row_numeric_range`
  - `state_requires_non_null_time`

也就是说，`business_conservation_rules` 的其余类型目前在生成阶段**不自动修正**，主要靠校验阶段兜底发现问题。

## 4.2 校验阶段（判定）
在 `src/validation/rule_engine.py` 的 `_evaluate_lifecycle_constraints` 中，四类规则都会被完整评估：

- 统计 `hard_checks_total` / `hard_checks_failed`
- 输出逐条 `rule_metrics`（checked、failed、pass_rate）
- 生成具体 violation（带 rule_id、行号、字段、错误信息）

---

## 5. 与其他配置的关系

- `src/project_config/config_groups.json` 已把该文件纳入 synth 配置组；
- `src/schema/financial_config_validate.py` 会做结构校验（字段必填、类型、operator 合法性、业务规则 type 白名单等）；
- `src/synth/declarative_fsm.py` 可按 `lifecycle_rule_id` 读取状态机给 FSM 采样/覆盖逻辑复用。

---

## 6. 常见配置建议

- **规则命名**：`rule_id` 统一使用业务语义 + 强度前缀（如 `HARD_...`）；
- **先校验再扩展**：新增 `business_conservation_rules.type` 时，先补 `financial_config_validate.py` 白名单；
- **生成与校验一致性**：如果新增规则希望“自动修正”，需同时补 `generator.py` 对应 apply 逻辑；
- **状态值规范化**：涉及状态比较的规则，尽量配合 `status_value_normalization`，减少别名误报；
- **跨表键对齐**：`left_key_columns` 与 `right_foreign_key_columns` 必须等长且语义一一对应。

---

## 7. 当前规则数量快照

- `state_machine_rules`: 8
- `temporal_order_rules`: 11
- `cross_table_temporal_rules`: 6
- `business_conservation_rules`: 11

合计：36 条 lifecycle/business 约束规则。
