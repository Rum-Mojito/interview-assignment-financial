# Requirement 2：金融业务贴合策略与规则清单（CRM 生命周期 + SCD）

> 目标 schema：`examples/test_schemas/r2_customer_lead_opportunity_account_txn_scd.sql`  
> 当前仓库说明：Streamlit 默认引用了这个路径；对应优化逻辑落在 `src/project_config/**` 与 `src/synth/generator.py`。

## 1) 结构与关系优化

- **显式建模 CRM 生命周期事件路径（lead -> opportunity -> account -> txn）**  
  - 配置：`src/project_config/synth/topology/generation_topology.json`（`crm_customer_lifecycle_event_story`）  
  - 价值：数据生成遵循业务流程顺序，不是各表独立“铺行”。
- **显式声明 Lead/Opportunity/Account/Transaction 的 FK 链路**  
  - 配置：`src/project_config/common/concept_relation_graph.json`（`customer_to_lead`、`lead_to_sales_opportunity`、`sales_opportunity_to_account`、`account_to_transaction`）  
  - 价值：上下游主外键关系可解释，方便面试讲解和后续血缘核对。
- **针对 CRM 管道故事的概念到表映射**  
  - 配置：`src/project_config/common/concepts.json`（`leads`、`sales_opportunities`、`accounts`、`transactions`、`account_status_scd`）  
  - 价值：生命周期约束与分布 profile 能稳定绑定到目标表。

## 2) 基数与规模形态优化

- **为该生命周期 schema 定义专属按概念基数 profile**  
  - 配置：`src/project_config/synth/topology/cardinality_profiles.json`（`crm_customer_lifecycle_event_story`）  
  - 比例：`customer=1x`、`lead=2x`、`sales_opportunity=2x`、`account=2x`、`transaction=5x`，并包含 interaction/task/appointment/servicing  
  - 价值：更接近真实漏斗与开户后运营形态（每客户多事件、多交易）。
- **保留 CRM 存款链路 fallback**  
  - 配置：`src/project_config/synth/topology/generation_topology.json`（`crm_deposit_graph`、`party_deposit_ledger`）  
  - 价值：当完整生命周期链匹配不全时，仍可退化为稳定的 `customer-account-transaction` 生成。

## 3) 分布优化（让值更像业务数据）

- **Lead 状态与评分分布已配置**  
  - 配置：`src/project_config/synth/column_semantics/column_profiles_unified.json`（`leads.lead_status`、`leads.lead_score`）  
  - 价值：线索漏斗状态不再是均匀随机。
- **Opportunity 阶段分布已配置**  
  - 配置：`src/project_config/synth/column_semantics/column_profiles_unified.json`（`sales_opportunities.opportunity_stage`）  
  - 价值：中段阶段占比更高，关闭阶段占比更低，符合常见 CRM 漏斗。
- **预计金额与阶段锚定**  
  - 配置：`src/project_config/synth/column_semantics/column_profiles_unified.json`（`sales_opportunities.expected_value`，`ordinal_by_anchor_softmax`）  
  - 价值：商机阶段越成熟，金额分布越合理，避免“阶段-金额脱钩”。
- **胜率分布受限**  
  - 配置：`src/project_config/synth/column_semantics/column_profiles_unified.json`（`sales_opportunities.win_probability`）  
  - 价值：避免无约束数值导致的极端概率。
- **账户状态与账户状态 SCD 分布对齐**  
  - 配置：`src/project_config/synth/column_semantics/column_profiles_unified.json`（`accounts.status`、`account_status_scd.status`）  
  - 价值：当前状态与历史状态域一致，减少事实表与历史表冲突。

## 4) 状态机与状态迁移优化

- **账户生命周期状态机（硬约束）**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_ACCOUNT_STATUS_LIFECYCLE`）  
  - 迁移约束：`open -> pending -> active -> (dormant|frozen|closed)` 等  
  - 价值：避免不可能状态跳转。
- **账户状态 SCD 生命周期状态机（硬约束）**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_ACCOUNT_STATUS_SCD_LIFECYCLE`）  
  - 价值：SCD 时间线按合法路径推进，并支持审计字段生成。
- **Lead 生命周期状态机（硬约束）**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_LEAD_STATUS_LIFECYCLE`）  
  - 价值：线索状态演进符合漏斗逻辑（如 `new/contacted/qualified/converted/lost/recycled`）。
- **生成后强制执行生命周期约束**  
  - 代码：`src/synth/generator.py`（`_apply_lifecycle_constraints_for_generation`）  
  - 价值：即使初采样偏离，最终结果仍会纠正到合法状态路径。

## 5) 时序约束优化

- **Lead 创建/分配先后约束**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_LEAD_CREATED_BEFORE_ASSIGNED`）  
  - 价值：不会出现“先分配后创建”。
- **Lead -> Opportunity 的时序联动**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_LEAD_CAPTURED_BEFORE_OPPORTUNITY_EXPECTED_CLOSE`）  
  - 价值：商机时间不早于其来源线索。
- **Opportunity -> Account 的时序联动**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_OPPORTUNITY_EXPECTED_CLOSE_BEFORE_ACCOUNT_OPEN`）  
  - 价值：开户时间不会早于关键商机里程碑。
- **Account -> Transaction 的时序联动**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_ACCOUNT_OPEN_BEFORE_TRANSACTION_TIME`）  
  - 价值：交易时间符合账户生命周期。
- **Account -> SCD 历史时序联动**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_ACCOUNT_OPEN_BEFORE_STATUS_SCD_TIME`）  
  - 价值：状态历史不会早于账户存在时间。

## 6) 业务守恒与质量防线优化

- **关闭阶段商机必须有实际关闭时间**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_SALES_CLOSED_REQUIRES_ACTUAL_CLOSE_TIME`）  
  - 价值：避免 closed deal 缺少关闭时间戳。
- **商机金额与胜率范围约束**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_OPPORTUNITY_VALUE_POSITIVE`、`HARD_OPPORTUNITY_WIN_PROBABILITY_RANGE`）  
  - 价值：关键数值字段保持在业务合理区间。
- **Lead 评分范围约束**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_LEAD_SCORE_REASONABLE_RANGE`）  
  - 价值：评分口径可直接对齐下游看板/模型。
- **KYC 门控：open/active 账户必须 VERIFIED**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_ACCOUNT_OPEN_OR_ACTIVE_REQUIRES_VERIFIED_KYC`）  
  - 价值：账户状态与合规状态联动，贴近真实开户控制。
- **账户当前状态必须与 SCD 当前行一致**  
  - 配置：`src/project_config/synth/compliance/lifecycle_constraints.json`（`HARD_ACCOUNT_STATUS_SCD_CURRENT_MATCH`、`HARD_ACCOUNT_STATUS_SCD_CURRENT_IS_LATEST`）  
  - 价值：事实表与历史表内部一致。

## 7) 生成器侧专项后处理优化

- **商机关单时间硬化处理**  
  - 代码：`src/synth/generator.py`（`_enforce_sales_opportunity_close_time_order`）  
  - 行为：
    - 非关闭阶段清空 `actual_close_time`；
    - 关闭阶段强制 `actual_close_time >= expected_close_date`；
    - 若仅有 `actual_close_time`，自动回填 `expected_close_date`。
  - 价值：消除 CRM 管道里最常见的时间矛盾。
- **账户状态历史按账户最终状态自动同步**  
  - 代码：`src/synth/generator.py`（`_synchronize_account_status_history_fact`）  
  - 行为：从 lifecycle 配置推导 SCD 路径，并生成确定性的 `status_event_id`、`trace_id`、`is_current`、`source_system`。  
  - 价值：避免“账户当前状态”和“历史状态表”脱节。
- **生命周期检查前先做状态归一化**  
  - 配置 + 代码：`src/project_config/synth/compliance/status_value_normalization.json` + `src/synth/generator.py`（`_normalize_status_value_for_generation`）  
  - 价值：如 `opened/approved/blocked` 这类别名先归并为规范 token，再进入状态机校验。

## 8) 与该 schema 家族相关的 overlay 对齐

- **零售存款 overlay：账户状态/币种/金额形态对齐**  
  - 配置：`src/project_config/synth/behavior/scenario_overlays.json`  
  - 价值：即使是生命周期 schema，也能在账户与交易层面体现更真实的国家/账户类型分布。
- **SCD 历史来源系统分布**  
  - 配置：`src/project_config/synth/behavior/scenario_overlays.json`（`account_status_history_source_system_distribution`）  
  - 价值：SCD 行会带业务可解释的来源系统标签（如 `core_banking`）。

## 9) 快速验收清单（针对该 schema）

- `sales_opportunities`：`closed_won/closed_lost` 行始终有非空 `actual_close_time`。
- `sales_opportunities`：关闭阶段 `actual_close_time` 不早于 `expected_close_date`。
- `leads -> sales_opportunities`：`captured_time <= expected_close_date`。
- `sales_opportunities -> accounts`：`expected_close_date <= opened_time`。
- `accounts -> transactions`：`opened_time <= transaction_time`。
- `accounts <-> account_status_scd`：每个 `account_id` 只有 1 条 `is_current='Y'`，且与 `accounts.status` 一致。
- `accounts.status` 为 `open/active` 时，`customers.profile_json.kyc_status = VERIFIED`。