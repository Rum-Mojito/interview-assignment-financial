# Requirement 2 速览：从已有 Schema 到合成数据

## 1. 输入与输出


| 方向      | 内容                                                                            |
| ------- | ----------------------------------------------------------------------------- |
| **CLI** | `--schema-path`、`--output-dir`、`--record-count`、`--seed`；可选 `--graph-path-id` |
| **输出**  | 运行目录 `r2_data_*` 下 `**synthetic_data/*.csv`**                                 |


---

## 2. 端到端步骤（先有一张总表）


| 顺序  | 代码步骤                                  | 主要吃哪些配置                                                                                                        |
| --- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| 0   | `validate_financial_schema_configs()` | 启动前扫一遍 `project_config` 下相关 JSON，避免断引用                                                                         |
| 1   | `parse_schema`                        | 输入文件本身（DDL/JSON）                                                                                               |
| 2   | `plan_table_order`                    | 解析得到的 FK 依赖                                                                                                    |
| 3   | `map_schema_to_concepts`              | `schema/concepts.json`、`concept_relation_graph.json`、`synth/topology/generation_topology.json` 等               |
| 4   | `generate_dataset`                    | 拓扑、基数、manifest、列语义、JSON pack、overlay、`generation_rules`、`lifecycle_constraints`、`status_value_normalization` 等 |
| 5   | `export_synthetic_data_csv`           | 无额外配置文件                                                                                                        |


---

## 3. 配置驱动：目标与三层模型

与「随机造数」不同，本实现强调（**配置驱动，而非把业务规则写死在分支里**）：

- **业务语义以配置表达**：结构、分布、约束主要在 `src/project_config/**/*.json` 维护，代码负责编排与执行边界。
- **生成侧与规则侧共享同一套 `rule_id`**（如生命周期状态机）：降低「生成出来一套、校验又一套」的口径偏差。（说明：当前 **档位 A** 入口不落盘质检报告，但 `generate_dataset` 内仍按配置做状态机与门控采样。）

业务知识在配置里拆成 **三层**（每层对应下面 §4 中的若干文件）：


| 层级      | 回答的问题                           | 主要配置文件                                                                                                                                                                                                             |
| ------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **结构层** | 生成 **关系与路径**：表→概念、走哪条拓扑、父子顺序与基数 | `schema/concepts.json`、`schema/concept_relation_graph.json`、`synth/topology/generation_topology.json`、`synth/topology/cardinality_profiles.json`                                                                   |
| **分布层** | 字段值 **长什么样**：列分布、半结构化 JSON、场景偏置 | `synth/column_semantics/column_profiles_unified.json`、`synth/column_semantics/json_object_packs.json`、`synth/behavior/scenario_overlays.json`（另含 `synth/topology/generation_config_manifest.json` 的域→profile 等横切项） |
| **约束层** | **哪些结果必须成立**：状态机、时序、归一、列与规则的绑定  | `synth/compliance/lifecycle_constraints.json`、`synth/compliance/status_value_normalization.json`、`synth/behavior/generation_rules.json`                                                                            |


**入口编排**（`src/interfaces/cli_synth.py` 的 `run_requirement2()`）只做：解析 Schema → 规划表顺序 → 表→概念→拓扑 → `generate_dataset` → 写 CSV；**业务策略由上述 JSON 注入**，代码不承载具体银行条款。

下面按 **流水线顺序**，对 **每个关键配置文件** 给一个 **mini 用例**，帮助把 workflow 串起来（路径均相对于 `src/project_config/`）。

---

## 4. 配置文件与用例（沿流水线）

### 4.1 启动校验（多文件）

**做什么**：`validate_financial_schema_configs()` 会检查 concepts、field_packs、拓扑、synth 合规等 **JSON 之间引用是否一致**。

**用例**：若有人在 `generation_topology.json` 里写了一个不存在的 `cardinality_profile_id`，校验应 **失败**，避免生成到一半才报错。

---

### 4.2 `schema/concepts.json` + `schema/concept_relation_graph.json`

**在流水线哪一步**：`map_schema_to_concepts` 把 **物理表名/列** 对上 **业务概念**（如 `customers` → `customer`）。

**用例**：Schema 里有一张表叫 `customers`。配置里 `customer` 概念带有别名 `party`、`retail client` 等。映射器根据表名命中 `customer`，后续才能选用 **CRM** 相关拓扑，而不是交易链。

---

### 4.3 `synth/topology/generation_topology.json`

**做什么**：定义 **图路径** `graph_event_paths`（如 `crm_deposit_graph`：customer → account → transaction）和 **fallback 链** `chains`。

**用例**：你的 schema 里恰好有 `customer`、`account`、`transaction` 三类表，且命中 `party_deposit_ledger` 链。解析器会选 `**engine: event_first`**，按 **边顺序** 生成事件链上的行，而不是纯按表逐张「行级铺数」。  

---

### 4.4 `synth/topology/cardinality_profiles.json`

**做什么**：在选定拓扑后，决定 **每张表大概多少行**（相对 `--record-count` 的倍数）。

**用例**：`party_deposit_ledger` 里约定：`customer = base`，`account = 2 * base`，`transaction = 5 * base`。若 `record_count=300`，则大约 **300 个客户、600 个账户、1500 笔交易**（具体还受 event_first 实现影响，但数量级由该 profile 表达）。

---

### 4.5 `synth/topology/generation_config_manifest.json`

**做什么**：全 synth 的 **默认与横切参数**：例如 **按一级域选列语义 profile**（`column_semantics_profile_by_domain`）、币种小数位、简易 FX 表等。

**用例**：映射阶段推断主域为 `party_legal_relationship` 时，列语义会优先走 manifest 里映射到的 profile（如 `main_profile`），从而影响 **金额小数位、时间戳形态** 等基线。

---

### 4.6 `synth/column_semantics/column_profiles_unified.json`

**做什么**：按 **profile_id** 给 **列级分布**（金额混合分布、枚举权重、工作日时间窗等）。

**用例**：`compliance_story` profile 里可为 `aml_alerts.event_time` 配置 `**weekday_timestamp`**（只在工作日某时段内采样）。当生成器处理到 AML 相关列时，时间会 **落在配置的工作时段**，而不是均匀随机到半夜。

---

### 4.7 `synth/column_semantics/json_object_packs.json`

**做什么**：给 `**profile_json` 等半结构化列** 提供 **嵌套字段** 的生成模板（如 `kyc_status`、`risk_profile`）。

**用例**：`customer_profile_pack` 里 `kyc_status` 使用权重：`VERIFIED 82%`、`PENDING_REVIEW 14%`…… 生成出来的 `customers.profile_json` 里会出现 **结构化且可统计** 的 KYC 状态，供后续 **账户状态与 KYC 门控** 使用。

---

### 4.8 `synth/behavior/scenario_overlays.json`

**做什么**：在 **表/列齐全且命中场景条件** 时，叠加 **业务偏置**（账户类型权重、按国家拆交易币种、旅程阶段与账户状态联动等）。

**用例**：场景 `**retail_deposit_standard`** 要求存在 `customers`、`accounts`、`transactions` 等，且概念路径对上 `**crm_deposit_graph**`。命中后，`accounts.account_type` 可能在 `savings/checking/...` 上按配置的 **权重** 抽样，而不是均匀随机。

---

### 4.9 `synth/behavior/generation_rules.json`

**做什么**：声明 **行为包**、以及 **列 → 生命周期规则 ID**（供生成阶段 FSM 使用），例如 `accounts.status` 绑定 `HARD_ACCOUNT_STATUS_LIFECYCLE`。

**用例**：生成 `accounts.status` 时，不是独立乱抽枚举，而是按 `**lifecycle_constraints.json` 里同名的状态机** 做 **有序迁移**（与 `event_time` 等列配合）。

---

### 4.10 `synth/compliance/lifecycle_constraints.json`

**做什么**：**状态机、跨表条件** 等规则的 **单一声明源**（生成器与若存在的离线校验共用同一 `rule_id`）。

**用例**：`HARD_ACCOUNT_STATUS_LIFECYCLE` 规定 `open → pending → active → ...` 的 **允许迁移**。生成阶段据此产生 **时间上合理** 的状态序列。  
另一条 `HARD_ACCOUNT_OPEN_OR_ACTIVE_REQUIRES_VERIFIED_KYC`：若账户状态为 `open`/`active`，则对应客户的 `profile_json.kyc_status` 必须为 `**VERIFIED`**——体现 **KYC 门控**。

---

### 4.11 `synth/compliance/status_value_normalization.json`

**做什么**：把 **别名枚举** 收拢到 **规范 token**，便于 FSM 与规则一致。

**用例**：Schema 或上游若写出 `accounts.status = "opened"`，归一化映射到 `**open`**，与 `lifecycle_constraints` 里的状态名一致，避免 **同义不同字导致状态机断裂**。

---

## 5. 串起来的一小段「故事」（对应上面文件）

1. 表名对上 `**concepts`** → 选中 `**crm_deposit_graph**` 拓扑与 `**party_deposit_ledger**` 基数。
2. `**manifest**` 选定列语义 profile；`**column_profiles_unified**` 决定金额/时间形态。
3. `**json_object_packs**` 生成客户 `profile_json.kyc_status`。
4. `**scenario_overlays**` 在零售存贷场景下偏置账户类型/币种等。
5. `**generation_rules**` 把 `accounts.status` 绑到 **lifecycle** 状态机；`**status_value_normalization`** 统一状态字符串。
6. 最后 `**export_synthetic_data_csv**` 按表写出 CSV。

---

## 6. 配置驱动的业务落地示例（摘自 `requirement2_config_driven_business_guide` §4）

**示例目标**：让「中国零售储蓄账户」场景下，交易 **更偏向人民币**，且金额 **偏小额长尾**。

**只需改配置**（无需改 `generator.py` 主流程）：

- `**scenario_overlays.json`**  
  - 中与币种、金额相关的段（如 `transaction_currency_by_country_account_type`、`transaction_amount_mixture_by_account_type` 等，以文件内实际 key 为准）。
- （可选）`**column_profiles_unified.json**`  
  - 中 `transactions.currency`、`transactions.amount` 的分布定义。

这体现 **业务变化通过配置发布**：迭代时优先调 JSON，而不是在代码里加 `if country == "CN"` 式分支。

---

## 7. 工程取舍与可迁移价值（摘自 `requirement2_config_driven_business_guide` §6–7，摘要）

**取舍**

- **优先配置化**：业务变更成本相对低、迭代快。  
- **保留代码边界**：编排、容错、执行顺序（解析顺序、`event_first` vs 行级回退等）仍由代码统一管理。  
- **规则标识一致**：同一 `rule_id` 连接生成侧 FSM 与（若启用时的）校验侧，减少口径漂移。

**已知边界**

- 极定制化逻辑仍可能需要代码扩展，并非 100% 配置覆盖。  
- 输入 Schema 的表名列名质量会影响概念映射置信度。  
- `event_first` 依赖路径解析与键池；不满足时会 **回退** `rowwise`，以保证能出数。

**可迁移性（为何能迁到 CRM / 交易 / 授信等域）**

- 规则表达与执行引擎解耦；结构 / 分布 / 约束三层模型稳定；便于换配置换故事线。

更完整的论述与条款级说明见 `**docs/requirement2_config_driven_business_guide.md`**。

---

## 8. 入口与深读


| 用途     | 位置                                                  |
| ------ | --------------------------------------------------- |
| CLI    | `python -m src.interfaces.cli_synth`                |
| Web    | `src/interfaces/streamlit_app.py`（R2 页）             |
| 生成核心   | `src/synth/generator.py`                            |
| 配置驱动长文 | `docs/requirement2_config_driven_business_guide.md` |
| 总体设计   | `docs/data_synthesizer_design.md` §3.2              |


执行入口示例：

```bash
python -m src.interfaces.cli_synth \
  --schema-path requirement/sample_schema.sql \
  --output-dir outputs \
  --record-count 500 \
  --seed 42
```

可选：`--graph-path-id` 与 `synth/topology/generation_topology.json` 中 `path_id` 对齐。

---

