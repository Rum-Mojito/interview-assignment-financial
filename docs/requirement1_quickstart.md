# Requirement 1 速览：从业务场景到多表 Schema

## 1. 输入与输出

| 方向 | 内容 |
|------|------|
| **CLI** | `--scenario-text`、`--output-dir`；可选 `--unknown-domain-policy`、`--write-schema-report`；默认 **干净模式**，`--full-artifacts` 可打开完整证据落盘 |
| **核心输出** | 运行目录 `r1_schema_*` 下 **`generated_schema.sql`**、**`generated_schema.json`** |
| **异常/兜底** | 匹配过弱或置信不足时可能 **`unknown_domain`**，并可能产出 **`draft_schema.*`**（完整模式下可有 onboarding 等；干净模式常跳过部分落盘） |

---

## 2. 端到端步骤（先有一张总表）

| 顺序 | 代码步骤 | 主要吃哪些配置 |
|------|----------|----------------|
| 0 | `validate_financial_schema_configs()` | `project_config` 下 schema 相关 JSON，避免断引用与矛盾 |
| 1 | 别名匹配 `_extract_concepts_from_scenario_text` | `schema/concepts.json`（`aliases` 等）、`schema/feedback_weights.json`（调权，若有） |
| 2 | 关系闭包（多轮） | `schema/concept_relation_graph.json`（父依赖、扩展概念） |
| 3 | 域/主题信号 + 域信号推断 + 域扩展规则 | `schema/domain_taxonomy.json`、`schema/domain_signal_inference_rules.json`、`schema/domain_extension_rules.json` |
| 4 | 系统画像推断 + 业务线补全 | `schema/system_profiles.json`、`schema/business_lines.json` |
| 5 | `_build_schema_from_concepts` | `schema/concepts.json` 的 `required_packs` → `schema/field_packs.json`（列、类型、半结构化） |
| 6 | `infer_foreign_keys()` | `concept_relation_graph` 边 + `*_id` 等列名启发 |

主编排：`generate_schema_from_scenario_with_report()`（`src/schema/scenario_generator.py`）。

---

## 3. 配置驱动：目标与三层模型

与「写死表清单」不同，本实现强调（**配置驱动**）：

- **概念与字段以 JSON 维护**：`concepts` + `field_packs` 决定「有哪些表、每表有哪些列」；改表结构优先改配置。
- **域与规则负责「从话里多捞出概念」**：`domain_taxonomy` 提供信号锚点；`domain_signal_inference_rules` / `domain_extension_rules` 用 **`business_rationale`** 表达补表原因，便于理解与审计对齐。
- **系统画像与业务线收敛边界**：`system_profiles`、`business_lines` 把全局概念库收到「像 CRM / 交易 / 信贷」这类**部署视角**，再配合剪枝控制噪声。

业务知识在配置里可粗分为 **三层**（与下面 §4 各文件对应）：

| 层级 | 回答的问题 | 主要配置文件（路径相对 `src/project_config/`） |
|------|------------|----------------|
| **概念与表结构层** | 「有哪些业务对象、每张表有哪些列」 | `schema/concepts.json`、`schema/field_packs.json`、`schema/concept_relation_graph.json` |
| **域与补全规则层** | 「从场景话术中识别域/主题、何时多挂一张卫星表」 | `schema/domain_taxonomy.json`、`schema/domain_signal_inference_rules.json`、`schema/domain_extension_rules.json` |
| **系统与组装层** | 「像哪套系统、条线还要补谁、最终如何组装 DDL 与 FK」 | `schema/system_profiles.json`、`schema/business_lines.json`；组装与 FK 在 `scenario_generator.py` / `relation_inference.py` |

**入口编排**（`src/interfaces/cli_schema.py` 的 `run_requirement1()` → `generate_schema_from_scenario_with_report`）：负责阈值、闭包顺序、剪枝与导出；**业务「长哪些表」由上述 JSON 注入**，代码不手写具体银行表名清单。

下面按 **流水线顺序**，对 **每个关键配置文件** 给一个 **mini 用例**（路径均相对于 `src/project_config/`，除非另行说明）。

---

## 4. 配置文件与用例（沿流水线）

### 4.1 启动校验（多文件）

**做什么**：`validate_financial_schema_configs()` 检查 concepts、field_packs、关系图、域规则、system_profiles 等 **引用一致**，避免生成到一半才发现 JSON 断了。

**用例**：若 `concepts.json` 里某概念的 `required_packs` 指向不存在的 pack id，校验应 **失败**，而不是产出缺列的 DDL。

---

### 4.2 `schema/concepts.json`

**在流水线哪一步**：别名匹配阶段用 **概念 id、aliases、反馈权重** 等，从 `scenario_text` 里捞出初始概念集合。

**用例**：场景里出现 “customer relationship” 与 “deposit accounts”。配置里 `customer` 概念带有别名 `client`、`party` 等，命中后进入后续闭包与域规则。**没有出现在 `concepts` 里的对象，绝不会凭空长成一张表**。

---

### 4.3 `schema/field_packs.json`

**做什么**：定义 **字段包**（列名、类型、是否半结构化等）；`concepts` 通过 `required_packs` **组合**出每张逻辑表的列。

**用例**：`customer` 概念要求 `party_core_pack` + `profile_json_pack`，组装后 `customers` 表既有主数据列，也有 **`profile_json`** 等半结构化承载位，无需在代码里逐列 `append`。

---

### 4.4 `schema/concept_relation_graph.json`

**做什么**：概念之间的 **依赖/父子** 边；闭包沿图扩展，避免「只要交易不要账户」的半张网。

**用例**：场景只直接命中 `transaction`，图上要求 `transaction` 依赖 `account`，闭包后 **自动纳入 `account`（及进一步父概念）**，保证 FK 故事能讲圆。

---

### 4.5 `schema/domain_taxonomy.json`

**做什么**：**Level-1 域**（如 `party_legal_relationship`）与 **Level-2 主题** 的 `signal_keywords`；运行时用 **词边界** 扫描场景文本，得到「这话像哪些金融业务域同时出现」。

**用例**：场景里同时出现 `customer`、`compliance` 类词汇，可能点亮 **party 域** 与 **compliance 域**，供下一步 `domain_signal_inference_rules` 做 **跨域合取** 推断。

---

### 4.6 `schema/domain_signal_inference_rules.json`

**做什么**：基于 **域/主题命中**（如 `when_domains_matched_all`），**追加** `append_concepts`；与「当前已有概念是否齐套」无关，偏 **叙事级补表**。

**用例**：规则约定「**party 域 + compliance 域** 同时亮灯 → 追加 `customer_identification`（KYC 证件卫星）」。场景没明说「证件表」，但配置认为 **合规叙事** 下应有该表。

---

### 4.7 `schema/domain_extension_rules.json`

**做什么**：当 **已匹配概念集合** 满足 `when_matched_contains_all` 时追加概念，偏 **模型级补链**（卫星表、授信链下一环等）。

**用例**：`when_matched_contains_all: ["obligor", "facility"]` → 追加 **`exposure`**（敞口），体现「债户 + 额度都在场时才需要敞口事实」。

---

### 4.8 `schema/system_profiles.json`

**做什么**：**crm / trading / credit** 等 profile 的 `default_concepts`、`required_concepts` 与权重；根据当前概念集合 **推断** `inferred_system_name`，收敛「像哪套系统」。

**用例**：命中概念与 **trading** profile 重叠度高，推断为交易系统，后续 **业务线与剪枝策略** 按 trading 语境处理（与纯 CRM 大包区分）。

---

### 4.9 `schema/business_lines.json`

**做什么**：在推断出的系统画像下，再按 **条线** 补一批概念（表），贴近「零售/公司与同业」等机构叙事。

**用例**：profile 为 `crm` 时补「管户、线索」等条线概念（以文件内实际配置为准），使表集合 **像真实 CRM 范围**，而不是只有三张裸表。

---

### 4.10 剪枝（实现于 `scenario_generator.py`，非独立 JSON）

**做什么**：非 CRM 等场景下，去掉「仅靠闭包挂上、且属于 CRM 噪声包、且非场景直接命中」的概念；并处理如 **`trading_account` vs 泛化 `account`** 的冲突。

**用例**：交易场景不应被无关 CRM 卫星表淹没；剪枝后概念集合 **更聚焦**，再进入组装。

---

### 4.11 外键推断 `infer_foreign_keys()`（`src/schema/relation_inference.py`）

**做什么**：在表已组装后，优先用 **关系图边** + `*_id` **列名启发** 推断 FK。

**用例**：`accounts` 上有 `customer_id`，`concept_relation_graph` 上存在 account→customer 的依赖，则推断 **外键指向 `customers.customer_id`**，DDL 里可执行、面试官一眼能看懂引用链。

---

## 5. 串起来的一小段「故事」

1. **`concepts` + 别名** 从话里捞出 `customer`、`account`、`transaction`。  
2. **`concept_relation_graph`** 闭包补齐父概念，避免断链。  
3. **`domain_taxonomy`** 扫域信号 → **`domain_signal_inference_rules`** / **`domain_extension_rules`** 按规则补 KYC、敞口等卫星概念。  
4. **`system_profiles` + `business_lines`** 收敛到「像 CRM 存贷」并补条线表。  
5. **剪枝** 去掉跨 profile 噪声；**`field_packs`** 合并出列与半结构化列。  
6. **`infer_foreign_keys`** 拉上 FK；导出 **`generated_schema.sql` / `.json`**。

---

## 6. 配置驱动的业务落地示例

**示例目标**：希望场景里只要同时提到 **「借款人」和「授信额度」**，就 **自动带上敞口表**，而不用改 Python 主流程。

**只需改配置**：

- 在 **`domain_extension_rules.json`** 增加或调整一条规则：`when_matched_contains_all` 包含 `obligor` 与 `facility`，`append_concepts` 含 `exposure`，并写好 **`business_rationale`**（便于解释「为什么这两张表齐了就要敞口」）。

若希望某说法更容易命中概念，可在 **`concepts.json`** 给对应概念增加 **别名** 或调整 **`feedback_weights.json`**（若有），而无需改 `scenario_generator` 的匹配函数结构。

---

## 7. 工程取舍与可迁移价值

**取舍**

- **优先配置化**：新表、新卫星、新域规则以 JSON 迭代；代码保持单一路径（匹配 → 闭包 → 规则 → 画像 → 组装 → FK）。  
- **可解释落盘**：完整模式（`--full-artifacts`）下可有 **`entity_match_report.json`**（概念分、域信号、`domain_extension_rule_hits` 等），方便面试官追问「为什么多了这张表」。  
- **诚实未知**：`unknown_domain` + **`draft_schema.*`**，匹配不足时不硬凑完整业务模型。

**已知边界**

- 极短或极偏的场景可能导致 **unknown** 或 **低置信**；需调别名、域关键词或补配置。  
- **词边界** 是为减少误命中；若业务术语过生僻，要在 `concepts` / `domain_taxonomy` 里补信号。  
- `system_profiles` 只有有限几种部署束；新业务线通过 **`business_lines`** 与概念库扩展，而非无限新增 CLI 开关。

**可迁移性**

- 换行业故事线时，主要换 **concepts / field_packs / 域规则 / 画像**，编排代码可保持稳定。

更细的分层、流程图与模块职责见 **`docs/requirement1_design_workflow.md`**；逐步操作见 **`docs/non_tech_how_to_generate_scheam.md`**。

---

## 8. 入口与深读

| 用途 | 位置 |
|------|------|
| CLI | `python -m src.interfaces.cli_schema` |
| Web | `src/interfaces/streamlit_app.py`（R1 页） |
| 核心编排 | `src/schema/scenario_generator.py` |
| 图与闭包 | `src/schema/knowledge_graph.py` |
| 外键推断 | `src/schema/relation_inference.py` |
| 配置校验 | `src/schema/financial_config_validate.py` |

执行入口示例：

```bash
python -m src.interfaces.cli_schema \
  --scenario-text "CRM customers have accounts and transactions with json xml text fields" \
  --output-dir outputs/runs/manual
```

---
