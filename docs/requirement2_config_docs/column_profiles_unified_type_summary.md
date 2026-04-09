# `column_profiles_unified.json` Type 总结

本文总结 `src/project_config/synth/column_semantics/column_profiles_unified.json` 中出现的 `type`，说明其作用及适用业务场景。

## 1) 生成分布（`columns.*.distribution.type` + `cross_column_constraints.then.distribution.type`）


| type                           | 作用（生成逻辑）                                                     | 典型业务场景（当前配置）                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `weighted_enum`                | 按权重从离散枚举值中采样；适合状态、分类、渠道等离散字段。                                | 覆盖最广：`main_profile`、`transaction_story`、`trading_story`、`credit_story`、`compliance_story`、`instrument_story`、`treasury_story`（如状态流转、渠道分布、币种结构、买卖方向）。 |
| `uniform_int`                  | 在 `[min, max]` 内均匀采样整数。                                      | 评分/年龄/逾期天数等区间型整数：`main_profile`、`transaction_story`、`credit_story`。                                                                                  |
| `bucketed_int`                 | 先按桶权重选区间，再在桶内均匀采样整数；可表达“分段密度”。                               | 评分分层、逾期分层：`main_profile`（含跨列约束中 lead_score 条件分布）、`risk_story`。                                                                                       |
| `uniform_from_rule_catalog`    | 从 `rule_catalog` 的规则枚举（或规则范围）中均匀采样；与校验规则强绑定。                 | 规则对齐字段：`credit_story`、`trading_story`（如国家/币种从规则白名单中取值）。                                                                                              |
| `log_uniform_money_fx`         | 在金额对数空间均匀采样（更贴近金融长尾），并按币种做 FX 换算与小数位处理。                      | 单峰长尾金额：`main_profile`、`credit_story`、`risk_story`、`trading_story`、`ledger_story`（如交易金额、敞口、头寸、估值）。                                                    |
| `mixture_log_uniform_money_fx` | 多个 log-uniform 金额区间按权重混合采样；用于多客群/多层级金额结构。                    | 多峰金额结构：`main_profile`、`transaction_story`、`treasury_story`、`agreement_story`、`ledger_story`（如 AUM、机会价值、资金交易金额）。                                      |
| `uniform_timestamp_days`       | 在 `0..max_days` 天与 `0..max_minutes` 分钟内均匀偏移生成时间戳。            | 通用时间窗采样：`trading_story`、`transaction_story`（如订单/执行时间、预期关闭时间）。                                                                                        |
| `weekday_timestamp`            | 在给定天数窗口内优先生成工作日时间，并限制日内分钟区间。                                 | 业务日节奏数据：`compliance_story`、`instrument_story`、`transaction_story`（如告警事件时间、行情快照时间、工作时段采集时间）。                                                          |
| `ordinal_by_anchor_softmax`    | 依据锚点列（`anchor_column`）映射目标分数，再对枚举分值做 softmax 衰减采样；表达列间有序相关性。 | 强业务相关的有序字段联动：`main_profile`（如 `opportunity_stage -> expected_value`、`segment_name -> segment_tier -> risk_band`）。                                    |
| `structured_json_object`       | 按 `pack_id` 读取 `json_object_packs.json` 模板，组装结构化 JSON 字符串。   | 半结构化 payload 字段：`main_profile`（客户画像、账户元数据、交易明细、客群画像 payload）。                                                                                        |


## 2) 评估基线（`evaluation_baselines.columns.*.type`）

> 这部分不直接生成数据，主要用于“生成结果是否贴近目标分布”的评估对照。


| type                | 作用（评估语义）                                     | 典型业务场景（当前配置）                                                                                    |
| ------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `categorical_probs` | 给出离散值概率分布，用于分类字段分布对比（如 KL/偏差监控场景）。           | `main_profile`、`transaction_story`、`trading_story`、`credit_story`（如国家、币种、状态、渠道）。                |
| `numeric_buckets`   | 给出数值分桶边界 `edges` 与桶概率 `probs`，用于连续/数值字段分布对比。 | `main_profile`、`transaction_story`、`trading_story`、`credit_story`、`risk_story`（如金额、年龄、逾期天数、胜率）。 |


## 3) 按业务故事归纳（便于快速选型）

- **CRM/营销/销售漏斗**（`main_profile`, `transaction_story`）：以 `weighted_enum`、`bucketed_int`、`ordinal_by_anchor_softmax` 为主，强调状态分布与阶段联动。
- **交易/资金/账务金额**（`trading_story`, `treasury_story`, `ledger_story`, `agreement_story`）：以 `log_uniform_money_fx`、`mixture_log_uniform_money_fx` 为主，强调长尾金额与多峰结构。
- **风险/授信**（`risk_story`, `credit_story`）：常用 `log_uniform_money_fx`、`uniform_int`/`bucketed_int`、`uniform_from_rule_catalog`，强调规则一致性与风险分层。
- **合规/市场数据时序**（`compliance_story`, `instrument_story`）：常用 `weekday_timestamp`，强调工作日与日内时段节奏。
- **半结构化扩展字段**（`main_profile`）：使用 `structured_json_object` 统一生成 JSON payload。

## 4) 当前文件中的 type 清单（去重）

- 生成分布 type（10 个）：`weighted_enum`、`uniform_int`、`bucketed_int`、`uniform_from_rule_catalog`、`log_uniform_money_fx`、`mixture_log_uniform_money_fx`、`uniform_timestamp_days`、`weekday_timestamp`、`ordinal_by_anchor_softmax`、`structured_json_object`
- 评估基线 type（2 个）：`categorical_probs`、`numeric_buckets`

