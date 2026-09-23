# 项目进度

截至 2026-09-24：W1、W2 已完成；W3 进行中，C 的监控与评测骨架已完成，A/B 尚未开始。详见 [task_plan.md](task_plan.md)。

## W1 已完成


| 阶段          | 完成内容与验证记录                                                                                                                             |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 数据准备与标准化（B） | 完成四份数据剖析、Schema、时间转换、质量校验及去重。Taxi：9,554,778 输入、9,554,576 通过、202 拒绝；Weather：8,784 通过；Air：全国 8,139,551 行中，纽约范围 51,885 行通过；Zone：265 行通过。 |
| 环境与摄入（A）    | 固定 Python 3.11.9、JDK 21、Spark 4.2.0、Delta 4.4.0；完成通用 reader、Delta writer、CLI、同批次标记及 metadata，四份数据全量写入与读回通过。                           |
| 数据整合（C）     | 完成上下车区域关联、PM2.5 两级中位数汇总及小时环境关联。整合表保留 9,554,576 条唯一行程、91 个日期分区；NYC 范围 9,517,007 行的天气/空气小时匹配率均为 100%，具体指标缺测另计。                          |
| 性能实验（C）     | 完成 S0 不分区与 S1 按日期分区的对照，记录摄入时间、存储大小、文件数和查询耗时，核对结果一致。历史报告使用首次实验，最终复测记录单独保留。                                                             |
| 审核与最终回归     | 独立审核发现的问题已修复，复审通过；2026-09-09 最终完整回归为 27 项通过、276.692 秒。覆盖非法值、重复键、跨日/DST、关联行数与 Delta 读回等场景。                                             |
| 文档与打包       | 已整理英文设计/benchmark 文档和简洁英文 README，保留中文 README；2026-09-13 提交包已推送 `main`，提交为 `9ee8197`。                                                  |


### 最终证据与保留说明

- 整合结果与匹配统计：`data/delta/integrated/`。
- 首次 benchmark：`20260909T141616Z-ce81d328`，对应 [报告](docs/benchmark_report.md) 与 [原始计时](docs/benchmark_timings.csv)。
- 最终 benchmark：`data/benchmark/20260909T144949Z-b493da9f/results.json`，状态成功；四次摄入均保留 9,554,576 条唯一行程，24 次查询计时的结果一致。
- 提交包：[Week1_submission_2026-09-13.zip](submissions/Week1_submission_2026-09-13.zip)。已打包并推送不代表已在课程系统提交。
- 以上测试与全量运行是 W1 历史验证结果；本次只整理文档，没有重新运行 Spark 测试或全量数据。
- 天气来源时区仍有假设，环境关联只反映纽约范围的小时背景值；后续分析继续遵守 [数据契约](docs/data_contract.md)。

## W2 已完成

目标：六个 Spark SQL 查询、四张分析产品、四类优化对照实验。分工延续为 A 产品落盘 / B 查询口径 / C 优化与 benchmark。

| 角色 | 交付 | 状态 |
| --- | --- | --- |
| A | 四张产品（`data_products.py` + CLI + 元数据）、全量刷新入口 | 已合 `main`（PR #4）；审查修复后口径与 Q1–Q6 对齐，`schema_version` 环境产品升至 1.1.0 |
| B | Q1–Q6 Spark SQL、配置与小样本测试、[查询设计说明](docs/role_b_query_design.md) | 已合 `main`（PR #5） |
| C | 五项审查问题修复、13 项优化实验、[benchmark report](docs/w2_benchmark_report.md)、[优化策略](docs/w2_design_optimization.md) | 已完成于 `c/pipeline-fixes`（`1cab797`） |

审查（2026-09-19）五项 A/B 正确性问题（整合快照发布、Q3–Q5 日历、产品/查询口径、UTC 元数据、`tzdata`）均已修复；完整回归 **55 项通过**。全量产品刷新与六查询已在本机 W1 快照上跑通。

优化实验（运行 `20260919T143454Z-9d921a51`）：收益最大为数据产品改写（Q1 产品 9.27×）与 AQE（Q4 5.73×）；缓存对 Q4 几乎无收益；产品相对整合表存储开销约 0.016%。

## W3 当前状态

2026-09-23：已在 `task_plan.md` 确定 W3 分工——A：增量更新与分析一致性；B：校验扩展（不变）；C：监控、评测与材料汇总。

### 2026-09-24 C：监控与评测骨架（分支 `c/w3-monitoring`，提交 `bfe33a2`，未推送）

- 监控表 `metadata/pipeline_runs`（`monitoring.py`）取代 `ingestion_runs` 与 `product_refresh_runs`：一行对应一次执行中的一个阶段 × 目标，摄入、整合、产品刷新三处都已接入。`duplicate_count` 改为“目标表中已存在而跳过”，批内重复算作拒绝行，按码计数保留在 `validation_failure_counts_json`。
- 失败语义：阶段失败时永远抛阶段自己的异常，监控写入失败只附加 note；阶段成功但监控行丢失时抛 `MonitoringWriteError`。所有 CLI 新增 `--no-monitoring`，整合与产品刷新新增 `--run-id`。
- 五条运维 SQL 在 `sql/monitoring/`，入口 `scripts.run_monitoring_report`；`--import-legacy` 已把本机 W1/W2 的 4 行摄入、4 行产品刷新记录导入。现有数据上：Taxi 拒绝 202 行（`dropoff_before_pickup` 180、`timestamp_outside_source_period` 21、`duplicate_record` 1），最慢为 Taxi 摄入 156.8 秒。
- 评测骨架 `w3_evaluation.py` + `scripts.run_w3_evaluation`：每次运行（含预热）都在独立目录中使用基线的新副本，变体交替执行，输出计数不一致时不报时间。已可运行三项监控开销；增量、刷新、存储、校验四项标为 pending，并写明缺少的 A/B 入口。
- 子 agent 审查了方案，采纳的修改包括：计数守恒式、补充 `mode` / `validation_enabled` / `target_rows_before/after` / `input_paths_json` 等列、改用普通函数而非上下文管理器、放弃 Delta RESTORE 改为整目录复制（RESTORE 不删文件，存储数字会被污染）。
- 接口约定见 [docs/w3_interfaces.md](docs/w3_interfaces.md)，标 **agree** 的条目待 A/B 确认：`incremental.generate_update` / `apply_updates`、`refresh_data_products(mode=)`、校验开关、`check_schema`，以及有效时间窗口、溯源 lineage、跨批次去重、演进 CSV 读取顺序、manifest 重写这五项跨角色决定。
- 验证：完整回归 **70 项 OK，1009.6 秒**（新增 15 项）；`git diff --check` 通过。尚未在全量数据上运行评测。

下一步：

1. 把 [docs/w3_interfaces.md](docs/w3_interfaces.md) 发给 A、B 确认，时间窗口一条最先定，否则 A 生成的 Taxi 更新会被整批拒绝。
2. 在全量数据上跑三项监控开销（`python -m scripts.run_w3_evaluation --measurement monitoring_overhead_ingestion` 等），预计 40-60 分钟。
3. A 的 manifest 定型后放宽 `verify_integrated_provenance`；A/B 交付后跑其余四项评测。
