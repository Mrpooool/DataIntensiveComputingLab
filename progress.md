# 项目进度

截至 2026-09-25：W1、W2 已完成；W3 中 A 的增量/刷新已合入 `main`，B 的校验扩展与一致性策略已在 `feat/w3-role-b-validation` 完成，C 的监控与评测骨架已完成；尚待最终评测和提交材料汇总。详见 [task_plan.md](task_plan.md)。

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

### 角色 A 验收对照（Assignment Task 1–2）— **完成**

| Assignment 要求 | 状态 | 证据 |
| --- | --- | --- |
| Task 1：为 Taxi / Weather / Air 生成仅含新/改记录的更新文件 | 完成 | `generate_update`；全量：`data/updates/`（Taxi ~7% 新 + ~1.5% 重复；Weather/Air 各 168 小时 + `humidity`/`aqi`） |
| Task 1：增量管道插入新行、忽略重复、保留未变行、支持约定 Schema 演进、不整库重建 | 完成 | `apply_updates` MERGE；Taxi insert-only；Weather/Air insert+update + 加列；标准化 taxi **10,223,396**、weather **8,952**、air **52,053** |
| Task 1：更新后整合表可用 | 完成 | 新 Taxi append 至 integrated（**10,223,396**，version 1）；`sync-integrated` 用于补 partial apply 缺口 |
| Task 2：只刷新受影响产品；支持演进；查询尽量兼容；减少无谓重算 | 完成 | `refresh_data_products(mode=auto\|full)`；小时产品脏键 MERGE；zone/weather 产品选择性整表重建；演进列不进 integrated |
| Task 2 Discuss | 完成 | 写入 [docs/w3_role_a_incremental.md](docs/w3_role_a_incremental.md) |

CLI：`scripts.run_incremental generate \| apply`；产品：`scripts.run_data_products --mode auto\|full`（`sync-integrated` 已于 2026-09-26 由 C 删除，见下）。

### 2026-09-24 A：增量与产品刷新（分支 `feat/w3-role-a`）

- **代码**：`generate_update` / `apply_updates` / `sync_integrated_from_standardized`；`refresh_data_products(mode=)`（auto：`daily_mobility`/`air_quality_impact` 增量 MERGE；`taxi_zone`/`weather_impact` 整表重建）；`check_schema` 白名单；provenance `lineage`；`coverage_window.json`；监控 `stage=incremental_update`。
- **说明**：[docs/w3_role_a_incremental.md](docs/w3_role_a_incremental.md)。
- **单测**：`tests.test_incremental` 等；小时产品增量 vs full 对齐测试已加。
- **本机全量冒烟（2026-09-24）**：
  - generate（seed 0）+ apply：Weather/Air 成功；Taxi 首次 partial 后经 `sync-integrated` 补 **668,820** 行进 integrated。
  - `run_data_products --mode auto`：四产品 success；daily/air **2183→2207**（+24）；zone **773→943**；weather **1220**（与再跑 `mode=full` / live builder 一致；相对旧 export 3017 行是因为类别从 coco 码改为标签，非刷新错误）。
- **A 侧收尾（非功能缺口）**：分支待合入 `main` / 开 PR；设计报告增量章节由 C 汇总时并入；评测三项（`incremental_update` / `analytical_refresh` / `storage_overhead`）等合入后由 C 在同一版本跑。

### 2026-09-24 C：监控与评测骨架（分支 `c/w3-monitoring`，提交 `bfe33a2`，未推送）

- 监控表 `metadata/pipeline_runs`（`monitoring.py`）取代 `ingestion_runs` 与 `product_refresh_runs`：一行对应一次执行中的一个阶段 × 目标，摄入、整合、产品刷新三处都已接入。`duplicate_count` 改为“目标表中已存在而跳过”，批内重复算作拒绝行，按码计数保留在 `validation_failure_counts_json`。
- 失败语义：阶段失败时永远抛阶段自己的异常，监控写入失败只附加 note；阶段成功但监控行丢失时抛 `MonitoringWriteError`。所有 CLI 新增 `--no-monitoring`，整合与产品刷新新增 `--run-id`。
- 五条运维 SQL 在 `sql/monitoring/`，入口 `scripts.run_monitoring_report`；`--import-legacy` 已把本机 W1/W2 的 4 行摄入、4 行产品刷新记录导入。现有数据上：Taxi 拒绝 202 行（`dropoff_before_pickup` 180、`timestamp_outside_source_period` 21、`duplicate_record` 1），最慢为 Taxi 摄入 156.8 秒。
- 评测骨架 `w3_evaluation.py` + `scripts.run_w3_evaluation`：每次运行（含预热）都在独立目录中使用基线的新副本，变体交替执行，输出计数不一致时不报时间。已可运行三项监控开销；增量、刷新、存储、校验四项标为 pending，并写明缺少的 A/B 入口（**A 入口现已具备，合入后可解 pending**）。
- 子 agent 审查了方案，采纳的修改包括：计数守恒式、补充 `mode` / `validation_enabled` / `target_rows_before/after` / `input_paths_json` 等列、改用普通函数而非上下文管理器、放弃 Delta RESTORE 改为整目录复制（RESTORE 不删文件，存储数字会被污染）。
- 接口约定见 [docs/w3_interfaces.md](docs/w3_interfaces.md)，标 **agree** 的条目待 A/B 确认：`incremental.generate_update` / `apply_updates`、`refresh_data_products(mode=)`、校验开关、`check_schema`，以及有效时间窗口、溯源 lineage、跨批次去重、演进 CSV 读取顺序、manifest 重写这五项跨角色决定。
- 验证：完整回归 **70 项 OK，1009.6 秒**（新增 15 项）；`git diff --check` 通过。尚未在全量数据上运行评测。


### 2026-09-25 B：校验扩展、Schema 策略与一致性规则（分支 `feat/w3-role-b-validation`）

- **Schema 演进**：`check_schema` 现校验缺列、未知加列、重复列名、类型变化和必填 nullability；配置只自动接受 Weather `humidity: double?` 与 Air Quality `aqi: double?`，两者 schema/rule version 升到 `1.1.0`。CSV 先查 header、Parquet 查真实 StructType，不信任 manifest 自报变更。
- **新规则**：Taxi 上下车 Zone 必须存在于本次快照的 lookup；演进列必须完整且 `humidity ∈ [0,100]`、`aqi ∈ [0,500]`。坏行继续写既有 `rejected` 并按稳定错误码上报，不进入整合表/产品。
- **扩展能力**：`register_rule_builder(dataset, builder)` / `unregister_rule_builder` 支持数据集专用规则和 `*` 通用规则，不需修改校验调度核心。
- **A/C 接线**：修复 A 已声明但未生效的 `apply_updates(validate=...)`，全量摄入和增量更新共用 `prepare` 开关；`--no-validation` 仅供隔离评测；监控行写入真实 `validation_enabled`。
- **分析一致性**：`humidity`/`aqi` 留在标准化源表，当前不改变 integrated/Q1–Q6 输出 Schema；刷新边界和必须全量重算的条件记录于 [docs/w3_role_b_validation.md](docs/w3_role_b_validation.md)。
- **测试证据**：新增 Schema/校验测试 8/8；针对性回归 preparation 11/11、ingestion 7/7、incremental 5/5、W3 evaluation 4/4；最终完整回归 **84/84 通过（470.105 秒）**。静态编译、配置 JSON 校验与 `git diff --check` 均通过。

### 2026-09-26 C：修复 A 的增量实现并接通评测（分支 `c/w3-fixes`）

三个 PR（#8 C、#9 A、#10 B）合入 `main` 后审阅，B 的部分没有问题；A 的四处问题由 C 直接修复：

| 问题 | 位置（修复前） | 修复 |
| --- | --- | --- |
| Taxi 新行程只往后挪约 1 天，大部分仍在 1–3 月，不满足作业“timestamps occurring after the latest trip” | `incremental.py` 平移量 = 原最大时间 + 1 天 − 样本最大时间 | 按整周平移，周数 = 样本最早时间到原最大时间的整周数 + 1；保留星期和时刻。按全量数据的时间范围推算为平移 13 周，新行程落在 4–6 月（待全量评测确认） |
| auto 刷新按“快照 run_id”找脏小时：走过 `sync-integrated`，或两次 apply 之间没刷新，已有小时的计数不会更新；A 的对齐测试只覆盖新增小时 | `data_products._dirty_hour_keys`、`last_update_affects.json` | 产品是否刷新改为比较产品的 `source_delta_version` 与当前整合版本；脏小时 = 整合表中不属于该版本已有 run_id 的行程所在小时。删除 `last_update_affects.json` 与 `PRODUCTS_BY_DATASET` |
| apply 在 MERGE 后失败，重跑插入 0 行，整合表永久缺这批行程（只能手动 `sync-integrated`）；另把 66.9 万个 `record_id` collect 到 driver 再 `isin` | `apply_updates` | 整合步骤改为追加“run_id 不在整合表里的标准化 Taxi 行”，重跑自动补齐，lineage 同时补上；删除 `sync_integrated_from_standardized` 与 CLI 子命令 |
| 覆盖窗口写在 `metadata/coverage_window.json`，`load_calendar_coverage()` 总是读仓库 `data/delta` 下的文件，与所查快照无关；第二次 apply 还会把窗口重置回配置值 | `queries.py`、`apply_updates` | 窗口随 `completed_batch.json` 进入 `completed_integration.json` 的 `coverage_window`；`load_calendar_coverage(snapshot=...)` 从快照读，查询 CLI 传入；apply 从上一批的窗口继续延伸 |

另：Weather 更新的 `humidity` 原为 20–100 的随机数，与同一行的 `rhum`（本就是相对湿度）无关，现两列取同一值。

评测接线（`w3_evaluation.py`）：六项计时（三项监控开销、`validation_overhead`、`incremental_update`、`analytical_refresh`）加一份不计时的存储开销报告。更新文件由评测脚本从基线生成（seed 0），并先在一份基线副本上应用一次（不计时），刷新对比从这份副本开始，其存储报告与基线对比即存储开销。`analytical_refresh` 的一致性检查从行数改为每个产品的行数 + 内容哈希（排除元数据列，double 取 6 位小数），只比行数测不出上面第二个问题。校验开关前后通过行数本就不同，对照改比处理行数。

验证：受影响的 5 个套件 35 项 OK（2,230 秒），其余 8 个套件 51 项 OK（699 秒），全部 86 项通过；`git diff --check` 通过。新增用例覆盖已有小时收到新行程、MERGE 后崩溃再重跑、评测在更新后跑通 full/auto 刷新且内容一致。
