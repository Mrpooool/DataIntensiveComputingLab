# 项目规划历史归档

按日期保留两次压缩前的完整记录；内容中的进度和结论以对应日期为准。当前状态见仓库根目录规划文件。

<a id="archive-2026-09-28"></a>

## 2026-09-28 快照

> 2026-09-28 压缩前的历史快照；含已被后续修复取代的中间状态。当前状态以根目录规划文件和 W3 最终报告为准。仅调整相对链接以便归档后访问。

# 项目进度

截至 2026-09-27：W1、W2、W3 均已完成。W3 的 A、B、C 代码都已进 `main`（PR #8–#10），C 在 `c/w3-fixes` 修复了增量实现的问题、完成全量评测，并整理了设计报告、评测报告、中英 README 与提交包。详见 [task_plan.md](../../task_plan.md)。

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
- 首次 benchmark：`20260909T141616Z-ce81d328`，对应 [报告](../../docs/benchmark_report.md) 与 [原始计时](../../docs/benchmark_timings.csv)。
- 最终 benchmark：`data/benchmark/20260909T144949Z-b493da9f/results.json`，状态成功；四次摄入均保留 9,554,576 条唯一行程，24 次查询计时的结果一致。
- 提交包：[Week1_submission_2026-09-13.zip](../../submissions/Week1_submission_2026-09-13.zip)。已打包并推送不代表已在课程系统提交。
- 以上测试与全量运行是 W1 历史验证结果；本次只整理文档，没有重新运行 Spark 测试或全量数据。
- 天气来源时区仍有假设，环境关联只反映纽约范围的小时背景值；后续分析继续遵守 [数据契约](../../docs/data_contract.md)。

## W2 已完成

目标：六个 Spark SQL 查询、四张分析产品、四类优化对照实验。分工延续为 A 产品落盘 / B 查询口径 / C 优化与 benchmark。

| 角色 | 交付 | 状态 |
| --- | --- | --- |
| A | 四张产品（`data_products.py` + CLI + 元数据）、全量刷新入口 | 已合 `main`（PR #4）；审查修复后口径与 Q1–Q6 对齐，`schema_version` 环境产品升至 1.1.0 |
| B | Q1–Q6 Spark SQL、配置与小样本测试、[查询设计说明](../../docs/role_b_query_design.md) | 已合 `main`（PR #5） |
| C | 五项审查问题修复、13 项优化实验、[benchmark report](../../docs/w2_benchmark_report.md)、[优化策略](../../docs/w2_design_optimization.md) | 已完成于 `c/pipeline-fixes`（`1cab797`） |

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
| Task 2 Discuss | 完成 | 写入 [docs/w3_role_a_incremental.md](../../docs/w3_role_a_incremental.md) |

CLI：`scripts.run_incremental generate \| apply`；产品：`scripts.run_data_products --mode auto\|full`（`sync-integrated` 已于 2026-09-26 由 C 删除，见下）。

### 2026-09-24 A：增量与产品刷新（分支 `feat/w3-role-a`）

- **代码**：`generate_update` / `apply_updates` / `sync_integrated_from_standardized`；`refresh_data_products(mode=)`（auto：`daily_mobility`/`air_quality_impact` 增量 MERGE；`taxi_zone`/`weather_impact` 整表重建）；`check_schema` 白名单；provenance `lineage`；`coverage_window.json`；监控 `stage=incremental_update`。
- **说明**：[docs/w3_role_a_incremental.md](../../docs/w3_role_a_incremental.md)。
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
- 接口约定见 [docs/w3_interfaces.md](../../docs/w3_interfaces.md)，标 **agree** 的条目待 A/B 确认：`incremental.generate_update` / `apply_updates`、`refresh_data_products(mode=)`、校验开关、`check_schema`，以及有效时间窗口、溯源 lineage、跨批次去重、演进 CSV 读取顺序、manifest 重写这五项跨角色决定。
- 验证：完整回归 **70 项 OK，1009.6 秒**（新增 15 项）；`git diff --check` 通过。尚未在全量数据上运行评测。


### 2026-09-25 B：校验扩展、Schema 策略与一致性规则（分支 `feat/w3-role-b-validation`）

- **Schema 演进**：`check_schema` 现校验缺列、未知加列、重复列名、类型变化和必填 nullability；配置只自动接受 Weather `humidity: double?` 与 Air Quality `aqi: double?`，两者 schema/rule version 升到 `1.1.0`。CSV 先查 header、Parquet 查真实 StructType，不信任 manifest 自报变更。
- **新规则**：Taxi 上下车 Zone 必须存在于本次快照的 lookup；演进列必须完整且 `humidity ∈ [0,100]`、`aqi ∈ [0,500]`。坏行继续写既有 `rejected` 并按稳定错误码上报，不进入整合表/产品。
- **扩展能力**：`register_rule_builder(dataset, builder)` / `unregister_rule_builder` 支持数据集专用规则和 `*` 通用规则，不需修改校验调度核心。
- **A/C 接线**：修复 A 已声明但未生效的 `apply_updates(validate=...)`，全量摄入和增量更新共用 `prepare` 开关；`--no-validation` 仅供隔离评测；监控行写入真实 `validation_enabled`。
- **分析一致性**：`humidity`/`aqi` 留在标准化源表，当前不改变 integrated/Q1–Q6 输出 Schema；刷新边界和必须全量重算的条件记录于 [docs/w3_role_b_validation.md](../../docs/w3_role_b_validation.md)。
- **测试证据**：新增 Schema/校验测试 8/8；针对性回归 preparation 11/11、ingestion 7/7、incremental 5/5、W3 evaluation 4/4；最终完整回归 **84/84 通过（470.105 秒）**。静态编译、配置 JSON 校验与 `git diff --check` 均通过。

### 2026-09-26 C：修复 A 的增量实现并接通评测（分支 `c/w3-fixes`）

三个 PR（#8 C、#9 A、#10 B）合入 `main` 后审阅，B 的部分没有问题；A 的四处问题由 C 直接修复：

| 问题 | 位置（修复前） | 修复 |
| --- | --- | --- |
| Taxi 新行程只往后挪约 1 天，大部分仍在 1–3 月，不满足作业“timestamps occurring after the latest trip” | `incremental.py` 平移量 = 原最大时间 + 1 天 − 样本最大时间 | 按整周平移，周数 = 样本最早时间到原最大时间的整周数 + 1；保留星期和时刻。全量数据上实际平移 13 周：原行程止于 2024-04-01 03:59:59 UTC，668,820 条新行程落在 4–6 月，窗口终点变为 2024-07-01 04:00 |
| auto 刷新按“快照 run_id”找脏小时：走过 `sync-integrated`，或两次 apply 之间没刷新，已有小时的计数不会更新；A 的对齐测试只覆盖新增小时 | `data_products._dirty_hour_keys`、`last_update_affects.json` | 产品是否刷新改为比较产品的 `source_delta_version` 与当前整合版本；脏小时 = 整合表中不属于该版本已有 run_id 的行程所在小时。删除 `last_update_affects.json` 与 `PRODUCTS_BY_DATASET` |
| apply 在 MERGE 后失败，重跑插入 0 行，整合表永久缺这批行程（只能手动 `sync-integrated`）；另把 66.9 万个 `record_id` collect 到 driver 再 `isin` | `apply_updates` | 整合步骤改为追加“run_id 不在整合表里的标准化 Taxi 行”，重跑自动补齐，lineage 同时补上；删除 `sync_integrated_from_standardized` 与 CLI 子命令 |
| 覆盖窗口写在 `metadata/coverage_window.json`，`load_calendar_coverage()` 总是读仓库 `data/delta` 下的文件，与所查快照无关；第二次 apply 还会把窗口重置回配置值 | `queries.py`、`apply_updates` | 窗口随 `completed_batch.json` 进入 `completed_integration.json` 的 `coverage_window`；`load_calendar_coverage(snapshot=...)` 从快照读，查询 CLI 传入；apply 从上一批的窗口继续延伸 |

另：Weather 更新的 `humidity` 原为 20–100 的随机数，与同一行的 `rhum`（本就是相对湿度）无关，现两列取同一值。

评测接线（`w3_evaluation.py`）：六项计时（三项监控开销、`validation_overhead`、`incremental_update`、`analytical_refresh`）加一份不计时的存储开销报告。更新文件由评测脚本从基线生成（seed 0），并先在一份基线副本上应用一次（不计时），刷新对比从这份副本开始，其存储报告与基线对比即存储开销。`analytical_refresh` 的一致性检查从行数改为每个产品的行数 + 内容哈希（排除元数据列，double 取 6 位小数），只比行数测不出上面第二个问题。校验开关前后通过行数本就不同，对照改比处理行数。

验证：受影响的 5 个套件 35 项 OK（2,230 秒），其余 8 个套件 51 项 OK（699 秒），全部 86 项通过；`git diff --check` 通过。新增用例覆盖已有小时收到新行程、MERGE 后崩溃再重跑、评测在更新后跑通 full/auto 刷新且内容一致。

全量试跑又发现两处，均已修复并补测试：

- 生成器用 `collect()` 读最大时间戳，得到的是宿主本地时区的 naive 时间，本机上 Taxi 窗口终点晚 8 小时、Weather/Air 新小时与原数据之间空 8 小时（`0c8d49d`，改为按 `unix_micros` 读）。
- auto 刷新比 full 慢 54%（130.7 秒对 84.9 秒，内容一致）：产品 DataFrame 惰性求值，键检查、计数、MERGE 各扫一遍 1,022 万行整合表。改为产品结果和脏小时各物化一次（`adf0508`），相关 5 个套件 27 项 OK（1,480 秒）。

评测基线：用当前代码在 `data/benchmark/w3/baseline` 重建（`data/delta` 未动），Taxi 通过 9,554,576、拒绝 202，与 W1 一致。

### 2026-09-27 C：全量评测与交付材料

评测在提交 `adf0508` 上完成，两次运行 `20260926T174427Z-1ce27d00`（更新与刷新）和 `20260926T180757Z-afffd333`（校验与监控开销），每项预热一次、交替测三次取中位数，全部输出一致：

| 测量 | 结果 |
| --- | --- |
| 增量更新 | 134.4 秒（Taxi MERGE 33.4、Weather 16.3、Air 13.8、整合追加 43.7）；从头摄入 + 整合原数据 312.7 秒。重复应用插入 0 行，随后 `auto` 刷新跳过全部产品 |
| 分析刷新 | full 66.3 秒，auto 87.2 秒，内容哈希一致；小时产品 MERGE 比重建慢（找脏小时要扫整合表） |
| 存储 | 快照 2,105.5 → 2,271.8 MB（+7.9%）；快照文件 116 → 517，Weather/Air 各从 1 个变 90 个小文件 |
| 校验开销 | 176.1 → 223.1 秒（+26.7%），Taxi 占 45.7 秒 |
| 监控开销 | 摄入 +12.6%、整合 +13.8%、刷新 +59.8%；单行写入探针 5–6.5 秒（首次 15 秒），与数据量无关 |

另在基线副本上按 README 跑了一遍 W3 命令（generate → apply → auto → 重复 apply → auto → Q3 → 运维报告）：Q3 日历延伸到 7 月 1 日（4,367 小时），运维报告写入 `data/benchmark/w3/monitoring_report.json`。

交付材料：[设计报告](../../docs/w3_design_report.md)、[评测报告](../../docs/w3_evaluation_report.md)、[原始样本](../../docs/w3_evaluation_timings.csv)，中英 README 增加 W3 运行说明与结果，提交包 `submissions/Week3_submission_2026-09-27.zip`（含两份报告的 PDF）。文档已按 humanizer 规则改写。

<a id="archive-2026-10-07"></a>

## 2026-10-07 快照

> 2026-10-07 压缩前历史快照；包含已被后续完成记录取代的中间状态。当前状态以根目录规划文件和 W4 最终报告为准。仅调整相对链接以便归档后访问。

# 项目进度

截至 2026-10-03：W1–W3 已完成并合入 main；W4 四个 Task 的代码、全量实验、两份报告和提交包已在 `c/w4-fixes` 完成，待推送和 PR。下一步见 [task_plan.md](../../task_plan.md)。

## W1–W2 历史摘要

- W1：四源全量摄入与整合完成。Taxi 输入 9,554,778、通过 9,554,576、拒绝 202；Weather 8,784，NYC Air 51,885，Zone 265；整合保留全部通过行程。27 项历史测试通过，两种存储布局结果一致。
- W2：Q1–Q6、四张产品、13 项优化实验完成，55 项历史测试通过。最终实验 `20260919T143454Z-9d921a51`；[评测报告](../../docs/w2_benchmark_report.md)。
- 早期调查、审核和运行细节见[原进度归档](../../docs/planning_archive/progress.md#archive-2026-09-28)。

## W3 完成摘要

| 角色 | 最终交付 |
| --- | --- |
| A | 三类增量文件、MERGE、Schema 加列、整合追加、产品 auto/full 刷新 |
| B | Schema 白名单、引用/数值/完整性校验、拒绝隔离、规则注册、统一校验开关 |
| C | pipeline_runs、运维 SQL、增量修复、全量评测、README 和交付汇总 |

最终行为：apply 重跑自动补齐 MERGE 后未整合数据；已删除 sync-integrated；覆盖窗口由完成快照发布；产品按来源 Delta 版本刷新。Weather/Air 演进列暂不进整合表，环境修正不回填既有 Taxi。

历史验证：2026-09-26 全部 86 项通过；UTC 时间修复与产品物化后，相关 5 套件 27 项通过。本次未重跑这些测试。

全量评测提交 `adf0508`，运行 `20260926T174427Z-1ce27d00` 和 `20260926T180757Z-afffd333`；预热一次，每项三次取中位数，全部输出一致：

| 测量 | 结果 |
| --- | --- |
| 增量更新 | 134.4 秒；重复应用插入 0 行，随后 auto 跳过所有产品 |
| 分析刷新 | full 66.3 秒、auto 87.2 秒；内容哈希一致 |
| 存储 | 快照 2,105.5 → 2,271.8 MB（+7.9%），文件 116 → 517 |
| 校验 | 176.1 → 223.1 秒（+26.7%） |
| 监控 | 摄入 +12.6%、整合 +13.8%、刷新 +59.8% |

原始基线 `data/benchmark/w3/baseline`。曾在其副本按 README 跑 generate → apply → auto → 重复 apply → auto → Q3 → 运维报告，Q3 覆盖至 7 月 1 日；这不是 W4 预测效果验收。

交付：[设计报告](../../docs/w3_design_report.md)、[评测报告](../../docs/w3_evaluation_report.md)、[计时样本](../../docs/w3_evaluation_timings.csv)、[提交包](../../submissions/Week3_submission_2026-09-27.zip)。W3 代码已通过 PR #11、#12 合入 main；课程系统提交状态未核验。

## 2026-09-28：W4 需求与计划更新

- 使用 planning-with-files；session-catchup 未返回未同步上下文，开工前工作区干净。
- 对照 W4 四个 Task，读取数据契约、代码接口和 W3 最终报告，没有启动 ML 实现。
- 三份旧文件归档至 `docs/planning_archive/`，加历史快照说明并调整相对链接；当前文件压缩历史，移除当前视图中的旧 pending/旧入口描述。
- 更新六阶段计划：需求整理 complete；契约、特征、模型、两路对照、验证交付 pending。
- 建议 Zone 下一小时需求预测及 A 数据集/B 特征模型/C 对照交付，均未当作已确认决定。补充时间切分、预测时特征可用性、train-only 拟合及模拟数据隔离。
- 编辑中 apply_patch 拒绝同路径 Delete/Add；随后发现 PowerShell 管道中文编码问题，改用 ASCII 转义 JSON 传输和 UTF-8 写入，重新检查全文与归档。
- 本次只整理规划文档；核验文档差异、相对链接和阶段状态，不重跑 Spark 或全量评测。

## 2026-10-01：W4 同学 B 实现

- 从最新 `origin/main` 的合并提交建立 `feat/w4-role-b-features-model`；开工时 W4 只有计划，没有 A/C 实现可接。
- 固定 Zone-hour 需求预测的 B 输入接口：键、标签、三段时间 split、位置、需求 lag 和一小时滞后的环境字段；写入 `configs/ml.json` 与数据契约。
- 新增 `ml_pipeline.py`：输入校验、纽约本地周期特征、缺失标记、train-only 中位数填补/类别编码/缩放、未知类别处理及 `features` 组装。
- 完成前一天同小时基线和 Linear Regression 闭环：validation RMSE 选候选，test 最终评估 MAE/RMSE/R²，保存并 reload 后核对固定样本预测。
- 新增特征物化和训练/重训练两个 CLI；每次训练保存完整 PipelineModel、配置快照、环境、输入路径、split 行数、候选指标与耗时。
- 新增 4 个针对性 Spark 测试，覆盖接口字段、重复键拒绝、train-only 填补、缺失标记、未知类别、特征向量、指标和模型 reload。
- Python 语法编译通过，配置可导入；当前 macOS 没有 Java Runtime 且 `.venv` 的 3.11.9 解释器链接失效，Spark 测试无法在本机启动，未将其误报为通过。

## 下次接续

在 GitHub 网页上为 `c/w4-fixes` 开 PR，请队友审阅 [设计报告](../../docs/w4_design_report.md) 和 [评测报告](../../docs/w4_evaluation_report.md)，合并后在课程系统提交 `submissions/Week4_submission_2026-10-03.zip`。

## 2026-10-01：W4 同学 A 训练集

- 从固定原始 1–3 月完成批次的五张固定版本 Delta 表生成 Zone-hour 训练集；原始文件标识、版本和覆盖窗口记录在 `data/delta/ml/training_dataset_metadata.json`。入口拒绝缺月或 W3 模拟更新快照。
- 覆盖窗口为 `[2024-01-01 05:00:00, 2024-04-01 04:00:00)` UTC，262 个 NYC Zone × 2,183 个完整小时，共 571,946 行，其中 335,531 行零订单。训练/验证/测试分别为 401,122 / 85,936 / 84,888 行。
- 输入整合行程 9,554,576 条，其中 37,569 条非 NYC 范围；标签总数 9,517,007 与纳入的 NYC 行程数一致。Delta 输出版本 1，已回读核验；未运行 B/C 的真实数据训练或路线对照。
- A 的完整针对性 Spark 测试 5/5 通过；B 接口测试 4/4 通过。`git diff --check` 通过。Windows Spark 退出时报告临时 JAR 清理失败，生成与测试命令退出码均为 0。

## 2026-10-02：W4 同学 C 审查与修复

- 审查 PR #13、#14（分支 `c/w4-fixes`）。W4 两套测试 9/9 通过，B 的测试首次在 Windows + JDK 21 下运行；本地 `.venv` 按 `requirements.txt` 补装 `numpy==2.3.5`。
- 修复：原始批次的 manifest 没有 `coverage_window`（只有 incremental apply 写入），`run_ml_dataset` 在按 README 新跑的快照上必然失败；改用 `load_calendar_coverage`，与 Q3–Q5 的覆盖窗口口径一致。测试改用不带窗口的真实 manifest 形态。
- 修复：特征和训练 CLI 改为读取 A 元数据（`--training-metadata`），按其 `output_version` 加载训练集，不读最新版本；`metrics.json` 记录训练集路径、版本和来源 run ID。删除未使用的 parquet 输入。新增回归断言：重写训练表后仍读到元数据登记的版本。
- 本地真实数据按 README 跑通三条命令：训练集 571,946 行、split 401,122 / 85,936 / 84,888、零订单 335,531，与 A 的记录一致；特征向量 299 维。Linear Regression 选中 reg=0.0，test RMSE 13.35、MAE 4.88、R² 0.941；`demand_lag_24h` 基线 20.48 / 5.52 / 0.862；重载核对 20 行一致。耗时：数据集 71 秒、特征 62 秒、训练 78 秒（含 Spark 启动）。运行 `w4-check-20261002`，输出在已忽略的 `data/delta/ml/` 与 `artifacts/w4/`。

## 2026-10-03：W4 同学 C 的 Task 4、重训演示与报告

- 新增 `ml_raw_route.py`（Approach A）：从六个原始文件自行把 Taxi 纽约时间转 UTC、套用平台的 Taxi 拒绝规则和去重、筛选纽约 Air 站点并做两级中位数，再调用共用 builder。builder 改为接收逐小时 PM2.5，平台路线在调用前聚合。
- B 的 `train_and_evaluate` 改为在 train 上只拟合一次特征流水线，每个候选只拟合回归；保存的仍是完整 PipelineModel，真实数据指标与改前完全相同（test RMSE 13.3459）。
- 新增 `w4_evaluation.py` 与 `scripts.run_w4_evaluation`：两路预热后逐行对比，之后每次运行须复现同一哈希和 test 指标；特征组按列名前缀累加。夹具测试用平台真实的摄入和整合对照 raw 路线，覆盖每种 Taxi 拒绝情况、重复行、非纽约 Air 站点和夏令时。
- 全量运行 `20261002T155150Z`：两路 571,946 行 0 差异。中位数：准备 8.6 秒（平台）对 43.0 秒（原始文件）；特征拟合 6.2 / 6.0 秒，模型拟合均 1.3 秒，训练合计 12.7 / 12.2 秒。准备代码 8 行对 97 行；平台一次性摄入+整合 312.7 秒（W3 实测）。
- 特征组 test RMSE：仅 Taxi 13.349、+Weather 13.347、+Air 13.346；validation 上仅 Taxi 最好，环境特征对线性模型无提升。
- 36 条被平台拒绝的行程（35 条下车早于上车、1 条重复）位于纽约 Zone 且在窗口内；raw 路线若不复刻这些规则，标签会不同。
- 重训演示：W3 更新快照 + `configs/ml_w3_update.json`，同样两条命令生成 1,144,154 行并训练 `retrain-w3-update`。Taxi 更新是 Spark 目录，源文件校验改为按目录名识别 part 文件。合成数据只演示机制。
- 主训练 `main-20261003`：test RMSE 13.35、MAE 4.88、R² 0.941，基线 20.48 / 5.52 / 0.862，重载核对 20 行一致。
- 交付：[设计报告](../../docs/w4_design_report.md)、[评测报告](../../docs/w4_evaluation_report.md)、[计时样本](../../docs/w4_evaluation_timings.csv)，README 中英文与 CLAUDE.md 命令已更新。两份报告按 humanizer 规则改写，PDF 各 5 页。
- 测试：W4 三个测试文件共 12 项通过（`test_ml_dataset` 最后一次改动后 5 项再次通过）。全量测试跑了约一小时后被后台时限终止，未得到结果；W1–W3 平台模块本次未改动，按 AGENTS.md 无需全量重跑，用户决定不再重跑。
- 提交包：`submissions/Week4_submission_2026-10-03.zip`，含源码、配置、测试、文档及两份报告 PDF。

<a id="archive-2026-10-07-w4-closeout"></a>

## 2026-10-07 W4 完成摘要（WikiPulse 启动前）

# 项目进度

更新：2026-10-07。**W1–W4 已完成，本轮仅做 W4 文档收尾与压缩。**本地分支 `c/w4-fixes`，检查基点 `7abb884`；开工时工作区干净。

## W1–W3 历史摘要

| 周次 | 完成与历史验证 |
| --- | --- |
| W1 | 四源摄入与整合，9,554,576 条通过行程、202 条拒绝；27 项测试通过，两种布局结果一致 |
| W2 | 六个查询、四张产品、13 项优化实验；55 项测试通过 |
| W3 | 增量、Schema 演进、选择性刷新、校验和监控；86 项测试通过，后续物化修复相关 27 项通过；完整评测和交付包已形成 |

详细历史保留在 [2026-09-28 归档](../../docs/planning_archive/progress.md#archive-2026-09-28) 与 [本轮归档](../../docs/planning_archive/progress.md#archive-2026-10-07)，不再逐次展开过程。

## W4 最终结果

A 完成训练集，B 完成特征/模型/再训练入口，C 完成联调修复、raw 路线、全量评测和材料汇总。

| 项目 | 完成证据 |
| --- | --- |
| 训练集 | 571,946 行，335,531 行零需求；train/validation/test 为 401,122 / 85,936 / 84,888；标签合计 9,517,007 |
| 特征与模型 | 299 维；Linear Regression 选中 reg=0.0；test RMSE 13.346、MAE 4.883、R² 0.941 |
| 简单基线 | 前一天同小时需求；test RMSE 20.483、MAE 5.519、R² 0.862 |
| 模型重载 | 主训练 `main-20261003`，20 个固定测试样本预测核对一致 |
| raw/platform | 全量运行 `20261002T155150Z`，双向逐行差异均为 0；准备 43.0/8.6 秒，端到端 55.3/21.3 秒 |
| 再训练 | `retrain-w3-update` 使用新配置和同一入口生成 1,144,154 行并训练；仅为模拟数据机制演示 |

这些是 2026-10-02/03 的历史实测。本轮只读核对 `results.json`、主训练 `metrics.json` 的 success 状态、指标和重载结果，没有重新执行实验。评测报告关联代码 `ae244c1`；路线实验早于该提交最后一次 Spark 目录来源识别修改，该分支不影响路线实验。

## 验证边界与交付

- 历史测试：W4 三个测试文件共 12 项通过，最后修改后 `test_ml_dataset` 的 5 项再次通过。
- 全量测试曾运行约一小时后被后台时限终止，没有完整结果；此前用户决定不再重跑，不能称为全量回归通过。本轮未修改业务代码或运行 Spark。
- 交付：[设计报告](../../docs/w4_design_report.md)、[评测报告](../../docs/w4_evaluation_report.md)、[计时样本](../../docs/w4_evaluation_timings.csv)、[英文 README](../../README.md)、[中文 README](../../README-zh.md)、[提交包](../../submissions/Week4_submission_2026-10-03.zip)。
- 本轮检查提交包目录：104 项，含设计与评测两份 PDF。远端推送、PR 合并和课程系统提交状态未核验。

## 2026-10-07 文档收尾

- 使用 planning-with-files；session-catchup 未返回未同步上下文。核对当前计划、最终报告、保存的原始结果及交付包。
- 按用户要求将两次归档合并到 `docs/planning_archive/`；三份归档文档内按日期保留完整快照，更新链接后移除两个旧目录。
- 主计划切为 W1–W4 完成态，清除过时建议和待交接描述；保留正式入口、结果、测试边界和已知限制，不创建后续作业。
- 核验归档正文与压缩前版本一致、中文 UTF-8、文档相对链接、阶段 complete 状态和 `git diff --check`。
- 沙箱启动异常通过自动审批后的命令完成；误写的中文 README 文件名已纠正。未提交或推送本轮文档修改。
