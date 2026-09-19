# 项目进度

截至 2026-09-19：W1 已交付代码包；W2 的 A 数据产品与 B 分析查询均已合并进 `main`。审查确认的五项 A/B 问题已修复，C 的四类优化实验与 benchmark report 已完成，全部在分支 `c/pipeline-fixes` 上尚未合并回 `main`。剩余：W2 完整设计报告与提交包。详见 [task_plan.md](task_plan.md)。

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



## W2 当前状态与下一步

2026-09-15：已在 `task_plan.md` 确定六个 SQL 查询、四张数据产品、四类优化实验和 A/B/C 交接接口。

### 2026-09-17 改动记录（A）

- `src/dic_pipeline/data_products.py`：产品注册、全量刷新、写回校验、审计表；缺快照时从 batch + 整合表补发 `completed_integration.json`。
- `configs/data_products.json`：四张产品名、schema version、业务键。
- `scripts/run_data_products.py`：产品刷新 CLI。
- `scripts/export_products_csv.py`：四张产品导出为 `data/exports/<产品名>.csv`，便于 Excel 查看。
- `tests/test_data_products.py`：产品小样本测试。



### 2026-09-17 四张产品全量刷新（A）

入口：`python -m scripts.run_data_products`（约 4 分钟，`status=success`）。输出在 `data/delta/analytics/<product>/`，审计在 `analytics/metadata/product_refresh_runs/`。


| 产品                           | 行数    | 耗时（秒） | 文件数 | 大小（字节） |
| ---------------------------- | ----- | ----- | --- | ------ |
| `daily_mobility_summary`     | 2,183 | 55.7  | 1   | 70,085 |
| `taxi_zone_statistics`       | 773   | 49.2  | 1   | 31,384 |
| `weather_impact_summary`     | 3,017 | 33.4  | 1   | 48,616 |
| `air_quality_impact_summary` | 2,183 | 25.9  | 1   | 41,951 |


完整测试：`unittest discover` 31 项通过。

### A → B 产品口径交接（2026-09-17）

配置：`configs/data_products.json`；默认聚合：`DEFAULT_PRODUCT_BUILDERS`。B 的 Q1–Q6 / `PRODUCT_BUILDERS` 应对齐或显式替换。


| 产品                           | 业务键                                        | 粒度与指标                                                                         | 空值 / 分类                                       |
| ---------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------- | --------------------------------------------- |
| `daily_mobility_summary`     | `pickup_hour_utc`                          | UTC 小时；当地日/时/星期；`trip_count`；距离/时长 sum+valid count                            | 缺测不填 0                                        |
| `taxi_zone_statistics`       | `local_pickup_month`, `pickup_location_id` | 当地月 × Zone；`trip_count`；距离/车费 sum+count                                       | Zone 名可空                                      |
| `weather_impact_summary`     | `pickup_location_id`, `weather_category`   | Zone × 天气类；`trip_count`；`valid_hour_count`；距离 sum+count                       | 未匹配→`unmatched`；无 coco→`missing_code`；不做晴雨雪映射 |
| `air_quality_impact_summary` | `pickup_hour_utc`                          | UTC 小时；`air_quality_pm25`；`trip_count`；matched/unmatched count；`match_status` | PM2.5 不填 0、不分箱                                |


公共：源=`integrated_taxi_trips`（钉在 `completed_integration.json`）；分析时区 `America/New_York`，小时键 UTC；全量覆盖刷新；行上元数据 `data_source` / version / created/refreshed / `schema_version`。B 可用 `--builders-module` 覆盖。W2 不做：零订单补齐、天气语义标签、PM2.5 分箱、增量刷新。

### 2026-09-18 改动记录（B）

- `src/dic_pipeline/queries.py` 与 `src/dic_pipeline/sql/`：完成 Q1–Q6 Spark SQL 查询库、输入 Schema 检查、日期参数和稳定输出契约。
- `configs/analytical_queries.json`：固定纽约分析时区、Meteostat 天气分类、PM2.5 描述性分箱及 Q4 最低样本小时数。
- `scripts/run_analytical_queries.py`：复用 A 的 Delta 快照注册入口，支持选择查询、日期范围、SQL 展示和 `EXPLAIN FORMATTED`。
- `tests/test_queries.py`：8 项小样本测试覆盖六个查询、零订单小时、空值分母、两级 PM2.5 中位数、天气下 Zone 变化排名、并列高峰与环比。
- `docs/role_b_query_design.md`：记录统计粒度、输出、缺测规则和 A/B/C 职责边界。

任务 B 的 8 项测试已在 Spark 4.2.0 / Delta 4.4.0 上通过；随后完整仓库回归为 39 项全部通过（140.705 秒）。当前电脑没有纳入 Git 的全量 `data/delta/`，因此尚未在这里重跑六个全量查询，不能用小样本结果替代全量结果。

## 2026-09-19 审查交接：W2-REVIEW-20260919

### 目标、版本与授权

- 工作目录：`D:\Tools\Coding\DataIntensiveComputingLab`。本次目标是检查队友 A/B 是否完成 W2；结论为主体代码已写好，但不能直接验收。
- 用户本次仅调用 `$project-handoff`，授权保存材料；没有授权自动新建对话、修复、合并 B 分支、提交或推送。下一轮按用户实际指令确定执行范围。
- 当前分支 `main`，提交 `da6ca59bed5d75e9a56b81fab1e48497a45348b1`；A 的实现提交 `aa3f9cb` 已经合并。
- B 位于 `origin/feat/role-b-analytical-queries`，提交 `173a541cc62efe1b4a253c863cf61986fa173c72`；已 fetch 确认，以当前 main 为基底，尚未合并。
- 审查前工作区干净，审查未改业务源码。本次保存只更新 `progress.md` 并新增 `docs/review_evidence/w2-2026-09-19/` 两份证据，未提交/推送。
- 状态：材料已保存，等待接手核验。没有创建接手任务，接手任务编号为空。两项测试进程均已结束，没有本次遗留的测试任务需要接管。

### 权威资料与状态冲突

- `Assignment.md` 的 Week 2 部分：课程任务与交付物要求。
- `task_plan.md`：已确认的 A/B/C 分工、六个查询、四张产品、四类优化及验收口径；其中“尚未开始实现”等状态落后于代码，以本次核查记录说明差异，不能把整个计划视为已完成。
- `docs/data_contract.md`：W1 字段、单位、时间和关联限制。
- B 分支的 `docs/role_b_query_design.md`、`configs/analytical_queries.json`：查询定义及分类口径；当前 main 没有这些文件，应通过 `git show` 或隔离副本读取。
- A 产品仍使用原始天气代码、只记录有订单的小时；B 的规范查询采用天气分组及零订单小时补齐。这一接口差异尚未解决，不能把产品查询当成已经等价的优化版本。

### 已确认问题与建议修复方向

| 问题 | 位置与证据 | 建议及验收 |
| --- | --- | --- |
| A：整合快照发布流程不完整 | `data_products.py:106–117` 在缺 manifest 时直接拼接当前 batch 与最新整合版本；`scripts/run_integration.py:20–32` 未发布/更新该 manifest。静态确认，未对真实数据执行重摄入复现。 | 成功整合后发布来源批次及整合版本；验证重跑后使用新快照，拒绝把新 batch 与旧整合表拼成成功快照。 |
| B：日期边界漏掉零订单小时 | Q3 SQL 第 8 行、Q4 第 13 行、Q5 第 7 行都从筛选后行程的 min/max 取边界。完整一天的 Q3 样例只计 4 小时；两周一的 Q5 应在 00/01 点并列，却只返回 00 点。 | 结合已知数据覆盖范围和显式查询区间构建 UTC 小时日历，补测首尾零订单、空区间和 DST。不要把数据未覆盖时段当成零需求。 |
| A/B：产品与查询口径不同 | `build_weather_impact_summary` 的分母只含该 Zone 有订单的小时且保留原始代码；`build_air_quality_impact_summary` 计入域外订单，而 Q3 只取 NYC。一个 NYC + 一个域外订单的样例，产品计 2，Q3 应计 1。B 未提供 PRODUCT_BUILDERS 替换。 | 统一统计范围、天气分类和小时分母；用产品重算与基础 SQL 做结果一致性验证，再做性能比较。 |
| A：UTC 元数据偏移 | `data_products.py:189–192,212` 去掉 Python datetime 时区后交给 Spark。本机复现 UTC 12:00 被写成 UTC 04:00。 | 保留 aware datetime 或使用 Spark 时间表达式；按 epoch 核对创建、刷新及审计时间，并验证重复刷新保留创建时间。 |
| A：Windows 测试依赖缺失 | `tests/test_data_products.py:126` 的 ZoneInfo 查询报缺少 `tzdata`，`requirements.txt` 未声明该依赖。 | 修正依赖/跨平台测试方案，使用项目 Python 环境重跑失败测试；不能把此次结果写成全部通过。 |

以上修复是 Agent 建议，尚未执行。建议先解决正确性与接口问题，再启动 C 的缓存、分区裁剪、广播连接和 AQE 对照。

### 验证与证据

- 使用项目 `.venv\Scripts\python.exe`（Python 3.11.9），在 B 分支的临时副本运行 `python -m unittest tests.test_queries tests.test_data_products -v`。
- 实际结果：12 项耗时 302.916 秒，11 项通过，1 项报错。B 的 8 项全部通过；A 的四产品测试因缺少 `tzdata` 中断，后续断言没有执行。
- A 文档中的 31 项全测/四产品全量成功及 B 文档中的 39 项全测成功是队友记录，不是此次复测结果。本次没有重跑六个全量查询、全套 39 项测试或 C benchmark。
- 已保留可随项目移动的[小样本结果](docs/review_evidence/w2-2026-09-19/review-probes.json)和[复现脚本](docs/review_evidence/w2-2026-09-19/review_probes.py)。脚本依赖 B 分支的查询及测试 fixture，需在对应代码副本根目录、`PYTHONPATH=src` 下运行；输出 `review-probes.json` 到运行目录。没有固化完整运行日志。
- 仅本机临时材料：`C:\Users\h1349\AppData\Local\Temp\dic-w2-review-173a541\`，含 B 代码副本、`review-tests.log`、`review-probes.log`、`review-notes.md`；临时目录可能被清理，不能作为唯一交接来源。
- Spark 结束阶段有 Windows 临时目录清理消息；测试最终结果按 unittest 的 `FAILED (errors=1)` 和退出码判断，不能把它解释成全测通过。

### 接续第一步与完成标准

1. 只读核对本记录编号、当前 Git 状态、A/B 提交是否变化，以及上述两份证据是否可读取；如队友已更新，先复核差异，避免重复修复。
2. 当前授权止于审查与保存。若用户下一轮明确要求修复，再在隔离分支/副本处理上述五项；若只要求查看，则只说明状态，不自动改源码。
3. 获得修复授权后的验收：新增能捕获各问题的回归测试，A/B 受影响测试通过；若改共享模块则完整回归；之后在独立输出目录验证全量查询与产品等价，保留 W1 数据和历史实验。
4. C 的优化实验、最终 W2 设计/benchmark 报告、提交包仍未完成；“A/B 写好”不代表整周作业已完成。

### 2026-09-19 合并后更新（记录发出后的变化）

- 上述审查记录是 2026-09-19 只读核查时的快照，其中“B 尚未合并”“当前 main 没有 `docs/role_b_query_design.md` / `configs/analytical_queries.json`”等表述已过期。
- 复核结论：队友在审查后没有新提交，A 仍为 `aa3f9cb`、B 仍为 `173a541`；五项问题逐条静态复核，全部仍然存在（引用的行号也未变）。
- 经用户授权，审查材料已提交，B 分支 `173a541` 已合并进 `main`，`docs/role_b_query_design.md`、`configs/analytical_queries.json`、`src/dic_pipeline/queries.py`、`src/dic_pipeline/sql/q1–q6`、`scripts/run_analytical_queries.py`、`tests/test_queries.py` 现已在 `main` 上，可直接读取，无需 `git show`。
- 合并只是代码入主干，不代表验收：五项问题仍未修复，也没有重跑全量查询、全套测试或 C benchmark。
- 下一步在 `c/pipeline-fixes` 上继续 C 的工作；该分支已同步到合并后的 `main`。

## 2026-09-19 修复记录：W2-REVIEW-20260919 五项问题

分支 `c/pipeline-fixes`（基于合并 B 之后的 `main` = `758dc73`）。以下为实际改动与验证结果，不是计划。

### 改动

| 问题 | 修复 | 位置 |
| --- | --- | --- |
| 1 整合快照发布不完整 | `scripts.run_integration` 改为调用 `integration.build_integrated_table`：写入并校验行数后发布 `completed_integration.json`（批次 run_id、四张标准表版本、整合表版本）。发布函数迁到 `integration.py`（`data_products` 仍可导入），发布前用整合表 `run_id` 与批次 `run_id` 做来源校验；旧 W1 输出缺 manifest 时只有校验通过才自动补发，否则报错要求重跑整合。`ingestion.load_completed_batch` 抽出供复用。 | `src/dic_pipeline/integration.py`、`ingestion.py`、`data_products.py`、`scripts/run_integration.py` |
| 2 Q3/Q4/Q5 日历漏首尾零订单小时 | 日历改为“请求的当地日期区间 ∩ `configs/datasets.json` 校验过的 Taxi 上车窗口（`valid_pickup_start_utc`/`valid_pickup_end_utc_exclusive`）”，全部在 UTC 小时上用 `SEQUENCE` 生成，`CASE WHEN` 守护空区间（ANSI 模式下起点大于终点会抛错）。Q4 的天气小时改为与日历连接。`render_query`/`run_query` 新增 `coverage` 参数，默认读 `queries.load_calendar_coverage()`。 | `src/dic_pipeline/queries.py`、`sql/q3_*.sql`、`q4_*.sql`、`q5_*.sql` |
| 3 产品与查询口径不同 | `weather_impact_summary`、`air_quality_impact_summary` 只保留 `environment_in_scope` 行程（与 Q2–Q4 同一总体）；天气标签改用 `queries.integrated_weather_category_sql`（与 Q2 模板同一个 `CASE` 表达式，Q2 模板同步改为引用它）；`valid_hour_count` 更名 `observed_hour_count`，新增 `pickup_borough`；两张产品 `schema_version` 升到 1.1.0。`daily_mobility_summary`、`taxi_zone_statistics` 保持全量行程（与 Q1/Q5/Q6 一致）。 | `src/dic_pipeline/data_products.py`、`configs/data_products.json` |
| 4 UTC 元数据偏移 | 删除 `_as_utc_naive`；进入 Spark 的 Python datetime 一律带时区，`created_at` 用 `unix_micros` 读回 epoch 再还原为 aware UTC（`collect()` 返回的是宿主本地时间的 naive 值）。 | `src/dic_pipeline/data_products.py` |
| 5 Windows 缺 `tzdata` | `requirements.txt` 增加 `tzdata==2026.4`，`.venv` 已安装。 | `requirements.txt` |

### 新增/调整的测试

- `tests/test_queries.py`（12 项）：Q3 整天 24 小时补齐（原断言 4 小时是问题本身）；日历裁剪到校验窗口（全区间 2183 小时、请求 2023-12-25 起仍从 2024-01-01 05:00Z 开始）；两周一首端零订单并列（00/01 点各平均 1）；2024-03-10 DST 日 23 小时且无 2 点；窗口外区间 Q3/Q4/Q5 返回 0 行；Q4 天气小时超出末单仍计入。
- `tests/test_data_products.py`（6 项）：缺 manifest 时来源匹配才补发、不匹配报错且不落盘；产品标签/范围断言；元数据时间戳（产品行与审计表）落在刷新前后的 epoch 窗口内；fixture 增加 `run_id`、`environment_in_scope` 及一条“在范围内但环境缺测”的行程。
- `tests/test_integration.py`（4 项）：`build_integrated_table` 首次发布 manifest；第二批次重跑后 manifest 指向新版本，`register_analytics_inputs` 读到新快照。
- `tests/test_product_query_alignment.py`（新，5 项）：用产品重算并与 Q1–Q6 对比——Q1/Q6 精确相等，Q2 分类别 sum/count 重算均值相等，Q3 每小时 NYC 订单数一致（同小时一条域外订单不再计入），Q4 用产品订单数 ÷ 日历天气小时数重建 `demand_range` 与查询相等，Q5 峰值总量与产品分桶一致。

### 验证结果（本机，2026-09-19，`.venv` Python 3.11.9 / Spark 4.2.0 / Delta 4.4.0）

- 受影响套件：`test_queries` 12 项 OK（75.9 s）；`test_integration` + `test_product_query_alignment` 9 项 OK；`test_data_products` 6 项 OK（438 s，与其他测试并行时的耗时）。
- 完整回归 `unittest discover -s tests`：**51 项 OK，1085.8 s**（与全量数据 smoke 并行，耗时偏大）。`git diff --check` 通过。
- 全量数据（本机 `data/delta/`，W1 批次 `563b32da…`）：`register_analytics_inputs` 自动补发 `completed_integration.json`（来源校验通过，整合版本 0），注册 9,554,576 行。`python -m scripts.run_data_products` 四张产品 `status=success`：`daily_mobility_summary` 2,183 行、`taxi_zone_statistics` 773 行、`weather_impact_summary` 1,217 行（原 3,017 行是原始天气代码 × 全量行程）、`air_quality_impact_summary` 2,183 行；创建/刷新时间为正确的 UTC 瞬时（12:18Z = 本机 20:18）。`python -m scripts.run_analytical_queries --query all` 六个查询全部运行：Q3 日历合计 2,183 小时、订单 9,517,007（NYC 范围，无 `missing_pm25`）；Q5 每个星期×小时 `observed_hour_count`=13（恰好 13 周）；Q6 三个月合计 9,554,576。
- 未做：产品与查询在全量数据上的逐项等价核对（小样本对齐测试已覆盖口径，全量等价留给 C 的 benchmark 框架）；未推送、未合并回 `main`。
- 注意：`docs/review_evidence/w2-2026-09-19/review_probes.py` 引用了已删除的 `_as_utc_naive`，它是针对 `173a541` 的复现脚本，保留原样作为证据，不在新代码上运行。

### 下一步

1. 合并 `c/pipeline-fixes` 回 `main`（无冲突预期：`main` 自 `758dc73` 后无新提交）。
2. C：在此基线上搭 `query_benchmark.py`，先做 Q1/Q2/Q6 的缓存与 AQE 对照，再做 Q3–Q5、分区裁剪、广播连接，并把“产品 vs 基础 SQL”的全量等价核对纳入实验框架。

## 2026-09-19 W2 优化实验（C）

分支 `c/pipeline-fixes`。框架提交 `0e71253`，全量实验运行 ID `20260919T143454Z-9d921a51`。

### 新增代码

- `src/dic_pipeline/query_benchmark.py`：实验定义（`Experiment`/`Variant` 数据类）、配置上下文管理器、计划事实提取、产品表注册、运行器。
- `scripts/run_query_benchmark.py`：CLI，支持 `--list`、`--experiment`、`--repeats`、`--start-date`/`--end-date`、`--skip-products`。
- `src/dic_pipeline/sql/products/`：六个产品版改写，与基础查询共用 `render_template`，日期过滤/小时日历/分类表达式是同一份文本。
- `src/dic_pipeline/queries.py`：抽出 `render_template`；`render_query`/`run_query` 新增 `pickup_date_filter`（分区裁剪用的等价谓词）和 `views`（替换源视图）。
- `tests/test_query_benchmark.py`：4 项，验证 13 个实验全部结果相等、计划事实符合预期、不一致时不计时、配置与缓存被还原。

### 实验协议

各变体预热一次，交替测三次，`collect()` 真算，报加速比前必须结果相等（计数精确，浮点 rel 1e-9 / abs 1e-6）。广播实验额外与基础 Q1 核对。两处协议偏离已记录在 `results.json`：缓存实验先测基线再建缓存（Spark 会把已缓存计划替换进任何匹配查询）；缓存内存按构建前后差值报告（Delta 自身缓存 log-state RDD）。

### 全量结果（9,554,576 行，中位数秒，13 项全部结果相等）

| 实验 | 技术 | 基线 | 优化 | 加速 |
| --- | --- | ---: | ---: | ---: |
| `q1_partition_pruning` | 分区裁剪 | 1.84 | 1.47 | 1.25× |
| `q6_partition_pruning` | 分区裁剪 | 0.81 | 0.63 | 1.28× |
| `q4_cache_projection` | 缓存 | 4.87 | 4.86 | 1.00× |
| `q6_cache_projection` | 缓存 | 1.44 | 1.03 | 1.39× |
| `q1_broadcast_zones` | 广播连接 | 8.05 | 5.56 | 1.45× |
| `q4_aqe` | AQE | 28.15 | 4.91 | 5.73× |
| `q6_aqe` | AQE | 1.45 | 1.28 | 1.13× |
| `q1_product_taxi_zone_statistics` | 数据产品 | 4.03 | 0.43 | 9.27× |
| `q2_product_weather_impact_summary` | 数据产品 | 0.66 | 0.33 | 1.99× |
| `q3_product_air_quality_impact_summary` | 数据产品 | 1.54 | 1.18 | 1.31× |
| `q4_product_weather_impact_summary` | 数据产品 | 4.38 | 0.84 | 5.21× |
| `q5_product_daily_mobility_summary` | 数据产品 | 0.94 | 0.32 | 2.99× |
| `q6_product_daily_mobility_summary` | 数据产品 | 1.06 | 0.37 | 2.89× |

计划证据：裁剪变体多出 3 个 `PartitionFilters`；广播基线 `SortMergeJoin` → 优化 `BroadcastHashJoin`；缓存只在优化侧出现 `InMemoryTableScan`；AQE 只在开启时出现 `isFinalPlan=true` 和 `AQEShuffleRead`。AQE 开关两侧的 Exchange 计数不可比（自适应计划文本内嵌子计划），未用于比较。

存储与刷新：四张产品合计 172,329 字节 / 4 文件，整合表 1,058,134,566 字节 / 91 文件，开销 0.016%；全量刷新 142.5 秒。六个查询各跑一轮省 9.14 秒，约 16 轮回本。

缓存成本：Q4 五列投影构建 4.59 秒、77.1 MB；Q6 单列 0.76 秒、19.5 MB；均已释放。

### 交付物

- [W2 benchmark report](docs/w2_benchmark_report.md)：方法、环境、结果、T5 五个讨论题、局限、复现。
- [原始计时](docs/w2_benchmark_timings.csv)：78 条样本。
- [优化策略与权衡](docs/w2_design_optimization.md)：C 在设计报告中负责的一节，待 A 整合进完整 3–5 页文档。
- 运行产物 `data/benchmark/w2/20260919T143454Z-9d921a51/`（results.json、sql/、plans/），不纳入 Git。

### 未完成

- W2 完整设计报告（A 整合 + B 的查询/产品设计说明）、提交包。
- 组合优化（先做的是单因素独立对照）；十城扩展的建议是基于数据形状的外推，非实测。
