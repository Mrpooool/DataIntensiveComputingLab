# 项目进度

截至 2026-09-18：W1 开发、验证与提交包已完成；W2 的 A 数据产品和 B 分析查询已进入实现与联调，C 优化实验仍待完成。详细安排见 [task_plan.md](task_plan.md)。

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
