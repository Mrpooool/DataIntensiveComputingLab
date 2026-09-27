# DataIntensiveComputingLab

[English](README.md)

课程项目：用 PySpark + Delta Lake 处理四份 2024 年纽约市数据。W1 完成摄入、清洗、校验与行程整合；W2 在此基础上增加六个分析查询、四张可复用数据产品，以及针对它们的四类优化对照实验。W3 支持增量更新与 Schema 演进、扩展校验、统一监控，并做了生产就绪评测。

截至 2026-09-27：W1、W2、W3 的代码、全量运行与评测均已完成。进度见 [progress.md](progress.md)。

## 环境与输入

统一使用 Python 3.11.9、JDK 21、PySpark 4.2.0、Delta 4.4.0；依赖固定在 `requirements.txt`。先安装 Python/JDK，设置 `JAVA_HOME`，再在项目根目录的 PowerShell 执行：

```powershell
python --version
java -version
powershell -ExecutionPolicy Bypass -File .\scripts\setup_env.ps1
```

安装脚本创建 `.venv`、安装依赖并检查版本；原生 Windows 还会下载固定版本的 Hadoop helpers 并校验哈希。每人创建自己的环境，不复制 `.venv`。Linux/macOS 用 `python3.11 -m venv .venv`，再用 `.venv/bin/python -m pip install -r requirements.txt`；不需要 Windows helpers。

从 [课程要求](Assignment.md) 中的共享目录下载以下文件到 `data/raw/`：

- `yellow_tripdata_2024-01.parquet`、`yellow_tripdata_2024-02.parquet`、`yellow_tripdata_2024-03.parquet`
- `weather.csv`、`taxi_zone_lookup.csv`
- `air_quality.zip` 解压得到的 `hourly_88101_2024.csv`

## 运行 W1

从仓库根目录依次运行；每一步都依赖上一步产生的标记文件。

```powershell
# A/B：读取、清洗并写入四份数据
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all

# C：关联地点、天气、PM2.5，保存整合表与匹配统计，并发布分析快照
.\.venv\Scripts\python.exe -m scripts.run_integration

# C：重建两种 Taxi 布局，执行 Task 6 存储实验
.\.venv\Scripts\python.exe -m scripts.run_benchmark
```

默认使用 `local[4]`、4 GB JVM 堆和 128 个 shuffle 分区；各命令支持 `--master`、`--driver-memory`、`--shuffle-partitions`，用 `--help` 查看全部参数。

| 输出 | 路径 |
| --- | --- |
| 四张标准表、拒绝表 | `data/delta/{standardized,rejected}/<dataset>/` |
| 摄入记录、四表完成标记、整合快照 | `data/delta/metadata/` |
| 整合表（按 `pickup_date` 分区） | `data/delta/integrated/integrated_taxi_trips/` |
| 匹配统计 | `data/delta/integrated/integration_metrics.json` |
| W1 存储实验 | `data/benchmark/<run_id>/` |
| W2 优化实验结果、SQL、执行后计划 | `data/benchmark/w2/<run_id>/` |

只有四表全部摄入成功才发布 `completed_batch.json`，整合读取其中记录的四个 Delta 版本；单表重跑会使完成标记失效，需重新运行 `--dataset all`。整合成功后会重新发布 `completed_integration.json`，W2 的查询与产品固定读取其中记录的版本；若只有旧的 W1 输出而缺该文件，只有在整合表的 `run_id` 与当前批次一致时才自动补发。同一输出目录只运行一个摄入进程；摄入和整合会覆盖各自目标表，benchmark 每次新建目录。

## W1 存储实验结果

S0 不分区 vs S1 按纽约当地 `pickup_date` 分区，其余参数完全相同。方法口径（两轮交替导入、预热一次后测三次、结果核对、文件统计只取当前快照）见 [W1 性能报告](docs/benchmark_report.md)。

最终复测 `20260909T144949Z-b493da9f`，状态 `success`，四次导入均保留 9,554,576 条唯一行程，24 次查询测量结果一致。时间取中位数，数据大小按十进制 MB 计：

| 指标 | S0 不分区 | S1 按日期分区 |
| --- | ---: | ---: |
| 导入（秒） | 218.536 | 234.876 |
| 当前数据大小（MB） | 1042.245 | 1044.463 |
| 当前数据文件数 | 8 | 728 |
| 各上车 borough 行程数（秒） | 1.128 | 1.859 |
| 每日平均行程时长（秒） | 0.999 | 1.502 |
| 各上车 borough 平均车费（秒） | 1.064 | 1.878 |
| 2 月 1 至 7 日统计（秒） | 0.618 | 0.575 |

三个全量查询 S0 更快，一周范围查询差距很小。每项只测三次且操作系统缓存不可控，这些耗时不能视为固定性能指标。[性能报告](docs/benchmark_report.md) 与 [原始耗时](docs/benchmark_timings.csv) 记录的是首次实验 `20260909T141616Z-ce81d328`（导入约 130/155 秒），与上表不是同一次运行。

## 运行 W2

W2 直接复用 W1 的 `data/delta/`，无需新建仓库或复制表。确认 W1 摄入与整合成功后运行：

```powershell
# B：六个 Spark SQL 分析查询
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries --query all

# 只执行 Q3、Q5；日期范围是纽约当地日期的左闭右开区间
.\.venv\Scripts\python.exe -m scripts.run_analytical_queries `
  --query q3 --query q5 --start-date 2024-01-01 --end-date 2024-02-01 --explain

# A：四张可复用数据产品
.\.venv\Scripts\python.exe -m scripts.run_data_products

# C：优化实验（分区裁剪、缓存、广播连接、AQE、产品版改写）；--list 列出实验名
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark
.\.venv\Scripts\python.exe -m scripts.run_query_benchmark --experiment q1_partition_pruning --repeats 5
```

六个查询的粒度、天气分类、PM2.5 分箱、零订单小时和缺测处理见 [B 的查询设计](docs/role_b_query_design.md)。Q3 至 Q5 的小时日历取“请求区间 ∩ `configs/datasets.json` 中校验过的 Taxi 时间窗”，不由首末订单推导，因此窗口内无订单的小时计为零需求，窗口外不会凭空补出小时。用 `--show-sql` 查看实际提交给 Spark 的 SQL。

## W2 优化实验结果

每个基础查询配一个优化变体，各预热一次后交替测三次并 `collect()` 真算；只有优化结果与基础结果相等才报告加速比（涉及非规范查询的变体还要与规范查询核对）。缓存实验先测基线再建缓存，因为 Spark 会把已缓存的计划替换进任何匹配的查询。

全量运行 `20260919T143454Z-9d921a51`，13 项实验全部结果一致，中位数加速比：

| 技术 | 实验 | 加速比 |
| --- | --- | ---: |
| AQE | Q4 / Q6 | 5.73× / 1.13× |
| 广播连接 | Q1（基础表重建） | 1.45× |
| 分区裁剪 | Q1 / Q6 | 1.25× / 1.28× |
| 缓存 | Q6 / Q4 | 1.39× / 1.00× |
| 数据产品 | Q1-Q6 | 1.31× ～ 9.27× |

Q4 的缓存无收益且占 77 MB，因为扫描不是它的瓶颈；这一负面结果与 AQE 在 Q4 上的 5.73× 一起说明该工作负载的开销集中在 shuffle 与聚合，而非 I/O。四张产品合计 172,329 字节（整合表 1,058,134,566 字节，开销 0.016%），全量刷新 142.5 秒，六个查询各跑一轮省 9.14 秒，约 16 轮回本。

完整方法、T5 五个讨论题、局限与复现说明见 [W2 benchmark report](docs/w2_benchmark_report.md)；78 条原始样本见 [原始计时](docs/w2_benchmark_timings.csv)；优化策略与权衡见 [设计报告 C 节](docs/w2_design_optimization.md)。`results.json` 保存中位数、原始样本、计划事实（分区过滤、连接策略、内存扫描、AQE 最终计划）、缓存成本与产品存储，`plans/` 保存每个变体的 `EXPLAIN FORMATTED` 和执行后计划。产品版改写（`src/dic_pipeline/sql/products/`）只对完整覆盖区间有效。

## 运行 W3

W3 在已发布的 `data/delta/` 上直接做增量更新，不重建平台：

```powershell
# 从已发布的表生成三个更新文件到 data/updates/，另写 update_manifests.json（新增数、重复数、Schema 变化）
.\.venv\Scripts\python.exe -m scripts.run_incremental generate

# 校验并 MERGE 更新，把新行程追加进整合表，重新发布快照；中途失败后重跑即可补齐
.\.venv\Scripts\python.exe -m scripts.run_incremental apply

# 只刷新基于旧整合版本建成的产品
.\.venv\Scripts\python.exe -m scripts.run_data_products --mode auto

# 校验报告与运维指标（读 metadata/pipeline_runs）
.\.venv\Scripts\python.exe -m scripts.run_monitoring_report
.\.venv\Scripts\python.exe -m scripts.run_monitoring_report --query failure_codes_by_target
```

Schema 演进需要事先登记：只有写在 `configs/datasets.json` 的 `schema_evolution.allow_add` 里、带类型的列才能自动加入，目前是 Weather 的 `humidity` 和 Air Quality 的 `aqi`。读取更新时按文件真实的表头或 Parquet Schema 检查，新列以可空列加进标准化表；删列、改名、改类型或未登记的列会让这次更新以 `SchemaValidationError` 停下。演进列不进整合表，所以六个查询和四张产品都不用改。

监控报告包含五个查询：各数据集校验失败情况、按错误码的失败数、各阶段耗时、每次执行的拒绝行数、耗时趋势。被拒绝的行本身在 `data/delta/rejected/<dataset>/`，附 `error_reasons`。`--import-legacy` 会把 W1、W2 的旧运行记录一次性导入 `pipeline_runs`。

复现评测：先用当前代码在单独目录建基线，再在它的副本上测量；基线本身不会被修改，每次运行都用新副本。

```powershell
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all --output-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_integration --delta-root data/benchmark/w3/baseline
.\.venv\Scripts\python.exe -m scripts.run_data_products --delta-root data/benchmark/w3/baseline

# 六项计时加存储报告；--measurement 选单项，--list 列出全部
.\.venv\Scripts\python.exe -m scripts.run_w3_evaluation --baseline-root data/benchmark/w3/baseline
```

校验默认开启。`--no-validation` 和 `--no-monitoring` 只用于评测，不要用在正式数据上。

## W3 评测结果

2026-09-26 全量评测（`20260926T174427Z-1ce27d00`、`20260926T180757Z-afffd333`），每项预热一次后交替测三次取中位数，所有运行的输出一致：

| 测量 | 结果 |
| --- | --- |
| 增量更新 | 应用三个更新文件 134 秒；从头摄入并整合原数据要 313 秒。插入 668,820 条新行程，跳过 143,319 条已有行程的副本 |
| 分析刷新 | 全量重建 66 秒，`auto` 87 秒，产品内容相同。这个规模下找出受影响的小时要扫一遍整合表，和直接重建这些小产品差不多；`auto` 只有在整合表没变时才省事（跳过全部产品） |
| 存储开销 | 快照增加 166 MB（7.9%），与新增 7% 行程相当；但 Weather/Air 表从 1 个文件变成 90 个小文件 |
| 校验开销 | 全量摄入多 47 秒（27%），几乎都在 Taxi；拒绝 202 条（0.0021%），新规则 `missing_reference_record` 在原数据上为 0 |
| 监控开销 | 每写一行约 6 秒：摄入多 13%，整合多 14%，产品刷新多 60% |

第一次全量运行时 `auto` 比全量慢 54%（131 秒对 85 秒），原因是惰性求值导致产品被重复计算；物化一次后两条路径都变快。完整数据、解释与 Task 5 讨论见 [评测报告](docs/w3_evaluation_report.md)，设计与权衡见 [设计报告](docs/w3_design_report.md)，原始样本见 [w3_evaluation_timings.csv](docs/w3_evaluation_timings.csv)。

## 测试与协作

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

测试使用真实 Spark/Delta 和小样本，不需要原始数据。覆盖非法时间、NaN/Infinity、整数边界、去重选择、跨日与夏令时、环境缺测、关联行数保持、Delta 读回、六个分析查询、产品与查询口径等价，以及实验框架本身。Windows 上 `zoneinfo` 依赖 `tzdata`，已固定在 `requirements.txt`。

测试数量随项目增长：W1 完成时 27 项（2026-09-09），加入 W2 A/B 后 39 项（2026-09-18），修复审查问题并加入实验框架后 55 项（2026-09-19），W3 增量、校验、监控与评测完成后 86 项（2026-09-27）。Windows 上跑完全部约 50 分钟，大部分时间在增量和评测两个套件。全量数据验证需另跑上述流水线。Windows 退出时偶发 JAR 清理日志，应结合测试 `OK` 和退出码判断。

| 角色 | 代码职责 |
| --- | --- |
| A：摄入、数据产品与增量更新 | `ingestion.py`、`data_products.py`、`incremental.py`、环境安装与运行入口 |
| B：口径与查询 | `schemas.py`、`transforms.py`、`validation.py`、`preparation.py`、`queries.py`、`sql/` |
| C：整合、性能、监控与评测 | `integration.py`、`benchmark.py`、`query_benchmark.py`、`monitoring.py`、`w3_evaluation.py` 及对应入口 |

在个人分支开发，通过 PR 合并；只提交相关源码、配置、文档和测试。不要提交 `data/`、`.venv/`、`.hadoop/` 或密钥。更改依赖需同步 `requirements.txt`。

## 数据口径与文档

全量摄入已验证：Taxi 9,554,576 条通过、202 条拒绝；Weather 8,784 条、NYC Air 51,885 条、Zones 265 条通过。整合表行数和唯一记录数均为 9,554,576。纽约范围内的 9,517,007 条行程均匹配到天气和空气质量小时记录；域外或未知上车地点不附加纽约环境值。

Weather 的纽约背景和 UTC 时区仍是显式假设；100% 小时匹配不代表指标完整或假设已验证。Air 保留全国原文件，标准表筛选纽约五区；当前六个站点仅覆盖其中三区。

- W1：[设计报告](docs/w1_design_report.md) · [性能报告](docs/benchmark_report.md) · [原始耗时](docs/benchmark_timings.csv) · [架构图](docs/architecture.md)
- W2：[benchmark report](docs/w2_benchmark_report.md) · [原始计时](docs/w2_benchmark_timings.csv) · [优化策略与权衡](docs/w2_design_optimization.md) · [B 的查询设计](docs/role_b_query_design.md)
- W3：[设计报告](docs/w3_design_report.md) · [评测报告](docs/w3_evaluation_report.md) · [A 的增量说明](docs/w3_role_a_incremental.md) · [B 的校验说明](docs/w3_role_b_validation.md) · [角色间接口](docs/w3_interfaces.md)
- 共用：[执行计划](task_plan.md) · [数据目录](docs/data_catalog.md) · [数据契约](docs/data_contract.md) · [进度](progress.md)
