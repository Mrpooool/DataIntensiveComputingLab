# DataIntensiveComputingLab

[English](README.md)

W1 课程项目：用 PySpark 清洗 Taxi、Weather、Air Quality 和 Taxi Zones，写入 Delta 表，并生成每行代表一趟行程的整合表。

截至 2026-09-09，四表摄入、行程整合和两种 Taxi 布局的全量性能实验均已跑通，27 项小样本回归测试通过。整合表保留 9,554,576 条唯一行程。设计说明、数据契约、架构图和实验报告见文末链接。

## 环境与输入

统一使用 **Python 3.11.9、JDK 21、PySpark 4.2.0、Delta 4.4.0**；依赖固定在 `requirements.txt`。先安装 Python/JDK，设置 `JAVA_HOME`，再在项目根目录的 PowerShell 执行：

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

从仓库根目录依次运行以下命令。整合和 benchmark 都需要四表摄入成功后生成的完成标记，其中 benchmark 使用该批次的 Zone 表，并重新读取原始 Taxi 文件构建实验布局。

```powershell
# A/B：读取、清洗并写入四份数据
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all

# C：关联地点、天气、PM2.5，保存整合表与匹配统计
.\.venv\Scripts\python.exe -m scripts.run_integration

# C：重建两种 Taxi 布局，执行 Task 6 性能实验
.\.venv\Scripts\python.exe -m scripts.run_benchmark
```

默认使用 `local[4]`、4 GB JVM 堆和 128 个 shuffle 分区。摄入与 benchmark 支持 `--master`、`--driver-memory`、`--shuffle-partitions`；使用 `--help` 查看路径参数。

| 输出 | 路径 |
| --- | --- |
| 四张标准表、拒绝表 | `data/delta/{standardized,rejected}/<dataset>/` |
| 摄入记录、四表完成标记 | `data/delta/metadata/` |
| 整合表（按 `pickup_date` 分区） | `data/delta/integrated/integrated_taxi_trips/` |
| 匹配统计 | `data/delta/integrated/integration_metrics.json` |
| 每次独立实验、原始耗时、查询计划 | `data/benchmark/<run_id>/` |

只有四表全部摄入成功才发布 `completed_batch.json`；关联读取其中记录的四个 Delta 版本。单表重跑会使完成标记失效，此时重新运行 `--dataset all`。同一输出目录只运行一个摄入进程；W1 不清理交接版本。摄入和关联会覆盖各自目标表，benchmark 每次新建目录。

## Task 6 实验口径

- S0 不分区；S1 按纽约当地 `pickup_date` 分区。同一原始输入、清洗规则、压缩和 `coalesce(8)` 写入参数。
- 导入各跑两次，顺序 S0/S1、S1/S0；计时包含读取、清洗及 accepted/rejected 提交，读回验证在计时外。
- 三个指定查询：各上车 borough 行程数、每日平均时长、各上车 borough 平均车费；另测 2 月 1 至 7 日范围查询。
- 每个查询/布局预热一次，交替测三次；核对结果后报告中位数。Taxi 不缓存，操作系统缓存不可控。
- 文件数和大小取当前 Delta 快照，不包含 `.crc`、日志及历史废文件。查询使用第二轮写出的表。

最终复测于 2026-09-09 完成，运行 ID 为 `20260909T144949Z-b493da9f`，状态为 `success`。四次导入均保留 9,554,576 条唯一行程，24 次查询测量的结果一致。下表为本次复测结果，时间均取中位数，数据大小按十进制 MB 计算。

| 指标 | S0 不分区 | S1 按日期分区 |
| --- | ---: | ---: |
| 导入（秒） | 218.536 | 234.876 |
| 当前数据大小（MB） | 1042.245 | 1044.463 |
| 当前数据文件数 | 8 | 728 |
| 各上车 borough 行程数（秒） | 1.128 | 1.859 |
| 每日平均行程时长（秒） | 0.999 | 1.502 |
| 各上车 borough 平均车费（秒） | 1.064 | 1.878 |
| 2 月 1 至 7 日统计（秒） | 0.618 | 0.575 |

三个全量查询仍是 S0 更快，一周范围查询的差距较小。操作系统缓存不可控，每项查询只有三次测量，这些耗时不能视为固定性能指标，也不能直接用于判断更宽的整合表应采用哪种布局。

本次复测的完整结果保存在本机 `data/benchmark/20260909T144949Z-b493da9f/results.json`，SQL 与执行计划位于同目录下的 `queries.sql` 和 `plans/`。这些产物不纳入 Git，可按上述命令重新生成。[性能报告](docs/benchmark_report.md) 和 [原始耗时](docs/benchmark_timings.csv) 记录的是首次实验 `20260909T141616Z-ce81d328`，其中约 130/155 秒的导入耗时属于该次运行。

## 测试与协作

```powershell
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

2026-09-09 最终回归记录为 `Ran 27 tests in 276.692s`、`OK`；该次运行将小样本 Delta 快照并行度设为 2。测试使用真实 Spark/Delta 和小样本，覆盖非法时间、NaN/Infinity、整数边界、去重选择、跨日与夏令时、环境缺测、关联行数保持及 Delta 读回等情况。

完整数据验证需另跑上述流水线。Windows 退出时偶发 JAR 清理日志，应结合测试 `OK` 和退出码判断。

| 角色 | 代码职责 |
| --- | --- |
| A：Task 2-3 | `ingestion.py`、环境安装、读写入口 |
| B：Task 1、4 | `schemas.py`、`transforms.py`、`validation.py`、`preparation.py` |
| C：Task 5-6 | `integration.py`、`benchmark.py` 及运行入口 |

在个人分支开发，通过 PR 合并；只提交相关源码、配置、文档和测试。不要提交 `data/`、`.venv/`、`.hadoop/` 或密钥。更改依赖需同步 `requirements.txt`。

## 数据口径与文档

全量摄入已验证：Taxi 9,554,576 条通过、202 条拒绝；Weather 8,784 条、NYC Air 51,885 条、Zones 265 条通过。整合表行数和唯一记录数均为 9,554,576。纽约范围内的 9,517,007 条行程均匹配到天气和空气质量小时记录；域外或未知上车地点不附加纽约环境值。

Weather 的纽约背景和 UTC 时区仍是显式假设；100% 小时匹配不代表指标完整或假设已验证。Air 保留全国原文件，标准表筛选纽约五区；当前六个站点仅覆盖其中三区。

- [正式设计报告（Markdown，已润色）](docs/w1_design_report.md) · [4 页 PDF（润色前版本）](docs/w1_design_report.pdf)
- [性能实验报告（首次实验）](docs/benchmark_report.md) · [首次实验原始耗时](docs/benchmark_timings.csv)
- [执行计划](task_plan.md) · [数据目录](docs/data_catalog.md) · [数据契约](docs/data_contract.md)
- [存储与摄入设计](docs/role_a_task2_task3.md) · [架构图](docs/architecture.md) · [进度](progress.md)
