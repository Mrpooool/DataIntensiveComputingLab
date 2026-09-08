# Week 1 — Role A: Task 2 and Task 3

本文按照 `Assignment.md` 的原问题，说明当前仓库中已经实现的存储架构和通用摄入
框架。架构图见 `docs/architecture.md`。

## Task 2: Design Your Storage Architecture

### Directory structure

```text
data/
  raw/                              # 原始下载文件，不修改
  delta/
    standardized/
      taxi/                         # 通过校验的 Taxi Delta 表
      weather/                      # 通过校验的 Weather Delta 表
      air_quality/                  # NYC 范围内通过校验的 Air Quality Delta 表
      taxi_zones/                   # 通过校验的 Zone lookup Delta 表
    rejected/
      taxi/
      weather/
      air_quality/
      taxi_zones/
    metadata/
      ingestion_runs/               # 每个数据集、每次运行的摄入记录
    integrated/
      integrated_taxi_trips/        # 由 C 生成
  benchmark/
    taxi_unpartitioned/             # Task 6 策略 S0，由 C 生成
    taxi_partitioned_by_pickup_date/# Task 6 策略 S1，由 C 生成
```

`raw` 是不可变输入层。A 读取原文件，但不覆盖或修改它们。`standardized` 保存 B 的
`prepare()` 返回的 accepted 数据，供 C 使用；`rejected` 保存未通过质量规则的行和
错误原因；`metadata` 保存运行状态和统计。`data/` 已加入 `.gitignore`，因为这些文件
可以从原始输入重新生成，而且 Taxi Delta 表接近 1 GB。

### Delta table organization

每个叶子目录是一张独立 Delta 表，而不是一个普通 Parquet 文件。例如：

```text
data/delta/standardized/taxi/
  part-00000-....snappy.parquet
  ...
  _delta_log/
    00000000000000000000.json
```

`part-*.parquet` 保存实际列式数据；`_delta_log` 记录当前有效文件、Schema 和提交
历史。读取时必须读取表目录：

```python
taxi = spark.read.format("delta").load("data/delta/standardized/taxi")
```

不应直接枚举或拼接 `part-*` 文件，否则会绕过 Delta transaction log。

当前 A 写出四张 standardized 表、四张 rejected 表和一张
`metadata/ingestion_runs` 表。C 后续写 integrated 和 benchmark 表时可以复用
`write_delta()`。

### Naming conventions

- 目录名、表名和标准列名使用小写 `snake_case`；
- 原始列名只存在于 raw DataFrame，B 在
  `src/dic_pipeline/transforms.py` 中统一名称；
- UTC 时间字段带 `_utc`，例如 `pickup_timestamp_utc` 和
  `pickup_hour_utc`；
- 来源时间使用 `_source`，例如 `pickup_timestamp_source`；
- 稳定记录键统一命名为 `record_id`，Zone lookup 使用业务键
  `location_id`；
- 审计字段使用固定名称：`source_file`、`run_id`、`schema_version` 和
  `rule_version`。

### Partitioning strategy

分区和文件数是两个不同设计：

- **分区**决定是否产生 `pickup_date=.../` 目录，主要影响 partition pruning；
- **文件数**决定同一张表由多少个 Parquet data files 组成，主要影响并行度和
  small-file overhead；
- 不分区的表也可以有多个文件；一个分区也可以包含多个文件。

当前全量写入后的实际布局如下。

| 表 | 当前策略 | 实际规模 | 原因 |
| --- | --- | ---: | --- |
| Taxi standardized | 不分区，8 个文件 | 9,554,576 行，约 994 MB | 作为 Task 6 的 S0 基线；8 个文件约 124 MB/文件 |
| Weather | 不分区，1 个文件 | 8,784 行，约 0.74 MB | 表太小，全表读取成本低 |
| Air Quality | 不分区，1 个文件 | NYC 51,885 行，约 3.79 MB | C 需要全表做小时聚合，分区没有收益 |
| Taxi Zones | 不分区，1 个文件 | 265 行，约 0.03 MB | 小型 lookup table |
| Rejected | 不分区，每表 1 个文件 | 当前最多 Taxi 202 行 | 体量很小 |
| Ingestion metadata | 不分区 | 当前每个数据集一条运行记录 | 运行日志体量很小 |
| Integrated Taxi Trips | 初始建议按 `pickup_date` | C 生成后测量 | 宽事实表，日期过滤可能受益 |

`configs/datasets.json` 中的 `output_files` 是当前 W1 全量写入的文件数目标，不是
partition count。A 使用 `coalesce(output_files)` 避免小表继承 128 个 shuffle
分片并产生上百个小文件。如果数据规模变化，应重新按实际输出大小调整，不能永久
固定 Taxi 为 8。

### Task 6 的两个 Taxi storage strategies

A 提供设计和公共 writer，C 负责生成副本并 benchmark：

1. **S0 — unpartitioned**
   `data/benchmark/taxi_unpartitioned`，与当前 standardized Taxi 一样不分区。
2. **S1 — partitioned by `pickup_date`**
   `data/benchmark/taxi_partitioned_by_pickup_date`，产生
   `pickup_date=YYYY-MM-DD/` 目录。

两张实验表必须来自同一 accepted Taxi DataFrame，使用相同 Schema、列和压缩格式。
行数及查询结果必须相同，主要实验变量是 partition layout。不能预先断言 S1 更快：
课程指定的三个聚合查询未必包含日期过滤；额外的固定日期范围查询才适合验证
partition pruning。

### Which datasets should conceptually be treated as lookup tables?

`taxi_zones` 是 lookup table。它只有 265 行，把 `location_id` 映射到 borough、
zone 和 service zone，由 Taxi 的 pickup/dropoff location ID 两次引用。它规模小、
变化慢、被事实表引用，符合 lookup/dimension table 的特征。

Taxi 是 trip fact table。Weather 和 Air Quality 是随时间增长的 observation fact
tables，不是 lookup tables。

### Which datasets should not be partitioned? Why?

- **Taxi Zones**：只有 265 行，任何分区都会增加目录和 metadata 开销；
- **Weather**：全年 8,784 行、不到 1 MB，扫描整表比管理分区更便宜；
- **Air Quality standardized**：B 将全国 8,139,551 行限定为 NYC 51,885 行，
  当前不到 4 MB；C 还需要完整聚合这些监测记录；
- **Rejected 和 ingestion metadata**：当前都是小表；
- **Taxi standardized 基线**：保持不分区，作为 Task 6 S0，与 S1 公平比较。

这里的结论针对当前 standardized 表。原始全国 Air Quality CSV 很大，但 raw 文件
不在本任务中重新组织为分区 Delta 表。

### Which datasets require different partitioning strategies?

- Taxi 和 integrated Taxi 是大型事实表，需要根据时间范围查询评估时间分区；
- Weather、NYC Air Quality 和 Taxi Zones 当前保持不分区；
- Task 6 对 Taxi 明确比较不分区与 `pickup_date` 分区；
- integrated 表更宽，并且预期经常按日期分析，因此可先按 `pickup_date` 写入，
  但仍需由 C 检查每个分区大小和查询计划。

### Under what conditions does partitioning become harmful?

- 表本身很小；
- 分区列基数过高，例如按 timestamp、record ID 或监测站点分区；
- 每个分区只有很少数据，产生大量小文件和目录；
- 查询不按分区列过滤，无法 partition pruning；
- 数据倾斜导致少数分区特别大；
- 写入频繁触碰大量分区，增加提交、文件列表和调度成本。

分区不是“越多越快”。收益来自跳过不相关目录；如果查询仍扫描全部分区，就只剩
额外管理成本。

### What changes if total volume grows by 20×?

- Taxi standardized 可能从约 1 GB 增长到约 20 GB，应使用增量写入，并根据常用
  查询范围在月分区和日分区之间实测选择；
- 继续以约 128–256 MB 为目标文件大小，动态调整文件数并合并 small files；
- NYC Air Quality 按当前比例约 76 MB，Weather 约 15 MB，仍不应仅因为“增长
  20×”就自动分区；
- 若改为保存多年或全美国 Air Quality，并且查询经常限定日期，可考虑
  `year/month`，但不按小时或站点分区；
- 监控每张表的当前快照大小、文件数、每分区数据量和数据倾斜；
- 将 `--output-root` 指向 HDFS 或对象存储时，保持 reader/prepare/writer 接口，
  并配置相应 Spark connector；
- 用增量批次和稳定 `run_id` 代替每次重写全部历史。

## Task 3: Build a Generic Ingestion Framework

### Current implementation flow

```text
configs/datasets.json
        |
        v
A read_source(): CSV/Parquet + reader options + explicit CSV raw Schema
        |
        v
B prepare(): schema validation + standardization + timestamp/type conversion
             + dataset rules + duplicate detection
        |
        +------ accepted ------+
        |                      |
        +------ rejected ------+--> A write_delta()
                                      |
                                      v
                              read back and count
                                      |
                                      v
                         metadata/ingestion_runs
```

入口为 `scripts/run_ingestion.py`。选择 `all` 时，脚本为四个数据集生成同一个
`run_id`，依次调用 `ingest_dataset()`。

### Assignment requirements and implementation

| Assignment requirement | Current implementation |
| --- | --- |
| Load CSV and Parquet | `read_source()` 根据 `source_format` 调用 Spark DataFrameReader |
| Validate input schema | CSV 使用 `RAW_SCHEMAS`；Parquet 使用文件 Schema；`prepare()` 再检查 required raw columns |
| Standardize column names | B 的 `standardize_column_names()` 转为 `snake_case` |
| Normalize timestamps/types | B 的 Taxi、Weather、Air Quality transformer |
| Dataset-specific rules | `TRANSFORMERS` 和 `RULE_BUILDERS` 映射表 |
| Data-quality checks | `prepare()` 检查缺键、非法时间/数值和重复记录 |
| Store as Delta | A 的 `write_delta()` |
| Generate metadata | A 的 `write_metadata()`，记录成功和普通失败运行 |

### Which components are generic and reusable?

- `create_spark()`：统一 Delta extension、UTC、ANSI mode、资源和 Windows runtime；
- `read_source()`：根据 JSON 配置读取 CSV/Parquet 和通配路径；
- `prepare()` 调用协议：所有数据集返回 accepted、rejected 和 metrics；
- `write_delta()`：支持输出路径、overwrite/append、partition columns 和目标文件数；
- `ingest_dataset()`：统一执行读取、prepare、两张表写入、读回计数和 metadata；
- `write_metadata()`：统一运行记录 Schema；
- `scripts/run_ingestion.py`：统一 CLI，支持 dataset、data directory、output root、
  run ID 和 Spark resource 参数。

这些组件不包含 Taxi、Weather 或 Air Quality 的业务判断，因此可以复用。

### Which components remain dataset-specific, and why?

- 源文件名、文件格式和 reader options；
- raw Schema 和 required columns；
- 原始列到标准列的映射；
- Taxi 本地时间、Weather UTC、Air GMT 字段的不同时间解析；
- 类型、单位、合法范围和 missing-value 规则；
- 主键/稳定 fingerprint 和 duplicate key；
- Air Quality 的 NYC scope、污染物和单位要求；
- 每个数据集的合理输出文件数。

这些内容依赖字段的业务语义。例如同一个负数对 trip distance 是非法值，对
fare amount 可能代表退款；同一个时间字符串在 Taxi 中按纽约当地时间解释，在
Air Quality 中优先使用 GMT 字段。因此不能用一个通用规则替代。

### How are transformation rules defined and maintained?

- `configs/datasets.json`：数据源、版本、时区、scope 和 duplicate key；
- `src/dic_pipeline/schemas.py`：raw Spark Schema 和 required columns；
- `src/dic_pipeline/transforms.py`：标准字段和时间/类型转换，
  `TRANSFORMERS[dataset]` 负责注册；
- `src/dic_pipeline/validation.py`：错误和质量标记，
  `RULE_BUILDERS[dataset]` 负责注册；
- `src/dic_pipeline/preparation.py`：按统一顺序调用以上组件并生成 metrics。

修改业务规则时只修改对应数据集函数，并更新 `rule_version`；不兼容的输出 Schema
变化需要更新 `schema_version`。A 的 reader/writer 不需要随每条业务规则变化。

### How are metadata managed?

`data/delta/metadata/ingestion_runs` 是不分区 Delta 表，每个 dataset/run 写一行：

- `run_id`、`dataset`；
- `started_at`、`finished_at`、`execution_seconds`；
- `raw_input_count`、`scope_excluded_count`、`input_count`；
- `accepted_count`、`rejected_count`、`duplicate_count`；
- `schema_version`、`rule_version`；
- `status`、`error_counts_json`、`quality_flag_counts_json`、`error_message`。

只有 accepted/rejected 写完并从 Delta 读回、确认行数与 B metrics 相等后才记录
`success`。普通 Python/Spark 异常会尝试记录 `failed`。如果 JVM 因进程崩溃或
断电完全不可用，则无法依靠同一 JVM 写失败记录，这是本地 W1 实现的限制。

最终全量运行 `role-a-final-20260908` 的结果：

| Dataset | Raw input | Scope excluded | Accepted | Rejected | Duplicate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Taxi | 9,554,778 | 0 | 9,554,576 | 202 | 1 |
| Weather | 8,784 | 0 | 8,784 | 0 | 0 |
| Air Quality | 8,139,551 | 8,087,666 | 51,885 | 0 | 0 |
| Taxi Zones | 265 | 0 | 265 | 0 | 0 |

### How does the design reduce duplication and maintenance?

读取、Spark 配置、Delta 写入、输出校验、metadata 和 CLI 各实现一次。新增数据集
不复制整条 pipeline，只注册配置、Schema、transform 和 validation。A 与 B 通过
`PreparationResult` 解耦：B 不负责存储，A 不重复实现业务质量规则。C 也可直接
复用 `write_delta()` 写 integrated 和 benchmark 表。

### What changes are required for 20 new datasets?

对于每个新数据集：

1. 在 `configs/datasets.json` 添加 `source_path`、`source_format`、reader
   options、版本、duplicate key 和业务配置；
2. 在 `RAW_SCHEMAS` 注册 raw Schema；
3. 在 `TRANSFORMERS` 注册一个 dataset-specific transformer；
4. 在 `RULE_BUILDERS` 注册 dataset-specific validation rules；
5. 添加针对关键边界值的小样本测试。

如果 Spark DataFrameReader 已支持格式，例如 CSV、Parquet、JSON 或 ORC，
`read_source()` 不需要修改，只配置 `source_format`。只有格式需要第三方
connector 或在 Spark 读取前进行特殊解码时，才增加专用 reader adapter；不是每个
数据集都创建独立 reader 文件。

### How to run

从零配置环境和 Windows 注意事项见 README 的“A：存储与通用摄入”。环境准备后：

```powershell
$env:PYTHONPATH = "src"
.\.venv\python.exe -m unittest -v
.\.venv\python.exe -m scripts.run_ingestion --dataset all `
  --driver-memory 6g --master "local[4]" --shuffle-partitions 128
```
