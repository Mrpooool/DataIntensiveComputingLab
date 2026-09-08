# Week 1 — Role A: Task 2 & Task 3

本文回答 `Assignment.md` 中 Week 1 的 Task 2 和 Task 3。

## Task 2: Design Your Storage Architecture

### 1. Directory structure

```text
data/
  raw/                         # 下载原文件；不可修改
  delta/
    standardized/              # 四张清洗、标准化后的 Delta 表
      taxi_trips/
      weather/
      air_quality/
      taxi_zone_lookup/
    rejected/                  # 各数据集未通过质量检查的记录
    metadata/
      ingestion_runs/          # 摄入行数、拒绝数、耗时、schema 版本
    integrated/
      integrated_taxi_trips/   # C 负责生成的整合表
  benchmark/                   # C 用于比较 Taxi 存储策略
```

`data/raw` 保留源文件，不在原文件上直接修改。`standardized` 提供统一接口给后续
integration；`rejected` 让错误记录可追踪；`metadata` 保存每次摄入的运行信息。

### 2. Delta table organization

每个数据集对应一个独立 Delta 表目录：

- `standardized/taxi_trips`
- `standardized/weather`
- `standardized/air_quality`
- `standardized/taxi_zone_lookup`
- `integrated/integrated_taxi_trips`
- `metadata/ingestion_runs`

Delta 表使用 Parquet 数据文件和 `_delta_log` 事务日志，提供 ACID 写入、Schema
信息和版本历史。原始文件仍放在 `raw`，不强制转换成 Delta。

### 3. Naming conventions

- 表名、目录名和标准列名统一使用 `snake_case`；
- 名称使用完整业务含义，例如 `taxi_zone_lookup`，避免难理解的缩写；
- 时间字段由 B 统一为明确名称，例如 `pickup_timestamp`、`pickup_hour_utc`；
- 原始列名只在 `raw` 层保留，标准表使用统一后的列名。

### 4. Partitioning strategy

| 表 | 初始策略 | 原因 |
| --- | --- | --- |
| Taxi standardized | 不分区 | 当前三个文件合计约 160 MB，按天分区可能产生大量小文件 |
| Weather | 不分区 | 全年只有 8,784 行 |
| Air Quality | 不分区 | 标准化/地域筛选后的实际大小需先测量 |
| Taxi Zone Lookup | 不分区 | 265 行，是小型 lookup |
| Integrated Taxi Trips | 初始按 `pickup_date` | 表较宽，后续查询经常按日期过滤；写完后检查每个分区大小 |

Task 6 单独建立 Taxi 的“不分区”和“按 `pickup_date` 分区”副本进行实验，不预先
假设按天分区一定更快。

### 5. Which datasets should conceptually be lookup tables?

`taxi_zone_lookup` 是 lookup table。它把 `LocationID` 映射到 Borough、Zone 和
service zone，记录少、变化慢，并由 Taxi 表的上下车 Location ID 引用。

Weather 和 Air Quality 是按小时增长的观测事实，不是 lookup table；Taxi Trips
是主要事实表。

### 6. Which datasets should not be partitioned?

- `taxi_zone_lookup`：数据太小，分区只会增加目录和文件开销；
- Weather：只有 8,784 行，全表扫描成本很低；
- ingestion metadata 和 rejected 小表：初期体量小，没有分区收益；
- 当前 Taxi 标准表先不分区，以便 Task 6 与日期分区版本公平比较。

### 7. Which datasets require different partitioning strategies?

Taxi 和 Integrated 是大事实表，可能需要按时间分区；Weather、Zone 是小表，应
保持不分区；Air Quality 的策略取决于过滤后的体量和查询方式，规模增大后可考虑
按日期或月份分区。

### 8. When does partitioning become harmful?

以下情况分区会有害：

- 分区字段基数太高，产生很多只有少量记录的小文件；
- 表本身很小；
- 查询不使用分区字段过滤，无法进行 partition pruning；
- 每次写入同时修改大量分区，增加提交和元数据成本；
- 单个分区过小，调度任务的开销超过实际读取成本。

### 9. What changes if data volume grows by 20×?

- 改为增量摄入，不再每次重写全部历史数据；
- 根据查询范围选择按月或按日分区；
- 定期合并小文件；
- 监控每个分区大小、文件数和数据倾斜；
- 将存储路径改为 HDFS/对象存储时，保留相同 reader/writer 接口；
- 对 Air Quality 建立按地域和时间过滤后的分析表，但仍保留完整 raw 数据。

## Task 3: Build a Generic Ingestion Framework

### 实现流程

`src/ingestion.py` 对所有数据集使用同一流程：

```text
读取 CSV/Parquet
  -> 检查 required_columns
  -> 列名转 snake_case
  -> 调用 dataset-specific transform
  -> 检查 input = accepted + rejected
  -> 写 standardized/rejected Delta 表
  -> 写 ingestion metadata
```

配置集中在 `configs/datasets.py`，运行入口是 `scripts/run_ingestion.py`。

### Assignment 要求对应

| Assignment 要求 | 实现位置 |
| --- | --- |
| 加载 CSV 和 Parquet | `read_dataset()` 根据配置选择 Spark reader |
| 验证输入 Schema | 读取后检查 `required_columns` |
| 标准化列名 | `snake_case()` |
| 统一 timestamp / 类型 | B 的 dataset-specific transform |
| 应用专用转换规则 | `TRANSFORMS[dataset]` |
| 重复、空主键、非法时间/数值 | B 的 transform 生成 accepted/rejected |
| 存为 Delta | `ingest_dataset()` 写 `format("delta")` |
| 生成 ingestion metadata | 记录 processed、rejected、耗时、schema version |

### 1. Which components are generic and reusable?

通用部分包括：

- 根据配置读取 CSV/Parquet；
- 检查必需字段；
- 将列名转为 `snake_case`；
- accepted/rejected 行数对账；
- 写标准表、拒绝表和 metadata Delta 表；
- 统一 Spark 配置和运行入口。

这些步骤不依赖某个数据集的业务含义，可以复用于新增数据集。

### 2. Which components remain dataset-specific, and why?

以下内容必须是数据集专用的：

- 不同源字段到统一字段的映射；
- Taxi、Weather、Air Quality 的时间解析方式；
- 各字段的数据类型和合法范围；
- 主键/去重字段；
- Taxi 负距离、非法时长等规则；
- Air Quality 的站点、污染物、单位和质量标记规则。

原因是这些规则依赖字段语义，无法用一个通用函数正确处理所有数据集。

### 3. How are transformation rules defined and maintained?

B 在 `src/transforms.py` 中用一个字典维护数据集与转换函数的关系：

```python
TRANSFORMS = {
    "taxi_trips": transform_taxi,
    "weather": transform_weather,
    "air_quality": transform_air_quality,
    "taxi_zone_lookup": transform_zones,
}
```

新增或修改规则只改对应函数，不改通用 ingestion 流程。Schema 和规则发生不兼容
变化时更新 `SCHEMA_VERSION`。

### 4. How are metadata managed?

`data/delta/metadata/ingestion_runs` 是 Delta 表，每次成功摄入追加一行，包含：

- dataset；
- ingestion timestamp；
- processed records；
- rejected records；
- execution time；
- schema version。

因此可以比较数据集规模、拒绝数量和执行时间，并追踪使用的 Schema 版本。

### 5. How does the design reduce duplication and maintenance?

读取、Schema 检查、命名、对账、Delta 写入和 metadata 只实现一次。每个数据集
只提供自己的配置和 transform，不需要复制整条 pipeline。输出路径和格式也集中在
`configs/datasets.py`，避免散落硬编码。

### 6. What changes are needed for 20 new datasets?

每个新数据集只需：

1. 在 `DATASETS` 中增加路径、格式、必需字段和 reader options；
2. 增加一个 dataset-specific transform；
3. 将函数注册进 `TRANSFORMS`。

若新数据仍为 CSV 或 Parquet，通用框架无需修改；只有出现新的文件格式时才需要在
`read_dataset()` 中增加对应 reader。
