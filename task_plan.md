# W1 执行方案

依据：[课程要求](Assignment.md)；[审核记录](findings.md)。范围仅为 W1，当前尚未实现平台。

## 1. 统一决策

- 一个 Python + PySpark + Delta 项目。统一采用 Python 3.11.9、JDK 21、PySpark 4.2.0、delta-spark 4.4.0；已固定本机安装的直接及间接依赖，`pip check` 通过。A 下一步验证 Delta 写入、读回与查询。
- 四份原始数据 → 通用导入 → 四张标准 Delta 表 → 小时环境关联 → `integrated_taxi_trips`。
- 配置集中管理路径与规则；用普通函数和数据集处理函数映射表组织代码。
- W1 采用全量批处理；重复运行替换指定输出，不累计追加。成功记录在所有输出校验后写入；失败运行不得作为 W2 输入。

| 表 | 初始布局 |
| --- | --- |
| Taxi 标准表 | 不分区；另建按 `pickup_date` 分区的实验副本 |
| Weather、Air Quality、Zone 标准表 | 不分区；保留有效源数据，地域筛选在关联阶段执行 |
| 整合表 | 保留纽约当地 `pickup_date`，初始按天分区；检查实际文件大小后调整并记录 |
| rejected、运行统计 | 小表不分区 |

数据目录统一为 `data/raw/`、`data/delta/{standardized,integrated,rejected,metadata}/`、`data/benchmark/`。
Taxi 源文件合计约 160 MB，按天分区可能过细；Taxi 实验结论也不能直接证明更宽的整合表布局最优。

## 2. 三人推进顺序

以下按五个工作阶段安排，可根据课期伸缩；每阶段先满足完成标志。

| 阶段 | A：环境与流程 | B：数据与规则 | C：关联与实验 | 完成标志 |
| --- | --- | --- | --- | --- |
| ① 起步 | 固定环境、输入文件清单；忽略锁文件，解包 Air | 检查四份真实 Schema/覆盖范围，填写 Catalog 六项，制定 contract v1 | 核对关联键、环境聚合规则；制作边界样例 | 环境写读通过；字段、单位、时间假设、唯一键已记录 |
| ② 并行 | 实现读取、写入、入口、日志及存储切换 | 实现标准化、校验、去重及问题记录 | 按 contract 实现小时汇总、左连接、三个 SQL 和计时框架 | 各模块用小样本通过验收 |
| ③ 联调 | 串起四份输入到五张表 | 核对统计、拒绝原因和 Schema | 检查行数、唯一键、环境和区域命中 | 小样本从原始文件到整合表完整跑通 |
| ④ 全量 | 固定同一机器/资源和配置，执行两种布局 | 复核全量质量统计 | 执行 benchmark，保存结果和计划 | 指定三个查询结果一致，四类指标齐全 |
| ⑤ 交付 | 汇总 README、架构图与流程说明 | 完成 Catalog、转换说明 | 完成关联策略和 benchmark report | 换一个干净输出目录，按 README 可复现 |

①完成前，C 可以写样例和框架；真实数据正式关联以 contract v1 为门槛。未知来源允许记录显式假设，不伪装成已核实事实。
A 管 `io/ingestion/cli`，B 管 `schemas/transforms/validation`，C 管 `integration/benchmark`；各自负责对应测试，公共配置由 A 汇总。

## 3. 三个交接接口

| 负责人 | 接口约定 | 输出 |
| --- | --- | --- |
| B → A | `prepare(df, dataset_config)` | accepted、rejected、metrics |
| A → C | 四张同一成功批次的标准 Delta 表 | 配置中的路径、Schema 版本 |
| C → A | `integrate(taxi, weather, air, zones, config)` | 整合 DataFrame、匹配统计；A 的公共 writer 落盘 |

`contracts.md` 只保留必要字段表：原始列 → 标准列、类型、单位、空值、粒度、唯一键、转换规则、关联字段和来源假设。
- Taxi 至少包含稳定记录键、上下车时间/地点、距离、车费、`pickup_date`、`pickup_hour_utc`、`trip_duration_seconds`；具体源列以 Parquet 为准。
- 无业务主键时采用固定字段序列的稳定指纹；排除批次号、文件路径和环境字段。承认“完全相同记录视为重复”的业务局限，固定指纹规则版本。
- 成功批次满足：输入数 = accepted 数 + rejected 数。重复丢弃计入 rejected，另记 duplicate 数；一条记录多个错误仍只计一条。
- rejected 保留原始值、原因数组、来源文件、批次号。可疑负车费、零距离等先确认含义；环境缺失不拒绝 Taxi。
- metadata 至少记录 run_id、dataset、起止/耗时、输入/通过/拒绝/重复数、schema_version、rule_version、状态。

## 4. 时间与关联规则

- Spark 会话设 UTC，显式固定 `spark.sql.ansi.enabled=true`；非法转换须记录拒绝原因。存储 UTC 时间戳，日期分析使用 `America/New_York`。Taxi 无时区值暂按纽约当地时间，Weather 暂按 UTC；B 保留原始时间并做跨日、夏令时样例核验。
- 若 Air 是 EPA hourly 格式，优先用 `Date GMT + Time GMT`；Local Standard Time 不可直接按纽约夏令时转换。真实格式、单位及质量标记必须在①确认。
- 当前 Weather 每小时一行，直接使用；不虚构站点，不把 `*_source` 当站点列。保留来源标记；`snwd/wpgt` 全空，`prcp/coco` 可空。
- Air 先选纽约五区适用站点；仅合并同污染物、统一单位、相容测量口径的数据。按监测器/站点处理重复，再形成站点小时值，最后对站点小时值取 median；各污染物分列，使每小时最多一行。
- 如果未来 Weather 出现多站点，标量可取 median；天气类别用 `mode(..., deterministic=True)`，并列取最小代码。风向、累计降水需专门规则，不能一律取 median。
- 对 `pickup_hour_utc` 做同小时左连接；Zone 用上下车 ID 分别左连接。先检查所有右表关联键唯一，禁止关联后再去重掩盖行数膨胀。
- 纽约环境仅用于上车点属于纽约五区的行程；域外/未知地点及缺测仍保留 Taxi，环境列为 null。说明区域背景值的空间局限。

验收：整合行数 = accepted Taxi 行数，记录键非空且唯一；分别报告天气、各污染物、上下车 Zone 的匹配率。
分母统一为 accepted Taxi 行数，另报 NYC 范围内覆盖率；“小时记录存在”与“指标非空”分开统计。

## 5. Benchmark 固定口径

S0/S1 使用同一份 Taxi、同一清洗逻辑与列，仅改变是否按 `pickup_date` 分区。

| 查询 | 统一含义 |
| --- | --- |
| 各 borough 行程数 | Taxi 左连接同一 Zone 表，按 pickup borough 分组 |
| 每日平均行程时间 | 按纽约当地 pickup_date，平均 trip_duration_seconds |
| 各 borough 平均车费 | 同样的 pickup borough；平均 fare_amount，不替换为 total_amount |
| 辅助日期范围查询 | 可加固定日期区间并检查分区裁剪，不替代上述三个查询 |

- 同一机器、资源、Spark 配置、压缩与缓存策略；同一输入清单。每轮写入新实验目录，导入计时覆盖 raw 读取、转换/校验到 Delta 提交完成。
- 查询预热一次后各测三次，交替 S0/S1 顺序，保留原始耗时和中位数；使用 `collect()` 完整计算这些小型聚合结果，不能只对结果 `count()`。
- 不缓存 Taxi 查询输入；记录 OS 缓存不可控，不能声称是冷缓存实验。AQE 等设置保持相同，W2 再单独研究。
- 记录导入时间、查询时间、当前 Delta 快照的数据字节数与文件数；历史废文件、日志另计，不能混入布局比较。
- 先比较结果：计数精确相等，浮点均值用预先固定的容差；保存 SQL 和 `EXPLAIN FORMATTED`。不得预设按天分区更快。

## 6. W1 交付检查

- [ ] Task 1：四份 Catalog 均回答实体、主键、关联、时间、类别、增长属性六项。
- [ ] Task 2：解释 lookup、小表不分区、不同布局、小文件及数据增长 20× 后的调整。
- [ ] Task 3–4：统一入口处理 CSV/Parquet；验证 Schema/数据质量、记录全部转换与统计；说明增加 20 个数据集需要什么改动。
- [ ] Task 5：五张 Delta 表可读，行程不增不减、键唯一；缺测、域外、重复右键、跨日/夏令时样例均有验收。
- [ ] Task 6：两种 Taxi 布局、三个指定查询、四类指标、结果一致性证据齐全。
- [ ] 提交完整代码/配置/测试、3–5 页设计报告、架构图、短 benchmark report、README。

W2 保留逐趟明细和四张基础表；W3 保留稳定键、版本及批次追踪；W4 保留原始文件与来源。整小时环境汇总可能含上车后信息，预测任务须另做时间泄漏检查。
