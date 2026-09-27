# 审核结论

总体架构和三人分工可采用；补充的三项方向正确，但还不足以直接冻结接口。以下修正已纳入[执行方案](task_plan.md)。

| 核查点 | 结论 |
| --- | --- |
| 课程覆盖 | 原方案覆盖 W1 六个 Task、五类交付物；本次只制定 W1 执行计划 |
| 整合表分区 | 必须明确策略；按天只是初始方案，当前规模可能产生小文件 |
| 天气聚合 | 实际已每小时一行，无需再聚合；通用 median/mode 规则不能覆盖风向、降水及并列众数 |
| 空气关联 | 除污染物，还需确认单位、测量口径、监测器重复及纽约适用站点 |
| 时间 | 区分源时间与统一存储时间；EPA Local Standard Time 与带夏令时的纽约时间不同 |
| 数据正确性 | 补充去重对账、右表唯一性、域外处理及分指标覆盖率 |
| 实验 | 补充相同 Zone 关联、完整执行聚合、缓存说明和当前快照文件统计 |

## 已检查的真实数据

来源：[老师共享目录](https://drive.google.com/drive/folders/1qjBtPVDepDE22j0axqrLVR0A2a969Qyy)。
- Taxi：目录有 2024 年 1–3 月三个 Parquet，合计 160,389,205 字节；本次未读取 Parquet Schema/全量记录。
- [Weather](https://drive.google.com/file/d/1q-Lw24XFqJ42XSJRuUV_ced3aJ6kKQ2M/view)：全文 8,784 行，2024 年全年小时键无重复；没有地点、站点或时区列。
- Weather 字段为年月日时及 `temp/rhum/prcp/snwd/wdir/wspd/wpgt/pres/cldc/coco`，每个指标附 `*_source`。来源包含 `isd_lite/metar/dwd_mosmix`；不能直接套 NOAA 原始格式。
- `snwd/wpgt` 全空，`prcp` 缺 553 行，`coco` 缺 6 行；其他核心数值字段无空字符串。尚未做完整业务数值校验。
- [Zone](https://drive.google.com/file/d/1-gO-MhXTIPbHRkyJQo8nTxly72gT3r9t/view)：全文 265 行，字段为 `LocationID/Borough/Zone/service_zone`；存在 EWR、Unknown、Outside of NYC。
- Air：确认 `air_quality.zip` 大小约 66 MB；当前连接器返回的文件引用无法直接在本机解包，压缩包内字段/污染物/覆盖范围仍待①检查。目录中的办公软件锁文件应忽略。

## 明确保留的假设

老师未另行规定环境或天气来源。依课程背景和用户判断，天气暂按纽约背景数据；具体站点未知。UTC 仅为暂定解析口径，不能用匹配率高来证明其正确。
字段和来源形式与 Meteostat 数据相似，但不足以确认下载参数；单位、时区和模型补值须记录依据。来源标记保留，避免把全部数据称为实测观测。

## 技术依据

- 环境统一为 Python 3.11、JDK 21、[PySpark 4.2.0](https://spark.apache.org/docs/4.2.0/)及 [delta-spark 4.4.0](https://pypi.org/project/delta-spark/4.4.0/)；[Delta 发布说明](https://github.com/delta-io/delta/releases/tag/v4.4.0)确认配套。本机 java/javac 21.0.7 已验证，Spark/Delta 尚未实测。
- [Delta 分区建议](https://docs.delta.io/best-practices/)提醒考虑每分区数据量；日期分区并非作业硬性要求。
- [EPA hourly 格式](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html)说明 GMT、本地标准时、POC、单位和质量标记；实际课程 Air 字段仍以解包结果为准。
- Spark 4.2 支持 `mode(..., deterministic=True)` 处理并列众数；[ANSI 模式默认开启](https://spark.apache.org/docs/4.2.0/sql-migration-guide.html)，校验需显式处理非法类型与时间转换。
- [Meteostat 数据来源](https://dev.meteostat.net/data/bulk/hourly)含模型替代数据；[小时 API](https://dev.meteostat.net/api/stations/hourly)允许指定时区/单位，因此不能凭字段名断言本文件使用默认参数。

## 2026-09-09 更新

上述 9 月 6 日的尚未读取/验证为历史状态；四表摄入与整合已完成全量验证，详见数据目录和 progress.md。Task 6 使用 Delta detail 的 numFiles/sizeInBytes 统计当前有效文件，避免把 CRC、日志和旧版本文件算入布局大小。

## 2026-09-19 W2 审核与实测结论

W2 的独立审查（W2-REVIEW-20260919）确认了 A/B 的五项问题，五项均已在 `c/pipeline-fixes` 修复并有回归测试覆盖；问题清单、证据和修复对照见 [progress.md](progress.md)，证据脚本在 `docs/review_evidence/w2-2026-09-19/`。下面只记录会影响后续判断的结论，不重复那份清单。

### 口径类

| 结论 | 依据 |
| --- | --- |
| 零订单小时必须由"已校验的数据覆盖窗口"界定，不能由首末订单推导 | 按 min/max 取边界会把首尾无订单的小时整段丢掉：完整一天的 Q3 只算出 4 小时；两个周一的 Q5 本应在 00/01 点并列，只返回 00 点 |
| 数据产品与规范查询必须共用同一份 SQL 片段，不能各写一遍 | 天气标签和统计范围分头维护时已经漂移：产品用原始 `coco` 且含域外行程，Q2-Q4 用分类标签且只取 NYC。现在两边都渲染同一个 `CASE`，并有对齐测试 |
| 产品版查询只在产品的键覆盖范围内等价 | `weather_impact_summary` 不按时间分键，所以 Q4 的产品版只对完整覆盖区间成立，窄区间必须回到基础查询 |

### 平台行为（实测，非文档推断）

- `collect()` 取回的 timestamp 是宿主本地时区的 naive datetime。W1 以来的 UTC 元数据偏移就出在这里；核对时间必须比对 epoch（`unix_micros`），不能比对 `collect()` 的结果。
- Spark 会把已缓存的计划替换进**任何**包含相同逻辑子计划的后续查询。缓存对照若按常规交替顺序测量，"未缓存"那一侧会被静默地从缓存读取。缓存实验因此先测基线再建缓存。
- Delta 自身会缓存 log-state RDD。缓存占用必须按构建前后的差值报告，否则会把 Delta 的内存算到实验头上。
- ANSI 模式下 `SEQUENCE` 起点大于终点会抛错，空区间需要 `CASE WHEN` 守护。

### 性能结论

- 该工作负载的瓶颈是 shuffle 与聚合，不是 I/O。证据是一正一反两项：Q4 关掉 AQE 慢 5.73 倍（128 个固定 shuffle 分区压垮一个只输出 259 行的查询），而缓存 Q4 的投影毫无收益且占 77 MB。
- 决定性的数据特征是坍缩比：955 万行程坍缩到 2,183 个小时、265 个 Zone、6 个天气类别，比输入小四到五个数量级。这个形状回报物化和分区合并，留给扫描层优化的空间很小。
- 四张产品合计 0.172 MB，占整合表的 0.016%，但全量刷新要 142.5 秒。物化的理由是重复出报表，不是单次查询变快。
- 以上均为单机、`local[4]`、单一数据量、每组三次测量的结果；十城扩展的建议是基于数据形状的外推。详见 [W2 benchmark report](docs/w2_benchmark_report.md)。

## 2026-09-24 W3 监控与评测设计依据

方案先经子 agent 对照作业、计划与全部相关代码审查；标注“实测”的条目由它在本机 Spark 4.2.0 / Delta 4.4.0 上验证，其余为代码阅读结论。

### 现有代码对增量更新的限制

| 发现 | 位置 | 影响 |
| --- | --- | --- |
| 有效上车时间窗口写死为 2024-01-01 05:00 至 2024-04-01 04:00（UTC） | `configs/datasets.json:10-11`，`validation.py:68-80` | 作业要求的新行程全部晚于原数据，会被整批标成 `timestamp_outside_source_period`；Q3-Q5 的日历窗口 `load_calendar_coverage` 读的也是它 |
| `mark_duplicate_rows` 只在同一 DataFrame 内开窗 | `validation.py:203-223` | 从原数据复制过来的 1-2% 重复行程发现不了，必须对目标表做 anti-join 或 MERGE |
| `verify_integrated_provenance` 要求整合表只有一个 `run_id` | `integration.py:138-153` | 增量追加后必然有多个，需要改成 lineage 子集检查 |
| `ingest_dataset` 进入时就删除 `completed_batch.json` | `ingestion.py` | 增量路径不能照搬，必须原子重写四表版本 |
| Taxi 的 `record_id` 对行值做哈希 | `transforms.py:69-99` | 修正过的行程得到新 id，只能算插入；`updated_count` 只对按小时分键的 Weather / Air 有意义 |

### Spark / Delta 行为（实测）

- 固定 Schema 读取多一列 `humidity` 的 CSV：`count()` 能通过（列裁剪跳过表头检查），`collect()` 报 `FAILED_READ_FILE`。只跑 `count()` 的冒烟测试会给出假阳性。
- Delta MERGE 的 `execute()` 直接返回 `num_inserted_rows` / `num_updated_rows` 等计数；同一文件再 MERGE 一次插入 0 行。增量阶段拿计数不需要额外扫描。
- `RESTORE` 能让快照回到基线，但不删除文件，版本号继续增长：玩具表的快照字节 838,571 → 923,770 → 838,571，磁盘字节 849 KB → 940 KB → 968 KB。用 RESTORE 在评测重复之间重置会污染存储数字，所以改为每次复制基线。
- `vacuum(0)` 默认被拒（`DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT`）。评测不在重复之间 VACUUM；生产环境按 7 天保留期回答存储问题。

### 采用的设计决定

| 决定 | 理由 |
| --- | --- |
| 一张 `pipeline_runs` 表，替换两张旧表，不做视图 | 没有 metastore，临时视图不持久；双写会虚增监控开销，也会有两个事实来源 |
| `duplicate_count` 只指“目标表已有而跳过”，批内重复算拒绝行 | W1 口径下重复是拒绝的子集，增量忽略的重复又不落盘，混在一列里守恒式不成立 |
| 普通函数 `run_row` + `record_run`，不用上下文管理器 | 现有调用点都有自己的 try/except；也符合 AGENTS.md 的“不做投机抽象” |
| 阶段失败时抛阶段异常并附 note；阶段成功但监控行丢失时抛 `MonitoringWriteError` | 监控错误不是业务失败的原因，不能用 `raise ... from`；成功后丢的行无法重算，不能只打日志 |
| 关掉监控只省掉行写入和只为监控取的元数据；按错误码计数照常执行 | 按码计数是 Task 4 要求的“报告无效记录”，属于校验；子 agent 建议把它算进监控，未采纳 |
| 每次评测运行用新路径 | 避免 Delta 按路径缓存的日志看到被换回旧版本的表（预防措施，未实测复现） |
| 去掉原方案的 `context` 列 | 评测都跑在复制目录里，监控行本来就不会进真实表；run_id 已带评测标记 |

### 真实数据上的监控结果

`--import-legacy` 导入的 W1/W2 历史：Taxi 拒绝 202 行（`dropoff_before_pickup` 180、`timestamp_outside_source_period` 21、`duplicate_record` 1）；最慢为 Taxi 摄入 156.8 秒，其次是 `daily_mobility_summary` 刷新 48.7 秒。每个目标只有一次历史运行，趋势查询要等评测或增量运行积累数据。

## 2026-09-27 W3 评测前审阅与全量试跑

- A 的更新生成器用 `collect()` 取回最大时间戳后当作 UTC，又踩了 W2 记录过的坑：`collect()` 给出的是宿主本地时区的 naive 时间。本机 UTC+8 上，Taxi 窗口终点晚 8 小时（`2024-07-01 12:00` 而不是 `04:00`），Weather/Air 的新小时从真实末尾之后 8 小时才开始，中间缺 8 小时；A 的机器 UTC+2，偏 2 小时。单测在 UTC 主机上发现不了，现改为按 `unix_micros` 读取，测试与 Spark 端 `date_format` 结果对比。
- 全量数据上 Taxi 更新：新行程 668,820（基线 9,554,576 的 7%），重复 143,319（1.5%），按 13 周平移。
- 用当前代码重建的基线与 W1 一致：Taxi 通过 9,554,576、拒绝 202；B 新增的 `missing_reference_record` 在原数据上为 0。摄入约 5.5 分钟，整合约 2.8 分钟，四张产品全量刷新约 2.9 分钟。
- 第一次全量评测（`20260926T164337Z-aeab3e72`，修复前代码）：增量更新中位数 143.9 秒（Taxi MERGE 33.9、Weather 15.7、Air 15.9、整合追加 49.3）；auto 刷新 130.7 秒反而比 full 的 84.9 秒慢 54%，两边产品内容哈希一致。原因是产品 DataFrame 是惰性的：增量路径的键检查、`count()`、新键计数和 MERGE 各自重新扫描 1,022 万行整合表并重算脏小时（每张小时产品 32–35 秒，full 只要 11–15 秒），full 路径也会算三遍。改为把产品结果和脏小时各物化一次（`localCheckpoint`），之后重跑全部评测。

### 最终评测的结论（`adf0508`）

- 监控开销按行数而不按数据量增长：单行 Delta 追加本身要 5–6.5 秒（会话内首次约 15 秒），把快照分区降到 1 只省约 1 秒，所以成本在提交本身（写作业与日志提交）。短阶段（产品刷新）因此多出 60%。一次运行的多行合并成一次提交可以把摄入和刷新各从 4 次提交降到 1 次。
- 小时产品的增量 MERGE 在这个规模不划算：找脏小时要读两个版本整合表的 `run_id`（约 2,000 万行），再按脏小时重算又扫一遍；全量重建只是一次聚合。要让增量刷新更快，需要不扫表就能定位新行（Delta change data feed 或分区谓词）。
- 小更新的 MERGE 会产生大量小文件：168 行写出 89 个文件。持续按日更新需要 `OPTIMIZE` 或在 MERGE 前合并分区。
