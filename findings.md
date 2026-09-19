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
