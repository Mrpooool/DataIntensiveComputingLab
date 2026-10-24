# WikiPulse B/C 接口 v2

更新：2026-10-13。v1 的逻辑口径不变，本版补齐字段类型、枚举、Cassandra 物理表、模块函数和阈值。规则已用 A 的交接快照（`wikipulse_bc_handoff_20261013_0800_compact`，Delta version 0，事件时间 2026-10-12 21:48:50 至 10-13 05:59:59 UTC）核对，核对结果见文末。`events.py`、`store.py` 和基线批处理已实现并实测（见 [C 运行手册](wikipulse_c_runbook.md)）；看板尚待验证。

## 职责与调用关系

```text
C：归档批处理 → Cassandra 历史基线 → B：读取并执行异常检测
B：实时计算 → 调用 C 的写入模块 → Cassandra → C：看板
```

- B 负责流计算、异常判定，以及在自己的 Spark 作业内调用 C 的读写模块；不额外增加交接服务。
- 公共解析、过滤、去重和回退识别在 `src/dic_pipeline/wikipulse/events.py`，C 先实现，B 评审后批流共用。修改规则须同时更新本文件和该模块的测试。
- 只发布水位线确认完成的窗口，不发布仍会变化的暂算排名或计数。

## 公共口径（`events.py`）

### 解析

输入 DataFrame 须有字符串列 `raw_json`。A 的归档表已有该列；B 从 Kafka 读取时用 `CAST(value AS STRING) AS raw_json`。`parse_events(frame)` 保留输入的全部列（同名列被覆盖），追加：

| 列 | 类型 | 来源 |
| --- | --- | --- |
| `source_id` | string | `meta.id` |
| `event_time` | timestamp | `timestamp`（epoch 秒），不用 `meta.dt` 或 Kafka 时间 |
| `wiki`、`server_name`、`type`、`title`、`user`、`comment` | string | 同名字段；`comment` 缺失时为空串 |
| `namespace` | long | 同名字段 |
| `is_bot`、`minor` | boolean | `bot`、`minor` |
| `bytes_changed` | long | `length.new - length.old`，新建页面的 `old` 按 0 |
| `revision_new` | long | `revision.new`，同一秒内的排序依据 |
| `is_revert` | boolean | `type = 'edit'` 且 `comment` 匹配回退关键词 |

坏 JSON 解析为空值，由范围过滤剔除。

### 范围与去重

`analysis_events(frame)` = 解析 → 范围过滤 → 去重，是批流的统一入口：

- 范围：`server_name` 以 `.wikipedia.org` 结尾、`type ∈ {edit,new}`、`namespace = 0`，且 `source_id`、`event_time`、`wiki`、`title` 非空。
- 去重键 `source_id`。批处理 `dropDuplicates`；流处理先 `withWatermark("event_time", "2 minutes")` 再 `dropDuplicatesWithinWatermark`。B 的后续窗口聚合沿用这一 watermark，不再另设。
- `user` 为空（被隐藏的用户）的行保留并计入编辑数，但不计入编辑人数和编辑战用户序列。

### 回退关键词

不区分大小写，按 Unicode 词边界匹配；中日文按子串匹配。所有 wiki 共用一个并集：

| 语言 | 关键词 | 实测 |
| --- | --- | --- |
| en | `revert*`、`rv`、`rvv`、`undid`、`undo`、`rollback` | 已核对；`undo` 有少量误报 |
| de | `zurückgesetzt`、`rückgängig` | 已核对 |
| fr | `annulation`、`révocation`、`révoqu*` | 已核对；不用 `révo*`，避免匹配 “révolution” |
| zh | `回退`、`撤销`、`撤銷`、`还原`、`還原` | 回退、撤销已核对；还原未命中 |
| sv | `återställ*`、`ångra*` | **未经实测验证**：样本 426 次编辑零命中 |
| ja | `取り消し`、`差し戻し`、`巻き戻し` | 已核对 |

### 时间与范围

- UTC，窗口左闭右开 `[window_start, window_end)`，用 Spark `window()` 对齐 epoch，即整点对齐。
- 热门条目和编辑战只算 `FOCUS_WIKIS` = en/de/fr/zh/sv/ja；5 分钟活动和异常检测覆盖全部 Wikipedia。
- 所有流聚合用 append 模式，只输出 watermark 越过 `window_end` 的窗口；超过 watermark 的迟到事件丢弃，与批处理的差异在报告中说明。
- 有限回放不注入假事件。比对与实验只使用 `window_end ≤ 已观测最大事件时间 − 2 分钟` 的窗口，尾部未完成窗口不计。

## B → C：实时结果

B 在 `foreachBatch` 内调用 `store.write_rows(batch_df, table, keyspace=...)`。B 只提供下表的业务列；`day`（`window_start` 的 UTC 日期）和 `written_at`（写入时刻）由模块添加。同一主键重复写入覆盖，不累加。

### `hot_pages`：热门条目

- 窗口 10 分钟、步长 1 分钟；只算 `FOCUS_WIKIS`，**排除 `is_bot` 编辑**；按 `(wiki, title)` 聚合。
- `edit_count` = 编辑数；`distinct_editors` = `size(collect_set(user))`（流聚合不支持 `countDistinct`）。
- `score = edit_count * ln(1 + distinct_editors)`，自然对数。
- 每个 `(wiki, window)` 在 `foreachBatch` 内排名，顺序为 `score` 降序、`edit_count` 降序、`title` 升序，取 `row_number ≤ 10`。

列：`wiki`、`window_start`、`window_end`、`rank` int、`title`、`edit_count` int、`distinct_editors` int、`score` double。

### `edit_war_candidates`：编辑战候选

- 窗口 30 分钟、步长 5 分钟；只算 `FOCUS_WIKIS`，只取 `is_revert` 且非 `is_bot`、`user` 非空的事件；按 `(wiki, title)` 聚合。
- 聚合中收集 `collect_list(struct(event_time, revision_new, user))`，在 `foreachBatch` 内按 `(event_time, revision_new)` 排序，得到回退用户序列。
- 判定须同时满足：
  - 回退次数 ≥ 3；
  - 不同回退用户 ≥ 2；
  - **交替**：序列中存在某一位，它与前一位不同，且在更早位置出现过，即出现 A → 他人 → A，中间可以隔其他回退。
- 同一冲突会出现在多个重叠窗口中，看板按 `(wiki, title)` 取最新窗口。

列：`wiki`、`window_start`、`window_end`、`title`、`revert_count` int、`distinct_nonbot_editors` int、`reverter_sequence` list<string>、`user_switches` int（相邻换人次数）、`first_revert_at` timestamp、`last_revert_at` timestamp。

### `activity_5m`：5 分钟活动

- 5 分钟滚动窗口，全部 Wikipedia，按 `wiki` 聚合。
- `edit_count` = 全部编辑数；`revert_count` = `is_revert` 数，含机器人；`bot_count` / `human_count` 按 `is_bot` 划分，两者之和等于 `edit_count`。

列：`wiki`、`window_start`、`window_end`、`edit_count` int、`revert_count` int、`bot_count` int、`human_count` int。

### `anomalies`：异常检测

- 输入是 `activity_5m` 的同一组窗口，`metric ∈ {edit_count, revert_count}`，用 `(wiki, hour(window_start))` 关联基线。
- `status` 依次判定：
  - 无基线行 → `no_baseline`；
  - `sample_count < 24` → `insufficient_samples`；
  - 该指标标准差为 0 → `zero_std`；
  - 否则 → `ok`。
- 只有 `ok` 时才计算 `z_score = (observed_count − mean) / std` 和 `is_anomaly = z_score > 3`，其余状态两列留空。所有检测结果都写入。
- 流侧只对有事件的窗口检测，而基线包含补零窗口。由于只判定高于基线的异常，这一差异不影响结论。

列：`wiki`、`window_start`、`window_end`、`metric`、`observed_count` int、`baseline_version` string（可空）、`z_score` double（可空）、`is_anomaly` boolean（可空）、`status`。

## C → B：历史基线

B 启动时以参数指定 `baseline_version`，调用 `store.read_baseline(spark, version, keyspace="wikipulse")` 取得 DataFrame，运行期间固定。版本必须已在 `baseline_versions` 中发布，否则函数报错。C 发布新版本后，通过受控重启 B 作业切换。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `baseline_version` | string | 历史结束时刻，格式 `20261013T0600Z` |
| `wiki` | string | wiki ID |
| `hour_of_day` | int | UTC 0–23 |
| `edit_mean`、`edit_std`、`revert_mean`、`revert_std` | double | 历史 5 分钟计数的均值和样本标准差（`stddev_samp`） |
| `sample_count` | int | 有效 5 分钟窗口数 |
| `history_start`、`history_end` | timestamp | 历史范围，左闭右开 |

C 的生成规则如下：

- 输入是 A 归档的一个固定 Delta 版本，经 `analysis_events` 处理。
- 历史取 `history_end` 之前最近 7 天；数据不足 7 天时用全部可用数据。
- 采集缺口用全部原始事件的事件时间判断：相邻事件间隔超过 60 秒视为缺口，跨越历史起止边界的间隔同样计入。与缺口重叠的 5 分钟窗口、首尾不完整的窗口都不计入。其余窗口里，没有事件的 wiki 记为 0。
- 先写完 `baseline_by_hour`，最后写 `baseline_versions` 一行，这一行存在就表示已发布。已发布的版本不可变：同名版本只允许用相同的归档路径和 Delta 版本重跑，换了输入就报错，这样重启后的 B 不会在同一个版本名下读到变化的统计值。
- 回放实验使用的基线，其 `history_end` 必须不晚于回放数据的起点。

## Cassandra 物理表

- keyspace：实时结果和基线都在 `wikipulse`；每次回放实验写入独立的 `wikipulse_replay_<run_id>`，`run_id` 只能含 `[a-z0-9_]`。
- 连接地址：容器内 `cassandra:9042`，宿主机 `localhost:9042`。

| 表 | 主键 | 主要读取方式 |
| --- | --- | --- |
| `hot_pages` | `((wiki, day), window_end, rank)`，`window_end DESC` | 某 wiki 最新窗口 Top 10 |
| `edit_war_candidates` | `((wiki, day), window_end, title)`，`window_end DESC` | 某 wiki 当天候选 |
| `activity_5m` | `((day), wiki, window_start)` | 单 wiki 时间序列、全语言热力图 |
| `anomalies` | `((day), wiki, window_start, metric)` | 当天异常、单 wiki 检测记录 |
| `baseline_by_hour` | `((baseline_version), wiki, hour_of_day)` | B 启动时整版读取 |
| `baseline_versions` | `(baseline_version)` | 发布标识，另含 `history_start`、`history_end`、`source_archive`、`source_version`、`row_count`、`published_at` |

## 交接快照核对结果（2026-10-13）

- **去重**：12 组重复 `source_id` 都是相邻 Kafka offset，相隔 8–11 秒，payload 相同，属于 producer 重试；其中 2 条是正文事件。2 分钟 watermark 内去重足够。
- **迟到**：正文事件相对已到达最大事件时间的迟到程度 p99 为 11 秒、最大 87 秒，超过 120 秒为 0 条。另外，事件时间到 Kafka 写入的中位延迟约 148 秒，这属于来源延迟，不计入系统延迟。
- **缺口**：Kafka 时间上有一段 181 秒空档（22:51–22:54 UTC），但 `Last-Event-ID` 重连已补回，事件时间上无超过 20 秒的空档。原始流混有少量 2011–2024 年的旧时间戳，因此缺口检测须限定在快照时间范围内。
- **规模**：正文 edit/new 共 105,478 条，来自 214 个 Wikipedia，机器人占 43%，4 行 `user` 为空。en 平均每 5 分钟约 12 次回退，其他重点语言不足 1 次，小语言的回退检测多会落入 `zero_std` 或 `insufficient_samples`。
- **编辑战**：非机器人回退共 1,319 次。按上述规则 8.2 小时内约 4 个候选条目；只要求“换人 ≥ 2 次”时为 9 个，多出的 5 个是不同巡查员各自回退一次。frwiki 的反破坏机器人 Salebot 没有 bot 标记，会被计为真人，不做特殊处理。
- 8.2 小时历史中每个日内小时最多 12 个窗口，低于 24 的最低样本数，因此全部基线行都会是 `insufficient_samples`。可靠基线需要 A 的多日导出。

## 待定与验收

- Cassandra 写入方式：spark-cassandra-connector 截至 2026-10 没有 Spark 4 正式版（最新 3.5.1 只支持 Spark 3.5），因此 `store` 在 driver 端用 Python `cassandra-driver` 3.30.1 执行 prepared INSERT。已在宿主机（Windows）和 `spark-client` 容器（Linux，经主机名 `cassandra`）实测写入、重复写覆盖、UTC `day` 和基线读取。B 的 `foreachBatch` 端到端调用待 B 的作业联调。
- 看板：先试 Grafana 12.x 加 `hadesarchitect-cassandra-datasource` 3.3.0（社区签名，用 `GF_PLUGINS_PREINSTALL` 安装）。该插件的时序查询须命中分区键，并用 `$__timeFrom`/`$__timeTo` 过滤聚簇的时间列；上表主键已按此设计。插件不可用时改用 Streamlit。
- 性能实验的延迟起点和终点、吞吐指标：由 C 在实验设计中确定。
- 验收要求：
  - 用同一小样本核对批流的过滤、去重、回退、5 分钟边界和 bot/human 计数；
  - 核对缺基线、低样本、零标准差的状态；
  - 同一完成窗口重复写入不增加行；
  - 重启恢复不遗漏结果，基线切换不改变重试中已有的判定。
