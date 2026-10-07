# WikiPulse B/C 接口 v1

更新：2026-10-07。用户已确认第一版方案；这是逻辑契约，尚未完成实现或技术验证。具体类型、Cassandra 物理表和未定参数见文末。

## 职责与调用关系

```text
C：归档批处理 → Cassandra 历史基线 → B：读取并执行异常检测
B：实时计算 → 调用 C 的写入模块 → Cassandra → C：看板
```

- B 负责流计算、异常判定，以及在自己的 Spark 作业内调用读写模块。
- C 负责 Cassandra 建表、字段映射、基线读取和结果写入模块；不额外增加交接服务。
- B 维护公共解析、过滤和回退识别函数，C 批处理复用。双方分别测试计算与存储交接。
- 第一版只展示水位线确认完成的窗口，不发布仍会变化的暂算排名或计数。

## 公共口径

- `wiki` 使用 wiki ID，例如 `enwiki`；时间统一 UTC，窗口为 `[window_start, window_end)`。
- 5 分钟窗口按整点对齐。分析过滤为 Wikipedia 域名、`type ∈ {edit,new}`、`namespace = 0`。
- `edit_count` 包含过滤后的 edit/new；bot/human 使用相同分析范围和分母。
- 过滤、去重、回退识别与窗口规则须在批流两侧一致；事件去重键仍待结合真实样本确定。
- 仅在确认采集覆盖完整的无事件窗口补零；采集缺口不等于零活动。
- 使用 proposal 的 2 分钟事件时间 watermark；这不保证墙钟时间严格延迟 2 分钟。无新数据和有限回放的尾部窗口完成方式需要验证。

## C → B：历史基线

每行逻辑键为 `(baseline_version, wiki, hour_of_day)`。

| 字段 | 含义 |
| --- | --- |
| `baseline_version` | 完整发布的基线版本 |
| `wiki` | wiki ID |
| `hour_of_day` | UTC 日内小时，0–23 |
| `edit_mean`、`edit_std` | 历史 5 分钟编辑数的均值、标准差 |
| `revert_mean`、`revert_std` | 历史 5 分钟回退数的均值、标准差 |
| `sample_count` | 有效历史 5 分钟窗口数量 |
| `history_start`、`history_end` | 历史数据范围，左闭右开 |

例如 `[10:15, 10:20)` 关联同 wiki 的 `hour_of_day=10`；比较的是 5 分钟计数，不是整小时总数。基线只能使用检测窗口之前的历史，回放同样遵守该限制。

第一版启动时指定已完整发布的版本，运行期间固定。C 每日生成新版本后，通过受控重启 B 作业切换；允许短暂停顿，checkpoint 恢复及重试时版本一致性须验证。发布完成的标识方式尚待确定。

B 按指标计算 `z_score = (observed_count - mean) / std`，`z_score > 3` 判为异常。无基线、样本不足或标准差为零时，`z_score` 和 `is_anomaly` 留空，由 `status` 标明原因；不将未检测记作正常。

## B → C：实时结果

所有结果包含 `wiki`、`window_start`、`window_end`。

| 结果 | 业务字段 |
| --- | --- |
| 热榜 | `title`、`edit_count`、`distinct_editors`、`score`、`rank` |
| 编辑战候选 | `title`、`revert_count`、`distinct_nonbot_editors`、交替回退的证据摘要（字段名/结构待定） |
| 5 分钟活动 | `edit_count`、`revert_count`、`bot_count`、`human_count` |
| 异常检测 | `metric`、`observed_count`、`baseline_version`、`z_score`、`is_anomaly`、`status` |

异常检测保存所有检测结果，看板筛选异常；无基线时也保留未检测记录。`baseline_version` 在没有可用版本时可为空。

热门条目使用 10 分钟窗口、1 分钟滑动；编辑战使用 30 分钟窗口，步长及交替判定待定；活动和异常检测使用 5 分钟窗口。详细分析关注 en/de/fr/zh/sv/ja，语言活动统计覆盖所有 Wikipedia。

写入使用稳定业务键，同一结果重试时覆盖，不累加计数。C 按查询需求设计实际分区键和聚簇键；普通 upsert 不等于已证明整条链路 exactly-once。Spark `foreachBatch` 默认只保证至少一次写入，需通过重试验收。[Spark 文档](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch)、[Cassandra 文档](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/ddl.html)

## 待定项与最小验收

- 确定 Spark/Cassandra 字段类型、物理表、完整主键、模块函数签名、`metric`/`status` 枚举；区分实时、历史和独立回放运行的输出。
- 确定基线历史长度、最低样本数、标准差使用总体或样本定义，以及完整发布和版本切换机制。
- 确定 Top-N、排名并列规则、机器人是否参与趋势评分、编辑战窗口步长和交替证据结构。
- 用同一小样本核对批流过滤、回退数、5 分钟边界及 bot/human 计数；另行说明流侧超水位线迟到数据与离线完整数据的差异。
- 核对缺基线、低样本、零标准差的空值/状态，以及正常和异常两类计算结果。
- 验证同一完成窗口重复写入不增加行或计数；重启恢复不遗漏结果，基线切换不会改变重试中的既有判定。
- 验证事件时间推进后窗口可输出，单独处理有限回放的尾部完成条件；看板明确显示窗口时间。

以上是后续 Phase 1 工作，不代表技术验证已通过；未固定技术栈版本。
