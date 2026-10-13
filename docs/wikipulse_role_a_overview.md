# WikiPulse 角色 A：职责、实现与后续工作

更新：2026-10-13。本文只讨论 Final Project：WikiPulse。W1–W4 的 Taxi 代码、数据和测试与本项目独立，WikiPulse 业务代码位于 `src/dic_pipeline/wikipulse/`。

## 1. A 负责什么

依据 [`task_plan.md`](../task_plan.md)，A 负责 Wikimedia EventStreams 接入、Kafka 原始输入、HDFS/Delta 全量原始归档、固定输入与限速回放、Docker Compose 主体、整体联调和 README 运行说明。A 提供 B/C 可复用的输入及归档契约、回放数据和启动入口；分析计算归 B，历史基线、Cassandra 结果存储和看板归 C。A 牵头最终集成，但不代替 B/C 实现其模块。

## 2. 当前已实现

| A 任务 | 实现位置 | 当前行为 |
| --- | --- | --- |
| SSE 采集与 Kafka 写入 | `src/dic_pipeline/wikipulse/producer.py`、`scripts/run_wikipulse_producer.py` | 接收未按项目过滤的 recentchange SSE；原始 `data` 写入 `wikipulse.raw.live`。优先用 `meta.id` 作 key，否则用原始 bytes 的 SHA-256；在 Kafka `acks=all` 后推进本地 SSE 状态。断线重连、限次采集和统计日志已实现。 |
| 原始归档 | `src/dic_pipeline/wikipulse/archive.py`、`scripts/run_wikipulse_archive.py` | Structured Streaming 读取 Kafka，原样保存 payload、key、headers 和 topic/partition/offset，并提取时间及来源字段。坏 JSON 和非 Wikipedia 事件照样保留。写入 HDFS Delta，按 Kafka 时间的 UTC 日期分区；checkpoint 支持重启恢复。 |
| 归档检查 | `scripts/inspect_wikipulse_archive.py`、`scripts/send_wikipulse_fixture.py` | 检查总行数、物理 key 数、坏 JSON fixture、Wikipedia 子集数量及 Delta 版本；可注入一条坏 JSON 测试保留行为。 |
| 固定版本限速回放 | `src/dic_pipeline/wikipulse/replay.py`、`scripts/run_wikipulse_replay.py` | 从指定 Delta 版本按 Kafka 时间和物理位置稳定排序，按给定速率写到独立 topic；保留原 key、payload、headers，附加回放来源字段并输出 manifest。 |
| 回放核对 | `scripts/check_wikipulse_replay.py` | 对有限回放逐条比对 payload、key、原始 headers 和来源位置，发现丢失、重复或内容变化则失败。 |
| 运行环境与契约 | `compose.wikipulse.yml`、`docker/wikipulse-spark.Dockerfile`、`requirements-wikipulse.txt`、`docs/wikipulse_a_contract.md`、`docs/wikipulse_a_runbook.md` | 配置 Kafka、Spark master/worker/client、HDFS NameNode/DataNode、Cassandra 基础服务；记录输入字段、保留策略和操作命令。Cassandra 目前只是服务容器，尚无结果表或看板。 |

原始层的物理记录键是 `(kafka_topic, kafka_partition, kafka_offset)`；`source_id` 是分析侧优先考虑的逻辑去重键。生产者和归档支持至少一次处理，不保证跨 SSE、Kafka、Spark 的端到端 exactly-once。SSE 状态只表示最后确认的 ID，Wikimedia 的恢复能力和长时间断线后的缺口仍需按日志评估。字段细节见 [A 输入契约](wikipulse_a_contract.md)。

## 3. 数据现在怎样流动

```text
Wikimedia EventStreams / recentchange
    |
    v
A: Python SSE producer --原始 data + 来源 headers--> Kafka: wikipulse.raw.live
    |                                                    |
    | 本地状态文件                                         +--> B: 流解析、过滤、去重与实时分析 [待实现]
    |                                                           |
    v                                                           v
data/wikipulse/producer_A.json                            C: Cassandra 结果表 [待实现]
                                                         --> C: 看板 [待实现]

Kafka: wikipulse.raw.live
    |
    v
A: Spark Structured Streaming 原始归档
    |
    +--> HDFS Delta: /wikipulse/raw_events
    |      checkpoint: /wikipulse/checkpoints/raw_live
    |      +--> C: Spark SQL 历史指标/基线 [待实现]
    |      +--> A: 固定 Delta 版本、可控速率回放
    |                  |
    |                  v
    |             Kafka: 独立 replay topic --> B/C 实验链路 [待联调]
    |
    +--> 保留所有 Wikimedia 事件和坏 JSON；不在 A 层做 Wikipedia 分析过滤
```

设计中的 B 分析范围是 `server_name` 以 `.wikipedia.org` 结尾、`type` 为 `edit/new`、`namespace=0` 的事件。B 负责流侧解析、过滤、回退识别、热门条目、编辑战候选、bot/human 指标及异常检测；C 使用相同规则做历史 5 分钟指标与基线，并提供 Cassandra 读写及展示。B/C 的第一版逻辑接口见 [B/C 契约](wikipulse_bc_contract.md)。上述 B/C 箭头表示计划中的数据流，不表示相应作业已经运行。

事件时间与 Kafka/接收时间用途不同：B/C 窗口应使用来源事件时间；A 的 `ingest_date_utc` 只用于归档分区。固定回放不改写原始事件时间，也不注入推进水位线的假事件。有限回放结束时的尾部窗口完成规则需要 B/C 在实验中确定。

## 4. 已验证到哪里

- `tests.test_wikipulse_a` 的 6 项本地测试通过：SSE 帧、key/时间、Kafka 确认后的状态、坏 JSON 和 Spark 归档投影。
- 2026-10-07 的真实 EventStreams 小样本：首次采集 100 条，重启后再采集 20 条；随后注入坏 JSON。Delta version 4 有 121 行、121 个物理 key；从 version 2 回放 20 条并逐条核对成功。
- Docker smoke：七个服务启动；新坏 JSON 经 Kafka/Spark 落入 HDFS Delta。version 5 有 122 行、122 个物理 key，坏 JSON 共 2 行（其中 1 行来自本次）；从 version 5 回放 5 条，manifest 为 `sent=5/source_rows=5`，校验脚本退出码 0、`verified=5`。
- 前一轮 producer 在 2026-10-13 00:50 记录累计发布 100,123 条、`bad_json=0`、16 次重连；进程退出后已于 00:54 从状态文件恢复并重新启动，00:57 新进程已发布 5,248 条、`bad_json=0`，累计约 105,371 条。代码已加入按进程隔离临时文件和有限替换重试。Kafka 与 Delta 当前累计数尚未检查，避免检查作业争用归档资源。
- 本机 Compose 使用忽略文件 `data/wikipulse/compose.local.yml` 将 HDFS UI 映射为 `19870:9870`；主 Compose 默认仍为 `9870:9870`。容器内 HDFS 使用 `8020`。

这证明 A 的采集、归档、回放链路能工作；尚未证明从实时分析到 Cassandra/看板的完整项目链路。121/122 行样本也不能外推全天流量、持续采集覆盖或长期存储增长。

## 5. A 后续还要做什么

1. **持续采集证据：**在正式实验前运行更长时间的 producer 与 archive，保留连接/中断日志、Kafka offset、Delta 版本、事件数量和磁盘增长；识别采集缺口。历史不足或覆盖不明时，不将无记录窗口解释为零活动。
2. **B/C 接口交接：**把 `raw_json`、原始 bytes、来源 ID、事件时间和物理 key 的实际样本交给 B/C；一起确认去重与坏记录处理。A 不在原始层提前过滤或去重。
3. **完整链路联调：**B/C 代码到位后，A 牵头从少量真实事件验证 producer → Kafka → B 分析 → C Cassandra → 看板，同时验证 C 从归档生成基线、B 读取固定基线版本。重点复核重启、重复事件和独立输出目录。
4. **复现实验支持：**为 C 固定 Delta 版本、回放 topic、run ID、输入行数、速率及 Kafka 分区数；共同记录 B 的 trigger interval、实际吞吐和尾部窗口规则。性能分析与报告由 C 主导。
5. **最终运行说明：**B/C 完成后，把其启动顺序、基线发布、结果查询、看板、停止和数据保留步骤整合进 WikiPulse README/runbook，再做一次全链路验收并记录限制。

目前 A 的核心代码已交付并通过 A 链路 smoke，producer 与归档仍在持续运行。A 还需保留采集覆盖、offset、Delta 版本及存储增长证据；第 3–5 项需要 B/C 模块到位才能完成。Cassandra 物理表、Grafana/备选看板、四类分析和历史基线不是 A 的独立实现任务。

## 6. 从哪里启动和测试

从仓库根目录执行本地 A smoke：设置 Python 3.11.9、JDK 21 的 `JAVA_HOME`、`PYTHONPATH=src`，运行 `.\\.venv\\Scripts\\python.exe -m unittest tests.test_wikipulse_a -v`。Docker 联调依次启动 Compose、创建 live topic、启动 archive，再启动 producer；归档版本检查、坏 JSON fixture、回放和逐条校验命令见 [A 运行手册](wikipulse_a_runbook.md)。宿主机 producer 连 `localhost:9092`，容器内 Spark 连 `kafka:29092`；回放须使用新 topic 和新 run ID。本机已通过 `data/wikipulse/compose.local.yml` 使用 `19870:9870` 映射 HDFS UI。不要使用 `docker compose down -v` 清理需要保留的历史数据卷。
