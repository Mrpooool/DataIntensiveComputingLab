# WikiPulse 角色 A：职责与实现概览

更新：2026-10-25。A 的采集、归档、回放与 Compose 主体已交付；B 的流分析与 C 的基线、Cassandra、看板和实验已于 2026-10-24 完成并联调。完整运行顺序见 [README](../README.md#running-wikipulse-end-to-end)，当前状态见 [progress.md](../progress.md)。本文只讨论 Final Project：WikiPulse。W1–W4 的 Taxi 代码、数据和测试与本项目独立，WikiPulse 业务代码位于 `src/dic_pipeline/wikipulse/`。

## 1. A 负责什么

依据 [`task_plan.md`](../task_plan.md)，A 负责 Wikimedia EventStreams 接入、Kafka 原始输入、HDFS/Delta 全量原始归档、固定输入与限速回放、Docker Compose 主体、整体联调和 README 运行说明。A 提供 B/C 可复用的输入及归档契约、回放数据和启动入口；分析计算归 B，历史基线、Cassandra 结果存储和看板归 C。A 牵头最终集成，但不代替 B/C 实现其模块。

## 2. 已实现

| A 任务 | 实现位置 | 当前行为 |
| --- | --- | --- |
| SSE 采集与 Kafka 写入 | `src/dic_pipeline/wikipulse/producer.py`、`scripts/run_wikipulse_producer.py` | 接收未按项目过滤的 recentchange SSE；原始 `data` 写入 `wikipulse.raw.live`。优先用 `meta.id` 作 key，否则用原始 bytes 的 SHA-256；在 Kafka `acks=all` 后推进本地 SSE 状态。断线重连、限次采集、分组确认和统计日志已实现。 |
| 原始归档 | `src/dic_pipeline/wikipulse/archive.py`、`scripts/run_wikipulse_archive.py` | Structured Streaming 读取 Kafka，原样保存 payload、key、headers 和 topic/partition/offset，并提取时间及来源字段。坏 JSON 和非 Wikipedia 事件照样保留。写入 HDFS Delta，按 Kafka 时间的 UTC 日期分区；checkpoint 支持重启恢复。 |
| 归档检查 | `scripts/inspect_wikipulse_archive.py`、`scripts/send_wikipulse_fixture.py` | 检查总行数、物理 key 数、坏 JSON fixture、Wikipedia 子集数量及 Delta 版本；可注入一条坏 JSON 测试保留行为。 |
| 固定版本限速回放 | `src/dic_pipeline/wikipulse/replay.py`、`scripts/run_wikipulse_replay.py` | 从指定 Delta 版本按 Kafka 时间和物理位置稳定排序，按给定速率写到独立 topic；保留原 key、payload、headers，附加回放来源字段并输出 manifest。 |
| 回放核对 | `scripts/check_wikipulse_replay.py` | 对有限回放逐条比对 payload、key、原始 headers 和来源位置，发现丢失、重复或内容变化则失败。 |
| 运行环境与契约 | `compose.wikipulse.yml`、`docker/wikipulse-spark.Dockerfile`、`requirements-wikipulse.txt`、`docs/wikipulse_a_contract.md`、`docs/wikipulse_a_runbook.md` | 配置 Kafka、Spark master/worker/client、HDFS NameNode/DataNode、Cassandra 基础服务；记录输入字段、保留策略和操作命令。Cassandra 结果表和 Grafana 看板由 C 提供。 |

原始层的物理记录键是 `(kafka_topic, kafka_partition, kafka_offset)`；`source_id` 是分析侧优先考虑的逻辑去重键。生产者和归档支持至少一次处理，不保证跨 SSE、Kafka、Spark 的端到端 exactly-once。SSE 状态只表示最后确认的 ID，Wikimedia 的恢复能力和长时间断线后的缺口仍需按日志评估。字段细节见 [A 输入契约](wikipulse_a_contract.md)。

## 3. 数据流动

```text
Wikimedia EventStreams / recentchange
    |
    v
A: Python SSE producer --原始 data + 来源 headers--> Kafka: wikipulse.raw.live
    |                                                    |
    | 本地状态文件                                         +--> B: 流解析、过滤、去重与实时分析
    |                                                           |
    v                                                           v
data/wikipulse/producer_A.json                            C: Cassandra 结果表
                                                         --> C: Grafana 看板

Kafka: wikipulse.raw.live
    |
    v
A: Spark Structured Streaming 原始归档
    |
    +--> HDFS Delta: /wikipulse/raw_events
    |      checkpoint: /wikipulse/checkpoints/raw_live
    |      +--> C: Spark SQL 历史指标/基线
    |      +--> A: 固定 Delta 版本、可控速率回放
    |                  |
    |                  v
    |             Kafka: 独立 replay topic --> B/C 实验链路
    |
    +--> 保留所有 Wikimedia 事件和坏 JSON；不在 A 层做 Wikipedia 分析过滤
```

B 的分析范围是 `server_name` 以 `.wikipedia.org` 结尾、`type` 为 `edit/new`、`namespace=0` 的事件。B 负责流侧解析、过滤、回退识别、热门条目、编辑战候选、bot/human 指标及异常检测；C 使用相同规则做历史 5 分钟指标与基线，并提供 Cassandra 读写及展示。接口见 [B/C 契约](wikipulse_bc_contract.md)。

事件时间与 Kafka/接收时间用途不同：B/C 窗口应使用来源事件时间；A 的 `ingest_date_utc` 只用于归档分区。固定回放不改写原始事件时间，也不注入推进水位线的假事件。有限回放结束时只统计 `window_end ≤ 已观测最大事件时间 − 2 分钟` 的窗口。

## 4. 已验证

- 本地单测：`tests.test_wikipulse_producer`（SSE 帧、key/时间、Kafka 确认后的状态）与 `tests.test_wikipulse_archive`（坏 JSON 与 Spark 归档投影）。
- 2026-10-07 A 链路 smoke：真实 EventStreams 小样本、坏 JSON fixture、固定版本回放与逐条核对；细节见 [A 运行手册](wikipulse_a_runbook.md)。
- 2026-10-13 交接快照：Kafka 时间约 8.2 小时、783,315 行，供 B/C 核对去重与规则；后续 7 天补拉与基线由 C 完成，见 [C 运行手册](wikipulse_c_runbook.md)。
- 2026-10-24 端到端：producer → Kafka → 归档 / 流分析 → Cassandra → Grafana，以及受控重启后的恢复，已在完整链路实测；命令与限制见 README 与 A/B/C 运行手册。

短样本（121/122 行）不能外推全天流量；正式流量与存储数字以 7 天归档与 [最终报告](wikipulse_final_report.md) 为准。

## 5. 当前边界（非未完成任务）

- 交付物与运行入口已齐；团队剩余事项见 [task_plan.md](../task_plan.md)（例如编辑战标注的第二人复核）。
- 操作细节以 [A 运行手册](wikipulse_a_runbook.md) 为准；B/C 启动、基线、看板和实验分别见各自 runbook。
- Cassandra 物理表、Grafana 看板、四类分析和历史基线由 C 实现，不在 A 的模块内重复建设。

## 6. 从哪里启动和测试

从仓库根目录执行本地 producer/archive smoke：设置 Python 3.11.9、JDK 21 的 `JAVA_HOME`、`PYTHONPATH=src`，运行：

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_wikipulse_producer tests.test_wikipulse_archive -v
```

Docker 联调依次启动 Compose、创建 live topic、启动 archive，再启动 producer；归档版本检查、坏 JSON fixture、回放和逐条校验命令见 [A 运行手册](wikipulse_a_runbook.md)。宿主机 producer 连 `127.0.0.1:9092`（避免 `localhost` 优先走 IPv6），容器内 Spark 连 `kafka:29092`；回放须使用新 topic 和新 run ID。本机可用忽略文件 `data/wikipulse/compose.local.yml` 将 HDFS UI 映射为 `19870:9870`。不要使用 `docker compose down -v` 清理需要保留的历史数据卷。
