# 项目执行方案

更新：2026-10-25。当前任务依据：[WikiPulse proposal](proposal_fixed.pdf)（1 页）。[Assignment.md](Assignment.md) 对应的 W1–W4 已完成；现在进入 **Final Project：WikiPulse**，不将其称为 W5，也不延续 Taxi 预测作为新目标。

## 已完成基础

W1–W3 已有 Spark/Delta 摄入、校验、整合、SQL、增量和评测经验；W4 已完成需求预测流水线、两路线对照和交付。最终结果见 [progress.md](progress.md)，完整历史统一保留在 [归档](docs/planning_archive/task_plan.md#archive-2026-10-07-w4-closeout)。这些经验可复用，Taxi Schema、训练模型及既有测试不能直接充当 WikiPulse 实现或验收。

## Proposal 已约定的范围

- 数据链路：Wikimedia EventStreams → Python producer → Kafka → Spark Structured Streaming；原始全流另归档至 HDFS 的 Delta/Parquet 表；Spark SQL 批处理生成历史基线；结果写 Cassandra；Grafana 展示，连接插件不顺时用 Streamlit + Plotly。
- 部署：笔记本上的单套 Docker Compose；版本、资源上限和连接器组合待验证。
- 原始层保留所有 Wikimedia 项目事件；分析层只取 `server_name` 以 `.wikipedia.org` 结尾、`type ∈ {edit,new}`、`namespace = 0` 的事件。
- 详细分析关注 en/de/fr/zh/sv/ja，分语言统计覆盖所有 Wikipedia 版本。流量与 Wikipedia 占比须实测。
- 团队：Muyang Huang、Yanjun Wang、Hao Tang；A/B/C 职责已确认（见文末），角色与姓名映射未在本轮指定。

| 输出 | Proposal 口径 |
| --- | --- |
| 热门条目 | 10 分钟窗口、1 分钟滑动；按 wiki/title 聚合，`edit_count * log(1 + distinct_editors)` 评分，每语言排名 |
| 潜在编辑战 | 30 分钟内至少 3 次回退、至少 2 个非机器人用户，且回退交替出现；人工抽样评价 precision |
| 机器人/人类活动 | 各语言编辑占比及按小时活动热力图 |
| 异常活动 | 每语言 5 分钟编辑数/回退数，对照同语言、同日内小时的历史均值和标准差；z-score > 3 |

回退识别为摘要关键词启发式（revert/rv/undid 及本地化词），使用 2 分钟 event-time watermark。定义上的待定点见 [findings.md](findings.md)。

### Phase 0：需求对齐

**Status:** complete

- [x] 提取并目视核对 proposal 全页，记录架构、四类分析、实验和交付要求。
- [x] 主计划切换到 WikiPulse，保留 W1–W4 完成状态；代码尚未开始。

### Phase 1：契约与技术最小验证

**Status:** complete

- [x] 确认 A 数据接入/集成、B 流处理、C 批处理/结果存储/展示的分工。
- [x] 确认 [B/C 第一版逻辑接口](docs/wikipulse_bc_contract.md)：C 提供读写模块、B 调用；只展示完成窗口，基线按版本固定并通过受控重启切换。
- [x] 完成接口剩余类型、物理表、参数和函数签名设计，并用 A 的交接快照核对（[接口 v2](docs/wikipulse_bc_contract.md)）。
- [x] 确定 WikiPulse 独立代码目录并隔离既有课程产物；固定 A 原始事件 Schema、物理记录键、事件/接收时间与来源字段。
- [x] 固定 UTC、左闭右开窗口、整点对齐的 5 分钟窗口，以及批流复用公共解析/过滤/回退识别规则。
- [x] 编辑战 30 分钟窗口、5 分钟步长，交替为 A → 他人 → A；热榜排除机器人编辑；回退关键词按 Unicode 词边界、六语言并集（sv 未实测）。
- [x] 异常检测只用 5 分钟计数，不引入回退率；无基线/低样本/零标准差各有 `status`；采集缺口按原始事件时间 > 60 秒判定；迟到超过 2 分钟 watermark 丢弃。
- [x] 验证 Docker Compose 下 Spark/Kafka/HDFS/Delta/Cassandra 的兼容版本和内存预算；Grafana 12.2 + Cassandra 插件 3.3.0 可用，未启用 Streamlit 备选。多日批处理需在 `spark-client` 以 local 模式加大内存运行（见 C 运行手册）。
- [x] 用小夹具验证 distinct editor 统计、每语言 Top-N、状态窗口和 Cassandra 幂等写入；B 的四表流式写入已按冻结接口联调。
- [x] 形成 WikiPulse 专属 B/C 契约，并在实现确定后同步 README、B/C 运行手册和项目进度。

### Phase 2：采集、归档与最小端到端（A 牵头集成）

**Status:** complete

- [x] 实现 SSE producer、Kafka topic/key、连接重试与恢复策略；保留原始事件，记录采集覆盖时间、数量和可观察的缺口。
- [x] Kafka 原始事件落 HDFS，定义路径/分区、checkpoint 和批次元数据；为 Delta 分区裁剪实验准备明确的 Delta 表。
- [x] A 链路小样本验收：真实事件从 producer 到 Kafka、Spark/HDFS Delta；坏 JSON 保留、checkpoint 恢复和固定版本回放核对通过。
- [x] B/C 链路验收（2026-10-24）：实时事件经 B 的作业写入 Cassandra 四表并在看板展示；杀掉驱动后从 checkpoint 重启，窗口连续、无重复。
- [x] C 从 EventStreams 补拉 7 天历史（Delta version 348，20,398,815 行，与 Kafka offset 一致；每天约 2.9 M 条，Wikipedia 项目 28.9%、文章编辑 11.9%，16.6 GiB）。

### Phase 3：四类分析与历史基线

**Status:** complete

- [x] B 完成流侧解析/过滤和 `bytes_changed`、`is_revert`，实现热榜、编辑战候选、实时 bot/human 指标，并只发布 watermark 已关闭的窗口。
- [x] C 的批处理生成与流侧一致的 5 分钟指标，再按 wiki/日内小时计算均值、样本标准差和样本数，保存不可变基线版本；7 天归档上的基线 `20261023T1200Z` 无缺口、每格 72 个样本，看板含基线热力图。
- [x] B 的流任务启动时固定读取 C 的已发布基线，按 `z-score > 3` 检测异常；新版本通过受控重启切换。
- [x] C 的 Cassandra 表和幂等写入接口已由 B 的 `foreachBatch` 调用；实时与回放使用隔离的 keyspace/checkpoint。
- [x] 小夹具核对交替回退、bot 过滤、Top-N 并列顺序、全部语言活动、无基线/低样本/零方差状态；文件流到 Cassandra 四表联调通过。
- [x] 固定 Kafka 回放的批流一致性：198/198 个完成窗口、8,821 次编辑一致；checkpoint 重启在实时链路上验证。按事件时间顺序回放不产生超过 watermark 的迟到事件，迟到丢弃行为只由 B 的夹具测试覆盖。

### Phase 4：可重复实验与展示

**Status:** complete

- [x] A 提供回放工具，C 组织实验；固定同一归档输入，按受控速率（含高于自然流量）重放 Kafka；记录版本、数据量、Kafka 分区数和 trigger interval。
- [x] 明确回放 event time、水位线推进和尾部窗口规则：不注入假事件，只比较 `window_end ≤ max(event_time) − 2 minutes` 的完成窗口；延迟起点为窗口可关闭时刻（事件时间首次越过 `window_end` + 2 分钟的回放事件的 Kafka 时间），终点为 Cassandra `written_at`。
- [x] C 完成性能实验：同一 5 万行切片，速率 100/200/400、trigger 5/30 秒、分区 1/3，各 2 次，另加 2 次排空吞吐。全部配置都跟得上；trigger 决定延迟（5–10 秒约 18 秒，30 秒约 50 秒），分区数无可测影响，最大吞吐约 634–737 条/秒。实时瓶颈是 producer 逐条确认（约 32 条/秒）。
- [x] B 在 8.2 小时固定快照上完成编辑战候选人工评估：24 个重叠窗口折叠为全部 4 个候选页面，按实质内容争议口径标注 1 个正例、3 个反破坏/傀儡清理负例，precision 为 25%；扩大样本留待多日回放。
- [x] C 完成分区裁剪实验：按事件日期分区的副本上，同一一天 5 分钟计数任务输出一致；读取文件 292 对 4,580（−94%），耗时中位数 51.4 对 60.9 秒（−16%）。
- [x] C 的看板完成热门条目、潜在编辑战、bot/human 比例、语言占比、基线与活动热力图和异常；只展示已完成窗口，时间轴为 UTC。
- [x] C 在 7 天归档上扩大编辑战样本：209 个候选页面随机抽 50 个，precision 19.5%（8/41，Wilson 95% 区间 10–34%），见 [抽样记录](docs/wikipulse_c_edit_war_sample.md)。

### Phase 5：最终交付

**Status:** in_progress

- [x] 交付 producer、Spark 流/批作业、Cassandra Schema、Docker Compose、Grafana 看板。
- [x] C 完成 [2 页报告](docs/wikipulse_final_report.md)（附 PDF）；README 补齐完整运行顺序（启动、采集、基线、流作业、看板、回放与实验、停止与数据保留），各模块细节在 A/B/C 运行手册。
- [x] 完整链路和重启恢复已在 2026-10-24 实测（见 Phase 2）；回放和实验写入独立的 topic、checkpoint 与 `wikipulse_replay_<run_id>` keyspace，命令和实测限制记录在 C 运行手册。W1–W4 交付内容保留。
- [ ] 团队至少一人复核 C 的 50 页编辑战标注（目前为 AI 辅助单人标注，见 [抽样记录](docs/wikipulse_c_edit_war_sample.md)）。

## 已确认分工

| 角色 | 负责内容 | 交付与边界 |
| --- | --- | --- |
| **A：数据接入与存储** | SSE producer → Kafka、原始数据 HDFS/Delta 归档、可控速率回放工具；Docker Compose 主体、整体集成、README 运行说明 | 提供输入与归档契约、固定回放数据和启动入口；B/C 配合组件接入与排障 |
| **B：流处理** | Structured Streaming 解析/Wikipedia 过滤、热榜滑动窗口、疑似编辑战、实时 bot/human 指标；关联历史基线并做异常检测 | 提供流处理结果、运行指标和正确性测试；负责编辑战候选的人工抽样评估 |
| **C：批处理、结果存储与展示** | Spark SQL 5 分钟指标和 wiki × hour 基线、历史活动聚合；Cassandra 表设计、Grafana 仪表盘 | 组织性能实验、负责 Delta 分区裁剪实验和最终报告；A 提供回放，B 提供流处理指标 |

各人负责自己组件的启动配置、测试、排障和模块说明。A 牵头集成，不承担替其他人完成模块实现的责任；不以“代码量最少”作为 A 工作量较轻的依据。

## B/C 接口

完整契约见 [WikiPulse B/C 接口](docs/wikipulse_bc_contract.md)（v2 已冻结并按实现同步）。

- C 负责建表、字段映射和读写模块（`store.py`）；B 在流作业的 `foreachBatch` 内调用，不新增交接服务。
- B 输出热榜、编辑战候选、5 分钟活动和异常检测结果；只输出完成窗口，使用稳定业务键覆盖重试结果。
- C 提供按版本/wiki/UTC 日内小时组织的历史 5 分钟计数基线；B 启动时固定版本，通过受控重启切换。
- 无基线、低样本或零标准差时保留未检测状态；公共解析、过滤和回退规则在 C 维护的 `events.py` 中，批流共用。
