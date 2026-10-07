# 项目执行方案

更新：2026-10-07。当前任务依据：[WikiPulse proposal](proposal_fixed.pdf)（1 页）。[Assignment.md](Assignment.md) 对应的 W1–W4 已完成；现在进入 **Final Project：WikiPulse**，不将其称为 W5，也不延续 Taxi 预测作为新目标。

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

**Status:** in_progress

- [x] 确认 A 数据接入/集成、B 流处理、C 批处理/结果存储/展示的分工。
- [x] 确认 [B/C 第一版逻辑接口](docs/wikipulse_bc_contract.md)：C 提供读写模块、B 调用；只展示完成窗口，基线按版本固定并通过受控重启切换。
- [ ] 完成接口剩余类型、物理表、参数和函数签名设计，并通过小样本交接验证。
- [ ] 确定最终项目代码目录，隔离既有课程产物；固定事件 Schema、标识/去重键、事件时间与接收时间、异常记录和来源字段。
- [x] 固定 UTC、左闭右开窗口、整点对齐的 5 分钟窗口，以及批流复用公共解析/过滤/回退识别规则。
- [ ] 确定编辑战 30 分钟窗口步长和“交替回退”的判定；明确机器人是否参与趋势分数，摘要关键词按词匹配及多语言范围。
- [ ] 明确回退数与回退率的关系：proposal 算法按 5 分钟计数检测，若增加回退比例需另定分母和零值规则；明确冷启动、标准差为 0、采集中断和迟到事件的处理。
- [ ] 验证 Docker Compose 下 Spark/Kafka/HDFS/Delta/Cassandra 的兼容版本和内存预算；验证 Cassandra 到 Grafana 的最小查询，必要时采用 proposal 允许的展示备选。
- [ ] 用小夹具验证 distinct editor 统计、每语言 Top-N、状态窗口和 Cassandra 幂等写入的可行实现，再冻结接口。
- [ ] 形成 WikiPulse 专属契约；新实现确定时再同步仓库规范/README，不提前把旧环境兼容性当成已验证。

### Phase 2：采集、归档与最小端到端（A 牵头集成）

**Status:** pending

- [ ] 实现 SSE producer、Kafka topic/key、连接重试与恢复策略；保留原始事件，记录采集覆盖时间、数量和可观察的缺口。
- [ ] Kafka 原始事件落 HDFS，定义路径/分区、checkpoint 和批次元数据；为 Delta 分区裁剪实验准备明确的 Delta 表。
- [ ] 最小验收：少量真实事件从 producer 到 Kafka、Spark、Cassandra 再到看板；坏 JSON、重复事件、重启恢复有小样本验证。
- [ ] 尽早开始积累历史数据，测量原始流量、Wikipedia 文章编辑占比、存储增长；历史不足时不宣称异常基线可靠。

### Phase 3：四类分析与历史基线

**Status:** pending

- [ ] B 完成流侧解析/过滤和 `bytes_changed`、`is_revert`，实现热榜、编辑战候选与实时 bot/human 指标；C 完成历史活动聚合和热力图展示。
- [ ] C 的每日批处理先生成与流侧一致的 5 分钟指标，再按 wiki/日内小时计算均值、标准差和样本数，保存基线版本。
- [ ] B 的流任务关联 C 提供的历史基线并检测 z-score > 3；启动固定已发布版本，通过受控重启切换，验证 checkpoint 恢复和重试时版本一致性。
- [ ] C 设计 Cassandra 表，根据看板查询确定分区与聚簇键，保留窗口、指标/条目和基线版本；使同窗口更新、重放和重试不产生重复结果。
- [ ] 小样本核对乱序/迟到、重复、关键词误报、交替回退、bot 过滤、零分母、无基线和批流口径一致性。

### Phase 4：可重复实验与展示

**Status:** pending

- [ ] A 提供回放工具，C 组织实验；固定同一归档输入，按受控速率（含高于自然流量）重放 Kafka；记录版本、数据量、Kafka 分区数和 trigger interval。
- [ ] 明确回放 event time、水位线推进、结束后的窗口完成规则和延迟起终点；避免把原始事件年龄算作系统延迟。
- [ ] C 负责性能实验，B 提供流处理指标，A 支持回放与环境；比较吞吐、端到端延迟及积压，建议报告 p50/p95，并标注运行资源、预热、重复次数和异常/丢失计数。
- [ ] 固定窗口与输入核对结果一致后比较配置；B 负责编辑战候选人工抽样评估，记录抽样规则、数量、标签和 precision，不把正常反破坏当作真实争议结论。
- [ ] C 对同一批任务比较 Delta 分区裁剪开/关的耗时与扫描范围，保证查询输出相同。
- [ ] C 的看板完成热门条目、潜在编辑战、bot/human 比例和语言活动热力图；第一版只展示已完成窗口，明确窗口时间及等待语义。

### Phase 5：最终交付

**Status:** pending

- [ ] 交付 producer、Spark 流/批作业、Cassandra Schema、Docker Compose、Grafana 看板（或说明采用的备选）。
- [ ] C 汇总 **2 页报告**，A 汇总 README 运行说明；各人提供自己模块的说明和实测结果，覆盖启动、采集、批基线、重放、实验复现、停止与数据保留。
- [ ] A 牵头、B/C 配合，在独立输出目录验收完整链路、重启恢复和复现实验；记录实测限制，保留 W1–W4 已交付内容。

## 已确认分工

| 角色 | 负责内容 | 交付与边界 |
| --- | --- | --- |
| **A：数据接入与存储** | SSE producer → Kafka、原始数据 HDFS/Delta 归档、可控速率回放工具；Docker Compose 主体、整体集成、README 运行说明 | 提供输入与归档契约、固定回放数据和启动入口；B/C 配合组件接入与排障 |
| **B：流处理** | Structured Streaming 解析/Wikipedia 过滤、热榜滑动窗口、疑似编辑战、实时 bot/human 指标；关联历史基线并做异常检测 | 提供流处理结果、运行指标和正确性测试；负责编辑战候选的人工抽样评估 |
| **C：批处理、结果存储与展示** | Spark SQL 5 分钟指标和 wiki × hour 基线、历史活动聚合；Cassandra 表设计、Grafana 仪表盘 | 组织性能实验、负责 Delta 分区裁剪实验和最终报告；A 提供回放，B 提供流处理指标 |

各人负责自己组件的启动配置、测试、排障和模块说明。A 牵头集成，不承担替其他人完成模块实现的责任；不以“代码量最少”作为 A 工作量较轻的依据。

## B/C 接口：第一版已确认

完整逻辑契约见 [WikiPulse B/C 接口 v1](docs/wikipulse_bc_contract.md)。

- C 负责建表、字段映射和读写模块；B 在流作业内调用，不新增交接服务。
- B 输出热榜、编辑战候选、5 分钟活动和异常检测结果；第一版只输出完成窗口，使用稳定业务键覆盖重试结果。
- C 提供按版本/wiki/UTC 日内小时组织的历史 5 分钟计数基线；B 启动固定版本，通过受控重启切换每日更新。
- 无基线、低样本或零标准差时保留未检测状态；公共规则由 B 维护、C 复用。
- 类型、Cassandra 物理表、函数签名、样本阈值、标准差定义和窗口算法细节待定；Phase 1 技术验证仍未完成。

下一步细化契约并推进 Phase 1 的技术验证。proposal 未提供截止日期、采集天数或量化性能目标，暂不填造。

本轮编辑问题：一次写入命令超过 Windows 命令行长度限制，进程未启动；已拆为归档和逐文件写入。

分工更新时 PowerShell 内嵌 Python 引号解析失败，未写入文件；改用 apply_patch 完成。
