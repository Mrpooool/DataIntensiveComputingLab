# 项目发现与决策依据

更新：2026-10-07。当前依据为 [WikiPulse proposal](proposal_fixed.pdf) 第 1 页；已完成文本提取和整页目视核对。以下区分 proposal 的承诺和实施前待定事项，尚未验证实时数据或技术栈。

## 项目变化与已有基础

- 新目标是 Wikipedia 编辑流的实时分析平台，采用 Kafka、Spark 流/批处理、HDFS、Cassandra 和看板，交付 2 页报告。W4 的离线 Taxi 预测仍为已完成课程任务。
- 可复用的是 Spark/Delta、时间与质量契约、结果一致性检查和可重复评测经验；Kafka/SSE、Structured Streaming 状态处理、HDFS 部署、Cassandra、看板均为新的工作。
- 本次仓库文件检查未发现 WikiPulse、Kafka/Cassandra 或 Compose 实现。旧模型、测试及单机性能数字不证明新系统可用。
- W4 的完整契约、结果和限制保存在 [统一归档](docs/planning_archive/findings.md#archive-2026-10-07-w4-closeout)。

## Proposal 要求摘录

| 项目 | 约定 |
| --- | --- |
| 原始来源 | Wikimedia EventStreams recentchange，SSE；原始层包含所有 Wikimedia 项目 |
| 分析集 | Wikipedia 域名、edit/new、namespace=0；重点 en/de/fr/zh/sv/ja，语言统计覆盖所有 Wikipedia |
| 事件字段 | wiki、server_name、title、namespace、user、bot、type、timestamp、comment、minor、length before/after |
| 派生与迟到 | bytes_changed；摘要关键词识别 is_revert；2 分钟事件时间 watermark |
| 趋势 | 10 分钟窗口、1 分钟滑动，按 wiki/title；评分 edit_count × log(1 + distinct_editors) |
| 编辑战候选 | 30 分钟至少 3 次回退、至少 2 位非 bot 用户交替回退；人工抽样评估 |
| 批基线 | 每日任务先做每语言 5 分钟指标（编辑数、回退数、bot share），再按 wiki/日内小时算均值和标准差 |
| 异常 | 流侧 5 分钟编辑数与回退数对照历史基线，z-score > 3 |
| 服务/展示 | Cassandra 按 wiki/time window 组织；Grafana，必要时 Streamlit + Plotly |
| 实验 | 固定归档重放，比较 trigger/分区数下吞吐与延迟；编辑战 precision；Delta 分区裁剪批任务成本 |

proposal 的 20–50 events/s、每日约 200–400 万原始事件，以及语言规模描述是提案假设/估计，本次未联网验证，正式报告以采集实测为准。

## 需要先固定的口径（实施建议，不是新增作业要求）

1. **计数与比例**：问题描述写 edit/revert rate，算法具体写 5 分钟 edit/revert count。固定时间内计数可表征事件速率，回退占编辑比例则是另一指标，不能混用。
2. **事件标识与恢复**：从真实样本确定稳定标识、Kafka key、重复处理和断线恢复语义；原始归档保留什么、分析去重在哪一步都应明确，不能提前声称 exactly-once。
3. **回退识别**：关键词与本地化词只是候选特征；尤其 rv 需防止任意子串误匹配。30 分钟窗口步长、交替序列、同时间事件顺序、多用户及 bot 是否计入三次阈值需要定义。
4. **批流一致性**：固定时区、窗口边界、过滤/去重与关键词版本。只有观测覆盖完整的无事件窗口才可补零，采集中断不等于零活动；历史基线不能包含正在检测的未来/当前窗口。
5. **基线可用性**：明确最少历史样本、标准差为零、初始无历史和基线更新方式；proposal 没规定历史天数，不强行写死。高 z-score 是活动异常，不等于破坏行为。
6. **窗口输出与可视化**：区分尚在更新的 Top-N 和已完成窗口。验证 distinct editor、排名、状态清理和 Cassandra 重试/覆盖写策略，不能把批 SQL 直接当作已可流式运行。
7. **重放公平性**：相同归档、过滤和事件顺序语义；定义 event-time 重映射/推进策略和尾部窗口完成条件。系统延迟从本次注入或接收起算，并另说明最终窗口结果的等待时间。
8. **分区与兼容性**：Cassandra 的 wiki/time window 是设计方向，须按读模式确定完整主键；先核验 Grafana 插件和 Spark/Kafka/Delta/Cassandra 连接器版本，再定 Compose。
9. **存储选择**：proposal 允许原始层 Delta/Parquet，但承诺 Delta 分区裁剪实验，因此实验数据必须有明确 Delta 落地；不要高基数过度分区。

## 2026-10-07 已确认职责与接口状态

- A：接入、Kafka、HDFS/Delta 归档、回放、Compose 主体、整体集成与 README。
- B：流处理四类实时指标/检测、历史基线关联，以及疑似编辑战人工抽样评估。
- C：批处理 5 分钟指标与基线、Cassandra、看板，组织性能/裁剪实验并汇总报告。
- 各人承担自己模块的启动配置、测试和排障；A 的集成职责不替代模块责任。
- 后续用户已确认 [B/C 接口 v1](docs/wikipulse_bc_contract.md)：C 提供读写模块，B 在流作业内调用；只展示完成窗口。C 基线按版本发布，B 启动固定版本，通过受控重启切换。
- UTC、左闭右开和 5 分钟整点对齐已确认；共享过滤/回退规则。基线统计对象为历史 5 分钟计数，而非整小时总数；缺基线/低样本/零标准差保留未检测状态。
- 字段类型、物理表、阈值及算法细节仍待定；上述决定收敛了前文待定点，不代表已完成实现或验证。
- 已查阅官方文档：`foreachBatch` 默认至少一次写入，Cassandra 支持按主键 upsert；因此约定稳定键覆盖重试结果，仍需端到端验证。来源链接见接口契约。

## 验证范围与问题记录

本次只阅读 PDF、本地规范、配置和模块清单并更新规划；未连接 EventStreams、启动容器或执行新实验。Poppler 渲染提示 Symbol/ArialUnicode 字体警告，但输出整页内容可读，已与文本核对；未修改原 PDF。
