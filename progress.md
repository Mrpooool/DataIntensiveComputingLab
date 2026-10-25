# 项目进度

更新：2026-10-25。**W1–W4 已完成；WikiPulse 全链路、7 天基线、看板和三项实验已完成并有实测结果。**C 的 PR #19 已合并，README 已补齐运行顺序。剩余：团队复核 C 的 50 页编辑战标注。

## W1–W3 历史摘要

| 周次 | 完成与历史验证 |
| --- | --- |
| W1 | 四源摄入与整合，9,554,576 条通过行程、202 条拒绝；27 项测试通过，两种布局结果一致 |
| W2 | 六个查询、四张产品、13 项优化实验；55 项测试通过 |
| W3 | 增量、Schema 演进、选择性刷新、校验和监控；86 项测试通过，后续物化修复相关 27 项通过；完整评测和交付包已形成 |

详细历史保留在 [2026-09-28 归档](docs/planning_archive/progress.md#archive-2026-09-28) 与 [本轮归档](docs/planning_archive/progress.md#archive-2026-10-07)，不再逐次展开过程。

## W4 最终结果

A 完成训练集，B 完成特征/模型/再训练入口，C 完成联调修复、raw 路线、全量评测和材料汇总。

| 项目 | 完成证据 |
| --- | --- |
| 训练集 | 571,946 行，335,531 行零需求；train/validation/test 为 401,122 / 85,936 / 84,888；标签合计 9,517,007 |
| 特征与模型 | 299 维；Linear Regression 选中 reg=0.0；test RMSE 13.346、MAE 4.883、R² 0.941 |
| 简单基线 | 前一天同小时需求；test RMSE 20.483、MAE 5.519、R² 0.862 |
| 模型重载 | 主训练 `main-20261003`，20 个固定测试样本预测核对一致 |
| raw/platform | 全量运行 `20261002T155150Z`，双向逐行差异均为 0；准备 43.0/8.6 秒，端到端 55.3/21.3 秒 |
| 再训练 | `retrain-w3-update` 使用新配置和同一入口生成 1,144,154 行并训练；仅为模拟数据机制演示 |

这些是 2026-10-02/03 的历史实测。本轮只读核对 `results.json`、主训练 `metrics.json` 的 success 状态、指标和重载结果，没有重新执行实验。评测报告关联代码 `ae244c1`；路线实验早于该提交最后一次 Spark 目录来源识别修改，该分支不影响路线实验。

## 验证边界与交付

- 历史测试：W4 三个测试文件共 12 项通过，最后修改后 `test_ml_dataset` 的 5 项再次通过。
- 全量测试曾运行约一小时后被后台时限终止，没有完整结果；此前用户决定不再重跑，不能称为全量回归通过。本轮未修改业务代码或运行 Spark。
- 交付：[设计报告](docs/w4_design_report.md)、[评测报告](docs/w4_evaluation_report.md)、[计时样本](docs/w4_evaluation_timings.csv)、[英文 README](README.md)、[中文 README](README-zh.md)、[提交包](submissions/Week4_submission_2026-10-03.zip)。
- 本轮检查提交包目录：104 项，含设计与评测两份 PDF。远端推送、PR 合并和课程系统提交状态未核验。

## 2026-10-07 文档收尾

- 使用 planning-with-files；session-catchup 未返回未同步上下文。核对当前计划、最终报告、保存的原始结果及交付包。
- 按用户要求将两次归档合并到 `docs/planning_archive/`；三份归档文档内按日期保留完整快照，更新链接后移除两个旧目录。
- 主计划切为 W1–W4 完成态，清除过时建议和待交接描述；保留正式入口、结果、测试边界和已知限制，不创建后续作业。
- 核验归档正文与压缩前版本一致、中文 UTF-8、文档相对链接、阶段 complete 状态和 `git diff --check`。
- 沙箱启动异常通过自动审批后的命令完成；误写的中文 README 文件名已纠正。未提交或推送本轮文档修改。

## 2026-10-07：WikiPulse proposal 与新计划

- 阅读并目视核对 [proposal_fixed.pdf](proposal_fixed.pdf) 第 1 页，明确 Kafka/Spark/HDFS/Cassandra/看板链路和四类分析。
- 将主计划切到 Final Project：需求对齐 complete，契约验证、采集归档、分析基线、实验展示、最终交付 pending；分工为建议，未映射个人。
- W4 完成摘要追加到同一 `docs/planning_archive/` 的三份文档，保留日期锚点，不新增归档目录。
- 更新 findings 区分提案要求、流量估计和待定语义；不把 W4 的旧模型/性能当作 WikiPulse 结果。
- 未修改 proposal、Assignment、业务代码或运行配置；未联网采集、启动服务或跑测试。PDF 预览在已忽略的 `artifacts/proposal_review/`；渲染字体警告已目视核对不影响阅读。
- 初次批量写入超过 Windows 命令行长度限制，进程未启动；拆为归档和逐文件写入后完成。核对 UTF-8、相对链接、归档内容和 git diff --check。
- 后续分工已在下述记录确认；事件/窗口/回退与基线契约、兼容版本及最小链路仍待推进。

## 2026-10-07：确认分工

- 按用户方案确定 A 数据接入/存储/Compose/整体集成/README，B 流处理，C 批处理/结果存储/展示。
- 补齐责任：B 实时 bot/human 指标及编辑战人工抽样；C 历史聚合、性能/Delta 裁剪实验和最终报告；A 提供回放，B 提供运行指标，各人提供模块说明和测试。
- Phase 1 更新为 in_progress：分工已完成，B/C 接口按用户安排留待稍后讨论，不提前冻结。
- 仅同步三份规划文档；未改业务代码、执行测试或启动服务。首次编辑命令引号解析失败，未写入文件，已改用 apply_patch。

## 2026-10-07：确认 B/C 第一版接口

- 用户接受讨论稿；新增 `docs/wikipulse_bc_contract.md`，并从现有 `docs/data_contract.md` 链接，保留 W1–W4 契约正文。
- 确认 C 提供读写模块、B 在流作业内调用；第一版只展示完成窗口，稳定业务键覆盖重试结果。
- 记录基线和四类结果字段、UTC/窗口口径、共享解析规则及未检测状态；基线运行期间固定版本，受控重启切换。
- 同步 task_plan/findings；类型、物理表、函数签名、样本阈值及算法细节仍待定，Phase 1 保持 in_progress。
- 本轮为文档落地，未实现代码、启动服务或运行 Spark 测试；窗口完成、基线切换和重试行为留待验证。

## WikiPulse 角色 A 当前实现与验证

角色 A 已实现 Wikimedia SSE → Kafka 原始输入、Kafka → HDFS Delta 原始归档、checkpoint 恢复、坏 JSON 原样保留、固定 Delta 版本限速回放及逐条核对；对应代码、契约、运行手册和 Compose 入口已在仓库中。A 的本地测试 6 项通过，Docker smoke 已验证七个服务启动、Delta version 5 为 122 行/122 个物理键、坏 JSON 2 行，以及 5 条回放的 `sent=5/source_rows=5` 和 `verified=5`；此前 100+20 条真实样本、坏 JSON、20 条回放也逐条核对通过。前一轮 producer 在 00:50 记录累计发布 100,123 条、`bad_json=0`、16 次重连；进程退出后已于 00:54 从状态文件恢复并重新启动，00:57 新进程已发布 5,248 条、`bad_json=0`，累计约 105,371 条。已在 producer 中加入按进程隔离临时文件和有限替换重试；Kafka offset、Delta 当前版本/行数和存储增长尚未核实，现有样本不足以外推全天流量或历史基线。A 的输入契约已可交给 B/C，但 Cassandra 结果表、看板、B/C 分析和完整端到端验收尚未完成。

## 2026-10-13：C 冻结 B/C 接口 v2 与公共解析模块

- 用 A 的交接快照（Delta version 0，783,315 行，正文 edit/new 105,478 行）核对去重、迟到、采集缺口、回退关键词和编辑战规则；数字记录在 `docs/wikipulse_bc_contract.md` 文末。
- 依据核对结果：fr 关键词收紧以排除 “révolution”；sv 关键词标为未实测；编辑战“交替”改为出现 A → 他人 → A；采集缺口按原始事件时间判断（Kafka 时间上的 181 秒空档已由 `Last-Event-ID` 补回）。
- 接口 v2 补齐字段类型、`metric`/`status` 枚举、四类结果和基线的 Cassandra 物理表、`store.write_rows`/`read_baseline` 签名、热榜 Top 10 与并列规则、基线 7 天/最低 24 样本/样本标准差及发布标识。
- 新增 `src/dic_pipeline/wikipulse/events.py`（解析、范围过滤、批流去重、回退识别）和 `tests/test_wikipulse_events.py`，5 项通过；在交接快照上得到 105,476 行（去掉 2 条正文重复）、重点语言非机器人回退 1,319 次，与核对脚本一致。
- 资料核查：spark-cassandra-connector 尚无 Spark 4 正式版，写入改用 Python `cassandra-driver` 3.30.1；Grafana 可用 `hadesarchitect-cassandra-datasource` 3.3.0（Grafana 12.x）。两者均未实测。
- 新增 `store.py`（建表、四类结果写入、基线发布/读取，Python driver）、`baseline.py` 与 `scripts/run_wikipulse_baseline.py`，以及 `docs/wikipulse_c_runbook.md`；`requirements-wikipulse.txt` 增加 `cassandra-driver==3.30.1`。
- 测试：A 6 项、events 5 项、baseline 5 项、store 6 项在同一进程共 22 项通过（后两者使用本机 Cassandra 容器，临时 keyspace）。`test_wikipulse_a` 改用 `create_spark()`：原先自建无 Delta 的 session，先运行时会使同进程后续 Delta 测试失败。`spark-client` 镜像内经 `cassandra` 主机名读基线、重复写入只留 1 行。
- 独立代码审查后修复：已发布基线版本不可变（换输入重建同名版本报错）、跨历史边界的采集缺口、零星旧时间戳拉长 `history_start`；修复后 WikiPulse 测试 23 项通过。
- 在交接快照上发布基线 `20261013T0600Z`：98/98 个窗口覆盖、无缺口、1,926 行（214 个 wiki × 9 个 UTC 小时），无一行达到 24 个样本。
- 未完成：看板（Grafana 插件实测）、B 流作业联调、性能与分区裁剪实验、报告；可用基线依赖 A 的多日导出。

## 2026-10-24：B 完成 Structured Streaming 分析与 C 接口联调

- 在 `feat/wikipulse-role-b` 基于 C 的接口分支实现 `streaming.py` 和运行入口：共享解析/2 分钟 watermark 后分成热榜、编辑战、5 分钟活动/异常三条独立查询，各自使用持久 checkpoint。
- 热榜实现 10 分钟窗口/1 分钟步长、非机器人过滤、编辑人数和稳定 Top 10；编辑战实现 30 分钟窗口/5 分钟步长、按事件时间/修订号排序及 A→他人→A 判定；活动覆盖全部 Wikipedia，异常固定关联启动时指定的基线版本。
- 实时结果写 `wikipulse`；回放要求 `run_id`，自动写 `wikipulse_replay_<run_id>` 并隔离 checkpoint，避免污染实时结果。每个 `foreachBatch` 输出 JSON 行数日志供 C 的实验记录使用。
- B 的 6 项纯转换测试通过；文件流推进 watermark 后，`hot_pages`、`edit_war_candidates`、`activity_5m`、`anomalies` 四表端到端 Cassandra 联调通过。A/公共事件/C 基线非存储部分/B 合并回归 22 项通过，容器内因既有测试默认连接 `localhost` 跳过 2 个 Cassandra 测试类；B 自己的 Cassandra 联调使用 `cassandra` 主机名实测通过。
- 在 Delta version 0 固定快照上运行与生产相同的编辑战规则：完成窗口得到 24 个重叠候选，折叠为全部 4 个候选页面。按“实质内容/来源争议”人工标注 1 个正例、3 个反破坏或傀儡清理负例，页面级 precision 为 25%；证据与局限见 `docs/wikipulse_b_edit_war_evaluation.md`。
- 待做：用 A 的固定 Kafka 回放验证 checkpoint 重启、迟到丢弃和同一固定输入的批流一致性；C 接手四张结果表做看板、性能实验和最终报告。现有 8.2 小时基线全部低于 24 个样本，异常状态会是 `insufficient_samples`，不能包装成可靠异常检测。

## 2026-10-24/25：C 补拉 7 天数据、看板、实验与报告

- **修复：** B 的时区相关测试改为比较时刻；`write_activity` 缓存 micro-batch，避免同一批次计算两次。回放改为按组确认 Kafka 发送（约 90 → 600 条/秒），并增加 `--from-event-time`：补拉数据是抓取顺序，codfw 领先 eqiad 数天，按事件时间回放才不会把水位线推过后续事件。`events.analysis_rows` 暴露去重前的行；基线作业和 B 的编辑战评估只缓存所需列，否则 7 天数据会内存不足。
- **补拉：** producer 增加 `--topic` 和 `--ack-every`，从 EventStreams 补拉 2026-10-17 12:00 至 10-24 18:15 UTC。归档 Delta version 348 共 20,398,815 行，等于 Kafka 三个分区 offset 之和；每天约 2.9 M 条，Wikipedia 项目占 28.9%，文章编辑/新建占 11.9%（2,426,933 行）。
- **基线：** `20261023T1200Z` 覆盖 6 天，1,728/1,728 个窗口无缺口，8,040 行，每行 72 个样本。
- **看板：** Grafana 12.2 + Cassandra 插件 3.3.0 随 Compose 预置，含热门条目、编辑战候选、bot/human、语言占比、基线与活动热力图、异常等面板；实时 keyspace 上截图核对。
- **实时验收：** 实时链路写入四表并在看板展示；捕获到 enwiki 回退突增（151 对基线约 17，z = 16.7）。杀掉驱动后从 checkpoint 重启，5 分钟窗口连续、无重复。看板约落后 15 分钟，原因是 producer 默认逐条确认（约 32 条/秒），低于晚间流量，不是 Spark 的问题。
- **性能实验：** 同一 5 万行切片，16 次定速运行加 2 次排空。所有配置都跟得上（最高 400 条/秒，约自然流量 12 倍）；热门条目延迟中位数在 5–10 秒 trigger 下约 17–19 秒，30 秒 trigger 下约 48–54 秒；Kafka 分区数无可测影响；排空吞吐 634–737 条/秒。逐次结果见 `docs/wikipulse_perf_results.csv`。
- **批流一致性：** 一次回放上 198/198 个完成窗口、8,821 次编辑完全一致。
- **分区裁剪：** 在按事件日期分区的副本上，同一个一天的任务输出一致；读取文件 292 对 4,580，耗时中位数 51.4 对 60.9 秒。Parquet 行组 min/max 过滤已为未裁剪的扫描跳过大部分数据。
- **编辑战：** 7 天得到 209 个候选页面，随机抽 50 个标注，结果为 8 正、33 负、9 不确定，precision 19.5%（Wilson 95% 区间 10–34%）。标注由 AI 助手按 B 的标准完成，尚无第二人复核。
- **交付：** C 运行手册重写，新增 [2 页报告](docs/wikipulse_final_report.md)（附 PDF）、编辑战抽样记录和性能结果 CSV；B/C 契约同步看板、延迟定义和 `analysis_rows`。
- **环境：** 单核 1 GiB 的 Spark worker 扫描多日归档太慢，批处理改在 `spark-client` 中以 local 模式运行，会话内用 `docker update` 把容器内存提到 8 GiB；Compose 文件未改。实验中间结果在已忽略的 `artifacts/wikipulse/`。

## 2026-10-25：交付前文档收尾

- 按队友审核意见，在英文和中文 README 补齐 WikiPulse 完整运行顺序：启动 Compose、建 topic、归档与 producer、基线、流作业、看板、回放与实验、停止与数据保留，细节链接到 A/B/C 运行手册。
- 更新过时状态：`task_plan.md` 的 Phase 5 和 B/C 接口小节、`docs/wikipulse_role_a_overview.md` 的数据流标注、中文 README 的角色 A 标题。
- 宿主机 WikiPulse 单测 23 项通过、3 项跳过（Docker 未启动，Cassandra 不可达）。W1–W4 测试仍按文件在宿主机运行，不放进 2 GiB 的 `spark-client` 容器做全量回归。
