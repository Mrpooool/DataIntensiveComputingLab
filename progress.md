# 项目进度

截至 2026-10-02：W1–W3 已完成并合入 main；W4 中 A 的训练集、B 的特征与模型已合入，C 已修复两处接入问题并完成首次真实数据训练，路线对照待做。下一步见 [task_plan.md](task_plan.md)。

## W1–W2 历史摘要

- W1：四源全量摄入与整合完成。Taxi 输入 9,554,778、通过 9,554,576、拒绝 202；Weather 8,784，NYC Air 51,885，Zone 265；整合保留全部通过行程。27 项历史测试通过，两种存储布局结果一致。
- W2：Q1–Q6、四张产品、13 项优化实验完成，55 项历史测试通过。最终实验 `20260919T143454Z-9d921a51`；[评测报告](docs/w2_benchmark_report.md)。
- 早期调查、审核和运行细节见[原进度归档](docs/planning_archive_2026-09-28/progress.md)。

## W3 完成摘要

| 角色 | 最终交付 |
| --- | --- |
| A | 三类增量文件、MERGE、Schema 加列、整合追加、产品 auto/full 刷新 |
| B | Schema 白名单、引用/数值/完整性校验、拒绝隔离、规则注册、统一校验开关 |
| C | pipeline_runs、运维 SQL、增量修复、全量评测、README 和交付汇总 |

最终行为：apply 重跑自动补齐 MERGE 后未整合数据；已删除 sync-integrated；覆盖窗口由完成快照发布；产品按来源 Delta 版本刷新。Weather/Air 演进列暂不进整合表，环境修正不回填既有 Taxi。

历史验证：2026-09-26 全部 86 项通过；UTC 时间修复与产品物化后，相关 5 套件 27 项通过。本次未重跑这些测试。

全量评测提交 `adf0508`，运行 `20260926T174427Z-1ce27d00` 和 `20260926T180757Z-afffd333`；预热一次，每项三次取中位数，全部输出一致：

| 测量 | 结果 |
| --- | --- |
| 增量更新 | 134.4 秒；重复应用插入 0 行，随后 auto 跳过所有产品 |
| 分析刷新 | full 66.3 秒、auto 87.2 秒；内容哈希一致 |
| 存储 | 快照 2,105.5 → 2,271.8 MB（+7.9%），文件 116 → 517 |
| 校验 | 176.1 → 223.1 秒（+26.7%） |
| 监控 | 摄入 +12.6%、整合 +13.8%、刷新 +59.8% |

原始基线 `data/benchmark/w3/baseline`。曾在其副本按 README 跑 generate → apply → auto → 重复 apply → auto → Q3 → 运维报告，Q3 覆盖至 7 月 1 日；这不是 W4 预测效果验收。

交付：[设计报告](docs/w3_design_report.md)、[评测报告](docs/w3_evaluation_report.md)、[计时样本](docs/w3_evaluation_timings.csv)、[提交包](submissions/Week3_submission_2026-09-27.zip)。W3 代码已通过 PR #11、#12 合入 main；课程系统提交状态未核验。

## 2026-09-28：W4 需求与计划更新

- 使用 planning-with-files；session-catchup 未返回未同步上下文，开工前工作区干净。
- 对照 W4 四个 Task，读取数据契约、代码接口和 W3 最终报告，没有启动 ML 实现。
- 三份旧文件归档至 `docs/planning_archive_2026-09-28/`，加历史快照说明并调整相对链接；当前文件压缩历史，移除当前视图中的旧 pending/旧入口描述。
- 更新六阶段计划：需求整理 complete；契约、特征、模型、两路对照、验证交付 pending。
- 建议 Zone 下一小时需求预测及 A 数据集/B 特征模型/C 对照交付，均未当作已确认决定。补充时间切分、预测时特征可用性、train-only 拟合及模拟数据隔离。
- 编辑中 apply_patch 拒绝同路径 Delete/Add；随后发现 PowerShell 管道中文编码问题，改用 ASCII 转义 JSON 传输和 UTF-8 写入，重新检查全文与归档。
- 本次只整理规划文档；核验文档差异、相对链接和阶段状态，不重跑 Spark 或全量评测。

## 2026-10-01：W4 同学 B 实现

- 从最新 `origin/main` 的合并提交建立 `feat/w4-role-b-features-model`；开工时 W4 只有计划，没有 A/C 实现可接。
- 固定 Zone-hour 需求预测的 B 输入接口：键、标签、三段时间 split、位置、需求 lag 和一小时滞后的环境字段；写入 `configs/ml.json` 与数据契约。
- 新增 `ml_pipeline.py`：输入校验、纽约本地周期特征、缺失标记、train-only 中位数填补/类别编码/缩放、未知类别处理及 `features` 组装。
- 完成前一天同小时基线和 Linear Regression 闭环：validation RMSE 选候选，test 最终评估 MAE/RMSE/R²，保存并 reload 后核对固定样本预测。
- 新增特征物化和训练/重训练两个 CLI；每次训练保存完整 PipelineModel、配置快照、环境、输入路径、split 行数、候选指标与耗时。
- 新增 4 个针对性 Spark 测试，覆盖接口字段、重复键拒绝、train-only 填补、缺失标记、未知类别、特征向量、指标和模型 reload。
- Python 语法编译通过，配置可导入；当前 macOS 没有 Java Runtime 且 `.venv` 的 3.11.9 解释器链接失效，Spark 测试无法在本机启动，未将其误报为通过。

## 下次接续

真实数据特征与训练已跑通（见 2026-10-02 记录）。下一步由 C 从 raw 路线生成同接口数据，再做两路对照和分阶段计时。

## 2026-10-01：W4 同学 A 训练集

- 从固定原始 1–3 月完成批次的五张固定版本 Delta 表生成 Zone-hour 训练集；原始文件标识、版本和覆盖窗口记录在 `data/delta/ml/training_dataset_metadata.json`。入口拒绝缺月或 W3 模拟更新快照。
- 覆盖窗口为 `[2024-01-01 05:00:00, 2024-04-01 04:00:00)` UTC，262 个 NYC Zone × 2,183 个完整小时，共 571,946 行，其中 335,531 行零订单。训练/验证/测试分别为 401,122 / 85,936 / 84,888 行。
- 输入整合行程 9,554,576 条，其中 37,569 条非 NYC 范围；标签总数 9,517,007 与纳入的 NYC 行程数一致。Delta 输出版本 1，已回读核验；未运行 B/C 的真实数据训练或路线对照。
- A 的完整针对性 Spark 测试 5/5 通过；B 接口测试 4/4 通过。`git diff --check` 通过。Windows Spark 退出时报告临时 JAR 清理失败，生成与测试命令退出码均为 0。

## 2026-10-02：W4 同学 C 审查与修复

- 审查 PR #13、#14（分支 `c/w4-fixes`）。W4 两套测试 9/9 通过，B 的测试首次在 Windows + JDK 21 下运行；本地 `.venv` 按 `requirements.txt` 补装 `numpy==2.3.5`。
- 修复：原始批次的 manifest 没有 `coverage_window`（只有 incremental apply 写入），`run_ml_dataset` 在按 README 新跑的快照上必然失败；改用 `load_calendar_coverage`，与 Q3–Q5 的覆盖窗口口径一致。测试改用不带窗口的真实 manifest 形态。
- 修复：特征和训练 CLI 改为读取 A 元数据（`--training-metadata`），按其 `output_version` 加载训练集，不读最新版本；`metrics.json` 记录训练集路径、版本和来源 run ID。删除未使用的 parquet 输入。新增回归断言：重写训练表后仍读到元数据登记的版本。
- 本地真实数据按 README 跑通三条命令：训练集 571,946 行、split 401,122 / 85,936 / 84,888、零订单 335,531，与 A 的记录一致；特征向量 299 维。Linear Regression 选中 reg=0.0，test RMSE 13.35、MAE 4.88、R² 0.941；`demand_lag_24h` 基线 20.48 / 5.52 / 0.862；重载核对 20 行一致。耗时：数据集 71 秒、特征 62 秒、训练 78 秒（含 Spark 启动）。运行 `w4-check-20261002`，输出在已忽略的 `data/delta/ml/` 与 `artifacts/w4/`。
