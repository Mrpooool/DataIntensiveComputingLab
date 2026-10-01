# 项目执行方案

更新：2026-10-01。依据：[Assignment.md](Assignment.md)。W1–W3 已完成本地实现与材料；W4 已确认同学 B 负责可复用特征工程及模型生命周期。B 的接口、代码、CLI 和小样本测试已实现；A 的训练集及 C 的路线对照仍待接入和全量运行。

## W1–W3 完成摘要

| 周次 | 已完成 | 证据 |
| --- | --- | --- |
| W1 | 四源摄入、标准化、校验、Delta 存储、逐趟整合与存储实验 | 全量 9,554,576 条整合行程；[设计报告](docs/w1_design_report.md)、[数据契约](docs/data_contract.md) |
| W2 | Q1–Q6、四张产品、缓存/裁剪/广播/AQE 对照 | 55 项历史回归、13 项实验；[评测报告](docs/w2_benchmark_report.md) |
| W3 | 增量 MERGE、Schema 白名单、选择性刷新、拒绝隔离、监控和全量评测 | 86 项历史回归，后续物化修复相关 27 项通过；[设计报告](docs/w3_design_report.md)、[评测报告](docs/w3_evaluation_report.md) |

W3 最终实测：增量更新 134.4 秒；full/auto 刷新 66.3/87.2 秒且内容一致；快照存储 +7.9%；校验开销 +26.7%；监控摄入/整合/刷新开销 +12.6%/+13.8%/+59.8%。以上为历史单机结果，本次未重跑。

W3 最终代码及提交包位于本地 `c/w3-fixes`（HEAD `4cdfd3c`）；不据此声称最终修复已合入 main、推送或在课程系统提交。旧记录保留于[计划归档](docs/planning_archive_2026-09-28/task_plan.md)、[发现归档](docs/planning_archive_2026-09-28/findings.md)、[进度归档](docs/planning_archive_2026-09-28/progress.md)。

## W4 目标与建议题目

构建可复用、可复现、可重新训练的 Spark MLlib 流程，并用同一训练集比较 raw 与 integrated 两条准备路线。重点是数据工程与工作流，不要求复杂模型或最高精度。

建议选择**各 NYC Zone 下一小时需求预测**：在小时 h 开始前预测该小时上车数量，每行是 `pickup_location_id × target_hour_utc`，标签为 `trip_count`。可复用 W2 的时间、区域及零需求口径。也可改选行程时长或车费，只做一个任务。

### Phase 0：整理历史与拆解要求

**Status:** complete

- [x] 对照 W4 四个 Task 和四类交付物；归档旧记录，统一 W3 最终状态。
- [x] 写出建议题目、分工、验收顺序及待定接口。

### Phase 1：固定训练集契约（Task 1）

**Status:** pending

- [ ] 确定题目、预测时点、标签、范围、特征及分工；形成 `docs/w4_ml_design.md`，同步 `docs/data_contract.md` 的正式新增契约。
- [ ] 主实验固定原始 2024 年 1–3 月对应的完成快照，记录输入文件标识、Delta 路径/版本和覆盖窗口；先核验快照，不默认当前 `data/delta` 仍是原始基线。
- [ ] 若采用小时需求：用覆盖窗口 × NYC Zone 补齐零订单小时；范围外和环境缺测不能当作零需求。UTC 定位小时，纽约当地时间提取时段/星期，保留 DST 边界。
- [ ] 按完整小时顺序切 train/validation/test，建议约 70%/15%/15%，实际日期边界写入配置；同一小时所有 Zone 属于同一 split。
- [ ] Spark 自动生成带键、标签、原始特征和 split 的训练集，建议存为独立 Delta 表；记录行数、缺失率、标签分布及筛选原因。

### Phase 2：可复用特征工程（Task 2）

**Status:** Role B implementation complete; dataset integration pending

- [x] 时间周期特征（纽约小时/星期/月）、Zone/borough 类别编码、缺失处理、缩放和 `features` 组装已在 `ml_pipeline.py` 实现。
- [ ] A/C 生成训练集时，历史需求只用 h 以前计数；先补齐小时，再算 lag/滚动窗口。B 已把三个 lag 字段和预测时语义写进强制接口。
- [ ] A/C 生成训练集时，历史环境按小时唯一化并滞后一小时。B 已排除目标小时事后实测值，并记录离线发布延迟假设。
- [x] 环境缺失标记保留；填充值、编码器、缩放器只在 train 拟合，validation/test 只 transform；特征输出删除无关字段。
- [x] 使用 `configs/ml.json` + 普通 Spark Pipeline，可配置特征列表，并能单独保存/复用预处理 PipelineModel。

### Phase 3：训练、评估、保存和再训练（Task 3）

**Status:** Role B implementation complete; real-data run pending

- [x] 用 `demand_lag_24h` 建需求基线，并完成 MLlib Linear Regression 候选流水线。
- [x] validation RMSE 选参数，test 只对胜出模型评估一次；记录 MAE、RMSE、R² 并对照基线，不使用 MAPE。
- [x] 保存完整预处理+回归 PipelineModel、配置快照、输入路径、split 行数、候选参数、环境版本、指标和耗时；A 仍需提供正式 Delta 快照版本及 split 日期边界。
- [x] 保存后重新加载模型，对按键排序的固定样本逐条按容差核对预测。
- [x] `scripts.run_ml_training` 是训练与新快照重训练的同一入口，每个 run ID 保存独立产物；真实新快照实验仍待 A/C 数据。

### Phase 4：raw 与平台路线对照（Task 4）

**Status:** pending

这里的 Approach A/B 是作业路线名称，与成员 A/B 无关。

- [ ] Approach A：从四份原始文件加载、清洗、校验、整合，再做特征工程；不能读取已有整合表。准备步骤在代码和计时中明确呈现，口径与平台一致。
- [ ] Approach B：从固定版本 integrated Delta 读取，再做相同特征工程；说明平台已承担的摄入/校验/整合工作。
- [ ] 两路采用相同原始范围、标签、特征、split、模型参数和环境；共用后续特征/训练逻辑，先核对样本键、标签和特征内容，再比较耗时。
- [ ] 分别测准备、特征处理、模型训练和总耗时；执行 Spark action，区分预处理拟合与模型拟合，记录预热/缓存规则和重复样本，不预设平台让训练本身更快。
- [ ] 比较实现复杂度、预处理复杂度、训练时间、可复现性，用具体代码及保存产物佐证。
- [ ] 建议做特征组对照（时间/位置基础、加历史需求、加环境）支撑数据贡献讨论；这是建议实验，不是作业硬性模型数量要求。

### Phase 5：验证与交付

**Status:** pending

- [ ] 针对性测试：零订单、小时键/DST、split 不交叉、历史窗口不读未来、train-only 拟合、未知类别/缺测、两路样本一致、保存加载和再训练。
- [ ] 跑受影响测试；若改共享平台模块，跑全部测试。在独立输出目录完成真实数据端到端及路线对照，保存环境、快照、参数和原始样本。
- [ ] 完整源码/配置/测试、3–5 页设计报告、简短 evaluation report、README；说明训练集生成、特征处理、训练评估、对照复现及新数据再训练，同步中文说明。
- [ ] 回答各 Task 讨论题：特征与假设、预处理负担、复用和扩展、多任务支持、新数据集接入、前三周工程决定及未来改进。
- [ ] `git diff --check`、按 README 复现、核对提交包；模型和生成数据放已忽略的 `data/` 或 `artifacts/`。

## 建议三人分工与交接

| 角色 | 建议任务 | 交接物 |
| --- | --- | --- |
| A：数据集与入口 | Task 1 数据集生成、Delta 落盘、split/快照；把正式快照交给共用训练入口 | 训练集 Schema、固定快照、生成 CLI、小样本 |
| B：特征与模型 | Task 2 特征流程；Task 3 模型、指标、保存加载和重训练入口 | train-only Pipeline、特征可用时间、训练 CLI |
| C：对照与交付 | Task 4 raw 路线、公平对照、特征组实验、最终联调及材料 | 双路线一致性、分阶段计时、evaluation report |

键、标签、split、特征可用时间和 B 的输入接口已固定在 `configs/ml.json`、`docs/data_contract.md` 与 `docs/w4_role_b_features_model.md`。B 已创建 `ml_pipeline.py` 及两个薄 CLI；A/C 应共用该下游实现，不复制特征和训练逻辑。

## 下一步与错误记录

下一步：A 产出符合契约的正式训练 Delta（含快照版本和 split 边界），随后运行 B 的针对性 Spark 测试和真实数据训练；C 再用相同接口接 raw 路线并做公平对照。

| 本次错误 | 次数 | 处理 |
| --- | --- | --- |
| apply_patch 拒绝同补丁对同路径 Delete/Add | 1 | 无文件被该补丁改写；归档后改为直接写入文档 |
| JSON 管道带 BOM，首次解析失败 | 1 | 用 utf-8-sig 解码后解析，未改动文件内容 |
| PowerShell 管道默认编码把新写入中文转为问号 | 1 | 改用 ASCII 转义 JSON 传递并以 UTF-8 写入；重新核验中文和归档正文 |
| 当前 macOS 无 Java Runtime，Spark 针对性测试无法启动 | 1 | 语法编译和配置导入通过；保留测试，需在 README 规定的 JDK 21 环境运行 |
| 本地 `.venv` 的 Python 3.11.9 符号链接失效 | 1 | 未改用户环境；用可用 Python 完成静态检查，正式验证前按 setup 脚本重建环境 |

W3 错误保留在归档；后续特别注意宿主时区、Spark 惰性计算重复执行及模拟更新的评估边界。
