# 项目执行方案

更新：2026-10-03。依据：[Assignment.md](Assignment.md)。W1–W3 已完成并合入 main。W4 四个 Task 的代码、全量实验、设计报告和评测报告已在 `c/w4-fixes` 完成；待全量测试、队友审阅、提交包和 PR。

## W1–W3 完成摘要

| 周次 | 已完成 | 证据 |
| --- | --- | --- |
| W1 | 四源摄入、标准化、校验、Delta 存储、逐趟整合与存储实验 | 全量 9,554,576 条整合行程；[设计报告](docs/w1_design_report.md)、[数据契约](docs/data_contract.md) |
| W2 | Q1–Q6、四张产品、缓存/裁剪/广播/AQE 对照 | 55 项历史回归、13 项实验；[评测报告](docs/w2_benchmark_report.md) |
| W3 | 增量 MERGE、Schema 白名单、选择性刷新、拒绝隔离、监控和全量评测 | 86 项历史回归，后续物化修复相关 27 项通过；[设计报告](docs/w3_design_report.md)、[评测报告](docs/w3_evaluation_report.md) |

W3 最终实测：增量更新 134.4 秒；full/auto 刷新 66.3/87.2 秒且内容一致；快照存储 +7.9%；校验开销 +26.7%；监控摄入/整合/刷新开销 +12.6%/+13.8%/+59.8%。以上为历史单机结果，本次未重跑。

W3 最终代码已通过 PR #11、#12 合入 main；课程系统提交状态未核验。旧记录保留于[计划归档](docs/planning_archive_2026-09-28/task_plan.md)、[发现归档](docs/planning_archive_2026-09-28/findings.md)、[进度归档](docs/planning_archive_2026-09-28/progress.md)。

## W4 目标与建议题目

构建可复用、可复现、可重新训练的 Spark MLlib 流程，并用同一训练集比较 raw 与 integrated 两条准备路线。重点是数据工程与工作流，不要求复杂模型或最高精度。

建议选择**各 NYC Zone 下一小时需求预测**：在小时 h 开始前预测该小时上车数量，每行是 `pickup_location_id × target_hour_utc`，标签为 `trip_count`。可复用 W2 的时间、区域及零需求口径。也可改选行程时长或车费，只做一个任务。

### Phase 0：整理历史与拆解要求

**Status:** complete

- [x] 对照 W4 四个 Task 和四类交付物；归档旧记录，统一 W3 最终状态。
- [x] 写出建议题目、分工、验收顺序及待定接口。

### Phase 1：固定训练集契约（Task 1）

**Status:** complete（设计报告 `docs/w4_design_report.md`）

- [x] 固定 A 的预测时点、标签、范围、特征与来源要求；见 `docs/w4_role_a_training_dataset.md`、`configs/ml.json` 和 `docs/data_contract.md`。W4 总设计由后续联调统一整理。
- [x] 主实验固定原始 2024 年 1–3 月对应的完成快照，核对四类输入文件标识、Delta 路径/版本和覆盖窗口；拒绝 W3 模拟更新快照。
- [x] 用覆盖窗口 × NYC Zone 补齐零订单小时；范围外和环境缺测不当作零需求。UTC 定位小时，纽约当地时间由 B 的特征流水线提取，保留 DST 边界。
- [x] 按完整小时顺序切 train/validation/test，约 70%/15%/15%，边界写入配置；同一小时所有 Zone 属于同一 split。
- [x] Spark 生成带键、标签、原始特征和 split 的独立 Delta 训练表；记录行数、缺失率、标签分布及筛选原因。

### Phase 2：可复用特征工程（Task 2）

**Status:** complete

- [x] 时间周期特征（纽约小时/星期/月）、Zone/borough 类别编码、缺失处理、缩放和 `features` 组装已在 `ml_pipeline.py` 实现。
- [x] 先补齐小时，再只用 h 以前计数计算 lag/滚动窗口；raw 路线共用同一 builder，全量逐行一致。
- [x] 环境按小时唯一化并滞后一小时；raw 路线自行做 PM2.5 两级中位数，结果一致。离线发布延迟假设已记录。
- [x] 环境缺失标记保留；填充值、编码器、缩放器只在 train 拟合，validation/test 只 transform；特征输出删除无关字段。
- [x] 使用 `configs/ml.json` + 普通 Spark Pipeline，可配置特征列表，并能单独保存/复用预处理 PipelineModel。

### Phase 3：训练、评估、保存和再训练（Task 3）

**Status:** complete（主训练 `main-20261003`，重训演示 `retrain-w3-update`）

- [x] 用 `demand_lag_24h` 建需求基线，并完成 MLlib Linear Regression 候选流水线。
- [x] validation RMSE 选参数，test 只对胜出模型评估一次；记录 MAE、RMSE、R² 并对照基线，不使用 MAPE。
- [x] 保存完整预处理+回归 PipelineModel、配置快照、训练集路径/Delta 版本/来源 run ID、split 行数、候选参数、环境版本、指标和耗时；特征和训练按 A 元数据登记的 `output_version` 读取训练集。
- [x] 保存后重新加载模型，对按键排序的固定样本逐条按容差核对预测。
- [x] `scripts.run_ml_training` 是训练与新快照重训练的同一入口，每个 run ID 保存独立产物；已在 W3 更新快照上用 `configs/ml_w3_update.json` 演示（`retrain-w3-update`，仅演示机制）。

### Phase 4：raw 与平台路线对照（Task 4）

**Status:** complete（全量运行 `20261002T155150Z`）

这里的 Approach A/B 是作业路线名称，与成员 A/B 无关。

- [x] Approach A：`ml_raw_route.py` 从六个原始文件自行转换时区、套用平台的 Taxi 拒绝规则和去重、筛选纽约 Air 站点并做两级中位数；不读平台任何表。
- [x] Approach B：`platform_training_dataset` 读钉定快照；平台已承担的摄入/校验/整合写入评测报告。
- [x] 两路共用 builder、B 的流水线和同一配置；预热后先逐行对比（0 行差异），之后每次运行须复现同一哈希和 test 指标。
- [x] 分阶段计时：准备、特征拟合、模型拟合、训练合计、端到端；预热 1 次 + 交替 3 次取中位数；训练本身两路相同。
- [x] 实现复杂度（97 行对 8 行）、预处理复杂度、训练时间、可复现性写入评测报告。
- [x] 特征组对照：仅 Taxi、+Weather、+Air；环境特征对线性模型无提升。

### Phase 5：验证与交付

**Status:** in progress

- [x] 针对性测试：零订单、小时键/DST、split 不交叉、历史窗口不读未来、train-only 拟合、未知类别/缺测、两路样本一致、保存加载和再训练。
- [ ] 跑受影响测试；若改共享平台模块，跑全部测试。在独立输出目录完成真实数据端到端及路线对照，保存环境、快照、参数和原始样本。
- [x] 完整源码/配置/测试、3–5 页设计报告、简短 evaluation report、README；说明训练集生成、特征处理、训练评估、对照复现及新数据再训练，同步中文说明。
- [x] 回答各 Task 讨论题：特征与假设、预处理负担、复用和扩展、多任务支持、新数据集接入、前三周工程决定及未来改进。
- [ ] `git diff --check`、按 README 复现、核对提交包；模型和生成数据放已忽略的 `data/` 或 `artifacts/`。

## 建议三人分工与交接

| 角色 | 建议任务 | 交接物 |
| --- | --- | --- |
| A：数据集与入口 | Task 1 数据集生成、Delta 落盘、split/快照；把正式快照交给共用训练入口 | 训练集 Schema、固定快照、生成 CLI、小样本 |
| B：特征与模型 | Task 2 特征流程；Task 3 模型、指标、保存加载和重训练入口 | train-only Pipeline、特征可用时间、训练 CLI |
| C：对照与交付 | Task 4 raw 路线、公平对照、特征组实验、最终联调及材料 | 双路线一致性、分阶段计时、evaluation report |

键、标签、split、特征可用时间和 B 的输入接口已固定在 `configs/ml.json`、`docs/data_contract.md` 与 `docs/w4_role_b_features_model.md`。B 已创建 `ml_pipeline.py` 及两个薄 CLI；A/C 应共用该下游实现，不复制特征和训练逻辑。

## 下一步与错误记录

下一步：全量测试通过后由队友审阅两份报告；再生成提交包（报告 PDF + 源码），推送 `c/w4-fixes` 并开一个 PR。

| 本次错误 | 次数 | 处理 |
| --- | --- | --- |
| apply_patch 拒绝同补丁对同路径 Delete/Add | 1 | 无文件被该补丁改写；归档后改为直接写入文档 |
| JSON 管道带 BOM，首次解析失败 | 1 | 用 utf-8-sig 解码后解析，未改动文件内容 |
| PowerShell 管道默认编码把新写入中文转为问号 | 1 | 改用 ASCII 转义 JSON 传递并以 UTF-8 写入；重新核验中文和归档正文 |
| 当前 macOS 无 Java Runtime，Spark 针对性测试无法启动 | 1 | 语法编译和配置导入通过；保留测试，需在 README 规定的 JDK 21 环境运行 |
| 本地 `.venv` 的 Python 3.11.9 符号链接失效 | 1 | 未改用户环境；用可用 Python 完成静态检查，正式验证前按 setup 脚本重建环境 |

W3 错误保留在归档；后续特别注意宿主时区、Spark 惰性计算重复执行及模拟更新的评估边界。
