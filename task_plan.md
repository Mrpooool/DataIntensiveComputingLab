# 项目执行方案

依据：[课程要求](Assignment.md)。W1、W2 已完成；W3 实现中：C 的阶段 ① 已完成，A、B 尚未开始。

## W1 完成总结

- 已实现四份原始数据的通用摄入、标准化、校验、去重和 Delta 存储，并生成逐趟整合表 `integrated_taxi_trips`（天气、PM2.5、上下车区域和 borough）。
- 验证：2026-09-09 共 27 项小样本测试通过；全量保留 9,554,576 条唯一行程；S0/S1 存储实验完成且查询结果一致。
- 提交包在 `main`：`submissions/Week1_submission_2026-09-13.zip`。规则见 [设计报告](docs/w1_design_report.md)、[数据契约](docs/data_contract.md)、[benchmark report](docs/benchmark_report.md)。
- 环境固定 Python 3.11.9、JDK 21、`requirements.txt`；UTC 存储、纽约当地时间分析、同小时环境关联与缺测保留规则继续沿用。

## W2 完成总结

目标：六个 Spark SQL 查询、至少四张可复用汇总 Delta 表、四类优化对照实验。

| 负责人 | 主要任务 | 交付现状 |
| --- | --- | --- |
| A：数据产品与运行入口 | 四张汇总表、刷新元数据、CLI、配置 | 已合 `main`（PR #4）；审查修复后环境产品口径与 Q1–Q6 对齐 |
| B：分析口径与查询 | Q1–Q6 Spark SQL、统计规则、小样本测试 | 已合 `main`（PR #5）；设计说明见 `docs/role_b_query_design.md` |
| C：优化与 benchmark | 缓存 / 分区裁剪 / 广播 / AQE；结果核对与计时 | 13 项实验完成；`docs/w2_benchmark_report.md`、`docs/w2_design_optimization.md` 已写；已合 `main`（PR #7） |

要点：审查五项问题已修；完整回归 55 项通过；全量实验运行 `20260919T143454Z-9d921a51`。W2 口径与产品接口是 W3 增量刷新的基线，不可无说明地改动。

## W3 目标与分工

目标：把平台升级为可增量更新、可演进 Schema、可监控、可维护的生产态；在不整库重建的前提下处理新数据与重复记录，并保持分析产品可用。

| 负责人 | 主要任务 | 交付与职责边界 |
| --- | --- | --- |
| A：增量更新与分析一致性 | 生成各数据集的增量更新文件，实现增量写入管道；在新数据与 Schema 演进后刷新受影响的分析产品并保持查询可用 | 更新文件生成器、增量 merge、产品选择性/增量刷新与测试；设计报告中的增量策略与分析一致性说明 |
| B：校验扩展与一致性规则 | 扩展校验框架；定义 Schema 演进策略与分析一致性规则（哪些产品可增量刷新、哪些需全量重算） | 校验规则库与隔离拒绝记录；设计报告中的校验框架、Schema 策略、一致性讨论 |
| C：监控、评测与材料汇总 | 搭建平台监控；完成生产就绪评测；最终联调、README、设计/evaluation 报告整合与提交包 | 监控表与运维查询、评测代码与 evaluation report；汇总交付材料 |

各自维护对应测试；规则与 Schema 策略由 B 维护，增量/产品刷新由 A 维护，监控与评测口径由 C 维护。C 负责最终联调与材料汇总。

## 1. 增量更新数据集与管道（A 实现，B 定义 Schema 策略）

为 Taxi / Weather / Air Quality 各生成一份**仅含新增或修改记录**的更新文件（非全量重发），并实现增量管道。

| 数据集 | 更新文件要求 | 管道行为 |
| --- | --- | --- |
| Taxi Trips（Parquet） | 约 5–10% 新行程，时间晚于原数据最新上车时间；另含约 1–2% 从原数据复制的重复行程 | 插入新行；忽略重复；保留未变行 |
| Weather（CSV） | 接续原数据之后的新小时观测；新增列 `humidity`（约 20–100 的相对湿度百分比） | 支持 Schema 演进；写入对应 Delta 表 |
| Air Quality（CSV） | 接续原数据之后的新小时观测；新增列 `aqi`（约 0–500） | 同上 |

- 更新文件必须语法合法，并遵循原 Schema（显式演进列除外）。
- 管道不得整库重建；应更新标准化 / 整合相关 Delta 表后，使后续分析仍可读取。
- A 记录每个更新文件的新行数、重复行数（如适用）与 Schema 变更；B 事先固定：哪些演进可自动接受（如可空新列）、哪些必须人工介入（如改类型、删列、改主键）。
- Zone Lookup 本周无增量要求；若管道统一处理四表，对 Zone 明确为 no-op 并写进配置。

## 2. 分析一致性（A 落盘刷新，B 定规则）

W2 的六个查询与四张产品在增量与 Schema 演进后仍须正确、尽量少重算。

| 产品（W2） | 初步刷新策略（首阶段由 B 确认） |
| --- | --- |
| `daily_mobility_summary` | 优先按受影响当地日/小时增量重算后合并 |
| `taxi_zone_statistics` | 优先按受影响月份 × Zone 增量重算 |
| `weather_impact_summary` | Schema 含天气分类时可能需更大范围重算；B 判定增量边界 |
| `air_quality_impact_summary` | 新小时可追加；`aqi` 等新列策略由 B 规定（自动忽略 / 扩展 Schema / 人工） |

共同规则：

- 只刷新受新数据或 Schema 变更影响的产品；能证明不受影响的跳过。
- 尽量保持现有分析查询兼容；破坏性 Schema 变更必须有显式迁移步骤与版本号。
- W2 已约定全量覆盖刷新可重复执行；W3 在此之上增加增量路径，失败不得把产品标为已刷新。
- 讨论题由 B 起草、A 用实现与耗时佐证：哪些可增量、哪些必须全量、哪些 Schema 变更可自动处理、如何减少无谓计算。

## 3. 平台监控（C）

每次管道执行自动写入监控元数据（已定为单表 `data/delta/metadata/pipeline_runs/`，取代 W1/W2 的 `ingestion_runs` 与 `product_refresh_runs`）。至少记录：

- 管道执行时间、处理行数、插入行数、拒绝行数、Schema 版本、校验失败计数。

提供 Spark SQL（或薄封装）回答：哪类数据集最常校验失败、谁最耗时、每次拒绝多少行、多次执行耗时如何变化。

C 在报告中讨论：哪些指标最有用、如何支撑排障与维护、生产环境还缺什么。监控写入失败不得静默吞掉业务失败状态。A 的增量管道与产品刷新须回传约定计数，供 C 写入监控表。

**现状（`bfe33a2`）**：表、写入接口（`run_row` + `record_run`）、五条运维 SQL（`scripts.run_monitoring_report`）已完成，摄入、整合、产品刷新已接入；A 的 `incremental_update` 阶段按 [docs/w3_interfaces.md](docs/w3_interfaces.md) 写入同一张表。计数口径：`processed = inserted + updated + duplicate + rejected`，其中 `duplicate` 只指目标表中已存在的键。

## 4. 扩展校验框架（B）

在 W1 校验之上自动检测：重复记录、非法属性值、缺失引用（如 Zone）、意外或不支持的 Schema 变更、不完整记录。

无效记录须：不中断整批处理、被隔离、被报告、不进入分析产品。新规则应能以最小改动注册（配置或插件式函数），区分通用规则与数据集专用规则。

与 A 的交接：增量管道调用同一套校验入口；拒绝行写入既有 `rejected` 路径或等价结构，并回传计数给 C 的监控。

## 5. 生产就绪评测与材料汇总（C）

在固定机器与同一基线快照上测量：

- 增量更新耗时、分析刷新耗时、更新后存储开销、校验额外耗时、监控额外耗时。

讨论（须用实测与实现例子）：W1 哪些设计简化了维护、改动最大的组件、对未来新数据集的支持程度、若今日重做会改什么。评测入口为 `scripts/run_w3_evaluation.py`，结果写入 `data/benchmark/w3/<run_id>/`。

**现状（`bfe33a2`）**：骨架完成。每次运行（含预热）都复制一份基线到独立目录，变体交替，输出计数不一致不报时间。三项监控开销可测；`incremental_update`、`analytical_refresh`、`storage_overhead`、`validation_overhead` 为 pending，等 A 的 `apply_updates` / `refresh_data_products(mode=)` 与 B 的 `validate` 开关。

C 同时负责最终联调、英文/中文 README、设计报告与 evaluation report 整合、提交包。

## 6. 三个交接接口

| 交接 | 首阶段需要固定的内容 |
| --- | --- |
| B → A/C：规则接口 | 校验规则注册方式、拒绝原因码、Schema 演进允许列表、各产品刷新边界（增量键 / 必须全量条件） |
| A → B/C：增量与产品接口 | 更新文件路径与格式、增量管道入口、写入后的批次/版本 manifest、产品刷新 CLI、整合表是否重跑及版本发布方式 |
| C → A/B：运维与评测接口 | 监控表 Schema 与写入 API、运维查询入口、评测脚本如何读取增量/刷新/监控耗时（已写入 [docs/w3_interfaces.md](docs/w3_interfaces.md)） |

建议模块：`incremental.py` / 更新文件生成与扩展 `data_products`（A）、扩展 `validation`（B）、`monitoring.py` + 评测脚本与交付汇总（C）。保持普通函数与配置驱动，不另搭框架。

## 7. 推进顺序与验收

| 阶段 | A | B | C | 完成标志 |
| --- | --- | --- | --- | --- |
| ① 接口与最小链路 | 生成一份 Taxi 更新文件并 merge；一产品可按开关全量或按日刷新 | 固定 Schema 允许列表与 2–3 条新校验规则 | 监控表最小写入打通 | 一次增量 → 校验 → 监控有记录 → 一产品可刷新 |
| ② 并行实现 | Weather/Air 更新（含新列）+ 整合表增量路径 + 四产品选择性刷新 | 完整校验扩展 + 一致性规则文档 | 完整监控查询；评测框架骨架 | 模块测试通过；拒绝行不进入产品 |
| ③ 全量评估 | 提供增量与刷新耗时可复现入口 | 复核 Schema/校验讨论题 | 跑评测五类耗时与存储开销 | 有前后对照数字，结论有证据 |
| ④ 交付 | 增量与一致性实现说明交 C 汇总 | 完成校验与一致性设计说明 | 整合 README、evaluation report、提交包 | 按 README 在独立输出目录可复现 |

当前进度（2026-09-24）：`c/pipeline-fixes` 已由 PR #7 合入 `main`。C 的阶段 ① 已完成：`pipeline_runs` 监控表、写入接口与五条运维 SQL、评测框架骨架，接口约定见 [docs/w3_interfaces.md](docs/w3_interfaces.md)，其中标 **agree** 的条目待 A/B 确认。A、B 的实现尚未开始。

- [ ] 三份增量更新文件可生成，并记录新行/重复行/Schema 变更。
- [ ] 增量管道：插入新行、忽略重复、保留未变行、支持约定内 Schema 演进，且不整库重建。
- [ ] 受影响分析产品可刷新；查询在演进后仍兼容或有明确迁移；无效记录不进入产品。
- [x] 监控表记录约定字段，并有 SQL/入口回答课程四个运维问题（`bfe33a2`；增量阶段的行待 A 接入）。
- [ ] 校验框架可扩展，通用与专用规则边界清晰。
- [ ] 评测覆盖增量、刷新、存储、校验、监控五类开销，结论有实测支撑。
- [ ] 受影响测试通过；改共享模块时跑完整测试；交付前 `git diff --check`。
- [ ] 提交完整代码/配置/测试、3–5 页英文设计报告、简短英文 evaluation report、简洁英文 README；同步中文 README。

## W3 C 阶段状态

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| C① 监控最小链路 | `pipeline_runs` 表、写入接口、三处阶段接入、`--no-monitoring` | complete（`bfe33a2`） |
| C② 运维查询与评测骨架 | 五条 SQL、`--import-legacy`、`w3_evaluation.py` 与 CLI、pending 机制 | complete（`bfe33a2`） |
| C③ 接口对齐 | 把 [docs/w3_interfaces.md](docs/w3_interfaces.md) 发给 A、B，确认标 **agree** 的条目；时间窗口一条最先定 | in_progress |
| C④ 放宽溯源校验 | `verify_integrated_provenance` 改为 lineage 子集检查 | pending（等 A 的 manifest 形状） |
| C⑤ 全量评测 | A/B 交付后在同一代码版本上一次跑完全部七项，监控开销也不提前单跑（摄入代码还会变） | pending |
| C⑥ 交付材料 | evaluation report、设计报告整合、中英 README、提交包 | pending |

分支 `c/w3-monitoring` 只在本地，按约定整周做完再统一提 PR。

## W3 C 遇到的错误

| 错误 | 尝试次数 | 解决方案 |
| --- | --- | --- |
| 用切片脚本删除 `REFRESH_METADATA_SCHEMA` 时吞掉了一个空格，得到 `ProductBuilder =Callable` | 1 | `git diff` 审阅时发现，用 sed 修正；语法本身合法，测试未受影响 |
| Bash 中 `cd` 进 `sql/monitoring/` 后工作目录被保留，后续相对路径命令落在错误目录 | 1 | 之后的命令都先 `cd` 到仓库根目录的绝对路径 |
