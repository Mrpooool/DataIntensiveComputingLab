# 项目进度

- 2026-09-06：课程方案已审核；Weather/Zone 已读，Taxi/Air 仅确认文件信息。
- 2026-09-07：B 已完成四份真实数据的全量剖析、标准 Schema、字段转换、时区处理、范围筛选、数据质量规则、稳定键和重复处理；6 个 Spark 单元测试通过。
- B 全量结果：Taxi 9,554,778 输入、9,554,576 通过、202 拒绝；Weather 8,784 全部通过；Air 8,139,551 全国输入、范围内 51,885 全部通过；Zone 265 全部通过。
- 2026-09-08（角色 A，`feat/role-a-ingestion-platform`）：
  - 已配置 Python 3.11.9、JDK 21、Spark 4.2.0、Delta 4.4.0 和 Windows Hadoop helpers，`pip check` 通过；
  - 四份输入已整理至 `data/raw/`，并完成通用 reader、可配置 Delta writer、统一 CLI、读回行数校验和运行 metadata；
  - 已接入 `prepare()`，A/B 共 9 个自动化测试通过；
  - 使用同一 `run_id` 全量写入并读回验证四张 standardized、四张 rejected 和 metadata Delta 表，结果与 B 的全量统计一致；
  - 已完成环境配置脚本、存储与摄入说明及架构图。
- 下一步：C 以标准键构建环境小时汇总、整合表和 benchmark。
