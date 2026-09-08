# 项目进度

- 2026-09-06：课程方案已审核；Weather/Zone 已读，Taxi/Air 仅确认文件信息。
- Python 3.11.9 / JDK 21 / Spark 4.2.0 / Delta 4.4.0；依赖已安装并固定版本，`pip check` 通过。
- 2026-09-06（角色 A，`feat/role-a-ingestion-platform`）：
  - 已配置 `.venv` 和 Windows 本地 Spark 环境，四份输入已整理至 `data/raw/`；
  - 以 `configs/datasets.py`、`src/ingestion.py`、`scripts/run_ingestion.py` 完成最小通用导入框架；
  - Delta 写入/读回测试及 Zone 265 行框架测试通过。
- 2026-09-07：B 已完成四份真实数据的全量剖析、标准 Schema、字段转换、时区处理、范围筛选、数据质量规则、稳定键和重复处理；6 个 Spark 单元测试通过。
- B 全量结果：Taxi 9,554,778 输入、9,554,576 通过、202 拒绝；Weather 8,784 全部通过；Air 8,139,551 全国输入、范围内 51,885 全部通过；Zone 265 全部通过。
- 下一步：A 接入 `prepare()` 并写 accepted/rejected/metadata Delta 表；C 以标准键构建环境小时汇总和整合表。
