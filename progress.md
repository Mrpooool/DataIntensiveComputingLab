# 项目进度

- 2026-09-06：课程方案已审核；Weather/Zone 已读，Taxi/Air 仅确认文件信息。
- Python 3.11.9 / JDK 21 / Spark 4.2.0 / Delta 4.4.0；依赖已安装并固定版本，`pip check` 通过。
- 2026-09-06（角色 A，`feat/role-a-ingestion-platform`）：
  - 已配置 `.venv` 和 Windows 本地 Spark 环境，四份输入已整理至 `data/raw/`；
  - 以 `configs/datasets.py`、`src/ingestion.py`、`scripts/run_ingestion.py` 完成最小通用导入框架；
  - Delta 写入/读回测试及 Zone 265 行框架测试通过。
- 下一步（B）：实现四个正式 transform（标准类型、时间、去重、质量规则）。
- 下一步（C）：在四张正式标准表就绪后实现 integration 与 benchmark。
