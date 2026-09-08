# DataIntensiveComputingLab

课程作业：使用 PySpark 清洗出租车、天气、空气质量和区域数据，保存为 Delta 表，并生成每行代表一趟行程的整合表，供后续分析使用。

当前状态：依赖已安装且 `pip check` 通过；Spark/Delta 读写、课程处理流程和性能实验尚未验收。

## 环境安装

三人统一使用 **Python 3.11.9、JDK 21、PySpark 4.2.0、Delta 4.4.0**。Python 包及其间接依赖版本见 `requirements.txt`。

先安装 Python 和 JDK，将 `JAVA_HOME` 指向 JDK 目录，并将其 `bin` 加入 `PATH`。以下命令在项目根目录的 PowerShell 执行：

```powershell
# 确认 python 指向 3.11.9；否则改用该版本 python.exe 的完整路径
python --version
java -version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
```

每人创建自己的 `.venv`，不要复制或提交虚拟环境。`pip check` 只检查 Python 依赖关系，后续还需验证 Spark 启动和 Delta 写入、读回。

## 数据与文档

- [课程要求](Assignment.md)：任务、数据下载链接和交付要求。
- [W1 执行方案](task_plan.md)：接口、关联规则及验收要求。
- [审核记录](findings.md)：真实数据检查结果及待确认事项。
- 原始数据自行下载到 `data/raw/`，空气质量 ZIP 先解压；运行生成的表放在 `data/delta/`。`data/` 不提交 Git。

## W1 分工

| 角色 | 主负责 Task | 工作 |
| --- | --- | --- |
| A | 2、3 | 存储设计、通用导入和运行入口 |
| B | 1、4 | 数据目录、字段约定、清洗与校验 |
| C | 5、6 | 数据关联、两种存储方案的性能实验 |

先共同确认字段和时间规则，再并行实现；按老师 Task 1–6 验收。W1 交付代码、3–5 页设计报告、架构图、简短性能报告和本 README。

## Git 协作

克隆私有仓库前，队友需获得仓库访问权限：

```powershell
git clone https://github.com/Mrpooool/DataIntensiveComputingLab.git
cd DataIntensiveComputingLab
```

拉取最新 `main` 后，每人创建自己的功能分支，例如 `feat/integration`。提交前检查 `git diff`，只提交相关代码和配置；推送分支后通过 Pull Request 合并。新增依赖时同步更新版本清单，并通知队友安装。

## 角色 A 实现

角色 A 的 Task 2、Task 3 实现按职责放置：

```text
configs/
  datasets.py             # 路径、格式、必需字段、输出位置
src/
  ingestion.py            # 通用读取、schema 检查、Delta 写入、metadata
  transforms.py           # B 负责的数据集专用转换（待合并）
scripts/
  run_ingestion.py        # 命令行运行入口
```

通用部分支持 CSV/Parquet、必需字段校验、`snake_case`、accepted/rejected
对账、Delta 输出及摄入统计。数据集专用的 timestamp/type 转换、去重和质量规则由
B 在 `src/transforms.py` 中实现。

B 的 `src/transforms.py` 合并后运行：

```powershell
.\.venv\python.exe -m scripts.run_ingestion --dataset all
# 或只运行一个
.\.venv\python.exe -m scripts.run_ingestion --dataset weather
```

课程问题的书面回答见 `docs/role_a_task2_task3.md`；课程要求与详细分工见
`Assignment.md` 和 `task_plan.md`。
