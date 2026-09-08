# DataIntensiveComputingLab

课程作业：使用 PySpark 清洗出租车、天气、空气质量和区域数据，保存为 Delta 表，并生成每行代表一趟行程的整合表，供后续分析使用。

当前状态：A 的通用摄入与 Delta 存储、B 的数据契约与质量处理均已实现并通过全量验证；C 的关联与性能实验仍待完成。

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

## B：标准化与校验

B 的实现位于 `src/dic_pipeline/`，数据规则位于 `configs/datasets.json`，已验证的数据目录与字段契约位于 `docs/`。

在 macOS/Linux 中运行测试：

```bash
PYTHONPATH=src python -m unittest -v
```

在 PowerShell 中运行测试：

```powershell
$env:PYTHONPATH = "src"
python -m unittest -v
```

只运行 B 的转换和校验、暂不写 Delta：

```bash
PYTHONPATH=src spark-submit scripts/run_b_preparation.py all --data-dir "/path/to/lab-data"
```

供 A 调用的接口为：

```python
from dic_pipeline import load_dataset_config, prepare

result = prepare(raw_df, load_dataset_config("taxi"), run_id="batch-id")
# A writes result.accepted, result.rejected and result.metrics.
result.release()
```

必须在 accepted 和 rejected 都写完后调用 `release()`，以释放它们共享的 Spark 缓存。

## Git 协作

克隆私有仓库前，队友需获得仓库访问权限：

```powershell
git clone https://github.com/Mrpooool/DataIntensiveComputingLab.git
cd DataIntensiveComputingLab
```

拉取最新 `main` 后，每人创建自己的功能分支，例如 `feat/integration`。提交前检查 `git diff`，只提交相关代码和配置；推送分支后通过 Pull Request 合并。新增依赖时同步更新版本清单，并通知队友安装。

## A：存储与通用摄入

A 的实现位于 `src/dic_pipeline/ingestion.py`，数据源和输出文件配置位于
`configs/datasets.json`，统一入口为 `scripts/run_ingestion.py`，测试位于
`tests/test_ingestion.py`。

### Windows 从零配置

先安装 Miniconda 或 Anaconda，并确认新 PowerShell 中可以运行 `conda --version`。
然后在项目根目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_env.ps1
```

该脚本会在项目内创建 `.venv`，安装 Python 3.11.9、JDK 21 和
`requirements.txt`，并执行 `pip check`。原生 Windows 的 Spark 本地文件系统还
需要 `.hadoop/bin/winutils.exe` 和 `hadoop.dll`；脚本会从固定地址下载 Hadoop
3.3.6 community helpers，并验证固定 SHA-256。Apache 不发布官方 winutils，因此
若小组不接受第三方 binary，应统一改用 WSL/Linux。

等价的手动命令为：

```powershell
conda create --prefix .\.venv -y -c conda-forge python=3.11.9 openjdk=21 pip
.\.venv\python.exe -m pip install -r requirements.txt
powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows_hadoop.ps1
.\.venv\python.exe -m pip check
```

在 macOS/Linux 中不需要 Windows Hadoop helpers：

```bash
conda create --prefix ./.venv -y -c conda-forge python=3.11.9 openjdk=21 pip
./.venv/bin/python -m pip install -r requirements.txt
./.venv/bin/python -m pip check
```

### 文件结构

```text
configs/
  datasets.json                   # A/B 共用的数据源与规则配置
src/dic_pipeline/
  ingestion.py                    # A：读取、Delta 写入、读回校验、metadata
  preparation.py                  # B：标准化与质量检查接口
scripts/
  run_ingestion.py                # A：四数据集统一入口
tests/
  test_ingestion.py               # A：reader/writer/metadata 测试
```

在 PowerShell 中运行 A/B 全部测试：

```powershell
$env:PYTHONPATH = "src"
.\.venv\python.exe -m unittest -v
```

在 macOS/Linux 中运行测试：

```bash
PYTHONPATH=src ./.venv/bin/python -m unittest -v
```

全量摄入命令（本机 16 GB 内存的已验证配置）：

```powershell
.\.venv\python.exe -m scripts.run_ingestion --dataset all `
  --driver-memory 6g --master "local[4]" --shuffle-partitions 128
```

只运行一个数据集时，将 `all` 改为 `taxi`、`weather`、`air_quality` 或
`taxi_zones`。可以用 `--data-dir`、`--output-root` 和 `--run-id` 覆盖默认输入、
输出和批次号。

供 C 写 integrated 和 benchmark Delta 表的公共接口为：

```python
from dic_pipeline import write_delta

write_delta(frame, output_path, partition_by=["pickup_date"])
```

A 调用 B 的 `prepare()`，写出 accepted/rejected，重新读取两张 Delta 表核对行数，
最后向 `metadata/ingestion_runs` 追加运行状态和统计。

课程问题的书面回答见 `docs/role_a_task2_task3.md`，架构图见
`docs/architecture.md`；课程要求与详细分工见 `Assignment.md` 和 `task_plan.md`。
全量联调已使用同一 `run_id` 成功写入 Taxi 9,554,576、Weather 8,784、
Air Quality 51,885 和 Taxi Zones 265 条标准记录。
