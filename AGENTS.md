# Repository Guidelines

## Project Structure & Module Organization

- `src/dic_pipeline/`: A owns ingestion and Delta storage; B owns schemas, transforms, validation, and preparation; C owns integration and performance experiments.
- `scripts/`: environment setup and pipeline entry points.
- `tests/`: Python `unittest` suites using small Spark fixtures and temporary Delta tables.
- `configs/datasets.json`: source paths, output settings, and cleaning rules.
- `docs/`: data catalog, contracts, and architecture. Read `Assignment.md` and `task_plan.md` for course requirements and responsibilities.
- `data/raw/` and `data/delta/`: local inputs and generated tables; excluded from Git.

## Build, Test, and Development Commands

Use Python 3.11.9, JDK 21, and the exact package versions in `requirements.txt`. Run these commands from the repository root in PowerShell:

```powershell
# Create the virtual environment and install dependencies/Windows Hadoop helpers
powershell -ExecutionPolicy Bypass -File .\scripts\setup_env.ps1

# Run all tests
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# Ingest all four datasets, then create the integrated Delta table
.\.venv\Scripts\python.exe -m scripts.run_ingestion --dataset all
.\.venv\Scripts\python.exe -m scripts.run_integration
```

Download inputs into `data/raw/` before ingestion. Integration requires a completed four-table batch. Run one ingestion process per output directory.

## Coding Style & Naming Conventions

Use four-space indentation, `snake_case` functions/columns, `PascalCase` classes, and `UPPER_CASE` constants. Follow existing type hints and short docstrings. Keep transformations in reusable modules and CLI handling in `scripts/`. No formatter or linter is currently configured.

Prefer readable, direct code. Avoid speculative abstractions and excessive fallback logic. Keep user-facing explanations concise and in Chinese unless requested otherwise.

## Testing Guidelines

Name files `test_*.py` and methods `test_*`. No numeric coverage threshold is configured. Add focused regression tests for behavior changes: timestamp parsing, invalid rows, duplicate keys, join row preservation, and Delta readback. Run affected suites; run all suites for shared pipeline changes. Report actual results and distinguish fixture tests from full-data runs.

## Commit & Pull Request Guidelines

History uses short, imperative subjects such as `Complete role A ingestion integration`; Conventional Commits are not required. Work on a personal feature branch and preserve unrelated edits. PRs should describe the change, affected course tasks, validation, and contract changes. Check `git diff --check` before submission.

Never commit `.venv/`, `.hadoop/`, datasets, generated tables, or secrets. Update `docs/data_contract.md` when changing field names, units, time assumptions, or join rules.
