from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "datasets.json"


def load_dataset_config(
    dataset: str, config_path: str | Path = DEFAULT_CONFIG_PATH
) -> dict[str, Any]:
    """Load one dataset contract from the shared JSON configuration."""
    with Path(config_path).open(encoding="utf-8") as stream:
        configs = json.load(stream)
    if dataset not in configs:
        available = ", ".join(sorted(configs))
        raise KeyError(f"Unknown dataset {dataset!r}; available datasets: {available}")
    return dict(configs[dataset])
