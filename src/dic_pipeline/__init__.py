"""Standardization and data-quality components owned by role B."""

from .contracts import load_dataset_config
from .ingestion import create_spark, ingest_dataset, read_source, write_delta
from .preparation import PreparationResult, SchemaValidationError, prepare

__all__ = [
    "PreparationResult",
    "SchemaValidationError",
    "create_spark",
    "ingest_dataset",
    "load_dataset_config",
    "prepare",
    "read_source",
    "write_delta",
]
