"""
Shared helpers for dataset-scoped ONS Airflow DAGs.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

REQUIRED_DATASET_KEYS = ["dataset_id", "edition", "version", "raw_table_name"]


def load_enabled_dataset_config(config_path: Path) -> list[dict[str, Any]]:
    """
    Load enabled ONS dataset entries from the shared YAML config file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"ONS dataset config does not exist: {config_path}")

    with config_path.open() as file:
        config = yaml.safe_load(file) or {}

    datasets = config.get("datasets")

    if not isinstance(datasets, list):
        raise ValueError(f"Config file must contain a datasets list: {config_path}")

    enabled_datasets = [
        dataset
        for dataset in datasets
        if dataset.get("enabled", True)
    ]

    for dataset in enabled_datasets:
        for key in REQUIRED_DATASET_KEYS:
            if key not in dataset:
                raise ValueError(f"Dataset config missing required key {key}: {dataset}")

    return enabled_datasets


def dataset_dag_suffix(dataset: dict[str, Any]) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured dataset edition.
    """
    name = f"{dataset['dataset_id']}_{dataset['edition']}"
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_").lower()
