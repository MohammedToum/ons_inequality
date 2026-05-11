from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any


class _Variable:
    """Minimal Airflow Variable stub used while importing DAG modules."""

    values: dict[str, str] = {}

    @classmethod
    def get(cls, key: str, default: str = "") -> str:
        """Return a configured variable value or the provided default."""
        return cls.values.get(key, default)


class _LoadJobConfig:
    """Minimal BigQuery LoadJobConfig stub that records constructor settings."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def _dag(*args: Any, **kwargs: Any) -> Any:
    """Return a decorator that prevents DAG construction during tests."""

    def decorator(func: Any) -> Any:
        def wrapper(*call_args: Any, **call_kwargs: Any) -> None:
            return None

        return wrapper

    return decorator


def _task(func: Any) -> Any:
    """Return the task function unchanged for unit-level helper imports."""
    return func


def install_dag_import_stubs() -> None:
    """Install Airflow and Google module stubs needed to import DAG files."""
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    airflow_sdk_module.Variable = _Variable
    airflow_sdk_module.dag = _dag
    airflow_sdk_module.task = _task

    google_module = types.ModuleType("google")
    google_cloud_module = types.ModuleType("google.cloud")
    storage_module = types.ModuleType("google.cloud.storage")
    bigquery_module = types.ModuleType("google.cloud.bigquery")

    storage_module.Client = object
    bigquery_module.Client = object
    bigquery_module.LoadJobConfig = _LoadJobConfig
    bigquery_module.SourceFormat = types.SimpleNamespace(CSV="CSV")
    bigquery_module.WriteDisposition = types.SimpleNamespace(WRITE_TRUNCATE="WRITE_TRUNCATE")
    bigquery_module.CreateDisposition = types.SimpleNamespace(CREATE_NEVER="CREATE_NEVER")

    google_cloud_module.storage = storage_module
    google_cloud_module.bigquery = bigquery_module
    google_module.cloud = google_cloud_module

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["google"] = google_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.storage"] = storage_module
    sys.modules["google.cloud.bigquery"] = bigquery_module


def load_dag_module(module_name: str, relative_path: str) -> Any:
    """Load a DAG module from a repo-relative path with test stubs installed."""
    install_dag_import_stubs()

    repo_root = Path(__file__).resolve().parents[2]
    dags_path = str(repo_root / "airflow" / "dags")

    if dags_path not in sys.path:
        sys.path.insert(0, dags_path)

    module_path = repo_root / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
