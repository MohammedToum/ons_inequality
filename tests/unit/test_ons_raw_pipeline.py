from __future__ import annotations

from pathlib import Path

from tests.unit.dag_imports import load_dag_module


def test_pipeline_module_creates_one_dag_per_enabled_dataset(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The raw pipeline should expose one combined DAG per enabled dataset."""
    config_path = tmp_path / "ons_datasets.yml"
    config_path.write_text(
        """datasets:
  - dataset_id: selected-dataset
    edition: time-series
    version: latest
    raw_table_name: raw_selected_dataset
  - dataset_id: disabled-dataset
    edition: time-series
    version: latest
    raw_table_name: raw_disabled_dataset
    enabled: false
"""
    )
    monkeypatch.setenv("ONS_DATASETS_CONFIG_PATH", str(config_path))

    pipeline_module = load_dag_module(
        "ons_raw_pipeline_under_test",
        "airflow/dags/ons_raw_pipeline.py",
    )

    assert "ons_raw_pipeline__selected_dataset_time_series" in vars(pipeline_module)
    assert "ons_raw_pipeline__disabled_dataset_time_series" not in vars(pipeline_module)
