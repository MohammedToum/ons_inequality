from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.unit.dag_imports import load_dag_module


load_gcs_to_bq = load_dag_module(
    "load_gcs_to_bq_under_test",
    "airflow/dags/load_gcs_to_bq.py",
)


class FakeStorageClient:
    """Small storage client fake that records list_blobs calls."""

    def __init__(self, blob_names: list[str]) -> None:
        """Store the available fake blob names for prefix-filtered listing."""
        self.blob_names = blob_names
        self.calls: list[dict[str, str]] = []

    def list_blobs(self, bucket_name: str, prefix: str) -> list[SimpleNamespace]:
        """Return fake blob objects whose names match the requested prefix."""
        self.calls.append({"bucket_name": bucket_name, "prefix": prefix})
        return [
            SimpleNamespace(name=blob_name)
            for blob_name in self.blob_names
            if blob_name.startswith(prefix)
        ]


def _write_config(path: Path, content: str) -> None:
    """Write a temporary dataset config file for config-loading tests."""
    path.write_text(content)


def test_version_from_blob_name_parses_version_folder() -> None:
    """Version parsing should extract the numeric suffix from version folders."""
    version = load_gcs_to_bq._version_from_blob_name(
        "raw/ons/uk-spending-on-cards/time-series/version_130/data.csv"
    )

    assert version == 130


def test_version_from_blob_name_raises_when_version_folder_missing() -> None:
    """Version parsing should reject GCS object names without version folders."""
    with pytest.raises(ValueError, match="Could not parse version folder"):
        load_gcs_to_bq._version_from_blob_name(
            "raw/ons/uk-spending-on-cards/time-series/data.csv"
        )


def test_load_dataset_config_returns_enabled_datasets_only(tmp_path: Path) -> None:
    """Dataset config loading should exclude datasets marked as disabled."""
    config_path = tmp_path / "ons_datasets.yml"
    _write_config(
        config_path,
        """datasets:
  - dataset_id: enabled-dataset
    edition: time-series
    version: latest
    raw_table_name: raw_enabled_dataset
  - dataset_id: disabled-dataset
    edition: time-series
    version: 1
    raw_table_name: raw_disabled_dataset
    enabled: false
""",
    )

    datasets = load_gcs_to_bq._load_dataset_config(config_path)

    assert datasets == [
        {
            "dataset_id": "enabled-dataset",
            "edition": "time-series",
            "version": "latest",
            "raw_table_name": "raw_enabled_dataset",
        }
    ]


def test_load_dataset_config_raises_when_file_missing(tmp_path: Path) -> None:
    """Dataset config loading should fail clearly for a missing config file."""
    with pytest.raises(FileNotFoundError, match="ONS dataset config does not exist"):
        load_gcs_to_bq._load_dataset_config(tmp_path / "missing.yml")


def test_load_dataset_config_raises_when_datasets_is_not_a_list(tmp_path: Path) -> None:
    """Dataset config loading should require the top-level datasets list."""
    config_path = tmp_path / "ons_datasets.yml"
    _write_config(config_path, "datasets: {}\n")

    with pytest.raises(ValueError, match="must contain a datasets list"):
        load_gcs_to_bq._load_dataset_config(config_path)


def test_load_dataset_config_raises_when_required_key_missing(tmp_path: Path) -> None:
    """Enabled dataset entries must include all fields needed for load planning."""
    config_path = tmp_path / "ons_datasets.yml"
    _write_config(
        config_path,
        """
datasets:
  - dataset_id: missing-table-name
    edition: time-series
    version: 1
""",
    )

    with pytest.raises(ValueError, match="missing required key raw_table_name"):
        load_gcs_to_bq._load_dataset_config(config_path)


def test_select_csv_blob_selects_specific_version() -> None:
    """Blob selection should search only the configured explicit version folder."""
    storage_client = FakeStorageClient(
        [
            "raw/ons/cards/time-series/version_130/data.csv",
            "raw/ons/cards/time-series/version_131/data.csv",
        ]
    )

    blob_name = load_gcs_to_bq._select_csv_blob(
        storage_client=storage_client,
        bucket_name="raw-bucket",
        dataset={
            "dataset_id": "cards",
            "edition": "time-series",
            "version": 130,
        },
    )

    assert blob_name == "raw/ons/cards/time-series/version_130/data.csv"
    assert storage_client.calls == [
        {
            "bucket_name": "raw-bucket",
            "prefix": "raw/ons/cards/time-series/version_130",
        }
    ]


def test_select_csv_blob_selects_latest_version() -> None:
    """Blob selection should resolve latest to the highest numeric version."""
    storage_client = FakeStorageClient(
        [
            "raw/ons/cards/time-series/version_1/data.csv",
            "raw/ons/cards/time-series/version_12/data.csv",
            "raw/ons/cards/time-series/version_3/data.csv",
        ]
    )

    blob_name = load_gcs_to_bq._select_csv_blob(
        storage_client=storage_client,
        bucket_name="raw-bucket",
        dataset={
            "dataset_id": "cards",
            "edition": "time-series",
            "version": "latest",
        },
    )

    assert blob_name == "raw/ons/cards/time-series/version_12/data.csv"


def test_select_csv_blob_ignores_internal_csv_files() -> None:
    """Blob selection should ignore internal CSV files with underscore names."""
    storage_client = FakeStorageClient(
        [
            "raw/ons/cards/time-series/version_130/_temporary.csv",
            "raw/ons/cards/time-series/version_130/data.csv",
        ]
    )

    blob_name = load_gcs_to_bq._select_csv_blob(
        storage_client=storage_client,
        bucket_name="raw-bucket",
        dataset={
            "dataset_id": "cards",
            "edition": "time-series",
            "version": 130,
        },
    )

    assert blob_name == "raw/ons/cards/time-series/version_130/data.csv"


def test_select_csv_blob_raises_when_no_csv_files_found() -> None:
    """Blob selection should fail when no data CSV exists for the dataset."""
    storage_client = FakeStorageClient(
        ["raw/ons/cards/time-series/version_130/metadata.json"]
    )

    with pytest.raises(FileNotFoundError, match="No CSV files found"):
        load_gcs_to_bq._select_csv_blob(
            storage_client=storage_client,
            bucket_name="raw-bucket",
            dataset={
                "dataset_id": "cards",
                "edition": "time-series",
                "version": 130,
            },
        )


def test_select_csv_blob_raises_when_multiple_csv_files_match() -> None:
    """Blob selection should fail rather than guessing between multiple CSVs."""
    storage_client = FakeStorageClient(
        [
            "raw/ons/cards/time-series/version_130/data.csv",
            "raw/ons/cards/time-series/version_130/extra.csv",
        ]
    )

    with pytest.raises(ValueError, match="Expected exactly one CSV"):
        load_gcs_to_bq._select_csv_blob(
            storage_client=storage_client,
            bucket_name="raw-bucket",
            dataset={
                "dataset_id": "cards",
                "edition": "time-series",
                "version": 130,
            },
        )


def test_build_load_job_config_uses_strict_raw_load_settings() -> None:
    """BigQuery load jobs should use strict raw-table load settings."""
    job_config = load_gcs_to_bq._build_load_job_config()

    expected_settings: dict[str, Any] = {
        "source_format": "CSV",
        "skip_leading_rows": 1,
        "autodetect": False,
        "write_disposition": "WRITE_TRUNCATE",
        "create_disposition": "CREATE_NEVER",
        "max_bad_records": 0,
        "allow_quoted_newlines": True,
        "encoding": "UTF-8",
    }

    for setting, expected_value in expected_settings.items():
        assert getattr(job_config, setting) == expected_value
