from __future__ import annotations

from pathlib import Path

import pytest

from tests.unit.dag_imports import load_dag_module


load_local_to_gcs = load_dag_module(
    "load_local_to_gcs_under_test",
    "airflow/dags/load_local_to_gcs.py",
)


def _write_dataset_version(
    raw_root: Path,
    dataset_id: str = "uk-spending-on-cards",
    edition: str = "time-series",
    version: str = "130",
    *,
    include_csv: bool = True,
    include_metadata: bool = True,
    include_csvw_metadata: bool = False,
) -> Path:
    """Create a representative raw ONS dataset version folder for tests."""
    version_dir = raw_root / dataset_id / edition / f"version_{version}"
    version_dir.mkdir(parents=True)

    if include_csv:
        (version_dir / "data.csv").write_text("a,b\n1,2\n")

    if include_metadata:
        (version_dir / "metadata.json").write_text('{"id": "dataset"}\n')

    if include_csvw_metadata:
        (version_dir / "csvw_metadata.json").write_text('{"tables": []}\n')

    return version_dir


def test_build_blob_name_preserves_relative_path_with_prefix(tmp_path: Path) -> None:
    """Blob names should keep the raw-root-relative path under the GCS prefix."""
    raw_root = tmp_path / "raw" / "ons"
    local_path = raw_root / "dataset" / "edition" / "version_1" / "data.csv"
    local_path.parent.mkdir(parents=True)
    local_path.touch()

    blob_name = load_local_to_gcs._build_blob_name(
        local_path=str(local_path),
        raw_root=str(raw_root),
        prefix="raw/ons",
    )

    assert blob_name == "raw/ons/dataset/edition/version_1/data.csv"


def test_build_blob_name_allows_empty_prefix(tmp_path: Path) -> None:
    """Blob names should not include a leading separator when no prefix is set."""
    raw_root = tmp_path / "raw" / "ons"
    local_path = raw_root / "dataset" / "edition" / "version_1" / "data.csv"
    local_path.parent.mkdir(parents=True)
    local_path.touch()

    blob_name = load_local_to_gcs._build_blob_name(
        local_path=str(local_path),
        raw_root=str(raw_root),
        prefix="",
    )

    assert blob_name == "dataset/edition/version_1/data.csv"


def test_discover_dataset_assets_returns_csv_and_metadata_assets(tmp_path: Path) -> None:
    """Asset discovery should return upload metadata for CSV and JSON files."""
    raw_root = tmp_path / "raw" / "ons"
    version_dir = _write_dataset_version(raw_root, include_csvw_metadata=True)
    (version_dir / "_temporary.csv").write_text("ignore,this\n")

    assets = load_local_to_gcs._discover_dataset_assets(raw_root)

    assert assets == [
        {
            "local_path": str(version_dir / "data.csv"),
            "blob_name": "raw/ons/uk-spending-on-cards/time-series/version_130/data.csv",
            "content_type": "text/csv",
        },
        {
            "local_path": str(version_dir / "metadata.json"),
            "blob_name": "raw/ons/uk-spending-on-cards/time-series/version_130/metadata.json",
            "content_type": "application/json",
        },
        {
            "local_path": str(version_dir / "csvw_metadata.json"),
            "blob_name": "raw/ons/uk-spending-on-cards/time-series/version_130/csvw_metadata.json",
            "content_type": "application/json",
        },
    ]


def test_discover_dataset_assets_can_target_one_configured_dataset(
    tmp_path: Path,
) -> None:
    """Asset discovery should only return assets for the requested dataset."""
    raw_root = tmp_path / "raw" / "ons"
    selected_dir = _write_dataset_version(
        raw_root,
        dataset_id="selected-dataset",
        edition="time-series",
        version="2",
    )
    _write_dataset_version(
        raw_root,
        dataset_id="other-dataset",
        edition="time-series",
        version="1",
    )

    assets = load_local_to_gcs._discover_dataset_assets(
        raw_root,
        {
            "dataset_id": "selected-dataset",
            "edition": "time-series",
            "version": 2,
        },
    )

    assert assets == [
        {
            "local_path": str(selected_dir / "data.csv"),
            "blob_name": "raw/ons/selected-dataset/time-series/version_2/data.csv",
            "content_type": "text/csv",
        },
        {
            "local_path": str(selected_dir / "metadata.json"),
            "blob_name": "raw/ons/selected-dataset/time-series/version_2/metadata.json",
            "content_type": "application/json",
        },
    ]


def test_discover_dataset_assets_raises_when_raw_root_missing(tmp_path: Path) -> None:
    """Discovery should fail clearly when the configured raw root is missing."""
    with pytest.raises(FileNotFoundError, match="Raw ONS folder does not exist"):
        load_local_to_gcs._discover_dataset_assets(tmp_path / "missing")


def test_discover_dataset_assets_raises_when_no_version_folders(tmp_path: Path) -> None:
    """Discovery should fail when the raw root has no dataset version folders."""
    raw_root = tmp_path / "raw" / "ons"
    raw_root.mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="No ONS dataset version folders"):
        load_local_to_gcs._discover_dataset_assets(raw_root)


def test_discover_dataset_assets_raises_when_version_has_no_csv(tmp_path: Path) -> None:
    """Each dataset version must include at least one uploadable CSV file."""
    raw_root = tmp_path / "raw" / "ons"
    _write_dataset_version(raw_root, include_csv=False)

    with pytest.raises(FileNotFoundError, match="No CSV file found"):
        load_local_to_gcs._discover_dataset_assets(raw_root)


def test_discover_dataset_assets_raises_when_metadata_missing(tmp_path: Path) -> None:
    """Each dataset version must include metadata.json beside its CSV data."""
    raw_root = tmp_path / "raw" / "ons"
    _write_dataset_version(raw_root, include_metadata=False)

    with pytest.raises(FileNotFoundError, match="metadata.json missing"):
        load_local_to_gcs._discover_dataset_assets(raw_root)
