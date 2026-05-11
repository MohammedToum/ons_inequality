"""
Helpers for uploading local raw ONS dataset assets to Google Cloud Storage.

The helpers mirror the local raw storage layout under the configured GCS prefix
and upload CSV data files plus JSON metadata files for dataset version folders.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from airflow.sdk import Variable
from google.cloud import storage

logger = logging.getLogger(__name__)

LOCAL_RAW_ROOT = Path(os.environ.get("ONS_RAW_LOCAL_ROOT", "/opt/airflow/data/raw/ons"))
GCS_BUCKET_ENV_VAR = "ONS_RAW_GCS_BUCKET"
GCS_BUCKET_AIRFLOW_VAR = "ons_raw_gcs_bucket"
GCS_PREFIX = os.environ.get("ONS_RAW_GCS_PREFIX", "raw/ons").strip("/")

CONTENT_TYPES = {
    ".csv": "text/csv",
    ".json": "application/json",
}


def _build_blob_name(local_path: str, raw_root: str, prefix: str) -> str:
    """
    Build the destination GCS object name for a local raw asset.

    The object path preserves the file path relative to the raw ONS root and
    optionally prepends a GCS folder-style prefix.
    """
    relative_path = Path(local_path).relative_to(raw_root).as_posix()

    if not prefix:
        return relative_path

    return f"{prefix}/{relative_path}"


def _resolve_bucket_name() -> str:
    """
    Resolve the target GCS bucket name from runtime configuration.

    Docker Compose exposes the preferred `ONS_RAW_GCS_BUCKET` environment
    variable. The lowercase `ons_raw_gcs_bucket` Airflow Variable is supported
    as a fallback for values configured through the Airflow UI.
    """
    bucket_name = os.environ.get(GCS_BUCKET_ENV_VAR, "").strip()

    if bucket_name:
        return bucket_name

    return Variable.get(GCS_BUCKET_AIRFLOW_VAR, default="").strip()


def _dataset_version_dirs(raw_root: Path, dataset: dict[str, Any]) -> list[Path]:
    """
    Return local version folders for a single configured dataset edition.
    """
    dataset_root = raw_root / str(dataset["dataset_id"]) / str(dataset["edition"])

    if str(dataset["version"]) == "latest":
        return sorted(path for path in dataset_root.glob("version_*") if path.is_dir())

    version_dir = dataset_root / f"version_{dataset['version']}"
    return [version_dir] if version_dir.is_dir() else []


def _discover_dataset_assets(
    raw_root: Path,
    dataset: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    """
    Discover CSV and JSON assets in local ONS dataset version folders.

    Each dataset version folder is expected to contain at least one CSV file and
    a `metadata.json` file. `csvw_metadata.json` is included when present.
    When a dataset config is provided, discovery is limited to that dataset and
    edition instead of scanning the full raw root.
    """
    if not raw_root.exists():
        raise FileNotFoundError(f"Raw ONS folder does not exist: {raw_root}")

    if dataset is None:
        version_dirs = sorted(
            path for path in raw_root.glob("*/*/version_*") if path.is_dir()
        )
        missing_message = f"No ONS dataset version folders found under {raw_root}"
    else:
        version_dirs = _dataset_version_dirs(raw_root, dataset)
        missing_message = (
            "No ONS dataset version folders found for "
            f"{dataset['dataset_id']}/{dataset['edition']}/version={dataset['version']} "
            f"under {raw_root}"
        )

    if not version_dirs:
        raise FileNotFoundError(missing_message)

    assets: list[dict[str, str]] = []

    for version_dir in version_dirs:
        csv_files = sorted(
            path
            for path in version_dir.glob("*.csv")
            if path.is_file() and not path.name.startswith("_")
        )
        metadata_path = version_dir / "metadata.json"
        csvw_metadata_path = version_dir / "csvw_metadata.json"

        if not csv_files:
            raise FileNotFoundError(f"No CSV file found in {version_dir}")

        if not metadata_path.exists():
            raise FileNotFoundError(f"metadata.json missing from {version_dir}")

        for path in [*csv_files, metadata_path, csvw_metadata_path]:
            if path.exists():
                assets.append(
                    {
                        "local_path": str(path),
                        "blob_name": _build_blob_name(
                            local_path=str(path),
                            raw_root=str(raw_root),
                            prefix=GCS_PREFIX,
                        ),
                        "content_type": CONTENT_TYPES[path.suffix],
                    }
                )

    return assets


def _upload_assets_to_gcs(assets: list[dict[str, str]]) -> list[str]:
    """
    Upload discovered raw assets to Google Cloud Storage.
    """
    bucket_name = _resolve_bucket_name()

    if not bucket_name:
        raise ValueError(
            f"Set the {GCS_BUCKET_ENV_VAR} environment variable or the "
            f"{GCS_BUCKET_AIRFLOW_VAR} Airflow Variable to the target GCS "
            "bucket name before running this DAG."
        )

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    uploaded_blob_names: list[str] = []

    for asset in assets:
        blob = bucket.blob(asset["blob_name"])
        blob.upload_from_filename(
            asset["local_path"],
            content_type=asset["content_type"],
        )
        uploaded_blob_names.append(asset["blob_name"])

        logger.info(
            "Uploaded %s to gs://%s/%s",
            asset["local_path"],
            bucket_name,
            asset["blob_name"],
        )

    return uploaded_blob_names
