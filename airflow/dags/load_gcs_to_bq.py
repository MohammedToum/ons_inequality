"""
Helpers for loading uploaded raw ONS CSV files from GCS into BigQuery.

The helpers select one configured CSV from GCS and load it into its existing
raw BigQuery table with strict load settings.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

from airflow.sdk import Variable
from google.cloud import bigquery, storage

from ons_dataset_config import load_enabled_dataset_config

logger = logging.getLogger(__name__)

DATASETS_CONFIG_PATH = Path(
    os.environ.get("ONS_DATASETS_CONFIG_PATH", "/opt/airflow/configs/ons_datasets.yml")
)
GCS_BUCKET_ENV_VAR = "ONS_RAW_GCS_BUCKET"
GCS_BUCKET_AIRFLOW_VAR = "ons_raw_gcs_bucket"
GCS_PREFIX = os.environ.get("ONS_RAW_GCS_PREFIX", "raw/ons").strip("/")

BQ_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "ons-inequality-dev")
BQ_RAW_DATASET_ENV_VAR = "ONS_RAW_BQ_DATASET"
BQ_RAW_DATASET_AIRFLOW_VAR = "ons_raw_bq_dataset"
BQ_RAW_DATASET_DEFAULT = "raw_ons"
BQ_LOCATION = os.environ.get("ONS_RAW_BQ_LOCATION", "europe-west2")

VERSION_PATTERN = re.compile(r"/version_(?P<version>\d+)/")


def _resolve_bucket_name() -> str:
    bucket_name = os.environ.get(GCS_BUCKET_ENV_VAR, "").strip()

    if bucket_name:
        return bucket_name

    return Variable.get(GCS_BUCKET_AIRFLOW_VAR, default="").strip()


def _resolve_raw_dataset_id() -> str:
    dataset_id = os.environ.get(BQ_RAW_DATASET_ENV_VAR, "").strip()

    if dataset_id:
        return dataset_id

    return Variable.get(BQ_RAW_DATASET_AIRFLOW_VAR, default=BQ_RAW_DATASET_DEFAULT).strip()


def _version_from_blob_name(blob_name: str) -> int:
    match = VERSION_PATTERN.search(f"/{blob_name}")

    if not match:
        raise ValueError(f"Could not parse version folder from GCS object: {blob_name}")

    return int(match.group("version"))


def _load_dataset_config(config_path: Path) -> list[dict[str, Any]]:
    return load_enabled_dataset_config(config_path)


def _select_csv_blob(
    storage_client: storage.Client,
    bucket_name: str,
    dataset: dict[str, Any],
) -> str:
    object_prefix = "/".join(
        part
        for part in [
            GCS_PREFIX,
            dataset["dataset_id"],
            str(dataset["edition"]),
        ]
        if part
    )
    requested_version = str(dataset["version"])

    if requested_version != "latest":
        object_prefix = f"{object_prefix}/version_{requested_version}"

    csv_blobs = sorted(
        blob.name
        for blob in storage_client.list_blobs(bucket_name, prefix=object_prefix)
        if blob.name.endswith(".csv") and "/_" not in blob.name
    )

    if not csv_blobs:
        raise FileNotFoundError(
            f"No CSV files found in gs://{bucket_name}/{object_prefix}"
        )

    if requested_version == "latest":
        latest_version = max(_version_from_blob_name(blob_name) for blob_name in csv_blobs)
        matching_blobs = [
            blob_name
            for blob_name in csv_blobs
            if f"/version_{latest_version}/" in f"/{blob_name}"
        ]
    else:
        matching_blobs = csv_blobs

    if len(matching_blobs) != 1:
        raise ValueError(
            "Expected exactly one CSV for "
            f"{dataset['dataset_id']}/{dataset['edition']}/version={requested_version}; "
            f"found {len(matching_blobs)}: {matching_blobs}"
        )

    return matching_blobs[0]


def _build_load_job_config() -> bigquery.LoadJobConfig:
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        max_bad_records=0,
        allow_quoted_newlines=True,
        encoding="UTF-8",
    )


def _build_load_plan_for_dataset(
    storage_client: storage.Client,
    bucket_name: str,
    raw_dataset_id: str,
    dataset: dict[str, Any],
) -> dict[str, str]:
    """
    Build one strict BigQuery load job specification for a configured dataset.
    """
    blob_name = _select_csv_blob(
        storage_client=storage_client,
        bucket_name=bucket_name,
        dataset=dataset,
    )
    table_id = f"{BQ_PROJECT}.{raw_dataset_id}.{dataset['raw_table_name']}"

    return {
        "source_uri": f"gs://{bucket_name}/{blob_name}",
        "target_table": table_id,
        "dataset_id": dataset["dataset_id"],
        "edition": str(dataset["edition"]),
        "configured_version": str(dataset["version"]),
        "gcs_version": str(_version_from_blob_name(blob_name)),
    }


def _load_raw_table(load_plan: dict[str, str]) -> dict[str, str | int]:
    """
    Load one CSV into its existing raw BigQuery table with strict settings.
    """
    bigquery_client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    job_config = _build_load_job_config()

    load_job = bigquery_client.load_table_from_uri(
        source_uris=load_plan["source_uri"],
        destination=load_plan["target_table"],
        job_config=job_config,
        location=BQ_LOCATION,
    )
    load_job.result()

    table = bigquery_client.get_table(load_plan["target_table"])

    result = {
        "source_uri": load_plan["source_uri"],
        "target_table": load_plan["target_table"],
        "job_id": load_job.job_id,
        "output_rows": int(load_job.output_rows or 0),
        "table_rows": int(table.num_rows or 0),
    }

    logger.info(
        "Loaded %s into %s | job_id=%s output_rows=%s table_rows=%s",
        result["source_uri"],
        result["target_table"],
        result["job_id"],
        result["output_rows"],
        result["table_rows"],
    )

    if result["output_rows"] <= 0:
        raise ValueError(
            f"BigQuery load produced zero rows for {load_plan['target_table']}"
        )

    return result
