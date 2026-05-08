"""
Airflow DAG for loading uploaded raw ONS CSV files from GCS into BigQuery.

The DAG is intentionally separate from the local-to-GCS upload DAG. It loads
each configured CSV from GCS into its existing raw table.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from airflow.sdk import Variable, dag, task
from google.cloud import bigquery, storage

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
        for key in ["dataset_id", "edition", "version", "raw_table_name"]:
            if key not in dataset:
                raise ValueError(f"Dataset config missing required key {key}: {dataset}")

    return enabled_datasets


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


@dag(
    dag_id="gcs_ons_raw_to_bq",
    description="Load uploaded raw ONS CSV files from GCS into existing BigQuery raw tables.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ons", "raw", "gcs", "bigquery"],
)
def gcs_ons_raw_to_bq() -> None:
    """
    Define the GCS to BigQuery raw load workflow.

    This DAG loads the CSV files already uploaded by `local_ons_raw_to_gcs`.
    BigQuery tables and schemas are expected to exist before the load starts.
    """

    @task
    def build_load_plan() -> list[dict[str, str]]:
        """
        Build one strict BigQuery load job specification per enabled dataset.
        """
        bucket_name = _resolve_bucket_name()

        if not bucket_name:
            raise ValueError(
                f"Set the {GCS_BUCKET_ENV_VAR} environment variable or the "
                f"{GCS_BUCKET_AIRFLOW_VAR} Airflow Variable before running this DAG."
            )

        raw_dataset_id = _resolve_raw_dataset_id()
        storage_client = storage.Client()
        datasets = _load_dataset_config(DATASETS_CONFIG_PATH)
        load_plan: list[dict[str, str]] = []

        for dataset in datasets:
            blob_name = _select_csv_blob(
                storage_client=storage_client,
                bucket_name=bucket_name,
                dataset=dataset,
            )
            table_id = f"{BQ_PROJECT}.{raw_dataset_id}.{dataset['raw_table_name']}"

            load_plan.append(
                {
                    "source_uri": f"gs://{bucket_name}/{blob_name}",
                    "target_table": table_id,
                    "dataset_id": dataset["dataset_id"],
                    "edition": str(dataset["edition"]),
                    "configured_version": str(dataset["version"]),
                    "gcs_version": str(_version_from_blob_name(blob_name)),
                }
            )

        logger.info("Built %s BigQuery raw load job(s)", len(load_plan))
        return load_plan

    @task
    def load_raw_tables(load_plan: list[dict[str, str]]) -> list[dict[str, str | int]]:
        """
        Load each CSV into its existing raw BigQuery table with strict settings.
        """
        bigquery_client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
        load_results: list[dict[str, str | int]] = []

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            max_bad_records=0,
            allow_quoted_newlines=True,
            encoding="UTF-8",
        )

        for load in load_plan:
            load_job = bigquery_client.load_table_from_uri(
                source_uris=load["source_uri"],
                destination=load["target_table"],
                job_config=job_config,
                location=BQ_LOCATION,
            )
            load_job.result()

            table = bigquery_client.get_table(load["target_table"])

            result = {
                "source_uri": load["source_uri"],
                "target_table": load["target_table"],
                "job_id": load_job.job_id,
                "output_rows": int(load_job.output_rows or 0),
                "table_rows": int(table.num_rows or 0),
            }
            load_results.append(result)

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
                    f"BigQuery load produced zero rows for {load['target_table']}"
                )

        return load_results

    load_raw_tables(build_load_plan())


gcs_ons_raw_to_bq()
