"""
Dataset-scoped Airflow DAGs for the raw ONS ingestion pipeline.

Each enabled dataset in the shared config gets one DAG. Within that DAG, local
raw assets are uploaded to GCS first, then the uploaded CSV is loaded into the
dataset's existing BigQuery raw table.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.sdk import dag, task
from google.cloud import storage

from load_gcs_to_bq import (
    DATASETS_CONFIG_PATH,
    _build_load_plan_for_dataset,
    _load_raw_table,
    _resolve_bucket_name,
    _resolve_raw_dataset_id,
)
from load_local_to_gcs import (
    LOCAL_RAW_ROOT,
    _discover_dataset_assets,
    _upload_assets_to_gcs,
)
from ons_dataset_config import dataset_dag_suffix, load_enabled_dataset_config

logger = logging.getLogger(__name__)

PIPELINE_START_DATE = datetime(2026, 1, 1)
PIPELINE_SCHEDULE = os.environ.get("ONS_RAW_PIPELINE_SCHEDULE") or None


def _build_load_plan_after_upload(
    dataset: dict[str, Any],
    uploaded_blob_names: list[str],
) -> dict[str, str]:
    """
    Build the BigQuery load plan after the dataset's GCS upload has completed.
    """
    bucket_name = _resolve_bucket_name()

    if not bucket_name:
        raise ValueError(
            "Set the ONS_RAW_GCS_BUCKET environment variable or the "
            "ons_raw_gcs_bucket Airflow Variable before running this DAG."
        )

    logger.info(
        "Building BigQuery load plan for %s/%s after uploading %s GCS object(s)",
        dataset["dataset_id"],
        dataset["edition"],
        len(uploaded_blob_names),
    )

    return _build_load_plan_for_dataset(
        storage_client=storage.Client(),
        bucket_name=bucket_name,
        raw_dataset_id=_resolve_raw_dataset_id(),
        dataset=dataset,
    )


def _create_ons_raw_pipeline_dag(dataset: dict[str, Any]) -> Any:
    """
    Create one end-to-end raw ONS pipeline DAG for a configured dataset.
    """
    dag_suffix = dataset_dag_suffix(dataset)

    @dag(
        dag_id=f"ons_raw_pipeline__{dag_suffix}",
        description=(
            "Upload local raw ONS files to GCS and load BigQuery raw table for "
            f"{dataset['dataset_id']} ({dataset['edition']})."
        ),
        schedule=PIPELINE_SCHEDULE,
        start_date=PIPELINE_START_DATE,
        catchup=False,
        tags=["ons", "raw", "gcs", "bigquery", str(dataset["dataset_id"])],
    )
    def ons_raw_pipeline_dataset() -> None:
        """
        Define the dataset-scoped raw ingestion pipeline.
        """

        @task
        def discover_assets() -> list[dict[str, str]]:
            """
            Return upload metadata for this configured dataset only.
            """
            assets = _discover_dataset_assets(LOCAL_RAW_ROOT, dataset)
            logger.info(
                "Discovered %s raw ONS asset(s) for %s/%s",
                len(assets),
                dataset["dataset_id"],
                dataset["edition"],
            )
            return assets

        @task
        def upload_assets(assets: list[dict[str, str]]) -> list[str]:
            """
            Upload this dataset's discovered raw assets to GCS.
            """
            return _upload_assets_to_gcs(assets)

        @task
        def build_load_plan(uploaded_blob_names: list[str]) -> dict[str, str]:
            """
            Build the BigQuery raw load plan after upload completion.
            """
            return _build_load_plan_after_upload(dataset, uploaded_blob_names)

        @task
        def load_raw_table(load_plan: dict[str, str]) -> dict[str, str | int]:
            """
            Load this dataset's CSV into its existing raw BigQuery table.
            """
            return _load_raw_table(load_plan)

        load_raw_table(build_load_plan(upload_assets(discover_assets())))

    return ons_raw_pipeline_dataset()


for configured_dataset in load_enabled_dataset_config(DATASETS_CONFIG_PATH):
    globals()[f"ons_raw_pipeline__{dataset_dag_suffix(configured_dataset)}"] = (
        _create_ons_raw_pipeline_dag(configured_dataset)
    )
