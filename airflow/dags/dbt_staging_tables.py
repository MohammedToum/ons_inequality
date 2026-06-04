"""
Model-scoped Airflow DAGs for dbt ONS staging tables.

Each enabled staging table in configs/dbt_staging_tables.yml gets one DAG per
configured batch in configs/time_vars.yml. The DAG builds the configured dbt
staging model and then runs its tests. Optional start/end year and quarter
values are passed through to the dbt model macros when configured.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from airflow.sdk import dag, get_current_context, task

logger = logging.getLogger(__name__)

PIPELINE_START_DATE = datetime(2026, 1, 1)
PIPELINE_SCHEDULE = os.environ.get("DBT_STAGING_TABLES_SCHEDULE") or None

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "ons_inequality"
LOCAL_DBT_PROFILES_DIR = REPO_ROOT / "dbt" / "profiles"
DEFAULT_DBT_PROJECT_DIR = Path("/opt/airflow/dbt/ons_inequality")
DEFAULT_DBT_PROFILES_DIR = Path("/opt/airflow/dbt/profiles")
DEFAULT_STAGING_TABLES_CONFIG_PATH = Path("/opt/airflow/configs/dbt_staging_tables.yml")
LOCAL_STAGING_TABLES_CONFIG_PATH = REPO_ROOT / "configs" / "dbt_staging_tables.yml"
DEFAULT_TIME_VARS_CONFIG_PATH = Path("/opt/airflow/configs/time_vars.yml")
LOCAL_TIME_VARS_CONFIG_PATH = REPO_ROOT / "configs" / "time_vars.yml"


def _resolve_path(
    env_var_name: str,
    default_path: Path,
    local_path: Path,
    fallback_env_var_name: str | None = None,
) -> Path:
    """
    Resolve a path from env, the Airflow container mount, or the local repo.
    """
    configured_path = os.environ.get(env_var_name)

    if configured_path and Path(configured_path).exists():
        return Path(configured_path)

    fallback_configured_path = (
        os.environ.get(fallback_env_var_name)
        if fallback_env_var_name
        else None
    )

    if fallback_configured_path and Path(fallback_configured_path).exists():
        return Path(fallback_configured_path)

    if default_path.exists():
        return default_path

    return local_path


DBT_PROJECT_DIR = _resolve_path(
    "DBT_PROJECT_DIR",
    DEFAULT_DBT_PROJECT_DIR,
    LOCAL_DBT_PROJECT_DIR,
)
DBT_PROFILES_DIR = _resolve_path(
    "DBT_STAGING_TABLES_PROFILES_DIR",
    DEFAULT_DBT_PROFILES_DIR,
    LOCAL_DBT_PROFILES_DIR,
    fallback_env_var_name="DBT_PROFILES_DIR",
)
STAGING_TABLES_CONFIG_PATH = Path(
    os.environ.get(
        "DBT_STAGING_TABLES_CONFIG_PATH",
        str(
            DEFAULT_STAGING_TABLES_CONFIG_PATH
            if DEFAULT_STAGING_TABLES_CONFIG_PATH.exists()
            else LOCAL_STAGING_TABLES_CONFIG_PATH
        ),
    )
)
TIME_VARS_CONFIG_PATH = _resolve_path(
    "TIME_VARS_CONFIG_PATH",
    DEFAULT_TIME_VARS_CONFIG_PATH,
    LOCAL_TIME_VARS_CONFIG_PATH,
    fallback_env_var_name="YEAR_VARS_CONFIG_PATH",
)

REQUIRED_STAGING_TABLE_KEYS = ["table_id", "model", "time_window"]
VALID_TIME_WINDOWS = {"annual", "quarterly"}
VALID_BATCH_VAR_KEYS = {
    "annual": {"start_year", "end_year"},
    "quarterly": {"start_year", "start_quarter", "end_year", "end_quarter"},
}
STAGING_WINDOW_PARAMS = {
    "start_year": None,
    "end_year": None,
    "start_quarter": None,
    "end_quarter": None,
}


def load_enabled_staging_tables(config_path: Path) -> list[dict[str, Any]]:
    """
    Load enabled dbt staging tables from the shared YAML config file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Staging tables config does not exist: {config_path}")

    with config_path.open() as file:
        config = yaml.safe_load(file) or {}

    staging_tables = config.get("staging_tables")

    if not isinstance(staging_tables, list):
        raise ValueError(f"Config file must contain a staging_tables list: {config_path}")

    enabled_staging_tables = [
        staging_table
        for staging_table in staging_tables
        if staging_table.get("enabled", True)
    ]

    for staging_table in enabled_staging_tables:
        for key in REQUIRED_STAGING_TABLE_KEYS:
            if key not in staging_table:
                raise ValueError(
                    f"Staging table config missing required key {key}: {staging_table}"
                )

        time_window = staging_table["time_window"]
        if time_window not in VALID_TIME_WINDOWS:
            raise ValueError(
                f"Staging table time_window must be one of {sorted(VALID_TIME_WINDOWS)}: "
                f"{staging_table}"
            )

        batch_vars = staging_table.get("batch_vars", {})
        if batch_vars is not None and not isinstance(batch_vars, dict):
            raise ValueError(f"batch_vars must be a mapping when provided: {staging_table}")

        invalid_batch_vars = set(batch_vars or {}) - VALID_BATCH_VAR_KEYS[time_window]
        if invalid_batch_vars:
            raise ValueError(
                f"Invalid {time_window} batch var(s) {sorted(invalid_batch_vars)} "
                f"for staging table {staging_table['table_id']}"
            )

    return enabled_staging_tables


def staging_table_dag_suffix(staging_table: dict[str, Any]) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured staging table.
    """
    return re.sub(r"[^a-zA-Z0-9_]+", "_", staging_table["table_id"]).strip("_").lower()


def batch_dag_suffix(batch: dict[str, Any]) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured time batch.
    """
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(batch["batch_id"])).strip("_").lower()


def year_dag_suffix(year: int) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured year.
    """
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(year)).strip("_").lower()


def load_staging_table_batches(
    config_path: Path,
    staging_tables: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """
    Load and validate time batch vars for each enabled staging table.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Time vars config does not exist: {config_path}")

    with config_path.open() as file:
        config = yaml.safe_load(file) or {}

    staging_table_batches = (config.get("pipelines") or {}).get("staging_tables")
    if not isinstance(staging_table_batches, dict):
        raise ValueError(
            f"Config file must contain pipelines.staging_tables mapping: {config_path}"
        )

    batches_by_table_id: dict[str, list[dict[str, Any]]] = {}

    for staging_table in staging_tables:
        table_id = staging_table["table_id"]
        pipeline_batches = staging_table_batches.get(table_id)
        if not isinstance(pipeline_batches, dict):
            raise ValueError(f"Missing time vars pipeline config for staging table: {table_id}")

        configured_time_window = pipeline_batches.get("time_window")
        if configured_time_window != staging_table["time_window"]:
            raise ValueError(
                "Time vars time_window mismatch for staging table "
                f"{table_id}: expected {staging_table['time_window']}, "
                f"got {configured_time_window}"
            )

        batches = pipeline_batches.get("batches")
        if not isinstance(batches, list) or not batches:
            raise ValueError(f"Time vars batches must be a non-empty list for: {table_id}")

        valid_batch_var_keys = VALID_BATCH_VAR_KEYS[staging_table["time_window"]]
        validated_batches = []

        for batch in batches:
            if not isinstance(batch, dict):
                raise ValueError(f"Time vars batch must be a mapping for {table_id}: {batch}")

            if "batch_id" not in batch:
                raise ValueError(f"Time vars batch missing batch_id for {table_id}: {batch}")

            batch_vars = batch.get("vars")
            if not isinstance(batch_vars, dict):
                raise ValueError(
                    f"Time vars batch vars must be a mapping for {table_id}: {batch}"
                )

            invalid_batch_vars = set(batch_vars) - valid_batch_var_keys
            if invalid_batch_vars:
                raise ValueError(
                    f"Invalid {staging_table['time_window']} time var(s) "
                    f"{sorted(invalid_batch_vars)} for {table_id} batch {batch['batch_id']}"
                )

            validated_batches.append(
                {
                    "batch_id": str(batch["batch_id"]),
                    "vars": _non_null_time_vars(batch_vars, valid_batch_var_keys),
                }
            )

        batches_by_table_id[table_id] = validated_batches

    return batches_by_table_id


def _single_year_from_batch(batch: dict[str, Any]) -> int | None:
    """
    Return the single year represented by a batch, or None for multi-year batches.
    """
    batch_vars = batch["vars"]
    start_year = batch_vars.get("start_year")
    end_year = batch_vars.get("end_year")

    if start_year is None or start_year != end_year:
        return None

    return int(start_year)


def group_staging_table_batches_by_year(
    staging_tables: list[dict[str, Any]],
    batches_by_table_id: dict[str, list[dict[str, Any]]],
) -> dict[int, dict[str, dict[str, Any]]]:
    """
    Group configured staging batches into complete year-level runs.
    """
    batches_by_table_and_year: dict[str, dict[int, dict[str, Any]]] = {}
    common_years: set[int] | None = None

    for staging_table in staging_tables:
        table_id = staging_table["table_id"]
        batches_by_year: dict[int, dict[str, Any]] = {}

        for batch in batches_by_table_id[table_id]:
            year = _single_year_from_batch(batch)
            if year is not None:
                batches_by_year[year] = batch

        batches_by_table_and_year[table_id] = batches_by_year
        table_years = set(batches_by_year)
        common_years = table_years if common_years is None else common_years & table_years

    return {
        year: {
            staging_table["table_id"]: batches_by_table_and_year[staging_table["table_id"]][year]
            for staging_table in staging_tables
        }
        for year in sorted(common_years or set())
    }


def _dbt_command(command_name: str) -> list[str]:
    """
    Return dbt command arguments shared by run and test invocations.
    """
    return [
        "dbt",
        command_name,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
        "--no-use-colors",
    ]


def _non_null_time_vars(
    values: dict[str, Any],
    valid_keys: set[str],
) -> dict[str, Any]:
    """
    Return supplied time-window vars, treating null and blank strings as unset.
    """
    return {
        key: value
        for key, value in values.items()
        if key in valid_keys and value is not None and value != ""
    }


def _dbt_vars(
    staging_table: dict[str, Any],
    time_batch_vars: dict[str, Any] | None = None,
    run_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Merge static vars, configured batch vars, time batch vars, and run overrides.
    """
    configured_vars = dict(staging_table.get("vars") or {})
    valid_batch_var_keys = VALID_BATCH_VAR_KEYS[staging_table["time_window"]]
    batch_vars = _non_null_time_vars(
        staging_table.get("batch_vars") or {},
        valid_batch_var_keys,
    )
    time_vars = _non_null_time_vars(time_batch_vars or {}, valid_batch_var_keys)
    param_vars = _non_null_time_vars(run_params or {}, valid_batch_var_keys)

    return {**configured_vars, **batch_vars, **time_vars, **param_vars}


def _append_dbt_vars(command: list[str], dbt_vars: dict[str, Any]) -> list[str]:
    """
    Add a dbt --vars YAML payload when this staging table has configured vars.
    """
    if not dbt_vars:
        return command

    return [
        *command,
        "--vars",
        yaml.safe_dump(dbt_vars, default_flow_style=True, sort_keys=True).strip(),
    ]


def _run_dbt_command(command: list[str], phase_name: str) -> None:
    """
    Run a dbt command and stream output into the Airflow task log.
    """
    started_at = time.monotonic()
    logger.info("Starting dbt %s phase.", phase_name)
    logger.info("Running command: %s", " ".join(command))
    subprocess.run(command, check=True)
    logger.info(
        "Finished dbt %s phase in %.2f seconds.",
        phase_name,
        time.monotonic() - started_at,
    )


def _log_staging_table_context(
    staging_table: dict[str, Any],
    batch: dict[str, Any],
    dbt_vars: dict[str, Any],
) -> None:
    """
    Log the staging table config that controls this generated DAG.
    """
    logger.info("Staging table id: %s", staging_table["table_id"])
    logger.info("Staging table description: %s", staging_table.get("description", ""))
    logger.info("Configured dbt model: %s", staging_table["model"])
    logger.info("Configured raw source table: %s", staging_table.get("source_raw_table", ""))
    logger.info("Configured time window type: %s", staging_table["time_window"])
    logger.info("Configured time batch id: %s", batch["batch_id"])
    logger.info("Configured dbt vars: %s", dbt_vars)


def _create_dbt_staging_table_dag(
    staging_table: dict[str, Any],
    batch: dict[str, Any],
) -> Any:
    """
    Create one dbt staging DAG for a configured staging table and time batch.
    """
    dag_suffix = staging_table_dag_suffix(staging_table)
    batch_suffix = batch_dag_suffix(batch)

    @dag(
        dag_id=f"dbt_staging_tables__{dag_suffix}__{batch_suffix}",
        description=(
            f"Run dbt staging table {staging_table['model']} "
            f"for batch {batch['batch_id']}."
        ),
        schedule=PIPELINE_SCHEDULE,
        start_date=PIPELINE_START_DATE,
        catchup=False,
        params=STAGING_WINDOW_PARAMS,
        tags=["dbt", "staging", staging_table["time_window"], dag_suffix, batch_suffix],
    )
    def dbt_staging_table_group() -> None:
        """
        Define the model-scoped dbt staging pipeline.
        """

        @task
        def run_staging_table() -> dict[str, Any]:
            """
            Build this staging table with its configured time-window vars.
            """
            context = get_current_context()
            dbt_vars = _dbt_vars(staging_table, batch["vars"], context["params"])
            _log_staging_table_context(staging_table, batch, dbt_vars)
            logger.info(
                "dbt adapter output below includes row counts and bytes processed "
                "where the adapter reports them."
            )

            command = _append_dbt_vars(
                [
                    *_dbt_command("run"),
                    "--select",
                    staging_table["model"],
                ],
                dbt_vars,
            )
            _run_dbt_command(command, "run")
            return {
                "model": staging_table["model"],
                "time_window": staging_table["time_window"],
                "batch_id": batch["batch_id"],
                "dbt_vars": dbt_vars,
            }

        @task
        def test_staging_table() -> dict[str, Any]:
            """
            Validate this staging table.
            """
            context = get_current_context()
            model = staging_table["model"]
            dbt_vars = _dbt_vars(staging_table, batch["vars"], context["params"])
            _log_staging_table_context(staging_table, batch, dbt_vars)
            logger.info("Starting validation for dbt model: %s", model)

            command = _append_dbt_vars(
                [
                    *_dbt_command("test"),
                    "--select",
                    model,
                    "--indirect-selection",
                    "buildable",
                ],
                dbt_vars,
            )
            _run_dbt_command(command, "test")
            logger.info(
                "dbt test summary is emitted above by dbt as PASS/WARN/ERROR/SKIP/TOTAL."
            )
            return {
                "model": model,
                "time_window": staging_table["time_window"],
                "batch_id": batch["batch_id"],
                "tested": True,
            }

        run_staging_table_task = run_staging_table()
        test_staging_table_task = test_staging_table()

        run_staging_table_task >> test_staging_table_task

    return dbt_staging_table_group()


def _create_dbt_staging_year_dag(
    year: int,
    batches_by_table_id: dict[str, dict[str, Any]],
    staging_tables: list[dict[str, Any]],
) -> Any:
    """
    Create one dbt staging DAG that runs every staging table for one year.
    """
    year_suffix = year_dag_suffix(year)

    @dag(
        dag_id=f"dbt_staging_tables__all__{year_suffix}",
        description=f"Run all dbt staging tables for {year}.",
        schedule=PIPELINE_SCHEDULE,
        start_date=PIPELINE_START_DATE,
        catchup=False,
        tags=["dbt", "staging", "all", year_suffix],
    )
    def dbt_staging_year_group() -> None:
        """
        Define the year-scoped dbt staging pipeline.
        """

        @task
        def run_staging_table_for_year(
            staging_table: dict[str, Any],
            batch: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Build one staging table for this year-level DAG.
            """
            dbt_vars = _dbt_vars(staging_table, batch["vars"])
            _log_staging_table_context(staging_table, batch, dbt_vars)
            logger.info(
                "dbt adapter output below includes row counts and bytes processed "
                "where the adapter reports them."
            )

            command = _append_dbt_vars(
                [
                    *_dbt_command("run"),
                    "--select",
                    staging_table["model"],
                ],
                dbt_vars,
            )
            _run_dbt_command(command, "run")
            return {
                "model": staging_table["model"],
                "time_window": staging_table["time_window"],
                "batch_id": batch["batch_id"],
                "dbt_vars": dbt_vars,
            }

        @task
        def test_staging_table_for_year(
            staging_table: dict[str, Any],
            batch: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Validate one staging table for this year-level DAG.
            """
            model = staging_table["model"]
            dbt_vars = _dbt_vars(staging_table, batch["vars"])
            _log_staging_table_context(staging_table, batch, dbt_vars)
            logger.info("Starting validation for dbt model: %s", model)

            command = _append_dbt_vars(
                [
                    *_dbt_command("test"),
                    "--select",
                    model,
                    "--indirect-selection",
                    "buildable",
                ],
                dbt_vars,
            )
            _run_dbt_command(command, "test")
            logger.info(
                "dbt test summary is emitted above by dbt as PASS/WARN/ERROR/SKIP/TOTAL."
            )
            return {
                "model": model,
                "time_window": staging_table["time_window"],
                "batch_id": batch["batch_id"],
                "tested": True,
            }

        for staging_table in staging_tables:
            table_suffix = staging_table_dag_suffix(staging_table)
            batch = batches_by_table_id[staging_table["table_id"]]

            run_staging_table_task = run_staging_table_for_year.override(
                task_id=f"run_{table_suffix}",
            )(staging_table, batch)
            test_staging_table_task = test_staging_table_for_year.override(
                task_id=f"test_{table_suffix}",
            )(staging_table, batch)

            run_staging_table_task >> test_staging_table_task

    return dbt_staging_year_group()


configured_staging_tables = load_enabled_staging_tables(STAGING_TABLES_CONFIG_PATH)
configured_batches_by_table_id = load_staging_table_batches(
    TIME_VARS_CONFIG_PATH,
    configured_staging_tables,
)
configured_year_batches_by_table_id = group_staging_table_batches_by_year(
    configured_staging_tables,
    configured_batches_by_table_id,
)

for configured_staging_table in configured_staging_tables:
    for configured_batch in configured_batches_by_table_id[configured_staging_table["table_id"]]:
        globals()[
            "dbt_staging_tables__"
            f"{staging_table_dag_suffix(configured_staging_table)}__"
            f"{batch_dag_suffix(configured_batch)}"
        ] = _create_dbt_staging_table_dag(configured_staging_table, configured_batch)

for configured_year, configured_year_batches in configured_year_batches_by_table_id.items():
    globals()[f"dbt_staging_tables__all__{year_dag_suffix(configured_year)}"] = (
        _create_dbt_staging_year_dag(
            configured_year,
            configured_year_batches,
            configured_staging_tables,
        )
    )
