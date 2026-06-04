"""
Model-scoped Airflow DAGs for dbt current-state intermediate models.

Each configured int_*_current model gets one DAG per configured batch in
configs/time_vars.yml. The DAG builds the configured dbt intermediate model and
then runs its tests. Optional start/end year values are passed through to dbt so
the batch context matches the upstream staging and downstream fact-mart runs.
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
PIPELINE_SCHEDULE = os.environ.get("DBT_INTERMEDIATE_CURRENTS_SCHEDULE") or None

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "ons_inequality"
LOCAL_DBT_PROFILES_DIR = REPO_ROOT / "dbt" / "profiles"
DEFAULT_DBT_PROJECT_DIR = Path("/opt/airflow/dbt/ons_inequality")
DEFAULT_DBT_PROFILES_DIR = Path("/opt/airflow/dbt/profiles")
DEFAULT_INTERMEDIATE_CURRENTS_CONFIG_PATH = Path(
    "/opt/airflow/configs/dbt_intermediate_currents.yml"
)
LOCAL_INTERMEDIATE_CURRENTS_CONFIG_PATH = REPO_ROOT / "configs" / "dbt_intermediate_currents.yml"
DEFAULT_TIME_VARS_CONFIG_PATH = Path("/opt/airflow/configs/time_vars.yml")
LOCAL_TIME_VARS_CONFIG_PATH = REPO_ROOT / "configs" / "time_vars.yml"

REQUIRED_INTERMEDIATE_CURRENT_KEYS = [
    "pipeline_id",
    "model",
    "source_staging_tables",
    "time_window",
]
VALID_TIME_WINDOWS = {"annual"}
VALID_BATCH_VAR_KEYS = {"annual": {"start_year", "end_year"}}
INTERMEDIATE_WINDOW_PARAMS = {
    "start_year": None,
    "end_year": None,
}


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
    "DBT_INTERMEDIATE_CURRENTS_PROFILES_DIR",
    DEFAULT_DBT_PROFILES_DIR,
    LOCAL_DBT_PROFILES_DIR,
    fallback_env_var_name="DBT_PROFILES_DIR",
)
INTERMEDIATE_CURRENTS_CONFIG_PATH = Path(
    os.environ.get(
        "DBT_INTERMEDIATE_CURRENTS_CONFIG_PATH",
        str(
            DEFAULT_INTERMEDIATE_CURRENTS_CONFIG_PATH
            if DEFAULT_INTERMEDIATE_CURRENTS_CONFIG_PATH.exists()
            else LOCAL_INTERMEDIATE_CURRENTS_CONFIG_PATH
        ),
    )
)
TIME_VARS_CONFIG_PATH = _resolve_path(
    "TIME_VARS_CONFIG_PATH",
    DEFAULT_TIME_VARS_CONFIG_PATH,
    LOCAL_TIME_VARS_CONFIG_PATH,
    fallback_env_var_name="YEAR_VARS_CONFIG_PATH",
)


def _safe_suffix(value: str) -> str:
    """
    Build a stable, Airflow-safe suffix.
    """
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()


def batch_dag_suffix(batch: dict[str, Any]) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured time batch.
    """
    return _safe_suffix(str(batch["batch_id"]))


def year_dag_suffix(year: int) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured year.
    """
    return _safe_suffix(str(year))


def load_enabled_intermediate_currents(config_path: Path) -> list[dict[str, Any]]:
    """
    Load enabled dbt intermediate-current model configs.
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"Intermediate currents config does not exist: {config_path}"
        )

    with config_path.open() as file:
        config = yaml.safe_load(file) or {}

    intermediate_currents = config.get("intermediate_currents")

    if not isinstance(intermediate_currents, list):
        raise ValueError(
            f"Config file must contain an intermediate_currents list: {config_path}"
        )

    enabled_intermediate_currents = [
        intermediate_current
        for intermediate_current in intermediate_currents
        if intermediate_current.get("enabled", True)
    ]

    for intermediate_current in enabled_intermediate_currents:
        for key in REQUIRED_INTERMEDIATE_CURRENT_KEYS:
            if key not in intermediate_current:
                raise ValueError(
                    f"Intermediate current config missing required key {key}: "
                    f"{intermediate_current}"
                )

        source_staging_tables = intermediate_current["source_staging_tables"]
        if not isinstance(source_staging_tables, list) or not source_staging_tables:
            raise ValueError(
                "Intermediate current must define at least one source staging table: "
                f"{intermediate_current}"
            )

        time_window = intermediate_current["time_window"]
        if time_window not in VALID_TIME_WINDOWS:
            raise ValueError(
                f"Intermediate current time_window must be one of "
                f"{sorted(VALID_TIME_WINDOWS)}: {intermediate_current}"
            )

        configured_vars = intermediate_current.get("vars", {})
        if configured_vars is not None and not isinstance(configured_vars, dict):
            raise ValueError(f"vars must be a mapping when provided: {intermediate_current}")

        batch_vars = intermediate_current.get("batch_vars", {})
        if batch_vars is not None and not isinstance(batch_vars, dict):
            raise ValueError(
                f"batch_vars must be a mapping when provided: {intermediate_current}"
            )

        invalid_batch_vars = set(batch_vars or {}) - VALID_BATCH_VAR_KEYS[time_window]
        if invalid_batch_vars:
            raise ValueError(
                f"Invalid {time_window} batch var(s) {sorted(invalid_batch_vars)} "
                f"for intermediate current {intermediate_current['pipeline_id']}"
            )

    return enabled_intermediate_currents


def load_intermediate_batches(
    config_path: Path,
    intermediate_models: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """
    Load and validate time batch vars for each intermediate-current model.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Time vars config does not exist: {config_path}")

    with config_path.open() as file:
        config = yaml.safe_load(file) or {}

    intermediate_batches = (config.get("pipelines") or {}).get("intermediate_currents")
    if not isinstance(intermediate_batches, dict):
        raise ValueError(
            f"Config file must contain pipelines.intermediate_currents mapping: {config_path}"
        )

    batches_by_pipeline_id: dict[str, list[dict[str, Any]]] = {}

    for intermediate_model in intermediate_models:
        pipeline_id = intermediate_model["pipeline_id"]
        pipeline_batches = intermediate_batches.get(pipeline_id)
        if not isinstance(pipeline_batches, dict):
            raise ValueError(
                f"Missing time vars pipeline config for intermediate current: {pipeline_id}"
            )

        time_window = pipeline_batches.get("time_window")
        if time_window != intermediate_model["time_window"]:
            raise ValueError(
                "Time vars time_window mismatch for intermediate current "
                f"{pipeline_id}: expected {intermediate_model['time_window']}, "
                f"got {time_window}"
            )

        batches = pipeline_batches.get("batches")
        if not isinstance(batches, list) or not batches:
            raise ValueError(
                f"Time vars batches must be a non-empty list for: {pipeline_id}"
            )

        valid_batch_var_keys = VALID_BATCH_VAR_KEYS[time_window]
        validated_batches = []

        for batch in batches:
            if not isinstance(batch, dict):
                raise ValueError(f"Time vars batch must be a mapping for {pipeline_id}: {batch}")

            if "batch_id" not in batch:
                raise ValueError(f"Time vars batch missing batch_id for {pipeline_id}: {batch}")

            batch_vars = batch.get("vars")
            if not isinstance(batch_vars, dict):
                raise ValueError(
                    f"Time vars batch vars must be a mapping for {pipeline_id}: {batch}"
                )

            invalid_batch_vars = set(batch_vars) - valid_batch_var_keys
            if invalid_batch_vars:
                raise ValueError(
                    f"Invalid {time_window} time var(s) {sorted(invalid_batch_vars)} "
                    f"for {pipeline_id} batch {batch['batch_id']}"
                )

            validated_batches.append(
                {
                    "batch_id": str(batch["batch_id"]),
                    "time_window": time_window,
                    "vars": _non_null_time_vars(batch_vars, valid_batch_var_keys),
                }
            )

        batches_by_pipeline_id[pipeline_id] = validated_batches

    return batches_by_pipeline_id


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


def group_intermediate_batches_by_year(
    intermediate_models: list[dict[str, Any]],
    batches_by_pipeline_id: dict[str, list[dict[str, Any]]],
) -> dict[int, dict[str, dict[str, Any]]]:
    """
    Group configured intermediate batches into complete year-level runs.
    """
    batches_by_pipeline_and_year: dict[str, dict[int, dict[str, Any]]] = {}
    common_years: set[int] | None = None

    for intermediate_model in intermediate_models:
        pipeline_id = intermediate_model["pipeline_id"]
        batches_by_year: dict[int, dict[str, Any]] = {}

        for batch in batches_by_pipeline_id[pipeline_id]:
            year = _single_year_from_batch(batch)
            if year is not None:
                batches_by_year[year] = batch

        batches_by_pipeline_and_year[pipeline_id] = batches_by_year
        pipeline_years = set(batches_by_year)
        common_years = pipeline_years if common_years is None else common_years & pipeline_years

    return {
        year: {
            intermediate_model["pipeline_id"]: batches_by_pipeline_and_year[
                intermediate_model["pipeline_id"]
            ][year]
            for intermediate_model in intermediate_models
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
    intermediate_model: dict[str, Any],
    batch: dict[str, Any],
    run_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Merge static vars, configured batch vars, time batch vars, and run overrides.
    """
    valid_batch_var_keys = VALID_BATCH_VAR_KEYS[batch["time_window"]]
    configured_vars = dict(intermediate_model.get("vars") or {})
    configured_batch_vars = _non_null_time_vars(
        intermediate_model.get("batch_vars") or {},
        valid_batch_var_keys,
    )
    time_vars = _non_null_time_vars(batch["vars"], valid_batch_var_keys)
    param_vars = _non_null_time_vars(run_params or {}, valid_batch_var_keys)

    return {**configured_vars, **configured_batch_vars, **time_vars, **param_vars}


def _append_dbt_vars(command: list[str], dbt_vars: dict[str, Any]) -> list[str]:
    """
    Add a dbt --vars YAML payload when this batch has configured vars.
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


def _log_intermediate_context(
    intermediate_model: dict[str, Any],
    batch: dict[str, Any],
    dbt_vars: dict[str, Any],
) -> None:
    """
    Log the intermediate config that controls this generated DAG.
    """
    logger.info("Intermediate pipeline id: %s", intermediate_model["pipeline_id"])
    logger.info("Intermediate description: %s", intermediate_model.get("description", ""))
    logger.info("Configured dbt model: %s", intermediate_model["model"])
    logger.info(
        "Configured source staging table(s): %s",
        intermediate_model["source_staging_tables"],
    )
    logger.info("Configured time window type: %s", batch["time_window"])
    logger.info("Configured time batch id: %s", batch["batch_id"])
    logger.info("Configured dbt vars: %s", dbt_vars)


def _create_dbt_intermediate_current_dag(
    intermediate_model: dict[str, Any],
    batch: dict[str, Any],
) -> Any:
    """
    Create one dbt intermediate-current DAG for a configured model and batch.
    """
    model_suffix = _safe_suffix(intermediate_model["pipeline_id"])
    batch_suffix = batch_dag_suffix(batch)
    model_name = intermediate_model["model"]

    @dag(
        dag_id=f"dbt_intermediate_currents__{model_suffix}__{batch_suffix}",
        description=f"Run dbt intermediate model {model_name} for batch {batch['batch_id']}.",
        schedule=PIPELINE_SCHEDULE,
        start_date=PIPELINE_START_DATE,
        catchup=False,
        params=INTERMEDIATE_WINDOW_PARAMS,
        tags=["dbt", "intermediate", "current", batch["time_window"], model_suffix, batch_suffix],
    )
    def dbt_intermediate_current_group() -> None:
        """
        Define the model-scoped dbt intermediate-current pipeline.
        """

        @task
        def run_intermediate_current() -> dict[str, Any]:
            """
            Build this intermediate-current model with its configured time-window vars.
            """
            context = get_current_context()
            dbt_vars = _dbt_vars(intermediate_model, batch, context["params"])
            _log_intermediate_context(intermediate_model, batch, dbt_vars)
            logger.info(
                "dbt adapter output below includes row counts and bytes processed "
                "where the adapter reports them."
            )

            command = _append_dbt_vars(
                [
                    *_dbt_command("run"),
                    "--select",
                    model_name,
                ],
                dbt_vars,
            )
            _run_dbt_command(command, "run")
            return {
                "model": model_name,
                "time_window": batch["time_window"],
                "batch_id": batch["batch_id"],
                "dbt_vars": dbt_vars,
            }

        @task
        def test_intermediate_current() -> dict[str, Any]:
            """
            Validate this intermediate-current model.
            """
            context = get_current_context()
            dbt_vars = _dbt_vars(intermediate_model, batch, context["params"])
            _log_intermediate_context(intermediate_model, batch, dbt_vars)
            logger.info("Starting validation for dbt model: %s", model_name)

            command = _append_dbt_vars(
                [
                    *_dbt_command("test"),
                    "--select",
                    model_name,
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
                "model": model_name,
                "time_window": batch["time_window"],
                "batch_id": batch["batch_id"],
                "tested": True,
            }

        run_intermediate_current_task = run_intermediate_current()
        test_intermediate_current_task = test_intermediate_current()

        run_intermediate_current_task >> test_intermediate_current_task

    return dbt_intermediate_current_group()


def _create_dbt_intermediate_year_dag(
    year: int,
    batches_by_pipeline_id: dict[str, dict[str, Any]],
    intermediate_models: list[dict[str, Any]],
) -> Any:
    """
    Create one dbt intermediate-current DAG that runs every current model for one year.
    """
    year_suffix = year_dag_suffix(year)

    @dag(
        dag_id=f"dbt_intermediate_currents__all__{year_suffix}",
        description=f"Run all dbt intermediate current models for {year}.",
        schedule=PIPELINE_SCHEDULE,
        start_date=PIPELINE_START_DATE,
        catchup=False,
        tags=["dbt", "intermediate", "current", "all", year_suffix],
    )
    def dbt_intermediate_year_group() -> None:
        """
        Define the year-scoped dbt intermediate-current pipeline.
        """

        @task
        def run_intermediate_current_for_year(
            intermediate_model: dict[str, Any],
            batch: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Build one intermediate-current model for this year-level DAG.
            """
            model_name = intermediate_model["model"]
            dbt_vars = _dbt_vars(intermediate_model, batch)
            _log_intermediate_context(intermediate_model, batch, dbt_vars)
            logger.info(
                "dbt adapter output below includes row counts and bytes processed "
                "where the adapter reports them."
            )

            command = _append_dbt_vars(
                [
                    *_dbt_command("run"),
                    "--select",
                    model_name,
                ],
                dbt_vars,
            )
            _run_dbt_command(command, "run")
            return {
                "model": model_name,
                "time_window": batch["time_window"],
                "batch_id": batch["batch_id"],
                "dbt_vars": dbt_vars,
            }

        @task
        def test_intermediate_current_for_year(
            intermediate_model: dict[str, Any],
            batch: dict[str, Any],
        ) -> dict[str, Any]:
            """
            Validate one intermediate-current model for this year-level DAG.
            """
            model_name = intermediate_model["model"]
            dbt_vars = _dbt_vars(intermediate_model, batch)
            _log_intermediate_context(intermediate_model, batch, dbt_vars)
            logger.info("Starting validation for dbt model: %s", model_name)

            command = _append_dbt_vars(
                [
                    *_dbt_command("test"),
                    "--select",
                    model_name,
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
                "model": model_name,
                "time_window": batch["time_window"],
                "batch_id": batch["batch_id"],
                "tested": True,
            }

        for intermediate_model in intermediate_models:
            model_suffix = _safe_suffix(intermediate_model["pipeline_id"])
            batch = batches_by_pipeline_id[intermediate_model["pipeline_id"]]

            run_intermediate_current_task = run_intermediate_current_for_year.override(
                task_id=f"run_{model_suffix}",
            )(intermediate_model, batch)
            test_intermediate_current_task = test_intermediate_current_for_year.override(
                task_id=f"test_{model_suffix}",
            )(intermediate_model, batch)

            run_intermediate_current_task >> test_intermediate_current_task

    return dbt_intermediate_year_group()


configured_intermediate_currents = load_enabled_intermediate_currents(
    INTERMEDIATE_CURRENTS_CONFIG_PATH,
)
configured_batches_by_pipeline_id = load_intermediate_batches(
    TIME_VARS_CONFIG_PATH,
    configured_intermediate_currents,
)
configured_year_batches_by_pipeline_id = group_intermediate_batches_by_year(
    configured_intermediate_currents,
    configured_batches_by_pipeline_id,
)

for configured_intermediate_model in configured_intermediate_currents:
    for configured_batch in configured_batches_by_pipeline_id[
        configured_intermediate_model["pipeline_id"]
    ]:
        globals()[
            "dbt_intermediate_currents__"
            f"{_safe_suffix(configured_intermediate_model['pipeline_id'])}__"
            f"{batch_dag_suffix(configured_batch)}"
        ] = _create_dbt_intermediate_current_dag(
            configured_intermediate_model,
            configured_batch,
        )

for configured_year, configured_year_batches in configured_year_batches_by_pipeline_id.items():
    globals()[f"dbt_intermediate_currents__all__{year_dag_suffix(configured_year)}"] = (
        _create_dbt_intermediate_year_dag(
            configured_year,
            configured_year_batches,
            configured_intermediate_currents,
        )
    )
