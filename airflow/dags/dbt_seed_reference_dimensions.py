"""
Seed-scoped Airflow DAGs for dbt reference dimensions.

Each enabled seed group in configs/dim_seeds.yml gets one DAG. Within that DAG,
dbt uploads the configured seed CSV files first, then runs the configured
dimension model(s) that consume those seeds.
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
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

PIPELINE_START_DATE = datetime(2026, 1, 1)
PIPELINE_SCHEDULE = os.environ.get("DBT_SEED_DIMENSIONS_SCHEDULE") or None

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "ons_inequality"
LOCAL_DBT_PROFILES_DIR = REPO_ROOT / "dbt" / "profiles"
DEFAULT_DBT_PROJECT_DIR = Path("/opt/airflow/dbt/ons_inequality")
DEFAULT_DBT_PROFILES_DIR = Path("/opt/airflow/dbt/profiles")
DEFAULT_DIM_SEEDS_CONFIG_PATH = Path("/opt/airflow/configs/dim_seeds.yml")
LOCAL_DIM_SEEDS_CONFIG_PATH = REPO_ROOT / "configs" / "dim_seeds.yml"


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
    "DBT_SEED_DIMENSIONS_PROFILES_DIR",
    DEFAULT_DBT_PROFILES_DIR,
    LOCAL_DBT_PROFILES_DIR,
    fallback_env_var_name="DBT_PROFILES_DIR",
)
DIM_SEEDS_CONFIG_PATH = Path(
    os.environ.get(
        "DIM_SEEDS_CONFIG_PATH",
        str(
            DEFAULT_DIM_SEEDS_CONFIG_PATH
            if DEFAULT_DIM_SEEDS_CONFIG_PATH.exists()
            else LOCAL_DIM_SEEDS_CONFIG_PATH
        ),
    )
)

REQUIRED_SEED_GROUP_KEYS = ["group_id", "seeds", "models"]


def load_enabled_seed_groups(config_path: Path) -> list[dict[str, Any]]:
    """
    Load enabled dbt seed groups from the shared YAML config file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Dimension seed config does not exist: {config_path}")

    with config_path.open() as file:
        config = yaml.safe_load(file) or {}

    seed_groups = config.get("seed_groups")

    if not isinstance(seed_groups, list):
        raise ValueError(f"Config file must contain a seed_groups list: {config_path}")

    enabled_seed_groups = [
        seed_group
        for seed_group in seed_groups
        if seed_group.get("enabled", True)
    ]

    for seed_group in enabled_seed_groups:
        for key in REQUIRED_SEED_GROUP_KEYS:
            if key not in seed_group:
                raise ValueError(f"Seed group config missing required key {key}: {seed_group}")

        if not isinstance(seed_group["seeds"], list) or not seed_group["seeds"]:
            raise ValueError(f"Seed group must define at least one seed: {seed_group}")

        if not isinstance(seed_group["models"], list) or not seed_group["models"]:
            raise ValueError(f"Seed group must define at least one model: {seed_group}")

    return enabled_seed_groups


def seed_group_dag_suffix(seed_group: dict[str, Any]) -> str:
    """
    Build a stable, Airflow-safe DAG suffix for a configured seed group.
    """
    return re.sub(r"[^a-zA-Z0-9_]+", "_", seed_group["group_id"]).strip("_").lower()


def _dbt_command(command_name: str) -> list[str]:
    """
    Return dbt command arguments shared by seed and run invocations.
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


def _model_selectors(seed_group: dict[str, Any]) -> list[str]:
    """
    Build dbt model selectors for a seed group.
    """
    selectors = list(seed_group.get("upstream_models", []))

    if seed_group.get("run_upstream_models", False):
        selectors.extend(f"+{model_name}" for model_name in seed_group["models"])
    else:
        selectors.extend(seed_group["models"])

    return selectors


def _log_seed_group_context(seed_group: dict[str, Any], model_selectors: list[str]) -> None:
    """
    Log the seed group config that controls this generated DAG.
    """
    logger.info("Seed group id: %s", seed_group["group_id"])
    logger.info("Seed group description: %s", seed_group.get("description", ""))
    logger.info("Configured seed selector(s): %s", seed_group["seeds"])
    logger.info("Configured final dimension model(s): %s", seed_group["models"])
    logger.info("Configured upstream model selector(s): %s", seed_group.get("upstream_models", []))
    logger.info("run_upstream_models: %s", seed_group.get("run_upstream_models", False))
    logger.info("dbt model/test selector(s): %s", model_selectors)


def _create_dbt_seed_dimension_dag(seed_group: dict[str, Any]) -> Any:
    """
    Create one dbt seed-to-dimension DAG for a configured seed group.
    """
    dag_suffix = seed_group_dag_suffix(seed_group)

    @dag(
        dag_id=f"dbt_seed_reference_dimensions__{dag_suffix}",
        description=(
            "Run dbt seeds and dimension models for "
            f"{seed_group['group_id']} reference data."
        ),
        schedule=PIPELINE_SCHEDULE,
        start_date=PIPELINE_START_DATE,
        catchup=False,
        tags=["dbt", "seed", "dimensions", dag_suffix],
    )
    def dbt_seed_dimension_group() -> None:
        """
        Define the seed-group-scoped dbt dimension pipeline.
        """

        @task
        def seed_reference_tables() -> dict[str, list[str]]:
            """
            Upload this group's seed CSV files into BigQuery seed tables.
            """
            model_selectors = _model_selectors(seed_group)
            seed_selectors = list(seed_group["seeds"])
            _log_seed_group_context(seed_group, model_selectors)
            logger.info("dbt seed selector(s): %s", seed_selectors)

            command = [
                *_dbt_command("seed"),
                "--select",
                *seed_selectors,
            ]
            _run_dbt_command(command, "seed")
            return {"seeded": seed_selectors}

        @task
        def run_dimension_models(seed_result: dict[str, list[str]]) -> dict[str, list[str]]:
            """
            Build this group's configured dimension model(s).
            """
            logger.info("Seeded dbt seed(s): %s", seed_result["seeded"])
            model_selectors = _model_selectors(seed_group)
            _log_seed_group_context(seed_group, model_selectors)
            logger.info("Final dimension model(s) to build: %s", seed_group["models"])
            logger.info(
                "dbt adapter output below includes row counts and bytes processed "
                "where the adapter reports them."
            )

            command = [
                *_dbt_command("run"),
                "--select",
                *model_selectors,
            ]
            _run_dbt_command(command, "run")
            return {
                "models": list(seed_group["models"]),
                "model_selectors": model_selectors,
            }

        @task
        def test_dimension_models(model_result: dict[str, list[str]]) -> dict[str, list[str]]:
            """
            Validate this group's configured dimension model(s) and selected upstream models.
            """
            model_selectors = list(model_result["model_selectors"])
            _log_seed_group_context(seed_group, model_selectors)
            logger.info("Built final dimension model(s): %s", model_result["models"])
            logger.info("Starting validation for dbt selector(s): %s", model_selectors)

            command = [
                *_dbt_command("test"),
                "--select",
                *model_selectors,
                "--indirect-selection",
                "buildable",
            ]
            _run_dbt_command(command, "test")
            logger.info(
                "dbt test summary is emitted above by dbt as PASS/WARN/ERROR/SKIP/TOTAL."
            )
            return {
                "models": list(model_result["models"]),
                "tested_selectors": model_selectors,
            }

        test_dimension_models(run_dimension_models(seed_reference_tables()))

    return dbt_seed_dimension_group()


for configured_seed_group in load_enabled_seed_groups(DIM_SEEDS_CONFIG_PATH):
    globals()[
        f"dbt_seed_reference_dimensions__{seed_group_dag_suffix(configured_seed_group)}"
    ] = _create_dbt_seed_dimension_dag(configured_seed_group)
