"""
Entry point for ONS data ingestion.

This script supports two execution modes:

1. Single dataset mode
   - Useful for testing, debugging, or ad-hoc runs

2. Batch mode via YAML config
   - Preferred for reproducibility and orchestration

The script delegates ingestion logic to the extraction layer.
"""

import argparse
import logging
from pathlib import Path
from typing import Any

import yaml

from clients.ons_api import ONSClient
from extraction.ons_extractor import ONSExtractor

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for either single dataset or config-driven ingestion.
    """
    parser = argparse.ArgumentParser(description="ONS Data Ingestion Script")

    parser.add_argument("--dataset_id", help="ONS dataset identifier")
    parser.add_argument("--edition", help="Dataset edition, e.g. 'time-series'")
    parser.add_argument("--version", help="Dataset version, e.g. 130 or 'latest'")

    parser.add_argument(
        "--config",
        help="Path to YAML config file defining datasets to ingest",
    )

    return parser.parse_args()


def load_config(config_path: str) -> dict[str, Any]:
    """
    Load dataset configuration from YAML.

    Expected structure:

    datasets:
      - dataset_id: ...
        edition: ...
        version: ...
        enabled: true
    """
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"Config file is empty or invalid: {config_path}")

    return config


def main() -> None:
    """
    Select the execution mode and trigger the extraction flow.
    """
    args = parse_args()

    client = ONSClient()
    extractor = ONSExtractor(
        client=client,
        raw_path=Path("data/raw/ons"),
    )

    if args.config:
        logger.info("Running batch ingestion from config: %s", args.config)

        config = load_config(args.config)

        if "datasets" not in config:
            raise ValueError("Config file must contain a 'datasets' key")

        logger.info("Found %s dataset(s) in config", len(config["datasets"]))

        extractor.download_many(config["datasets"])

        logger.info("Batch ingestion completed successfully")
        return

    if not all([args.dataset_id, args.edition, args.version]):
        raise ValueError(
            "Provide either --config OR all of "
            "--dataset_id, --edition, --version"
        )

    logger.info(
        "Downloading dataset: %s | %s | version=%s",
        args.dataset_id,
        args.edition,
        args.version,
    )

    extractor.download_dataset(
        dataset_id=args.dataset_id,
        edition=args.edition,
        version=args.version,
    )

    logger.info("Dataset ingestion completed successfully")


if __name__ == "__main__":
    main()
