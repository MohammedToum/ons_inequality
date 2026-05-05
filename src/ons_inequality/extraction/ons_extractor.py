"""
Entry point for ONS data ingestion.

This script supports two execution modes:

1. Single dataset mode (CLI arguments)
   - Useful for testing, debugging, or ad-hoc runs

2. Batch mode via YAML config
   - Preferred for reproducibility and orchestration (e.g. Airflow)

The script delegates all business logic to the extraction layer.
It only handles:
- argument parsing
- execution mode selection
"""

import argparse
from pathlib import Path
import yaml

from ons_inequality.clients.ons_api import ONSClient
from ons_inequality.extraction.ons_extractor import ONSExtractor


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments.

    Supports:
    - Direct dataset specification (dataset_id, edition, version)
    - Config-driven batch ingestion via YAML
    """
    parser = argparse.ArgumentParser(
        description="ONS Data Ingestion Script"
    )

    # Single dataset mode
    parser.add_argument("--dataset_id", help="ONS dataset identifier")
    parser.add_argument("--edition", help="Dataset edition (e.g. 'time-series')")
    parser.add_argument("--version", help="Dataset version (e.g. 130 or 'latest')")

    # Config mode (preferred for production / Airflow)
    parser.add_argument(
        "--config",
        help="Path to YAML config file defining datasets to ingest",
    )

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    """
    Load dataset configuration from YAML.

    Expected structure:

    datasets:
      - dataset_id: ...
        edition: ...
        version: ...
        enabled: true
    """
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main() -> None:
    """
    Main execution flow.

    Chooses between:
    - Batch ingestion via config file
    - Single dataset ingestion via CLI args

    Delegates actual ingestion to ONSExtractor.
    """
    args = parse_args()

    client = ONSClient()

    extractor = ONSExtractor(
        client=client,
        raw_path=Path("data/raw/ons"),
    )

    # ----------------------------
    # Mode 1: YAML config (preferred)
    # ----------------------------
    if args.config:
        config = load_config(args.config)

        if "datasets" not in config:
            raise ValueError("Config file must contain a 'datasets' key")

        extractor.download_many(config["datasets"])
        return

    # ----------------------------
    # Mode 2: Single dataset CLI
    # ----------------------------
    if not all([args.dataset_id, args.edition, args.version]):
        raise ValueError(
            "Provide either --config OR all of "
            "--dataset_id, --edition, --version"
        )

    extractor.download_dataset(
        dataset_id=args.dataset_id,
        edition=args.edition,
        version=args.version,
    )


if __name__ == "__main__":
    main()