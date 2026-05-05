import json
import logging
import time
from pathlib import Path
from typing import Any

from ons_inequality.clients.ons_api import ONSClient

logger = logging.getLogger(__name__)


class ONSExtractor:
    """
    Extracts raw ONS dataset assets to local storage.

    Responsibilities:
    - Fetch dataset version metadata
    - Extract download URLs from metadata
    - Download CSV data
    - Download CSVW metadata when available
    - Save everything in a reproducible folder structure
    """

    def __init__(self, client: ONSClient, raw_path: Path) -> None:
        self.client = client
        self.raw_path = raw_path

    def download_dataset(
        self,
        dataset_id: str,
        edition: str,
        version: str | int,
    ) -> Path:
        """
        Download one ONS dataset version into the raw data layer.

        Supports either an explicit version number or version="latest".
        """
        if version == "latest":
            logger.info("Resolving latest version for %s | %s", dataset_id, edition)

            versions = self.client.get_dataset_versions(
                dataset_id=dataset_id,
                edition=edition,
            )

            items = versions.get("items", [])
            if not items:
                raise ValueError(f"No versions found for {dataset_id}/{edition}")

            version = max(int(item["version"]) for item in items)

        logger.info(
            "Fetching metadata for %s | %s | version=%s",
            dataset_id,
            edition,
            version,
        )

        metadata = self.client.get_dataset_version(
            dataset_id=dataset_id,
            edition=edition,
            version=version,
        )

        dataset_path = self._build_dataset_path(
            dataset_id=dataset_id,
            edition=edition,
            version=version,
        )
        dataset_path.mkdir(parents=True, exist_ok=True)

        self._save_json(metadata, dataset_path / "metadata.json")

        download_urls = self.client.get_download_urls(metadata)

        csv_url = download_urls.get("csv")
        if not csv_url:
            raise ValueError(
                f"No CSV download URL found for {dataset_id}/{edition}/version_{version}"
            )

        csv_path = dataset_path / "data.csv"
        logger.info("Downloading CSV to %s", csv_path)
        self._download_to_file(csv_url, csv_path)

        csvw_url = download_urls.get("csvw")
        csvw_path = dataset_path / "csvw_metadata.json"

        if csvw_url:
            if csvw_path.exists():
                logger.info("Skipping existing CSVW metadata: %s", csvw_path)
            else:
                logger.info("Downloading CSVW metadata to %s", csvw_path)

                try:
                    csvw_json = self.client.get_json(csvw_url)
                    self._save_json(csvw_json, csvw_path)
                    time.sleep(2)
                except Exception as exc:
                    logger.warning(
                        "Failed to download CSVW metadata for %s/%s/version_%s: %s",
                        dataset_id,
                        edition,
                        version,
                        exc,
                    )
        else:
            logger.info("No CSVW metadata available for this dataset version")

        logger.info("Dataset assets saved to %s", dataset_path)

        return dataset_path

    def download_many(self, datasets: list[dict[str, Any]]) -> list[Path]:
        """
        Download multiple datasets from config-style dictionaries.
        """
        downloaded_paths: list[Path] = []

        for dataset in datasets:
            if not dataset.get("enabled", True):
                logger.info("Skipping disabled dataset: %s", dataset.get("dataset_id"))
                continue

            downloaded_path = self.download_dataset(
                dataset_id=dataset["dataset_id"],
                edition=dataset["edition"],
                version=dataset["version"],
            )
            downloaded_paths.append(downloaded_path)

        return downloaded_paths

    def _download_to_file(
        self,
        url: str,
        output_path: Path,
        overwrite: bool = False,
    ) -> Path:
        """
        Download bytes from a URL and write them to disk.

        Existing files are skipped by default to avoid unnecessary repeat
        requests during local reruns and Airflow retries.
        """
        if output_path.exists() and not overwrite:
            logger.info("Skipping existing file: %s", output_path)
            return output_path

        output_path.parent.mkdir(parents=True, exist_ok=True)

        content = self.client.get_bytes(url)
        output_path.write_bytes(content)

        time.sleep(2)

        return output_path

    def _save_json(
        self,
        data: dict[str, Any],
        output_path: Path,
        overwrite: bool = True,
    ) -> Path:
        """
        Save metadata as valid, formatted JSON.
        """
        if output_path.exists() and not overwrite:
            logger.info("Skipping existing JSON file: %s", output_path)
            return output_path

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return output_path

    def _build_dataset_path(
        self,
        dataset_id: str,
        edition: str,
        version: str | int,
    ) -> Path:
        """
        Build the raw-layer folder path for one dataset version.
        """
        return self.raw_path / dataset_id / edition / f"version_{version}"