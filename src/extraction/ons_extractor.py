import csv
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

from clients.ons_api import ONSClient

logger = logging.getLogger(__name__)


class ONSExtractor:
    """
    Extracts raw ONS dataset assets to local storage.
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

        temp_csv_path = dataset_path / "_download.tmp.csv"

        logger.info("Downloading CSV to temporary path: %s", temp_csv_path)
        self._download_to_file(csv_url, temp_csv_path)

        min_period, max_period = self._extract_time_range(temp_csv_path)

        csv_filename = self._build_csv_filename(
            dataset_id=dataset_id,
            edition=edition,
            version=version,
            min_period=min_period,
            max_period=max_period,
        )

        csv_path = dataset_path / csv_filename

        if csv_path.exists():
            logger.info("Final CSV already exists: %s", csv_path)
            temp_csv_path.unlink(missing_ok=True)
        else:
            logger.info("Renaming CSV to %s", csv_path)
            temp_csv_path.rename(csv_path)

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

    def _extract_time_range(self, csv_path: Path) -> tuple[str | None, str | None]:
        """
        Extract the min and max period values from the CSV's time column.
        """
        with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames:
                logger.warning("CSV has no header row: %s", csv_path)
                return None, None

            time_column = self._find_time_column(reader.fieldnames)

            if not time_column:
                logger.warning(
                    "No recognised time column found in %s. Available columns: %s",
                    csv_path,
                    reader.fieldnames,
                )
                return None, None

            periods: list[tuple[tuple[int, int], str]] = []

            for row in reader:
                raw_value = row.get(time_column)

                if not raw_value:
                    continue

                period_label = raw_value.strip()
                sort_key = self._parse_period_sort_key(period_label)

                if sort_key:
                    periods.append((sort_key, period_label))

        if not periods:
            logger.warning("No parseable period values found in %s", csv_path)
            return None, None

        min_period = min(periods, key=lambda value: value[0])[1]
        max_period = max(periods, key=lambda value: value[0])[1]

        return min_period, max_period

    def _find_time_column(self, fieldnames: list[str]) -> str | None:
        """
        Find the most likely ONS time column from a CSV header.
        """
        normalised_fields = {
            self._normalise_column_name(fieldname): fieldname for fieldname in fieldnames
        }

        candidates = (
            "time",
            "yyyy_yy",
            "yyyy_q",
            "yyyy",
            "calendar_years",
            "calendar_year",
            "quarters",
            "quarter",
            "date",
        )

        for candidate in candidates:
            if candidate in normalised_fields:
                return normalised_fields[candidate]

        return None

    def _normalise_column_name(self, value: str) -> str:
        """
        Normalise CSV column names for robust matching.
        """
        value = value.strip().lower()
        value = re.sub(r"[^a-z0-9]+", "_", value)
        value = re.sub(r"_+", "_", value)
        return value.strip("_")

    def _parse_period_sort_key(self, value: str) -> tuple[int, int] | None:
        """
        Convert common ONS period labels into comparable sort keys.
        """
        cleaned_value = value.strip().lower()

        quarter_match = re.search(
            r"\b(19\d{2}|20\d{2})\s*[- ]?q([1-4])\b",
            cleaned_value,
        )
        if quarter_match:
            return int(quarter_match.group(1)), int(quarter_match.group(2))

        range_match = re.search(
            r"\b(19\d{2}|20\d{2})\s*-\s*\d{2}\b",
            cleaned_value,
        )
        if range_match:
            return int(range_match.group(1)), 0

        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", cleaned_value)
        if year_match:
            return int(year_match.group(1)), 0

        return None

    def _build_csv_filename(
        self,
        dataset_id: str,
        edition: str,
        version: str | int,
        min_period: str | None,
        max_period: str | None,
    ) -> str:
        """
        Build a descriptive raw CSV filename.
        """
        parts = [dataset_id, edition, f"v{version}"]

        if min_period and max_period:
            parts.extend([min_period, "to", max_period])

        filename = "__".join(parts)

        return f"{self._safe_filename(filename)}.csv"

    def _safe_filename(self, value: str) -> str:
        """
        Convert arbitrary strings into filesystem-safe filenames.
        """
        value = value.strip().lower()
        value = re.sub(r"[^a-z0-9]+", "_", value)
        value = re.sub(r"_+", "_", value)
        return value.strip("_")

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
