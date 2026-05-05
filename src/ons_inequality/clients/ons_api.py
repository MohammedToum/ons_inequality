from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class ONSAPIError(Exception):
    """Base exception for all ONS API-related errors."""


class ONSNotFoundError(ONSAPIError):
    """Raised when a requested ONS resource does not exist (HTTP 404)."""


class ONSClient:
    """
    Thin HTTP client for interacting with the ONS Beta API.

    Responsibilities:
    - Make HTTP requests
    - Handle retries for transient failures
    - Raise meaningful exceptions
    - Return raw JSON / bytes

    Explicitly does NOT:
    - Handle file storage
    - Transform data
    - Contain business logic
    """

    def __init__(
        self,
        base_url: str = "https://api.beta.ons.gov.uk/v1",
        timeout: float = 30.0,
    ) -> None:
        # Base API URL (trimmed to avoid double slashes)
        self.base_url = base_url.rstrip("/")

        # httpx Timeout object gives finer control vs plain float
        self.timeout = httpx.Timeout(timeout)

    # ----------------------------
    # Core HTTP methods
    # ----------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        # Only retry on network-related failures, not bad responses (e.g. 400/404)
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
        reraise=True,
    )
    def get_json(
        self,
        endpoint_or_url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Perform a GET request and return parsed JSON.

        Accepts either:
        - Relative endpoint (e.g. "/datasets")
        - Full URL (e.g. download.ons.gov.uk link)
        """
        response = self._get(endpoint_or_url, params=params)
        return response.json()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
        reraise=True,
    )
    def get_bytes(self, endpoint_or_url: str) -> bytes:
        """
        Perform a GET request and return raw bytes.

        Used for downloading files (CSV, CSVW, XLS, etc).
        """
        response = self._get(endpoint_or_url)
        return response.content

    # ----------------------------
    # ONS-specific endpoints
    # ----------------------------

    def list_datasets(self) -> dict[str, Any]:
        """Return all available datasets from ONS."""
        return self.get_json("/datasets")

    def get_dataset(self, dataset_id: str) -> dict[str, Any]:
        """Return metadata for a specific dataset."""
        return self.get_json(f"/datasets/{dataset_id}")

    def get_dataset_editions(self, dataset_id: str) -> dict[str, Any]:
        """Return all editions for a dataset (e.g. time-series, monthly, etc)."""
        return self.get_json(f"/datasets/{dataset_id}/editions")

    def get_dataset_versions(self, dataset_id: str, edition: str) -> dict[str, Any]:
        """Return all available versions for a given dataset edition."""
        return self.get_json(
            f"/datasets/{dataset_id}/editions/{edition}/versions"
        )

    def get_dataset_version(
        self,
        dataset_id: str,
        edition: str,
        version: str | int,
    ) -> dict[str, Any]:
        """
        Return metadata for a specific dataset version.

        This includes:
        - release date
        - download links (CSV, CSVW, XLS)
        - dataset structure
        """
        return self.get_json(
            f"/datasets/{dataset_id}/editions/{edition}/versions/{version}"
        )

    def get_observations(
        self,
        dataset_id: str,
        edition: str,
        version: str | int,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Query observation-level data.

        Typically used for filtered queries rather than full dataset ingestion.
        """
        return self.get_json(
            f"/datasets/{dataset_id}/editions/{edition}/versions/{version}/observations",
            params=params,
        )

    # ----------------------------
    # Helpers
    # ----------------------------

    def get_download_urls(self, metadata: dict[str, Any]) -> dict[str, str | None]:
        """
        Extract download URLs from dataset metadata.

        Keeps API response parsing in one place to reduce breakage if
        ONS changes response structure.
        """
        downloads = metadata.get("downloads", {})

        return {
            "csv": downloads.get("csv", {}).get("href"),
            "csvw": downloads.get("csvw", {}).get("href"),
            "xls": downloads.get("xls", {}).get("href"),
        }

    def _get(
        self,
        endpoint_or_url: str,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """
        Internal method to perform GET requests with consistent error handling.

        Handles:
        - URL resolution
        - HTTP status validation
        - Mapping HTTP errors to domain-specific exceptions
        """
        url = self._resolve_url(endpoint_or_url)

        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            response = client.get(url, params=params)

        # Explicit handling for 404 to allow upstream logic to react accordingly
        if response.status_code == 404:
            raise ONSNotFoundError(f"ONS resource not found: {url}")

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # Wrap generic HTTP errors in a domain-specific exception
            raise ONSAPIError(
                f"ONS API request failed: {response.status_code} {url}"
            ) from exc

        return response

    def _resolve_url(self, endpoint_or_url: str) -> str:
        """
        Resolve relative endpoints into full URLs.

        Allows flexibility:
        - "/datasets" → base_url + endpoint
        - full URLs (e.g. download links) pass through unchanged
        """
        if endpoint_or_url.startswith(("http://", "https://")):
            return endpoint_or_url

        return f"{self.base_url}/{endpoint_or_url.lstrip('/')}"