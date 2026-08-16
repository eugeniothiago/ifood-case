"""Reliable, idempotent downloading of NYC TLC monthly Parquet files.

Provides multiple download strategies with automatic fallback:
  1. Python requests (primary)
  2. wget via subprocess (fallback when Python DNS resolver fails)
  3. curl via subprocess (second fallback)

This resilience is necessary because Databricks Community Edition clusters
can have intermittent DNS resolution issues from the Python process while
shell tools (wget/curl) use the system resolver and may succeed.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Final, Iterable

import requests

from .config import taxi_file_url

CHUNK_SIZE_BYTES: Final[int] = 1024 * 1024
REQUEST_TIMEOUT: Final[tuple[int, int]] = (15, 300)


def _download_with_requests(url: str, destination: Path) -> None:
    """Download a single file using the requests library with streaming."""
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=destination.parent, prefix=f".{destination.name}.", delete=False
        ) as tmp:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if chunk:
                    tmp.write(chunk)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = tmp.name
    if os.path.getsize(tmp_path) == 0:
        os.unlink(tmp_path)
        raise IOError(f"Downloaded file is empty: {url}")
    os.replace(tmp_path, destination)


def _download_with_wget(url: str, destination: Path) -> None:
    """Download a single file using wget via subprocess.

    wget uses the system C library DNS resolver (getaddrinfo) which may
    succeed when Python's requests library fails due to DNS issues.
    """
    result = subprocess.run(
        ["wget", "-q", "-O", str(destination), url],
        capture_output=True,
        timeout=600,
    )
    if result.returncode != 0 or not destination.is_file() or destination.stat().st_size == 0:
        raise RuntimeError(f"wget failed for {url}: {result.stderr.decode('utf-8', errors='replace')}")


def _download_with_curl(url: str, destination: Path) -> None:
    """Download a single file using curl via subprocess."""
    result = subprocess.run(
        ["curl", "-sL", "-o", str(destination), url],
        capture_output=True,
        timeout=600,
    )
    if result.returncode != 0 or not destination.is_file() or destination.stat().st_size == 0:
        raise RuntimeError(f"curl failed for {url}: {result.stderr.decode('utf-8', errors='replace')}")


def download_taxi_data(year: int, months: Iterable[int], landing_path: str) -> list[str]:
    """Download requested TLC files and return local paths in month order.

    Existing non-empty files are treated as complete and skipped. New files
    are downloaded using a multi-strategy approach with automatic fallback:
    requests -> wget -> curl. This ensures resilience against DNS resolution
    differences between Python and system tools on Databricks clusters.

    Args:
        year: Four-digit TLC data year.
        months: Months to download, each from 1 through 12.
        landing_path: Local filesystem directory for downloaded files.

    Returns:
        Ordered list of local file paths for the requested months.

    Raises:
        RuntimeError: If all download strategies fail for a file.
    """
    target_dir = Path(landing_path)
    target_dir.mkdir(parents=True, exist_ok=True)
    downloaded_paths: list[str] = []

    for month in months:
        url = taxi_file_url(year, month)
        destination = target_dir / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"

        # Idempotent: skip existing non-empty files.
        if destination.is_file() and destination.stat().st_size > 0:
            downloaded_paths.append(str(destination))
            continue

        # Try each download strategy in order, falling back on failure.
        strategies = [
            ("requests", lambda: _download_with_requests(url, destination)),
            ("wget", lambda: _download_with_wget(url, destination)),
            ("curl", lambda: _download_with_curl(url, destination)),
        ]

        success = False
        errors: list[str] = []
        for name, strategy in strategies:
            try:
                strategy()
                if destination.is_file() and destination.stat().st_size > 0:
                    print(f"  Downloaded {destination.name} via {name}")
                    success = True
                    break
                else:
                    errors.append(f"{name}: file empty after download")
            except Exception as exc:
                errors.append(f"{name}: {exc}")
                # Clean up partial file before trying next strategy.
                if destination.exists():
                    destination.unlink(missing_ok=True)

        if not success:
            raise RuntimeError(
                f"All download strategies failed for {url}:\n  "
                + "\n  ".join(errors)
            )

        downloaded_paths.append(str(destination))

    return downloaded_paths
