"""Reliable, idempotent downloading of NYC TLC monthly Parquet files."""

import os
import tempfile
from pathlib import Path
from typing import Final, Iterable

import requests

from .config import taxi_file_url

CHUNK_SIZE_BYTES: Final[int] = 1024 * 1024
REQUEST_TIMEOUT: Final[tuple[int, int]] = (15, 300)


def download_taxi_data(year: int, months: Iterable[int], landing_path: str) -> list[str]:
    """Download requested TLC files and return local paths in month order.

    Existing non-empty files are treated as complete and skipped. New files are
    streamed to a temporary file in the target directory, then atomically moved
    into place with ``os.replace`` so Spark never sees a partial download.
    """
    target_dir = Path(landing_path)
    target_dir.mkdir(parents=True, exist_ok=True)
    downloaded_paths: list[str] = []

    for month in months:
        url = taxi_file_url(year, month)
        destination = target_dir / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
        if destination.is_file() and destination.stat().st_size > 0:
            downloaded_paths.append(str(destination))
            continue

        temporary_path: str | None = None
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(
                    mode="wb", dir=target_dir, prefix=f".{destination.name}.", delete=False
                ) as temporary_file:
                    temporary_path = temporary_file.name
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                        if chunk:
                            temporary_file.write(chunk)
                    temporary_file.flush()
                    os.fsync(temporary_file.fileno())

            if temporary_path is None or os.path.getsize(temporary_path) == 0:
                raise IOError(f"Downloaded file is empty: {url}")
            os.replace(temporary_path, destination)
            downloaded_paths.append(str(destination))
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to download NYC TLC file from {url}: {exc}") from exc
        except OSError as exc:
            raise RuntimeError(f"Failed to write NYC TLC file to {destination}: {exc}") from exc
        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)

    return downloaded_paths
