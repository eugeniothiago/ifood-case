"""Unit tests for idempotent, atomic taxi-file ingestion.

Updated for the multi-strategy download fallback (requests -> wget -> curl).
The subprocess fallbacks are mocked to prevent real network calls.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.ingestion import download_taxi_data


def _response(chunks: list[bytes]) -> MagicMock:
    response = MagicMock()
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    response.iter_content.return_value = iter(chunks)
    return response


def _mock_subprocess_failure(*args, **kwargs):
    """Simulate a failed subprocess.run for wget/curl fallbacks."""
    result = MagicMock()
    result.returncode = 1
    result.stderr = b"command not found"
    return result


def test_download_creates_directory() -> None:
    """The landing directory must be created before downloading."""
    with TemporaryDirectory() as root:
        landing_path = Path(root) / "nested" / "landing"
        with patch("src.ingestion.requests.get", return_value=_response([b"data"])):
            download_taxi_data(2023, [1], str(landing_path))
        assert landing_path.is_dir()


def test_download_skips_existing_file() -> None:
    """A non-empty existing file must not trigger a network request."""
    with TemporaryDirectory() as root:
        landing_path = Path(root)
        destination = landing_path / "yellow_tripdata_2023-01.parquet"
        destination.write_bytes(b"already complete")
        with patch("src.ingestion.requests.get") as get:
            paths = download_taxi_data(2023, [1], str(landing_path))
        get.assert_not_called()
        assert paths == [str(destination)]
        assert destination.read_bytes() == b"already complete"


def test_download_returns_correct_paths() -> None:
    """Returned paths must match the requested local filenames."""
    with TemporaryDirectory() as root:
        with patch("src.ingestion.requests.get", return_value=_response([b"data"])):
            paths = download_taxi_data(2023, [5], root)
        assert paths == [str(Path(root) / "yellow_tripdata_2023-05.parquet")]


def test_download_raises_on_http_error() -> None:
    """HTTP response errors must be surfaced as RuntimeError from all strategies."""
    response = _response([])
    response.raise_for_status.side_effect = requests.HTTPError("server error")
    with TemporaryDirectory() as root, patch(
        "src.ingestion.requests.get", return_value=response
    ), patch("src.ingestion.subprocess.run", side_effect=_mock_subprocess_failure):
        with pytest.raises(RuntimeError, match="All download strategies failed"):
            download_taxi_data(2023, [1], root)


def test_download_raises_on_empty_body() -> None:
    """A successful response with no body must be rejected after all strategies fail."""
    with TemporaryDirectory() as root, patch(
        "src.ingestion.requests.get", return_value=_response([])
    ), patch("src.ingestion.subprocess.run", side_effect=_mock_subprocess_failure):
        with pytest.raises(RuntimeError, match="All download strategies failed"):
            download_taxi_data(2023, [1], root)


def test_download_uses_temp_file_then_replaces() -> None:
    """Downloads must leave final content and no temporary staging files."""
    with TemporaryDirectory() as root:
        with patch("src.ingestion.requests.get", return_value=_response([b"part", b"ial"])):
            paths = download_taxi_data(2023, [1], root)
        destination = Path(paths[0])
        assert destination.read_bytes() == b"partial"
        assert list(Path(root).glob(".*.parquet.*")) == []


def test_download_multiple_months() -> None:
    """Each requested month must produce one path and one final file."""
    responses = [_response([f"month-{month}".encode()]) for month in (1, 2, 3)]
    with TemporaryDirectory() as root, patch(
        "src.ingestion.requests.get", side_effect=responses
    ) as get:
        paths = download_taxi_data(2023, [1, 2, 3], root)
        assert get.call_count == 3
        assert len(paths) == 3
        assert all(Path(path).is_file() for path in paths)


def test_download_fallback_to_wget_succeeds() -> None:
    """If requests fails, wget should be tried as fallback."""
    def _wget_success(*args, **kwargs):
        result = MagicMock()
        result.returncode = 0
        result.stderr = b""
        # Write the file so the size check passes
        cmd_list = args[0]  # subprocess.run receives a list as first positional arg
        destination = Path(cmd_list[3])  # "-O" is cmd_list[2], destination is cmd_list[3]
        destination.write_bytes(b"wget data")
        return result

    with TemporaryDirectory() as root, patch(
        "src.ingestion.requests.get", side_effect=requests.ConnectionError("DNS failed")
    ), patch("src.ingestion.subprocess.run", side_effect=_wget_success):
        paths = download_taxi_data(2023, [1], root)
        assert Path(paths[0]).read_bytes() == b"wget data"


def test_download_all_strategies_fail() -> None:
    """If all three strategies fail, the error lists each failure."""
    with TemporaryDirectory() as root, patch(
        "src.ingestion.requests.get", side_effect=requests.ConnectionError("DNS failed")
    ), patch("src.ingestion.subprocess.run", side_effect=_mock_subprocess_failure):
        with pytest.raises(RuntimeError) as exc_info:
            download_taxi_data(2023, [1], root)
        error_msg = str(exc_info.value)
        assert "requests" in error_msg
        assert "wget" in error_msg
        assert "curl" in error_msg
