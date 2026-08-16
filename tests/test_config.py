"""Unit tests for pipeline configuration and source URL construction."""

from dataclasses import FrozenInstanceError

import pytest

from src.config import (
    COMMUNITY_BRONZE_TABLE,
    COMMUNITY_GOLD_TABLE,
    COMMUNITY_SILVER_TABLE,
    DEFAULT_BRONZE_TABLE,
    DEFAULT_GOLD_TABLE,
    DEFAULT_MONTHS,
    PARTITION_COLUMN,
    DEFAULT_SILVER_TABLE,
    DEFAULT_YEAR,
    PipelineConfig,
    taxi_file_url,
)


def test_taxi_file_url_january_2023() -> None:
    """The January 2023 URL must use the official CloudFront path."""
    assert taxi_file_url(2023, 1) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )


def test_taxi_file_url_december_2023() -> None:
    """December URLs must include the two-digit month suffix."""
    assert taxi_file_url(2023, 12).endswith("yellow_tripdata_2023-12.parquet")


def test_taxi_file_url_single_digit_month() -> None:
    """Single-digit months must be zero-padded."""
    assert taxi_file_url(2023, 5).endswith("yellow_tripdata_2023-05.parquet")


@pytest.mark.parametrize("year", [1999, 0, -1])
def test_taxi_file_url_rejects_year_before_2000(year: int) -> None:
    """Years before 2000 must be rejected."""
    with pytest.raises(ValueError):
        taxi_file_url(year, 1)


def test_taxi_file_url_rejects_month_zero() -> None:
    """Month zero must be rejected."""
    with pytest.raises(ValueError):
        taxi_file_url(2023, 0)


def test_taxi_file_url_rejects_month_thirteen() -> None:
    """Month thirteen must be rejected."""
    with pytest.raises(ValueError):
        taxi_file_url(2023, 13)


def test_taxi_file_url_rejects_negative_month() -> None:
    """Negative months must be rejected."""
    with pytest.raises(ValueError):
        taxi_file_url(2023, -1)


def test_pipeline_config_default_table_names() -> None:
    """Default configuration must target the three-level Unity Catalog tables."""
    config = PipelineConfig()
    assert config.bronze_table == DEFAULT_BRONZE_TABLE
    assert config.silver_table == DEFAULT_SILVER_TABLE
    assert config.gold_table == DEFAULT_GOLD_TABLE


def test_pipeline_config_default_year() -> None:
    """The default case year must be 2023."""
    assert PipelineConfig().year == DEFAULT_YEAR == 2023


def test_pipeline_config_default_months() -> None:
    """The default case months must cover January through May."""
    assert PipelineConfig().months == DEFAULT_MONTHS == (1, 2, 3, 4, 5)


def test_pipeline_config_default_partition_column() -> None:
    """Gold must use the daily pickup_date partition column by default."""
    assert PipelineConfig().partition_column == PARTITION_COLUMN == "pickup_date"


def test_pipeline_config_rejects_year_before_2000() -> None:
    """Pipeline configurations must reject unsupported years."""
    with pytest.raises(ValueError):
        PipelineConfig(year=1999)


def test_pipeline_config_rejects_empty_months() -> None:
    """At least one source month is required."""
    with pytest.raises(ValueError):
        PipelineConfig(months=())


def test_pipeline_config_rejects_invalid_month() -> None:
    """Pipeline configurations must validate every configured month."""
    with pytest.raises(ValueError):
        PipelineConfig(months=(13,))


def test_pipeline_config_rejects_invalid_partition_column() -> None:
    """Only the contract partition column is accepted."""
    with pytest.raises(ValueError):
        PipelineConfig(partition_column="foo")


def test_community_edition_uses_two_level_names() -> None:
    """Community Edition must use its two-level fallback table names."""
    config = PipelineConfig.community_edition()
    assert config.bronze_table == COMMUNITY_BRONZE_TABLE
    assert config.silver_table == COMMUNITY_SILVER_TABLE
    assert config.gold_table == COMMUNITY_GOLD_TABLE


def test_community_edition_accepts_custom_landing_path() -> None:
    """Community Edition must preserve a caller-provided landing path."""
    assert PipelineConfig.community_edition(landing_path="/tmp/taxi").landing_path == "/tmp/taxi"


def test_community_edition_accepts_custom_months() -> None:
    """Community Edition must preserve configured month order."""
    assert PipelineConfig.community_edition(months=[2, 4]).months == (2, 4)


def test_pipeline_config_is_frozen() -> None:
    """Configuration attributes must not be mutable after construction."""
    config = PipelineConfig()
    with pytest.raises(FrozenInstanceError):
        config.year = 2024  # type: ignore[misc]


def test_pipeline_config_accepts_custom_values() -> None:
    """Custom table names, year, and months must be retained."""
    config = PipelineConfig(
        landing_path="/tmp/landing",
        bronze_table="b",
        silver_table="s",
        gold_table="g",
        year=2024,
        months=(6, 7),
    )
    assert (config.landing_path, config.bronze_table, config.silver_table, config.gold_table) == (
        "/tmp/landing",
        "b",
        "s",
        "g",
    )
    assert config.year == 2024
    assert config.months == (6, 7)
