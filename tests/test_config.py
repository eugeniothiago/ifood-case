"""Unit tests for configuration validation and URL generation."""

import pytest
from src.config import (
    PipelineConfig,
    taxi_file_url,
    taxi_file_urls,
    DEFAULT_YEAR,
    DEFAULT_MONTHS,
    COMMUNITY_BRONZE_TABLE,
    COMMUNITY_SILVER_TABLE,
    COMMUNITY_GOLD_TABLE,
    DBFS_LANDING_PATH,
)


def test_taxi_file_url_january_2023() -> None:
    assert taxi_file_url(2023, 1) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2023-01.parquet"
    )


def test_taxi_file_url_december_2023() -> None:
    assert taxi_file_url(2023, 12) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2023-12.parquet"
    )


def test_taxi_file_url_single_digit_month() -> None:
    assert "2023-01" in taxi_file_url(2023, 1)


def test_taxi_file_urls_returns_multiple() -> None:
    """taxi_file_urls should return at least the primary URL."""
    urls = taxi_file_urls(2023, 1)
    assert len(urls) >= 1
    assert "yellow_tripdata_2023-01.parquet" in urls[0]


def test_dbfs_landing_path_is_dbfs() -> None:
    """DBFS landing path must start with dbfs: for Spark readability."""
    assert DBFS_LANDING_PATH.startswith("dbfs:")


@pytest.mark.parametrize("year", [1999, 0, -1])
def test_taxi_file_url_rejects_year_before_2000(year: int) -> None:
    with pytest.raises(ValueError, match="year"):
        taxi_file_url(year, 1)


def test_taxi_file_url_rejects_month_zero() -> None:
    with pytest.raises(ValueError, match="month"):
        taxi_file_url(2023, 0)


def test_taxi_file_url_rejects_month_thirteen() -> None:
    with pytest.raises(ValueError, match="month"):
        taxi_file_url(2023, 13)


def test_taxi_file_url_rejects_negative_month() -> None:
    with pytest.raises(ValueError, match="month"):
        taxi_file_url(2023, -1)


def test_pipeline_config_default_table_names() -> None:
    cfg = PipelineConfig()
    assert cfg.bronze_table == "nyc_taxi.bronze.yellow_tripdata"
    assert cfg.silver_table == "nyc_taxi.silver.yellow_tripdata"
    assert cfg.gold_table == "nyc_taxi.gold.yellow_tripdata"


def test_pipeline_config_default_year() -> None:
    assert PipelineConfig().year == DEFAULT_YEAR


def test_pipeline_config_default_months() -> None:
    assert PipelineConfig().months == DEFAULT_MONTHS


def test_pipeline_config_default_partition_column() -> None:
    assert PipelineConfig().partition_column == "pickup_date"


def test_pipeline_config_rejects_year_before_2000() -> None:
    with pytest.raises(ValueError, match="year"):
        PipelineConfig(year=1999)


def test_pipeline_config_rejects_empty_months() -> None:
    with pytest.raises(ValueError, match="months"):
        PipelineConfig(months=())


def test_pipeline_config_rejects_invalid_month() -> None:
    with pytest.raises(ValueError, match="months"):
        PipelineConfig(months=(0, 1))


def test_pipeline_config_rejects_invalid_partition_column() -> None:
    with pytest.raises(ValueError, match="partition_column"):
        PipelineConfig(partition_column="wrong")


def test_community_edition_uses_two_level_names() -> None:
    cfg = PipelineConfig.community_edition()
    assert cfg.bronze_table == COMMUNITY_BRONZE_TABLE
    assert cfg.silver_table == COMMUNITY_SILVER_TABLE
    assert cfg.gold_table == COMMUNITY_GOLD_TABLE


def test_community_edition_accepts_custom_landing_path() -> None:
    cfg = PipelineConfig.community_edition(landing_path="/custom/path")
    assert cfg.landing_path == "/custom/path"


def test_community_edition_accepts_custom_months() -> None:
    cfg = PipelineConfig.community_edition(months=[3, 4])
    assert cfg.months == (3, 4)


def test_community_edition_schema_properties() -> None:
    """Community edition config should expose bronze/silver/gold schema names."""
    cfg = PipelineConfig.community_edition()
    assert cfg.bronze_schema == "bronze"
    assert cfg.silver_schema == "silver"
    assert cfg.gold_schema == "gold"
    assert set(cfg.all_schemas) == {"bronze", "silver", "gold"}


def test_default_config_schema_properties() -> None:
    """Default (Unity Catalog) config should expose three-level schema names."""
    cfg = PipelineConfig()
    assert cfg.bronze_schema == "nyc_taxi"
    assert cfg.silver_schema == "nyc_taxi"
    assert cfg.gold_schema == "nyc_taxi"
    assert set(cfg.all_schemas) == {"nyc_taxi"}


def test_pipeline_config_is_frozen() -> None:
    cfg = PipelineConfig()
    with pytest.raises(Exception):
        cfg.year = 2024  # type: ignore[misc]


def test_pipeline_config_accepts_custom_values() -> None:
    cfg = PipelineConfig(year=2024, months=(6, 7), landing_path="/data")
    assert cfg.year == 2024
    assert cfg.months == (6, 7)
    assert cfg.landing_path == "/data"
