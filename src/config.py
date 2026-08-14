"""Configuration primitives for the NYC Taxi medallion pipeline."""

from dataclasses import dataclass, field
from typing import Final, Iterable

# Databricks Community Edition does not support POSIX mkdir on /dbfs/ FUSE
# mounts (raises OSError [Errno 95] Operation not supported).  The local
# /tmp directory on the driver node is fully POSIX-compatible and Spark can
# read files from it via file:// URIs.  This is the standard landing zone
# location for Community Edition notebooks.
DEFAULT_LANDING_PATH: Final[str] = "/tmp/nyc_taxi/landing"
DEFAULT_BRONZE_TABLE: Final[str] = "nyc_taxi.bronze.yellow_tripdata"
DEFAULT_SILVER_TABLE: Final[str] = "nyc_taxi.silver.yellow_tripdata"
DEFAULT_GOLD_TABLE: Final[str] = "nyc_taxi.gold.yellow_tripdata"
COMMUNITY_BRONZE_TABLE: Final[str] = "bronze.yellow_tripdata"
COMMUNITY_SILVER_TABLE: Final[str] = "silver.yellow_tripdata"
COMMUNITY_GOLD_TABLE: Final[str] = "gold.yellow_tripdata"
DEFAULT_YEAR: Final[int] = 2023
DEFAULT_MONTHS: Final[tuple[int, ...]] = (1, 2, 3, 4, 5)
PARTITION_COLUMN: Final[str] = "pickup_date"
SOURCE_URL_TEMPLATE: Final[str] = (
    "https://d37ci6v3ury3vh.cloudfront.net/trip-data/"
    "yellow_tripdata_{year:04d}-{month:02d}.parquet"
)


def taxi_file_url(year: int, month: int) -> str:
    """Return the official NYC TLC CloudFront URL for one monthly file."""
    if year < 2000:
        raise ValueError("year must be a four-digit calendar year")
    if month not in range(1, 13):
        raise ValueError("month must be between 1 and 12")
    return SOURCE_URL_TEMPLATE.format(year=year, month=month)


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable runtime configuration shared by notebooks and source modules.

    The default table names target Unity Catalog. ``community_edition`` provides
    two-level Hive Metastore names for Databricks Community Edition, which does
    not provide Unity Catalog in the same way as a full workspace.
    """

    landing_path: str = DEFAULT_LANDING_PATH
    bronze_table: str = DEFAULT_BRONZE_TABLE
    silver_table: str = DEFAULT_SILVER_TABLE
    gold_table: str = DEFAULT_GOLD_TABLE
    year: int = DEFAULT_YEAR
    months: tuple[int, ...] = field(default_factory=lambda: DEFAULT_MONTHS)
    partition_column: str = PARTITION_COLUMN

    def __post_init__(self) -> None:
        """Validate configuration values once at construction time."""
        if self.year < 2000:
            raise ValueError("year must be a four-digit calendar year")
        if not self.months:
            raise ValueError("months must contain at least one month")
        if any(month not in range(1, 13) for month in self.months):
            raise ValueError("months must contain values between 1 and 12")
        if self.partition_column != PARTITION_COLUMN:
            raise ValueError(f"partition_column must be {PARTITION_COLUMN!r}")

    @classmethod
    def community_edition(
        cls,
        landing_path: str = DEFAULT_LANDING_PATH,
        year: int = DEFAULT_YEAR,
        months: Iterable[int] = DEFAULT_MONTHS,
    ) -> "PipelineConfig":
        """Build configuration using two-level Hive Metastore table names."""
        return cls(
            landing_path=landing_path,
            bronze_table=COMMUNITY_BRONZE_TABLE,
            silver_table=COMMUNITY_SILVER_TABLE,
            gold_table=COMMUNITY_GOLD_TABLE,
            year=year,
            months=tuple(months),
        )
