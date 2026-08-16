"""Unit tests for the explicit source and consumption schemas."""

from pyspark.sql.types import DateType, DoubleType

from src.schemas import CONSUMPTION_SCHEMA, GOLD_SCHEMA, RAW_SCHEMA, REQUIRED_COLUMNS


def test_raw_schema_has_19_columns() -> None:
    """RAW_SCHEMA must preserve all 19 NYC TLC source columns."""
    assert len(RAW_SCHEMA.fields) == 19


def test_consumption_schema_has_5_required_columns() -> None:
    """Consumption output contains exactly the five case-required fields."""
    assert CONSUMPTION_SCHEMA.names == list(REQUIRED_COLUMNS)
    assert len(CONSUMPTION_SCHEMA.fields) == 5


def test_gold_schema_includes_pickup_date() -> None:
    """Gold adds a native DateType daily partition column."""
    assert "pickup_date" in GOLD_SCHEMA.names
    assert isinstance(GOLD_SCHEMA["pickup_date"].dataType, DateType)


def test_required_columns_match_consumption_schema() -> None:
    """The tuple contract and StructType field names must remain aligned."""
    assert tuple(CONSUMPTION_SCHEMA.names) == REQUIRED_COLUMNS


def test_raw_schema_field_names_in_order() -> None:
    """RAW_SCHEMA field names must preserve the source column order."""
    assert RAW_SCHEMA.names == [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
    ]


def test_raw_schema_key_field_types() -> None:
    """Key source columns must use the explicit contract data types."""
    from pyspark.sql.types import IntegerType, StringType, TimestampType

    assert isinstance(RAW_SCHEMA["VendorID"].dataType, IntegerType)
    assert isinstance(RAW_SCHEMA["tpep_pickup_datetime"].dataType, TimestampType)
    assert isinstance(RAW_SCHEMA["tpep_dropoff_datetime"].dataType, TimestampType)
    assert isinstance(RAW_SCHEMA["passenger_count"].dataType, DoubleType)
    assert isinstance(RAW_SCHEMA["total_amount"].dataType, DoubleType)
    assert isinstance(RAW_SCHEMA["store_and_fwd_flag"].dataType, StringType)


def test_raw_schema_all_fields_nullable() -> None:
    """All raw source fields remain nullable at ingestion."""
    assert all(field.nullable for field in RAW_SCHEMA.fields)


def test_consumption_schema_vendor_id_is_long() -> None:
    """Silver normalizes VendorID to Spark bigint/LongType."""
    from pyspark.sql.types import LongType

    assert isinstance(CONSUMPTION_SCHEMA["VendorID"].dataType, LongType)


def test_consumption_schema_all_fields_non_nullable() -> None:
    """All consumption fields must be non-nullable after quality filtering."""
    assert all(not field.nullable for field in CONSUMPTION_SCHEMA.fields)


def test_gold_schema_has_6_fields() -> None:
    """Gold contains the five consumption fields plus pickup_date."""
    assert len(GOLD_SCHEMA.fields) == 6


def test_gold_schema_pickup_date_is_last() -> None:
    """The daily partition field must be the final Gold field."""
    assert GOLD_SCHEMA.fields[-1].name == "pickup_date"


def test_gold_schema_all_fields_non_nullable() -> None:
    """Every Gold field must be published as non-nullable."""
    assert all(not field.nullable for field in GOLD_SCHEMA.fields)


def test_required_columns_are_subset_of_raw_schema() -> None:
    """Every consumption requirement must exist in the raw source schema."""
    assert set(REQUIRED_COLUMNS).issubset(set(RAW_SCHEMA.names))
