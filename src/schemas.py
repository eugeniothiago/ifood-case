"""Explicit schemas for the raw TLC source and consumption layers."""

from typing import Final

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_SCHEMA: Final[StructType] = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ]
)

REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)

CONSUMPTION_SCHEMA: Final[StructType] = StructType(
    [
        # Silver normalizes VendorID to LongType for a stable downstream contract.
        StructField("VendorID", LongType(), False),
        *[
            StructField(column, RAW_SCHEMA[column].dataType, False)
            for column in REQUIRED_COLUMNS
            if column != "VendorID"
        ],
    ]
)

GOLD_SCHEMA: Final[StructType] = StructType(
    [*CONSUMPTION_SCHEMA.fields, StructField("pickup_date", DateType(), False)]
)
