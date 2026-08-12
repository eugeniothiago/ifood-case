"""Reusable Delta Lake maintenance operations for the Gold table."""

from time import perf_counter
from typing import Callable, Final, Sequence

from pyspark.sql import DataFrame, SparkSession

DEFAULT_RETENTION_HOURS: Final[int] = 168


def _require_table_name(table_name: str) -> None:
    """Reject an empty table identifier before building a SQL statement."""
    if not table_name.strip():
        raise ValueError("table_name cannot be empty")


def optimize_table(spark: SparkSession, table_name: str) -> None:
    """Compact small Delta files with ``OPTIMIZE``.

    Partitioned writes and incremental loads can leave many small files inside
    each daily partition. ``OPTIMIZE`` combines those files into larger ones,
    reducing file-open and task-scheduling overhead for readers. It is a
    maintenance operation, not a logical data transformation, so it is kept
    separate from the Gold model and can be scheduled after new loads.
    """
    _require_table_name(table_name)
    spark.sql(f"OPTIMIZE {table_name}")


def zorder_table(
    spark: SparkSession, table_name: str, columns: list[str]
) -> None:
    """Compact and cluster Delta data with ``ZORDER BY`` the supplied columns.

    Z-ordering interleaves column values so records with similar values tend to
    share files. Delta can then use file-level statistics to skip files for
    predicates on those columns. Gold uses ``VendorID`` because a vendor filter
    can skip files that do not contain that vendor, even though the column has
    only two or three values. ``tpep_pickup_datetime`` is intentionally not
    z-ordered: Gold is already partitioned by its derived ``pickup_date`` and
    clustering the same time dimension would be redundant for the case queries.

    The function accepts a list so the same maintenance primitive can support
    future, carefully justified multi-column clustering policies.
    """
    _require_table_name(table_name)
    if not columns or any(not column.strip() for column in columns):
        raise ValueError("columns must contain at least one non-empty column")

    zorder_columns = ", ".join(columns)
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_columns})")


def vacuum_table(
    spark: SparkSession,
    table_name: str,
    retain_hours: int = DEFAULT_RETENTION_HOURS,
) -> None:
    """Remove obsolete Delta files with ``VACUUM`` after safe retention.

    Delta's transaction log stops referencing old files after overwrites and
    optimizations, but those files remain in storage for time travel until
    vacuumed. Removing them controls storage cost. The default seven-day
    (168-hour) retention is the standard safe minimum used here, preserving a
    week of rollback and audit capability. A shorter period is rejected to avoid
    accidentally undermining that recovery window.
    """
    _require_table_name(table_name)
    if retain_hours < DEFAULT_RETENTION_HOURS:
        raise ValueError(
            f"retain_hours must be at least {DEFAULT_RETENTION_HOURS} hours"
        )
    spark.sql(f"VACUUM {table_name} RETAIN {retain_hours} HOURS")


def describe_table_history(spark: SparkSession, table_name: str) -> DataFrame:
    """Return Delta transaction history for audit and time-travel inspection.

    Every Delta write creates a transaction-log version. ``DESCRIBE HISTORY``
    exposes those versions, operation names, and timestamps, demonstrating how
    Delta supports auditability and rollback/time travel independently of the
    current table contents.
    """
    _require_table_name(table_name)
    return spark.sql(f"DESCRIBE HISTORY {table_name}")


def run_all_optimizations(spark: SparkSession, gold_table: str) -> None:
    """Run the Gold maintenance sequence and print per-step elapsed time.

    The order is intentional: compact files first, apply the vendor clustering
    policy during a second OPTIMIZE, then remove files older than the retention
    window, and finally inspect the transaction history created by the writes.
    Progress is printed because these are Databricks actions whose duration is
    useful operational evidence in notebook logs.
    """
    steps: Sequence[tuple[str, Callable[[], None]]] = (
        ("OPTIMIZE", lambda: optimize_table(spark, gold_table)),
        ("ZORDER BY VendorID", lambda: zorder_table(spark, gold_table, ["VendorID"])),
        ("VACUUM", lambda: vacuum_table(spark, gold_table)),
    )

    for step_name, step in steps:
        started_at = perf_counter()
        print(f"Starting {step_name} on {gold_table}...")
        step()
        elapsed_seconds = perf_counter() - started_at
        print(f"Completed {step_name} in {elapsed_seconds:.2f}s")

    started_at = perf_counter()
    print(f"Starting DESCRIBE HISTORY on {gold_table}...")
    history_df = describe_table_history(spark, gold_table)
    elapsed_seconds = perf_counter() - started_at
    print(f"Completed DESCRIBE HISTORY in {elapsed_seconds:.2f}s")
    print(f"Delta history rows available: {history_df.count():,}")
