"""
Pattern 2: Schema evolution breaks downstream consumers
========================================================
Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns

PROBLEM
-------
An upstream ALTER TABLE ADD COLUMNS causes your CDF consumer to encounter columns
it was not built to handle. Depending on the implementation, this either throws a
schema mismatch error or silently drops new columns your downstream system needed.

Error: AnalysisException: cannot resolve column / silent data loss

RECOVERY
--------
Build defensive schema projection into your CDF reader. Select only the columns
your consumer knows about, fill missing expected columns with NULL, and log any
new upstream columns you are ignoring.

PREVENTION
----------
- Always project explicitly — never use SELECT * on a CDF read in production
- Log new upstream columns so schema changes are visible without breaking the pipeline
- Monitor source table schema changes using Unity Catalog system tables
- Coordinate with upstream teams before ALTER TABLE operations on CDF-enabled tables
"""

from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# CDF metadata columns — always present on a readChangeFeed result
CDF_METADATA_COLS = ["_change_type", "_commit_version", "_commit_timestamp"]


def safe_cdf_read(
    table: str,
    start_version: int,
    expected_columns: List[str],
    end_version: int = None,
) -> DataFrame:
    """
    Read CDF changes with defensive schema projection.

    Selects only columns the consumer knows about, fills any missing expected
    columns with NULL, and logs new upstream columns rather than failing.

    Args:
        table:            Fully qualified Delta table name
        start_version:    CDF startingVersion (inclusive)
        expected_columns: List of column names the consumer expects
        end_version:      Optional CDF endingVersion (inclusive). Reads to latest if None.

    Returns:
        DataFrame with expected_columns + CDF metadata columns
    """
    reader = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
    )
    if end_version is not None:
        reader = reader.option("endingVersion", end_version)

    cdf_df = reader.table(table)

    # Identify available, missing, and new columns
    available_cols = [c for c in expected_columns if c in cdf_df.columns]
    missing_cols   = [c for c in expected_columns if c not in cdf_df.columns]
    new_upstream   = [
        c for c in cdf_df.columns
        if c not in expected_columns and c not in CDF_METADATA_COLS
    ]

    # Log schema differences
    if missing_cols:
        print(f"[WARN] Expected columns not found in source (will be NULL): {missing_cols}")
    if new_upstream:
        print(f"[INFO] New upstream columns detected (ignored by consumer): {new_upstream}")

    # Build the projected DataFrame
    select_exprs = (
        [F.col(c) for c in available_cols]
        + [F.lit(None).cast("string").alias(c) for c in missing_cols]
        + [F.col(c) for c in CDF_METADATA_COLS]
    )

    return cdf_df.select(select_exprs)


# --- Example usage ---
# expected = ["customer_id", "name", "email", "status", "created_at"]
#
# changes = safe_cdf_read(
#     table="catalog.schema.customers",
#     start_version=last_processed_version + 1,
#     expected_columns=expected
# )
# changes.filter("_change_type = 'update_postimage'").show()
