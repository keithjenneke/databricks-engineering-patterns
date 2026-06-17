"""
Pattern 1: VACUUM removes change history
=========================================
Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns

PROBLEM
-------
VACUUM removes _change_data/ files that back CDF reads. If your pipeline falls
behind by more than the VACUUM retention window (default 7 days), those versions
are permanently unavailable.

Error: VersionNotFoundException: Cannot find version X of table

RECOVERY
--------
Read the current table as a full snapshot, use it as a new baseline, and reset
your watermark to the current version + 1. Future CDF reads start from there.

PREVENTION
----------
- Keep pipeline lag shorter than your VACUUM retention window
- Set retention explicitly (do not rely on the 7-day default)
- Alert when lag exceeds 80% of the retention window
- Monitor the gap between last_processed_version and current table version
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def recover_from_vacuum(source_table: str, sink_table: str) -> int:
    """
    Recover a CDF pipeline after VACUUM has removed required history.

    Reads the current table as a full snapshot, writes it to the sink,
    and returns the new watermark version to use for future CDF reads.

    Args:
        source_table: Fully qualified Delta table name (catalog.schema.table)
        sink_table:   Fully qualified sink table name

    Returns:
        new_watermark: The version to use as startingVersion on next CDF read
    """
    # Step 1: Get the current table version
    current_version = (
        spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 1")
        .collect()[0]["version"]
    )
    print(f"Current table version: {current_version}")

    # Step 2: Read the full snapshot at the current version
    full_snapshot = spark.read.format("delta").table(source_table)
    print(f"Full snapshot row count: {full_snapshot.count()}")

    # Step 3: Overwrite the sink with the full snapshot
    # Adjust the write mode and options for your sink table structure
    (
        full_snapshot.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(sink_table)
    )
    print(f"Sink table overwritten with full snapshot")

    # Step 4: Return the new watermark
    # Store this value — future CDF reads should use startingVersion = new_watermark
    new_watermark = current_version
    print(f"New watermark: {new_watermark}")
    print(f"Next CDF read: startingVersion = {new_watermark + 1}")

    return new_watermark


# --- Example usage ---
# new_watermark = recover_from_vacuum(
#     source_table="catalog.schema.source_table",
#     sink_table="catalog.schema.sink_table"
# )
# Save new_watermark to your pipeline state store
# e.g. spark.sql(f"UPDATE pipeline_state SET last_version = {new_watermark} WHERE table = 'source_table'")
