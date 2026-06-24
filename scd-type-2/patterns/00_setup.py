"""
00: Setup & sample data generator
==================================
Blog: SCD Type 2 on Databricks: Why APPLY CHANGES INTO Replaced 200 Lines of MERGE Logic
URL:  https://www.cypheragency.com.au/resources/scd-type-2-databricks-apply-changes-into
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/scd-type-2

Run this first. It creates the bronze source table used by every other pattern
file in this folder, and seeds it with deliberately awkward data — same-batch
duplicate changes, an out-of-order correction, and a sequence-column tie —
so the gotchas in the article are reproducible rather than theoretical.

Run this as a regular notebook cell or script. No Lakeflow pipeline required
for this file.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType
)
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# CONFIG — replace these values for your environment
# ---------------------------------------------------------------------------

catalog = "your_catalog"
schema = "your_schema"

bronze_table = f"{catalog}.{schema}.customer_updates_raw"
target_table = f"{catalog}.{schema}.customer_dim"          # manual MERGE target
target_table_v2 = f"{catalog}.{schema}.customer_dim_v2"     # apply_changes target


# ---------------------------------------------------------------------------
# Schema for the synthetic CDC feed
#
# Mirrors what a real CDC source (Debezium, Qlik Replicate, Delta CDF) would
# hand you: a natural key, attribute columns, an operation type, and a
# sequencing timestamp plus a monotonic commit version.
# ---------------------------------------------------------------------------

cdc_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("operation", StringType(), False),     # INSERT | UPDATE | DELETE
    StructField("updated_at", TimestampType(), False),  # business sequence column
    StructField("_commit_version", LongType(), False),  # monotonic tie-breaker
])


def build_sample_rows():
    """
    Constructs the synthetic change feed used throughout this folder's demos.

    Deliberately includes:
      - customer 1001: a clean insert then a clean update (the easy case)
      - customer 1002: TWO updates landing in the same notebook "batch" with
        identical updated_at timestamps — this is the sequence-tie scenario
      - customer 1003: an update that arrives out of order relative to an
        earlier correction — this is the out-of-order scenario the manual
        MERGE in pattern 01 does not handle correctly
      - customer 1004: an insert followed by a delete
    """
    base = datetime(2026, 1, 1, 9, 0, 0)

    rows = [
        # Customer 1001 — clean case
        (1001, "Alice Chen",   "SMB",      "INSERT", base,                              1),
        (1001, "Alice Chen",   "Mid-Market", "UPDATE", base + timedelta(hours=1),        2),

        # Customer 1002 — SAME-BATCH DUPLICATE with an identical timestamp.
        # Both rows carry the same updated_at on purpose — this is the tie
        # that pattern 02 (basic apply_changes) does not resolve deterministically,
        # and that pattern 03 fixes with a composite sequence_by.
        (1002, "Brendan Wu",   "SMB",      "INSERT", base,                              3),
        (1002, "Brendan Wu",   "Enterprise", "UPDATE", base + timedelta(hours=2),        4),
        (1002, "Brendan Wu",   "Mid-Market", "UPDATE", base + timedelta(hours=2),        5),  # tie on updated_at with the row above

        # Customer 1003 — OUT OF ORDER. The correction (commit_version 7) carries
        # an EARLIER business timestamp than the row that lands before it
        # (commit_version 6) — simulating a late-arriving correction.
        (1003, "Carla Singh",  "SMB",      "INSERT", base,                              6),
        (1003, "Carla Singh",  "Mid-Market", "UPDATE", base + timedelta(hours=3),        8),
        (1003, "Carla Singh",  "SMB",      "UPDATE", base + timedelta(hours=1, minutes=30), 7),  # arrives after the row above, but is earlier in business time

        # Customer 1004 — insert then delete
        (1004, "Dev Patel",    "SMB",      "INSERT", base,                              9),
        (1004, "Dev Patel",    "SMB",      "DELETE", base + timedelta(hours=4),         10),
    ]
    return rows


def setup():
    """Create the catalog/schema if needed and write the seeded bronze table."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    rows = build_sample_rows()
    df = spark.createDataFrame(rows, schema=cdc_schema)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_table)
    )

    print(f"✅ Bronze table ready: {bronze_table}")
    print(f"   Row count: {df.count()}")
    print(f"\nScenarios seeded:")
    print(f"   customer_id 1001 — clean insert + update")
    print(f"   customer_id 1002 — same-batch update with a SEQUENCE TIE")
    print(f"   customer_id 1003 — OUT-OF-ORDER correction (late-arriving, earlier business time)")
    print(f"   customer_id 1004 — insert then delete")


if __name__ == "__main__":
    setup()
