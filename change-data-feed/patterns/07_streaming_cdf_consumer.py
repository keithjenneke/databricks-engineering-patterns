"""
Pattern 7: Correct CDF streaming consumer setup
=================================================
Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns
Repo: https://github.com/keithjenneke/databricks-patterns/tree/main/change-data-feed

This file is the PREVENTION pattern — the correct way to set up a CDF streaming
consumer from the start, before anything goes wrong.

Getting this right eliminates the conditions that cause Patterns 3 (checkpoint
corruption) and partially 1 (VACUUM lag). Every CDF streaming pipeline you build
should follow this structure.

KEY DECISIONS EXPLAINED
------------------------

1. Checkpoint in a Unity Catalog Volume (/Volumes/...)
   Not /tmp/, not DBFS, not cluster-local storage.
   UC Volumes are persistent across cluster restarts and serverless sessions.
   See: 03_checkpoint_corruption_recovery.py for what happens when you get this wrong.

2. trigger(availableNow=True) on Databricks Serverless
   Serverless compute does not support continuous streaming (ProcessingTime trigger).
   availableNow=True processes all available data then terminates cleanly.
   For scheduled micro-batch processing this is the correct trigger on Serverless.
   For always-on streaming on dedicated clusters, use trigger(processingTime="N minutes").

3. Versioned checkpoint paths (stream_v1, stream_v2)
   Use a version suffix on every checkpoint path. When you need to restart from
   a new position (recovery), you create stream_v2 rather than deleting stream_v1.
   This preserves the history of your stream's lifecycle for debugging.

4. Store _commit_version in the sink table
   Every sink write includes _commit_version from the CDF metadata.
   This is required for Pattern 3 recovery and the Pattern 5 version guard.
   Without it, you cannot safely recover from a checkpoint loss.

5. startingVersion set explicitly
   Never rely on the default (latest). Always set startingVersion to your
   known baseline — the version at which CDF was enabled or last processed.

PREREQUISITES
-------------
Run 00_setup.py first to create the UC Volume and confirm CDF is enabled.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# CONFIG — match values from 00_setup.py
# ---------------------------------------------------------------------------

catalog      = "dbw_ae_dev_ca_01"
schema       = "demo"
source_table = f"{catalog}.{schema}.cdf_demo_customers"
sink_table   = f"{catalog}.{schema}.cdf_demo_customers_sync"

# Versioned checkpoint path in Unity Catalog Volume
checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/cdf_pipelines/stream_v1"

# The version CDF was enabled — set to the baseline version printed by 00_setup.py.
# Must be >= 1: version 0 is the CREATE TABLE, CDF is enabled at version 1 via
# ALTER TABLE, so no _change_data files exist for version 0.
starting_version = 1


# ---------------------------------------------------------------------------
# STEP 1: Create the sink table
#
# The sink table must include last_commit_version — this is required for:
#   - Pattern 3: recovery from checkpoint corruption
#   - Pattern 5: version guard in the idempotent MERGE
#
# Set delta.enableChangeDataFeed = false on the sink — consumers should
# not generate CDF on a table that is itself a CDF sink.
# ---------------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {sink_table} (
        id                  BIGINT      NOT NULL,
        name                STRING,
        email               STRING,
        city                STRING,
        status              STRING,
        updated_at          TIMESTAMP,
        _change_type        STRING,
        _commit_version     BIGINT      NOT NULL,   -- required for recovery + version guard
        _commit_timestamp   TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'false'
    )
""")

print(f"✅ Sink table ready: {sink_table}")
print(f"   Checkpoint path  : {checkpoint_path}")
print(f"   Starting version : {starting_version}")


# ---------------------------------------------------------------------------
# STEP 2: Define the streaming CDF consumer
#
# This is the correct production pattern. Each option is deliberate — see
# the KEY DECISIONS section at the top of this file for the reasoning.
# ---------------------------------------------------------------------------
def run_cdf_stream(
    source_table: str,
    sink_table: str,
    checkpoint_path: str,
    starting_version: int,
) -> None:
    """
    Run a CDF streaming consumer with the correct production configuration.

    Reads changes from source_table starting at starting_version and appends
    them to sink_table. Checkpoint is stored in a Unity Catalog Volume.

    On Databricks Serverless, trigger(availableNow=True) processes all
    available data then terminates. Schedule this function via a Databricks
    Job to achieve micro-batch processing at your desired frequency.

    Args:
        source_table:     Fully qualified source Delta table (CDF must be enabled)
        sink_table:       Fully qualified sink table (must include _commit_version)
        checkpoint_path:  Path in a Unity Catalog Volume — /Volumes/catalog/schema/vol/path
        starting_version: The version to start reading CDF from (inclusive)
    """
    print(f"Starting CDF stream...")
    print(f"  Source          : {source_table}")
    print(f"  Sink            : {sink_table}")
    print(f"  Checkpoint      : {checkpoint_path}")
    print(f"  Starting version: {starting_version}")

    query = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)          # explicit — never default
        .table(source_table)

        # Project only the columns the sink expects
        # Adjust to match your source table schema
        .select(
            F.col("id"),
            F.col("name"),
            F.col("email"),
            F.col("city"),
            F.col("status"),
            F.col("updated_at"),
            F.col("_change_type"),
            F.col("_commit_version"),
            F.col("_commit_timestamp"),
        )

        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)        # UC Volume — never /tmp/
        .trigger(availableNow=True)                           # Serverless-safe trigger
        .toTable(sink_table)
    )

    # Wait for the micro-batch to complete before returning
    query.awaitTermination()

    # Report results
    sink_count = spark.table(sink_table).count()
    last_version = (
        spark.sql(f"SELECT MAX(_commit_version) as v FROM {sink_table}")
        .collect()[0]["v"]
    )
    print(f"\n✅ Stream complete.")
    print(f"   Sink row count       : {sink_count}")
    print(f"   Last commit version  : {last_version}")
    print(f"   Checkpoint stored at : {checkpoint_path}")


# ---------------------------------------------------------------------------
# STEP 3: Run the stream
# ---------------------------------------------------------------------------
run_cdf_stream(
    source_table=source_table,
    sink_table=sink_table,
    checkpoint_path=checkpoint_path,
    starting_version=starting_version,
)


# ---------------------------------------------------------------------------
# HOW TO SCHEDULE THIS AS A DATABRICKS JOB
# ---------------------------------------------------------------------------
# 1. Create a new Job in Databricks (Workflows → Jobs → Create Job)
# 2. Set the task to run this script (or the notebook equivalent)
# 3. Set the schedule — e.g. every 15 minutes for near-real-time CDF processing
# 4. Each run:
#      - trigger(availableNow=True) picks up all changes since the last run
#      - The checkpoint tracks position so no changes are missed or duplicated
#      - If a run fails, the next run resumes from the checkpoint position
#
# KEY INSIGHT: The combination of availableNow=True + UC Volume checkpoint
# gives you micro-batch CDF processing that is:
#   - Safe to schedule (idempotent via checkpoint)
#   - Safe to run on Serverless (terminates cleanly)
#   - Safe to recover from failure (checkpoint persists across restarts)
#   - Observable (checkpoint browsable in Catalog Explorer)
#
# For always-on streaming on a dedicated cluster, replace:
#   .trigger(availableNow=True)
# with:
#   .trigger(processingTime="5 minutes")
# and remove query.awaitTermination() from the function.
# ---------------------------------------------------------------------------
