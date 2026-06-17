"""
Pattern 3: Streaming checkpoint corruption
===========================================
Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns

PROBLEM
-------
Structured Streaming checkpoints track exactly where your stream has processed up to.
If the checkpoint directory is corrupted, deleted, or inaccessible, your stream cannot
resume. It starts fresh — either reprocessing everything or skipping to the latest version.

A less obvious version: checkpoints stored in /tmp/ or cluster-local paths are
ephemeral on serverless compute. The stream appears to work in development, then
loses its checkpoint on every cluster restart in production.

Error: StreamingQueryException: Unable to load checkpoint / stream restarts from scratch

RECOVERY
--------
Query the sink table for the highest _commit_version successfully processed.
Use that as startingVersion for a recovery stream with a new checkpoint path.
This avoids both data loss and duplicate processing.

PREVENTION
----------
- Always store checkpoints in Unity Catalog Volumes (/Volumes/catalog/schema/volume/)
- Never use /tmp/, dbfs:/tmp/, or cluster-local paths for production checkpoints
- Use versioned checkpoint paths (stream_v1, stream_v2) so history is traceable
- Unity Catalog Volumes are persistent across cluster restarts and serverless sessions
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def recover_streaming_checkpoint(
    source_table: str,
    sink_table: str,
    recovery_checkpoint_path: str,
    commit_version_col: str = "_commit_version",
) -> None:
    """
    Recover a CDF streaming pipeline after checkpoint corruption or loss.

    Queries the sink table for the last successfully processed _commit_version,
    then restarts the stream from that point with a new checkpoint path.

    Args:
        source_table:             Fully qualified source Delta table
        sink_table:               Fully qualified sink table (must store _commit_version)
        recovery_checkpoint_path: New checkpoint path in a Unity Catalog Volume
                                  e.g. /Volumes/catalog/schema/checkpoints/stream_v2_recovery
        commit_version_col:       Column name storing _commit_version in the sink table
    """
    # Step 1: Find the last successfully processed version from the sink
    last_processed = (
        spark.sql(f"""
            SELECT COALESCE(MAX({commit_version_col}), 0) AS last_version
            FROM {sink_table}
        """)
        .collect()[0]["last_version"]
    )
    print(f"Last successfully processed _commit_version: {last_processed}")
    print(f"Recovery stream will start from version: {last_processed + 1}")
    print(f"New checkpoint path: {recovery_checkpoint_path}")

    # Step 2: Start recovery stream from last_processed + 1 with a new checkpoint
    recovery_query = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last_processed + 1)
        .table(source_table)
        .writeStream
        .option("checkpointLocation", recovery_checkpoint_path)
        .trigger(availableNow=True)   # Process all available data then stop
        .toTable(sink_table)
    )

    # Wait for the recovery batch to complete
    recovery_query.awaitTermination()
    print(f"Recovery complete. Checkpoint stored at: {recovery_checkpoint_path}")
    print("Update your pipeline configuration to use the new checkpoint path going forward.")


# --- Example usage ---
# recover_streaming_checkpoint(
#     source_table="catalog.schema.source_table",
#     sink_table="catalog.schema.sink_table",
#     recovery_checkpoint_path="/Volumes/catalog/schema/checkpoints/stream_v2_recovery"
# )

# --- Production checkpoint path pattern (use this going forward) ---
# Good:  /Volumes/catalog/schema/checkpoints/stream_v1
# Good:  /Volumes/catalog/schema/checkpoints/stream_v2_recovery
# Bad:   /tmp/checkpoints/my_stream          (ephemeral on serverless)
# Bad:   dbfs:/tmp/checkpoints/my_stream     (not Unity Catalog governed)
