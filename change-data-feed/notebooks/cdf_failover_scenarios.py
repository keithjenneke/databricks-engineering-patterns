# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CDF Failover Scenarios — Handling Broken Feeds
# MAGIC
# MAGIC This notebook builds on the [Change Data Feed Demo](#notebook-11247235496002) and demonstrates common failure scenarios when consuming Change Data Feeds, along with recovery patterns.
# MAGIC
# MAGIC **Scenarios covered:**
# MAGIC 1. History lost due to VACUUM (version no longer available)
# MAGIC 2. Schema evolution breaking downstream consumers
# MAGIC 3. Streaming checkpoint corruption & recovery
# MAGIC 4. CDF disabled mid-pipeline (feed gap)
# MAGIC 5. Late-arriving data and out-of-order processing
# MAGIC 6. Idempotent replay — safely reprocessing changes
# MAGIC
# MAGIC > **Prerequisite:** Run the Change Data Feed Demo notebook first to create `{catalog}.{schema}.cdf_demo_customers`.

# COMMAND ----------

# DBTITLE 1,Setup and shared config
# Shared config — same table from the CDF Demo notebook
catalog = "your_catalog"
schema = "your_schema"
source_table = f"{catalog}.{schema}.cdf_demo_customers"
failover_table = f"{catalog}.{schema}.cdf_failover_demo"
sink_table = f"{catalog}.{schema}.cdf_failover_sink"

# ---------------------------------------------------------------------------
# Checkpoint paths — using the UC Volume created in the Change Data Feed Demo.
# The volume is created there as part of the prerequisite setup.
# We use CREATE VOLUME IF NOT EXISTS here as a safety guard in case this
# notebook is run standalone.
# Format: /Volumes/<catalog>/<schema>/<volume>/<path>
# ---------------------------------------------------------------------------
volume_name = "checkpoints"
checkpoint_base = f"/Volumes/{catalog}/{schema}/{volume_name}/cdf_failover"
checkpoint_path = f"{checkpoint_base}/stream_v1"

# Safety guard — create volume if the Demo notebook hasn't been run first
spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
  COMMENT 'Streaming checkpoints for CDF pipelines'
""")

# Clean up any previous failover demo artifacts
spark.sql(f"DROP TABLE IF EXISTS {failover_table}")
spark.sql(f"DROP TABLE IF EXISTS {sink_table}")
dbutils.fs.rm(checkpoint_path, recurse=True)

print(f"✅ Config ready.")
print(f"   Source table : {source_table}")
print(f"   Checkpoints  : {checkpoint_base}/")
print(f"   Volume       : {catalog}.{schema}.{volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Why use a Volume for checkpoints?**
# MAGIC >
# MAGIC > | Approach | Discoverability | Governance | Persistence | Cross-cluster |
# MAGIC > | --- | --- | --- | --- | --- |
# MAGIC > | `/tmp/` (DBFS root) | ❌ Hidden | ❌ No ACLs | ❌ Cluster-local on serverless | ❌ Ephemeral |
# MAGIC > | DBFS mount / `dbfs:/...` | ⚠️ Requires DBFS browser | ❌ Legacy | ✅ Persists | ✅ Yes |
# MAGIC > | **UC Volume** `/Volumes/...` | ✅ Catalog Explorer | ✅ UC permissions | ✅ Persists | ✅ Yes |
# MAGIC >
# MAGIC > **Best practice:** Store all streaming checkpoints under a dedicated Volume (e.g., `checkpoints`) so you can browse, audit, and clean them up from a single governed location.
# MAGIC >
# MAGIC > **Naming convention:** `<checkpoint_base>/<pipeline_name>/<stream_name>` makes it easy to identify which pipeline owns which checkpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1: History Lost Due to VACUUM
# MAGIC
# MAGIC **Problem:** `VACUUM` removes old Delta log files and data files. If a CDF consumer hasn't processed changes before VACUUM runs, those versions become unavailable.
# MAGIC
# MAGIC **Error you'll see:**
# MAGIC ```
# MAGIC VersionNotFoundException: Cannot find version X of table...
# MAGIC ```
# MAGIC
# MAGIC **Recovery:** Fall back to a full table snapshot and reset your checkpoint.

# COMMAND ----------

# DBTITLE 1,Generate table versions
# Create a table and generate several versions with UPDATES
# UPDATEs rewrite data files, making old files eligible for VACUUM removal.
# Pure INSERTs won't work because all added files remain referenced by the current snapshot.
spark.sql(f"""
CREATE TABLE {failover_table} (
  id INT, value STRING
) TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Version 1: bulk insert
spark.sql(f"""
  INSERT INTO {failover_table}
  SELECT id, CONCAT('v1_', id) FROM RANGE(100)
""")

# Versions 2-5: UPDATE all rows (each rewrites the data files, orphaning the previous ones)
for i in range(2, 6):
    spark.sql(f"UPDATE {failover_table} SET value = CONCAT('v{i}_', id)")

print(f"✅ Created {failover_table} with 5 versions (1 insert + 4 full rewrites).")
print(f"   Old data files from versions 1-4 are now orphaned and eligible for VACUUM.")

# Show current table history
display(spark.sql(f"DESCRIBE HISTORY {failover_table}").select("version", "timestamp", "operation"))

# COMMAND ----------

# DBTITLE 1,Simulate aggressive VACUUM
# Simulate VACUUM removing old versions
# (Using retention of 0 hours to force removal — never do this in production!)
#
# On Serverless compute, spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled")
# is not available. Instead, set the TABLE-LEVEL retention property to 0, which allows
# VACUUM RETAIN 0 HOURS to pass the safety check.
spark.sql(f"""
  ALTER TABLE {failover_table}
  SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 seconds')
""")
spark.sql(f"VACUUM {failover_table} RETAIN 0 HOURS")

# Reset to a safe retention for the rest of the demo
spark.sql(f"""
  ALTER TABLE {failover_table}
  SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days')
""")

print("⚠️ VACUUM complete — old data files removed.")

# COMMAND ----------

# DBTITLE 1,Demonstrate version not found error
# Try to read CDF from version 1 — this SHOULD fail because VACUUM removed
# the old data files that backed the change data for early versions.
try:
    df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 1) \
        .table(failover_table)
    df.count()  # Force evaluation
    print("⚠️ Unexpectedly succeeded — Azure blob soft-delete may be retaining files.")
    print("   In production with real retention, this WILL fail.")
except Exception as e:
    print(f"❌ EXPECTED ERROR: {type(e).__name__}")
    print(f"   {str(e)[:300]}")
    print("\n→ Recovery: Fall back to full snapshot (see next cell).")

# COMMAND ----------

# DBTITLE 1,Recovery: Full snapshot fallback
# RECOVERY: Full snapshot fallback
# When CDF history is lost, take a full snapshot and reset your watermark

current_version = spark.sql(f"DESCRIBE HISTORY {failover_table} LIMIT 1").collect()[0]["version"]

# Read the current state as a full load
full_snapshot = spark.table(failover_table)
print(f"✅ RECOVERY: Full snapshot taken at version {current_version}")
print(f"   Rows recovered: {full_snapshot.count()}")
print(f"   Future CDF reads should start from version {current_version + 1}")
display(full_snapshot)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2: Schema Evolution Breaking Consumers
# MAGIC
# MAGIC **Problem:** An upstream `ALTER TABLE ADD COLUMNS` or column type change causes downstream CDF consumers to fail with schema mismatch errors.
# MAGIC
# MAGIC **Recovery:** Use `mergeSchema` option or implement defensive schema handling.

# COMMAND ----------

# DBTITLE 1,Evolve the schema
# Add a new column to the failover table (schema evolution)
spark.sql(f"ALTER TABLE {failover_table} ADD COLUMNS (priority STRING)")
spark.sql(f"INSERT INTO {failover_table} VALUES (6, 'v6', 'high')")

print("✅ Schema evolved — added 'priority' column and inserted a row.")

# COMMAND ----------

# DBTITLE 1,Detect schema mismatch
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Simulate a consumer with a RIGID expected schema (old schema)
old_schema = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType()),
])

# Reading CDF after schema change — demonstrate the mismatch
try:
    cdf_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", current_version) \
        .table(failover_table)
    
    # The new 'priority' column appears — consumer must handle it
    print("Current CDF schema:")
    cdf_df.printSchema()
    print(f"\n⚠️ Consumer expecting {len(old_schema.fields)} columns but got {len(cdf_df.columns) - 3} data columns (+ 3 CDF metadata).")
except Exception as e:
    print(f"❌ ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Recovery: Defensive schema handling
# RECOVERY: Defensive schema handling
from pyspark.sql.functions import col, lit

def safe_cdf_read(table, start_version, expected_columns):
    """Read CDF and project only expected columns, filling missing ones with NULL."""
    cdf_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .table(table)
    
    # Keep only columns the consumer knows + CDF metadata
    cdf_metadata = ["_change_type", "_commit_version", "_commit_timestamp"]
    available_cols = [c for c in expected_columns if c in cdf_df.columns]
    missing_cols = [c for c in expected_columns if c not in cdf_df.columns]
    
    result = cdf_df.select(*available_cols, *cdf_metadata)
    
    # Add NULLs for any columns that existed in old schema but were dropped
    for mc in missing_cols:
        result = result.withColumn(mc, lit(None).cast(StringType()))
    
    if missing_cols:
        print(f"⚠️ Missing columns filled with NULL: {missing_cols}")
    
    # Log new columns the consumer doesn't know about (for alerting)
    new_cols = [c for c in cdf_df.columns if c not in expected_columns and c not in cdf_metadata]
    if new_cols:
        print(f"ℹ️ New upstream columns detected (ignored): {new_cols}")
    
    return result

# Use the safe reader
safe_df = safe_cdf_read(failover_table, current_version, ["id", "value"])
print("\n✅ RECOVERY: Schema-safe read succeeded.")
display(safe_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3: Streaming Checkpoint Corruption & Recovery
# MAGIC
# MAGIC **Problem:** A streaming CDF consumer's checkpoint directory gets corrupted or accidentally deleted. The stream cannot resume from where it left off.
# MAGIC
# MAGIC **Recovery:** Reset checkpoint and use version tracking to avoid reprocessing.

# COMMAND ----------

# DBTITLE 1,Start streaming consumer
# Create a sink table for our streaming consumer
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {sink_table} (
  id INT, value STRING, priority STRING,
  _change_type STRING, _commit_version LONG
) TBLPROPERTIES (delta.enableChangeDataFeed = false)
""")

# Start a streaming CDF consumer with a checkpoint stored in our Volume
# NOTE: Serverless compute does not support infinite streaming (ProcessingTime trigger).
#       Use trigger(availableNow=True) to process all available data then stop.
#
# IMPORTANT: We start from `current_version` (set in cell 8) because earlier
# versions had their CDF files removed by VACUUM in Scenario 1.
print(f"Checkpoint location: {checkpoint_path}")
print(f"Starting from version: {current_version} (post-VACUUM safe point)")

query = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", current_version)
    .table(failover_table)
    .select("id", "value", "priority", "_change_type", "_commit_version")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(sink_table)
)

# availableNow processes all data then terminates — wait for it to finish
query.awaitTermination()

sink_count = spark.table(sink_table).count()
print(f"\n✅ Stream processed {sink_count} change records into sink table.")
print(f"   Checkpoint stored at: {checkpoint_path}")
print(f"   (Browse in Catalog Explorer under volume '{volume_name}')")

# COMMAND ----------

# DBTITLE 1,Corrupt the checkpoint
# Simulate checkpoint corruption — delete the checkpoint
dbutils.fs.rm(checkpoint_path, recurse=True)
print(f"💥 Checkpoint deleted from Volume! Simulating corruption...")
print(f"   Path was: {checkpoint_path}")

# Verify it's gone (you could also check Catalog Explorer)
try:
    dbutils.fs.ls(checkpoint_path)
    print("   ⚠️ Files still present (cached?)")
except:
    print("   ✅ Confirmed: checkpoint directory no longer exists.")

# Attempting to restart the stream will start from scratch
# This risks DUPLICATE processing

# COMMAND ----------

# DBTITLE 1,Recovery: Restart from last processed version
# RECOVERY: Determine safe restart version from the sink table
# Find the last version we successfully processed
last_processed = spark.sql(f"""
  SELECT COALESCE(MAX(_commit_version), 0) as last_version 
  FROM {sink_table}
""").collect()[0]["last_version"]

print(f"✅ RECOVERY: Last processed version in sink = {last_processed}")

# Simulate new data arriving while the stream was down (realistic scenario)
# In production, upstream writers continue regardless of your consumer's health.
spark.sql(f"INSERT INTO {failover_table} VALUES (200, 'arrived_while_down', 'critical')")
spark.sql(f"UPDATE {failover_table} SET value = 'updated_while_down' WHERE id = 200")
print(f"   New data arrived while stream was down (versions {last_processed + 1}+).")
print(f"   Restarting stream from version {last_processed + 1} to avoid duplicates.")

# Use a NEW checkpoint path within the same Volume (versioned naming)
# This makes it clear in Catalog Explorer which checkpoint is current
new_checkpoint = f"{checkpoint_base}/stream_v2_recovery"
dbutils.fs.rm(new_checkpoint, recurse=True)

print(f"   New checkpoint: {new_checkpoint}")

recovery_query = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_processed + 1)
    .table(failover_table)
    .select("id", "value", "priority", "_change_type", "_commit_version")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", new_checkpoint)
    .trigger(availableNow=True)
    .toTable(sink_table)
)

recovery_query.awaitTermination()

new_sink_count = spark.table(sink_table).count()
print(f"\n✅ Recovery stream completed. No duplicates introduced.")
print(f"   Sink now has {new_sink_count} total records.")
print(f"   Checkpoint persisted at: {new_checkpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4: CDF Disabled Mid-Pipeline
# MAGIC
# MAGIC **Problem:** Someone accidentally disables CDF on the source table. Downstream consumers that rely on `readChangeFeed` get an error.
# MAGIC
# MAGIC **Error:**
# MAGIC ```
# MAGIC DeltaIllegalStateException: Change data feed is not enabled on table...
# MAGIC ```
# MAGIC
# MAGIC **Recovery:** Re-enable CDF (changes during the disabled window are lost), detect the gap, and backfill.

# COMMAND ----------

# DBTITLE 1,Disable CDF and make changes
# Save the current version before disabling
pre_disable_version = spark.sql(f"DESCRIBE HISTORY {failover_table} LIMIT 1").collect()[0]["version"]

# Disable CDF
spark.sql(f"ALTER TABLE {failover_table} UNSET TBLPROPERTIES ('delta.enableChangeDataFeed')")
print(f"⚠️ CDF DISABLED at version {pre_disable_version}")

# Make changes while CDF is off (these won't be captured)
spark.sql(f"INSERT INTO {failover_table} VALUES (7, 'v7_no_cdf', 'low')")
spark.sql(f"UPDATE {failover_table} SET value = 'v1_updated' WHERE id = 1")
print("   Changes made while CDF was off — these are INVISIBLE to CDF consumers.")

# COMMAND ----------

# DBTITLE 1,Attempt CDF read while disabled
# Try to read CDF — this will fail
try:
    broken_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", pre_disable_version + 1) \
        .table(failover_table)
    broken_df.count()
except Exception as e:
    print(f"❌ EXPECTED ERROR: {type(e).__name__}")
    print(f"   {str(e)[:200]}")

# COMMAND ----------

# DBTITLE 1,Recovery: Re-enable and detect gap
# RECOVERY: Re-enable CDF and detect the gap
spark.sql(f"ALTER TABLE {failover_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

post_enable_version = spark.sql(f"DESCRIBE HISTORY {failover_table} LIMIT 1").collect()[0]["version"]
print(f"✅ CDF re-enabled at version {post_enable_version}")
print(f"⚠️ GAP DETECTED: versions {pre_disable_version + 1} to {post_enable_version} have NO CDF data.")
print(f"")
print("Recovery options:")
print("  1. Full snapshot diff: Compare last-known state with current state")
print("  2. Use DESCRIBE HISTORY to identify operations during the gap")
print("  3. Accept data loss for the gap window and document it")

# Option 1: Show what changed using history
print("\n--- Operations during the CDF gap ---")
display(
    spark.sql(f"""
        SELECT version, timestamp, operation, operationParameters 
        FROM (DESCRIBE HISTORY {failover_table})
        WHERE version > {pre_disable_version} AND version <= {post_enable_version}
        ORDER BY version
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5: Late-Arriving & Out-of-Order Data
# MAGIC
# MAGIC **Problem:** Multiple writers commit concurrently. A CDF consumer processing version N may see logically "earlier" data arrive in version N+2 due to write conflicts and retries.
# MAGIC
# MAGIC **Recovery:** Use `_commit_version` ordering and implement idempotent merge logic.

# COMMAND ----------

# DBTITLE 1,Demonstrate out-of-order events
# Simulate out-of-order business events arriving in later versions
# AND demonstrate what happens when batches are replayed out of order.

# Ensure CDF is enabled (may have been disabled by Scenario 4)
spark.sql(f"ALTER TABLE {failover_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Get the current version as our starting point (normally set by Scenario 4)
post_enable_version = spark.sql(f"DESCRIBE HISTORY {failover_table} LIMIT 1").collect()[0]["version"]

# Step 1: Create events for the SAME id across multiple versions
spark.sql(f"INSERT INTO {failover_table} VALUES (8, 'original_event', 'medium')")
spark.sql(f"UPDATE {failover_table} SET value = 'corrected_event' WHERE id = 8")
spark.sql(f"INSERT INTO {failover_table} VALUES (8, 'late_arriving_event', 'high')")

print("✅ Events created across 3 versions.")

# Read ALL changes for id=8
all_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", post_enable_version + 1) \
    .table(failover_table) \
    .filter("id = 8")

print("\nFull CDF (commit order):")
display(all_changes.orderBy("_commit_version"))

# Step 2: Split into batches to simulate out-of-order delivery
# Batch FIRST: correction (highest version) arrives first
# Batch LATE: original insert (lowest version) arrives late
from pyspark.sql.functions import col

versions = all_changes.select("_commit_version").distinct().orderBy("_commit_version").collect()
min_version = versions[0]["_commit_version"]
max_version = versions[-1]["_commit_version"]

batch_late = all_changes.filter(col("_commit_version") == min_version)   # original insert (arrives LATE)
batch_first = all_changes.filter(col("_commit_version") == max_version)  # correction (arrives FIRST)

print(f"\n--- Batch FIRST (arrives first, higher version {max_version}): ---")
display(batch_first.orderBy("_commit_version"))

print(f"\n--- Batch LATE (arrives second, lower version {min_version}): ---")
display(batch_late.orderBy("_commit_version"))

# COMMAND ----------

# DBTITLE 1,Recovery: Idempotent MERGE pattern
# RECOVERY: Idempotent MERGE pattern for safe replay
# This handles duplicates and out-of-order arrivals gracefully
#
# KEY INSIGHT: The guard `source._commit_version > target.last_commit_version`
# ensures that a stale (lower version) change CANNOT overwrite a newer one.
# This is what makes it safe to replay batches in any order.

idempotent_sink = f"{catalog}.{schema}.cdf_idempotent_sink"
spark.sql(f"DROP TABLE IF EXISTS {idempotent_sink}")
spark.sql(f"""
CREATE TABLE {idempotent_sink} (
  id INT, value STRING, priority STRING,
  last_commit_version LONG
)
""")

def idempotent_apply_changes(changes_df, target_table):
    """Apply CDF changes idempotently using MERGE — safe for replay."""
    changes_df.createOrReplaceTempView("incoming_changes")
    
    # For each id, take the latest change (highest commit version)
    # Apply inserts/updates via MERGE, deletes via matched-delete
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING (
            SELECT id, value, priority, _change_type, _commit_version
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id ORDER BY _commit_version DESC
                ) as rn
                FROM incoming_changes
                WHERE _change_type IN ('insert', 'update_postimage', 'delete')
            )
            WHERE rn = 1
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
        WHEN MATCHED AND source._commit_version > target.last_commit_version THEN
            UPDATE SET 
                target.value = source.value,
                target.priority = source.priority,
                target.last_commit_version = source._commit_version
        WHEN NOT MATCHED AND source._change_type != 'delete' THEN
            INSERT (id, value, priority, last_commit_version)
            VALUES (source.id, source.value, source.priority, source._commit_version)
    """)

# --- SIMULATE OUT-OF-ORDER DELIVERY ---
# Process batch_first (higher versions) FIRST
print("Step 1: Processing BATCH FIRST (higher versions arrive first)...")
idempotent_apply_changes(batch_first, idempotent_sink)
print("   Sink after Batch FIRST:")
display(spark.table(idempotent_sink))

# Now process batch_late (lower version) SECOND — this is the late arrival
print("\nStep 2: Processing BATCH LATE (lower version arrives second)...")
idempotent_apply_changes(batch_late, idempotent_sink)
print("   Sink after Batch LATE:")
display(spark.table(idempotent_sink).filter("id = 8"))

print("\n✅ RESULT: id=8 shows the latest event (highest commit version) due to version guard.")
print("   The late-arriving lower version did NOT overwrite the newer correction.")

# COMMAND ----------

# DBTITLE 1,Prove idempotent replay
# Prove idempotency — replay ALL changes again (duplicates of everything)
print("Step 3: Replaying ALL changes (full redelivery)...")
idempotent_apply_changes(all_changes, idempotent_sink)
replay_count = spark.table(idempotent_sink).count()
print(f"✅ After full replay: {replay_count} rows (no duplicates, no regressions).")
display(spark.table(idempotent_sink))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6: Monitoring & Alerting for CDF Health
# MAGIC
# MAGIC **Problem:** CDF failures happen silently. You need proactive detection.
# MAGIC
# MAGIC **Recovery:** Build a health-check function that validates CDF availability before processing.

# COMMAND ----------

# DBTITLE 1,CDF health check function
def check_cdf_health(table_name, last_processed_version):
    """
    Health check for CDF availability.
    Returns (healthy: bool, message: str, recommended_action: str)
    """
    issues = []
    
    # Check 1: Is CDF enabled?
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        cdf_enabled = any(
            row["key"] == "delta.enableChangeDataFeed" and row["value"] == "true"
            for row in props
        )
        if not cdf_enabled:
            issues.append("CDF_DISABLED: Change Data Feed is not enabled on this table.")
    except Exception as e:
        return (False, f"TABLE_ERROR: {e}", "Check table existence and permissions.")
    
    # Check 2: Can we read from the last processed version?
    if cdf_enabled:
        try:
            test_df = spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingVersion", last_processed_version + 1) \
                .table(table_name)
            test_df.limit(1).count()  # Trigger execution
        except Exception as e:
            error_msg = str(e)
            if "not been found" in error_msg or "does not exist" in error_msg:
                issues.append(f"VERSION_LOST: Version {last_processed_version + 1} unavailable (likely vacuumed).")
            else:
                issues.append(f"READ_ERROR: {error_msg[:150]}")
    
    # Check 3: Version gap detection
    current_version = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]["version"]
    version_lag = current_version - last_processed_version
    if version_lag > 100:
        issues.append(f"HIGH_LAG: {version_lag} versions behind (current={current_version}).")
    
    if not issues:
        return (True, "All checks passed.", "None — continue normal processing.")
    else:
        actions = []
        if any("CDF_DISABLED" in i for i in issues):
            actions.append("Re-enable CDF and perform full snapshot diff.")
        if any("VERSION_LOST" in i for i in issues):
            actions.append("Full snapshot reload and reset checkpoint.")
        if any("HIGH_LAG" in i for i in issues):
            actions.append("Increase processing frequency or resources.")
        return (False, " | ".join(issues), " ".join(actions))


# Run the health check
healthy, message, action = check_cdf_health(failover_table, post_enable_version)
print(f"Health: {'✅ HEALTHY' if healthy else '⚠️ UNHEALTHY'}")
print(f"Status: {message}")
print(f"Action: {action}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Recovery Patterns
# MAGIC
# MAGIC | Scenario | Detection | Recovery |
# MAGIC | --- | --- | --- |
# MAGIC | VACUUM removes history | `VersionNotFoundException` | Full snapshot + reset watermark |
# MAGIC | Schema evolution | Column count/type mismatch | Defensive projection with NULL fill |
# MAGIC | Checkpoint corruption | Stream fails to start | Query sink for last version, restart from there |
# MAGIC | CDF disabled mid-flight | `DeltaIllegalStateException` | Re-enable + DESCRIBE HISTORY to find gap |
# MAGIC | Out-of-order data | Business logic inconsistency | Idempotent MERGE with version tracking |
# MAGIC | Silent failures | Proactive health checks | Automated monitoring before each batch |
# MAGIC
# MAGIC **Key principles:**
# MAGIC - Always track `_commit_version` in your sink for recovery
# MAGIC - Use idempotent writes (MERGE) so replays are safe
# MAGIC - Monitor CDF health BEFORE processing, not after
# MAGIC - Keep VACUUM retention > your max processing lag

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleanup
# Stop any active streams
for stream in spark.streams.active:
    stream.stop()

# Clean up all checkpoint directories from the Volume
dbutils.fs.rm(checkpoint_base, recurse=True)
print(f"🧹 Removed checkpoint directory: {checkpoint_base}/")

# Uncomment to drop all demo tables:
# spark.sql(f"DROP TABLE IF EXISTS {failover_table}")
# spark.sql(f"DROP TABLE IF EXISTS {sink_table}")
# spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.cdf_idempotent_sink")
# print("🧹 All failover demo tables dropped.")

# Uncomment to drop the checkpoint volume entirely:
# spark.sql(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{volume_name}")
# print(f"🧹 Volume '{volume_name}' dropped.")

print("✅ Streams stopped, checkpoints cleaned. Uncomment DROP statements to remove tables/volume.")