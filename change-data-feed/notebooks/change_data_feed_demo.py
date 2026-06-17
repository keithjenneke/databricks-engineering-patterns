# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Change Data Feed (CDF) Demo
# MAGIC
# MAGIC Change Data Feed (CDF) records **row-level changes** (inserts, updates, deletes) made to a Delta table. When enabled, you can query the change history to see exactly what changed between versions.
# MAGIC
# MAGIC **Use cases:**
# MAGIC - **Incremental ETL pipelines** — process only what changed instead of full-table scans
# MAGIC - **Audit logging and compliance** — track who changed what and when
# MAGIC - **Replicating changes to downstream systems** — push deltas to warehouses, search indexes, or APIs
# MAGIC - **Building slowly changing dimensions (SCD)** — use pre/post images for Type 2 history
# MAGIC
# MAGIC > **Companion notebook:** See [CDF Failover Scenarios](#notebook-11247235496004) for handling broken feeds, checkpoint corruption, and recovery patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — Create a Delta Table with CDF Enabled
# MAGIC
# MAGIC You enable CDF by setting `delta.enableChangeDataFeed = true` as a table property.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - CDF adds storage overhead — Delta writes additional `_change_data/` files for UPDATE and DELETE operations. INSERT-only tables have minimal extra cost since the data files themselves serve as the change record.
# MAGIC - You cannot retroactively read changes from *before* CDF was enabled. Plan ahead.
# MAGIC - CDF is compatible with all Delta features (Z-ORDER, OPTIMIZE, partitioning) but VACUUM will remove old change data files too — set retention accordingly.

# COMMAND ----------

# DBTITLE 1,Create table with CDF enabled
# Create a demo catalog/schema namespace (adjust as needed)
catalog = "your_catalog"
schema = "your_schema"
table_name = f"{catalog}.{schema}.cdf_demo_customers"

# ---------------------------------------------------------------------------
# Checkpoint Volume — create a dedicated volume for streaming checkpoints.
# This makes checkpoints discoverable in Catalog Explorer, governed by UC
# permissions, and persistent across clusters. Both this notebook and the
# companion CDF Failover Scenarios notebook use this volume.
# ---------------------------------------------------------------------------
volume_name = "checkpoints"
checkpoint_base = f"/Volumes/{catalog}/{schema}/{volume_name}"

spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
  COMMENT 'Streaming checkpoints for CDF pipelines'
""")

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

spark.sql(f"""
CREATE TABLE {table_name} (
  customer_id INT,
  name STRING,
  email STRING,
  city STRING
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print(f"✅ Table '{table_name}' created with Change Data Feed enabled.")
print(f"✅ Volume '{catalog}.{schema}.{volume_name}' ready for checkpoints.")
print(f"   Checkpoint base path: {checkpoint_base}/")

# COMMAND ----------

# MAGIC %md
# MAGIC > **💡 Tip:** In production, prefer setting CDF at the schema level so all new tables inherit it automatically. You can also check if CDF is enabled on any table with:
# MAGIC > ```sql
# MAGIC > SHOW TBLPROPERTIES my_table ('delta.enableChangeDataFeed');
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC > **💡 Tip:** Use `DESCRIBE HISTORY table_name` to see exactly which versions correspond to which operations. This is essential for debugging CDF reads and understanding version numbering:
# MAGIC > ```sql
# MAGIC > DESCRIBE HISTORY {catalog}.{schema}.cdf_demo_customers;
# MAGIC > ```
# MAGIC > Each DML operation (INSERT, UPDATE, DELETE) creates a new version. OPTIMIZE and VACUUM also create versions but produce no CDF records.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > **💡 SQL alternative:** You can also query changes using the `table_changes()` SQL function:
# MAGIC > ```sql
# MAGIC > SELECT * FROM table_changes('my_catalog.my_schema.my_table', 1)
# MAGIC > ORDER BY _commit_version;
# MAGIC > ```
# MAGIC > Both approaches return the same data — choose based on whether you're working in SQL or DataFrame API.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > **💡 Pattern — Detecting which columns changed:**
# MAGIC > Compare `update_preimage` and `update_postimage` for the same `customer_id` and `_commit_version` to identify exactly which fields were modified. This is useful for partial replication (only sync changed columns downstream).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Initial Data (Version 1)
# MAGIC
# MAGIC **Use case — Initial load for replication:**
# MAGIC When replicating data to a downstream system, the initial INSERT creates version 1. Downstream consumers can start reading the CDF from this version to receive the full history, or from a later version for incremental-only processing.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - The first version of a table is version 0 (the CREATE TABLE), but inserts typically land in version 1+. Always verify your starting version with `DESCRIBE HISTORY`.
# MAGIC - Bulk `INSERT INTO ... SELECT` from a large table still produces a single CDF version — consumers will see a large batch of `insert` change types.

# COMMAND ----------

# DBTITLE 1,Insert initial rows
spark.sql(f"""
INSERT INTO {table_name} VALUES
  (1, 'Alice Johnson', 'alice@example.com', 'Sydney'),
  (2, 'Bob Smith', 'bob@example.com', 'Melbourne'),
  (3, 'Charlie Lee', 'charlie@example.com', 'Brisbane'),
  (4, 'Diana Nguyen', 'diana@example.com', 'Perth')
""")

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Make Changes — UPDATE and DELETE (Version 2 & 3)
# MAGIC
# MAGIC **Use case — Incremental ETL:**
# MAGIC Downstream pipelines only need to process the rows that changed rather than re-scanning the entire table. For a 1B row table where 1000 rows change daily, CDF lets you process just those 1000 rows.
# MAGIC
# MAGIC **Use case — Audit & compliance:**
# MAGIC CDF captures the *before* (`update_preimage`) and *after* (`update_postimage`) state of each row, giving you a full audit trail of who changed what.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - Each UPDATE generates **two** CDF records per row: `update_preimage` + `update_postimage`. A bulk update of 10K rows = 20K CDF records.
# MAGIC - DELETE generates one `delete` record per row (the row's final state before deletion).
# MAGIC - If you UPDATE a row multiple times across different commits, each commit creates its own pre/post image pair.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Apply updates
# UPDATE: Alice moves to Melbourne, Bob gets a new email
spark.sql(f"""
UPDATE {table_name}
SET city = 'Melbourne'
WHERE customer_id = 1
""")

spark.sql(f"""
UPDATE {table_name}
SET email = 'bob.smith@newmail.com'
WHERE customer_id = 2
""")

print("✅ Updates applied (Alice's city, Bob's email).")

# COMMAND ----------

# DBTITLE 1,Apply delete
# DELETE: Remove Charlie
spark.sql(f"""
DELETE FROM {table_name}
WHERE customer_id = 3
""")

print("✅ Delete applied (Charlie removed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read the Change Data Feed
# MAGIC
# MAGIC Use `table_changes()` (SQL) or `readChangeFeed` option (DataFrame API) to query all recorded changes. The CDF adds metadata columns:
# MAGIC
# MAGIC | Column | Description |
# MAGIC | --- | --- |
# MAGIC | `_change_type` | `insert`, `update_preimage`, `update_postimage`, or `delete` |
# MAGIC | `_commit_version` | Delta table version of the change |
# MAGIC | `_commit_timestamp` | Timestamp of the commit |
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - `_commit_version` is the Delta table version, NOT a sequential counter of changes. Versions can jump if OPTIMIZE or other maintenance operations occur between data changes.
# MAGIC - CDF reads are eventually consistent — there's a brief window after a commit where the change data may not yet be visible to readers.
# MAGIC - Reading CDF does NOT lock the table. Concurrent writes can continue while you read.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read all changes from CDF
# Read ALL changes from version 1 onward
changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table(table_name)

display(changes_df.orderBy("_commit_version", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Filter Changes by Type
# MAGIC
# MAGIC You can filter to see only specific change types — useful for incremental processing.
# MAGIC
# MAGIC **Use case — SCD Type 2 dimensions:**
# MAGIC Filter for `update_preimage` to close existing dimension records (set `end_date`), then use `update_postimage` to insert the new current record.
# MAGIC
# MAGIC **Use case — Soft-delete propagation:**
# MAGIC Filter for `delete` change types to propagate deletions to downstream systems that don't support hard deletes (e.g., append a tombstone record to a Kafka topic).
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - If you only process `update_postimage` without the `update_preimage`, you lose the ability to detect *which* columns actually changed — you'd need to compare against the target yourself.
# MAGIC - `insert` change types appear for both brand-new rows AND rows reintroduced via `INSERT` (not MERGE). Don't assume `insert` = "never seen before".
# MAGIC

# COMMAND ----------

# DBTITLE 1,Filter updates only
from pyspark.sql.functions import col

# Show only updates (pre and post image)
updates_df = changes_df.filter(
    col("_change_type").isin("update_preimage", "update_postimage")
)

print("=== Updates (before & after) ===")
display(updates_df.orderBy("customer_id", "_change_type"))

# COMMAND ----------

# DBTITLE 1,Filter deletes only
# Show only deletes
deletes_df = changes_df.filter(col("_change_type") == "delete")

print("=== Deleted rows ===")
display(deletes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Read Changes Between Specific Versions
# MAGIC
# MAGIC For incremental pipelines, you typically read only changes since your last checkpoint.
# MAGIC
# MAGIC **Use case — Micro-batch ETL:**
# MAGIC Store the last-processed `_commit_version` in a control table. On each run, read from `lastVersion + 1` to current, process, then update the watermark. This gives you exactly-once semantics if your write is idempotent.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - If `startingVersion` references a version removed by VACUUM, you'll get a `VersionNotFoundException`. Always keep your processing lag shorter than your VACUUM retention (default 7 days).
# MAGIC - `endingVersion` is *inclusive* — both start and end versions are included in the result.
# MAGIC - If no changes occurred between your start and end versions (e.g., only OPTIMIZE ran), you'll get an empty DataFrame, not an error.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read changes for version range
# Read changes between version 2 and 3 only
incremental_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 2) \
    .option("endingVersion", 3) \
    .table(table_name)

print("=== Changes between version 2 and 3 ===")
display(incremental_df.orderBy("_commit_version", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Read Changes by Timestamp
# MAGIC
# MAGIC You can also specify a time range instead of versions.
# MAGIC
# MAGIC **Use case — Time-based SLAs:**
# MAGIC When your downstream system requires "all changes from the last 4 hours", timestamp-based reads are more intuitive than tracking version numbers.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - Timestamps are resolved to the *nearest commit at or after* the given time. If no commit exists at that exact time, Delta finds the next one.
# MAGIC - `startingTimestamp` and `endingTimestamp` use the **commit timestamp** (UTC stored in the Delta log), not the time the data event occurred. If your writer has clock skew, timestamps may be surprising.
# MAGIC - You **cannot mix** version and timestamp options in the same read (e.g., `startingVersion` + `endingTimestamp` is not supported).
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read changes by timestamp
from datetime import datetime, timedelta

# Read changes from the last hour
start_time = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

timestamp_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", start_time) \
    .table(table_name)

print(f"=== Changes since {start_time} ===")
display(timestamp_df.orderBy("_commit_version", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Streaming CDF — Process Changes in Real Time
# MAGIC
# MAGIC CDF also works with Structured Streaming for continuous incremental processing.
# MAGIC
# MAGIC **Use case — Real-time dashboard refresh:**
# MAGIC A streaming job reads CDF from your fact table and maintains aggregated metrics in a downstream table, keeping dashboards up-to-date within seconds of source changes.
# MAGIC
# MAGIC **Use case — Event-driven microservices:**
# MAGIC Stream CDF changes to Kafka/Event Hub so downstream services react to data changes without polling the source table.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - Streaming CDF reads use checkpoints internally. If you VACUUM aggressively and the stream falls behind, it will fail with a version-not-found error. Set `spark.databricks.delta.retentionDurationCheck.enabled = true` (the default) to prevent this.
# MAGIC - `display(stream_df)` in a notebook is for development only — in production, write to a sink table with a proper `checkpointLocation`.
# MAGIC - Schema evolution on the source table will cause the stream to fail. You'll need to restart with `mergeSchema` or reset the checkpoint.
# MAGIC - The stream processes ALL change types. Filter before writing to your sink if you only need a subset.
# MAGIC
# MAGIC **📁 Checkpoint best practice — Use Unity Catalog Volumes:**
# MAGIC Store checkpoints in a UC Volume (`/Volumes/<catalog>/<schema>/<volume>/...`) rather than `/tmp/` or DBFS root. This makes checkpoints discoverable in Catalog Explorer, governed by UC permissions, and persistent across clusters. See the [CDF Failover Scenarios](#notebook-11247235496004) notebook for a full example.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Production pattern: Use the UC Volume created in the setup cell.
# Checkpoints stored here are browsable in Catalog Explorer, governed, and
# persistent across clusters/serverless sessions.
#
# For a production streaming job you'd write to a sink table:
#
#   stream_checkpoint = f"{checkpoint_base}/cdf_demo_stream"
#
#   query = (
#       spark.readStream.format("delta")
#       .option("readChangeFeed", "true")
#       .option("startingVersion", 1)
#       .table(table_name)
#       .writeStream
#       .format("delta")
#       .option("checkpointLocation", stream_checkpoint)
#       .toTable(f"{catalog}.{schema}.cdf_demo_sink")
#   )
# ---------------------------------------------------------------------------

# Development-mode streaming read (display only, no persistent checkpoint)
stream_df = spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 1) \
    .table(table_name)

# Display the stream (in a notebook this creates a live-updating view)
display(stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Enable CDF on an Existing Table
# MAGIC
# MAGIC You can also enable CDF on a table that already exists (changes are only recorded from that point forward).
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE my_catalog.my_schema.my_table
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC
# MAGIC **Use case — Enabling on production tables:**
# MAGIC You have an existing fact table and want to build incremental ETL downstream. Enable CDF, then take a one-time full snapshot as your baseline. Future changes are captured incrementally.
# MAGIC
# MAGIC **⚠️ Watch out:**
# MAGIC - CDF is **not retroactive** — you will NOT see changes that occurred before enabling. Your consumer needs a full snapshot baseline.
# MAGIC - Enabling CDF does NOT create a new table version by itself (the ALTER TABLE does create a version, but with no change data).
# MAGIC - To enable CDF for **all new tables** in a schema by default, consider setting it at the catalog or schema level:
# MAGIC   ```sql
# MAGIC   ALTER SCHEMA my_catalog.my_schema
# MAGIC   SET DBPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC   ```
# MAGIC - Disabling CDF later (`UNSET TBLPROPERTIES`) will break any downstream consumers immediately — they'll get an error on next read.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup
# MAGIC
# MAGIC **⚠️ Watch out before cleaning up in production:**
# MAGIC - Dropping a CDF-enabled table removes ALL change history permanently.
# MAGIC - If downstream consumers haven't processed all changes yet, they'll lose data.
# MAGIC - Consider disabling CDF first and giving consumers time to catch up before dropping.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cleanup
# Stop any active streams
for stream in spark.streams.active:
    stream.stop()

# Uncomment to drop the demo table:
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# print(f"🧹 Table '{table_name}' dropped.")

print("✅ Streams stopped. Uncomment the DROP statement to remove the demo table.")