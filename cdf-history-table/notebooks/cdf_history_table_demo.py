# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Building a CDF History Table That Outlives Your VACUUM Window
# MAGIC
# MAGIC Companion notebook to the article: [Building a CDF History Table That Outlives Your VACUUM Window](https://www.cypheragency.com.au/resources/cdf-history-table-databricks-vacuum-retention)
# MAGIC
# MAGIC Full pattern files [databricks-engineering-patterns/cdf-history-table](https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/cdf-history-table)
# MAGIC
# MAGIC This notebook demonstrates the core claim of the article directly: a history table built without filtering on `_change_type` captures changes that an SCD2 pipeline — filtering to business attributes only — never sees at all.
# MAGIC
# MAGIC **Important:** `@dp.table` pipeline definitions (patterns 01 and 02) only execute inside the Lakeflow Declarative Pipeline runtime, the same constraint that applies to `apply_changes` in the scd-type-2 folder.
# MAGIC Steps 1 and 2 below run directly in this notebook. Step 3 requires deploying a pipeline, then returning here to verify the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Config

# COMMAND ----------

catalog = "your_catalog"
schema = "your_schema"

source_table = f"{catalog}.{schema}.customer"
history_table = f"{catalog}.{schema}.customer_history"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Seed the source table
# MAGIC
# MAGIC Two customers, with one deliberately exercising the case this whole article is about: a write that touches only a non-business-attribute column.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {source_table}")

spark.sql(f"""
CREATE TABLE {source_table} (
    customer_id   BIGINT,
    name          STRING,
    segment       STRING,
    internal_note STRING
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
INSERT INTO {source_table} VALUES
    (2001, 'Alice Chen', 'SMB', 'created via signup form'),
    (2002, 'Brendan Wu', 'SMB', 'created via signup form')
""")

# A genuine business-attribute change — this WOULD generate a new SCD2 row
spark.sql(f"UPDATE {source_table} SET segment = 'Mid-Market' WHERE customer_id = 2001")

# A write touching ONLY internal_note — an SCD2 pipeline tracking only
# name/segment would never produce a row for this change at all.
spark.sql(f"""
    UPDATE {source_table} SET internal_note = 'flagged for billing review'
    WHERE customer_id = 2002
""")

spark.sql(f"DELETE FROM {source_table} WHERE customer_id = 2002")

display(spark.sql(f"DESCRIBE HISTORY {source_table}").select("version", "operation"))

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC ## Step 2 — Read the raw CDF and show what an SCD2 pipeline would drop
# MAGIC
# MAGIC This cell reads CDF directly (no pipeline required for a plain read) and simulates what an SCD2 pipeline tracking only `name` and `segment` would produce. The simulation pairs pre/post images and only keeps updates where a tracked attribute actually changed — the same logic `apply_changes` applies internally.

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql import functions as F

all_changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table(source_table)
)

print("All CDF events (what a history table captures — every change, both images):")
display(all_changes.orderBy("customer_id", "_commit_version"))

# Simulate what an SCD2 pipeline tracking ONLY name and segment would produce.
# apply_changes compares pre/post images on tracked attributes and only emits a
# new row when at least one tracked attribute actually changed. Updates that
# touch only non-tracked columns (like internal_note) are silently dropped.
pre_images = all_changes.filter("_change_type = 'update_preimage'")
post_images = all_changes.filter("_change_type = 'update_postimage'")

scd2_updates = (
    post_images.alias("post")
    .join(
        pre_images.alias("pre"),
        (F.col("post.customer_id") == F.col("pre.customer_id"))
        & (F.col("post._commit_version") == F.col("pre._commit_version")),
    )
    .filter(
        (F.col("post.name") != F.col("pre.name"))
        | (F.col("post.segment") != F.col("pre.segment"))
    )
    .select("post.*")
)

# SCD2 result: inserts + only attribute-changing updates + deletes
scd2_simulation = (
    all_changes.filter("_change_type IN ('insert', 'delete')")
    .unionByName(scd2_updates)
)

print("SCD2 simulation (only rows where tracked attributes name/segment changed):")
display(scd2_simulation.orderBy("customer_id", "_commit_version"))

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %md
# MAGIC Compare the two results above:
# MAGIC
# MAGIC * The **history table** (unfiltered) captures every CDF event — every insert, update (both pre and post images), and delete. Customer 2002's `internal_note`-only update appears as a pre/post image pair.
# MAGIC * The **SCD2 simulation** drops any update where the tracked attributes (`name`, `segment`) were unchanged. Customer 2002's `internal_note` update is **completely absent** because only `internal_note` changed — not a tracked attribute.
# MAGIC
# MAGIC This is correct behaviour for SCD2: it only tracks business attributes you tell it about. The gap is that the `internal_note` update — a real write to the source table — leaves no trace in the SCD2 dimension. Once the source table's VACUUM window passes, that event is gone forever unless a history table captured it.
# MAGIC
# MAGIC *(If you've already run Step 6, you'll also see the `loyalty_tier` update for customer 2001 in the unfiltered set but absent from the SCD2 set — same principle: `loyalty_tier` isn't a tracked attribute.)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Deploy the history table pipeline
# MAGIC
# MAGIC This part happens outside this notebook:
# MAGIC
# MAGIC 1. **Workflows → Pipelines → Create pipeline**
# MAGIC 2. Set the source code to `patterns/01_append_only_history_capture.py` (update the catalog/schema reference inside that file first)
# MAGIC 3. Set the target catalog/schema to match Step 0 above
# MAGIC 4. Run the pipeline
# MAGIC 5. Return here and run the verification cell below

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify the history table captured everything

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT customer_id, _change_type, _commit_version, captured_at
        FROM {history_table}
        ORDER BY customer_id, _commit_version
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm customer_id 2002 shows **four** rows here (insert, the internal_note-only update's pre and post images, and the delete) — not the two rows an SCD2 dimension would show.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Confirm append-only enforcement
# MAGIC
# MAGIC This should fail. That failure is the table working correctly.

# COMMAND ----------

try:
    spark.sql(f"DELETE FROM {history_table} WHERE customer_id = 2002")
    print("⚠️ Delete succeeded — delta.appendOnly was not enforced. Check the table properties.")
except Exception as e:
    print(f"✅ Delete correctly rejected: {type(e).__name__}")
    print(f"   {str(e)[:200]}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | | SCD2 (apply_changes) | History table (this pattern) |
# MAGIC |---|---|---|
# MAGIC | Captures internal_note-only update | ❌ No — not a tracked attribute | ✅ Yes — every change captured |
# MAGIC | Stores pre-image of updates | ❌ No — postimage only | ✅ Yes — both images |
# MAGIC | Bounded by entity cardinality | ✅ Yes | ❌ No — grows with every write, forever |
# MAGIC | Answers "what's the value at time X" | ✅ Yes — this is what it's for | ⚠️ Possible, but not its purpose |
# MAGIC | Answers "what exactly happened, and when" | ❌ No — by design | ✅ Yes — this is what it's for |
# MAGIC | Survives source table VACUUM | ✅ Yes, once captured | ✅ Yes, once captured |
# MAGIC
# MAGIC See `patterns/02_schema_evolution_history_table.py` for the schema widening variant, and `patterns/03_partitioning_and_retention.sql` for the partitioning strategy referenced in the article.

# COMMAND ----------

# DBTITLE 1,Step 6 — Test schema evolution (Pattern 02)
# MAGIC %md
# MAGIC ## Step 6 — Test schema evolution (Pattern 02)
# MAGIC
# MAGIC This exercises the schema widening variant from `patterns/02_schema_evolution_history_table.py`.
# MAGIC
# MAGIC **Run sequence** (assuming Steps 0–5 are already complete):
# MAGIC
# MAGIC 1. Drop the existing history table — Pattern 02 needs a fresh start so it can capture initial rows *before* the schema change
# MAGIC 2. Deploy a pipeline using `02_schema_evolution_history_table.py` as the source (update the catalog/schema reference inside that file to `dbw_ae_dev_ca_01.demo.customer`)
# MAGIC 3. Run the pipeline once — this captures the current source rows (without `loyalty_tier`) into a fresh history table
# MAGIC 4. Run the cell below to ALTER the source table (adds `loyalty_tier` and updates customer 2001)
# MAGIC 5. Re-run the pipeline — `mergeSchema` causes the history table to widen automatically
# MAGIC 6. Run the verification cell — confirms old rows show `NULL` for `loyalty_tier` (column didn't exist yet) and new rows show `'Gold'`
# MAGIC
# MAGIC The key point: the pipeline must run **before** and **after** the ALTER TABLE so you can see the contrast between rows captured when the column didn't exist vs. when it did.

# COMMAND ----------

# DBTITLE 1,Alter source table and insert schema-evolution test data
# Add a new column to the source table — this simulates a schema change
# that the history table should automatically absorb via mergeSchema.
spark.sql(f"ALTER TABLE {source_table} ADD COLUMNS (loyalty_tier STRING)")

# Update an existing row with the new column value
spark.sql(f"UPDATE {source_table} SET loyalty_tier = 'Gold' WHERE customer_id = 2001")

print(f"✅ Source table widened with loyalty_tier column and customer 2001 updated.")
print(f"   Now re-run the Pattern 02 pipeline, then run the verification cell below.")
display(spark.sql(f"SELECT * FROM {source_table}"))

# COMMAND ----------

# DBTITLE 1,Verify schema evolution in the history table
# After the pipeline re-runs, rows captured BEFORE the ALTER TABLE
# should show NULL for loyalty_tier. Rows captured AFTER should show
# the actual value. Both are correct — the NULL accurately records
# that the column did not exist at the time of capture.
display(
    spark.sql(f"""
        SELECT customer_id, _change_type, _commit_version, loyalty_tier, captured_at
        FROM {history_table}
        ORDER BY customer_id, _commit_version
    """)
)

# Confirm the schema widened correctly
history_cols = [f.name for f in spark.table(history_table).schema.fields]
assert "loyalty_tier" in history_cols, "❌ loyalty_tier column not found — schema evolution may not have triggered."
print(f"✅ Schema evolution confirmed: history table now has {len(history_cols)} columns including loyalty_tier.")

# COMMAND ----------

# DBTITLE 1,Step 7 — Partitioning and retention (Pattern 03)
# MAGIC %md
# MAGIC ## Step 7 — Partitioning and retention (Pattern 03)
# MAGIC
# MAGIC Pattern 03 (`patterns/03_partitioning_and_retention.sql`) is not a pipeline file — it is explicit DDL that shows how to create the history table with a **generated partition column** (`commit_date`) derived from `_commit_timestamp`. This gives you date-based partition pruning on audit queries without ever populating the column yourself.
# MAGIC
# MAGIC **Why not use the pipeline here?** The `@dp.table` decorator owns table creation — it will not adopt a pre-existing managed table and instead raises: *"Could not materialize … because a MANAGED table already exists with that name."* Pattern 03 is designed for direct writes where you control the DDL yourself.
# MAGIC
# MAGIC **Run sequence** (assuming Steps 0–2 and Step 6 are already complete):
# MAGIC
# MAGIC 1. **Run the DDL cell** — drops the existing history table and creates the partitioned variant with the generated `commit_date` column (includes `loyalty_tier` from Step 6)
# MAGIC 2. **Run the streaming write cell** — reads CDF from the source table and writes directly to the partitioned history table (no pipeline needed)
# MAGIC 3. **Run the audit query cell** — filtered on `commit_date = current_date()` to verify data landed in the expected partition
# MAGIC 4. **Run the EXPLAIN cell** — confirm `PartitionFilters` appears in the physical plan, proving the optimiser is pruning on `commit_date`

# COMMAND ----------

# DBTITLE 1,Create the partitioned history table (Pattern 03 DDL)
# MAGIC %sql
# MAGIC -- Recreate the history table with a generated partition column.
# MAGIC -- This is the DDL from patterns/03_partitioning_and_retention.sql,
# MAGIC -- adapted to use the demo catalog/schema from Step 0.
# MAGIC
# MAGIC DROP TABLE IF EXISTS dbw_ae_dev_ca_01.demo.customer_history;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbw_ae_dev_ca_01.demo.customer_history (
# MAGIC     customer_id          BIGINT,
# MAGIC     name                 STRING,
# MAGIC     segment              STRING,
# MAGIC     internal_note        STRING,
# MAGIC     loyalty_tier         STRING,
# MAGIC     _change_type         STRING,
# MAGIC     _commit_version      BIGINT,
# MAGIC     _commit_timestamp    TIMESTAMP,
# MAGIC     captured_at          TIMESTAMP,
# MAGIC     commit_date          DATE GENERATED ALWAYS AS (CAST(_commit_timestamp AS DATE))
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (commit_date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'false',
# MAGIC     'delta.appendOnly' = 'true',
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Direct streaming write to the partitioned history table
from pyspark.sql.functions import current_timestamp

# Clear the checkpoint so the streaming query reprocesses all CDF versions
# from scratch. Without this, re-running after dropping the history table
# produces 0 rows because the checkpoint still tracks previously processed
# versions from the source table.
checkpoint_path = f"/tmp/cdf_history_demo_checkpoint/{schema}"
dbutils.fs.rm(checkpoint_path, recurse=True)

# Read CDF from the source table and write directly to the pre-created
# partitioned history table. This is the non-pipeline equivalent of
# Pattern 01 — same logic, but you own the DDL and the partitioning.
cdf_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(source_table)
    .withColumn("captured_at", current_timestamp())
)

query = (
    cdf_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(history_table)
)

query.awaitTermination()
print(f"✅ Streaming write complete — history table populated with partitioned data.")
display(spark.sql(f"SELECT * FROM {history_table} ORDER BY customer_id, _commit_version"))

# COMMAND ----------

# DBTITLE 1,Verify partitioning and run audit query
# MAGIC %sql
# MAGIC -- Audit query using the generated partition column.
# MAGIC -- commit_date is derived from _commit_timestamp (when the change happened on
# MAGIC -- the SOURCE table), NOT from captured_at (when the history table ingested it).
# MAGIC -- This means partition pruning aligns with "what changed on date X" queries,
# MAGIC -- which is the dominant access pattern for audit and compliance.
# MAGIC --
# MAGIC -- Note: In this demo all source changes happened today, so there is only one
# MAGIC -- partition. In production, data accumulates across dates and the pruning
# MAGIC -- benefit becomes significant — the optimiser skips all non-matching partitions.
# MAGIC
# MAGIC SELECT customer_id, name, segment, _change_type, _commit_timestamp, commit_date
# MAGIC FROM dbw_ae_dev_ca_01.demo.customer_history
# MAGIC WHERE commit_date = current_date()
# MAGIC   AND customer_id = 2001
# MAGIC ORDER BY _commit_version;

# COMMAND ----------

# DBTITLE 1,Confirm partition pruning in the physical plan
# MAGIC %sql
# MAGIC -- Look for PartitionFilters in the physical plan output.
# MAGIC -- Expected: PartitionFilters: [isnotnull(commit_date), (commit_date = 2026-07-01)]
# MAGIC --
# MAGIC -- This proves the optimiser pushes the date predicate into partition pruning
# MAGIC -- rather than scanning all files and filtering after the fact. The generated
# MAGIC -- column (GENERATED ALWAYS AS) is invisible to writers — Delta computes it
# MAGIC -- automatically from _commit_timestamp on every append — but fully visible
# MAGIC -- to the query optimiser for pruning.
# MAGIC
# MAGIC EXPLAIN
# MAGIC SELECT *
# MAGIC FROM dbw_ae_dev_ca_01.demo.customer_history
# MAGIC WHERE commit_date = current_date();

# COMMAND ----------

# DBTITLE 1,Cleanup
# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drop the demo tables and schema created by this notebook.

# COMMAND ----------

# DBTITLE 1,Drop demo tables and schema
spark.sql(f"DROP TABLE IF EXISTS {history_table}")
spark.sql(f"DROP TABLE IF EXISTS {source_table}")
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")

# Remove the streaming checkpoint so re-runs start fresh
dbutils.fs.rm(f"/tmp/cdf_history_demo_checkpoint/{schema}", recurse=True)

print(f"✅ Cleaned up: {history_table}, {source_table}, schema {catalog}.{schema}, and checkpoint.")