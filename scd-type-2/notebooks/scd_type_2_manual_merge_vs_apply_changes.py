# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SCD Type 2 on Databricks — Manual MERGE vs apply_changes
# MAGIC %md
# MAGIC # SCD Type 2 on Databricks — Manual MERGE vs apply_changes
# MAGIC
# MAGIC Full pattern files: [databricks-engineering-patterns/scd-type-2](https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/scd-type-2)
# MAGIC
# MAGIC This notebook walks through:
# MAGIC 1. Seeding deliberately awkward CDC data (same-batch duplicate, out-of-order correction)
# MAGIC 2. Running the manual MERGE-based SCD2 implementation and showing where it breaks
# MAGIC 3. Deploying the `apply_changes` pipeline and verifying it does not have the same bug
# MAGIC
# MAGIC **Important:** `dlt.apply_changes()` only executes inside a Lakeflow Declarative
# MAGIC Pipeline runtime — it cannot be called directly in a notebook cell like a normal
# MAGIC function. Steps 1 and 2 below run entirely in this notebook. Step 3 requires you
# MAGIC to create a Lakeflow pipeline pointed at `patterns/02_apply_changes_basic.py`,
# MAGIC run it, then come back to this notebook to query the result — which is fine,
# MAGIC because querying a Delta table is just SQL and works anywhere.

# COMMAND ----------

# DBTITLE 1,Step 0 — Config
# MAGIC %md
# MAGIC ## Step 0 — Config
# MAGIC
# MAGIC Update these to match your environment before running anything below.

# COMMAND ----------

# DBTITLE 1,Configuration variables
catalog = "your_catalog"
schema = "your_schema"

bronze_table = f"{catalog}.{schema}.customer_updates_raw"
manual_target = f"{catalog}.{schema}.customer_dim"
apply_changes_target = f"{catalog}.{schema}.customer_dim_v2"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# DBTITLE 1,Step 1 — Seed the bronze CDC table
# MAGIC %md
# MAGIC ## Step 1 — Seed the bronze CDC table
# MAGIC
# MAGIC Four customers, each exercising a different scenario:
# MAGIC
# MAGIC | customer_id | Scenario |
# MAGIC |---|---|
# MAGIC | 1001 | Clean insert then a clean update — *should* be the easy case, but still breaks the MERGE |
# MAGIC | 1002 | Two updates in the same batch with an **identical** `updated_at` — the sequence-tie case |
# MAGIC | 1003 | A correction that arrives **after** a later row, but carries an **earlier** business timestamp — the out-of-order case |
# MAGIC | 1004 | Insert followed by a delete — the only scenario the MERGE handles correctly |
# MAGIC
# MAGIC All CDC rows land in a single bronze table and are processed in one pass.
# MAGIC This is the realistic scenario for an initial load or a catch-up batch
# MAGIC containing multiple changes per key — and is where the manual MERGE
# MAGIC pattern structurally fails.

# COMMAND ----------

# DBTITLE 1,Seed CDC data
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType
)
from datetime import datetime, timedelta

cdc_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("operation", StringType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("_commit_version", LongType(), False),
])

base = datetime(2026, 1, 1, 9, 0, 0)

rows = [
    (1001, "Alice Chen",  "SMB",        "INSERT", base,                                  1),
    (1001, "Alice Chen",  "Mid-Market", "UPDATE", base + timedelta(hours=1),              2),

    (1002, "Brendan Wu",  "SMB",        "INSERT", base,                                  3),
    (1002, "Brendan Wu",  "Enterprise", "UPDATE", base + timedelta(hours=2),              4),
    (1002, "Brendan Wu",  "Mid-Market", "UPDATE", base + timedelta(hours=2),              5),  # tie with the row above

    (1003, "Carla Singh", "SMB",        "INSERT", base,                                  6),
    (1003, "Carla Singh", "Mid-Market", "UPDATE", base + timedelta(hours=3),              8),
    (1003, "Carla Singh", "SMB",        "UPDATE", base + timedelta(hours=1, minutes=30),  7),  # arrives last, but earlier business time

    (1004, "Dev Patel",   "SMB",        "INSERT", base,                                  9),
    (1004, "Dev Patel",   "SMB",        "DELETE", base + timedelta(hours=4),            10),
]

cdc_df = spark.createDataFrame(rows, schema=cdc_schema)

(
    cdc_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

display(cdc_df.orderBy("customer_id", "_commit_version"))

# COMMAND ----------

# DBTITLE 1,Step 2 — Manual MERGE SCD2
# MAGIC %md
# MAGIC ## Step 2 — Run the manual MERGE-based SCD2 implementation
# MAGIC
# MAGIC This is the implementation from `patterns/01_manual_merge_scd2.sql`,
# MAGIC run here statement by statement so the result of each step is visible.

# COMMAND ----------

# DBTITLE 1,Manual MERGE SCD2 implementation
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {manual_target} (
    customer_id  BIGINT      NOT NULL,
    name         STRING,
    segment      STRING,
    start_date   TIMESTAMP   NOT NULL,
    end_date     TIMESTAMP,
    is_current   BOOLEAN     NOT NULL
)
USING DELTA
""")

# Step 2a — close out changed records
spark.sql(f"""
MERGE INTO {manual_target} AS target
USING (
    SELECT s.customer_id, s.name, s.segment, s.updated_at
    FROM {bronze_table} s
    INNER JOIN {manual_target} t
        ON s.customer_id = t.customer_id
        AND t.is_current = true
    WHERE s.operation != 'DELETE'
      AND (s.name != t.name OR s.segment != t.segment)
) AS source
ON target.customer_id = source.customer_id
   AND target.is_current = true
WHEN MATCHED THEN
    UPDATE SET
        target.end_date = source.updated_at,
        target.is_current = false
""")

# Step 2b — insert new current rows
spark.sql(f"""
INSERT INTO {manual_target} (customer_id, name, segment, start_date, end_date, is_current)
SELECT
    s.customer_id, s.name, s.segment, s.updated_at, NULL, true
FROM {bronze_table} s
LEFT JOIN {manual_target} t
    ON s.customer_id = t.customer_id AND t.is_current = true
WHERE s.operation != 'DELETE'
  AND (t.customer_id IS NULL OR s.name != t.name OR s.segment != t.segment)
""")

# Step 2c — apply deletes
spark.sql(f"""
MERGE INTO {manual_target} AS target
USING (
    SELECT customer_id, updated_at FROM {bronze_table} WHERE operation = 'DELETE'
) AS source
ON target.customer_id = source.customer_id AND target.is_current = true
WHEN MATCHED THEN
    UPDATE SET target.end_date = source.updated_at, target.is_current = false
""")

display(spark.table(manual_target).orderBy("customer_id", "start_date"))

# COMMAND ----------

# DBTITLE 1,Step 2d — Check for the bug
# MAGIC %md
# MAGIC ## Step 2d — Check for the bug
# MAGIC
# MAGIC A correct SCD2 table has at most one `is_current = true` row per
# MAGIC `customer_id`. Run the diagnostic below — it reveals the MERGE broke for
# MAGIC **every customer with multiple states**, not just the tricky edge cases:
# MAGIC
# MAGIC | customer_id | current_row_count | Why |
# MAGIC |---|---|---|
# MAGIC | 1001 | 2 | Even the "easy" insert-then-update case fails — Step 2a found nothing to close (target was empty), so Step 2b inserted both rows as current |
# MAGIC | 1002 | 3 | The INSERT plus both tied UPDATEs all landed as independent current rows |
# MAGIC | 1003 | 3 | All three CDC rows (INSERT + 2 UPDATEs) inserted as current with no sequencing |
# MAGIC
# MAGIC The root cause is structural: processing a full batch against an empty
# MAGIC target means Step 2a (close out changed records) has nothing to close —
# MAGIC the records it needs to expire haven't been inserted yet. Step 2b then
# MAGIC inserts everything as current because the LEFT JOIN finds no existing
# MAGIC rows to compare against.
# MAGIC
# MAGIC Only customer_id 1004 is correct (count 1, `is_current = false`) because
# MAGIC its only non-DELETE row is the initial INSERT — there's no second state
# MAGIC to conflict with.

# COMMAND ----------

# DBTITLE 1,Duplicate current-row check
display(
    spark.sql(f"""
        SELECT customer_id, COUNT(*) AS current_row_count
        FROM {manual_target}
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    """)
)

# COMMAND ----------

# DBTITLE 1,Step 3 — Deploy the apply_changes pipeline
# MAGIC %md
# MAGIC ## Step 3 — Deploy the apply_changes pipeline
# MAGIC
# MAGIC This part happens outside this notebook:
# MAGIC
# MAGIC 1. In the Databricks workspace sidebar: **Workflows → Pipelines → Create pipeline**
# MAGIC 2. Set the pipeline's source code to `patterns/02_apply_changes_basic.py`
# MAGIC    from this repo (update the catalog/schema inside that file first, to match
# MAGIC    the `catalog`/`schema` variables set in Step 0 of this notebook)
# MAGIC 3. Set the pipeline's target catalog and schema to the same values
# MAGIC 4. Click **Start** (development mode is fine for this demo)
# MAGIC 5. Once the pipeline run completes, come back here and run the cell below
# MAGIC
# MAGIC If you want to see the sequence-tie fix specifically, repeat the same steps
# MAGIC pointing at `patterns/03_apply_changes_sequence_tie_fix.py` instead, targeting
# MAGIC a different table name (or truncate and re-run) so you can compare both.

# COMMAND ----------

# DBTITLE 1,Step 4 — Verify the apply_changes result
# MAGIC %md
# MAGIC ## Step 4 — Verify the apply_changes result
# MAGIC
# MAGIC Run this *after* the pipeline from Step 3 has completed at least once.

# COMMAND ----------

# DBTITLE 1,Display apply_changes result
display(
    spark.sql(f"""
        SELECT customer_id, name, segment, __START_AT, __END_AT
        FROM {apply_changes_target}
        ORDER BY customer_id, __START_AT
    """)
)

# COMMAND ----------

# DBTITLE 1,Duplicate check on apply_changes target
# MAGIC %md
# MAGIC Run the same duplicate-current-row check as Step 2d, against the
# MAGIC `apply_changes` target this time:

# COMMAND ----------

# DBTITLE 1,Duplicate check — apply_changes target
display(
    spark.sql(f"""
        SELECT customer_id, COUNT(*) AS current_row_count
        FROM {apply_changes_target}
        WHERE __END_AT IS NULL
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    """)
)
# No error provided, code unchanged.

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC If you deployed `patterns/02_apply_changes_basic.py` (the basic version,
# MAGIC plain `sequence_by=col("updated_at")`), this query **may or may not**
# MAGIC return a row for customer_id 1002, depending on how Lakeflow happened to
# MAGIC resolve the tie on that run — and is not guaranteed to be the same result
# MAGIC if you truncate and re-run the pipeline. That non-determinism is the bug.
# MAGIC
# MAGIC If you deployed `patterns/03_apply_changes_sequence_tie_fix.py` instead
# MAGIC (the composite `sequence_by`), this query will reliably return zero rows,
# MAGIC including for customer_id 1002, on every run.
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC | Scenario | Manual MERGE | apply_changes (basic) | apply_changes (tie fix) |
# MAGIC |---|---|---|---|
# MAGIC | Clean update (1001) | ❌ 2 current rows — no close-before-insert | ✅ Correct | ✅ Correct |
# MAGIC | Same-batch tie (1002) | ❌ 3 current rows — all inserted independently | ⚠️ Non-deterministic tie resolution | ✅ Always correct |
# MAGIC | Out-of-order correction (1003) | ❌ 3 current rows — no sequence logic | ✅ Correct | ✅ Correct |
# MAGIC | Delete (1004) | ✅ Correct | ✅ Correct | ✅ Correct |
# MAGIC | Lines of SCD logic | \~60 (this simplified version; 150–250 in production) | \~12 | \~13 |
# MAGIC
# MAGIC The manual MERGE pattern is designed for **incremental** processing (one
# MAGIC batch at a time, where the target already contains prior state). When run
# MAGIC against a full batch from scratch — or whenever multiple changes to the
# MAGIC same key arrive in the same batch — it structurally cannot close out
# MAGIC records that haven't been inserted yet. `apply_changes` handles full-batch
# MAGIC and incremental scenarios identically because it resolves ordering
# MAGIC internally before writing.
# MAGIC
# MAGIC See `notebooks/scd_type_2_gotchas.py` for the schema-evolution and
# MAGIC backfill-drift gotchas referenced in the article, which this demo
# MAGIC doesn't cover.

# COMMAND ----------

# DBTITLE 1,Clean Up
# MAGIC %md
# MAGIC ## Clean Up
# MAGIC
# MAGIC Uncomment and run the cell below to drop the tables created by this
# MAGIC notebook. Leave commented out if you want to keep the data for further
# MAGIC exploration.

# COMMAND ----------

# DBTITLE 1,Drop demo tables
# spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
# spark.sql(f"DROP TABLE IF EXISTS {manual_target}")
# spark.sql(f"DROP TABLE IF EXISTS {apply_changes_target}")
# spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")  # WARNING: drops everything in the schema
# print("Clean up complete.")