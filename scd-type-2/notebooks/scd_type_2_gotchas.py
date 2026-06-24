# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,SCD Type 2 — The Gotchas That Don’t Show Up in the Tutorial
# MAGIC %md
# MAGIC # SCD Type 2 — The Gotchas That Don't Show Up in the Tutorial
# MAGIC
# MAGIC Companion notebook to the article:
# MAGIC 1. Sequence column ties
# MAGIC 2. `except_column_list` needs to be exhaustive
# MAGIC 3. Backfills can drift from incremental history
# MAGIC 4. Schema evolution on a target with existing history
# MAGIC
# MAGIC Run `notebooks/scd_type_2_manual_merge_vs_apply_changes.py` first if you have not already — this
# MAGIC notebook assumes the bronze table from that notebook's Step 1 exists.

# COMMAND ----------

# DBTITLE 1,Configuration
catalog = "your_catalog"
schema = "your_schema"
bronze_table = f"{catalog}.{schema}.customer_updates_raw"

# COMMAND ----------

# DBTITLE 1,Gotcha 1 — Sequence column ties
# MAGIC %md
# MAGIC ## Gotcha 1 — Sequence column ties
# MAGIC
# MAGIC Already demonstrated end-to-end in `scd_type_2_manual_merge_vs_apply_changes.py` via customer_id
# MAGIC 1002. This section isolates just the comparison so the mechanism is clear
# MAGIC without needing to deploy two separate pipelines.

# COMMAND ----------

# DBTITLE 1,Show tied rows
from pyspark.sql.functions import struct, col

# This is what apply_changes does internally when comparing two candidate
# "current" rows for the same key. With a plain timestamp:
tied_rows = spark.sql(f"""
    SELECT customer_id, name, segment, updated_at, _commit_version
    FROM {bronze_table}
    WHERE customer_id = 1002
    ORDER BY updated_at
""")
display(tied_rows)

# COMMAND ----------

# DBTITLE 1,Tied rows explanation
# MAGIC %md
# MAGIC Notice the two `UPDATE` rows for customer_id 1002 share an identical
# MAGIC `updated_at`. Sorting by `updated_at` alone (what `sequence_by=col("updated_at")`
# MAGIC does) leaves their relative order undefined — Spark's sort is not
# MAGIC guaranteed to be stable across partitions and re-runs for tied keys.
# MAGIC
# MAGIC Sorting by the composite `struct(updated_at, _commit_version)` instead —
# MAGIC what `sequence_by=struct(col("updated_at"), col("_commit_version"))` does —
# MAGIC produces a deterministic order every time, because `_commit_version` never
# MAGIC ties:

# COMMAND ----------

# DBTITLE 1,Resolved tie with composite sequence
tied_rows_resolved = spark.sql(f"""
    SELECT customer_id, name, segment, updated_at, _commit_version
    FROM {bronze_table}
    WHERE customer_id = 1002
    ORDER BY struct(updated_at, _commit_version)
""")
display(tied_rows_resolved)

# COMMAND ----------

# DBTITLE 1,Gotcha 2 — except_column_list needs to be exhaustive
# MAGIC %md
# MAGIC ## Gotcha 2 — except_column_list needs to be exhaustive
# MAGIC
# MAGIC Simulates a CDC source that gains a new metadata column upstream —
# MAGIC a Debezium transaction ID, in this example — that nobody added to
# MAGIC `except_column_list`. Without a check, this column would silently
# MAGIC become a versioned dimension attribute.

# COMMAND ----------

# DBTITLE 1,Simulate schema drift
from pyspark.sql.functions import lit

# Simulate the upstream schema drift: a new metadata column appears.
drifted_cdc = (
    spark.table(bronze_table)
    .withColumn("debezium_txn_id", lit("txn-88213"))
)

drifted_cdc.printSchema()

# COMMAND ----------

# DBTITLE 1,Except-list gap explanation
# MAGIC %md
# MAGIC If this DataFrame fed `apply_changes` directly with an
# MAGIC `except_column_list` that only listed `["operation", "updated_at"]`
# MAGIC (forgetting the new `debezium_txn_id` column), every row would now
# MAGIC carry a "new" attribute value — `debezium_txn_id` — that has nothing to
# MAGIC do with the customer's actual business attributes. Depending on whether
# MAGIC the value changes between batches, this can silently create spurious
# MAGIC SCD2 versions.
# MAGIC
# MAGIC `patterns/04_schema_validation_allowlist.py` catches this before it
# MAGIC reaches `apply_changes`, by validating against an explicit allow-list
# MAGIC rather than relying on remembering to update an except-list:

# COMMAND ----------

# DBTITLE 1,Schema validation allowlist demo
import sys
import importlib
# Adjust this path to wherever you've cloned the patterns folder
sys.path.append("/Workspace/Users/kjenneke@cypheragency.com.au/databricks-engineering-patterns/scd-type-2/patterns")

module = importlib.import_module("04_schema_validation_allowlist")
validate_cdc_schema = module.validate_cdc_schema

try:
    validate_cdc_schema(drifted_cdc)
except ValueError as e:
    print(f"Caught expected validation failure:\n\n{e}")

# COMMAND ----------

# DBTITLE 1,Gotcha 3 — Backfills can drift from incremental history
# MAGIC %md
# MAGIC ## Gotcha 3 — Backfills can drift from incremental history
# MAGIC
# MAGIC This is the hardest gotcha to demonstrate cleanly in a short notebook,
# MAGIC because the real-world version of this bug took five years of incremental
# MAGIC runs to surface. Below is a compressed reproduction of the underlying
# MAGIC mechanism: a sequence column that is monotonic *within* a batch, but not
# MAGIC monotonic *across* batches, due to a timezone inconsistency.

# COMMAND ----------

# DBTITLE 1,Backfill drift reproduction
from datetime import datetime, timezone, timedelta

# Batch 1 (processed incrementally, months ago): timestamps stored naively,
# implicitly in UTC+10 (Australia/Brisbane) but with no timezone marker.
batch_1 = [
    (2001, "Original record", datetime(2021, 6, 1, 9, 0, 0), 100),
]

# Batch 2 (processed incrementally, the following week): same naive
# convention, still implicitly UTC+10. Incremental processing never noticed
# the absence of a timezone because every batch used the same convention
# consistently relative to the batch before it.
batch_2 = [
    (2001, "First update", datetime(2021, 6, 8, 9, 0, 0), 101),
]

# Five years later: a FULL BACKFILL re-extracts from a source that NOW emits
# proper UTC timestamps. The same source event that batch_1 recorded as
# "2021-06-01 09:00:00" (naively, local time) is now correctly represented as
# UTC — which, after timezone conversion, lands at a different absolute
# instant than what the naive incremental pipeline assumed.
backfill_batch_1_utc_corrected = [
    (2001, "Original record", datetime(2021, 6, 1, 9, 0, 0, tzinfo=timezone.utc) - timedelta(hours=10), 100),
]

print("Incremental pipeline's understanding of batch 1 timestamp:")
print(f"  {batch_1[0][2]} (treated as naive/local)")
print()
print("Backfill's understanding of the SAME event, after correct UTC handling:")
print(f"  {backfill_batch_1_utc_corrected[0][2]} (10 hours earlier in absolute terms)")
print()
print("The relative ORDER between batch_1 and batch_2 is unaffected on its own.")
print("But once five years of batches are involved, and only SOME of the historical")
print("batches share this naive convention while others (post-fix upstream) do not,")
print("a full backfill processing all of history in one pass can resolve the GLOBAL")
print("order differently than incremental processing resolved it one batch at a time —")
print("because incremental processing never had to compare batch_1 against batch_500")
print("directly. A backfill does.")

# COMMAND ----------

# DBTITLE 1,Backfill lesson and practical check
# MAGIC %md
# MAGIC **The lesson, not just the bug:** verify your `sequence_by` column is
# MAGIC monotonic across the *entire* history you intend to process, not just
# MAGIC within the batches you've tested with so far. A full backfill is the
# MAGIC first time the entire timeline gets compared against itself at once —
# MAGIC it will surface any inconsistency that incremental processing was
# MAGIC structurally incapable of noticing.
# MAGIC
# MAGIC Practical check before backfilling a dimension with significant history:
# MAGIC
# MAGIC ```python
# MAGIC # Confirm the sequence column has no timezone ambiguity across the
# MAGIC # full extract before running a full apply_changes backfill.
# MAGIC full_extract = spark.table("bronze.customer_updates_full_history")
# MAGIC distinct_offsets = (
# MAGIC     full_extract
# MAGIC     .selectExpr("date_format(updated_at, 'XXX') AS utc_offset")
# MAGIC     .distinct()
# MAGIC )
# MAGIC display(distinct_offsets)
# MAGIC # More than one distinct offset across history is a signal to
# MAGIC # investigate before backfilling, not after.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Gotcha 4 — Schema evolution on a target with existing history
# MAGIC %md
# MAGIC ## Gotcha 4 — Schema evolution on a target with existing history
# MAGIC
# MAGIC Lakeflow does not automatically widen an SCD2 target's schema when the
# MAGIC source gains a new column, if that target already has history. This
# MAGIC mirrors the schema evolution discipline from the CDF article — the fix
# MAGIC is the same: never assume an additive schema change is free on a system
# MAGIC that is tracking history.

# COMMAND ----------

# DBTITLE 1,Schema tracking location config
# MAGIC %md
# MAGIC To allow `apply_changes` to evolve the target schema automatically when
# MAGIC the source gains a genuinely new business attribute, configure
# MAGIC `schema_tracking_location` on the pipeline:
# MAGIC
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="customer_dim_v2",
# MAGIC     source="customer_updates_cdc",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by=struct(col("updated_at"), col("_commit_version")),
# MAGIC     apply_as_deletes=expr("operation = 'DELETE'"),
# MAGIC     except_column_list=["operation", "updated_at", "_commit_version"],
# MAGIC     stored_as_scd_type="2",
# MAGIC     schema_tracking_location="/Volumes/your_catalog/your_schema/schema_tracking/customer_dim_v2",
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Without this, a new column on the source will either be silently dropped
# MAGIC (if it's in `except_column_list` by accident — see Gotcha 2) or cause the
# MAGIC pipeline to fail with a schema mismatch, depending on the exact change.
# MAGIC Treat any schema change to a CDC source feeding an SCD2 target as a
# MAGIC deliberate migration: update `EXPECTED_ATTRIBUTE_COLUMNS` in
# MAGIC `patterns/04_schema_validation_allowlist.py`, confirm the new column's
# MAGIC historical backfill behaviour (it will be NULL for all rows before the
# MAGIC column existed — is that acceptable for this attribute?), and only then
# MAGIC let it flow into the pipeline.

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Gotcha | Detection | Fix |
# MAGIC |---|---|---|
# MAGIC | Sequence ties | Two source rows, same key, same `sequence_by` value | Composite `sequence_by` — `struct(business_time, commit_version)` |
# MAGIC | Except-list gaps | New column appears in CDC source schema | Exhaustive allow-list validated before `apply_changes`, not an except-list |
# MAGIC | Backfill drift | Full backfill produces different `__START_AT` than incremental history | Confirm `sequence_by` is monotonic across the *entire* history, not just recent batches |
# MAGIC | Schema evolution | New source column, target has existing history | `schema_tracking_location`, treated as a deliberate migration |