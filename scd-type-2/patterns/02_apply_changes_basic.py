"""
Pattern 2: Basic apply_changes SCD Type 2 pipeline
=====================================================
Blog: SCD Type 2 on Databricks: Why APPLY CHANGES INTO Replaced 200 Lines of MERGE Logic
URL:  https://www.cypheragency.com.au/resources/scd-type-2-databricks-apply-changes-into
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/scd-type-2

IMPORTANT — this is a Lakeflow Declarative Pipeline source file, not a script
you run directly in a notebook cell. dp.create_auto_cdc_flow() only executes
inside the Lakeflow pipeline runtime. To run this:

  1. Run 00_setup.py first to seed the bronze table.
  2. In the Databricks workspace: Workflows -> Pipelines -> Create pipeline.
  3. Set this file (or this folder) as the pipeline's source code.
  4. Set the target catalog/schema to match your 00_setup.py CONFIG values.
  5. Run the pipeline once ("Start" with development mode, or a triggered run).
  6. Query the resulting customer_dim_v2 table from a regular notebook —
     querying a Delta table is fine outside the pipeline context, only the
     create_auto_cdc_flow call itself needs to run inside the pipeline.

This version does NOT fix the sequence-tie gotcha (customer_id 1002 in the
seed data) — see 03_apply_changes_sequence_tie_fix.py for that. It DOES
correctly resolve the out-of-order scenario (customer_id 1003), because
sequence_by — even using only updated_at — still gets the relative order of
non-tied records right. The tie is the one case a plain timestamp can't break.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ---------------------------------------------------------------------------
# Source view — reads the seeded bronze table from 00_setup.py
# ---------------------------------------------------------------------------

@dp.temporary_view()
def customer_updates_cdc():
    return spark.readStream.table("your_catalog.your_schema.customer_updates_raw")


# ---------------------------------------------------------------------------
# Target streaming table
# ---------------------------------------------------------------------------

dp.create_streaming_table("customer_dim_v2")


# ---------------------------------------------------------------------------
# The declarative SCD Type 2 statement
#
# keys                 — natural key of the dimension
# sequence_by           — determines true order of changes, NOT arrival order
# apply_as_deletes      — which records represent deletions
# except_column_list    — CDC metadata columns to exclude from the target schema
# stored_as_scd_type    — "2" keeps full history; "1" would overwrite in place
# ---------------------------------------------------------------------------

dp.create_auto_cdc_flow(
    target="customer_dim_v2",
    source="customer_updates_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "updated_at"],
    stored_as_scd_type="2",
)


# ---------------------------------------------------------------------------
# After the pipeline runs, verify from a regular notebook with:
#
#   SELECT customer_id, name, segment, __START_AT, __END_AT
#   FROM your_catalog.your_schema.customer_dim_v2
#   ORDER BY customer_id, __START_AT;
#
# Compare the row count and __END_AT IS NULL count per customer_id against
# the diagnostic query at the bottom of 01_manual_merge_scd2.sql.
# ---------------------------------------------------------------------------
