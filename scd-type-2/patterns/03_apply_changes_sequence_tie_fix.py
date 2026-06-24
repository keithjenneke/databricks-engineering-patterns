"""
Pattern 3: apply_changes with a tie-resistant composite sequence
====================================================================
Blog: SCD Type 2 on Databricks: Why APPLY CHANGES INTO Replaced 200 Lines of MERGE Logic
URL:  https://www.cypheragency.com.au/resources/scd-type-2-databricks-apply-changes-into
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/scd-type-2

Same deployment instructions as 02_apply_changes_basic.py — this is a Lakeflow
Declarative Pipeline source file, not a script you run directly in a notebook.

This is the fix for the sequence-tie gotcha. customer_id 1002 in the seed
data has two UPDATE records with the IDENTICAL updated_at timestamp. With
pattern 02's plain sequence_by=col("updated_at"), which of those two rows
"wins" as the current row is undefined and can differ between a fresh run
and a re-run of the same data — which is exactly the kind of bug that looks
fine in testing and bites you during a backfill six months later.

The fix: make the sequence column a composite that cannot tie. _commit_version
is monotonically increasing and unique per source commit, so pairing it with
the business timestamp as a struct guarantees a deterministic order even when
the business timestamp alone does not.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct


@dp.temporary_view()
def customer_updates_cdc():
    return spark.readStream.table("your_catalog.your_schema.customer_updates_raw")


dp.create_streaming_table("customer_dim_v2")


dp.create_auto_cdc_flow(
    target="customer_dim_v2",
    source="customer_updates_cdc",
    keys=["customer_id"],
    # struct() compares element by element, in order — updated_at first,
    # then _commit_version only breaks ties where updated_at is identical.
    # This preserves business-time ordering as the primary sort and only
    # falls back to commit order when business time cannot decide.
    sequence_by=struct(col("updated_at"), col("_commit_version")),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "updated_at", "_commit_version"],
    stored_as_scd_type="2",
)


# ---------------------------------------------------------------------------
# Verify the fix from a regular notebook after the pipeline runs:
#
#   SELECT customer_id, COUNT(*) AS current_row_count
#   FROM dbw_ae_dev_ca_01.demo.customer_dim_v2
#   WHERE __END_AT IS NULL
#   GROUP BY customer_id
#   HAVING COUNT(*) > 1;
#
# This should return zero rows — including for customer_id 1002, which is
# the case pattern 02 cannot guarantee deterministically.
#
# Re-run the pipeline a second time against the same source data (full
# refresh) and re-run the verification query. The result for customer_id
# 1002 should be identical to the first run. With pattern 02's plain
# sequence_by, re-running is NOT guaranteed to pick the same "winning" row
# on a tie — this is the specific behaviour this fix removes.
# ---------------------------------------------------------------------------
