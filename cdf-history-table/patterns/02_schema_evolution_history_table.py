"""
Pattern 2: History table with schema evolution
==================================================
Blog: Building a CDF History Table That Outlives Your VACUUM Window
URL:  https://www.cypheragency.com.au/resources/cdf-history-table-databricks-vacuum-retention
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/cdf-history-table

Same deployment mechanics as 01_append_only_history_capture.py — a Lakeflow
Declarative Pipeline source file.

The difference from pattern 1: this version explicitly enables mergeSchema
on the CDF read, so that when the source table gains a new column, the
history table widens to include it automatically rather than failing or
silently dropping the new field.

The posture here is deliberately the OPPOSITE of an SCD2 target. In SCD2,
a new source column is a decision about whether to track it. In a history
table, the default should be to capture it — the entire point of the table
is fidelity to what the source actually looked like at each point in time,
including before a given column existed.

Historical rows captured before a new column existed will show NULL for
that column. That NULL is correct and meaningful: it accurately records
that the column did not exist yet when that row was captured. Do not
backfill it with a default value — the gap is the truth, and overwriting
it would be a quiet falsification of the historical record this table
exists to protect.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="customer_history",
    comment=(
        "Append-only CDF history capture with automatic schema widening. "
        "New source columns are added automatically; historical rows show "
        "NULL for columns that did not exist at the time of capture — this "
        "is intentional and should never be backfilled."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "false",
        "delta.appendOnly": "true",
    },
)
@dp.expect_or_drop("valid_change_type", "_change_type IS NOT NULL")
def customer_history():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("mergeSchema", "true")
        .option("startingVersion", 0)
        .table("your_catalog.your_schema.customer")
        .withColumn("captured_at", current_timestamp())
    )


# ---------------------------------------------------------------------------
# To exercise the schema evolution path, alter the source table after the
# pipeline has run at least once, then re-run the pipeline:
#
#   ALTER TABLE your_catalog.your_schema.customer ADD COLUMNS (loyalty_tier STRING);
#   UPDATE your_catalog.your_schema.customer SET loyalty_tier = 'gold' WHERE customer_id = 2001;
#
# After the next pipeline run, query the history table:
#
#   SELECT customer_id, _commit_version, loyalty_tier
#   FROM your_catalog.your_schema.customer_history
#   ORDER BY customer_id, _commit_version;
#
# Rows captured BEFORE the ALTER TABLE will show NULL for loyalty_tier.
# Rows captured AFTER will show the actual value. Both are correct.
# ---------------------------------------------------------------------------