"""
Pattern 1: Append-only CDF history capture
=============================================
Blog: Building a CDF History Table That Outlives Your VACUUM Window
URL:  https://www.cypheragency.com.au/resources/cdf-history-table-databricks-vacuum-retention
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/cdf-history-table

This is a Lakeflow Declarative Pipeline source file — deploy it as a pipeline,
the same way as the apply_changes patterns in the scd-type-2 folder. It does
not call apply_changes; it is a plain streaming table definition, so the
deployment mechanics are slightly different (no apply_changes-specific
constraints) but it still belongs in a pipeline rather than a notebook cell,
because @dp.table() is a Lakeflow Declarative Pipeline decorator.

KEY DECISIONS
-------------
1. delta.appendOnly = true — enforced at the table level, not just by
   convention. Delta will reject any UPDATE or DELETE statement issued
   against this table.

2. delta.enableChangeDataFeed = false on the history table itself — this
   table is a terminus, not a source for further CDF consumers. If you do
   need to chain another consumer off this table, reconsider whether that
   consumer should instead read from the original source's CDF directly.

3. No filter on _change_type — every change type CDF produces is captured,
   including both update_preimage and update_postimage for every update,
   not just the post-image. This is the single most important difference
   from an SCD2 pipeline using apply_changes.

4. startingVersion=0 — capture from the true beginning of the source
   table's history. If you are enabling this on an existing table, take a
   full snapshot baseline first and document the date, exactly as you would
   for any other CDF consumer.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="customer_history",
    comment=(
        "Permanent append-only capture of every CDF event from customer. "
        "Never updated or deleted. Independent of source VACUUM retention. "
        "Includes both pre-image and post-image of every update, and "
        "captures changes regardless of which columns were touched."
    ),
    table_properties={
        "delta.enableChangeDataFeed": "false",
        "delta.appendOnly": "true",
    },
)
def customer_history():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table("your_catalog.your_schema.customer")
        .withColumn("captured_at", current_timestamp())
    )


# ---------------------------------------------------------------------------
# After the pipeline runs, verify with:
#
#   SELECT customer_id, _change_type, _commit_version, captured_at
#   FROM your_catalog.your_schema.customer_history
#   ORDER BY customer_id, _commit_version;
#
# Using the seed data from 00_setup.py, customer_id 2002 should show FOUR
# rows here — insert, the internal_note-only update, and the delete — even
# though an SCD2 pipeline filtering to business attributes (name, segment)
# would only ever produce TWO rows for that customer (insert, delete),
# because the internal_note-only update never touches a tracked attribute.
# ---------------------------------------------------------------------------