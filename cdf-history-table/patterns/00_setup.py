"""
00: Setup & sample CDC data generator
========================================
Blog: Building a CDF History Table That Outlives Your VACUUM Window
URL:  https://www.cypheragency.com.au/resources/cdf-history-table-databricks-vacuum-retention
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/cdf-history-table

Run this first. Creates a source customer table with CDF enabled, then writes
a sequence of changes — including a same-row update touching only an excluded
column, to illustrate the SCD2-vs-history-table difference directly — so the
demo notebook has real CDF events to capture.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# CONFIG — replace these values for your environment
# ---------------------------------------------------------------------------

catalog = "your_catalog"
schema = "your_schema"

source_table = f"{catalog}.{schema}.customer"
history_table = f"{catalog}.{schema}.customer_history"


def setup():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(f"DROP TABLE IF EXISTS {source_table}")
    spark.sql(f"""
        CREATE TABLE {source_table} (
            customer_id   BIGINT,
            name          STRING,
            segment       STRING,
            internal_note STRING   -- deliberately excluded from SCD2 dimensions
                                     -- in a real pipeline, but still worth capturing
                                     -- in a full history table
        )
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Version 1 — initial load
    spark.sql(f"""
        INSERT INTO {source_table} VALUES
            (2001, 'Alice Chen',  'SMB', 'created via signup form'),
            (2002, 'Brendan Wu',  'SMB', 'created via signup form')
    """)

    # Version 2 — a genuine business-attribute change (would generate an SCD2 row)
    spark.sql(f"""
        UPDATE {source_table} SET segment = 'Mid-Market' WHERE customer_id = 2001
    """)

    # Version 3 — a write that touches ONLY internal_note, not a tracked SCD2
    # attribute. An SCD2 pipeline filtering to business attributes produces
    # NO new row for this. A history table built without that filter
    # captures it anyway — this is the exact distinction the article makes.
    spark.sql(f"""
        UPDATE {source_table} SET internal_note = 'flagged for billing review'
        WHERE customer_id = 2002
    """)

    # Version 4 — delete
    spark.sql(f"""
        DELETE FROM {source_table} WHERE customer_id = 2002
    """)

    baseline_version = (
        spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 1")
        .collect()[0]["version"]
    )

    print(f"✅ Source table ready: {source_table}")
    print(f"   Current version: {baseline_version}")
    print(f"\nScenario seeded:")
    print(f"   customer_id 2001 — clean business-attribute change (segment)")
    print(f"   customer_id 2002 — a write touching ONLY internal_note,")
    print(f"                      then a delete")
    print(f"\n   The internal_note-only update is the row that an SCD2 pipeline")
    print(f"   filtering to business attributes will never capture, and that")
    print(f"   this history table pattern captures by design.")


if __name__ == "__main__":
    setup()
