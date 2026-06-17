"""
00: Setup & Configuration
==========================
Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns
Repo: https://github.com/keithjenneke/databricks-patterns/tree/main/change-data-feed

Run this file first. It defines the shared configuration used by all other
pattern files in this folder and creates the Unity Catalog Volume used to
store streaming checkpoints.

PREREQUISITES
-------------
- Azure Databricks Runtime 13.3 LTS or higher
- Unity Catalog enabled on your workspace
- A catalog and schema you have CREATE privilege on

WHAT THIS DOES
--------------
1. Creates the schema in the catalog if it does not already exist
2. Creates a Unity Catalog Volume for streaming checkpoint storage
3. Creates the demo source table with CDF enabled
4. Records the baseline version your consumers should start from

PLACEHOLDER REPLACEMENT
------------------------
Replace the values in the CONFIG section below before running.
Do not commit real catalog/schema names to a public repo.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# CONFIG — replace these values for your environment
# ---------------------------------------------------------------------------

catalog      = "dbw_ae_dev_ca_01"    # Unity Catalog catalog name
schema       = "demo"                # Schema name within the catalog
volume_name  = "checkpoints"         # Volume name for checkpoint storage

# Table names — update to match your source and sink table names
source_table = f"{catalog}.{schema}.cdf_demo_customers"
sink_table   = f"{catalog}.{schema}.cdf_demo_customers_sync"

# Checkpoint paths — all checkpoints live under the UC Volume
# Use versioned paths so recovery history is traceable
checkpoint_base     = f"/Volumes/{catalog}/{schema}/{volume_name}/cdf_pipelines"
checkpoint_stream   = f"{checkpoint_base}/stream_v1"          # active stream
checkpoint_recovery = f"{checkpoint_base}/stream_v1_recovery" # recovery stream


# ---------------------------------------------------------------------------
# STEP 1: Create the schema
#
# The schema must exist before the Volume or tables can be created.
# If the schema already exists this is a no-op.
# ---------------------------------------------------------------------------
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    COMMENT 'CDF demo schema — managed by Cypher Agency patterns'
""")

print(f"✅ Schema ready: {catalog}.{schema}")


# ---------------------------------------------------------------------------
# STEP 2: Create the Unity Catalog Volume for checkpoint storage
#
# Volumes are the correct location for streaming checkpoints on Databricks.
# They are persistent across cluster restarts and serverless sessions,
# governed by Unity Catalog permissions, and browsable in Catalog Explorer.
#
# Never use /tmp/ or DBFS paths for production streaming checkpoints.
# ---------------------------------------------------------------------------
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
    COMMENT 'Streaming checkpoints for CDF pipelines — managed by Cypher Agency patterns'
""")

print(f"✅ Volume ready: {catalog}.{schema}.{volume_name}")
print(f"   Checkpoint base path : {checkpoint_base}")
print(f"   Active stream path   : {checkpoint_stream}")
print(f"   Recovery stream path : {checkpoint_recovery}")


# ---------------------------------------------------------------------------
# STEP 3: Create the source table (if it doesn't exist) and enable CDF
#
# CDF must be enabled before any change data can be captured.
# Note: CDF is NOT retroactive — changes before this point are not available.
# If enabling on an existing table, read a full snapshot as your baseline first.
# ---------------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {source_table} (
        id         INT,
        name       STRING,
        email      STRING,
        city       STRING,
        status     STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
""")

spark.sql(f"""
    ALTER TABLE {source_table}
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Confirm CDF is enabled
props = spark.sql(f"SHOW TBLPROPERTIES {source_table}").collect()
cdf_enabled = any(
    r["key"] == "delta.enableChangeDataFeed" and r["value"] == "true"
    for r in props
)
print(f"\n{'✅' if cdf_enabled else '❌'} CDF enabled on {source_table}: {cdf_enabled}")


# ---------------------------------------------------------------------------
# STEP 4: Record the baseline version
#
# If enabling CDF on an existing table, capture the current version as your
# consumer's starting point. Never attempt to read CDF from before this version.
# ---------------------------------------------------------------------------
baseline_version = (
    spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 1")
    .collect()[0]["version"]
)
print(f"\n📌 Baseline version: {baseline_version}")
print(f"   All pattern files should use startingVersion >= {baseline_version}")
print(f"   Earlier versions have no CDF data available.")


# ---------------------------------------------------------------------------
# QUICK REFERENCE — checkpoint path best practices
# ---------------------------------------------------------------------------
print("""
Checkpoint path reference
--------------------------
✅  /Volumes/catalog/schema/checkpoints/stream_v1       — correct (UC Volume)
✅  /Volumes/catalog/schema/checkpoints/stream_v2       — correct (new version)
❌  /tmp/checkpoints/my_stream                          — wrong (ephemeral)
❌  dbfs:/tmp/checkpoints/my_stream                     — wrong (not UC governed)
❌  dbfs:/FileStore/checkpoints/my_stream               — wrong (not UC governed)
""")
