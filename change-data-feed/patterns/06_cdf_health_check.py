"""
Pattern 6: Pre-batch CDF health check
=======================================
Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns

PROBLEM
-------
CDF pipelines fail in ways that do not always surface immediately:
- A VersionNotFoundException is obvious — the job fails with an error.
- Processing lag creeping toward the VACUUM window is silent until it fires.
- A CDF-disabled gap is invisible until you are debugging stale data weeks later.

Without proactive detection, you find out about failures from a downstream
data quality complaint — not from your monitoring system.

RECOVERY AND PREVENTION (same answer)
--------------------------------------
Run a health check function BEFORE each batch processes any data.
If unhealthy: alert and stop. Do not process on a broken foundation.
If healthy: proceed.

The check validates three things:
  1. CDF is still enabled on the source table
  2. The starting version you want to read from is still available
  3. Your processing lag is within acceptable bounds relative to VACUUM retention

USAGE
-----
Call check_cdf_health() at the start of every pipeline run, before any CDF reads.
If it returns (False, ...), raise an exception to fail the job visibly rather than
processing silently on a broken state.
"""

from typing import Tuple
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def check_cdf_health(
    table_name: str,
    last_processed_version: int,
    max_acceptable_lag: int = 100,
) -> Tuple[bool, str, str]:
    """
    Run a pre-batch health check on a CDF source table.

    Validates CDF status, version availability, and processing lag.
    Run this BEFORE processing any data. If unhealthy, stop the pipeline
    and alert rather than processing on a broken foundation.

    Args:
        table_name:              Fully qualified Delta table name
        last_processed_version:  The last _commit_version your pipeline processed
        max_acceptable_lag:      Maximum acceptable version lag before alerting.
                                 Set this well below your VACUUM retention window.
                                 e.g. if VACUUM retains 200 versions, use 150 as max.

    Returns:
        Tuple of (healthy: bool, status_code: str, recommended_action: str)

    Status codes:
        ALL_CHECKS_PASSED   — healthy, safe to proceed
        CDF_DISABLED        — CDF property unset on source table
        VERSION_LOST        — startingVersion no longer available (VACUUM swept it)
        HIGH_LAG            — processing lag approaching VACUUM retention window
    """

    # ----------------------------------------------------------------
    # Check 1: CDF is still enabled on the source table
    # ----------------------------------------------------------------
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        cdf_enabled = any(
            r["key"] == "delta.enableChangeDataFeed" and r["value"] == "true"
            for r in props
        )
        if not cdf_enabled:
            return (
                False,
                "CDF_DISABLED",
                "Re-enable CDF: ALTER TABLE SET TBLPROPERTIES (delta.enableChangeDataFeed = true). "
                "Identify the gap window using DESCRIBE HISTORY and perform a snapshot diff "
                "for any UPDATE/DELETE operations that occurred while CDF was disabled.",
            )
    except Exception as e:
        return (False, f"TABLE_ACCESS_ERROR: {str(e)[:120]}", "Verify table exists and permissions are correct.")

    # ----------------------------------------------------------------
    # Check 2: The starting version is still available
    # ----------------------------------------------------------------
    starting_version = last_processed_version + 1
    try:
        (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .table(table_name)
            .limit(1)
            .count()
        )
    except Exception as e:
        error_msg = str(e)[:150]
        return (
            False,
            f"VERSION_LOST: {error_msg}",
            f"Version {starting_version} is no longer available — likely swept by VACUUM. "
            "Perform a full snapshot recovery: read current table state as baseline and "
            "reset watermark to current version. See 01_vacuum_recovery.py.",
        )

    # ----------------------------------------------------------------
    # Check 3: Processing lag is within acceptable bounds
    # ----------------------------------------------------------------
    try:
        current_version = (
            spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
            .collect()[0]["version"]
        )
        lag = current_version - last_processed_version

        if lag > max_acceptable_lag:
            return (
                False,
                f"HIGH_LAG: {lag} versions behind (max acceptable: {max_acceptable_lag})",
                f"Pipeline is {lag} versions behind. Increase processing frequency or "
                f"extend VACUUM retention. If lag exceeds retention window, a "
                f"VersionNotFoundException will occur on the next run.",
            )
    except Exception as e:
        return (False, f"HISTORY_READ_ERROR: {str(e)[:120]}", "Verify table history is accessible.")

    # ----------------------------------------------------------------
    # All checks passed
    # ----------------------------------------------------------------
    return (True, "ALL_CHECKS_PASSED", "None — safe to proceed.")


def run_pipeline_with_health_check(
    table_name: str,
    last_processed_version: int,
    max_acceptable_lag: int = 100,
) -> None:
    """
    Wrapper that runs the health check before pipeline execution.
    Raises RuntimeError if any check fails, stopping the pipeline visibly.
    """
    healthy, status, action = check_cdf_health(
        table_name=table_name,
        last_processed_version=last_processed_version,
        max_acceptable_lag=max_acceptable_lag,
    )

    if not healthy:
        raise RuntimeError(
            f"CDF health check failed — pipeline stopped.\n"
            f"Status:  {status}\n"
            f"Action:  {action}\n"
            f"Table:   {table_name}\n"
            f"Last processed version: {last_processed_version}"
        )

    print(f"[OK] CDF health check passed for {table_name} — proceeding with batch.")


# --- Example usage ---
# run_pipeline_with_health_check(
#     table_name="catalog.schema.source_table",
#     last_processed_version=last_processed_version,
#     max_acceptable_lag=150   # alert if more than 150 versions behind
# )
#
# If healthy, proceed with your CDF read:
# changes = spark.read.format("delta") \
#     .option("readChangeFeed", "true") \
#     .option("startingVersion", last_processed_version + 1) \
#     .table("catalog.schema.source_table")
