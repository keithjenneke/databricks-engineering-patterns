"""
Pattern 4: Schema validation via exhaustive allow-list
==========================================================
Blog: SCD Type 2 on Databricks: Why APPLY CHANGES INTO Replaced 200 Lines of MERGE Logic
URL:  https://www.cypheragency.com.au/resources/scd-type-2-databricks-apply-changes-into
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/scd-type-2

This is a plain Python utility module — not a Lakeflow pipeline file. Import
it into a notebook, a pipeline source file, or a CI check and call
validate_cdc_schema() before wiring a new CDC source into apply_changes.

THE PROBLEM THIS SOLVES
------------------------
except_column_list in dlt.apply_changes excludes named columns from becoming
dimension attributes. The risk is the opposite direction: anything you do NOT
explicitly exclude becomes a versioned attribute by default. If your CDC
source adds a new metadata column upstream — a new audit field, a Debezium
transaction ID — every change to that column silently creates a new SCD2 row,
even though nothing business-relevant changed.

An except-list only protects you against columns you already know about.
An allow-list protects you against columns you don't know about yet, because
anything unexpected fails loudly instead of silently versioning.

USAGE
-----
Call this against the source DataFrame/view before it feeds apply_changes,
ideally as part of a scheduled validation job or a pipeline expectation,
so a schema drift on the source is caught before it pollutes history rather
than after.
"""

from typing import Iterable, List, Tuple
from pyspark.sql import DataFrame


# The attribute columns this dimension is actually supposed to version.
# Update this list deliberately when the dimension's business attributes
# change — never implicitly.
EXPECTED_ATTRIBUTE_COLUMNS = ["customer_id", "name", "segment"]

# Columns produced by the CDC mechanism itself — never dimension attributes,
# always safe to see, never something to alert on.
CDC_METADATA_COLUMNS = ["operation", "updated_at", "_commit_version"]


def validate_cdc_schema(
    df: DataFrame,
    expected_columns: Iterable[str] = EXPECTED_ATTRIBUTE_COLUMNS,
    cdc_metadata_columns: Iterable[str] = CDC_METADATA_COLUMNS,
    raise_on_unexpected: bool = True,
) -> Tuple[bool, List[str], List[str]]:
    """
    Validate a CDC source DataFrame's schema against an explicit allow-list.

    Args:
        df: The source DataFrame (e.g. the CDC view feeding apply_changes)
        expected_columns: The dimension's known business attribute columns
        cdc_metadata_columns: Known CDC mechanism columns (operation,
            sequence columns) that are never dimension attributes
        raise_on_unexpected: If True, raises ValueError on any unexpected
            column. If False, returns the result for the caller to handle
            (e.g. log and alert without failing the pipeline).

    Returns:
        Tuple of (passed: bool, unexpected_columns: list, missing_columns: list)

    Raises:
        ValueError: if raise_on_unexpected is True and unexpected columns
            are found.
    """
    actual_columns = set(df.columns)
    allowed_columns = set(expected_columns) | set(cdc_metadata_columns)

    unexpected = sorted(actual_columns - allowed_columns)
    missing = sorted(set(expected_columns) - actual_columns)

    if missing:
        print(
            f"[WARN] Expected attribute columns not present in source: {missing}. "
            f"These will be NULL or absent in the target — confirm this is intentional."
        )

    if unexpected:
        message = (
            f"Unexpected columns found that are not in the attribute allow-list "
            f"or the CDC metadata list: {unexpected}.\n"
            f"If these are genuine new business attributes, add them to "
            f"EXPECTED_ATTRIBUTE_COLUMNS deliberately.\n"
            f"If these are new CDC mechanism metadata, add them to "
            f"CDC_METADATA_COLUMNS and to except_column_list in the apply_changes call.\n"
            f"Do not let either happen implicitly — every new column entering "
            f"a CDC source bound for an SCD2 target should be a reviewed decision."
        )
        if raise_on_unexpected:
            raise ValueError(message)
        else:
            print(f"[ERROR] {message}")
            return False, unexpected, missing

    return True, unexpected, missing


# ---------------------------------------------------------------------------
# Example usage in a notebook or as a pre-pipeline check:
#
#   from patterns.schema_validation_allowlist import validate_cdc_schema
#
#   cdc_df = spark.read.table("your_catalog.your_schema.customer_updates_raw")
#   validate_cdc_schema(cdc_df)
#   # Raises ValueError immediately if, say, a "debezium_txn_id" column
#   # appears that nobody has reviewed yet.
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    # Minimal smoke test using PySpark's local session — illustrates the
    # exact failure mode this module exists to catch.
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, LongType

    spark = SparkSession.builder.master("local[1]").getOrCreate()

    # A schema that matches the allow-list — should pass cleanly.
    clean_schema = StructType([
        StructField("customer_id", LongType()),
        StructField("name", StringType()),
        StructField("segment", StringType()),
        StructField("operation", StringType()),
        StructField("updated_at", StringType()),
        StructField("_commit_version", LongType()),
    ])
    clean_df = spark.createDataFrame([], clean_schema)
    passed, unexpected, missing = validate_cdc_schema(clean_df, raise_on_unexpected=False)
    print(f"Clean schema check — passed: {passed}")

    # A schema with an unreviewed new column — should fail loudly.
    drifted_schema = StructType(clean_schema.fields + [
        StructField("debezium_txn_id", StringType())
    ])
    drifted_df = spark.createDataFrame([], drifted_schema)
    passed, unexpected, missing = validate_cdc_schema(drifted_df, raise_on_unexpected=False)
    print(f"Drifted schema check — passed: {passed}, unexpected: {unexpected}")
