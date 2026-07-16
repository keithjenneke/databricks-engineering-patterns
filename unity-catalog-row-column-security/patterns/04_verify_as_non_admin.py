"""
Pattern 4: Verifying row/column security as a non-admin user
=================================================================
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/unity-catalog-row-column-security

THE MISTAKE THIS PATTERN EXISTS TO PREVENT
---------------------------------------------
Admin accounts typically bypass row filters, column masks, and dynamic
view restrictions entirely. Testing a security configuration while
logged in as an admin will show it "working" even when it is completely
broken, because the admin session never actually exercises the
restriction. The failure only surfaces once a genuine non-admin user
queries the same object — in production, in front of the person the
restriction was supposed to protect data from.

This is not specific to any one of the three mechanisms in this pattern
folder (ABAC, table-level filters, or dynamic views). It applies equally
to all three, and is the single most consistently reported mistake in
practitioner discussion of Unity Catalog row and column security.

USAGE
-----
Run this verification BEFORE considering any row filter, column mask, or
secured view production-ready. It does not replace manual testing as an
actual member of the target group — it is a lightweight, repeatable
sanity check to run as part of a CI/CD pipeline or a pre-deployment
checklist for this Data Integration, Analytics, and AI platform.

RUN ORDER AND CLEANUP BETWEEN PATTERNS
---------------------------------------
Patterns 01 (ABAC) and 02 (table-level filters/masks) both attach
protection directly to the same base table — {catalog}.{schema}.employees.
Setting up one without first cleaning up the other leaves both active on
the same table simultaneously, so a verification result here cannot tell
you which mechanism actually produced the restriction. Before setting up
and verifying a different pattern against the base employees table, run
the commented-out cleanup section at the bottom of the PREVIOUS pattern's
SQL file first (DROP POLICY / DROP FUNCTION for 01_abac_policies.sql, or
DROP ROW FILTER / DROP MASK / DROP FUNCTION for
02_table_level_row_filters_and_masks.sql).

Pattern 03's views (employees_filtered, employees_masked,
employees_secured) SELECT directly from that same base table. If pattern
01 or 02 is still active on the base table while testing pattern 03, the
view's own row/column logic can't be isolated — the base table's
restriction applies before the view's SELECT ever runs, so the view may
receive already-filtered rows or already-masked values, and the
comparison between mechanisms stops being meaningful. Clean up 01 and 02
from the base table before verifying pattern 03's views.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

catalog = "your_catalog"
schema = "your_schema"


def check_current_user_is_platform_admin() -> bool:
    """
    Returns True if the current session belongs to 'platform-admins' — the
    custom account-level group checked explicitly inside every row filter
    UDF/view in this pattern folder (region_row_filter in
    01_abac_policies.sql, state_filter in
    02_table_level_row_filters_and_masks.sql, and the view WHERE clause in
    03_dynamic_views.sql) as an intentional bypass.

    This is separate from, and not caught by, check_current_user_is_not_admin()
    (which only checks the built-in 'admins' group). Only relevant to row
    filters — column masks in this repo are gated by 'HR'/'Finance'
    membership, not 'platform-admins' — so this does not need to run ahead
    of verify_column_mask.
    """
    result = spark.sql(
        "SELECT is_account_group_member('platform-admins') AS is_platform_admin"
    ).collect()
    is_platform_admin = result[0]["is_platform_admin"]

    if is_platform_admin:
        print(
            "[WARNING] Current session is a member of 'platform-admins'. "
            "This pattern folder's row filter UDFs/views (01, 02, 03) "
            "explicitly bypass restriction for this group, so this session "
            "will see ALL rows regardless of whether the row filter is "
            "correct. This is expected behaviour for this group, not a "
            "defect — but it also means this session cannot validly test "
            "the row filter. Re-run verify_row_filter from a session "
            "authenticated as a member of an actual state/territory group "
            "instead."
        )
        return True

    return False


def verify_row_filter(table_name: str, expected_max_rows: int = None):
    """
    Runs a basic row count check against a row-filtered table or view,
    from the current (non-admin) session, and reports the result for
    manual comparison against the expected restricted view.

    Args:
        table_name: fully qualified table or view name to check
        expected_max_rows: optional — if provided, flags a warning when
            the actual row count exceeds this value, which may indicate
            the filter is not restricting as expected
    """
    if not check_current_user_is_not_admin():
        return
    if check_current_user_is_platform_admin():
        return

    row_count = spark.table(table_name).count()
    print(f"Row count visible to current session on {table_name}: {row_count}")

    if expected_max_rows is not None and row_count > expected_max_rows:
        print(
            f"[WARNING] Row count ({row_count}) exceeds expected maximum "
            f"({expected_max_rows}) for this session. Review the row "
            f"filter or dynamic view definition — the restriction may not "
            f"be applying as intended."
        )


def verify_column_mask(table_name: str, masked_column: str, unmasked_placeholder="XXX"):
    """
    Checks whether a masked column returns the expected placeholder value
    for the current (non-admin) session, rather than the real underlying
    value.

    Args:
        table_name: fully qualified table or view name to check
        masked_column: the column expected to be masked for this session
        unmasked_placeholder: the value the mask function returns when
            access is denied (e.g. 'XXX', NULL — adjust the check below
            to match your mask function's actual placeholder behaviour)
    """
    if not check_current_user_is_not_admin():
        return

    sample = spark.table(table_name).select(masked_column).limit(5).collect()
    values = [row[masked_column] for row in sample]

    print(f"Sample values for {masked_column} visible to current session: {values}")

    unexpected = [v for v in values if v is not None and v != unmasked_placeholder]
    if unexpected:
        print(
            f"[WARNING] Found values in '{masked_column}' that do not match "
            f"the expected masked placeholder ('{unmasked_placeholder}'). "
            f"This may indicate the mask is not applying for this session — "
            f"confirm this session is genuinely outside the group intended "
            f"to see the real value before treating this as a defect."
        )


# ---------------------------------------------------------------------------
# Example usage — run this file as a genuine non-admin user, authenticated
# as a member of a restricted state/territory group, not as an account
# admin, metastore admin, or 'platform-admins' member.
#
# expected_max_rows depends on the tester's state group — see 00_setup.py:
#   NSW / VIC / QLD = 3 employees each, SA / WA = 2 each, TAS / ACT / NT = 1 each
#
#   # Pattern 3 (dynamic views)
#   verify_row_filter(f"{catalog}.{schema}.employees_filtered", expected_max_rows=3)  # e.g. NSW
#   verify_column_mask(f"{catalog}.{schema}.employees_masked", "tfn")
#   verify_column_mask(f"{catalog}.{schema}.employees_masked", "salary", unmasked_placeholder=None)
#
#   # Pattern 1 (ABAC) / Pattern 2 (table-level) — both apply directly to
#   # the base table; clean up whichever pattern is NOT currently under
#   # test before running these (see RUN ORDER AND CLEANUP, above)
#   verify_row_filter(f"{catalog}.{schema}.employees", expected_max_rows=3)  # e.g. NSW
#   verify_column_mask(f"{catalog}.{schema}.employees", "tfn")
#   verify_column_mask(f"{catalog}.{schema}.employees", "salary", unmasked_placeholder=None)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    check_current_user_is_platform_admin()
