# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Row and Column Level Security — Comparison Demo
# MAGIC
# MAGIC Companion notebook for the pattern folder:
# MAGIC [databricks-engineering-patterns/unity-catalog-row-column-security](https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/unity-catalog-row-column-security)
# MAGIC
# MAGIC This notebook seeds the same sample `employees` table used by
# MAGIC `patterns/00_setup.py`, then walks through all three row and column
# MAGIC level security mechanisms — table-level filters/masks, dynamic views,
# MAGIC and ABAC policies — against that same underlying data, so the
# MAGIC practical differences are directly visible within this Data
# MAGIC Integration, Analytics, and AI security pattern.
# MAGIC
# MAGIC **Run the setup and pattern cells as an account admin.** Provisioning
# MAGIC tags, UDFs, policies, and row filters/masks requires elevated
# MAGIC privileges. Save the verification cell at the end for a genuinely
# MAGIC non-admin session — running it as an admin will not demonstrate the
# MAGIC restriction working.
# MAGIC
# MAGIC **Account-level groups required.** Every pattern below depends on
# MAGIC account-level groups that must exist BEFORE running this notebook —
# MAGIC create them in the account console (accounts.azuredatabricks.net →
# MAGIC User management → Groups). See `patterns/00_setup.py` for the full
# MAGIC list and instructions: `platform-admins`, one group per AU state/
# MAGIC territory (`NSW`, `VIC`, `QLD`, `SA`, `WA`, `TAS`, `ACT`, `NT`), `HR`,
# MAGIC and `Finance`.
# MAGIC
# MAGIC **Run order note.** Patterns 1 (ABAC) and 2 (table-level) both attach
# MAGIC protection directly to the same base `employees` table. This notebook
# MAGIC cleans up Pattern 2's row filter/masks before moving on, so Pattern 3
# MAGIC and Pattern 1 aren't affected by leftover state from Pattern 2. Don't
# MAGIC skip that cleanup cell if you re-run sections out of order.

# COMMAND ----------

catalog = "your_catalog"
schema = "your_schema"
governance_schema = f"{catalog}.governance"

employees_table = f"{catalog}.{schema}.employees"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {governance_schema}")

spark.sql(f"""
    CREATE OR REPLACE TABLE {employees_table} (
        employee_id BIGINT,
        name        STRING,
        department  STRING,
        state       STRING,
        tfn         STRING,
        salary      DECIMAL(10,2)
    )
""")

spark.sql(f"""
    INSERT INTO {employees_table} VALUES
        ( 1, 'Alice Chen',     'Finance',     'NSW', '111 222 333', 152000.00),
        ( 2, 'Brendan Wu',     'Sales',       'NSW', '222 333 444',  94000.00),
        ( 3, 'Carla Singh',    'HR',          'NSW', '333 444 555', 108000.00),
        ( 4, 'Dev Patel',      'Engineering', 'VIC', '444 555 666', 138000.00),
        ( 5, 'Elena Marsh',    'Finance',     'VIC', '555 666 777', 147000.00),
        ( 6, 'Frank Tran',     'Sales',       'VIC', '666 777 888',  91000.00),
        ( 7, 'Grace Kim',      'HR',          'QLD', '777 888 999', 103000.00),
        ( 8, 'Hamish Reid',    'Engineering', 'QLD', '888 999 000', 132000.00),
        ( 9, 'Isla Moore',     'Sales',       'QLD', '123 456 789',  88000.00),
        (10, 'Jake Nguyen',    'Finance',     'SA',  '234 567 890', 141000.00),
        (11, 'Kate Wilson',    'HR',          'SA',  '345 678 901',  99000.00),
        (12, 'Liam Costa',     'Engineering', 'WA',  '456 789 012', 143000.00),
        (13, 'Mia Zhang',      'Sales',       'WA',  '567 890 123',  96000.00),
        (14, 'Noah Byrne',     'Finance',     'TAS', '678 901 234', 136000.00),
        (15, 'Olivia Park',    'HR',          'ACT', '789 012 345', 115000.00),
        (16, 'Patrick Lee',    'Engineering', 'NT',  '890 123 456', 129000.00)
""")

display(spark.table(employees_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Governed tags
# MAGIC
# MAGIC Tag the PII/financial/state columns now. Tags apply no restriction on
# MAGIC their own, so this is safe to do upfront regardless of which pattern
# MAGIC you run next — Pattern 1 (ABAC) is the only mechanism that depends on
# MAGIC these tags; Patterns 2 and 3 ignore them.
# MAGIC
# MAGIC Creating the tag key itself requires account admin or `MANAGE`
# MAGIC privilege on the account, and is skipped if it already exists.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagPolicy, Value


def create_governed_tag():
    w = WorkspaceClient()
    try:
        w.tag_policies.create_tag_policy(
            tag_policy=TagPolicy(
                tag_key="classification",
                description="Data classification taxonomy for ABAC row/column security patterns",
                values=[Value(name="pii"), Value(name="financial"), Value(name="state")],
            )
        )
        print("Governed tag key created: classification")
    except Exception as e:
        print(f"Tag creation skipped or already exists: {str(e)[:150]}")


create_governed_tag()

spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN tfn SET TAGS ('classification' = 'pii')")
spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN salary SET TAGS ('classification' = 'financial')")
spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN state SET TAGS ('classification' = 'state')")

print("Columns tagged: tfn (pii), salary (financial), state (state)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 2 — Table-Level Row Filter and Column Masks
# MAGIC
# MAGIC See `patterns/02_table_level_row_filters_and_masks.sql` for the
# MAGIC standalone reference version of this pattern. Attached directly to
# MAGIC one table via `ALTER TABLE` — simplest to set up for a single table,
# MAGIC but does not scale across many tables without repeating the same
# MAGIC statements.

# COMMAND ----------

state_filter_fn = f"{catalog}.{schema}.state_filter"
mask_tfn_fn = f"{catalog}.{schema}.mask_tfn"
mask_salary_fn = f"{catalog}.{schema}.mask_salary"

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {state_filter_fn}(state STRING)
    RETURN is_account_group_member(state) OR is_account_group_member('platform-admins')
""")
spark.sql(f"ALTER TABLE {employees_table} SET ROW FILTER {state_filter_fn} ON (state)")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {mask_tfn_fn}(tfn STRING)
    RETURN CASE WHEN is_account_group_member('HR') THEN tfn ELSE 'XXX' END
""")
spark.sql(f"ALTER TABLE {employees_table} SET MASK {mask_tfn_fn} ON (tfn)")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {mask_salary_fn}(salary DECIMAL(10,2))
    RETURN CASE WHEN is_account_group_member('Finance') THEN salary ELSE NULL END
""")
spark.sql(f"ALTER TABLE {employees_table} SET MASK {mask_salary_fn} ON (salary)")

print(f"Pattern 2 applied: row filter + column masks attached directly to {employees_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC As an admin, querying the table directly still shows everything —
# MAGIC admin sessions bypass row filters and column masks. This is expected;
# MAGIC the real test is in the verification section at the end.

# COMMAND ----------

display(spark.table(employees_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up Pattern 2 before continuing
# MAGIC
# MAGIC Pattern 1 (ABAC) attaches directly to this same base table. Leaving
# MAGIC Pattern 2's row filter and masks in place would make it impossible to
# MAGIC tell which mechanism is actually restricting access once Pattern 1 is
# MAGIC applied later in this notebook.

# COMMAND ----------

spark.sql(f"ALTER TABLE {employees_table} DROP ROW FILTER")
spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN tfn DROP MASK")
spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN salary DROP MASK")

spark.sql(f"DROP FUNCTION {state_filter_fn}")
spark.sql(f"DROP FUNCTION {mask_tfn_fn}")
spark.sql(f"DROP FUNCTION {mask_salary_fn}")

print(f"Pattern 2 cleaned up — {employees_table} is unprotected again before Pattern 3 and Pattern 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 3 — Dynamic Views
# MAGIC
# MAGIC See `patterns/03_dynamic_views.sql`. Security logic lives in each
# MAGIC view's `SELECT` statement — users query the view, not the base table.
# MAGIC Creates separate objects, so nothing here needs cleanup before moving
# MAGIC on to Pattern 1.

# COMMAND ----------

employees_filtered = f"{catalog}.{schema}.employees_filtered"
employees_masked = f"{catalog}.{schema}.employees_masked"
employees_secured = f"{catalog}.{schema}.employees_secured"

spark.sql(f"""
    CREATE OR REPLACE VIEW {employees_filtered} AS
    SELECT *
    FROM {employees_table}
    WHERE
        is_account_group_member(state)
        OR is_account_group_member('platform-admins')
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {employees_masked} AS
    SELECT
        employee_id,
        name,
        department,
        state,
        CASE WHEN is_account_group_member('HR') THEN tfn ELSE 'XXX' END AS tfn,
        CASE WHEN is_account_group_member('Finance') THEN salary ELSE NULL END AS salary
    FROM {employees_table}
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {employees_secured} AS
    SELECT
        employee_id,
        name,
        department,
        state,
        CASE WHEN is_account_group_member('HR') THEN tfn ELSE 'XXX' END AS tfn,
        CASE WHEN is_account_group_member('Finance') THEN salary ELSE NULL END AS salary
    FROM {employees_table}
    WHERE
        is_account_group_member(state)
        OR is_account_group_member('platform-admins')
""")

print(f"Pattern 3 applied: {employees_filtered}, {employees_masked}, {employees_secured}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 1 — ABAC Policies
# MAGIC
# MAGIC See `patterns/01_abac_policies.sql`. Requires Databricks Runtime 16.4+
# MAGIC or Serverless compute — standard/dedicated compute on an older
# MAGIC runtime cannot access an ABAC-secured table at all. Policies are
# MAGIC tag-driven: they apply to the base table directly, and to any other
# MAGIC table in the catalog carrying the same tags (set in the governed tags
# MAGIC cell above), with no further configuration per table.

# COMMAND ----------

region_row_filter_fn = f"{governance_schema}.region_row_filter"
mask_pii_fn = f"{governance_schema}.mask_pii"
mask_financial_fn = f"{governance_schema}.mask_financial"

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {region_row_filter_fn}(state STRING)
    RETURN is_account_group_member(state) OR is_account_group_member('platform-admins')
""")

spark.sql(f"""
    CREATE POLICY regional_row_filter
    ON CATALOG {catalog}
    ROW FILTER {region_row_filter_fn}
    TO `account users`
    FOR TABLES
    MATCH COLUMNS has_tag_value('classification', 'state') AS state
    USING COLUMNS (state)
""")

spark.sql(f"""
    CREATE FUNCTION IF NOT EXISTS {mask_pii_fn}(value STRING)
    RETURN CASE WHEN is_account_group_member('HR') THEN value ELSE 'XXX' END
""")

spark.sql(f"""
    CREATE POLICY pii_column_mask
    ON CATALOG {catalog}
    COLUMN MASK {mask_pii_fn}
    TO `account users`
    FOR TABLES
    MATCH COLUMNS has_tag_value('classification', 'pii') AS tfn
    ON COLUMN tfn
""")

spark.sql(f"""
    CREATE FUNCTION IF NOT EXISTS {mask_financial_fn}(value DECIMAL(10,2))
    RETURN CASE WHEN is_account_group_member('Finance') THEN value ELSE NULL END
""")

spark.sql(f"""
    CREATE POLICY financial_column_mask
    ON CATALOG {catalog}
    COLUMN MASK {mask_financial_fn}
    TO `account users`
    FOR TABLES
    MATCH COLUMNS has_tag_value('classification', 'financial') AS salary
    ON COLUMN salary
""")

print(f"Pattern 1 (ABAC) applied: policies attached to catalog {catalog} — any tagged table inherits them")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — run this section as a non-admin user
# MAGIC
# MAGIC This is the step most commonly skipped. See
# MAGIC `patterns/04_verify_as_non_admin.py` for the full verification helper
# MAGIC functions — the cell below mirrors its two admin-bypass checks and
# MAGIC will warn you directly if either applies to the current session.
# MAGIC
# MAGIC Both checks matter here, for different reasons: a genuine Databricks
# MAGIC admin (`admins`) bypasses UC governance entirely, while a member of
# MAGIC `platform-admins` — the custom group referenced explicitly inside
# MAGIC every row filter UDF/view above — sees every row by design, which
# MAGIC looks identical to a broken row filter unless it's checked separately.

# COMMAND ----------

result = spark.sql(
    "SELECT is_account_group_member('admins') AS is_admin, "
    "is_account_group_member('platform-admins') AS is_platform_admin"
).collect()
is_admin = result[0]["is_admin"]
is_platform_admin = result[0]["is_platform_admin"]

if is_admin or is_platform_admin:
    group = "admins" if is_admin else "platform-admins"
    print(f"WARNING: This session is a member of '{group}'.")
    print("Row filters, column masks, and view restrictions above are")
    print("bypassed for both the built-in 'admins' group and the custom")
    print("'platform-admins' group referenced inside the row filter")
    print("UDFs/views. Re-run this verification from a session")
    print("authenticated as a member of an actual state/territory, HR, or")
    print("Finance group before treating any of the above configurations")
    print("as verified.")
else:
    print("OK — not an admin or platform-admins session. Proceeding to verify restrictions below.")
    display(spark.table(employees_secured))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Mechanism | Pattern | Scope | Best suited for |
# MAGIC |---|---|---|---|
# MAGIC | ABAC policies | 1 | Tag-driven, applies across many tables automatically | Consistent protection across a growing Data Integration, Analytics, and AI estate |
# MAGIC | Table-level filters/masks | 2 | One table at a time | A single table with bespoke, non-reusable logic |
# MAGIC | Dynamic views | 3 | A separate secured object | Multiple BI tools or teams needing different, version-controlled views of the same data |
# MAGIC
# MAGIC Regardless of mechanism: always verify as a non-admin, non-
# MAGIC platform-admins user before shipping to production.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC
# MAGIC Uncomment to remove everything this notebook created — Pattern 3's
# MAGIC views, Pattern 1's ABAC policies and UDFs, and the sample table and
# MAGIC schemas. Policies must be dropped before the UDFs they reference.

# COMMAND ----------

# spark.sql(f"DROP VIEW IF EXISTS {employees_filtered}")
# spark.sql(f"DROP VIEW IF EXISTS {employees_masked}")
# spark.sql(f"DROP VIEW IF EXISTS {employees_secured}")

# spark.sql(f"DROP POLICY regional_row_filter ON CATALOG {catalog}")
# spark.sql(f"DROP POLICY pii_column_mask ON CATALOG {catalog}")
# spark.sql(f"DROP POLICY financial_column_mask ON CATALOG {catalog}")

# spark.sql(f"DROP FUNCTION IF EXISTS {region_row_filter_fn}")
# spark.sql(f"DROP FUNCTION IF EXISTS {mask_pii_fn}")
# spark.sql(f"DROP FUNCTION IF EXISTS {mask_financial_fn}")

# spark.sql(f"DROP TABLE IF EXISTS {employees_table}")
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
# spark.sql(f"DROP SCHEMA IF EXISTS {governance_schema}")
