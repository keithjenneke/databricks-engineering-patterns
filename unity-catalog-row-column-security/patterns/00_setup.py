"""
00: Setup — sample data and governed tags for row/column security patterns
================================================================================
Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/unity-catalog-row-column-security

Run this first. Creates a sample employees table spanning all Australian
states and territories, defines a governed tag taxonomy, and tags the table
and columns accordingly, so the subsequent ABAC, table-level, and dynamic
view patterns can be exercised against real objects.

PREREQUISITES
-------------
- Unity Catalog enabled on your workspace
- For the ABAC patterns specifically: Databricks Runtime 16.4 or above,
  or Serverless compute. Standard/dedicated compute on an older runtime
  cannot access an ABAC-secured table at all.
- Account admin or MANAGE privilege on the account to create governed tags
  and account-level groups
- APPLY TAG privilege on the target table/columns

ACCOUNT-LEVEL GROUPS — MANUAL SETUP REQUIRED
---------------------------------------------
The following groups must be created manually in the Databricks account
console (accounts.azuredatabricks.net) BEFORE running this script or the
ABAC patterns in 01_abac_policies.sql.

Account-level groups are the ONLY groups recognised by is_account_group_member()
in ABAC UDFs. Workspace-level groups (created in workspace Settings →
Identity & Access) are scoped to a single workspace and are invisible to
is_account_group_member() — do not create them there.

Groups to create in the account console:

  platform-admins  — platform administrators; exempt from all row/column
                     restrictions during administration and troubleshooting

  State/territory groups — one per state; members see only rows where
  state matches the group name (enforced by the regional_row_filter UDF
  in 01_abac_policies.sql):

    NSW   — New South Wales
    VIC   — Victoria
    QLD   — Queensland
    SA    — South Australia
    WA    — Western Australia
    TAS   — Tasmania
    ACT   — Australian Capital Territory
    NT    — Northern Territory

  Role groups — control column-level visibility:

    HR       — members see unmasked TFN values (mask_pii UDF)
    Finance  — members see unmasked salary values (mask_financial UDF)

To create a group in the account console:
  1. Go to accounts.azuredatabricks.net
  2. Navigate to User management → Groups
  3. Click Add group, enter the group name, and save
  4. Add members to the group from the same page


SCHEMAS CREATED
---------------
- {catalog}.{schema}      — employees table, table-level UDFs, and dynamic views
                            (patterns 02 and 03)
- {catalog}.governance    — ABAC policy UDFs (pattern 01)
"""

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagPolicy, Value

spark = SparkSession.builder.getOrCreate()

catalog = "dbw_ae_dev_ca_01"
schema = "demo"

employees_table = f"{catalog}.{schema}.employees"


def create_governed_tag():
    """
    Creates the 'classification' governed tag via the Databricks SDK.
    Governed tags are account-level objects and cannot be created via SQL DDL.
    Requires account admin or MANAGE privilege on the account.
    Silently skips if the tag already exists.
    """
    w = WorkspaceClient()
    try:
        w.tag_policies.create_tag_policy(
            tag_policy=TagPolicy(
                tag_key="classification",
                description="Data classification taxonomy for ABAC row/column security patterns",
                values=[
                    Value(name="pii"),
                    Value(name="financial"),
                    Value(name="state"),
                ],
            )
        )
        print("Governed tag key created: classification")
    except Exception as e:
        print(f"Tag creation skipped or already exists: {str(e)[:150]}")


def setup():
    # {catalog}.{schema} — employees table, table-level UDFs (pattern 02), dynamic views (pattern 03)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # {catalog}.governance — ABAC policy UDFs (pattern 01)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.governance")

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

    print(f"Sample table ready: {employees_table} (16 rows across NSW, VIC, QLD, SA, WA, TAS, ACT, NT)")
    print(f"Schemas ready: {catalog}.{schema}, {catalog}.governance")

    # ---------------------------------------------------------------------
    # Governed tag taxonomy — required for the ABAC patterns.
    # Governed tags are account-level and access-controlled; creating the
    # tag key itself requires elevated privileges separate from tagging
    # individual objects. If your account already has a taxonomy defined,
    # skip tag creation and go straight to tagging the objects below.
    # ---------------------------------------------------------------------
    create_governed_tag()

    spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN tfn SET TAGS ('classification' = 'pii')")
    spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN salary SET TAGS ('classification' = 'financial')")
    spark.sql(f"ALTER TABLE {employees_table} ALTER COLUMN state SET TAGS ('classification' = 'state')")

    print("Columns tagged: tfn (pii), salary (financial), state (state)")


if __name__ == "__main__":
    setup()
