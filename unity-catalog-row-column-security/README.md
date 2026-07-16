# Unity Catalog Row and Column Level Security — ABAC, Table-Level Filters, and Dynamic Views

**Article:** [Row and Column Level Security in Unity Catalog: ABAC, Table-Level Filters, and Dynamic Views Compared](https://www.cypheragency.com.au/resources/unity-catalog-row-column-security-abac-dynamic-views)
**Stack:** Azure Databricks · Unity Catalog · Databricks SQL

---

## Overview

Three genuinely different mechanisms exist for row and column level security on Unity Catalog, each with a different scope and maintenance profile. This pattern folder implements all three against the same sample data, so the practical differences — not just the theoretical ones — are directly visible for anyone building row or column level security into a Data Integration, Analytics, and AI platform.

## Contents

```
unity-catalog-row-column-security/
├── README.md
├── notebooks/
│   └── Row_Column_Security_Demo.py
└── patterns/
    ├── 00_setup.py                                     ← Start here: sample table and governed tags
    ├── 01_abac_policies.sql                            ← Tag-driven ABAC row filters and column masks
    ├── 02_table_level_row_filters_and_masks.sql        ← Table-level ALTER TABLE filters and masks
    ├── 03_dynamic_views.sql                            ← SQL-based secured views
    └── 04_verify_as_non_admin.py                       ← Non-admin verification helper
```

## Prerequisites

- Unity Catalog enabled on your workspace
- For ABAC specifically (`01_abac_policies.sql`): Databricks Runtime 16.4 or above, or Serverless compute. Standard or dedicated compute on an older runtime cannot access an ABAC-secured table at all.
- `MANAGE` privilege or ownership on the securable object, to create policies, row filters, or masks
- `ASSIGN` on the governed tag and `APPLY TAG` on the object, for the tagging step in `00_setup.py`

## Account-Level Groups Required

Every pattern in this folder is inert without these groups — none of the row filters or column masks will restrict anything until they exist. Create them manually in the account console (accounts.azuredatabricks.net → User management → Groups) before running `00_setup.py`. Workspace-level groups are not visible to `is_account_group_member()` — these must be account-level.

- `platform-admins` — bypasses row filters in every pattern
- One group per AU state/territory: `NSW`, `VIC`, `QLD`, `SA`, `WA`, `TAS`, `ACT`, `NT`
- `HR` — unmasked `tfn`
- `Finance` — unmasked `salary`

See `patterns/00_setup.py` for full setup instructions.

## The Three Mechanisms

### ABAC Policies (`01_abac_policies.sql`)

Tag-driven, applies automatically to every current and future table carrying the matching governed tag. Databricks' current recommended default. Cannot be attached directly to a view — where a view sits on an ABAC-protected table, the policy evaluates using the view owner's permissions.

### Table-Level Row Filters and Column Masks (`02_table_level_row_filters_and_masks.sql`)

Attached directly to one table via `ALTER TABLE`. Simple for a single table; does not scale across many tables without repeating the same statement.

### Dynamic Views (`03_dynamic_views.sql`)

Security logic lives in the view's `SELECT` statement. Users query the view, not the base table. Easiest to manage in version control when multiple BI tools or teams need different views of the same data.

## The Decision That Actually Matters

Not which mechanism is technically superior — how many things are querying the same data. One table, one tool: a table-level filter or mask is sufficient. Same data, multiple consuming tools or teams: dynamic views are easier to maintain and review as code. Consistent protection needed across many current and future tables: ABAC is the mechanism built for that scope.

## Running Multiple Patterns Back-to-Back

Patterns 1 (ABAC) and 2 (table-level filters/masks) both attach protection directly to the same base `employees` table. Running one without cleaning up the other first leaves both active simultaneously, so you can no longer tell which mechanism actually produced a given restriction. Run the commented-out cleanup section at the bottom of the pattern you're moving away from (`DROP POLICY`/`DROP FUNCTION` for pattern 1, `DROP ROW FILTER`/`DROP MASK`/`DROP FUNCTION` for pattern 2) before setting up the other on the same table.

Pattern 3's views read directly from that same base table, so if pattern 1 or 2 is still active while you're testing pattern 3, the view's own row/column logic can't be isolated — clean those up first too.

## Verification (`04_verify_as_non_admin.py`)

Admin accounts bypass row filters, column masks, and view restrictions. Testing any of the above while logged in as an admin will appear to work even when it is completely broken. This pattern provides a lightweight helper to flag an admin session directly — including membership in `platform-admins`, the custom group these patterns' row filters check explicitly, which is a separate bypass from being a genuine Databricks admin — and to sanity-check row counts and masked column values from a genuine non-admin session before a configuration is considered production-ready.

## Further Reading

- [Attribute-based access control in Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/)
- [Create and manage row filter and column mask policies](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/policies)
- [Requirements, quotas, and limitations for row filter and column mask policies](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/requirements)
- **Article:** [Row and Column Level Security in Unity Catalog](https://www.cypheragency.com.au/post/row-and-column-level-security-unity-catalog) — cypheragency.com.au
