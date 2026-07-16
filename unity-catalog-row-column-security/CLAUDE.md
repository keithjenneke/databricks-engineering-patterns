# Unity Catalog Row & Column Security — Claude Code Context

## Project Overview

This is a **pattern/reference repo**, not an application. It's the companion code for a blog article comparing three mechanisms for row- and column-level security in Databricks Unity Catalog, all demonstrated against the same sample dataset so the practical (not just theoretical) differences are visible.

**Article:** [Row and Column Level Security in Unity Catalog: ABAC, Table-Level Filters, and Dynamic Views Compared](https://www.cypheragency.com.au/resources/unity-catalog-row-column-security-abac-dynamic-views)
**Stack:** Azure Databricks · Unity Catalog · Databricks SQL · PySpark

There is no frontend, backend service, build step, or test suite. Changes here are to SQL/Python pattern files and the companion notebook — treat every edit as something a reader will copy-paste into their own workspace.

## Repository Structure

```
unity-catalog-row-column-security/
├── CLAUDE.md
├── README.md                                       ← article summary + decision guidance
├── notebooks/
│   └── Row_Column_Security_Demo.py                 ← Databricks notebook, runs all 3 patterns end-to-end
└── patterns/
    ├── 00_setup.py                                 ← sample `employees` table + governed tag taxonomy
    ├── 01_abac_policies.sql                        ← Pattern 1: tag-driven ABAC row filters/column masks
    ├── 02_table_level_row_filters_and_masks.sql    ← Pattern 2: ALTER TABLE row filter/mask on one table
    ├── 03_dynamic_views.sql                        ← Pattern 3: security logic embedded in a view
    └── 04_verify_as_non_admin.py                   ← verification helper (admins bypass all 3 mechanisms)
```

## The Three Mechanisms (and when each applies)

| Mechanism | File | Scope | Best suited for |
|---|---|---|---|
| **ABAC policies** | `01_abac_policies.sql` | Tag-driven; applies to every current *and future* table carrying the matching governed tag | Consistent protection across many tables; Databricks' current recommended default |
| **Table-level filters/masks** | `02_table_level_row_filters_and_masks.sql` | Attached via `ALTER TABLE` to exactly one table | A single table with bespoke, non-reusable logic |
| **Dynamic views** | `03_dynamic_views.sql` | Security logic lives in the view's `SELECT` | Same data queried by multiple BI tools/teams needing different, version-controlled views |

The decision that matters is **not** which mechanism is technically superior — it's how many things query the same data. See README.md for the full reasoning; keep that framing when adding to or editing pattern explanations.

## Sample Data Model

`00_setup.py` creates `{catalog}.{schema}.employees` (16 rows spanning all 8 Australian states/territories):

```
employee_id BIGINT, name STRING, department STRING, state STRING, tfn STRING, salary DECIMAL(10,2)
```

Governed tag taxonomy (`classification`): `tfn` → `pii`, `salary` → `financial`, `state` → `state`. Default `catalog = "dbw_ae_dev_ca_01"`, `schema = "demo"` — these are placeholder identifiers a reader is expected to change; don't treat them as real infrastructure.

Every pattern's row filter logic depends on **account-level** Databricks groups (created manually in the account console at `accounts.azuredatabricks.net`, never workspace-level groups, since `is_account_group_member()` cannot see workspace groups):

- `platform-admins` — bypasses row filters
- One group per state: `NSW`, `VIC`, `QLD`, `SA`, `WA`, `TAS`, `ACT`, `NT`
- `HR` — unmasked `tfn`
- `Finance` — unmasked `salary`

## Conventions for This Repo

- **SQL patterns (`01`–`03`) are self-contained and idempotent-ish**: use `CREATE OR REPLACE` / `CREATE ... IF NOT EXISTS` so a reader can re-run a file without manual cleanup. Each file ends with a commented-out cleanup section (`DROP POLICY`/`DROP FUNCTION`/`DROP VIEW`) — keep that pattern when adding new statements: drop policies/masks before the UDFs they reference.
- **Every SQL pattern file carries a requirements/prerequisites header comment** (runtime version, privileges, account groups needed). When editing a pattern, keep that header accurate — a reader copies these files directly into a workspace without other context.
- **Python files use fully-qualified 3-level names** (`catalog.schema.table`) throughout, no partial qualification.
- **Verification is not optional and not decorative** — `04_verify_as_non_admin.py` exists specifically because admin sessions silently bypass row filters, column masks, and view restrictions, making a broken policy look like it works. Any new pattern or example must be paired with, or explicitly point to, non-admin verification. Never present a pattern as "working" based on an admin-session test.
- **ABAC-specific caveat to preserve wherever ABAC is discussed**: ABAC policies cannot attach directly to a view. Where a view sits on an ABAC-protected table, the policy evaluates using the *view owner's* identity/permissions, not the querying user's.
- **No secrets or real workspace identifiers**: catalog/schema names, group names, and sample data here are illustrative placeholders. Don't introduce real hostnames, tokens, or credentials into any pattern file.

## Prerequisites (for validating any change makes sense)

- Unity Catalog enabled on the workspace
- ABAC (`01_abac_policies.sql`) specifically requires Databricks Runtime 16.4+ or Serverless compute — standard/dedicated compute on an older runtime cannot access an ABAC-secured table at all (hard failure, not degraded behavior)
- `MANAGE` privilege or ownership on the securable object to create policies/filters/masks
- `ASSIGN` on the governed tag and `APPLY TAG` on the object for the tagging step

## References

- [Attribute-based access control in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)
- [Create and manage row filter and column mask policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
- [Requirements, quotas, and limitations for row filter and column mask policies](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/abac/requirements)
