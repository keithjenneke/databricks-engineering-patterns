-- Pattern 2: Table-level row filters and column masks
-- ========================================================
-- Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/unity-catalog-row-column-security
--
-- The direct predecessor to ABAC, and still fully supported. Use this
-- pattern when a single table needs bespoke logic that does not warrant
-- a tag-driven ABAC policy — the scope is exactly one table at a time,
-- attached directly via ALTER TABLE.
--
-- Ten tables needing the same logic means ten separate ALTER TABLE
-- statements, each a separate point of maintenance. This is the genuine
-- trade-off against ABAC's single, tag-driven policy — worth confirming
-- table count and reuse expectations before choosing this pattern over
-- Pattern 1.
--
-- ACCOUNT-LEVEL GROUPS REQUIRED
-- ------------------------------
-- The following account-level groups must exist before the filters below
-- will return rows. Create them in the account console at
-- accounts.azuredatabricks.net → User management → Groups.
--
-- Row filter — one per state value in the employees table, plus:
--   NSW, VIC, QLD, SA, WA, TAS, ACT, NT
--   platform-admins — members see all rows, checked explicitly in the
--                     state_filter UDF below
--
-- Column masks:
--   HR       — members see unmasked TFN values
--   Finance  — members see unmasked salary values
--
-- Genuine Databricks account/metastore admins bypass the row filter
-- automatically at the platform level — this is separate from, and in
-- addition to, the 'platform-admins' group above, which is an
-- application-level bypass checked explicitly inside the UDF itself.
--
-- ABAC POLICIES ATTACH TO THE SAME BASE TABLE
-- ----------------------------------------------
-- 01_abac_policies.sql attaches its row filter and column mask policies
-- to this same base table (your_catalog.your_schema.employees) via tag
-- matching. If pattern 1's policies are still active when this file
-- runs, both mechanisms restrict the same table at once and a query
-- result can no longer tell you which one produced the restriction —
-- run pattern 1's cleanup section (bottom of that file) first if you're
-- switching from it to this pattern.


-- ============================================================
-- Row filter: attach a UDF directly to one table
-- ============================================================

-- A user sees a row only if they are a member of an account-level group
-- whose name matches the state value on that row (e.g. a user in the
-- 'NSW' group sees NSW rows). Admins see all rows.
-- is_account_group_member() requires ACCOUNT-LEVEL groups only —
-- workspace groups are not visible to this function.
CREATE OR REPLACE FUNCTION your_catalog.your_schema.state_filter(state STRING)
RETURN is_account_group_member(state) OR is_account_group_member('platform-admins');

ALTER TABLE your_catalog.your_schema.employees
SET ROW FILTER your_catalog.your_schema.state_filter ON (state);


-- ============================================================
-- Column mask: attach a UDF directly to one column
-- ============================================================

CREATE OR REPLACE FUNCTION your_catalog.your_schema.mask_tfn(tfn STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('HR') THEN tfn ELSE 'XXX' END;

ALTER TABLE your_catalog.your_schema.employees
ALTER COLUMN tfn
SET MASK your_catalog.your_schema.mask_tfn;


CREATE OR REPLACE FUNCTION your_catalog.your_schema.mask_salary(salary DECIMAL(10,2))
RETURNS DECIMAL(10,2)
RETURN CASE WHEN is_account_group_member('Finance') THEN salary ELSE NULL END;

ALTER TABLE your_catalog.your_schema.employees
ALTER COLUMN salary
SET MASK your_catalog.your_schema.mask_salary;


-- ============================================================
-- Verification — table-level filters and masks ARE visible in
-- information_schema, unlike ABAC policies
-- ============================================================

SELECT * FROM your_catalog.your_schema.information_schema.row_filters
WHERE table_name = 'employees';

SELECT * FROM your_catalog.your_schema.information_schema.column_masks
WHERE table_name = 'employees';


-- ============================================================
-- Cleanup — run to remove filters, masks, and UDFs created
-- by this file, or before migrating to the ABAC pattern (01)
-- ============================================================

-- ALTER TABLE your_catalog.your_schema.employees DROP ROW FILTER;
-- ALTER TABLE your_catalog.your_schema.employees ALTER COLUMN tfn DROP MASK;
-- ALTER TABLE your_catalog.your_schema.employees ALTER COLUMN salary DROP MASK;

-- DROP FUNCTION your_catalog.your_schema.state_filter;
-- DROP FUNCTION your_catalog.your_schema.mask_tfn;
-- DROP FUNCTION your_catalog.your_schema.mask_salary;
