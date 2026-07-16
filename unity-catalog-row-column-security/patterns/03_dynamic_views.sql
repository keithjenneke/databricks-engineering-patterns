-- Pattern 3: Dynamic views for row and column level security
-- ================================================================
-- Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/unity-catalog-row-column-security
--
-- The traditional SQL-based approach. A view is created with conditional
-- logic embedded directly in the SELECT statement. Users query the view,
-- not the base table — the security logic lives in the view definition,
-- which is a genuinely separate secured object from the underlying table.
--
-- WHEN THIS IS THE RIGHT CHOICE
-- --------------------------------
-- The decision that matters in practice is not "which mechanism is
-- technically superior" but "how many things are querying the same
-- data." Where the same underlying data is queried by more than one BI
-- tool, or by different teams who each need a different view of it,
-- dynamic views tend to be easier to manage than either ABAC or
-- table-level filters — the view definition can be stored in version
-- control, reviewed like any other code change, and updated in one
-- place without touching the base table.
--
-- ACCOUNT-LEVEL GROUPS REQUIRED
-- --------------------------------
-- The same groups used in patterns 01 and 02 apply here. Create them in
-- the account console at accounts.azuredatabricks.net → User management
-- → Groups before querying these views.
--
-- Row filter:  NSW, VIC, QLD, SA, WA, TAS, ACT, NT, platform-admins
--              (platform-admins is checked explicitly in the WHERE
--              clauses below)
-- Column masks: HR (TFN), Finance (salary)
--
-- Genuine Databricks account/metastore admins bypass the row filter
-- automatically at the platform level — separate from, and in addition
-- to, the 'platform-admins' group above.
--
-- PATTERNS 1 AND 2 ATTACH TO THE UNDERLYING BASE TABLE
-- --------------------------------------------------------
-- These views SELECT directly from your_catalog.your_schema.employees. If
-- pattern 1 (ABAC) or pattern 2 (table-level filters/masks) is still
-- active on that base table, its restriction applies before this view's
-- own WHERE clause and CASE expressions ever run — the view may receive
-- already-filtered rows or already-masked values, so its own row/column
-- logic can no longer be isolated or verified independently. Clean up
-- patterns 1 and 2 (see their cleanup sections) before verifying these
-- views.


-- ============================================================
-- Row level security via a dynamic view
-- ============================================================

-- A user sees only rows where the state value matches an account-level
-- group they belong to — consistent with the region_row_filter UDF in
-- pattern 01 and the state_filter UDF in pattern 02.
CREATE OR REPLACE VIEW your_catalog.your_schema.employees_filtered AS
SELECT *
FROM your_catalog.your_schema.employees
WHERE
    is_account_group_member(state)
    OR is_account_group_member('platform-admins');


-- ============================================================
-- Column level security via a dynamic view
-- ============================================================

CREATE OR REPLACE VIEW your_catalog.your_schema.employees_masked AS
SELECT
    employee_id,
    name,
    department,
    state,
    -- Mask TFN: visible to HR only
    CASE WHEN is_account_group_member('HR') THEN tfn ELSE 'XXX' END AS tfn,
    -- Mask salary: visible to Finance only
    CASE WHEN is_account_group_member('Finance') THEN salary ELSE NULL END AS salary
FROM your_catalog.your_schema.employees;


-- ============================================================
-- Combined row AND column security in a single view
-- ============================================================

CREATE OR REPLACE VIEW your_catalog.your_schema.employees_secured AS
SELECT
    employee_id,
    name,
    department,
    state,
    CASE WHEN is_account_group_member('HR') THEN tfn ELSE 'XXX' END AS tfn,
    CASE WHEN is_account_group_member('Finance') THEN salary ELSE NULL END AS salary
FROM your_catalog.your_schema.employees
WHERE
    is_account_group_member(state)
    OR is_account_group_member('platform-admins');


-- ============================================================
-- Verification
-- ============================================================

SELECT * FROM your_catalog.your_schema.employees_filtered;
SELECT * FROM your_catalog.your_schema.employees_masked;
SELECT * FROM your_catalog.your_schema.employees_secured;


-- ============================================================
-- Note on maintainability
--
-- Store this file's CREATE VIEW statements in the same version control
-- system as the rest of this platform's code, and treat any change to
-- the security logic within a view as a reviewed pull request, exactly
-- as you would a change to a Lakeflow Declarative Pipeline definition.
-- This is the specific maintainability advantage dynamic views offer
-- over table-level filters and masks scattered across ALTER TABLE
-- statements.
-- ============================================================


-- ============================================================
-- Cleanup
-- ============================================================

-- DROP VIEW your_catalog.your_schema.employees_filtered;
-- DROP VIEW your_catalog.your_schema.employees_masked;
-- DROP VIEW your_catalog.your_schema.employees_secured;
