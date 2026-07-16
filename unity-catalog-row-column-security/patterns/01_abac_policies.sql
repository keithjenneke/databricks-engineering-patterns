-- Pattern 1: ABAC row filter and column mask policies
-- ========================================================
-- Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/unity-catalog-row-column-security
--
-- ABAC is Databricks' current recommended default for row and column
-- level security. Policies reference governed tags rather than named
-- tables, so a single policy applies automatically to every current and
-- future table carrying the matching tag — the key operational advantage
-- over table-level filters and masks.
--
-- REQUIREMENTS (confirm before running)
-- ----------------------------------------
-- - Databricks Runtime 16.4 or above, or Serverless compute. Standard or
--   dedicated compute on an older runtime CANNOT access an ABAC-secured
--   table at all — this is a hard failure, not a graceful degradation.
-- - MANAGE privilege on the securable object, or ownership, to create a
--   policy.
-- - Governed tags already applied to the target columns — see
--   00_setup.py, which tags tfn as 'pii', salary as 'financial', and
--   state as 'state'.
-- - ABAC policies cannot be attached directly to a view. Where a view
--   sits on top of an ABAC-protected table, the policy still evaluates,
--   using the VIEW OWNER's identity and permissions — the view owner
--   must have appropriate access to the underlying table.
-- - is_account_group_member() checks ACCOUNT-LEVEL groups only — groups
--   created in the Databricks account console at
--   accounts.azuredatabricks.net, not workspace-level groups. Workspace
--   groups are scoped to a single workspace and are NOT visible to
--   is_account_group_member(). Create and manage the groups used in these
--   UDFs (platform-admins, HR, Finance, NSW, VIC, QLD, etc.) in the
--   account console before running this file.
-- - Pattern 2 (02_table_level_row_filters_and_masks.sql) attaches its row
--   filter and column masks directly to this same base table
--   (your_catalog.your_schema.employees). If pattern 2 is still active when
--   this file runs, both mechanisms restrict the same table at once and
--   a query result can no longer tell you which one produced the
--   restriction — run pattern 2's cleanup section (bottom of that file)
--   first if you're switching from it to this pattern.

-- ============================================================
-- Row filter policy: restrict rows by state, tag-driven
-- ============================================================

-- UDF containing the filtering logic.
-- A user sees a row only if they are a member of an account-level group
-- whose name matches the state value on that row (e.g. a user in the 'NSW'
-- group sees rows where state = 'NSW').
-- is_account_group_member() requires an ACCOUNT-LEVEL group — create the
-- group in the account console (accounts.azuredatabricks.net) before use.
CREATE OR REPLACE FUNCTION your_catalog.your_schema.region_row_filter(state STRING)
RETURN is_account_group_member(state) OR is_account_group_member('platform-admins');

-- Policy applies to every table in the catalog carrying a column tagged
-- classification = 'state' — not just the employees table created in
-- 00_setup.py. Any future table tagged the same way inherits this policy
-- automatically, with no further configuration step.
CREATE POLICY regional_row_filter
ON CATALOG your_catalog
ROW FILTER your_catalog.your_schema.region_row_filter
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('classification', 'state') AS state
USING COLUMNS (state);


-- ============================================================
-- Column mask policy: mask PII columns (tfn), tag-driven
-- ============================================================

-- 'HR' must be an ACCOUNT-LEVEL group — not a workspace group.
CREATE FUNCTION IF NOT EXISTS your_catalog.your_schema.mask_pii(value STRING)
RETURN CASE WHEN is_account_group_member('HR') THEN value ELSE 'XXX' END;

CREATE POLICY pii_column_mask
ON CATALOG your_catalog
COLUMN MASK your_catalog.your_schema.mask_pii
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('classification', 'pii') AS tfn
ON COLUMN tfn;


-- ============================================================
-- Column mask policy: mask financial columns, tag-driven
-- ============================================================

-- 'Finance' must be an ACCOUNT-LEVEL group — not a workspace group.
CREATE FUNCTION IF NOT EXISTS your_catalog.your_schema.mask_financial(value DECIMAL(10,2))
RETURN CASE WHEN is_account_group_member('Finance') THEN value ELSE NULL END;

CREATE POLICY financial_column_mask
ON CATALOG your_catalog
COLUMN MASK your_catalog.your_schema.mask_financial
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('classification', 'financial') AS salary
ON COLUMN salary;


-- ============================================================
-- Verification — list policies applied within the catalog
--
-- There is no information_schema table for ABAC policies specifically;
-- information_schema.row_filters and .column_masks show only table-level
-- filters and masks. Use the REST API or Catalog Explorer to list ABAC
-- policies, or query the audit log directly for policy CRUD events:
-- ============================================================

SELECT event_time, action_name, user_identity.email AS actor,
       request_params.name AS policy_name, response.status_code
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('createPolicy', 'deletePolicy', 'getPolicy', 'listPolicies')
ORDER BY event_time DESC;


-- ============================================================
-- Cleanup — run this section to remove all policies and UDFs
-- created by this file. Once dropped, queries on previously
-- protected tables immediately return unmasked, unfiltered data.
-- Policies must be dropped before their referenced UDFs.
-- ============================================================

-- DROP POLICY regional_row_filter ON CATALOG your_catalog;
-- DROP POLICY pii_column_mask     ON CATALOG your_catalog;
-- DROP POLICY financial_column_mask ON CATALOG your_catalog;

-- DROP FUNCTION your_catalog.your_schema.region_row_filter;
-- DROP FUNCTION your_catalog.your_schema.mask_pii;
-- DROP FUNCTION your_catalog.your_schema.mask_financial;
