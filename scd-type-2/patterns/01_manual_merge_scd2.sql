-- Pattern 1: Manual MERGE-based SCD Type 2
-- ============================================
-- Blog: SCD Type 2 on Databricks: Why APPLY CHANGES INTO Replaced 200 Lines of MERGE Logic
-- URL:  https://www.cypheragency.com.au/resources/scd-type-2-databricks-apply-changes-into
-- Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/scd-type-2
--
-- This is the hand-rolled implementation discussed in the article. It works for
-- the simple case and breaks in two specific ways that this pattern file
-- deliberately does NOT fix — see the notebooks/SCD_Type_2_Demo notebook for a
-- runnable reproduction of both bugs using the seed data from 00_setup.py.
--
-- BUG 1 — Same-batch duplicate changes (customer_id 1002 in the seed data):
--   Two updates to the same customer in one micro-batch can both pass the
--   "is this a change" filter independently, producing two rows that are
--   each marked is_current = true. Nothing below resolves that.
--
-- BUG 2 — Out-of-order arrival (customer_id 1003 in the seed data):
--   A correction with an earlier business timestamp than the row already
--   marked current will be treated as a fresh change and inserted as the
--   new current row, ahead of data that is actually newer. There is no
--   sequence comparison anywhere in this MERGE.
--
-- Run this against the bronze table created by 00_setup.py.
-- Target table: {catalog}.{schema}.customer_dim


-- ============================================================
-- Target table DDL
-- ============================================================
CREATE TABLE IF NOT EXISTS your_catalog.your_schema.customer_dim (
    customer_id  BIGINT      NOT NULL,
    name         STRING,
    segment      STRING,
    start_date   TIMESTAMP   NOT NULL,
    end_date     TIMESTAMP,
    is_current   BOOLEAN     NOT NULL
)
USING DELTA;


-- ============================================================
-- Step 1: Close out changed records
-- (set end date, clear current flag on the row being superseded)
-- ============================================================
MERGE INTO your_catalog.your_schema.customer_dim AS target
USING (
    SELECT s.customer_id, s.name, s.segment, s.updated_at
    FROM your_catalog.your_schema.customer_updates_raw s
    INNER JOIN your_catalog.your_schema.customer_dim t
        ON s.customer_id = t.customer_id
        AND t.is_current = true
    WHERE s.operation != 'DELETE'
      AND (s.name != t.name OR s.segment != t.segment)
) AS source
ON target.customer_id = source.customer_id
   AND target.is_current = true
WHEN MATCHED THEN
    UPDATE SET
        target.end_date = source.updated_at,
        target.is_current = false;


-- ============================================================
-- Step 2: Insert new current rows for changed and brand-new records
-- ============================================================
INSERT INTO your_catalog.your_schema.customer_dim
    (customer_id, name, segment, start_date, end_date, is_current)
SELECT
    s.customer_id,
    s.name,
    s.segment,
    s.updated_at AS start_date,
    NULL AS end_date,
    true AS is_current
FROM your_catalog.your_schema.customer_updates_raw s
LEFT JOIN your_catalog.your_schema.customer_dim t
    ON s.customer_id = t.customer_id
    AND t.is_current = true
WHERE s.operation != 'DELETE'
  AND (
        t.customer_id IS NULL                                  -- brand new customer
        OR s.name != t.name OR s.segment != t.segment          -- changed customer
      );


-- ============================================================
-- Step 3: Apply deletes — close out the current row, insert nothing
-- ============================================================
MERGE INTO your_catalog.your_schema.customer_dim AS target
USING (
    SELECT customer_id, updated_at
    FROM your_catalog.your_schema.customer_updates_raw
    WHERE operation = 'DELETE'
) AS source
ON target.customer_id = source.customer_id
   AND target.is_current = true
WHEN MATCHED THEN
    UPDATE SET
        target.end_date = source.updated_at,
        target.is_current = false;


-- ============================================================
-- Diagnostic query — run this after the three steps above to check for
-- BUG 1 (duplicate current rows). A correct SCD2 table should return
-- zero rows from this query. The seed data from 00_setup.py will NOT
-- return zero rows for customer_id 1002 when only this manual MERGE is used.
-- ============================================================
SELECT customer_id, COUNT(*) AS current_row_count
FROM your_catalog.your_schema.customer_dim
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;
