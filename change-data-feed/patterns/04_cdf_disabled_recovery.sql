-- Pattern 4: CDF disabled mid-pipeline
-- ======================================
-- Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
-- URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns
--
-- PROBLEM
-- -------
-- Someone runs ALTER TABLE UNSET TBLPROPERTIES ('delta.enableChangeDataFeed') on
-- your source table. Your pipeline does not fail immediately. It fails on the next
-- run when it tries to read CDF from versions written while CDF was disabled.
--
-- The gap is invisible: DESCRIBE HISTORY still shows all DML operations, but there
-- are no associated _change_data/ files for the disabled period. You can see that
-- rows were updated, but you cannot see what changed.
--
-- Error: DeltaIllegalStateException: Change data not recorded for version X
--
-- RECOVERY
-- --------
-- 1. Re-enable CDF on the source table
-- 2. Use DESCRIBE HISTORY to identify what happened during the disabled window
-- 3. If only INSERTs occurred: reconstruct from history
-- 4. If UPDATEs or DELETEs occurred: perform a full snapshot diff
--
-- PREVENTION
-- ----------
-- Run check_cdf_health() (see 06_cdf_health_check.py) before each pipeline batch
-- Add CDF status validation to your CI/CD pipeline for table schema changes
-- Use Unity Catalog table policies to prevent accidental UNSET operations


-- ============================================================
-- STEP 1: Re-enable CDF on the source table
-- ============================================================
ALTER TABLE catalog.schema.source_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);


-- ============================================================
-- STEP 2: Confirm CDF is now enabled
-- ============================================================
SHOW TBLPROPERTIES catalog.schema.source_table;
-- Look for: delta.enableChangeDataFeed = true


-- ============================================================
-- STEP 3: Identify what happened during the disabled window
--
-- Replace {pre_disable_version} with the last version before CDF was disabled
-- Replace {post_enable_version} with the version where CDF was re-enabled
-- ============================================================
SELECT
    version,
    timestamp,
    operation,
    operationParameters,
    operationMetrics
FROM (DESCRIBE HISTORY catalog.schema.source_table)
WHERE version > {pre_disable_version}
  AND version <= {post_enable_version}
ORDER BY version ASC;


-- ============================================================
-- STEP 4a: If only INSERT operations occurred during the gap
-- Reconstruct by reading from history using VERSION AS OF
-- ============================================================
SELECT *
FROM catalog.schema.source_table VERSION AS OF {post_enable_version}
WHERE _metadata.file_modification_time > (
    SELECT timestamp
    FROM (DESCRIBE HISTORY catalog.schema.source_table)
    WHERE version = {pre_disable_version}
);


-- ============================================================
-- STEP 4b: If UPDATE or DELETE operations occurred during the gap
-- Perform a full snapshot diff between the two boundary versions
-- The intermediate states are lost — accept the current state as the new baseline
-- ============================================================

-- View state at the pre-disable boundary
SELECT * FROM catalog.schema.source_table VERSION AS OF {pre_disable_version};

-- View state at the post-enable boundary (current baseline)
SELECT * FROM catalog.schema.source_table VERSION AS OF {post_enable_version};

-- Identify rows that changed (present in both versions with different values)
SELECT
    post.id,
    pre.status  AS status_before,
    post.status AS status_after
FROM catalog.schema.source_table VERSION AS OF {post_enable_version} AS post
LEFT JOIN catalog.schema.source_table VERSION AS OF {pre_disable_version} AS pre
    ON post.id = pre.id
WHERE pre.id IS NULL                     -- new rows (INSERT)
   OR post.status != pre.status;         -- changed rows (UPDATE) — adjust columns as needed


-- ============================================================
-- STEP 5: Reset your pipeline watermark
-- After gap recovery, set startingVersion to post_enable_version + 1
-- Future CDF reads will pick up from where CDF is now enabled
-- ============================================================
-- In your pipeline state store, update:
-- last_processed_version = {post_enable_version}
-- Next CDF read: startingVersion = {post_enable_version} + 1
