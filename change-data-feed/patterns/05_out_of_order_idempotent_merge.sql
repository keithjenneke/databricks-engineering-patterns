-- Pattern 5: Out-of-order data — idempotent MERGE with version guard
-- ====================================================================
-- Blog: Delta Lake Change Data Feed in Production: Six Things That Will Break Your Pipeline
-- URL:  https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns
--
-- PROBLEM
-- -------
-- With multiple concurrent writers and retry logic, commits don't always arrive
-- in business-logic order. A lower-version (stale) record can arrive after a
-- higher-version (current) record and overwrite the correct state in your sink.
--
-- The symptom is subtle: rows that are correct most of the time, but occasionally
-- show stale values. Very difficult to catch in testing.
--
-- RECOVERY AND PREVENTION (same answer)
-- --------------------------------------
-- Use an idempotent MERGE with a version guard:
--   source._commit_version > target.last_commit_version
--
-- This ensures a lower-version (stale) record can NEVER overwrite a higher-version
-- (current) record, regardless of processing order.
--
-- REQUIREMENTS
-- ------------
-- Your sink table must include a last_commit_version column.
-- Populate it on every write from the CDF _commit_version metadata column.
-- This also makes the pipeline safe to replay — any batch can be re-delivered
-- and the sink will converge to the correct state.


-- ============================================================
-- Sink table DDL — must include last_commit_version
-- ============================================================
CREATE TABLE IF NOT EXISTS catalog.schema.sink_table (
    id                   BIGINT      NOT NULL,
    value                STRING,
    status               STRING,
    updated_at           TIMESTAMP,
    last_commit_version  BIGINT      NOT NULL   -- CDF version guard
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'false'      -- CDF not needed on sink
);


-- ============================================================
-- Idempotent MERGE with version guard
--
-- Handles: inserts, updates (postimage only), deletes
-- Safe to replay: re-delivering any batch produces the same result
-- Out-of-order safe: stale records cannot overwrite current state
-- ============================================================
MERGE INTO catalog.schema.sink_table AS target
USING (
    -- Deduplicate within the batch: keep only the highest version per id
    SELECT
        id,
        value,
        status,
        updated_at,
        _change_type,
        _commit_version,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _commit_version DESC
        ) AS rn
    FROM catalog.schema.incoming_cdf_changes
    WHERE _change_type IN ('insert', 'update_postimage', 'delete')
) AS source
ON target.id = source.id
AND source.rn = 1   -- Only process the highest version record per id in this batch

-- DELETE: only if the delete record is newer than what's in the sink
WHEN MATCHED
     AND source._change_type = 'delete'
     AND source._commit_version > target.last_commit_version
THEN DELETE

-- UPDATE: version guard ensures stale records never overwrite current state
WHEN MATCHED
     AND source._change_type = 'update_postimage'
     AND source._commit_version > target.last_commit_version
THEN UPDATE SET
    target.value               = source.value,
    target.status              = source.status,
    target.updated_at          = source.updated_at,
    target.last_commit_version = source._commit_version

-- INSERT: only for net-new rows (not deletes)
WHEN NOT MATCHED
     AND source._change_type != 'delete'
THEN INSERT (
    id,
    value,
    status,
    updated_at,
    last_commit_version
) VALUES (
    source.id,
    source.value,
    source.status,
    source.updated_at,
    source._commit_version
);


-- ============================================================
-- Verify idempotency: re-run the same batch, row counts should not change
-- ============================================================
-- Run the MERGE again with the same incoming_cdf_changes data.
-- The version guard (source._commit_version > target.last_commit_version)
-- ensures no rows are updated or deleted a second time.
-- Result: 0 rows affected on replay.
