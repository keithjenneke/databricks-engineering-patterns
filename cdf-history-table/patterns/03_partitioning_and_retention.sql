-- Pattern 3: Partitioning and retention for a CDF history table
-- ===================================================================
-- Blog: Building a CDF History Table That Outlives Your VACUUM Window
-- URL:  https://www.cypheragency.com.au/resources/cdf-history-table-databricks-vacuum-retention
-- Repo: https://github.com/keithjenneke/databricks-engineering-patterns/tree/main/cdf-history-table
--
-- A history table has no natural ceiling on growth — it gains one row for
-- every single write the source table ever processes, forever. That is the
-- trade-off you are accepting in exchange for permanent fidelity, and it
-- needs to be a deliberate architectural decision, not something discovered
-- when the table becomes the largest thing in the lakehouse.
--
-- This file shows the explicit table DDL (rather than relying on the
-- @dlt.table decorator's default table creation) so the partitioning
-- strategy is visible and intentional from the start.


-- ============================================================
-- Table DDL with a generated partition column
--
-- commit_date is GENERATED ALWAYS AS a derived DATE from _commit_timestamp.
-- Generated columns are computed automatically on write and used for
-- partition pruning by the query optimiser — you never populate this
-- column yourself.
-- ============================================================
CREATE TABLE IF NOT EXISTS your_catalog.your_schema.customer_history (
    customer_id          BIGINT,
    name                 STRING,
    segment              STRING,
    internal_note        STRING,
    _change_type          STRING,
    _commit_version        BIGINT,
    _commit_timestamp      TIMESTAMP,
    captured_at            TIMESTAMP,
    commit_date            DATE GENERATED ALWAYS AS (CAST(_commit_timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (commit_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'false',
    'delta.appendOnly' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);


-- ============================================================
-- Why DATE, not a coarser or finer grain
--
-- Daily partitions are the right default for most history tables, because
-- the overwhelming majority of audit and compliance queries against a
-- table like this are bounded by a date range ("show me everything that
-- happened to this customer between these two dates"), and daily partition
-- pruning keeps that query fast without creating an excessive number of
-- small files for moderate write volumes.
--
-- For genuinely high-write-volume source tables (tens of millions of
-- changes per day), consider monthly partitions instead — too many small
-- daily partitions becomes its own performance problem. For low-write-volume
-- tables, daily is still fine; Delta handles sparse partitions well.
-- ============================================================


-- ============================================================
-- Example time-bounded audit query, using partition pruning
-- ============================================================
SELECT customer_id, name, segment, _change_type, _commit_timestamp
FROM your_catalog.your_schema.customer_history
WHERE commit_date BETWEEN '2026-01-01' AND '2026-01-31'
  AND customer_id = 2001
ORDER BY _commit_version;


-- ============================================================
-- A note on retention — this is an architecture decision, not a query
--
-- "Capture everything forever" is the right default for tables under a
-- genuine audit or compliance obligation. It is the wrong default to apply
-- blanket-style across every table in a lakehouse. Before building this
-- pattern against a new table, answer explicitly:
--
--   1. What specific obligation (regulatory, contractual, internal policy)
--      justifies permanent retention for THIS table?
--   2. What is the expected write volume, and therefore the expected
--      storage growth per year?
--   3. Who reviews that decision, and on what cadence?
--
-- Write the answer down next to the pipeline definition. A history table
-- with no documented reason for existing is itself a governance gap —
-- the same category of problem this pattern exists to prevent.
-- ============================================================
