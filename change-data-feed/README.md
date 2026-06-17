# Change Data Feed — Delta Lake Real Patterns based on Production Implementations

## Overview

Change Data Feed (CDF) is one of the most powerful features in Delta Lake — and one of the most reliably misunderstood in production. The documentation makes it look straightforward. In practice, it breaks in six distinct ways, and most of them are silent until something downstream stops working.

This folder contains the notebooks and recovery patterns from the article. The notebooks are what I used to develop and test the patterns. The `patterns/` files are production-ready implementations you can drop into your pipelines.

---

## Contents

```
change-data-feed/
├── README.md                                    ← This file
├── notebooks/
│   ├── Change_Data_Feed_Demo.ipynb              ← CDF fundamentals and setup
│   └── cdf_failover_scenarios                   ← All six failure scenarios with recovery
└── patterns/
    │
    │   ── INITIAL SETUP (run once, in this order) ──────────────────────────
    ├── 00_setup.py                              ← Step 1: catalog, Volume, CDF enable
    ├── 07_streaming_cdf_consumer.py             ← Step 2: sink schema, checkpoint, consumer
    │
    │   ── EACH BATCH (run in this order on every pipeline run) ─────────────
    ├── 06_cdf_health_check.py                   ← Step 3: pre-batch health check
    ├── 02_schema_evolution_defensive_read.py    ← Step 4: defensive CDF read
    ├── 05_out_of_order_idempotent_merge.sql     ← Step 5: idempotent MERGE write
    │
    │   ── RECOVERY (run if the corresponding error occurs) ──────────────────
    ├── 01_vacuum_recovery.py                    ← Pattern 1: VACUUM removes history
    ├── 03_checkpoint_corruption_recovery.py     ← Pattern 3: Checkpoint corruption
    └── 04_cdf_disabled_recovery.sql             ← Pattern 4: CDF disabled mid-pipeline
```

> **Note on file numbering:** Files `01`–`06` are numbered to match the six failure patterns described in the article. The run order for setup and batch execution differs from the file numbering — see the Recommended production setup section below.

---

## Prerequisites

- Azure Databricks Runtime **13.3 LTS** or higher
- Delta Lake **3.0+**
- Unity Catalog enabled on your workspace
- A catalog and schema you have `CREATE` privilege on

Run `00_setup.py` first — it handles all prerequisite steps automatically:

- Creates the Unity Catalog Volume for checkpoint storage
- Enables CDF on your source table
- Records the baseline version to use as your consumer's starting point

If you prefer to set up manually:

```sql
ALTER TABLE catalog.schema.your_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

> **Note:** CDF is not retroactive. You cannot read changes from before CDF was enabled. If enabling on an existing table, read a full snapshot first and use that as your consumer baseline.

---

## Start here

### Step 1 — `00_setup.py` — Configuration and prerequisites

**File:** `patterns/00_setup.py`

Run this before any other pattern file. It sets the shared configuration (catalog, schema, table names, checkpoint paths) used across all patterns, creates the Unity Catalog Volume for checkpoint storage, enables CDF on your source table, and records the baseline version your consumers should start from.

Replace the placeholder values in the `CONFIG` section at the top of the file before running:

```python
catalog      = "your_catalog"
schema       = "your_schema"
source_table = f"{catalog}.{schema}.your_source_table"
sink_table   = f"{catalog}.{schema}.your_sink_table"
```

---

### Step 2 — `07_streaming_cdf_consumer.py` — Correct production consumer setup

**File:** `patterns/07_streaming_cdf_consumer.py`

Run this second, after `00_setup.py`. This is the prevention pattern — the correct way to set up a CDF streaming consumer before anything goes wrong. Getting this right eliminates the conditions that cause Pattern 3 (checkpoint corruption) and reduces the risk of Pattern 1 (VACUUM lag).

Four decisions are explained in detail in the file header:

| Decision              | Correct                              | Wrong                           |
| --------------------- | ------------------------------------ | ------------------------------- |
| Checkpoint location   | `/Volumes/catalog/schema/vol/path`   | `/tmp/`, DBFS, cluster-local    |
| Trigger on Serverless | `trigger(availableNow=True)`         | `trigger(processingTime=...)`   |
| Checkpoint versioning | `stream_v1`, `stream_v2` (versioned) | Reusing or deleting checkpoints |
| Sink schema           | Includes `_commit_version` column    | No version tracking             |

For scheduled micro-batch processing on Databricks Serverless, `trigger(availableNow=True)` is the correct trigger. Schedule the job via Databricks Jobs to run at your required frequency — each run picks up all changes since the previous run via the checkpoint.

---

## The Six Failure Patterns

### 1. VACUUM removes change history

**File:** `patterns/01_vacuum_recovery.py`

VACUUM removes the `_change_data/` files that back CDF reads. If your pipeline falls behind by more than the VACUUM retention window (default 7 days), those versions are permanently unavailable.

**Error:** `VersionNotFoundException: Cannot find version X of table`  
**Recovery:** Full snapshot fallback — read current table state as baseline, reset watermark to current version + 1.

---

### 2. Schema evolution breaks downstream consumers

**File:** `patterns/02_schema_evolution_defensive_read.py`

An upstream `ALTER TABLE ADD COLUMNS` causes your CDF consumer to encounter columns it was not built to handle. Depending on the implementation, this either throws a schema mismatch error or silently drops new data.

**Error:** `AnalysisException` or silent data loss  
**Recovery:** Defensive schema projection — select expected columns explicitly, fill missing ones with NULL, log new upstream columns.

---

### 3. Streaming checkpoint corruption

**File:** `patterns/03_checkpoint_corruption_recovery.py`

If a checkpoint directory is corrupted, deleted, or inaccessible, the stream cannot resume. Checkpoints stored in `/tmp/` or cluster-local paths are ephemeral on serverless compute.

**Error:** Stream restarts from scratch — duplicate processing or data loss  
**Recovery:** Query sink for highest `_commit_version` processed, restart from there with a new checkpoint path in a Unity Catalog Volume.

---

### 4. CDF disabled mid-pipeline

**File:** `patterns/04_cdf_disabled_recovery.sql`

Someone runs `ALTER TABLE UNSET TBLPROPERTIES ('delta.enableChangeDataFeed')`. The pipeline doesn't fail immediately — it fails on the next run, with a gap of invisible changes in the history.

**Error:** `DeltaIllegalStateException: Change data not recorded for version X`  
**Recovery:** Re-enable CDF, use `DESCRIBE HISTORY` to identify the gap window, perform snapshot diff for any UPDATE/DELETE operations during the disabled period.

---

### 5. Out-of-order data

**File:** `patterns/05_out_of_order_idempotent_merge.sql`

With concurrent writers and retry logic, lower-version (stale) records can arrive after higher-version (current) records, overwriting the correct state in your sink.

**Error:** Silent regression — rows occasionally show stale values  
**Recovery:** Idempotent MERGE with a version guard (`source._commit_version > target.last_commit_version`). Store `_commit_version` in the sink table on every write.

---

### 6. No health checks

**File:** `patterns/06_cdf_health_check.py`

Processing lag creeping toward the VACUUM window, disabled CDF, version gaps — none of these surface immediately. Without proactive detection, you find out from a downstream data quality complaint.

**Error:** Discovered three weeks later  
**Recovery:** Run `check_cdf_health()` before each batch. Validates CDF status, version availability, and lag. Stop the pipeline visibly if unhealthy.

---

## Recommended production setup

Use all patterns together for a complete, resilient CDF pipeline:

```
INITIAL SETUP (run once)
    ├── 00_setup.py                              Step 1: Create Volume, enable CDF, record baseline
    └── 07_streaming_cdf_consumer.py             Step 2: Correct checkpoint path, trigger, sink schema

EACH BATCH (run in this order)
    └── check_cdf_health()                       ← 06_cdf_health_check.py     Step 3: pre-flight guard
            │ healthy
            ▼
        safe_cdf_read()                          ← 02_schema_evolution_defensive_read.py   Step 4
            │
            ▼
        MERGE with version guard                 ← 05_out_of_order_idempotent_merge.sql    Step 5
            └── _commit_version stored on sink

RECOVERY (if needed)
    ├── VACUUM swept versions    → 01_vacuum_recovery.py
    ├── Checkpoint lost          → 03_checkpoint_corruption_recovery.py
    └── CDF disabled gap         → 04_cdf_disabled_recovery.sql
```

---

## Notebooks

### `cdf_failover_scenarios`

**Path:** `notebooks/cdf_failover_scenarios`

Walks through all six failure scenarios with runnable code. Each scenario follows the same structure: a header cell describing the problem, one or two cells that create the exact failure condition, and a recovery cell. Uses a dedicated `cdf_failover_demo` table for all destructive operations (VACUUM, schema changes, CDF disable) so the production `cdf_demo_customers` source table is never affected.

**Run order:**

1. Run **cell 2 (Setup)** first — defines all shared variables and creates the UC Volume. Every other cell depends on this.
2. Work through **scenarios in sequence** — do not skip ahead. Later scenarios use variables (`current_version`, `post_enable_version`) set by earlier recovery cells.
3. Run **cell 29 (Cleanup)** at the end to stop streams and remove checkpoint directories.

**Scenario map:**

| Scenario | Cells | Creates the issue | Recovery | Pattern file |
| --- | --- | --- | --- | --- |
| 1 — VACUUM removes history | 4–8 | Generates 5 versions via UPDATEs then VACUUMs with 0h retention | Full snapshot fallback, reset watermark to `current_version + 1` | `01_vacuum_recovery.py` |
| 2 — Schema evolution | 9–12 | `ALTER TABLE ADD COLUMNS (priority STRING)` | `safe_cdf_read()` with defensive projection — NULLs missing columns, logs new ones | `02_schema_evolution_defensive_read.py` |
| 3 — Checkpoint corruption | 13–16 | Starts streaming consumer, then deletes the checkpoint directory | Query sink for `MAX(_commit_version)`, restart stream from that version with a new versioned checkpoint path | `03_checkpoint_corruption_recovery.py` |
| 4 — CDF disabled mid-pipeline | 17–20 | `UNSET TBLPROPERTIES`, writes changes while CDF is off | Re-enable CDF, use `DESCRIBE HISTORY` to surface the gap window | `04_cdf_disabled_recovery.sql` |
| 5 — Out-of-order data | 21–24 | Creates events across multiple versions, delivers batches in reverse order | Idempotent MERGE with `_commit_version > last_commit_version` guard — proves replay produces no duplicates | `05_out_of_order_idempotent_merge.sql` |
| 6 — Health check | 25–26 | — (monitoring only) | `check_cdf_health()` — validates CDF enabled, version availability, and lag before any batch runs | `06_cdf_health_check.py` |

**Relationship to the pattern files:** Each recovery cell in the notebook is a simplified inline version of the corresponding pattern file. The notebook is the learning environment — run it to see the error and understand the recovery. The pattern file is the production-ready extraction, parameterised as a reusable function with typed signatures and full error handling, ready to drop into a pipeline.

---

### `Change_Data_Feed_Demo.ipynb`

Covers CDF fundamentals — enabling CDF, reading changes using `readChangeFeed`, version-based and timestamp-based reads, streaming CDF, filtering by change type, and common use cases including SCD Type 2 and incremental ETL.

> **Note:** `cdf_failover_scenarios` references this notebook as a prerequisite for `cdf_demo_customers`, but includes a safety guard (`CREATE VOLUME IF NOT EXISTS`) and can run standalone.

---

## Further reading

- [Delta Lake CDF documentation](https://docs.databricks.com/en/delta/delta-change-data-feed.html)
- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Structured Streaming checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)