# CDF History Table — Permanent Change Capture

## Overview

This is a different pattern from [`scd-type-2`](../scd-type-2/), not a smaller
version of it. SCD Type 2 answers *what was this entity's state at this point
in time* — it's built for point-in-time joins and only versions on a tracked
attribute change. A CDF history table answers a different question: *what
exactly happened, and when* — every event, including no-op writes to
non-tracked columns, with full pre-image and post-image fidelity.

It is also the long-term fix for the first failure pattern in my [Change
Data Feed article](../change-data-feed/) — VACUUM removing history before a
lagging pipeline processes it. A permanent history table never depends on
the source table's retention window in the first place.

---

## Contents

```
cdf-history-table/
├── README.md                                    ← This file
├── notebooks/
│   └── cdf_history_table_demo                   ← SCD2-vs-history-table comparison, runnable
└── patterns/
    ├── 00_setup.py                               ← Start here: seeds source data
    ├── 01_append_only_history_capture.py         ← The core pipeline pattern
    ├── 02_schema_evolution_history_table.py      ← Schema widening variant
    └── 03_partitioning_and_retention.sql         ← Partitioning DDL + retention guidance
```

---

## Prerequisites

- Azure Databricks Runtime **13.3 LTS** or higher (for the notebook and setup)
- Lakeflow Declarative Pipelines enabled on your workspace (for patterns 01 and 02)
- A catalog and schema you have `CREATE` privilege on

Run `patterns/00_setup.py` first. It seeds a `customer` source table with
CDF enabled and a deliberate edge case: a write that touches only a
non-business-attribute column (`internal_note`), specifically to make the
SCD2-vs-history-table difference visible rather than theoretical.

---

## A note before you run anything

`@dp.table` pipeline definitions (patterns 01 and 02) only execute inside
the Lakeflow Declarative Pipeline runtime — they cannot be called directly
in a notebook cell. Pattern 00 (setup) and pattern 03 (SQL DDL) run fine
anywhere.

To run patterns 01 or 02: create a Lakeflow pipeline (sidebar **Pipelines →
Create pipeline**), point its source code at the relevant file, set the
target catalog/schema, and run it. Querying the resulting table afterward is
just SQL.

---

## The Pattern

### `01_append_only_history_capture.py` — The core pipeline
**File:** `patterns/01_append_only_history_capture.py`

A plain streaming table definition (not `apply_changes`) that captures
every CDF event with no filter on `_change_type`. Three deliberate
decisions:

- `delta.appendOnly = true` — enforced at the table level. Delta rejects
  any UPDATE or DELETE against the table, not just by convention.
- `delta.enableChangeDataFeed = false` — this table is a terminus, not a
  source for further CDF chaining.
- No `_change_type` filter — both `update_preimage` and `update_postimage`
  are captured for every update, which is the opposite of the SCD2 filter.

---

### `02_schema_evolution_history_table.py` — Schema widening
**File:** `patterns/02_schema_evolution_history_table.py`

Adds `mergeSchema=true` so new source columns widen the history table
automatically. Historical rows show NULL for columns that didn't exist
yet at capture time — that NULL is correct and should never be backfilled.

---

### `03_partitioning_and_retention.sql` — Scaling the pattern
**File:** `patterns/03_partitioning_and_retention.sql`

Explicit table DDL with a `GENERATED ALWAYS AS` partition column derived
from `_commit_timestamp`, plus guidance on the retention decision a history
table requires: this pattern should be applied deliberately to tables with
a genuine audit obligation, not blanket-style across a lakehouse.

**Note:** Pattern 03 DDL is for direct writes (streaming or batch) where you
control table creation yourself. It is NOT compatible with `@dp.table` in a
pipeline — the pipeline runtime owns table creation and will not adopt a
pre-existing managed table.

---

## Recommended production setup

```
SETUP (once)
    └── 00_setup.py                               Seed source table with CDF enabled

DEPLOY AS A LAKEFLOW PIPELINE (pick one)
    ├── 01_append_only_history_capture.py         Core append-only capture
    └── 02_schema_evolution_history_table.py      Use instead of 01 if source schema may evolve

PRE-CREATE TARGET TABLE (optional, for partitioning)
    └── 03_partitioning_and_retention.sql         Apply BEFORE deploying the pipeline, OR
                                                    use with a direct streaming write instead
                                                    of a pipeline (see notebook Step 7)
```

**Important:** Patterns 01 and 02 are alternatives, not layers. Pick one based
on whether your source table's schema is stable or expected to evolve. If you
use pattern 03's DDL, you must use a direct streaming write rather than a
pipeline — `@dp.table` will not adopt a pre-existing table.

---

## Notebook

### `cdf_history_table_demo`
A self-contained walkthrough with the following steps:

| Step | What it does |
| --- | --- |
| 0 | Config — set catalog, schema, table names |
| 1 | Seed source table with CDF and deliberate edge cases |
| 2 | Read raw CDF and simulate what an SCD2 pipeline drops (compares pre/post images on tracked attributes, not just `_change_type` filtering) |
| 3 | Instructions for deploying the Pattern 01 pipeline |
| 4 | Verify all events captured in the history table |
| 5 | Confirm `delta.appendOnly` rejects a DELETE against the history table |
| 6 | Test schema evolution (Pattern 02) — ALTER source table, re-run pipeline, verify old rows show NULL for new column |
| 7 | Test partitioning (Pattern 03) — pre-create partitioned table with generated `commit_date` column, populate via direct streaming write, verify partition pruning in EXPLAIN |
| Cleanup | Drop demo tables, schema, and streaming checkpoint |

**Run scenarios:**
- Steps 0–5: Core pattern demonstration (requires deploying Pattern 01 pipeline between steps 2 and 4)
- Steps 0–2, then Step 6: Schema evolution (requires deploying Pattern 02 pipeline)
- Steps 0–2, Step 6, then Step 7: Partitioning (no pipeline needed — uses direct streaming write)

---

## Further reading

- [Lakeflow Declarative Pipelines documentation](https://docs.databricks.com/en/dlt/index.html)
- [Delta Lake table properties reference](https://docs.databricks.com/en/delta/table-properties.html) — for `delta.appendOnly` and related properties
- **Article:** [Building a CDF History Table](https://www.cypheragency.com.au/resources/cdf-history-table-databricks-vacuum-retention) — cypheragency.com.au
- **Related:** [Delta Lake Change Data Feed in Production](../change-data-feed/) — the original six failure patterns, including the VACUUM issue this pattern prevents
- **Related:** [SCD Type 2 on Databricks](../scd-type-2/) — the companion pattern this article is explicitly distinguished from
