# SCD Type 2 — Manual MERGE vs apply_changes

## Overview

This repo contains the companion code for the SCD Type 2 article on cypheragency.com.au. It demonstrates why declarative CDC processing (`dp.create_auto_cdc_flow`) replaces hundreds of lines of hand-rolled MERGE logic — and the subtle gotchas you'll hit along the way.

The patterns progress from a manual MERGE (pattern 01) through a basic declarative pipeline (02), to a production-hardened version with tie-resistant composite sequencing (03) and an exhaustive schema drift guard (04). Each pattern uses the same deliberately awkward seed data — same-batch ties, out-of-order arrivals, and deletes — so you can compare outputs directly.

Start with `patterns/00_setup.py` to seed the bronze table, then work through the patterns in order. The two notebooks (`scd_type_2_manual_merge_vs_apply_changes` and `scd_type_2_gotchas`) provide guided walkthroughs if you prefer a narrative format.

---

## Contents

```
scd-type-2/
├── README.md                                    ← This file
├── notebooks/
│   ├── scd_type_2_manual_merge_vs_apply_changes.py                       ← Manual MERGE vs apply_changes, side by side
│   └── scd_type_2_gotchas.py                    ← The four gotchas, reproduced
└── patterns/
    ├── 00_setup.py                               ← Start here: seeds the bronze table
    ├── 01_manual_merge_scd2.sql                  ← The hand-rolled MERGE implementation
    ├── 02_apply_changes_basic.py                 ← Basic apply_changes pipeline
    ├── 03_apply_changes_sequence_tie_fix.py      ← Tie-resistant composite sequence_by
    └── 04_schema_validation_allowlist.py         ← Exhaustive allow-list schema check
```

---

## Prerequisites

- Azure Databricks Runtime **13.3 LTS** or higher
- Lakeflow Declarative Pipelines (formerly Delta Live Tables) enabled on your workspace
- A catalog and schema you have `CREATE` privilege on

Run `patterns/00_setup.py` first — it creates the schema if needed and seeds a
bronze `customer_updates_raw` table with deliberately awkward CDC data:

| customer_id | Scenario |
|---|---|
| 1001 | Clean insert then update — the easy case |
| 1002 | Two same-batch updates with an **identical** sequence timestamp — the tie case |
| 1003 | A correction arriving **after** a later row, with an **earlier** business timestamp — the out-of-order case |
| 1004 | Insert followed by a delete |

Update the `catalog`/`schema` placeholder values at the top of every pattern
file before running.

---

## A note before you run anything

`dp.create_auto_cdc_flow()` (patterns 02 and 03) only executes inside the
Lakeflow Declarative Pipeline runtime — it cannot be called directly in a
regular notebook cell like a normal function. Pattern 01 (the manual MERGE)
and pattern 04 (schema validation) run fine in any regular notebook or script.

To run patterns 02 or 03: create a Lakeflow pipeline in your workspace
(**Workflows → Pipelines → Create pipeline**), point its source code at the
relevant file, set the target catalog/schema, and run it. Querying the
resulting Delta table afterward is just SQL — that part works anywhere.

The two notebooks in this folder are written with this constraint in mind:
each one tells you exactly which cells run directly and which require a
pipeline to be deployed first.

---

## The Two Approaches

### `01_manual_merge_scd2.sql` — Manual MERGE
**File:** `patterns/01_manual_merge_scd2.sql`

The hand-rolled, two-step MERGE pattern most production Databricks
environments built before `apply_changes` matured. Works for the simple
case. Has no sequence comparison logic at all, so it gets two specific
scenarios wrong:

- **Same-batch duplicates** (customer_id 1002): produces two rows both
  marked `is_current = true`
- **Out-of-order arrivals** (customer_id 1003): has no concept of "true"
  order versus arrival order, so a late-arriving correction with an earlier
  business timestamp is processed as if it were the newest change

A diagnostic query at the bottom of the file checks for the first bug
directly.

---

### `02_apply_changes_basic.py` — Declarative SCD2
**File:** `patterns/02_apply_changes_basic.py`

The twelve-line declarative replacement. Correctly resolves the
out-of-order case (1003) because `sequence_by` determines true order
regardless of arrival order. Does **not** reliably resolve the sequence-tie
case (1002) — see pattern 03.

---

### `03_apply_changes_sequence_tie_fix.py` — Tie-resistant sequence
**File:** `patterns/03_apply_changes_sequence_tie_fix.py`

Same pipeline, with `sequence_by` changed from a plain timestamp to a
composite `struct(updated_at, _commit_version)`. This is the fix for
customer_id 1002 — the `_commit_version` component only breaks ties where
`updated_at` is identical, so business-time ordering is still the primary
sort.

---

### `04_schema_validation_allowlist.py` — Schema drift guard
**File:** `patterns/04_schema_validation_allowlist.py`

A standalone Python utility, not a pipeline file. Validates a CDC source's
schema against an explicit allow-list of expected attribute columns, rather
than relying on `except_column_list` to catch every unwanted column. Raises
loudly the moment an unreviewed column appears upstream, instead of letting
it silently become a versioned dimension attribute.

---

## Recommended production setup

```
SETUP (once)
    └── 00_setup.py                               Seed bronze table, create schema

VALIDATE BEFORE WIRING A NEW SOURCE
    └── validate_cdc_schema()                      ← 04_schema_validation_allowlist.py
            │ passed
            ▼
DEPLOY AS A LAKEFLOW PIPELINE
    └── apply_changes with composite sequence_by   ← 03_apply_changes_sequence_tie_fix.py
            └── schema_tracking_location configured for future evolution

RECOVERY / ESCAPE HATCH
    └── 01_manual_merge_scd2.sql                   When SCD logic needs business rules
                                                     more complex than attribute comparison,
                                                     or point-in-time historical correction
```

---

## Notebooks

### `scd_type_2_manual_merge_vs_apply_changes.py`
Seeds the bronze table, runs the manual MERGE implementation end to end,
shows the duplicate-current-row bug for customer_id 1002, then walks
through deploying and verifying the `apply_changes` pipeline against the
same data. Ends with a side-by-side summary table.

### `scd_type_2_gotchas.py`
Reproduces all four gotchas from the article in isolation: sequence ties,
an exhaustive except-list failure, a compressed reproduction of the
backfill/incremental drift mechanism, and the schema evolution
configuration needed for a target with existing history.

---

## Further reading

- [Lakeflow Declarative Pipelines — apply_changes API reference](https://docs.databricks.com/en/dlt/python-ref.html)
- [Delta Lake CDF documentation](https://docs.databricks.com/en/delta/delta-change-data-feed.html) — relevant if your CDC source is Delta CDF rather than Debezium/Qlik
- **Article:** [SCD Type 2 on Databricks](https://www.cypheragency.com.au/resources/scd-type-2-databricks-apply-changes-into) — cypheragency.com.au
- **Related:** [Delta Lake Change Data Feed in Production](https://www.cypheragency.com.au/resources/delta-lake-change-data-feed-production-failure-patterns) — the companion article on CDF failure patterns
