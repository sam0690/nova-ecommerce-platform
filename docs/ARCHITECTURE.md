# Nova Analytics — Project Documentation

A batch analytics pipeline that takes daily CSV order files, validates and loads them into
PostgreSQL, models them with dbt, and serves them through a self-hosted Lightdash dashboard.

Built as a learn-by-doing data engineering project. This document explains **what exists and why**;
`docs/RUNBOOK.md` explains **how to run it**.

---

## 1. What the system does

Eight weeks of e-commerce order data (2026-07-01 → 2026-08-25) flows through five stages:

```
 data/orders_YYYY-MM-DD.csv
        │
        │  1. EXTRACT + VALIDATE  (pydantic; bad rows → dead-letter queue)
        ▼
 shop.stg_orders            ← transient landing zone, truncated every run
        │
        │  2. LOAD  (INSERT ... ON CONFLICT DO NOTHING)
        ▼
 shop.fact_orders           ← durable fact table, 157,803 rows / 68 days
        │
        │  3. TRANSFORM  (dbt: sources → models, 8 data tests)
        ▼
 analytics.fct_orders       ← view
 analytics.dim_customers    ← view
        │
        │  4. SEMANTIC LAYER  (metrics + dimensions declared in schema.yml)
        ▼
 Lightdash "kpi_dashboard"  ← 9 charts, no hand-written SQL
```

### Current volumes

| Measure | Value |
|---|---|
| Fact rows | 124,322 |
| Days covered | 56 (2026-07-01 → 2026-08-25) |
| Staging rows after a run | ~1,600–3,000 (one day only) |
| dbt models / tests | 2 / 8 |
| Revenue (completed orders) | 23,182,141.71 |
| Gross amount (all statuses) | 92,751,995.94 |

---

## 2. Component reference

### 2.1 Data generation — `scripts/generte_orders_data.py`

Produces 56 daily CSV files of synthetic orders.

Two design decisions matter:

- **The catalog mirrors `sql/ddl/002_seed.sql` exactly** — same ten product ids, names, categories
  and prices. An earlier version invented its own catalog, which meant `product_id = 3` was
  "USB-C Hub / Electronics" in the CSVs but "27in 1440p Monitor / displays" in the database. Any
  category chart would have split a single product across two categories.
- **Volume varies by weekday plus an upward drift.** A per-weekday base (quiet midweek ~1,600,
  weekend peak ~2,500) is multiplied by a 0.5%/day compounding drift and ±8% noise. Uniform-random
  volume produces a flat, uninformative trend line. The term for this is
  **synthetic data with seasonality**.

`random.seed(42)` makes the output deterministic — regenerating reproduces byte-identical files,
which is why the CSVs are gitignored rather than committed.

About 0.3% of rows are deliberately corrupted (negative amount, unknown status, unparseable date)
to exercise the validation path without crossing the 1% abort threshold.

### 2.2 Ingestion — `ingestion/csv_ingestion/`

| File | Responsibility |
|---|---|
| `etl.py` | Orchestrates the run; owns the transaction boundary |
| `csv_loader.py` | Reads CSV, validates rows, writes rejects to the DLQ, enforces the threshold |
| `validated_reader.py` | Reads validated rows back from the temp JSONL file |
| `batching.py` | Groups an iterator into fixed-size batches |
| `staging.py` | `COPY` a batch into `shop.stg_orders` |
| `staging_loader.py` | Drives batching + copy, returns a count |
| `fact_loader.py` | `INSERT ... SELECT ... ON CONFLICT DO NOTHING` into the fact table |
| `db.py` | Builds a psycopg connection from `.env` |

**Validation** (`src/models.py`) is a pydantic `Order` model: `quantity > 0`, `amount >= 0`,
`unit_price >= 0`, and a `field_validator` restricting `status` to
{pending, completed, cancelled, refunded}.

**Dead-letter queue.** Invalid rows are not dropped and do not stop the run — they are appended to
`data/dead_letter_YYYY-MM-DD.jsonl` with the reason. This keeps a bad row inspectable instead of
invisible. If more than **1%** of rows in a file are rejected, `check_rejection_rate` aborts the run
**before any database write** — the assumption being that a file that bad is a broken upstream
export, not a few malformed records.

**Batched `COPY`.** Rows load 1,000 at a time via `COPY`, which is substantially faster than
row-by-row `INSERT` because it avoids per-statement parse and round-trip overhead.

### 2.3 Two properties the pipeline guarantees

**Idempotency** — rerunning a date that has already been loaded produces no change. The fact load
ends in `ON CONFLICT (order_id) DO NOTHING`, so duplicate order ids are discarded rather than
inserted or updated. This is what makes retries and backfills safe: a job that fails halfway can
simply be run again.

**Atomicity** — the truncate, the staging load and the fact load all happen inside a single
`with get_connection() as conn:` block with one explicit `conn.commit()` at the end. If any step
raises, the whole transaction rolls back and staging is left as it was. `tests/test_atomicity.py`
asserts this directly.

The `TRUNCATE shop.stg_orders` sits **inside** that block for exactly this reason. Staging is a
landing zone for the current batch, not an archive — before the truncate was added, it accumulated
every batch ever loaded and the fact load re-scanned all of it on every run.

### 2.4 Schema

`sql/ddl/001_shop.sql` and `002_seed.sql` create and seed the source tables
(`customers`, `products`, `orders`, `order_items`). `sql/migrations/003_orders_etl.sql` adds the
ETL's two tables, and `004_refunds.sql` adds the refund event table.

| Table | Purpose |
|---|---|
| `shop.stg_orders` | Transient staging; all columns text; cleared each run |
| `shop.fact_orders` | Durable fact; typed; PK `order_id`; CHECKs on amount and quantity |
| `shop.refunds` | Refund events; PK `order_id` (one refund per order); `refund_date` separate from `order_date` |

`refunds` exists because a refund is an event with its own date, not a status flag on the order.
Storing it as a flag meant a September refund silently reduced July's revenue — **restatement**,
happening by accident. Its rows come from `scripts/backfill_refunds.py`, not from the CSVs.

`fact_orders` also carries `loaded_at` and `source_file` — **lineage columns** that record when a
row arrived and which file it came from. They make "where did this row come from?" answerable
without guessing.

### 2.5 Transformation — `nova_analytics/`

dbt builds into its own **`analytics`** schema, deliberately separate from `shop`. Keeping dbt's
view `fct_orders` in the same schema as the ETL's table `fact_orders` would leave two objects one
character apart.

**`models/sources.yml`** declares `shop` as a source with tables `fact_orders`, `customers`,
`products`. Models then reference `{{ source('shop', 'fact_orders') }}` rather than a hardcoded
name, which gives dbt the dependency graph — lineage, downstream selection (`--select source:shop+`),
and a place to attach freshness checks.

**Models:**

| Model | What it is |
|---|---|
| `fct_orders` | View over the source fact table — the measures |
| `dim_customers` | View over the customers source, `id AS customer_id` — the attributes |

Facts hold what you measure, dimensions hold what you group by; joined on a shared key, that is a
**star schema**. Aliasing `id` to `customer_id` means the join key is spelled identically on both
sides, so `USING (customer_id)` works.

Both are **views** (dbt's default materialization) — no data is copied, so a rebuild over 124k rows
takes under a second. At larger volumes the next step would be `materialized: incremental`.

**Tests** (`models/marts/schema.yml`), 8 in total:

| Test | Column | Guards against |
|---|---|---|
| `unique`, `not_null` | `fct_orders.order_id` | duplicate or missing keys |
| `not_null` | `amount`, `order_date` | missing measures |
| `accepted_values` | `status` | unexpected status values |
| `relationships` | `customer_id` → `customers.id` | orphaned foreign keys |
| `unique`, `not_null` | `dim_customers.customer_id` | join fan-out silently multiplying revenue |

Data tests assert things about **rows**, not code. They are the layer that stops a bad load from
quietly becoming a wrong number on a dashboard — a second line of defence behind pydantic, which
guards the entry point.

### 2.6 Semantic layer

Metrics and dimensions are declared in `schema.yml` under `meta:` and read by Lightdash:

| Metric | Model | Definition | Note |
|---|---|---|---|
| `net_revenue` | `fct_revenue_events` | `sum(signed_amount)` | **official revenue by Event Date (as booked)**; by Order Date it is restated |
| `gross_revenue` | `fct_revenue_events` | `sum(signed_amount)` filtered to `event_type = booking` | before refunds |
| `booked_orders` | `fct_revenue_events` | `count_distinct(order_id)` filtered to bookings | denominator for revenue-derived metrics |
| `avg_order_value` | `fct_revenue_events` | `${gross_revenue} / NULLIF(${booked_orders}, 0)` | model-level |
| `net_revenue_by_attribution` | `revenue_by_month_attribution` | `sum(net_revenue)` | long format, for the as-booked vs restated chart |
| `gross_amount` | `fct_orders` | `sum(amount)`, unfiltered | **not revenue** — all statuses, for by-status comparison |
| `total_orders` | `fct_orders` | `count_distinct(order_id)` | all statuses |
| `total_quantity` | `fct_orders` | `sum(quantity)` | |

**`total_revenue` was removed in Step 33**, along with the `avg_order_value` derived from it. It
was `sum(amount)` filtered to `status = 'completed'`, which made it the *restated* figure by
accident — a refunded order stops being `completed`, so a September refund silently reduced July.
The policy is now stated on `fct_revenue_events` in `schema.yml`: official revenue is net revenue
by **Event Date**, so closed months stay closed. Restated is the same metric by Order Date, correct
for cohort work, never the headline.

This is the point of the whole exercise. **Revenue is defined once.** Rather than every analyst
remembering which filter to apply, the definition is welded into the metric, so no chart
can quietly disagree about what revenue means. `gross_amount` exists so the excluded value is still
answerable — "how much are we losing to cancellations and refunds?" is 92.7M vs 23.2M.

Dimensions are declared with `meta.dimension`: `order_date` as `date` (which is what unlocks
day/week/month granularity in charts), plus `status`, `category`, `product_name`, `customer_id`, and
`name`/`country` on the dimension table. A `meta.joins` entry joins `dim_customers` on
`customer_id`, so customer names are groupable without anyone writing SQL.

Two behaviours worth knowing: dbt treats `meta:` as an **opaque passthrough** — it compiles fine
even if the contents are wrong, and only the BI tool validates them. And a dbt model is
**invisible to Lightdash until it has a YAML entry**, regardless of whether the view exists in the
database.

### 2.7 Presentation — `docker/lightdash-compose.yml`

Three containers: Lightdash (pinned to `2.17.0`), its own metadata Postgres, and MinIO for
S3-compatible storage, which Lightdash 2.x requires.

The stack joins the warehouse's existing Docker network as an **external** network, so Lightdash
reaches Postgres at `postgres:5432` — container-to-container, never crossing the host, which means
a host firewall or VPN cannot break it.

**`kpi_dashboard`** contains 9 charts, none of which required SQL:

| Row | Charts |
|---|---|
| KPIs | Total Revenue · Total Orders · Avg Order Value · Items Sold |
| Trend | `total_revenue` by `order_date` (week) |
| Performance | `total_revenue` by `category` · Top 10 `product_name` |
| Operations | `total_orders` by `status` · Top 10 customers by name |

---

## 3. Verified results

| Check | Result |
|---|---|
| Fact rows / days | 124,322 / 56 |
| Staging after a run | one day's rows, not 56 |
| Rerun a loaded date | fact count unchanged (idempotent) |
| `dbt build` | PASS=10 (2 models, 8 tests) |
| Revenue by category | 5 categories, no Electronics/peripherals split |
| Dashboard KPIs vs SQL | match exactly |

Revenue by category (completed orders):

```
furniture     9,848,766.00
displays      8,297,587.78
audio         2,728,395.20
peripherals   1,690,351.50
accessories     617,041.23
```

Five categories with no phantom `Electronics` is the end-to-end proof that the catalog fix holds all
the way from CSV generation to a rendered chart.

---

## 4. Known limitations

Honest scope boundaries rather than hidden defects:

- **No orchestration.** Backfills are a bash `for` loop. No retries, no scheduling, no dependency
  graph, no alerting. This is the pain that justifies Airflow or Dagster.
- **Views, not incremental models.** `fct_orders` recomputes on every query. Fine at 124k rows,
  wrong at 124M.
- **No slowly-changing dimensions.** If a customer changes country, history is overwritten.
  `dbt snapshot` is the tool for this.
- **Uniform customer distribution.** The generator draws customers from `randint(1, 10)`, so all
  ten have ~12.5k orders. Real data has a long tail; the "Top Customers" chart is therefore flat by
  construction, not by finding.
- **The `relationships` test passes by coincidence** — the generator's customer range happens to
  match the ten seeded customers. Nothing enforces that at the database level.
- **Default credentials.** `POSTGRES_PASSWORD=changeme`; MinIO uses `minioadmin`. Acceptable for a
  local-only stack, unacceptable if anything is ever exposed.
- **The API source reads the same CSVs.** `ingestion/rest_api/` is live as of Step 19, but the
  endpoint it pulls from is `scripts/fake_orders_api.py` serving `data/orders_*.csv` over HTTP.
  It exercises real pagination against a real socket; it is not a real vendor.

---

## 5. Vocabulary

| Term | Meaning |
|---|---|
| **Backfill** | Running a pipeline across a range of past dates |
| **Idempotency** | Rerunning produces the same result; never duplicates |
| **Staging** | Transient landing zone for the current batch, cleared each run |
| **DLQ** | Dead-letter queue — rejected rows kept for inspection, not dropped |
| **Star schema** | Fact table of measures + dimension tables of attributes |
| **Source (dbt)** | A declared raw table, giving dbt lineage and testability |
| **Materialization** | How dbt persists a model — view, table, or incremental |
| **Semantic layer** | Metrics defined once in one place, reused by every chart |
| **Lineage columns** | `loaded_at` / `source_file` — where a row came from and when |
