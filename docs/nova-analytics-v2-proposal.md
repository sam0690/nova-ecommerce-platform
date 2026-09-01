# Nova Analytics v2 — Proposal

**From working prototype to unattended production platform**

| | |
|---|---|
| **Author** | Sam |
| **Date** | August 2026 |
| **Status** | Proposal — for review |
| **Current system** | Nova Analytics v1 (built, running, documented) |
| **Proposed** | Five phases, sequenced; Phases 1–2 are the core ask |

---

## 1. Summary

Nova Analytics v1 works. It ingests eight weeks of daily order CSVs, validates them,
loads 124,322 rows into PostgreSQL, models them with dbt, and serves nine charts through a
self-hosted Lightdash dashboard with a semantic layer. The numbers on the dashboard match the
numbers in the database.

What v1 does **not** do is run without me.

There is no scheduler. Backfills are a bash `for` loop. If a load fails at 3am, nothing retries it
and nobody is told. If an upstream file arrives late, the dashboard silently shows yesterday's
numbers with today's date on it. There is one data source. There are two known-broken test files
and no CI.

**v1 proved the data model is correct. v2 proves the system can be trusted to run without me
watching it.** That sentence is the whole proposal.

The work is proposed in five phases. Phases 1 and 2 are the substantive ask and deliver a pipeline
that schedules itself, recovers from failure, ingests from two sources, and refuses to publish bad
data. Phases 3–5 are sequenced behind them and can be approved separately or dropped.

---

## 2. Where the system is today

### 2.1 Current architecture

```
 data/orders_YYYY-MM-DD.csv
        │
        │  EXTRACT + VALIDATE   (pydantic; bad rows → dead-letter queue)
        ▼
 shop.stg_orders                ← transient landing zone, truncated every run
        │
        │  LOAD                 (COPY in 1,000-row batches, then
        │                        INSERT ... ON CONFLICT DO NOTHING)
        ▼
 shop.fact_orders               ← durable fact, 124,322 rows / 56 days
        │
        │  TRANSFORM            (dbt: 2 models, 8 data tests)
        ▼
 analytics.fct_orders  ·  analytics.dim_customers
        │
        │  SEMANTIC LAYER       (metrics + dimensions in schema.yml)
        ▼
 Lightdash "kpi_dashboard"      ← 9 charts, no hand-written SQL

              ▲
              │
    ALL OF THIS IS TRIGGERED BY A HUMAN TYPING A COMMAND
```

### 2.2 What v1 already gets right

These are not being rebuilt. They are the foundation v2 stands on.

| Property | How it is achieved |
|---|---|
| **Idempotency** | Fact load ends in `ON CONFLICT (order_id) DO NOTHING`. Rerunning a loaded date changes nothing. |
| **Atomicity** | Truncate, staging load and fact load share one transaction with a single explicit commit. Asserted by `tests/test_atomicity.py`. |
| **Bad rows are kept, not dropped** | Invalid rows go to `data/dead_letter_YYYY-MM-DD.jsonl` with a reason. A >1% rejection rate aborts before any database write. |
| **Lineage columns** | `loaded_at` and `source_file` on every fact row answer "where did this come from?" without guessing. |
| **A single definition of revenue** | `total_revenue` is `sum(amount)` filtered to `status = 'completed'`, declared once in the semantic layer. No chart can quietly disagree. |
| **Load performance** | Batched `COPY` rather than row-by-row `INSERT`. |

### 2.3 Verified v1 results

| Check | Result |
|---|---|
| Fact rows / days covered | 124,322 / 56 |
| Staging rows after a run | one day's rows, not 56 |
| Rerun a loaded date | fact count unchanged |
| `dbt build` | `PASS=10` (2 models, 8 tests) |
| Revenue by category | 5 categories, no phantom `Electronics` split |
| Dashboard KPIs vs raw SQL | exact match |

---

## 3. The gaps v2 addresses

Taken directly from v1's own "Known limitations" section. These are documented scope boundaries,
not discovered defects.

| # | Gap | Consequence today | Phase |
|---|---|---|---|
| G1 | No orchestration | Backfills are a bash loop. No retries, no schedule, no dependency graph, no alerting. | 1 |
| G2 | No failure notification | A 3am failure is discovered whenever someone next looks at a chart. Time-to-detection is effectively unbounded. | 1 |
| G3 | dbt can run on a failed load | Nothing enforces that transform runs only after a successful load. A green dashboard can be built on stale data. | 1 |
| G4 | Secrets in plaintext | `POSTGRES_PASSWORD=changeme`; MinIO on `minioadmin`. | 1 |
| G5 | One data source | `ingestion/rest_api/` was started and abandoned; it does not import. | 2 |
| G6 | No freshness or volume checks | An upstream file that arrives half-empty passes every current test. | 2 |
| G7 | Broken tests, no CI | `tests/test_staging.py` and `tests/test_fact_loader.py` are known-broken. Nothing runs on a pull request. | 2 |
| G8 | Views, not incremental models | `fct_orders` recomputes on every query. Fine at 124k rows, wrong at 124M. | 3 |
| G9 | No slowly-changing dimensions | If a customer changes country, history is overwritten and past revenue silently moves between regions. | 3 |
| G10 | Batch only | Latency floor is one day. | 4 |
| G11 | Local-only storage | No object storage, no open table format. | 5 |

The organising principle behind all of these is the one v1 already states: **silent wrong data is
worse than a loud failure.** Every phase below is measured against whether it converts a silent
failure into a loud one.

---

## 4. Proposed target architecture

End state after Phase 2. Phases 3–5 extend it rather than replace it.

```
   ┌──────────────────┐        ┌──────────────────┐
   │  daily CSV drop  │        │    REST API      │
   └────────┬─────────┘        └────────┬─────────┘
            │                           │
            ▼                           ▼
   ┌─────────────────────────────────────────────┐
   │              AIRFLOW 3.2                    │
   │                                             │
   │  ingest_csv ──┐                             │
   │               ├─→ soda_scan_source  (GATE)  │
   │  ingest_api ──┘          │                  │
   │                          ▼                  │
   │                    load_to_fact             │
   │                          │                  │
   │                   [asset: fact_orders,      │
   │                    partitioned by date]     │
   │                          │                  │
   │                          ▼                  │
   │            Cosmos DbtTaskGroup              │
   │        (one Airflow task per dbt node,      │
   │         source sensors upstream)            │
   │                          │                  │
   │                          ▼                  │
   │                  soda_scan_marts  (GATE)    │
   └──────────────────────────┬──────────────────┘
                              │
                     on failure / deadline
                              ▼
                    ┌──────────────────┐
                    │  alert channel   │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │    Lightdash     │
                    └──────────────────┘
```

Three quality gates, each catching a different class of failure:

1. **pydantic** — row is structurally wrong (already exists)
2. **Soda Core at the source** — the *file* is wrong: late, short, schema-drifted
3. **dbt tests post-transform** — the *result* is wrong: duplicate keys, orphaned FKs, fan-out

---

## 5. Phases

### Phase 1 — Orchestration and reliability *(core ask)*

**Goal:** the pipeline runs on a schedule, recovers from failure on its own, and tells someone when
it cannot.

**Scope**

- Airflow 3.2 deployed via Docker Compose alongside the existing Postgres and Lightdash stack,
  joining the same external Docker network.
- A DAG covering the full path: extract → validate → stage → load → dbt build → publish.
- Task-level retries with exponential backoff. `retries >= 2` on every dbt model task.
- `fact_orders` modelled as an **Airflow asset partitioned by `order_date`**, so downstream
  transform triggers on the specific date partition rather than on any change to the table.
- dbt orchestrated through **Astronomer Cosmos** as a `DbtTaskGroup`, so each dbt model and test is
  its own Airflow task with its own logs and retry behaviour — model-level failure visibility rather
  than one opaque `dbt build` step.
- **Source sensors upstream of Cosmos source nodes** (see §7.2 — this is the piece that stops dbt
  from transforming data that was never loaded).
- Catchup and backfill across the existing 56-day range, replacing the bash loop.
- Deadline alerts for freshness SLA; failure callbacks to an alert channel.
- Airflow Connections and Variables replacing `.env`; `changeme` and `minioadmin` retired.

**Acceptance criteria**

| # | Criterion |
|---|---|
| A1 | A single command backfills 56 days unattended and produces a fact count identical to v1's 124,322. |
| A2 | A deliberately injected mid-run failure (killed connection during the fact load) is retried automatically and succeeds without manual intervention or duplicate rows. |
| A3 | An unrecoverable failure produces an alert within 5 minutes. |
| A4 | dbt does not run when the load task fails. Demonstrated, not asserted. |
| A5 | No credential appears in the repository. |
| A6 | Rerunning any already-loaded date remains a no-op (v1's idempotency guarantee is preserved). |

**Measured before/after**

| Metric | v1 | Phase 1 target |
|---|---|---|
| Backfill 56 days | manual bash loop, human-supervised | one trigger, unattended |
| Time-to-detection of a failed load | unbounded | < 5 min |
| Recovery from a transient failure | manual rerun | automatic |
| Credentials in repo | 2 defaults | 0 |

---

### Phase 2 — Second source and quality gates *(core ask)*

**Goal:** more than one source, and a pipeline that refuses to publish data it does not trust.

**Scope**

- Finish `ingestion/rest_api/` as a real source: pagination, rate limiting, retry with backoff,
  timeout handling, and an **incremental cursor** so each run pulls only what is new.
  This is deliberately hand-written rather than an Airbyte connector (§6.4), and it survives
  Track E's adoption as the reference implementation.
- **Soda Core** checks at the source boundary: row-count delta against the trailing average,
  freshness (has today's file arrived?), schema drift, null rate per column.
- Alerting on DLQ rate crossing a threshold, rather than the current behaviour of aborting silently
  at 1%.
- Repair `tests/test_staging.py` and `tests/test_fact_loader.py`.
- CI on every pull request: lint, unit tests, and `dbt build` against a scratch schema.

**Acceptance criteria**

| # | Criterion |
|---|---|
| B1 | The API source loads into `fact_orders` through the same idempotent path as the CSV source. |
| B2 | Re-running the API extract pulls zero duplicate records (cursor works). |
| B3 | A file with 40% of its normal row count is blocked before the fact load, with an alert naming the check that failed. |
| B4 | A file with a renamed column is blocked, not silently loaded as nulls. |
| B5 | All tests pass; CI is green on `main` and fails a PR that breaks a model. |

**Measured before/after**

| Metric | v1 | Phase 2 target |
|---|---|---|
| Sources | 1 | 2 |
| Failure classes caught before publication | row-level only | row, file, and result level |
| Broken test files | 2 | 0 |
| Checks run on a pull request | 0 | full suite + `dbt build` |

---

### Phase 3 — Scale and historical correctness

**Goal:** the model stays correct as volume grows and as dimensions change.

- `fct_orders` from view to `materialized: incremental`, with a benchmark at 10M+ generated rows
  showing rebuild time before and after.
- Partitioning on `order_date`.
- `dbt snapshot` for SCD Type 2 on `dim_customers`, with a demonstration that a customer country
  change no longer rewrites historical revenue attribution.

**Acceptance criterion:** a customer moves country; last quarter's regional revenue is unchanged.

---

### Phase 4 — Streaming

**Goal:** reduce the latency floor from one day to seconds, without abandoning the batch path.

- Debezium CDC on `shop.orders` into Kafka.
- A consumer writing to the same fact table through the same idempotent load path.

**Acceptance criterion:** the streaming path and the batch path, run over the same window, produce
identical totals. That convergence is the proof, not the throughput number.

---

### Phase 5 — Object storage and open table format *(stretch)*

- Raw data landed as Parquet in S3-compatible storage (MinIO is already in the stack).
- Iceberg tables with a catalog; DuckDB or Trino as a query engine over them.

Only if Phases 1–4 land cleanly. Listed for completeness and to show the direction of travel.

---

## 6. Alternatives considered

Recording the reasoning so the choices can be challenged.

### 6.1 Orchestrator — Airflow vs Dagster vs Prefect

**Chosen: Apache Airflow 3.2.**

| Option | Case for | Case against | Verdict |
|---|---|---|---|
| **Airflow** | Largest ecosystem, most battle-tested production record, widest hiring pool. Airflow 3.2's asset partitioning maps directly onto a date-partitioned pipeline. Managed path exists (Amazon MWAA supports 3.2). | Heaviest to operate. Task-centric rather than asset-centric mental model. | **Chosen** |
| **Dagster** | Asset-centric model and lineage are a genuinely better fit for a dbt-heavy project. Better local development and testing story. | Smaller ecosystem, fewer solved edge cases in the community. | Rejected — the DX gain does not outweigh the ecosystem and transferability gap |
| **Prefect** | Fastest path from a Python function to a production workflow. Lowest operational overhead. | Best suited to modest orchestration needs; less standard in analytics/ETL contexts. | Rejected — the point of this phase is to build orchestration muscle, not to minimise it |

The honest version of the Dagster argument: it is probably the more pleasant tool for this specific
project. Airflow is chosen because it is the more widely applicable skill and the more common
production reality, and because the migration cost of switching later is real but bounded.

### 6.2 dbt orchestration — Cosmos vs a single BashOperator

**Chosen: Cosmos `DbtTaskGroup`.** A single `BashOperator` running `dbt build` gives one task, one
log, and all-or-nothing retries. Cosmos gives one Airflow task per dbt node, which means a failing
model is identifiable from the Airflow UI and retryable in isolation. Mixing the two in one
environment is acceptable — Cosmos for model-level DAGs, a plain operator for one-shot operational
commands.

Execution mode: **`ExecutionMode.LOCAL`**, not `VIRTUALENV`. Virtualenv mode creates and tears down
a virtual environment per dbt model; teams running many tasks in parallel have hit disk exhaustion
as a result.

### 6.3 Data quality — Soda Core vs Great Expectations vs dbt tests alone

**Chosen: Soda Core at the source boundary, existing dbt tests post-transform.**

| Option | Case for | Case against | Verdict |
|---|---|---|---|
| **dbt tests alone** | Already in place; zero new tooling. | Run *after* the load and only over things dbt built. Cannot catch a late or half-empty file. | Retained, but insufficient alone |
| **Soda Core** | SodaCL is YAML; a freshness, row-count or schema check is a couple of lines. Easy to review in a PR. Runs against the source, before the load. | Company-backed OSS: no UI, no historical monitoring in the free tier; some capability reserved for the paid product. | **Chosen** |
| **Great Expectations** | Richest expectation library; Data Docs produce human-readable HTML validation reports. | Steeper onboarding, documentation frequently described as hard to get started with, and its Python expressiveness is largely redundant here because pydantic already guards the row level. | Rejected for this scope |

The checks needed are the boring, high-value kind — freshness, volume delta, schema drift. That is
SodaCL's sweet spot, and its limitations only bite at a scale this project will not reach.

---

### 6.4 Extraction — hand-written vs Airbyte

**Chosen: hand-written for v2; Airbyte deferred to v3 Track E.**

The question is whether Phase 2's REST API source should be an Airbyte connector instead of Python.
It should not, and the reason is the phase's own acceptance criteria: B1 and B2 are "loads through
the same idempotent path" and "re-running pulls zero duplicates". Pagination, backoff, cursor state
and dedupe-on-rerun *are* Phase 2. Airbyte supplies all four, which would leave the phase shipping
its central lesson bought rather than learned.

The economics do not favour it yet either. A connector catalog earns its operational cost at N
sources; v2 has two, one of which is a CSV on local disk. And Airbyte OSS's supported install path
(`abctl`) provisions a local Kubernetes cluster, which §9 rules out.

Deferred, not rejected — see v3 Track E, which adopts it as PyAirbyte (library, no platform) at the
point where Tracks A and B push the source count to four.

---

## 7. Risks and known landmines

### 7.1 dbt Core 2.0 / Fusion breaks a naive install *(high, mitigated)*

Fivetran and dbt Labs completed their merger on 1 June 2026 and simultaneously released dbt Core
v2.0 with the Rust-based **Fusion** engine, which is now the default when installing dbt. **Fusion
does not yet support the Postgres adapter.** As of the merger date, `pip install dbt-postgres`
pulls `dbt-core 2.0.0-alpha.1` and `dbt init` fails with *"The 'postgres' adapter is not yet
supported by dbt Fusion."*

**Mitigation:** pin `dbt-core<2.0` in `requirements.txt` and in the Airflow image. Track the Postgres
adapter's progress toward Fusion support; adapter rollout order was Snowflake, Databricks, BigQuery,
Redshift, with Postgres expected later.

**Secondary consideration:** dbt Core remains Apache 2.0 and both companies have committed to
keeping it community-maintained. The community's concern is not the licence but that engineering
investment may shift toward the commercial products, leaving Core on maintenance. This is a watch
item, not a reason to change tooling now.

### 7.2 Cosmos source nodes create false lineage *(high, mitigated)*

By default Cosmos renders dbt sources as `EmptyOperator` tasks. They look like lineage in the
Airflow graph but are not actually wired to the Airflow task that loads the source data. The result
is that dbt models can run on incomplete or outdated data while the DAG appears green — precisely
the silent-wrong-data failure this project exists to prevent.

**Mitigation:** add a sensor as an **upstream dependency of** the existing `EmptyOperator`, rather
than replacing the operator with a sensor (replacing it burdens the scheduler). This is a known
pattern with a published production write-up.

### 7.3 DagBag import timeout *(low, mitigated)*

Larger dbt projects rendered through Cosmos can exceed Airflow's DAG import timeout.
**Mitigation:** raise `core.dagbag_import_timeout`.

### 7.4 The `relationships` test passes by coincidence *(medium, carried from v1)*

The generator's customer range happens to match the ten seeded customers; nothing enforces this at
the database level. Phase 2 should add a real foreign key constraint or an explicit source-level
check rather than continuing to rely on the coincidence.

### 7.5 Scope creep across phases *(medium)*

Five phases is a lot. **Mitigation:** Phases 1 and 2 are the ask. Each later phase is separately
approvable and each has a single acceptance criterion that determines whether it is done.

### 7.6 Synthetic data limits what can be demonstrated *(low, accepted)*

Customers are drawn uniformly, so the "Top Customers" chart is flat by construction. Real data has a
long tail. This is a property of the generator, not a finding, and is stated rather than hidden.

---

## 8. Demo plan

The proposal is judged on a demo, not a document. Four demonstrations, in order:

1. **Unattended backfill.** Trigger one DAG run covering 56 days. Walk away. Return to 124,322 rows
   and a green graph.
2. **Break it on purpose.** Kill the database connection mid-load. Show the task fail, retry, and
   succeed. Show the fact count is correct and contains no duplicates.
3. **Block a bad file.** Feed in a file with 40% of the expected rows. Show Soda failing the run
   *before* the fact load, the alert firing, and the dashboard still showing yesterday's correct
   numbers rather than today's wrong ones.
4. **Show the gate hold.** Force the load task to fail. Show that the dbt task group does not run,
   and that no partially-transformed data reaches Lightdash.

Demo 2 and demo 3 are the ones that matter. Anyone can show a pipeline succeeding.

---

## 9. Deliberate non-goals

Stating what this project is not, so the boundaries are a decision rather than an omission.

- **Not a Kubernetes deployment.** Docker Compose throughout. Container orchestration is a separate
  skill set and would double the surface area without improving the data outcome.
- **Not a cloud migration.** Local stack with MinIO standing in for object storage. Cloud is
  Phase 5 at the earliest.
- **Not multi-tenant.** Airflow 3.2's multi-team support exists but is experimental and irrelevant
  at ten people.
- **Not a data catalog.** Lineage stays within dbt and Airflow. DataHub/OpenMetadata is out of scope.
- **Not a managed EL tool.** Extraction stays hand-written through v2 so the cursor, backoff and
  idempotency lessons are actually learned. Airbyte arrives in v3 Track E, as a library rather
  than a platform. See §6.4.
- **Not real-time until Phase 4.** Batch is the correct default for daily sales analytics; streaming
  is added to demonstrate the capability and prove convergence, not because the use case demands it.
- **Not a rewrite.** Every v1 guarantee — idempotency, atomicity, the DLQ, lineage columns, the
  single revenue definition — is preserved, not rebuilt.

---

## 10. Sequencing and dependencies

The phase order is not a preference; it is forced by dependencies.

```
   Phase 1  Orchestration
      │       └─ everything else needs something to schedule it,
      │          retry it, and alert on it
      ▼
   Phase 2  Second source + quality gates
      │       └─ gates need a scheduler to hang off;
      │          a second source needs orchestration to coordinate
      ▼
   Phase 3  Incremental + SCD2
      │       └─ incremental models are only meaningfully testable
      │          once runs are scheduled and repeatable
      ▼
   Phase 4  Streaming (CDC + Kafka)
      │       └─ a consumer with no orchestration is a process
      │          nobody restarts when it dies
      ▼
   Phase 5  Object storage + open table format
              └─ paying for storage of full refreshes before
                 Phase 3 lands is money set on fire
```

---

## 11. What I expect to learn

Stated plainly, because this is partly a development exercise and pretending otherwise would be
dishonest.

- Orchestration as a discipline: idempotent task design, backfill semantics, catchup behaviour,
  dependency modelling, and why data-aware scheduling differs from cron.
- Failure engineering: retries, backoff, dead letters, alerting thresholds, and the difference
  between a system that fails loudly and one that fails silently.
- Incremental modelling and slowly-changing dimensions, which are where most real analytics bugs live.
- CDC and stream/batch convergence.

The transferable skill is not "I can use Airflow." It is knowing which failure modes a pipeline has
and having built the thing that catches each one.

---

## Appendix A — Repository structure after Phase 2

```
nova-analytics/
├── dags/                        # Airflow DAGs
│   ├── nova_daily.py            #   main pipeline, asset-partitioned by order_date
│   └── nova_api_incremental.py  #   API source with cursor
├── ingestion/
│   ├── csv_ingestion/           # existing, unchanged
│   └── rest_api/                # completed in Phase 2
├── nova_analytics/              # dbt project
│   ├── models/
│   ├── snapshots/               # Phase 3
│   └── tests/
├── quality/
│   └── soda/                    # SodaCL checks + configuration
├── sql/
│   ├── ddl/
│   └── migrations/
├── docker/
│   ├── docker-compose.yml       # postgres, redis, airflow
│   └── lightdash-compose.yml    # existing
├── tests/                       # unit + integration
├── .github/workflows/ci.yml     # Phase 2
├── docs/
│   ├── RUNBOOK.md
│   ├── ARCHITECTURE.md
│   └── adr/                     # decision records
└── requirements.txt             # dbt-core pinned <2.0
```

---

## Appendix B — Version and pinning notes

| Component | Version / constraint | Note |
|---|---|---|
| Apache Airflow | 3.2.x | Asset partitioning, deadline alerts, Task SDK |
| dbt Core | `<2.0` — **pinned** | dbt Core 2.0 defaults to the Fusion engine, which does not yet support Postgres |
| dbt adapter | `dbt-postgres` | Must be installed against a pinned `dbt-core<2.0` |
| Astronomer Cosmos | current | `ExecutionMode.LOCAL`; `retries >= 2` on dbt tasks |
| Soda Core | current OSS | No UI or historical monitoring in OSS; acceptable at this scope |
| PostgreSQL | 16 | Unchanged from v1 |
| Lightdash | 2.17.0 | Pinned in v1; unchanged |

---

## Appendix C — Open questions for review

1. **Alert channel.** Slack, email, or PagerDuty-style? Preference depends on what the team already
   watches. Airflow's native notifiers cover all three.
2. **Approval boundary.** Are Phases 1–2 approvable as one unit, or should Phase 1 be reviewed
   before Phase 2 begins?
3. **Real data.** Is there a non-synthetic dataset that could replace or supplement the generator?
   Several current limitations (uniform customer distribution, the coincidental `relationships`
   test) are artifacts of synthetic data.
4. **Elementary vs native Airflow alerting** for the notification layer — not yet evaluated, does
   not block Phase 1.
5. **Debezium's current Postgres CDC story** — not yet evaluated, relevant only from Phase 4.
