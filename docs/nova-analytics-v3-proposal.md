# Nova Analytics v3 — Proposal

**From a reliable pipeline to a data platform with an opinion**

| | |
|---|---|
| **Author** | Sam |
| **Date** | August 2026 |
| **Status** | Proposal — for review |
| **Depends on** | Nova Analytics v2 (orchestration, quality gates, incrementality, streaming) |
| **Proposed** | Five tracks; Tracks A, B and E are the core ask |

---

## 1. Why v3 exists

v1 and v2 were forced moves. Every competent data platform ends up with orchestration, retries,
quality gates, incremental models and history — not because anyone chose them, but because the
alternative is a pipeline nobody can trust. There was no real decision to make.

v3 is the first version where there is a choice. The platform is reliable; the question is what to
point it at.

The answer proposed here is: **make the domain harder, and make the guarantees stronger.**

Nova today knows about orders. Real ecommerce data is considerably nastier than that — returns that
land months after the sale, orders that arrive backdated, customers who checked out as guests before
they registered, and behaviour that happens long before anyone buys anything. Nova also currently
*blocks* bad data at the gate. The stronger guarantee is that bad data can be written, inspected,
and discarded without a consumer ever seeing it.

**v2 made the pipeline trustworthy. v3 makes the answers trustworthy.**

Five tracks, in dependency order:

| Track | Theme | Status |
|---|---|---|
| **A** | Domain depth — returns, restatement, lifecycle, multi-grain | Core ask |
| **B** | Behaviour — clickstream, sessionisation, funnel, attribution | Core ask |
| **E** | Managed ingestion — PyAirbyte, connector-based sources, EL-first validation | Core ask |
| **C** | Reliability engineering — WAP, data SLOs, disaster recovery | Core ask |
| **D** | Activation and serving — real-time OLAP, reverse ETL, metrics API | Stretch |

---

## 2. Where v2 leaves the system

Assumed complete before v3 begins.

```
   CSV drop ──┐
              ├─→ Airflow 3.2 ─→ Soda gate ─→ fact_orders ─→ Cosmos/dbt ─→ Soda gate ─→ Lightdash
   REST API ──┘        │                          │
                       │                     [SCD2 snapshots,
                  Kafka + CDC                 incremental models]
```

**Guarantees carried into v3:** idempotency, atomicity, dead-letter queue, lineage columns, a single
definition of revenue, scheduled and retrying execution, alerting on failure, freshness and volume
checks, incremental materialisation, SCD Type 2 on customers, and batch/stream convergence.

**What v3 does not touch:** none of the above is rebuilt. Every track below is additive.

---

## 3. Track A — Domain depth

The cheapest track to build and the most disproportionately valuable in interviews. It stress-tests
everything v2 built, using no new infrastructure.

### A1. Returns and refunds as first-class events

Today a refund is a value in a `status` column. That is a modelling shortcut, and it hides the most
interesting question in ecommerce analytics.

Model returns as their own fact, at their own grain, with their own date. Then confront the
consequence: **when a July order is refunded in September, does July's revenue change?**

Both answers are defensible, and the point of the exercise is to choose one deliberately and make
the system enforce it:

| Approach | Behaviour | Cost |
|---|---|---|
| **Restate** | July's revenue drops when the September refund lands | Historical reports change after publication; every downstream consumer must tolerate that |
| **As-of / no restatement** | July keeps its revenue; the refund is a September event | Reports are stable, but "net revenue for July" needs a stated as-of date to be meaningful |

**Deliverable:** both metrics exist in the semantic layer (`gross_revenue`, `net_revenue_restated`,
`net_revenue_as_booked`), the difference between them is visible on a chart, and the documentation
states which one the business uses and why.

This is the single highest-value item in the whole proposal relative to its build cost.

### A2. Order lifecycle as an accumulating snapshot

`pending → completed → refunded` is a sequence of events, not a column.

The Kimball pattern is an **accumulating snapshot fact table**: <cite index="105-1">one row represents the entire lifecycle of a business entity, so an order moving through created, shipped and delivered produces a single row with multiple date columns rather than three rows. The row is updated as milestones are reached, populating null fields — the table is physically shorter but wider than a transaction table.</cite> <cite index="106-1">There is a date foreign key for each critical milestone, and revisiting and updating rows is unique among the three fact table types.</cite>

The payoff is query simplicity: <cite index="105-1">calculating time between order and ship in a transaction schema requires self-joins or window functions to locate two distinct event rows; in an accumulating snapshot the lag is a column subtraction, and lags can be precomputed at ETL time as additive facts.</cite>

Two things worth getting right, because most implementations get them wrong:

- **Pair it with a transaction fact, don't replace one.** <cite index="107-1">The accumulating snapshot tells you the pipeline's current state but glosses over intermediate states — it cannot tell you when and why an entity looped through states multiple times. The standard solution is two fact tables: a transaction fact with one row per state change, and the accumulating snapshot derived from it.</cite> Since Nova will have the event stream from v2's CDC work, building the transaction fact first and deriving the snapshot is the natural order.
- **Design for late-arriving events.** <cite index="109-1">This pattern is highly sensitive to out-of-order arrival: if events 1, 3 and 4 are processed in one run and event 2 arrives later, there is no consistent way to insert it, and a reload becomes necessary.</cite> Which leads directly to A3.

**Deliverable:** `fct_order_events` (transaction grain) and `fct_order_lifecycle` (accumulating
snapshot), plus a time-to-fulfilment metric and a cart-abandonment count that were impossible before.

### A3. Late-arriving and backdated data

Files that show up three days late. Corrections to rows already loaded. Events arriving out of order.

This is where v2's idempotency guarantee stops being a slogan and starts being tested. Current
behaviour is `ON CONFLICT (order_id) DO NOTHING` — a correction to an existing order is *silently
discarded*. That is the right default for duplicate deliveries and the wrong one for genuine updates.

**Deliverable:** an explicit policy — probably `DO UPDATE` gated on a source-side version or
`updated_at` — with a test proving that a corrected row updates and a replayed identical row does not.

### A4. Multi-grain modelling

`order_items` is seeded and unused. Adding a line-grain fact forces the fan-out problem to become
real rather than theoretical — right now `dim_customers`' uniqueness test guards against a bug that
cannot currently occur.

**Deliverable:** `fct_order_lines`, a documented grain per fact table, and a demonstration of what
happens to revenue when a header-grain metric is joined to a line-grain fact without care.

### A5. Entity resolution *(optional within track)*

Guest checkouts that later register; identity stitching across email, device and customer id.
Genuinely hard, very common, and rare on junior portfolios. Include only if A1–A4 land early.

---

## 4. Track B — Behaviour

Nova has orders but no behaviour, which means it can say what was bought and nothing about why. This
roughly doubles the size of the domain and gives v2's Kafka work a reason to exist beyond a demo.

### B1. Clickstream generation and ingestion

Synthetic events — page view, product view, add to cart, checkout start, purchase — generated with
the same seeded determinism as the order generator, flowing through the existing Kafka path.

Volumes should be realistic: roughly 50–200 events per order, which finally puts the pipeline at a
scale where the incremental models from v2 matter.

### B2. Sessionisation

Turning an unbounded event stream into sessions with an inactivity timeout. A classic problem with
two viable implementations, and choosing between them is the interesting part.

**In-warehouse (SQL/dbt):** window functions over the event table — `LAG` on timestamp, a boolean for
"gap exceeded", a running sum to assign session ids. Simple, testable, batch-latency.

**In-stream (Flink or Spark Structured Streaming):** session windows maintained in engine state.

The engine comparison, if the streaming route is taken:

<cite index="73-1">Flink supports event-time, processing-time and session windows, making it suitable for complex event pattern detection, and provides efficient state management for user activity tracking and session management.</cite> <cite index="76-1">Session windows group events by activity, defined by a gap of inactivity. Spark Structured Streaming supports event-time windowing with watermarks, but data arriving after the watermark is typically dropped, and its windowing is primarily time-based and considered less versatile than Flink's for custom or session window logic.</cite> Spark did add native session windows in 3.2, so this is a difference of degree rather than capability.

The latency difference is architectural, not incidental: <cite index="78-1">Flink processes event-at-a-time and achieves sub-second end-to-end latency; Spark processes in micro-batches, which introduces a latency floor in the seconds range but integrates with Spark's batch ecosystem. Spark's watermark checks occur at micro-batch boundaries, tying event-time logic to the batch execution cadence.</cite>

**Recommendation: implement sessionisation in dbt first, then in Flink second, and compare them.**
The comparison is the deliverable. Two implementations of the same definition that disagree by 0.3%
is a far better artifact than one implementation that works — it forces you to explain watermarks,
late events, and where the discrepancy comes from.

One practical caveat: <cite index="79-1">Flink's Python support is less mature than Spark's</cite>, which matters for a Python-first codebase. PyFlink is workable; Flink SQL is the smoother path.

### B3. Funnel and conversion analysis

Drop-off between view → cart → checkout → purchase. Cart abandonment rate. Time-to-purchase
distribution. These are the metrics the semantic layer was built for, and none of them are
answerable today.

### B4. Marketing attribution

The reason this earns a place rather than being decoration: **three attribution models give three
different answers to the same question**, which is the clearest possible demonstration of why a
semantic layer matters.

<cite index="104-1">First-touch attribution hands 100% of credit to the first touchpoint, surfacing the awareness channels that start the journey; last-touch hands 100% to the last, surfacing the channels that close. The standard models are first-touch, last-touch, linear, time-decay, position-based, w-shaped, and data-driven.</cite>

The failure mode is worth stating in the docs: <cite index="104-1">a team on last-click over-funds branded search and retargeting because those sit at the bottom of the funnel and scoop up the credit; a team on first-touch over-funds top-of-funnel content. Both are wrong, and both will defend their calls with attribution data that says exactly what they want to hear.</cite>

**Deliverable:** first-touch, last-touch and linear models as dbt models, all three exposed as
metrics, and a single chart showing the same channel ranked differently under each. A reference
implementation exists — <cite index="100-1">Snowplow's attribution dbt package supports first-touch, last-touch, position-based and linear models entirely in SQL inside the warehouse with no external dependencies</cite> — worth reading rather than importing, since building it is the point.

---

## 5. Track C — Reliability engineering

The track nobody does. Highest signal per hour of work.

### C1. Write-Audit-Publish

v2's quality gates *block* bad data before the load. WAP is strictly stronger: bad data gets
**written**, inspected in place, and discarded — without any consumer ever seeing it.

<cite index="88-1">WAP is a three-stage commit process: write new data to an isolated branch invisible to production consumers, run automated quality checks against that branch, and publish by fast-forwarding to main only if the checks pass.</cite>

Apache Iceberg makes this cheap. <cite index="88-1">The staging branch shares data files with main, so there is no duplication; the fast-forward merge is a metadata-only operation and therefore instantaneous; and readers see a consistent snapshot throughout, with no downtime or partial states.</cite> The alternative — <cite index="88-1">a separate staging database, ETL to copy validated data across, and a swap involving downtime or reader inconsistency</cite> — is what most teams actually build.

The mechanics are a handful of lines: <cite index="84-1">set `spark.wap.branch` to an audit branch, insert, run the validation workflow against that branch, then `CALL catalog.system.fast_forward('table', 'main', 'audit-branch')` to update main. The branch reference is removed when `expireSnapshots` runs.</cite> Analysts can inspect before publication — <cite index="86-1">the branch is queryable directly via time-travel syntax, and because branching is metadata-only, the underlying Parquet files are written just once during the write phase.</cite>

Two honest costs to state up front:

- <cite index="90-1">The primary challenge of WAP is increased latency: the multistep process delays data availability for downstream consumers, which may be problematic for near real-time use cases. It also requires more sophisticated orchestration than the DLQ approach.</cite> Nova already has both the orchestration and the DLQ, which is why this is affordable now and would not have been in v1.
- It requires Iceberg, which means the object-storage work deferred from v2 Phase 5 comes forward. MinIO is already in the stack, so this is less of a jump than it sounds.

**Deliverable:** a pipeline where a bad load is written, caught, and dropped, with the production
table demonstrably never having contained it — verified by snapshot history, not by assertion.

### C2. Data SLOs and error budgets

Moving from "did the DAG succeed?" to "is this dataset fit for purpose right now?"

<cite index="91-1">The distinction that matters: a pipeline can be up while the business is wrong. The most damaging failures are not hard outages but silent degradations — freshness slips, completeness drops, duplication spikes — while the platform looks healthy. The shift is toward dataset-level SLOs on freshness, completeness and validity, reviewed like operational KPIs.</cite>

The SRE machinery transfers directly. <cite index="95-1">The SLO is the internal goal; the SLA is the externally promised floor, typically a relaxation of the SLO to absorb operational variance. The error budget is the amount of SLO violation allowed before remediation takes priority over new features — if the SLO is 99.5% freshness, the error budget is 0.5% of measurement windows. Burn it too fast and the team shifts from delivering new pipelines to stabilising existing ones.</cite>

One discipline to import from SRE practice: <cite index="97-1">avoid more than one page-worthy SLO per critical journey unless each target drives a distinct operational action — multiple SLOs on the same path create duplicate pages with no extra insight.</cite>

**Proposed SLOs for Nova** (three, deliberately):

| SLI | SLO | Why this one |
|---|---|---|
| Freshness — `fct_orders` max `order_date` within N hours of wall clock | 99.5% of hourly windows | The failure a dashboard viewer would notice first |
| Completeness — daily row count within expected band | 99% of daily windows | Catches the half-empty file |
| Correctness — dbt test pass rate | 100%, no budget | Silent wrong data gets no error budget |

**Deliverable:** SLIs instrumented and recorded over time, a burn-rate view, and a written error
budget policy that says what happens when it is exhausted. <cite index="95-1">The runbook should exist before the first breach.</cite>

### C3. Disaster recovery drill

**Delete the warehouse. Restore it from raw files and git. Measure how long it takes.**

This is the single most impressive item on this list and almost nobody does it. It also validates a
claim v1 made and never tested: that raw data is kept and the pipeline is reproducible.

The drill exposes real questions. Are the raw CSVs actually retained, or gitignored and regenerable
only because the generator is seeded? Do the dbt snapshots survive, and if not, is SCD2 history
permanently lost? Can Airflow backfill 56+ days without hitting rate limits on the API source?

**Deliverable:** a documented RTO with an actual measured number, and a `docs/DR.md` runbook that
someone else could follow.

### C4. Chaos testing

Kill workers mid-task. Corrupt a file. Introduce network partitions between containers. Fill a disk.
Prove the pipeline degrades loudly rather than silently.

**Deliverable:** a `chaos/` directory of failure injection scripts and a table of failure mode →
observed behaviour → whether it alerted.

---

## 6. Track D — Activation and serving *(stretch)*

Only if A, B and C land. Listed for direction of travel.

### D1. Real-time OLAP

Once clickstream volume is real, Postgres stops being the right place to serve interactive queries
over events. Three candidates:

| Engine | Strength | Weakness |
|---|---|---|
| **ClickHouse** | <cite index="67-1">Highest raw throughput for internal analytics — Cloudflare processes over 20 million events per second on it.</cite> Simplest to operate. | <cite index="69-1">Not streaming-first: it integrates with Kafka, but real-time pipelines often need custom engineering, and query isolation can suffer as concurrency grows.</cite> |
| **Apache Druid** | <cite index="67-1">Excels at time-series rollups and pre-aggregation; Netflix uses it for real-time analytics.</cite> | Heaviest operationally; most moving parts. |
| **Apache Pinot** | <cite index="67-1">Purpose-built for user-facing low-latency analytics with strict p99 SLAs — LinkedIn runs it over hundreds of billions of rows at single-digit millisecond p99.</cite> | <cite index="69-1">The configuration model is powerful but complex; schema design, index selection and cluster tuning need planning, and teams without prior experience underestimate the operational overhead.</cite> |

**Recommendation: ClickHouse.** Nova's requirement is internal dashboards, not a user-facing product
serving thousands of concurrent queriers. Pinot's advantage is concurrency Nova does not have, at an
operational cost Nova cannot justify. Druid's advantage is scale Nova will not reach.

### D2. Reverse ETL

Push a "churn risk" or "VIP" segment from the warehouse back into an operational tool. Closes the
loop from warehouse to application, which most portfolios skip entirely.

Tooling note: the open-source picture here is thin. <cite index="115-1">Airbyte acquired Grouparoo, the main open-source reverse ETL project, in 2022, with Grouparoo's founder noting they saw less demand for being further down the value chain.</cite> The commercial options (Hightouch, Census) are not appropriate for this project. **Build it: a small Airflow DAG that reads a dbt mart and writes to a target API with the same retry and idempotency discipline as the ingestion side.** Reverse ETL is not conceptually hard; treating it with the same rigour as ingestion is the point.

### D3. Metrics API

Expose the semantic layer over HTTP so applications — not just Lightdash — can ask for
`total_revenue`. The natural extension in 2026 is an MCP server over the same layer, so an LLM
answers questions through governed metric definitions rather than by writing raw SQL against the
warehouse. This is the clearest possible payoff for having defined revenue exactly once.

---

## 7. Track E — Managed ingestion

**Goal:** stop hand-writing extractors once there are enough sources for that to be the wrong job,
without giving up any v1 guarantee.

By this point the platform has four sources: the CSV drop, the v2 REST API, Track B's clickstream
and Track A's returns feed. Two of those are hand-written. Writing the third and fourth by hand is
no longer teaching anything the second did not already teach.

### E1. PyAirbyte in an Airflow task

`pip install airbyte`. Connectors run as Docker containers driven from a Python task; incremental
cursor state is managed by the library. No Airbyte server, no UI, no Temporal, no Kubernetes — the
platform install (`abctl`) provisions a kind cluster, which §9.5 rules out.

The upgrade path to the full platform is recorded and deliberately not taken. The trigger that
would justify it: someone other than the maintainer needs to add a source without writing code.

### E2. Where validation goes in an EL-first pipeline

The load-bearing question of this track. Airbyte lands raw rows and has no equivalent of v1's
row-level rejection with a reason, so the dead-letter queue — "bad rows are kept, not dropped", a
guarantee every version so far has preserved — has to move rather than disappear.

Proposed: validation shifts *after* the load, into a dbt staging model that routes failing rows to a
`_rejected` table carrying the same reason string the pydantic DLQ writes today. The acceptance
criterion is that a bad row is still traceable to its reason and its source file, by the same query,
whichever ingestion path it arrived on.

### E3. Migration and coexistence

The v2 hand-written REST API source is not deleted. It stays as the reference implementation — the
thing that makes it legible what the connector is doing for free. New sources go through PyAirbyte;
the existing two do not move.

**Acceptance criteria**

| # | Criterion |
|---|---|
| E-a | A new source is added by configuring a connector, with no new extraction code. |
| E-b | Re-running a connector sync pulls zero duplicate records — the same guarantee v2's B2 proved by hand. |
| E-c | A bad row from a PyAirbyte source is traceable to its reason and source, by the same query that finds a bad row from the CSV path. |
| E-d | Container count unchanged. |

---

## 8. Sequencing

```
   Track A  Domain depth
      │       └─ no new infrastructure; stress-tests v2's guarantees;
      │          A2 needs the transaction fact that B1's events also feed
      ▼
   Track B  Behaviour
      │       └─ needs A's event modelling; gives v2's Kafka work
      │          an actual purpose; creates the volume that makes
      │          incremental models and real-time OLAP meaningful
      ▼
   Track E  Managed ingestion
      │       └─ pointless at two sources; A and B make it four,
      │          which is where a connector catalog starts paying
      ▼
   Track C  Reliability engineering
      │       └─ WAP needs Iceberg; SLOs need enough history to
      │          measure against; the DR drill is only interesting
      │          once there is something complex to restore
      ▼
   Track D  Activation and serving
              └─ real-time OLAP is unjustifiable until clickstream
                 volume exists; metrics API is unjustifiable until
                 there are metrics worth exposing
```

A and C are partially parallelisable — C2 (SLOs) needs no new modelling and could start alongside A.

---

## 9. Alternatives considered

### 9.1 Sessionisation engine

**Chosen: dbt first, then Flink, and compare.** Spark Structured Streaming was the third option and
is the weaker fit — its micro-batch model and watermark-at-batch-boundary semantics make it the
compromise choice for a workload that is genuinely event-driven. Flink is the better teaching tool
precisely because its event-time semantics are explicit rather than inherited from a batch cadence.
Spark stays in the stack for batch work.

### 9.2 Real-time OLAP engine

**Chosen: ClickHouse** (see §6.1). Pinot and Druid both solve problems Nova does not have, at
operational costs Nova cannot absorb at ten people.

### 9.3 WAP mechanism

**Chosen: Iceberg branching.** The alternative is the clone-and-swap approach — a staging table with
an identical schema, validated, then swapped. That works, but it duplicates data and the swap is not
atomic in the same way. <cite index="89-1">Even in warehouses without native branching, the recommended pattern is to write to a staging area with an identical schema, run the checks, and publish only on pass — and the quality checks built that way carry forward regardless of the underlying publish mechanism.</cite> Iceberg is chosen because it makes the publish step free rather than because the checks differ.

### 9.4 Restatement policy

**Deliberately not chosen in advance.** Both are correct depending on the business. The deliverable
is that the system supports both and the documentation states which one Nova uses and why. Choosing
silently is the failure mode.

### 9.5 Rejected outright

| Idea | Why rejected |
|---|---|
| **Data mesh** | An organisational pattern for many teams with many domains. At ten people it would be cosplay. |
| **Building a custom orchestrator or query engine** | Educational, but reads as not knowing what already exists. |
| **Adding Snowflake or Databricks** | Expensive, and teaches a vendor UI more than a concept. Iceberg on MinIO gives the same lessons. |
| **Kubernetes** | Deferred again. Doubles the operational surface without improving any data outcome in these tracks. |
| **A feature store / ML track** | Genuinely valuable, but the point-in-time correctness lesson it teaches is the same lesson SCD2 and the restatement work already teach. Redundant at this stage. |

---

## 10. Risks

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | **Scope.** Four tracks is more than v2's five phases. | High | A, B, C are the ask; D is explicitly stretch. Each track has one headline deliverable that determines done. |
| R2 | **Iceberg pulls forward deferred infrastructure.** WAP needs an open table format and object storage. | Medium | MinIO already exists in the stack; use it as the S3 target rather than adding a cloud dependency. Scope to a single table first. |
| R3 | **Flink's Python maturity.** PyFlink lags Spark's Python support. | Medium | Use Flink SQL rather than PyFlink for sessionisation. The dbt implementation lands first regardless, so the Flink version is never on the critical path. |
| R4 | **Late-arriving data breaks the accumulating snapshot.** Out-of-order events cannot always be inserted consistently and may force a reload. | Medium | Design the transaction fact as the source of truth and rebuild the snapshot from it, so a reload is cheap and expected rather than exceptional. |
| R5 | **Synthetic data limits what the behaviour track can show.** Attribution and sessionisation results are only as interesting as the generator's realism. | Medium | Invest in generator realism first: correlated journeys, a long-tail customer distribution, channel mix. Accept and state that findings are properties of the generator. |
| R6 | **WAP adds latency.** The multistep write-audit-publish path delays availability. | Low | Nova is daily batch. The latency cost is invisible at this cadence and should be stated as a known trade-off rather than hidden. |
| R7 | **SLO theatre.** Instrumenting SLOs nobody acts on. | Medium | Three SLOs maximum, each tied to a distinct action, with a written error budget policy. If a breach would not change what anyone does, do not instrument it. |
| R8 | **DR drill discovers the raw data is not actually retained.** The CSVs are gitignored and only regenerable because the generator is seeded. | Medium | This is the drill working correctly. Fix the retention before declaring the drill passed; do not weaken the drill. |

---

## 11. Demo plan

Five demonstrations. Each maps to one track.

1. **The refund that moves revenue.** Show July's revenue under both restatement policies, side by
   side, from the same data. Explain which one Nova uses and why.
2. **Two sessionisations that disagree.** The dbt and Flink implementations of the same session
   definition, with the discrepancy quantified and explained — watermarks, late events, boundary
   effects.
3. **Three attribution models, three winners.** One chart, one channel, three different rankings.
   Then the semantic layer definition that makes the difference explicit rather than accidental.
4. **Bad data that production never saw.** Write a corrupt batch to an audit branch. Show the check
   failing, the branch dropped, and the main table's snapshot history proving it never contained the
   data.
5. **Delete the warehouse.** Restore from raw and git. Show the stopwatch.

Demo 1 and demo 5 are the ones that separate this from a tooling exercise.

---

## 12. Deliberate non-goals

- **Not a cloud migration.** MinIO and Iceberg locally. Cloud remains deferred.
- **Not Kubernetes.** Docker Compose throughout, for the third version running.
- **Not a data catalog.** Lineage stays in dbt and Airflow.
- **Not ML.** No feature store, no models. The point-in-time correctness lesson is already covered.
- **Not multi-tenant, not data mesh, not a platform for other teams.** Nova has one owner.
- **Not real user data.** Everything is synthetic and stated as such.
- **Not a rewrite.** Every v1 and v2 guarantee is preserved.

---

## 13. What I expect to learn

- **Modelling under ambiguity.** Restatement, grain, and lifecycle are problems with no single right
  answer, only defensible ones. Choosing and documenting is the skill.
- **Event-time semantics.** Watermarks, late arrival, out-of-order processing, and why two correct
  implementations of the same definition disagree.
- **Reliability as a discipline rather than a feature.** SLOs, error budgets, and the difference
  between a pipeline that is up and data that is right.
- **Recovery.** Most engineers have never restored anything. Doing it once, deliberately, with a
  stopwatch, is worth more than reading about backups.

The transferable claim after v3 is not "I built a pipeline." It is: *I can tell you every way this
system can be wrong, what it does when it is, and how long it takes to get back.*

---

## Appendix A — Repository structure after Track C

```
nova-analytics/
├── dags/
│   ├── nova_daily.py
│   ├── nova_api_incremental.py
│   ├── nova_clickstream.py          # Track B
│   ├── nova_wap.py                  # Track C — write/audit/publish
│   └── nova_slo_recorder.py         # Track C — SLI capture
├── ingestion/
│   ├── csv_ingestion/
│   ├── rest_api/
│   └── clickstream/                 # Track B — event producer
├── streaming/
│   └── flink/                       # Track B — sessionisation job
├── nova_analytics/                  # dbt
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   │   └── sessions/            # Track B
│   │   └── marts/
│   │       ├── fct_order_events     # Track A — transaction grain
│   │       ├── fct_order_lifecycle  # Track A — accumulating snapshot
│   │       ├── fct_order_lines      # Track A — line grain
│   │       ├── fct_returns          # Track A
│   │       └── attribution/         # Track B
│   └── snapshots/
├── quality/
│   ├── soda/
│   └── slo/                         # Track C — SLI definitions, burn rate
├── chaos/                           # Track C — failure injection
├── docker/
├── docs/
│   ├── RUNBOOK.md
│   ├── ARCHITECTURE.md
│   ├── DR.md                        # Track C — measured RTO
│   ├── GRAIN.md                     # Track A — grain per fact table
│   ├── ERROR_BUDGET_POLICY.md       # Track C
│   └── adr/
└── requirements.txt
```

---

## Appendix B — New components and versions

| Component | Role | Track | Note |
|---|---|---|---|
| Apache Iceberg | Open table format enabling branch-based WAP | C | Branching and tagging supported in the Java library and in Spark and Flink integrations |
| MinIO | S3-compatible object store for Iceberg data files | C | Already in the stack for Lightdash |
| Apache Flink | Sessionisation with event-time session windows | B | Flink SQL preferred over PyFlink |
| ClickHouse | Real-time OLAP over clickstream | D (stretch) | Chosen over Pinot and Druid on operational cost |
| dbt | Unchanged; still pinned `<2.0` | all | Fusion still lacks a Postgres adapter — carried forward from v2 |
| Airflow | Unchanged, 3.2.x | all | Asset partitioning now covers event partitions too |
| PyAirbyte | Connector-based extraction with managed cursor state | E | Library, not the platform; `abctl` would pull in Kubernetes, which §9.5 rules out |

---

## Appendix C — Open questions for review

1. **Restatement policy.** Which does the business actually want — restated history, or stable
   published figures with an as-of date? This is a business decision, not an engineering one, and
   the answer shapes Track A.
2. **Track ordering.** Is the domain work (A) or the reliability work (C) more valuable to review
   first? They are largely independent.
3. **Iceberg scope.** Convert one table or the whole warehouse? Recommendation is one table
   (`fct_orders`) to prove the pattern, then decide.
4. **Generator realism budget.** How much effort is worth spending making synthetic clickstream
   behave like real user journeys? Too little and Track B's findings are artifacts; too much and it
   becomes its own project.
5. **Is there any real data available?** Several limitations across v1, v2 and v3 exist purely
   because the data is generated. This remains the highest-leverage unlock in the entire programme.
