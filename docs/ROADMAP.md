# Roadmap

Sequenced plan for the work remaining across the v2 and v3 proposals. Written 2026-09-17,
after v2 Phase 3 closed.

This is the *order* and the *why*. The per-step instructions get written when the step is
picked up — planning them in detail now would be waste, since each step's shape depends on
what the one before it turned up.

## Where things stand

| | Scope | State |
|---|---|---|
| v2 Phase 1 | Orchestration — Airflow | done |
| v2 Phase 2 | Second source, quality gates, CI | done |
| v2 Phase 3 | Scale and historical correctness | done (Steps 21–29) |
| v3 Block 1 | Track A1 — returns as first-class events | Steps 30–31 done, 32–33 open |
| v2 Phase 4 | Streaming — Debezium CDC → Kafka | **resequenced, see below** |
| v2 Phase 5 | Object storage, Parquet/Iceberg | stretch, unchanged |
| v3 Tracks A–E | Domain depth, behaviour, reliability, serving, ingestion | not started |

Deferred on purpose, not forgotten: Step 22 (10M-row benchmark) and Step 25 (partitioning).
See PROGRESS.md for the reasoning.

## The one sequencing decision

**v2 Phase 4 does not come next, despite being the next number.**

Phase 4 puts Debezium CDC on `shop.orders` into Kafka. The warehouse currently takes about
2,300 orders a day. Streaming that is a demo of Kafka, not a use of it — and the acceptance
criterion (batch and streaming totals converge) would pass on a volume where nobody could
tell the difference.

The v3 proposal's own sequencing says Track B "gives v2's Kafka work an actual purpose" and
"creates the volume that makes incremental models real". B1 generates 50–200 clickstream
events per order — two orders of magnitude more than the order stream. That is a stream
worth having.

So Phase 4 merges into the B1 block and gets built when there is something to stream.

Everything before that point needs **no new infrastructure**. That is deliberate: the
project's rule is to add a tool only after feeling the pain it removes.

---

## Block 1 — Track A1: returns as first-class events (Steps 30–33)

*Why first: no new infrastructure, and it runs directly on the as-of reasoning proved in
Steps 27–28 while that is still fresh. The v3 proposal calls it "the single highest-value
item in the whole proposal relative to its build cost."*

The problem, concretely: 39,553 orders carry `status = 'refunded'` and **not one has a
refund date**. `total_revenue` filters to `status = 'completed'`, so a refunded July order
contributes zero to July. The system already restates history — silently, and nobody chose it.

**Step 30 — give the refund its own date and grain.** ✅ **done 2026-09-19** (`a6aa170`).
`shop.refunds` created and backfilled: 39,553 rows, **12,754 of them land in a month other than
their order's** — 32% of refunds, and the size of the restatement problem. Grain is one refund per
order, enforced by `order_id` as PK. Rows are laptop-only; a fresh environment runs
`python -m scripts.backfill_refunds` after the migration. Detail in PROGRESS.md.
`sql/migrations/004_refunds.sql`, `scripts/backfill_refunds.py`. A refund becomes a business
event with its own event date, backfilled from orders already loaded with a seeded lag wide
enough that many refunds cross a month boundary. Not in the CSV generator — we are not
regenerating the CSVs.
*Acceptance:* refunds per month beside orders per month, and a count of refunds whose month
differs from their order's month. That count is the size of the restatement problem.

**Step 31 — three revenue metrics that disagree.** ✅ **done 2026-09-21** (`0ee857c`).
Built as `fct_revenue_events` (one row per booking or refund, signed amount, carrying both
`event_date` and `order_date`) plus two metrics — `net_revenue` and `gross_revenue`.

*This step's acceptance criterion, as originally written, was untestable. Twice.* Recorded here
because the mistake is more instructive than the step:

- **"all three resolve for 2026 Q3"** — every order (2026-07-01→09-06) and every refund
  (07-02→09-19) is inside Q3, so restated and as-booked are *identical* at the quarter grain.
  Restatement is invisible at any grain coarse enough to contain both dates: a refund is a
  transfer between two buckets, and if both buckets are in frame it nets to zero. The comparison
  has to happen at the month.
- **"the difference equals Step 30's crossing count"** — it cannot. A July→September refund
  appears in *two* months' differences. The correct identity, per month M:
  `restated(M) − as_booked(M) = refunds landing in M − refunds whose order was in M`. Verified to
  the cent against `shop.refunds` without touching the model.
- **Three metrics was also wrong.** `net_revenue_restated` and `net_revenue_as_booked` are not two
  metrics — they are one sum under two different date dimensions. The names stay alive in Step 33,
  where the choice between them is a policy rather than a column.

*Verified:* monthly gross/restated/as-booked, total restated = total as-booked =
29,496,512.65, difference 0.00. September as-booked is **negative** (−2,793,777.78): six days of
orders absorbing three months of refunds. `dbt build` 29/29.

**Step 32 — make the divergence visible.**
A chart in Lightdash showing restated against as-booked over time. Concept: a number without
a stated as-of date is not a number.
*Acceptance:* the gap is visible without explanation.

**Step 33 — state the policy and enforce it.** ✅ **done 2026-09-21** (`72a319d`, `a9f9843`).
**Official revenue is net revenue by Event Date — as booked.** Closed periods stay closed; a
number already reported never moves. Cost, stated alongside it: a month takes a hit for refunds
on orders it never booked, so a bad refund month reads as a bad sales month. Restated is the same
metric by Order Date, correct for cohort/LTV work, never the headline.

Written inline in `schema.yml` on `fct_revenue_events`, not in a separate doc — the policy sits
where the metric is defined, so it cannot be read without it.

*Enforcement is deletion, not a test.* `total_revenue` (`sum(amount)` filtered to
`status = 'completed'`) was removed from `fct_orders`: it was the restated figure **by accident**
— a refunded order stops being `completed` — sitting in the Lightdash dropdown under a friendly
name. `avg_order_value` was derived from it and moved to `fct_revenue_events` as
`gross_revenue / booked_orders`, gross because what an order was worth when placed should not move
when a refund arrives. New `booked_orders` metric; `total_orders` counts cancelled and pending and
is wrong as a revenue denominator. `gross_amount` kept, relabelled "not revenue".
Honest limit: nothing stops a *new* ambiguous metric being added tomorrow. Deletion removes
today's wrong answer, not tomorrow's. `revenue_events_reconcile_to_completed` is the partial guard.

**Step 33b — refund ingestion gets an owner.** `dags/nova_daily.py`: a `backfill_refunds` task
between the load and `dbt_build`. It had to be before dbt, which reads `shop.refunds` as a source.
Safe daily because the insert is `ON CONFLICT DO NOTHING`. Same question as 33, asked about the
pipeline instead of the metric. **Not yet verified against a running scheduler** — see PROGRESS.md.

*Block 1 (Track A1) is complete.*

## Block 2 — Track A3: late-arriving and backdated data (Steps 34–36)

*Why second: it is the direct continuation of Steps 24 and 29. The lookback window's stated
ceiling — a backfill older than 7 days is silently missed — is now unit-tested, which means
the gap is documented rather than fixed. A3 is the fix.*

Current behaviour is `ON CONFLICT (order_id) DO NOTHING` in
`ingestion/csv_ingestion/fact_loader.py`. That is right for a duplicate delivery and wrong
for a genuine correction, which is discarded without a word.

**Step 34 — make a correction arrive.** A late file that revises rows already loaded, so the
silent discard is observed rather than argued about.

**Step 35 — an explicit update policy.** Probably `DO UPDATE` gated on a source-side version
or `updated_at`. Concept: **idempotency is not the same as immutability**.
*Acceptance:* a corrected row updates, a replayed identical row does not — both tested.

**Step 36 — reconcile with the lookback window.** A correction older than 7 days still sits
below `fct_orders`' window. Decide whether the loader signals a full refresh, or the window
learns about corrections.

## Block 3 — Track A4: multi-grain (Steps 37–39)

`order_items` is seeded and unused. A line-grain fact makes fan-out real — right now
`dim_customers`' uniqueness test guards a bug that cannot occur. `fct_order_lines`, a
documented grain per fact table, and a demonstration of revenue doubling when a header-grain
metric meets a line-grain fact carelessly.

## Block 4 — Track B1 + v2 Phase 4: clickstream and streaming (Steps 40+)

The first block that adds infrastructure, and the first where it is earned.

Seeded clickstream generation (page view → product view → add to cart → checkout → purchase)
at 50–200 events per order, Debezium and Kafka from v2 Phase 4, and a consumer writing
through the existing idempotent load path.

*Acceptance, from the v2 proposal, unchanged:* the streaming path and the batch path over the
same window produce identical totals. Convergence is the proof, not throughput.

## Beyond

Sketched only, in the v3 proposal's order: B2 sessionisation (dbt first, then Flink, and the
comparison is the deliverable), A2 accumulating snapshot, B3/B4 funnel and attribution,
Track E managed ingestion, Track C reliability, Track D serving. Re-plan when Block 4 lands.

## Ground rules carried forward

- One concept per step; roughly 20–40 lines of code. Ship something runnable every step.
- `random.seed(42)` stays load-bearing — CI regenerates the CSVs and depends on byte-identical
  output.
- Every step that changes load or model logic leaves a test behind, and the test is proved to
  fail before it is trusted.
- PROGRESS.md gets a line per completed step, with the concept it taught.
