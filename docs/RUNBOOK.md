# Runbook — running Nova Analytics from scratch

Every command needed to go from a fresh clone to a working dashboard, in order.
Companion to `docs/ARCHITECTURE.md`, which explains why each piece exists.

Each phase ends with a **Check**. Don't move on until it passes.

---

## 0. Prerequisites

| Requirement | Notes |
|---|---|
| Docker + Docker Compose | |
| Python 3.12+ | |
| ~4 GB free disk | Lightdash images are large |
| Ports free | 5434, 8080, 9000, 9001 |

**If you use a VPN:** Mullvad's kill switch blocks all host→container traffic, which looks like a
Postgres failure but isn't. Fix once:

```bash
mullvad lan set allow
mullvad lan get            # MUST print "allow" — the set command can silently fail
```

---

## 1. Environment

```bash
cd ~/nova-ecommerce-platform

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` in the project root (never committed):

```bash
cat > .env <<'EOF'
POSTGRES_USER=nova
POSTGRES_PASSWORD=changeme
POSTGRES_DB=nova
POSTGRES_PORT=5434
POSTGRES_HOST=localhost
EOF
```

Port **5434** avoids colliding with a native Postgres on 5432.

---

## 2. Start the warehouse

```bash
docker compose up -d --wait
```

**Check:**

```bash
docker compose ps
# → nova-postgres  Up (healthy)
```

> Any `psql` command below can be run inside the container instead, which bypasses host
> networking entirely — useful when the VPN is on:
> `docker compose exec -T postgres psql -U nova -d nova -c '<SQL>'`

---

## 3. Create the schema

```bash
docker compose exec -T postgres psql -U nova -d nova < sql/ddl/001_shop.sql
docker compose exec -T postgres psql -U nova -d nova < sql/ddl/002_seed.sql
docker compose exec -T postgres psql -U nova -d nova < sql/migrations/003_orders_etl.sql
```

**Check:**

```bash
docker compose exec -T postgres psql -U nova -d nova -c '\dt shop.*'
# → customers, products, orders, order_items, stg_orders, fact_orders
```

---

## 4. Generate the data

CSVs are gitignored because they're deterministic output (`random.seed(42)`), so a fresh clone has
an empty `data/` and must generate them.

```bash
python scripts/generte_orders_data.py
```

**Check:**

```bash
ls data/orders_2026-*.csv | wc -l      # → 57  (56 generated + the _bad fixture)
head -2 data/orders_2026-07-01.csv     # → a seed-catalog name like "27in 1440p Monitor"
```

---

## 5. Backfill all 56 days

```bash
# Start clean — safe to rerun at any time.
docker compose exec -T postgres psql -U nova -d nova -c \
  'TRUNCATE shop.stg_orders, shop.fact_orders;'
rm -f data/dead_letter_2026-*.jsonl     # the DLQ appends; clear stale rejects first

for d in $(python -c "
import datetime as dt
s = dt.date(2026, 7, 1)
print('\n'.join(str(s + dt.timedelta(days=i)) for i in range(56)))"); do
  echo "=== $d"
  python -m ingestion.csv_ingestion.etl "$d" || break
done
```

Takes roughly 1–2 minutes. `|| break` stops at the first failure rather than pressing on.

**Check:**

```bash
docker compose exec -T postgres psql -U nova -d nova \
  -c 'SELECT count(*), count(DISTINCT order_date::date) FROM shop.fact_orders;' \
  -c 'SELECT count(*) FROM shop.stg_orders;'
```

Expect **124,322 rows across 56 days**, and staging holding **one day's rows** (~1,600–3,000).
Staging showing ~124k means the truncate isn't running.

**Check idempotency** — rerun a loaded date and confirm the count does not move:

```bash
python -m ingestion.csv_ingestion.etl 2026-07-15
docker compose exec -T postgres psql -U nova -d nova -c 'SELECT count(*) FROM shop.fact_orders;'
# → still 124,322
```

---

## 6. Configure dbt

`~/.dbt/profiles.yml`:

```yaml
nova_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5434
      user: nova
      pass: changeme
      dbname: nova
      schema: analytics      # NOT shop — dbt builds into its own schema
      threads: 2
```

**Check:**

```bash
cd nova_analytics && dbt debug
# → All checks passed!
```

`Connection refused` → container down or wrong port.
`server closed the connection unexpectedly` → the VPN is blocking; see step 0.

---

## 7. Build and test the models

```bash
cd nova_analytics
dbt build          # runs models AND tests
```

**Check:** `PASS=10` — 2 models, 8 data tests.

```bash
docker compose exec -T postgres psql -U nova -d nova -c '\dv analytics.*'
# → fct_orders, dim_customers
```

dbt creates the `analytics` schema itself if it doesn't exist.

### Optional: watch a test catch bad data

Editing a CSV won't work — pydantic rejects a bad status before it reaches the warehouse, which is
the point of having two layers. To exercise the dbt test, insert past the validator:

```bash
docker compose exec -T postgres psql -U nova -d nova -c "
INSERT INTO shop.fact_orders VALUES
('ORD-BAD-001', 1, 1, 'Mechanical Keyboard 75%', 'peripherals',
 1, 129.00, 129.00, 'shipped', '2026-07-15 12:00:00+00', now(), 'manual');"

cd nova_analytics && dbt test        # → accepted_values FAILS, 1 result

# clean up
docker compose exec -T postgres psql -U nova -d nova -c \
  "DELETE FROM shop.fact_orders WHERE order_id = 'ORD-BAD-001';"
```

---

## 8. Start Lightdash

```bash
docker compose -f docker/lightdash-compose.yml up -d --wait
```

First run pulls several GB and migrates the metadata database — allow a few minutes.

**Check:**

```bash
curl -s http://localhost:8080/api/v1/health | head -c 120
# → {"status":"ok","results":{"healthy":true,...
```

If the pull stalls at "Pulling fs layer" with no byte progress, restart the Docker daemon
(`sudo systemctl restart docker`) — stale daemon state after a VPN disconnect causes exactly this.

---

## 9. Configure Lightdash

Open **http://localhost:8080**.

1. **Create the admin user** — first account becomes org owner.
2. **Select warehouse:** PostgreSQL.
3. **Warehouse connection:**

   | Field | Value |
   |---|---|
   | Host | `postgres` |
   | Port | `5432` |
   | Database | `nova` |
   | User | `nova` |
   | Password | `changeme` |
   | Schema | `analytics` |

   `postgres:5432` is the container hostname and internal port. `localhost:5434` is what *you*
   use from a terminal and will not work from inside a container.

4. **dbt project:** choose **dbt local files** (not GitHub — that routes through Lightdash's hosted
   OAuth broker and fails for self-hosted instances).

   | Field | Value |
   |---|---|
   | Project directory path | `/usr/app/dbt` |
   | Target name | `dev` |
   | Schema | `analytics` |

   Note this page has **two** sections — a dbt connection *and* a warehouse connection. Both need
   filling in; the second defaults to `localhost` and must be changed to `postgres`.

5. **Test and deploy.** All three steps should go green.

**Check:** open `fct_orders`. The sidebar should list metrics `total_revenue`, `gross_amount`,
`total_orders`, `total_quantity`, `avg_order_value`, and `order_date` should offer
day/week/month granularity.

---

## 10. Build the dashboard

Each chart follows the same loop: **pick metrics → pick dimensions → Run query → Charts tab →
Save**. A metric with no dimension is a single total; adding a dimension splits it into groups.

| Chart | Dimension | Metric | Visualization |
|---|---|---|---|
| Total Revenue | — | `total_revenue` | Big value |
| Total Orders | — | `total_orders` | Big value |
| Avg Order Value | — | `avg_order_value` | Big value |
| Items Sold | — | `total_quantity` | Big value |
| Revenue Trend | `order_date` → Week | `total_revenue` | Line |
| Revenue by Category | `category` | `total_revenue` | Bar |
| Top 10 Products | `product_name` | `total_revenue` | Horizontal bar, sort desc, limit 10 |
| Orders by Status | `status` | `total_orders` | Donut |
| Top 10 Customers | `dim_customers.name` | `total_revenue` | Horizontal bar, sort desc, limit 10 |

Then **Dashboards → New dashboard**, add the saved charts, and add a dashboard-level filter on
`order_date` so one control drives every tile.

---

## 11. Final verification

```bash
docker compose exec -T postgres psql -U nova -d nova -c "
SELECT
  sum(amount) FILTER (WHERE status='completed') AS total_revenue,
  sum(amount)                                   AS gross_amount,
  count(DISTINCT order_id)                      AS total_orders,
  sum(quantity)                                 AS items_sold
FROM analytics.fct_orders;"
```

Dashboard tiles must match:

| Tile | Expected |
|---|---|
| Total Revenue | 23,182,141.71 |
| Total Orders | 124,322 |
| Avg Order Value | 186.47 |
| Items Sold | 372,388 |

Revenue by Category must show **five** categories with no `Electronics`:

```
furniture 9,848,766.00 · displays 8,297,587.78 · audio 2,728,395.20
peripherals 1,690,351.50 · accessories 617,041.23
```

Gross (92.7M) being ~4× revenue (23.2M) is correct — only about a quarter of generated orders are
`completed`, and `total_revenue` filters to those. If the two are equal, the metric filter isn't
applying.

---

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Connection refused` on 5434 | Container down, or `.env` port ≠ published port |
| `server closed the connection unexpectedly` | VPN blocking host→container — `mullvad lan set allow`, then verify with `lan get` |
| `docker pull` stalls at "Pulling fs layer" | Stale daemon state — `sudo systemctl restart docker` |
| `dbt` can't find profile | Run dbt from inside `nova_analytics/` |
| dbt model missing in psql | Looking in `shop`; it builds into `analytics` |
| `Too many rejected rows` | >1% invalid in that CSV — inspect `data/dead_letter_<date>.jsonl` |
| Lightdash: `Failed to run "dbt deps"` | dbt project mount is `:ro`, or SELinux — the mount needs `:z` on Fedora |
| Lightdash: `ECONNREFUSED 127.0.0.1:5432` | Warehouse host is `localhost`; must be `postgres` |
| Lightdash: S3 ParseError on boot | MinIO not running, or the `S3_*` env vars are missing |
| Lightdash: "Join to dim_customers skipped" | The model has no entry in `schema.yml` — a model is invisible to Lightdash without one |
| GitHub connect → "Bad request: missing parameter" | GitHub integration doesn't work self-hosted; use **dbt local files** |

---

## Shutting down

```bash
docker compose -f docker/lightdash-compose.yml down    # add -v to wipe dashboards
docker compose down                                    # add -v to wipe warehouse data
```

Without `-v`, named volumes persist and everything comes back on the next `up`.
