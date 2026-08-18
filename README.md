# nova-ecommerce-platform

A learn-by-building data engineering project: ingest e-commerce data into Postgres with
Python, then grow it into a real pipeline one concept at a time.

Postgres runs in Docker. Ingestion is plain Python — no orchestrator yet, deliberately.
Airflow, dbt, and Kafka get added when a step actually needs them, not before.

## Requirements

- Docker with Compose v2 (`docker compose version`)
- Python 3.10+
- `psql` client, optional but useful

## Setup

```bash
git clone <repo-url> nova-ecommerce-platform
cd nova-ecommerce-platform
```

**1. Configure credentials**

```bash
cp .env.example .env
```

Open `.env` and set `POSTGRES_PASSWORD` to something of your own. If port 5432 is already
in use on your machine, change `POSTGRES_PORT` too — only the host side moves, the
container always listens on 5432 internally.

`.env` is gitignored. Keep it that way.

> These values are read by Postgres **only when the data volume is first created**. Changing
> them later has no effect until you wipe the volume (see [Reset](#reset)).

**2. Start the database**

```bash
docker compose up -d --wait
```

`--wait` blocks until the container's healthcheck passes, so when the command returns,
Postgres is genuinely ready for queries.

**3. Python environment**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**4. Run it**

```bash
python ingestion/rest_api/db.py
```

Creates the `products` table, upserts a row, and prints it back. Run it twice — the row
count stays at 1 while the timestamp advances. That's the pipeline being idempotent.

## Daily use

```bash
docker compose up -d --wait      # start
source .venv/bin/activate        # activate
python ingestion/rest_api/db.py  # run

docker compose stop              # finish — data is kept
```

## Verifying

Three layers, each ruling out a different failure:

```bash
docker compose ps                                             # Up (healthy)?
docker compose exec postgres pg_isready -U nova               # accepting connections?
docker compose exec postgres psql -U nova -d nova -c '\dt'    # can you query?
```

From your host, using the values in your `.env`:

```bash
psql -h localhost -p "$POSTGRES_PORT" -U nova -d nova
```

Pass the port explicitly. If you have another Postgres on the default port, omitting it
connects you to the wrong database and your tables will appear to be missing.

## Commands

| Command | Effect |
|---|---|
| `docker compose up -d --wait` | Start, wait until healthy |
| `docker compose ps` | Status |
| `docker compose logs -f postgres` | Live logs — start here when debugging |
| `docker compose stop` | Pause. Data and container kept |
| `docker compose down` | Remove container. **Data kept** |
| `docker compose down -v` | Also delete the volume — **destroys all data** |

`down` is safe; `down -v` is the only command that loses data.

## Troubleshooting

**`permission denied … docker.sock`** — your user isn't in the `docker` group in this shell:
`sudo usermod -aG docker $USER`, then open a new terminal (or `newgrp docker`).

**`Cannot connect to the Docker daemon`** — `sudo systemctl start docker`.

**`address already in use` / `port is already allocated`** — something owns that port. Find
it with `ss -ltnp | grep <port>` and `docker ps`, then set a free `POSTGRES_PORT` in `.env`
and bring it up again.

**`KeyError: 'POSTGRES_…'`** — `.env` is missing or a name is misspelled. `load_dotenv()`
must run before you read `os.environ`.

**`password authentication failed`** — `.env` no longer matches what the volume was built
with. Restore the original password, or reset.

**`relation "products" does not exist`** — the script hasn't run yet, or you're connected to
a different Postgres. Check your port.

### Reset

Destroys the database. This is the only way to change the Postgres user, password, or
database name after first boot:

```bash
docker compose down -v && docker compose up -d --wait
python ingestion/rest_api/db.py
```

## Layout

```
ingestion/
  rest_api/db.py       connect, create products, upsert, read back
  kafka/  postgres_cdc/
sql/  transformation/  dbt/  dags/  tests/  config/  scripts/  notebooks/
docker-compose.yml     Postgres 16 + named volume + healthcheck
requirements.txt       psycopg 3, python-dotenv, requests
```

Most directories are placeholders holding only a `README.md` — Git doesn't track empty
directories, so a one-line README is what makes them survive a clone. Each gets real
content from the step that needs it.
