import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag,task
from ingestion.csv_ingestion.etl import run_etl
from airflow.sdk.bases.hook import BaseHook

logger = logging.getLogger(__name__)

# Where docker/airflow-compose.yml mounts ../data. Deliberately a constant and
# NOT an Airflow Variable: this only changes when the mount changes, which is a
# code change anyway. Variables earn their keep for values that must change
# WITHOUT a deploy — a threshold someone tunes, a feature flag — at the cost of
# being hidden state that git cannot see.
DATA_DIR = Path("/opt/airflow/nova/data")

def alert(context):
	"""Called by Airflow when a task gives up — i.e. after the LAST retry.

	The grep-able marker is deliberate: it is what a log-scraper (or you, with
	`docker logs | grep`) keys on. Swapping this for Slack later means changing
	this function body and nothing else — that is why the alert channel is a
	one-function decision and not an architecture decision.
	"""
	ti = context["task_instance"]
	logger.error(
		"PIPELINE_ALERT dag=%s task=%s logical_date=%s attempt=%s error=%s",
		ti.dag_id,
		ti.task_id,
		context.get("logical_date"),
		ti.try_number,
		context.get("exception"),
	)


@dag(
	dag_id="nova_daily",
	schedule="@daily",
	start_date=datetime(2026,7,1),
	catchup=True,
	max_active_runs=1,
	# default_args is inherited by EVERY task in this DAG, so dbt_build gets the
	# same policy as the load without repeating it.
	default_args={
		# 2 retries = 3 attempts total. Betting the failure is transient.
		"retries": 2,
		# Wait this long before attempt 2.
		"retry_delay": timedelta(minutes=1),
		# Double it each time: 1min, 2min, 4min... instead of hammering a
		# warehouse that is trying to come back up.
		"retry_exponential_backoff": True,
		# The ceiling. Without this, backoff on a long-lived DAG can grow into
		# hours and your alert arrives tomorrow.
		"max_retry_delay": timedelta(minutes=10),
		# Fires once, after retries are exhausted. Inherited by all three tasks.
		"on_failure_callback": alert,
	},
)

def nova_daily():

	@task
	def run_daily_etl(**context):

		conn = BaseHook.get_connection("nova_warehouse")

		os.environ.update({

			"POSTGRES_HOST": conn.host,
			"POSTGRES_PORT": str(conn.port),
			"POSTGRES_DB": conn.schema,
			"POSTGRES_USER": conn.login,
			"POSTGRES_PASSWORD": conn.password,
		})

		logical_date = context["logical_date"].strftime("%Y-%m-%d")

		input_path = DATA_DIR / f"orders_{logical_date}.csv"
		dlq_path = DATA_DIR / f"dead_letter_{logical_date}.jsonl"

		staging_count, fact_count = run_etl(
			input_path=input_path,
			dlq_path=dlq_path,
		)

		print(f"Staging rows :{staging_count}")
		print(f"fact count:{fact_count}")

	# Refund ingestion, given an owner (Step 33b).
	#
	# Until now backfill_refunds.py was a one-shot someone ran by hand. That is
	# fine for a backfill and wrong for a pipeline: every day the load brought
	# in orders with status = 'refunded' and no matching row in shop.refunds,
	# which makes fct_revenue_events understate refunds and turns
	# revenue_events_reconcile_to_completed red. Deciding which metric is
	# official (Step 33) and deciding who fills the table it reads are the same
	# question asked twice.
	#
	# BEFORE dbt_build, deliberately: dbt reads shop.refunds as a source, so the
	# refunds have to exist before the marts are rebuilt, not after.
	#
	# Safe to run every day: the insert is ON CONFLICT (order_id) DO NOTHING, so
	# a rerun writes nothing and already-dated refunds never move. The script
	# re-seeds its RNG each run and walks orders in order_id order, so existing
	# rows are skipped and only genuinely new orders draw a lag.
	#
	# Known limitation, data-generation not pipeline: refund_date_for() never
	# dates a refund in the future, so an order loaded today can only draw a lag
	# of ~0 days. Refunds crossing a month boundary are an artifact of the
	# original backfill against historical orders; the daily path will produce
	# few. If that matters for a demo, widen the window in the script rather
	# than here.
	@task.bash(
		env={
			"POSTGRES_HOST": "{{ conn.nova_warehouse.host }}",
			"POSTGRES_PORT": "{{ conn.nova_warehouse.port }}",
			"POSTGRES_DB": "{{ conn.nova_warehouse.schema }}",
			"POSTGRES_USER": "{{ conn.nova_warehouse.login }}",
			"POSTGRES_PASSWORD": "{{ conn.nova_warehouse.password }}",
		},
		append_env=True,
	)
	def backfill_refunds() -> str:
		# -m, not a path: PYTHONPATH is /opt/airflow/nova, and running the file
		# directly would put scripts/ on sys.path instead of the repo root and
		# fail on `import ingestion`.
		return "python -m scripts.backfill_refunds"

	# @task.bash: the function returns a COMMAND STRING, Airflow runs it.
	# No date in here — dbt rebuilds the marts from whatever is in fact_orders,
	# so this task is not date-parameterised the way the load is.
	# `env` is a TEMPLATE FIELD, so these render at run time — the connection is
	# never read at parse time. Airflow masks connection secrets in the log.
	# append_env=True MERGES into the inherited environment; without it, `env`
	# REPLACES it and dbt loses PATH and DBT_PROFILES_DIR ("dbt: command not found").
	@task.bash(
		env={
			"POSTGRES_HOST": "{{ conn.nova_warehouse.host }}",
			"POSTGRES_PORT": "{{ conn.nova_warehouse.port }}",
			"POSTGRES_DB": "{{ conn.nova_warehouse.schema }}",
			"POSTGRES_USER": "{{ conn.nova_warehouse.login }}",
			"POSTGRES_PASSWORD": "{{ conn.nova_warehouse.password }}",
		},
		append_env=True,
	)
	def dbt_build() -> str:
		return "dbt build --project-dir /opt/airflow/nova/nova_analytics"

	# The quality gate. dbt tests assert SHAPE — unique, not_null, valid
	# categories — and all of them pass on a day that lost 98% of its rows.
	# Soda asserts REALITY: is the data fresh, and is there enough of it.
	# Same templated env as dbt_build; the scan needs the warehouse too.
	# A failing check exits non-zero, which fails the task. That is the gate.
	@task.bash(
		env={
			"POSTGRES_HOST": "{{ conn.nova_warehouse.host }}",
			"POSTGRES_PORT": "{{ conn.nova_warehouse.port }}",
			"POSTGRES_DB": "{{ conn.nova_warehouse.schema }}",
			"POSTGRES_USER": "{{ conn.nova_warehouse.login }}",
			"POSTGRES_PASSWORD": "{{ conn.nova_warehouse.password }}",
		},
		append_env=True,
		# Overrides default_args. Retries are a bet that the failure is
		# transient; a failed quality check is not — the data is short, and
		# rescanning it fails identically three times while the alert sits
		# ~6 minutes behind the problem. dbt_build hits the same warehouse
		# seconds earlier, so a genuine connection blip fails there first.
		retries=0,
	)
	def soda_scan() -> str:
		return "soda scan -d nova -c /opt/airflow/nova/soda/configuration.yml /opt/airflow/nova/soda/checks.yml"

	# The edges. Calling a task returns a handle; >> makes the dependency.
	# Strictly linear: dbt cannot start unless the load succeeded, and the
	# scan cannot start unless dbt rebuilt the marts it reads.
	run_daily_etl() >> backfill_refunds() >> dbt_build() >> soda_scan()

nova_daily()
