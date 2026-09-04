import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag,task
from ingestion.csv_ingestion.etl import run_etl
from airflow.sdk.bases.hook import BaseHook

logger = logging.getLogger(__name__)

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
	catchup=False,
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
		# Fires once, after retries are exhausted. Inherited by both tasks.
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

		input_path = Path("/opt/airflow/nova/data") / f"orders_{logical_date}.csv"
		dlq_path = Path("/opt/airflow/nova/data") / f"dead_letter_{logical_date}.jsonl"

		staging_count, fact_count = run_etl(
			input_path=input_path,
			dlq_path=dlq_path,
		)

		print(f"Staging rows :{staging_count}")
		print(f"fact count:{fact_count}")

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

	@task.bash(
		env={
			"POSTGRES_HOST": "{{ conn.nova_warehouse.host }}",
                        "POSTGRES_PORT": "{{ conn.nova_warehouse.port }}",
                        "POSTGRES_DB": "{{ conn.nova_warehouse.schema }}",
                        "POSTGRES_USER": "{{ conn.nova_warehouse.login }}",
                        "POSTGRES_PASSWORD": "{{ conn.nova_warehouse.password }}",
		},
		append_env=True
	)
	def soda_scan() -> str:
		return "soda scan -d nova -c /opt/airflow/nova/soda/configuration.yml /opt/airflow/nova/soda/checks.yml" 

	# The edge. Calling a task returns a handle; >> makes the dependency.
	# dbt now cannot start unless the load succeeded.
	run_daily_etl() >> dbt_build() >> soda_scan()

nova_daily()
