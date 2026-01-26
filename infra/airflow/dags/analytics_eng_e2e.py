from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/dbt",
    "PYTHONPATH": "/opt/airflow",
}

with DAG(
    dag_id="analytics_eng_e2e",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["analytics", "e2e"],
) as dag:
    ingest_to_s3 = BashOperator(
        task_id="ingest_to_s3",
        bash_command="python /opt/airflow/src/ingest/ingest_to_s3.py",
        env=DEFAULT_ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/airflow/dbt",
        env=DEFAULT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/dbt",
        env=DEFAULT_ENV,
    )

    ge_checkpoint = BashOperator(
        task_id="ge_checkpoint",
        bash_command=(
            "great_expectations checkpoint run analytics_checkpoint "
            "--config /opt/airflow/great_expectations/great_expectations.yml"
        ),
        env=DEFAULT_ENV,
    )

    generate_bi_exports = BashOperator(
        task_id="generate_bi_exports",
        bash_command="python /opt/airflow/src/ingest/export_daily_metrics.py",
        env=DEFAULT_ENV,
    )

    ingest_to_s3 >> dbt_run >> dbt_test >> ge_checkpoint >> generate_bi_exports
