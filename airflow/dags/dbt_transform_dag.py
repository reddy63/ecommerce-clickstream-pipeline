from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "arun",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_transform_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 8),
    schedule_interval="@hourly",
    catchup=False,
    tags=["dbt", "postgres", "data-quality"],
    description="Run dbt models and tests on the analytics database",
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        docker exec dbt dbt run --profiles-dir /dbt
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        docker exec dbt dbt test --profiles-dir /dbt
        """
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
        docker exec dbt dbt docs generate --profiles-dir /dbt
        """
    )

    dbt_run >> dbt_test >> dbt_docs_generate
