from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "arun",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# dbt-postgres is installed in the Airflow image (see airflow/Dockerfile).
# dbt_project is mounted read-only at /opt/airflow/dbt_project.
# A writable target dir is created at runtime so dbt can write compiled SQL.
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_TARGET_DIR = "/tmp/dbt_target"

with DAG(
    dag_id="dbt_transform_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 8),
    schedule_interval="@hourly",
    catchup=False,
    tags=["dbt", "postgres", "data-quality"],
    description="Run dbt models and tests on the analytics PostgreSQL database",
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        mkdir -p {DBT_TARGET_DIR} && \
        dbt run \
            --project-dir {DBT_PROJECT_DIR} \
            --profiles-dir {DBT_PROJECT_DIR} \
            --target-path {DBT_TARGET_DIR}
        """,
        env={
            "POSTGRES_HOST":     os.environ.get("POSTGRES_HOST", ""),
            "POSTGRES_PORT":     os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_USER":     os.environ.get("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB1":      os.environ.get("POSTGRES_DB1", "analytics"),
            "PATH":              "/opt/spark/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        dbt test \
            --project-dir {DBT_PROJECT_DIR} \
            --profiles-dir {DBT_PROJECT_DIR} \
            --target-path {DBT_TARGET_DIR}
        """,
        env={
            "POSTGRES_HOST":     os.environ.get("POSTGRES_HOST", ""),
            "POSTGRES_PORT":     os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_USER":     os.environ.get("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB1":      os.environ.get("POSTGRES_DB1", "analytics"),
            "PATH":              "/opt/spark/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        dbt docs generate \
            --project-dir {DBT_PROJECT_DIR} \
            --profiles-dir {DBT_PROJECT_DIR} \
            --target-path {DBT_TARGET_DIR}
        """,
        env={
            "POSTGRES_HOST":     os.environ.get("POSTGRES_HOST", ""),
            "POSTGRES_PORT":     os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_USER":     os.environ.get("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB1":      os.environ.get("POSTGRES_DB1", "analytics"),
            "PATH":              "/opt/spark/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    dbt_run >> dbt_test >> dbt_docs_generate
