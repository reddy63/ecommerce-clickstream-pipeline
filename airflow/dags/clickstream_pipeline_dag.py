import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "arun",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="clickstream_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 8),
    schedule_interval="@hourly",
    catchup=False,
    tags=["spark", "hdfs", "postgres"],
) as dag:

    silver_job = BashOperator(
        task_id="silver_layer",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/silver_clickstream.py
        """
    )

    gold_job = BashOperator(
        task_id="gold_layer",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/airflow/jobs/gold_clickstream.py
        """
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --packages org.postgresql:postgresql:42.7.3 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.executorEnv.POSTGRES_HOST=$POSTGRES_HOST \
        --conf spark.executorEnv.POSTGRES_PORT=$POSTGRES_PORT \
        --conf spark.executorEnv.POSTGRES_DB1=$POSTGRES_DB1 \
        --conf spark.executorEnv.POSTGRES_USER=$POSTGRES_USER \
        --conf spark.executorEnv.POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
        --conf spark.yarn.appMasterEnv.POSTGRES_HOST=$POSTGRES_HOST \
        --conf spark.yarn.appMasterEnv.POSTGRES_PORT=$POSTGRES_PORT \
        --conf spark.yarn.appMasterEnv.POSTGRES_DB1=$POSTGRES_DB1 \
        --conf spark.yarn.appMasterEnv.POSTGRES_USER=$POSTGRES_USER \
        --conf spark.yarn.appMasterEnv.POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
        /opt/airflow/jobs/gold_to_postgres.py
        """,
        env={
            "POSTGRES_HOST": "{{ var.value.get('POSTGRES_HOST', '') or __import__('os').environ.get('POSTGRES_HOST', '') }}",
            "POSTGRES_PORT": "{{ var.value.get('POSTGRES_PORT', '') or __import__('os').environ.get('POSTGRES_PORT', '5432') }}",
            "POSTGRES_DB1": "{{ var.value.get('POSTGRES_DB1', '') or __import__('os').environ.get('POSTGRES_DB1', '') }}",
            "POSTGRES_USER": "{{ var.value.get('POSTGRES_USER', '') or __import__('os').environ.get('POSTGRES_USER', '') }}",
            "POSTGRES_PASSWORD": "{{ var.value.get('POSTGRES_PASSWORD', '') or __import__('os').environ.get('POSTGRES_PASSWORD', '') }}",
        }
    )

    silver_job >> gold_job >> gold_to_postgres
