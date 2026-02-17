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
        /opt/airflow/jobs/gold_to_postgres.py
        """
    )

    silver_job >> gold_job >> gold_to_postgres
