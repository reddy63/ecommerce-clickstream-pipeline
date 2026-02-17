
import os
from pyspark.sql import SparkSession

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB1 = os.getenv("POSTGRES_DB1")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


spark = SparkSession.builder \
    .appName("GoldToPostgres") \
    .getOrCreate()

gold_df = spark.read.parquet("hdfs://namenode:9000/data/gold")
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB1}"

gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "clickstream_metrics") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()
