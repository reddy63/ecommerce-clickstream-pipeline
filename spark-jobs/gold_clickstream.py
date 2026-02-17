

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder \
    .appName("GoldClickstream") \
    .getOrCreate()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

silver_df = spark.read.parquet("hdfs://namenode:9000/data/silver")

gold_df = (
    silver_df
    .groupBy("category", "event_date")
    .agg(
        count("*").alias("total_events"),
        sum("price").alias("total_revenue")
    )
)

gold_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("hdfs://namenode:9000/data/gold")

spark.stop()
