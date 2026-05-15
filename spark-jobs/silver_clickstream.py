
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, from_unixtime, to_timestamp, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

bronze_df = spark.read.parquet("hdfs://namenode:9000/data/bronze")

if bronze_df.rdd.isEmpty():
    print("Silver: Bronze layer is empty — nothing to process. Exiting.")
    spark.stop()
    exit(0)


bronze_df = bronze_df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("event_ts")))
)

bronze_df = bronze_df.withColumn(
    "event_date",
    to_date(col("event_time"))
)

# Deduplication: for each (product_id, event_ts) pair keep the row
# with the highest rating_count as a stable tiebreaker.
# Partitioning by BOTH product_id AND event_ts means all rows within
# a partition share the same event_ts — so we need a different ordering
# column; event_ts ordering inside its own partition is a no-op.
window_spec = Window.partitionBy("product_id", "event_ts") \
                    .orderBy(col("rating_count").desc())

silver_df = bronze_df.withColumn(
    "row_num",
    row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")


silver_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("hdfs://namenode:9000/data/silver")

spark.stop()
