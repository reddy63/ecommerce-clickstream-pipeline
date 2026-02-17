
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, from_unixtime, to_timestamp, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

bronze_df = spark.read.parquet("hdfs://namenode:9000/data/bronze")


bronze_df = bronze_df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("event_ts")))
)

bronze_df = bronze_df.withColumn(
    "event_date",
    to_date(col("event_time"))
)

window_spec = Window.partitionBy("product_id", "event_ts") \
                    .orderBy(col("event_ts").desc())

silver_df = bronze_df.withColumn(
    "row_num",
    row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")


silver_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("hdfs://namenode:9000/data/silver")

spark.stop()
