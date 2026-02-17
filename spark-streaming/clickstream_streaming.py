
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("ClickstreamBronze")
    .getOrCreate()
)

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_ts", LongType(), True)
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "clickstream")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
)

query = (
    parsed.writeStream
    .format("parquet")
    .option("path", "hdfs://namenode:9000/data/bronze")
    .option("checkpointLocation", "hdfs://namenode:9000/data/checkpoints/bronze")
    .outputMode("append")
    .start()
)

query.awaitTermination()
