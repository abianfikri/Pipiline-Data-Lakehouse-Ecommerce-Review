# Folder: spark-apps/spark_consumer_review.py
# ===========================================
# Code untuk consumer dataset review dari Kafka
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0  
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 
# /opt/spark-apps/spark_consumer_delta_review.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, BooleanType, LongType

spark = SparkSession.builder \
    .appName("Review Kafka Consumer With Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType() \
    .add("rating", FloatType()) \
    .add("title", StringType()) \
    .add("text", StringType()) \
    .add("images", StringType()) \
    .add("asin", StringType()) \
    .add("parent_asin", StringType()) \
    .add("user_id", StringType()) \
    .add("timestamp", LongType()) \
    .add("helpful_vote", LongType()) \
    .add("verified_purchase", BooleanType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "review-topic-v6") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = parsed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/opt/data-lake/checkpoint_review_delta") \
    .option("path", "/opt/data-lake/output_review_delta") \
    .outputMode("append") \
    .start()

query.awaitTermination()
