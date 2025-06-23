# Folder: spark-apps/spark_consumer_delta.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Inisialisasi SparkSession dengan dukungan Delta Lake
spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingWithDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Definisi schema data dari Kafka
schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", StringType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", StringType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# Membaca data stream dari Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "ecommerce-topic-v4") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsing JSON dari Kafka value
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Menulis stream ke Delta Lake
query = parsed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/opt/data-lake/checkpoint_ecommerce_delta") \
    .option("path", "/opt/data-lake/output_ecommerce_delta") \
    .outputMode("append") \
    .start()

query.awaitTermination()
