# Folder: spark-apps/spark_consumer_delta.py
# ===========================================
# Code untuk consumer data dari Kafka
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0  
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 
# /opt/spark-apps/spark_consumer_delta.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, udf
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

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

# Cleaning dan Konversi tipe data

# Definisi UDF untuk konversi aman
def to_int_safe(x):
    try:
        return int(float(x))
    except:
        return 0
    
def to_float_safe(x):
    try:
        return float(x)
    except:
        return 0.0
    
# Konversi tipe data
to_int = udf(to_int_safe, IntegerType())
to_float = udf(to_float_safe, DoubleType())

# Cleaning data dengan UDF
cleaned_df = parsed_df \
    .withColumn("Quantity", to_int(col("Quantity"))) \
    .withColumn("UnitPrice", to_float(col("UnitPrice"))) \
    .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))



# Menulis stream ke Delta Lake
query = cleaned_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/opt/data-lake/checkpoint_ecommerce_delta_revisi") \
    .option("path", "/opt/data-lake/output_ecommerce_delta_revisi") \
    .outputMode("append") \
    .start()

query.awaitTermination()
