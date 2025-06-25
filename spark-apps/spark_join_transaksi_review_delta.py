# Folder: spark-apps/spark_join_transaksi_review_delta.py
# ===========================================
# Code untuk melakukan join DATATSET transaksi dan review
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
# /opt/spark-apps/spark_join_transaksi_review_delta.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, udf
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("MergeCleanDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ======================
# 0. Skema transaksi Kafka
# ======================
standard_schema = StructType() \
    .add("InvoiceNo", StringType()) \
    .add("StockCode", StringType()) \
    .add("Description", StringType()) \
    .add("Quantity", StringType()) \
    .add("InvoiceDate", StringType()) \
    .add("UnitPrice", StringType()) \
    .add("CustomerID", StringType()) \
    .add("Country", StringType())

# ======================
# 1. Streaming dari Kafka
# ======================
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "ecommerce-topic-v4") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_kafka_df = raw_kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), standard_schema).alias("data")) \
    .select("data.*")

# ======================
# 2. Pembersihan dan konversi tipe data
# ======================
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

to_int_udf = udf(to_int_safe, IntegerType())
to_float_udf = udf(to_float_safe, DoubleType())

# Format InvoiceDate dari string "12/1/2010 8:26" → to_date("MM/d/yyyy H:mm")
clean_kafka_df = parsed_kafka_df \
    .withColumn("Quantity", to_int_udf(col("Quantity"))) \
    .withColumn("UnitPrice", to_float_udf(col("UnitPrice"))) \
    .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))# konversi ke DateType

# ======================
# 3. Static Delta Review
# ======================
review_df = spark.read.format("delta").load("/opt/data-lake/output_colab_revisi")

clean_review_df = review_df \
    .withColumn("user_id", col("user_id").cast(StringType()))

# ======================
# 4. Join dan Simpan ke Delta
# ======================
def write_to_delta_join(batch_df, batch_id):
    joined_df = batch_df.join(
        clean_review_df,
        batch_df.CustomerID == clean_review_df.user_id,
        how="inner"  # atau "left" kalau mau tetap simpan transaksi meskipun tidak ada review
    )

    # Simpan hasil join ke Delta
    joined_df.write.format("delta") \
        .mode("append") \
        .save("/opt/data-lake/output_joined_delta")

# ======================
# 5. Eksekusi streaming
# ======================
query = clean_kafka_df.writeStream \
    .foreachBatch(write_to_delta_join) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/data-lake/checkpoint_joined_delta") \
    .start()

query.awaitTermination()
