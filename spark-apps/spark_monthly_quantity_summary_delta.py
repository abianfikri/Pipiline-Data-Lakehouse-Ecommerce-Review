# Folder: spark-apps/spark_monthly_quantity_summary_delta.py
# ===========================================
# Code untuk menghitung total quantity berdasarkan bulan
# Hal ini dilakukan untuk mengurangi penggunaan memory dan waktu eksekusi query pada superset
# ===========================================
# running menggunakan spark-submit pada terminal
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
# /opt/spark-apps/spark_monthly_quantity_summary_delta.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, date_trunc, date_format

# 1. Inisialisasi SparkSession dengan Delta
spark = SparkSession.builder \
    .appName("MonthlyQuantitySummary") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Baca data dari Delta Lake
df = spark.read.format("delta").load("/opt/data-lake/output_joined_delta")

# 3. Filter berdasarkan InvoiceDate
filtered_df = df.filter(
    (col("InvoiceDate") >= "2010-01-01") & (col("InvoiceDate") <= "2011-12-31")
)

# 4. Buat kolom bulan ('YYYY-MM') dan agregasi quantity
monthly_df = filtered_df.withColumn(
    "month", date_format(date_trunc("month", col("InvoiceDate")), "yyyy-MM")
).groupBy("month").agg(
    _sum("Quantity").alias("total_quantity")
).orderBy("month")

# 5. Simpan ke Delta Lake
monthly_df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/data-lake/monthly_quantity_summary_delta")
