# Folder: spark-apps/spark_daily_quantity_summary_delta.py
# ===========================================
# Code untuk menghitung total quantity berdasarkan tanggal yang diganti menjadi nama hari
# Hal ini dilakukan untuk mengurangi penggunaan memory dan waktu eksekusi query pada superset
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
# /opt/spark-apps/spark_daily_quantity_summary_delta.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, date_format

# 1. Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("DailyQuantitySummary") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Baca dari Delta Lake
df = spark.read.format("delta").load("/opt/data-lake/output_joined_delta")

# 3. Filter tanggal
filtered_df = df.filter(
    (col("InvoiceDate") >= "2010-01-01") & (col("InvoiceDate") <= "2011-12-31")
)

# 4. Buat kolom day_of_week (nama hari)
day_df = filtered_df.withColumn(
    "day_of_week", date_format(col("InvoiceDate"), "EEEE")
).groupBy("day_of_week").agg(
    _sum("Quantity").alias("total_quantity")
).orderBy(col("total_quantity").desc())

# 5. Simpan hasil ke Delta Lake
day_df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/data-lake/daily_quantity_summary_delta")
# 6. Tampilkan hasil
day_df.show()