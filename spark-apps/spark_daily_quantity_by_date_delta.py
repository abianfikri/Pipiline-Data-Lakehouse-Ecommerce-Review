# Folder: spark-apps\spark_daily_quantity_by_date_delta.py
# ===========================================
# Code untuk menghitung total quantity berdasarkan tanggal
# Hal ini dilakukan untuk mengurangi penggunaan memory dan waktu eksekusi query pada superset
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
# /opt/spark-apps/spark_daily_quantity_by_date_delta.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, date_format

# 1. Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("DailyQuantityExactDate") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Load data dari Delta Lake
df = spark.read.format("delta").load("/opt/data-lake/output_joined_delta")

# 3. Filter berdasarkan InvoiceDate
filtered_df = df.filter(
    (col("InvoiceDate") >= "2010-01-01") & (col("InvoiceDate") <= "2011-12-31")
)

# 4. Tambahkan kolom tanggal 'YYYY-MM-DD' dan agregasi
daily_df = filtered_df.withColumn(
    "date", date_format(col("InvoiceDate"), "yyyy-MM-dd")
).groupBy("date").agg(
    _sum("Quantity").alias("total_quantity")
).orderBy("date")

# 5. Simpan hasil ke Delta Lake
daily_df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/data-lake/daily_quantity_by_date_delta")
