# Folder: spark-apps/spark_summary_rating.py
# ===========================================
# Code untuk menghitung rata-rata rating berdasarkan nama produk
# Hal ini dilakukan untuk mengurangi penggunaan memory dan waktu eksekusi query pada superset
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
# /opt/spark-apps/spark_summary_rating.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, when

# 1. Inisialisasi SparkSession dengan Delta
spark = SparkSession.builder \
    .appName("DeltaAggregateToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Baca data dari Delta Lake hasil join
df = spark.read.format("delta").load("/opt/data-lake/output_joined_delta")

# 3. Filter berdasarkan InvoiceDate
filtered_df = df.filter(
    (col("InvoiceDate") >= "2010-01-01") & (col("InvoiceDate") <= "2011-12-31")
)

# 4. Agregasi: group by Description, hitung avg dan count
agg_df = filtered_df.groupBy("Description").agg(
    round(avg("rating"), 2).alias("avg_rating"),
    count("rating").alias("review_count")
)

# 5. Kategorisasi berdasarkan avg_rating
filtered_categorized_df = agg_df.filter(
    col("review_count") >= 5
).withColumn(
    "rating_category",
    when(col("avg_rating") >= 4.5, "Good").otherwise("Bad")
)

# 6. Urutkan hasilnya berdasarkan avg_rating
result_df = filtered_categorized_df.orderBy(col("avg_rating").desc())


# 7. Simpan hasil ke Delta Lake
result_df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/data-lake/summary_rating_per_description_revisi")
