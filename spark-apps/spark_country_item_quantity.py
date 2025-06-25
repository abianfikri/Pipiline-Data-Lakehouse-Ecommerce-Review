# Folder: spark-apps/spark_country_item_quantity.py
# ===========================================
# Code untuk menghitung total quantity berdasarkan negara dan item
# Hal ini dilakukan untuk mengurangi penggunaan memory dan waktu eksekusi query pada superset
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit --packages io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0  
# --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
# --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 
# /opt/spark-apps/spark_country_item_quantity.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# 1. Inisialisasi SparkSession dengan Delta Lake
spark = SparkSession.builder \
    .appName("CountryItemQuantityToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Baca Delta hasil join (transaksi + review)
df = spark.read.format("delta").load("/opt/data-lake/output_joined_delta")

# 3. Filter InvoiceDate sesuai rentang waktu
filtered_df = df.filter(
    (col("InvoiceDate") >= "2010-01-01") & (col("InvoiceDate") <= "2011-12-31")
)

# 4. Group by Country, agregasi total Quantity
agg_df = filtered_df.groupBy("Country").agg(
    sum("Quantity").alias("total_quantity")
).orderBy(col("total_quantity").desc())

# 5. Simpan ke Delta Lake (output baru)
agg_df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/data-lake/top_country_item_quantity")
