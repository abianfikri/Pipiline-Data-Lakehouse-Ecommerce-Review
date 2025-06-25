# Code untuk convert parquet ke delta format

from pyspark.sql import SparkSession

# Inisialisasi SparkSession dengan dukungan Delta
spark = SparkSession.builder \
    .appName("CreateDeltaTable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Baca data dari Parquet atau sumber lain (misalnya file CSV)
# Misalnya dari Parquet:
df = spark.read.parquet("/opt/data-lake/output_colab")

# Simpan ulang dalam format Delta (overwrite kalau ingin ganti folder tersebut)
df.write.format("delta").mode("overwrite").save("/opt/data-lake/output_colab_revisi")

print("✅ Delta table berhasil dibuat di /opt/data-lake/output_colab")
