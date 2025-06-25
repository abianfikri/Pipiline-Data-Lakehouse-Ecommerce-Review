# ===========================================
# Code untuk simpan data ke PostgreSQL dari Delta Lake
# ===========================================
# running menggunakan spark-submit pada terminal dengan perintah:
# docker exec -it spark spark-submit 
# --packages io.delta:delta-core_2.12:2.1.0 --jars /opt/spark-apps/jars/postgresql-42.2.5.jar /opt/spark-apps/delta_to_postgres.py


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaToPostgres") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Baca dari Delta Lake
df = spark.read.format("delta").load("/opt/data-lake/output_ecommerce_delta_revisi")

# Tulis ke PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/superset") \
    .option("dbtable", "ecommerce_data") \
    .option("user", "superset") \
    .option("password", "superset") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
