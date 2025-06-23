import pandas as pd

# Membaca file Parquet hasil output Spark Streaming
import pandas as pd
import pyarrow.parquet as pq

# ./transaksi_clean.parquet || ./data-lake/output_ecommerce_delta

df = pd.read_parquet('./transaksi_clean.parquet', engine='pyarrow')


# Menampilkan informasi dasar
print("✅ File Parquet berhasil dibaca!")
print("🔎 Kolom-kolom tersedia:", df.columns.tolist())
print("🧮 Jumlah baris:", len(df))
print("\n📄 5 Baris pertama:")
print(df.head())
