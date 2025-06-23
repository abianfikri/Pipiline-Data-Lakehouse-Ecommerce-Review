 E-Commerce Review & Sales Analytics Pipeline

Proyek akhir mata kuliah MDIK  
Membangun pipeline analitik untuk menggabungkan data transaksi e-commerce dan ulasan pelanggan menggunakan teknologi big data.


# Deskripsi Proyek

Proyek ini bertujuan untuk:

- Menggabungkan data penjualan (CSV) dan ulasan produk (JSON)
- Menganalisis hubungan antara sentimen pelanggan dan penjualan
- Mendeteksi produk berisiko churn
- Menyediakan dashboard interaktif berbasis Superset

Arsitektur & Teknologi

- Apache Kafka — ingestion streaming
- Apache Spark + Delta Lake — pemrosesan & penyimpanan batch
- Apache Hive — data warehouse
- Apache Superset — visualisasi data & dashboard interaktif

Alur Pipeline

1. File transaksi (CSV) & ulasan (JSON) dikirim ke Kafka
2. Spark Streaming menyimpan data mentah ke Delta Lake
3. Spark Batch melakukan pembersihan & transformasi data
4. Data kurasi dimuat ke Hive → ditampilkan di Superset

  Fitur Dashboard

- Top Produk: Bar chart `Top 10 products by total_sales
- Analisis Sentimen: Korelasi `review_count vs total_sales
- Alert Otomatis: Produk dengan `avg_rating < 3.5

 Struktur Project
 Berikut penjelasan struktur proyek dalam bentuk **narasi tertulis** agar bisa kamu masukkan ke README tanpa blok kode:

---

# Struktur Proyek (Penjelasan)

# Konfigurasi File .env

Proyek ini membutuhkan file .env untuk menyimpan URL file dataset eksternal yang perlu diunduh secara manual. File .env ini digunakan sebagai referensi lokasi data mentah sebelum dijalankan dalam pipeline.

Berikut adalah isi file .env yang perlu dibuat di root folder proyek:

# URL dataset produk handmade
HANDMADE_PRODUCT_DATASET_URL="https://drive.google.com/file/d/1IenghjCb33L_F0DUtvufllWcQd_YSXcG/view?usp=drive_link"

# URL struktur data lake
DATA_LAKE_URL="https://drive.google.com/file/d/1UUTJqiz8EVI5-ygIvP_-PNlXxcRlACr_/view?usp=drive_link"

Cara Menggunakan:

   Buat file baru bernama .env di root folder proyek.

   Salin isi di atas ke dalam file .env.

   Unduh file secara manual melalui link Google Drive di atas.

   Ekstrak hasil unduhan ke dalam folder:
        datasets/ untuk data produk handmade.
        data-lake/ untuk struktur data lake yang diperlukan oleh Hive/Spark.
    Pastikan struktur folder sudah benar sebelum menjalankan pipeline, agar proses ingestion dan analisis berjalan tanpa error.

# Proyek ini memiliki beberapa direktori dan file utama sebagai berikut:

* docker-compose.yml`
  Berisi konfigurasi untuk menjalankan seluruh layanan (Kafka, Spark, Hive, Superset, dsb.) menggunakan Docker secara terintegrasi.

* Folder scripts/`
  Menyimpan kode program Spark dalam bahasa Scala, terdiri dari:

  * ingestion.scala: menangani pengambilan data dari Kafka dan penyimpanan awal ke Delta Lake.
  * transform.scala`: melakukan proses pembersihan, transformasi, dan integrasi data.

* *Foldersql/`
  Berisi query SQL untuk ETL di Hive dan analitik, seperti pembuatan tabel, partisi, dan agregasi data.

* Folder data/`
  Menyimpan file dataset mentah:

  * transactions.csv`: berisi data transaksi e-commerce.
  * reviews.json`: berisi data ulasan pelanggan.
 

* `README.md
  Dokumentasi utama proyek yang menjelaskan deskripsi, teknologi, cara kerja pipeline, cara instalasi, serta insight bisnis.




        
   
