### Folder: kafka/producer_csv.py
### ===========================================
### Code untuk mengirim data dari CSV ke Kafka
### ===========================================

from kafka import KafkaProducer
import csv
import time
import json
from tqdm import tqdm

producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Hitung total baris (tidak termasuk header)
with open('./datasets/data.csv', 'r') as file:
    total_rows = sum(1 for _ in file) - 1

with open('./datasets/data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in tqdm(reader, total=total_rows, desc="Streaming to Kafka"):
        producer.send('ecommerce-topic-v4', value=row)
        time.sleep(0)  # Jeda 0.1 detik antar pengiriman

print("✅ Semua data berhasil dikirim ke Kafka.")