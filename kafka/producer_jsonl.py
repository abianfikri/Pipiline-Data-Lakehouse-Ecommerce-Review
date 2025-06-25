### Folder: kafka/producer_jsonl.py
### ===========================================
### Code untuk mengirim data dari JSONL ke Kafka
### ===========================================

from kafka import KafkaProducer
import json
import time
from tqdm import tqdm

producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Baca seluruh baris file JSONL
with open('./datasets/Handmade_Products.jsonl', 'r') as file:
    lines = file.readlines()

# Kirim ke Kafka dengan tqdm progress bar
for i, line in enumerate(tqdm(lines, desc="Streaming JSONL to Kafka")):
    data = json.loads(line.strip())
    producer.send('review-topic-v6', value=data)
    print(f"Sent ({i+1}/{len(lines)}):", data.get('title', '[No Title]'))
    time.sleep(0)  # Jeda 0.1 detik antar pengiriman

print("✅ Semua data JSONL berhasil dikirim ke Kafka.")