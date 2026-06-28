# ============================================
# producer.py — API fetcher + Kafka producer
# ============================================
# Mengambil data dari Satudata Jakarta API / CSV fallback
# lalu mengirimkan ke Kafka topics.
#
# Sesuai Implementation Guide Section 6.3
# Topics: gizi-raw, faskes-raw, nakes-raw, populasi-raw
# ============================================
import json
import os
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "kafka-broker:9092"
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    retries=3,
    retry_backoff_ms=1000
)

DATASET_CONFIG = {
    "gizi-raw": "data/fallback/balita_stunting.csv",
    "faskes-raw": "data/fallback/sebaran_rumahSakit.csv",
    "nakes-raw": "data/fallback/jumlah_nakes.csv",
    "populasi-raw": "data/fallback/lajupertumbuhan.csv"
}


def load_dataset(filepath: str):
    try:
        df = pd.read_csv(filepath)

        print(
            f"[INFO] Loaded {len(df)} rows "
            f"from {filepath}"
        )

        return df.to_dict(orient="records")

    except Exception as e:
        print(
            f"[ERROR] Failed reading "
            f"{filepath}: {e}"
        )
        return []


def produce_to_kafka(topic: str, records: list):
    if not records:
        return

    ingested_at = datetime.utcnow().isoformat()

    for i, record in enumerate(records):

        record["ingested_at"] = ingested_at

        producer.send(
            topic,
            key=str(i),
            value=record
        )

    producer.flush()

    print(
        f"[INFO] Produced "
        f"{len(records)} records "
        f"to topic '{topic}'"
    )


def main():

    print("[INFO] Starting ingestion...")

    for topic, filepath in DATASET_CONFIG.items():

        records = load_dataset(filepath)

        produce_to_kafka(
            topic,
            records
        )

    producer.close()

    print("[INFO] Ingestion complete")


if __name__ == "__main__":
    main()