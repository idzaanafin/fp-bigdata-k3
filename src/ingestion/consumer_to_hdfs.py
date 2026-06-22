# ============================================
# consumer_to_hdfs.py — Kafka consumer → HDFS Bronze
# ============================================
# Mengkonsumsi data dari Kafka topics dan menulis
# sebagai Parquet ke HDFS Bronze layer.
#
# Sesuai Implementation Guide Section 6.4
# ============================================
import json
import math
import os
import subprocess
import tempfile
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020")

TOPIC_CONFIG = {
    "gizi-raw": {"hdfs_path": f"{HDFS_NAMENODE}/data/bronze/gizi"},
    "faskes-raw": {"hdfs_path": f"{HDFS_NAMENODE}/data/bronze/faskes"},
    "nakes-raw": {"hdfs_path": f"{HDFS_NAMENODE}/data/bronze/nakes"},
    "populasi-raw": {"hdfs_path": f"{HDFS_NAMENODE}/data/bronze/populasi"},
}

BATCH_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def sanitize_record(rec: dict) -> dict:
    for k, v in rec.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            rec[k] = None
    return rec


def upload_to_hdfs(local_path: str, hdfs_target: str) -> bool:
    hdfs_dir = hdfs_target.rsplit("/", 1)[0]

    subprocess.run(
        ["hadoop", "fs", "-rm", "-r", "-f", hdfs_dir],
        check=False, capture_output=True
    )

    subprocess.run(
        ["hadoop", "fs", "-mkdir", "-p", hdfs_dir],
        check=False, capture_output=True
    )

    result = subprocess.run(
        ["hadoop", "fs", "-put", local_path, hdfs_target],
        check=False, capture_output=True, text=True
    )

    if result.returncode != 0:
        print(f"[ERROR] HDFS put failed: {result.stderr.strip()}")
        return False
    return True


def write_topic_to_hdfs(records: list, topic: str) -> bool:
    hdfs_path = TOPIC_CONFIG[topic]["hdfs_path"]

    if not records:
        print(f"[WARNING] No records for '{topic}', skip")
        return True

    records = [sanitize_record(r) for r in records]
    table = pa.Table.from_pylist(records)
    filename = f"ingest_{BATCH_TIMESTAMP}.parquet"

    with tempfile.NamedTemporaryFile(
        suffix=".parquet", delete=False
    ) as tmp:
        pq.write_table(table, tmp.name, compression="snappy")
        temp_file = tmp.name

    target = f"{hdfs_path}/{filename}"
    success = upload_to_hdfs(temp_file, target)
    os.remove(temp_file)

    if success:
        print(f"[INFO] {topic}: {len(records)} records → {target}")
    return success


def consume_all():
    consumer = KafkaConsumer(
        *TOPIC_CONFIG.keys(),
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        group_id=f"hdfs-writer-{BATCH_TIMESTAMP}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=15000,
        enable_auto_commit=False
    )

    buffers = {t: [] for t in TOPIC_CONFIG}

    print("[INFO] Consuming from Kafka topics "
          f"({', '.join(TOPIC_CONFIG.keys())})...")

    msg_count = 0
    for message in consumer:
        topic = message.topic
        record = message.value
        record.setdefault("source", "csv_fallback")
        record.setdefault("ingested_at", datetime.utcnow().isoformat())
        record["ingested_batch"] = BATCH_TIMESTAMP
        buffers[topic].append(record)
        msg_count += 1

        if msg_count % 100 == 0:
            print(f"       Received {msg_count} messages...")

    print(f"\n[INFO] Total consumed: {msg_count} messages")
    print("=" * 50)

    all_success = True
    for topic in TOPIC_CONFIG:
        count = len(buffers[topic])
        print(f"  {topic}: {count} records")
        if not write_topic_to_hdfs(buffers[topic], topic):
            all_success = False
            break

    print("=" * 50)

    if all_success:
        try:
            consumer.commit()
            print("[INFO] Kafka offsets committed.")
        except Exception as e:
            print(f"[WARNING] Offset commit failed: {e}")
        print("[INFO] Consumer finished — Bronze layer populated.")
    else:
        print("[ERROR] HDFS write failed — offsets NOT committed. "
              "Fix HDFS connection and re-run this script.")

    consumer.close()


if __name__ == "__main__":
    consume_all()
