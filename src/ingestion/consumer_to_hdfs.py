# ============================================
# consumer_to_hdfs.py — Kafka consumer → HDFS Bronze
# ============================================
# Mengkonsumsi data dari Kafka topics dan menulis
# sebagai Parquet ke HDFS Bronze layer.
#
# Sesuai Implementation Guide Section 6.4
# ============================================

# TODO: Implement consumer logic
# Lihat implementation_guide_bigdata_project.md Section 6.4
import json
import os
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "kafka-broker:9092"
)

HDFS_NAMENODE = os.getenv(
    "HDFS_NAMENODE",
    "hdfs://hadoop-namenode:8020"
)

TOPIC_CONFIG = {
    "gizi-raw": {
        "hdfs_path": (
            f"{HDFS_NAMENODE}/data/bronze/gizi"
        )
    },

    "faskes-raw": {
        "hdfs_path": (
            f"{HDFS_NAMENODE}/data/bronze/faskes"
        )
    },

    "nakes-raw": {
        "hdfs_path": (
            f"{HDFS_NAMENODE}/data/bronze/nakes"
        )
    },

    "populasi-raw": {
        "hdfs_path": (
            f"{HDFS_NAMENODE}/data/bronze/populasi"
        )
    }
}


def write_to_hdfs(
    records: list,
    hdfs_path: str
):
    if not records:
        return

    table = pa.Table.from_pylist(records)

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False
    ) as tmp:

        pq.write_table(
            table,
            tmp.name,
            compression="snappy"
        )

        temp_file = tmp.name

    subprocess.run(
        [
            "hadoop",
            "fs",
            "-mkdir",
            "-p",
            hdfs_path
        ],
        check=False
    )

    subprocess.run(
        [
            "hadoop",
            "fs",
            "-put",
            "-f",
            temp_file,
            hdfs_path
        ],
        check=True
    )

    os.remove(temp_file)

    print(
        f"[INFO] Saved "
        f"{len(records)} records "
        f"to {hdfs_path}"
    )


def consume_all():

    consumer = KafkaConsumer(
        *TOPIC_CONFIG.keys(),
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        group_id="hdfs-writer-group",
        value_deserializer=lambda v:
        json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000
    )

    buffers = {
        topic: []
        for topic in TOPIC_CONFIG.keys()
    }

    print(
        "[INFO] Waiting for Kafka messages..."
    )

    for message in consumer:

        buffers[
            message.topic
        ].append(
            message.value
        )

    for topic, records in buffers.items():

        if not records:
            continue

        write_to_hdfs(
            records,
            TOPIC_CONFIG[topic]["hdfs_path"]
        )

    consumer.close()

    print(
        "[INFO] Consumer finished"
    )


if __name__ == "__main__":
    consume_all()