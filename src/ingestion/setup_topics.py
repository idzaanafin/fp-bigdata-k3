# ============================================
# setup_topics.py — Buat Kafka topics
# ============================================
# Membuat topics di Kafka untuk ingestion:
# gizi-raw, faskes-raw, nakes-raw, populasi-raw
#
# Sesuai Implementation Guide Section 6.2
# ============================================

# TODO: Implement topic setup logic
# Lihat implementation_guide_bigdata_project.md Section 6.2
from kafka.admin import (
    KafkaAdminClient,
    NewTopic
)

TOPICS = [
    "gizi-raw",
    "faskes-raw",
    "nakes-raw",
    "populasi-raw"
]

admin = KafkaAdminClient(
    bootstrap_servers="kafka-broker:9092",
    client_id="topic-setup"
)

existing = admin.list_topics()

new_topics = []

for topic in TOPICS:

    if topic not in existing:

        new_topics.append(
            NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1
            )
        )

if new_topics:
    admin.create_topics(new_topics)
    print("[INFO] Topics created")
else:
    print("[INFO] Topics already exist")

admin.close()