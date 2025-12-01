import json
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

# FIX: force ES compatibility mode v8
es = Elasticsearch(
    "http://localhost:9200",
    headers={
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    },
)

INDEX = "mental-health-index"

consumer = KafkaConsumer(
    "raw_posts_twitter",
    "raw_posts_reddit",
    "raw_posts_forums",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
)

print("🚀 Streaming started, sending normalized documents to Elasticsearch...")

while True:
    for msg in consumer:
        data = msg.value
        print("→ Incoming:", data)

        # UNIVERSAL TIMESTAMP FIELD
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # NORMALIZED FIELDS
        es_doc = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": msg.topic.replace("raw_posts_", ""),
            "text": data.get("text") or data.get("title") or data.get("post_text"),
            "user": data.get("author_id")
            or data.get("author_name")
            or data.get("author_handle"),
        }

        print("→ Sending to ES:", es_doc)
        es.index(index=INDEX, document=es_doc)
