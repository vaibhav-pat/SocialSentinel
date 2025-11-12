# Streaming Multilingual Social Media Analysis for Global Mental Health Risk Detection

This repository implements an end-to-end, streaming, multilingual pipeline for detecting depression, anxiety, and stress signals from social media in near-real-time. It includes:

1. **PySpark streaming apps** for Kafka ingestion, multilingual normalization, classification, and graph analytics.
2. **Distributed classification** with Spark MLlib on top of multilingual sentence embeddings.
3. **Kibana dashboard** visualizing emotional trends, clusters, and high-risk users.

> **Important:** This is a technical demo. Use it ethically and in compliance with the platforms' Terms of Service and privacy regulations. It is **not** a clinical diagnostic system.

---

## Architecture (High-Level)

Kafka (raw) → Spark Structured Streaming → (LangID → optional translation) → Embeddings (Pandas UDF) → MLlib Classifier → Kafka (normalized_posts)
→ Spark Streaming (Graph) → Delta (vertices/edges) → GraphFrames (PR, CC, k-core) → Risk score → Kafka (analytics_user) + Elasticsearch (Kibana)

---

## Quick Start

### 0) Prereqs
- Java 11+
- Apache Spark 3.5.x
- Python 3.10+
- Access to Kafka and Elasticsearch/Kibana (see `docker-compose.yml`)
- Install Spark packages when running:
  ```
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,graphframes:graphframes:0.8.3-spark3.5-s_2.12,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0
  ```

### 1) (Optional) Spin up infra locally
Use `docker-compose.yml` (Kafka/ZooKeeper, Elasticsearch 8.x, Kibana 8.x).
```bash
docker compose up -d
```

### 2) Create topics
```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic raw_posts_reddit --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic raw_posts_twitter --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic raw_posts_forums --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic normalized_posts --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic analytics_user --partitions 6 --replication-factor 1
```

### 3) Train classifier (Spark MLlib on multilingual embeddings)
Edit `config/config.yaml` if needed, then:
```bash
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0   apps/02_train_mllib_classifier.py
```

### 4) Run streaming classification
```bash
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0   apps/01_stream_ingest_nlp_classify.py
```

### 5) Run graph analytics (windowed GraphFrames + risk scoring → ES + Kafka)
```bash
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,graphframes:graphframes:0.8.3-spark3.5-s_2.12,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0   apps/03_graph_analytics.py
```

### 6) (Optional) Sink normalized posts into Elasticsearch for Kibana drilldowns
```bash
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0   apps/04_sink_posts_to_es.py
```

### 7) Kibana
- Import `dashboards/kibana_dashboard.ndjson` in **Kibana → Stack Management → Saved Objects → Import**.

---

## Configuration

See **`config/config.yaml`** for:
- Kafka bootstrap, topics
- Elasticsearch URL and indices
- Paths for Delta/Models/Checkpoints
- Risk weights and thresholds
- Window config for graph analytics

---

## Notes

- **Embeddings:** We use multilingual sentence embeddings via a Pandas UDF (e.g., `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`). Download the model ahead of time or mount it in executors.
- **Language ID:** Lightweight `langdetect` is included for routing; replace with CLD3/fastText if needed.
- **Translation:** Optional MarianMT via Hugging Face within a Pandas UDF (heavy). By default, we skip translation and rely on multilingual embeddings; enable translation in `config.yaml`.
- **Ethics/Privacy:** Hash user IDs; avoid storing PII; set retention; do not diagnose; use human-in-the-loop for severe cases.

---

## License
Apache-2.0 (example). Use responsibly.
