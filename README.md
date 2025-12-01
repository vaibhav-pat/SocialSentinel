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

# Big Data Assignment – Group Deliverable Summary

## Group Details

- **Group Number:** 15  
- **Assignment Number & Title:** HDA-4 – *Streaming Multilingual Social Media Analysis for Global Mental Health Risk Detection*

---

## Section A: Common Deliverables (Group Work)

List of shared components that every team member contributed to equally.

| S.No. | Common Task / Step                     | Description / Purpose                                                                                                                                                            | Tools / Techniques Used                                                                                                     |
|:-----:|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| 1     | System Architecture & Pipeline Design  | To collaboratively define the end-to-end data flow, tools, and the decoupled "hot" (real-time NLP) and "warm" (graph analytics) streaming pipelines.                             | Architectural diagrams, Spark Structured Streaming (concepts), Kafka (topic design), Delta Lake (schema design)            |
| 2     | Data Schema & Lexicon Definition       | To review the API documentation, agree on the exact data fields to stream (from X, Reddit), and to research, validate, and curate the multilingual lexicons (empathy, stress, anxiety). | X/Reddit API Docs, JSON (schema definition), Spark NLP (lexicon matching), WHO Mental Health Corpus                       |
| 3     | Baseline Model Training & Dashboard Scoping | To train an initial, simple classification model (e.g., English-only) to set a performance benchmark. Also, to define the key metrics and visualizations for the final Kibana/Grafana dashboard. | Spark MLlib (e.g., `LogisticRegression`), Pandas, Matplotlib/Seaborn (for initial EDA), Kibana/Grafana (mockup design) |

> Examples of common work: dataset collection, preprocessing, baseline EDA, common visualization, initial ML pipeline setup, etc.

---

## Section B: Unique / Exclusive Contributions (Individual Work)

Each member has at least one unique, non-overlapping contribution beyond the common work. These extend and enhance the common pipeline.

| Member Name     | Enrollment No. | Unique Contribution Title                 | Brief Description of Work Done                                                                                                                                                                                                                                                                         | Expected Outcome / Result                                                                                                                                                               |
|-----------------|---------------|-------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Vaibhav Patidar | MSE2024009    | Real-Time NLP & Classification Pipeline   | Developed the complete "hot path" for data ingestion and processing. This involved setting up the Kafka-to-Spark stream, building the complex Spark NLP pipeline for multilingual translation (MarianMT), and integrating the trained Spark MLlib model to perform real-time risk classification on posts. | A continuous, classified stream of all incoming social media posts, enriched with sentiment and risk scores, written to the primary Delta Lake table.                                   |
| Aditya          | MRM2024006    | Graph Analytics & Visualization Dashboard | Developed the "warm path" analytics and visualization layer. This involved reading from the Delta table (created by Vaibhav), using Spark GraphX to build dynamic user/topic graphs, calculating centrality and community metrics, and building the final Grafana/Kibana dashboard to visualize trends and high-risk clusters. | A live, interactive dashboard and a set of aggregated graph metric tables that identify influential high-risk users and track real-time emotional trend shifts. |

> Note: No two members list the same unique contribution. Contributions extend or enhance the common work (e.g., applying a new ML model, adding advanced analytics, optimizing parameters/algorithms, etc.).

---

## Contribution

Our group's coordination was anchored by the decoupled two-pipeline architecture established during the common work. The primary Delta Lake table served as our central integration point and **data contract**.

Vaibhav (NLP/ML Pipeline) and Aditya (Graph/Dashboard Pipeline) jointly defined this table's exact schema during the common phase. From there, our unique contributions operated as a **producer–consumer model**:

1. **Producer (Vaibhav):**  
   Focused on the "hot path" of ingesting, translating, and classifying posts, with the sole goal of writing this enriched data to the agreed-upon Delta table.

2. **Consumer (Aditya):**  
   Focused on the "warm path," treating the Delta table as his data source. He built his `readStream` and GraphX logic to consume Vaibhav's output and generate the analytics for the dashboard.

### Specific Challenges & Insights

- **Challenge:**  
  The main challenge was the producer–consumer dependency. Aditya's graph development was entirely reliant on the schema and data quality from Vaibhav's NLP pipeline. This was managed by using static data from our common baseline model for initial development before "plugging in" the live stream.

- **Insight:**  
  The key insight was the resilience of this decoupled design. Using Delta Lake as a buffer was highly effective. It allowed Vaibhav to focus on NLP throughput while Aditya focused on complex graph queries. If the graph analytics job (Aditya) failed, the data ingestion (Vaibhav) kept running, ensuring zero data loss and robust fault tolerance.

---
