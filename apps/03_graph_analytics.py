import yaml, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, lit, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta.tables import DeltaTable
from graphframes import GraphFrame

CONF = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")))

KAFKA_BOOTSTRAP = CONF["kafka"]["bootstrap_servers"]
TOPIC_IN        = CONF["kafka"]["topics"]["normalized"]
TOPIC_OUT       = CONF["kafka"]["topics"]["analytics_user"]

ES_URL          = CONF["elasticsearch"]["url"]
ES_USERS_INDEX  = CONF["elasticsearch"]["index_users"]

DELTA_VERTICES  = CONF["paths"]["delta_vertices"]
DELTA_EDGES     = CONF["paths"]["delta_edges"]
CHECKPOINTS     = os.path.join(CONF["paths"]["checkpoints"], "graph_analytics")

WINDOW_HOURS    = int(CONF["graph"]["window_hours"])
PR_RESET        = float(CONF["graph"]["pagerank"]["reset_prob"])
PR_TOL          = float(CONF["graph"]["pagerank"]["tol"])

W_CONTENT       = float(CONF["risk"]["weights"]["content"])
W_CENTRALITY    = float(CONF["risk"]["weights"]["centrality"])
W_SPIKE         = float(CONF["risk"]["weights"]["sentiment_spike"])
THRESH_ALERT    = float(CONF["risk"]["threshold_alert"])

spark = (SparkSession.builder
         .appName("Graph_Analytics_Risk")
         .getOrCreate())

value_schema = StructType([
    StructField("post_id", StringType()),
    StructField("author_id", StringType()),
    StructField("source", StringType()),
    StructField("event_time", TimestampType()),
    StructField("lang", StringType()),
    StructField("text_en", StringType()),
    StructField("probability", StringType()),  # vector in JSON string
    StructField("prediction", DoubleType()),
    StructField("classification", StringType())
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC_IN)
       .option("startingOffsets", "latest")
       .load()
       .select(from_json(col("value").cast("string"), value_schema).alias("j"))
       .select("j.*"))

def foreach_batch(df, batch_id):
    if df.rdd.isEmpty():
        return

    # Time window filter
    df = df.where(col("event_time") >= expr(f"timestampadd(HOUR, -{WINDOW_HOURS}, current_timestamp())"))

    # Build vertices and edges
    users = df.selectExpr("author_id as id", "'USER' as type", "event_time as ts")
    posts = df.selectExpr("post_id as id", "'POST' as type", "event_time as ts")
    v_new = users.unionByName(posts)

    e_new = df.selectExpr("author_id as src", "post_id as dst", "'POSTED' as relationship", "event_time as ts")

    spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")
    try:
        v_tbl = DeltaTable.forPath(spark, DELTA_VERTICES)
    except:
        v_new.write.format("delta").mode("overwrite").save(DELTA_VERTICES)
        v_tbl = DeltaTable.forPath(spark, DELTA_VERTICES)
    try:
        e_tbl = DeltaTable.forPath(spark, DELTA_EDGES)
    except:
        e_new.write.format("delta").mode("overwrite").save(DELTA_EDGES)
        e_tbl = DeltaTable.forPath(spark, DELTA_EDGES)

    (v_tbl.alias("v").merge(v_new.alias("n"), "v.id = n.id")
         .whenMatchedUpdate(set={"type": "n.type", "ts": "n.ts"})
         .whenNotMatchedInsertAll()
         .execute())

    (e_tbl.alias("e").merge(e_new.alias("n"),
        "e.src = n.src AND e.dst = n.dst AND e.relationship = n.relationship AND e.ts = n.ts")
        .whenNotMatchedInsertAll()
        .execute())

    # Load windowed snapshot for analytics
    v_df = spark.read.format("delta").load(DELTA_VERTICES).where(col("ts") >= expr(f"timestampadd(HOUR, -{WINDOW_HOURS}, current_timestamp())"))
    e_df = spark.read.format("delta").load(DELTA_EDGES).where(col("ts") >= expr(f"timestampadd(HOUR, -{WINDOW_HOURS}, current_timestamp())"))

    g = GraphFrame(v_df.select("id","type"), e_df.select("src","dst","relationship"))

    pr = g.pageRank(resetProbability=PR_RESET, tol=PR_TOL)
    cc = g.connectedComponents()
    # simple k-core approx via degree threshold (for demo)
    deg = g.degrees

    user_pr = pr.vertices.filter(col("type")=="USER").selectExpr("id as author_id","pagerank")
    user_cc = cc.vertices.filter(col("type")=="USER").selectExpr("id as id_cc","component")
    user_deg = deg.selectExpr("id as id_deg","degree")

    users_analytics = (user_pr
        .join(user_cc, user_pr.author_id==col("id_cc"), "left").drop("id_cc")
        .join(user_deg, user_pr.author_id==col("id_deg"), "left").drop("id_deg"))

    # Content score proxy from recent posts (severe probs + avg sentiment drop; here we parse classification only)
    # In production, compute severe class prob from probability vector.
    severe = df.where(col("classification").isin("depression","anxiety","stress")) \
               .groupBy("author_id").count().withColumnRenamed("count","severe_cnt")

    # Join and compute risk
    joined = (users_analytics.join(severe, "author_id", "left")
              .fillna({"severe_cnt": 0, "pagerank": 0.0, "component": 0, "degree": 0}))

    from pyspark.sql.functions import col as c, expr as e
    risk = (joined
            .withColumn("content_score", (c("severe_cnt") > 0).cast("double"))
            .withColumn("centrality_score", (c("pagerank") / (c("pagerank")+1e-6)))  # scaled 0..~1
            .withColumn("sentiment_spike", lit(0.0))  # placeholder; compute from time-series if indexed
            .withColumn("risk",
                W_CONTENT*c("content_score") + W_CENTRALITY*c("centrality_score") + W_SPIKE*c("sentiment_spike"))
            .withColumn("alert", (c("risk") >= lit(THRESH_ALERT)).cast("boolean"))
            .withColumn("window_hours", lit(WINDOW_HOURS))
            .withColumn("updated_at", current_timestamp()))

    # Sink A: Kafka
    out_kafka = (risk.selectExpr("author_id as key",
                 "to_json(named_struct('author_id', author_id, 'pagerank', pagerank, 'component', component, 'degree', degree, 'risk', risk, 'alert', alert, 'window_hours', window_hours, 'updated_at', updated_at)) as value"))
    (out_kafka.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_OUT)
        .save())

    # Sink B: Elasticsearch
    (risk
        .write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_URL)
        .option("es.resource", f"{ES_USERS_INDEX}/_doc")
        .option("es.mapping.id", "author_id")
        .mode("append")
        .save())

query = (raw.writeStream
         .foreachBatch(foreach_batch)
         .option("checkpointLocation", CHECKPOINTS)
         .trigger(processingTime="60 seconds")
         .start())

query.awaitTermination()
