import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CONF = yaml.safe_load(
    open(os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml"))
)

KAFKA_BOOTSTRAP = CONF["kafka"]["bootstrap_servers"]
TOPIC_IN = CONF["kafka"]["topics"]["normalized"]

ES_URL = CONF["elasticsearch"]["url"]
ES_POSTS_INDEX = CONF["elasticsearch"]["index_posts"]

spark = SparkSession.builder.appName("SinkPostsToES").getOrCreate()

value_schema = StructType(
    [
        StructField("post_id", StringType()),
        StructField("author_id", StringType()),
        StructField("source", StringType()),
        StructField("event_time", TimestampType()),
        StructField("lang", StringType()),
        StructField("text_en", StringType()),
        StructField("probability", StringType()),
        StructField("prediction", DoubleType()),
        StructField("classification", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), value_schema).alias("j"))
    .select("j.*")
)


def foreach_to_es(df, batch_id):
    if df.rdd.isEmpty():
        return
    (
        df.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_URL)
        .option("es.resource", f"{ES_POSTS_INDEX}/_doc")
        .mode("append")
        .save()
    )


query = (
    raw.writeStream.foreachBatch(foreach_to_es)
    .option("checkpointLocation", "/tmp/checkpoints/sink_posts_es")
    .start()
)

query.awaitTermination()
