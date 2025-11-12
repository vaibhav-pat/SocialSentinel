import yaml, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when, to_json, struct, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.ml import PipelineModel

from lib.embeddings_udf import make_sentence_embedding_udf
from lib.lang_and_translate import detect_lang_udf, make_translate_to_en_udf

CONF = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")))

KAFKA_BOOTSTRAP = CONF["kafka"]["bootstrap_servers"]
TOPIC_REDDIT     = CONF["kafka"]["topics"]["reddit"]
TOPIC_TWITTER    = CONF["kafka"]["topics"]["twitter"]
TOPIC_FORUMS     = CONF["kafka"]["topics"]["forums"]
TOPIC_OUT        = CONF["kafka"]["topics"]["normalized"]

MODEL_DIR        = CONF["paths"]["model_dir"]
CHECKPOINTS      = os.path.join(CONF["paths"]["checkpoints"], "ingest_nlp_classify")

EMB_MODEL        = CONF["embedding"]["model_name"]
EMB_BATCH        = int(CONF["embedding"]["batch_size"])

TRANSLATION_ON   = bool(CONF["translation"]["enabled"])
MT_MODEL         = CONF["translation"]["model_name"]
MT_BATCH         = int(CONF["translation"]["max_batch_size"])

spark = (SparkSession.builder
         .appName("Stream_NLP_Classify")
         .config("spark.sql.streaming.schemaInference", "true")
         .getOrCreate())

# Schemas for raw topics
schema_reddit = StructType([
    StructField("submission_id", StringType()),
    StructField("author_name", StringType()),
    StructField("title", StringType()),
    StructField("selftext", StringType()),
    StructField("body", StringType()),
    StructField("created_utc", DoubleType()),
    StructField("subreddit", StringType())
])

schema_twitter = StructType([
    StructField("tweet_id", StringType()),
    StructField("author_id", StringType()),
    StructField("text", StringType()),
    StructField("created_at", StringType()),
    StructField("lang", StringType()),
    StructField("geo", StringType())
])

schema_forum = StructType([
    StructField("post_hash", StringType()),
    StructField("url", StringType()),
    StructField("post_text", StringType()),
    StructField("scraped_at", DoubleType()),
    StructField("author_handle", StringType())
])

def read_kafka(topic, schema):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
            .select(from_json(col("value").cast("string"), schema).alias("data"))
           )

reddit = read_kafka(TOPIC_REDDIT, schema_reddit).selectExpr(
    "data.submission_id as post_id",
    "data.author_name as author_id",
    "coalesce(data.title, data.selftext, data.body) as text",
    "cast(data.created_utc as double) as ts_epoch",
    "'reddit' as source"
).withColumn("event_time", to_timestamp((col("ts_epoch")).cast("double")))

twitter = read_kafka(TOPIC_TWITTER, schema_twitter).selectExpr(
    "data.tweet_id as post_id",
    "data.author_id as author_id",
    "data.text as text",
    "data.created_at as created_at",
    "data.lang as lang_hint",
    "'twitter' as source"
).withColumn("event_time", to_timestamp(col("created_at")))

forum = read_kafka(TOPIC_FORUMS, schema_forum).selectExpr(
    "data.post_hash as post_id",
    "data.author_handle as author_id",
    "data.post_text as text",
    "cast(data.scraped_at as double) as ts_epoch",
    "'forums' as source"
).withColumn("event_time", to_timestamp(col("ts_epoch")))

raw_unified = reddit.select("post_id","author_id","text","source","event_time") \
    .unionByName(twitter.select("post_id","author_id","text","source","event_time","lang_hint")) \
    .unionByName(forum.select("post_id","author_id","text","source","event_time")) \
    .withColumn("lang_hint", when(col("lang_hint").isNull(), lit("")).otherwise(col("lang_hint")))

# Lightweight language detection
with_lang = raw_unified.withColumn("lang_detected", detect_lang_udf(col("text")))
lang = with_lang.withColumn("lang", when(col("lang_hint")!="", col("lang_hint")).otherwise(col("lang_detected")))

# Optional translation; otherwise use multilingual embeddings directly
if TRANSLATION_ON:
    translate_udf = make_translate_to_en_udf(MT_MODEL, MT_BATCH)
    lang = lang.withColumn("text_en", when(col("lang")=="en", col("text")).otherwise(translate_udf(col("text"))))
else:
    lang = lang.withColumn("text_en", col("text"))

# Embeddings
embed_udf = make_sentence_embedding_udf(EMB_MODEL, EMB_BATCH)
with_vec = lang.withColumn("features", embed_udf(col("text_en")))

# Load MLlib model
clf = PipelineModel.load(MODEL_DIR)
scored = clf.transform(with_vec)

# Optional: convert prediction to label
from pyspark.ml.feature import IndexToString
label_names = clf.stages[0].labels if hasattr(clf.stages[0], "labels") else ["other","anxiety","depression","stress"]
to_label = IndexToString(inputCol="prediction", outputCol="classification", labels=label_names)
final = to_label.transform(scored)

# Prepare output
out = (final
       .selectExpr(
           "post_id as key",
           "to_json(struct(post_id, author_id, source, event_time, lang, text_en, probability, prediction, classification)) as value"
       ))

query = (out.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", TOPIC_OUT)
         .option("checkpointLocation", CHECKPOINTS)
         .start())

query.awaitTermination()
