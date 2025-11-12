import yaml, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from lib.embeddings_udf import make_sentence_embedding_udf

CONF = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")))

INPUT_CSV   = CONF["training"]["input_csv"]
MODEL_DIR   = CONF["paths"]["model_dir"]
TEST_SPLIT  = float(CONF["training"]["test_split"])
SEED        = int(CONF["training"]["seed"])

EMB_MODEL   = CONF["embedding"]["model_name"]
EMB_BATCH   = int(CONF["embedding"]["batch_size"])

spark = (SparkSession.builder
         .appName("Train_MLlib_on_Embeddings")
         .getOrCreate())

df = (spark.read.option("header", True).csv(INPUT_CSV))
# expected columns: statement, label  (label in {depression, anxiety, stress, none/other})
df = df.select(col("label").alias("label_str"), col("statement")).na.drop(subset=["label_str","statement"])

# Embeddings via Pandas UDF
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, DoubleType
embed_udf = make_sentence_embedding_udf(EMB_MODEL, EMB_BATCH)
df = df.withColumn("features_array", embed_udf(col("statement")))

# Convert to MLlib Vector
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
@udf(returnType=VectorUDT())
def to_vector(arr):
    return Vectors.dense(arr if arr is not None else [])

df = df.withColumn("features", to_vector(col("features_array")))

# Label indexer
label_indexer = StringIndexer(inputCol="label_str", outputCol="indexedLabel", handleInvalid="keep")

# Classifier
lr = LogisticRegression(featuresCol="features", labelCol="indexedLabel", maxIter=60, regParam=0.0, elasticNetParam=0.0)

pipeline = Pipeline(stages=[label_indexer, lr])

train, test = df.randomSplit([1.0 - TEST_SPLIT, TEST_SPLIT], seed=SEED)
model = pipeline.fit(train)

pred = model.transform(test)
e1 = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="f1").evaluate(pred)
e2 = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy").evaluate(pred)
print(f"F1={e1:.3f}, ACC={e2:.3f}")

# Save pipeline model
model.write().overwrite().save(MODEL_DIR)
print(f"Saved model to: {MODEL_DIR}")
