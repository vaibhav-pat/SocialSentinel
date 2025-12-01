import pandas as pd
import pickle

import yaml, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

# Load config
CONF = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")))

MODEL_DIR   = CONF["paths"]["model_dir"]
TEST_SPLIT  = float(CONF["training"]["test_split"])
SEED        = int(CONF["training"]["seed"])

# Load pandas data (the file created from precompute_embeddings.py)
pdf = pd.read_pickle("data/embedded.pkl")

# Convert Pandas → Spark
spark = (SparkSession.builder
         .appName("Train_MLlib_on_Embeddings")
         .getOrCreate())

df = spark.createDataFrame(pdf)

# Convert array → MLlib vector
@udf(returnType=VectorUDT())
def to_vector(arr):
    return Vectors.dense(arr if arr is not None else [])

df = df.withColumn("features", to_vector(col("features")))

# Label processing
label_indexer = StringIndexer(inputCol="label", outputCol="indexedLabel", handleInvalid="keep")

lr = LogisticRegression(featuresCol="features", labelCol="indexedLabel", maxIter=60)

pipeline = Pipeline(stages=[label_indexer, lr])

train, test = df.randomSplit([1 - TEST_SPLIT, TEST_SPLIT], seed=SEED)
model = pipeline.fit(train)

pred = model.transform(test)
e1 = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="f1").evaluate(pred)
e2 = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy").evaluate(pred)

print(f"F1={e1:.3f}, ACC={e2:.3f}")

model.write().overwrite().save(MODEL_DIR)
print(f"Saved model to: {MODEL_DIR}")
