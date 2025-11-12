import numpy as np
import pandas as pd
from typing import List
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType
import threading

# Lazy-load model in executors (thread-safe)
_model_lock = threading.Lock()
_model_ref = {"model": None}

def _load_model(model_name: str):
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(model_name)

def _get_model(model_name: str):
    if _model_ref["model"] is None:
        with _model_lock:
            if _model_ref["model"] is None:
                _model_ref["model"] = _load_model(model_name)
    return _model_ref["model"]

def make_sentence_embedding_udf(model_name: str, batch_size: int = 64):
    @pandas_udf(ArrayType(DoubleType()))
    def _embed_udf(texts: pd.Series) -> pd.Series:
        # Replace NAs
        clean_texts: List[str] = [t if isinstance(t, str) else "" for t in texts.tolist()]
        model = _get_model(model_name)
        vecs = model.encode(clean_texts, batch_size=batch_size, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)
        return pd.Series([v.astype(float).tolist() for v in vecs])
    return _embed_udf
