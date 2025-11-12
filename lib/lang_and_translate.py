from langdetect import detect, DetectorFactory
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd
import threading

DetectorFactory.seed = 0

# Optional MarianMT translator (heavy). Loaded lazily.
_translator_ref = {"pipe": None}
_translator_lock = threading.Lock()

def _get_translator(model_name: str):
    if _translator_ref["pipe"] is None:
        with _translator_lock:
            if _translator_ref["pipe"] is None:
                from transformers import pipeline
                _translator_ref["pipe"] = pipeline("translation", model=model_name)
    return _translator_ref["pipe"]

@pandas_udf(StringType())
def detect_lang_udf(texts: pd.Series) -> pd.Series:
    out = []
    for t in texts.tolist():
        try:
            out.append(detect(t) if isinstance(t, str) and t.strip() else "und")
        except Exception:
            out.append("und")
    return pd.Series(out)

def make_translate_to_en_udf(model_name: str, max_batch_size: int = 8):
    @pandas_udf(StringType())
    def _translate_udf(texts: pd.Series) -> pd.Series:
        pipe = _get_translator(model_name)
        arr = texts.tolist()
        outs = []
        batch = []
        idxs = []
        for i, t in enumerate(arr):
            tt = t if isinstance(t, str) else ""
            batch.append(tt)
            idxs.append(i)
            if len(batch) >= max_batch_size:
                res = pipe(batch)
                outs.extend([r["translation_text"] for r in res])
                batch, idxs = [], []
        if batch:
            res = pipe(batch)
            outs.extend([r["translation_text"] for r in res])
        # outs is in order; but if empty inputs, keep blanks
        # Simple alignment for equal-length batches:
        # (This UDF expects full text for each row; for production, add better alignment)
        return pd.Series(outs if len(outs)==len(arr) else [o if i < len(outs) else "" for i,o in enumerate(outs)])
    return _translate_udf
