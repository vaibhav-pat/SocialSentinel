from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from sentence_transformers import SentenceTransformer

_model_cache = {}


def make_sentence_embedding_udf(model_name: str, max_length: int = 64):

    if model_name not in _model_cache:
        _model_cache[model_name] = SentenceTransformer(model_name)

    model = _model_cache[model_name]

    @udf(ArrayType(DoubleType()))
    def embed(text: str):
        if text is None:
            text = ""
        vec = model.encode([text], show_progress_bar=False, normalize_embeddings=True)
        return vec[0].tolist()

    return embed
