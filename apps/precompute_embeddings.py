import pandas as pd
from sentence_transformers import SentenceTransformer

df = pd.read_csv("data/sample_training.csv")

model = SentenceTransformer("all-MiniLM-L6-v2")

df["features"] = model.encode(
    df["statement"].tolist(),
    batch_size=32,
    show_progress_bar=True,
    convert_to_numpy=True,
    normalize_embeddings=True,
).tolist()

df.to_pickle("data/precomputed_embeddings.parquet")
print("Saved embeddings.")
