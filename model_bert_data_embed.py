import pandas as pd
from sentence_transformers import SentenceTransformer
import numpy as np
from tqdm import tqdm

########ENGLISH############
#bertModel = SentenceTransformer('all-MiniLM-L6-v2')
#bertModel = SentenceTransformer('all-MiniLM-L12-v2')
#bertModel = SentenceTransformer('all-mpnet-base-v2')

########RUSSIAN#############


#bertModel = SentenceTransformer('ai-forever/sbert_large_mt_nlu_ru') 
bertModel = SentenceTransformer('distiluse-base-multilingual-cased-v2')


df = pd.read_csv('db_full_cleaned.csv')
df["extra"] = df["extra"].fillna("")
batch_size = 64
embeddings = []

for i in tqdm(range(0, len(df), batch_size)):
    batch = df["extra"].iloc[i:i+batch_size].tolist()
    batch_embeddings = bertModel.encode(batch, show_progress_bar=True)
    embeddings.extend(batch_embeddings)

vector_df = pd.DataFrame(embeddings, columns=[f"vectorized_{i}" for i in range(len(embeddings[0]))])
df_bert = pd.concat([df.reset_index(drop=True), vector_df], axis=1)
df_bert = df_bert.drop(columns=["extra"])

df_bert.to_csv("db_full_cleaned_vectorized.csv", index=False)

