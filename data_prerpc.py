import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sentence_transformers import SentenceTransformer
import numpy as np
import pandas as pd
#df_train = pd.read_csv('db_train.csv')
#df_test = pd.read_csv('db_test.csv')

#df_test.columns=df_train.columns

#head_db = df_train.head()
#df_full = pd.concat([df_train, df_test], ignore_index=True)


df_full = pd.read_csv('db_full.csv')
df_full = df_full.dropna(subset=["price", "lat", "lon"])

df_full.fillna({
    "parsed_area": df_full["parsed_area"].median(),
    "parsed_rooms": df_full["parsed_rooms"].median(),
    "build_year": df_full["build_year"].median(),
    "building_type": "unknown",
    "complex_name": "unknown",
    "floor_info": "unknown",
    "title": "",
    "extra": "" }, inplace=True)

df_full.to_csv('new_full.csv')
# ===== Нормализация числовых признаков =====
from sklearn.preprocessing import StandardScaler
numeric_cols = ["parsed_area", "parsed_rooms", "lat", "lon", "build_year"]
scaler = StandardScaler()
X_numeric = scaler.fit_transform(df_full[numeric_cols])

# ===== Векторизация текстовых признаков =====
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-mpnet-base-v2")
X_title = model.encode(df_full["title"].tolist(), show_progress_bar=True)
X_extra = model.encode(df_full["extra"].tolist(), show_progress_bar=True)

# ===== Объединение всех признаков в финальный датафрейм =====


columns = [f"num_{col}" for col in numeric_cols] + \
          [f"title_vec_{i}" for i in range(X_title.shape[1])] + \
          [f"extra_vec_{i}" for i in range(X_extra.shape[1])]

X_combined = np.hstack([X_numeric, X_title, X_extra])
df_vectorized = pd.DataFrame(X_combined, columns=columns)
df_vectorized["price"] = df_full["price"].values

# ===== Сохранение =====
df_vectorized.to_csv("vectorized_apartments.csv", index=False)