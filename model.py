import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.cluster import KMeans
import xgboost as xgb
import joblib
import matplotlib.pyplot as plt

# import torch
# import torch.nn as nn
# import torch.optim as optim

# print("MPS available!" if torch.backends.mps.is_available() else "MPS not available.")

# df_train = pd.read_csv('db_train.csv')
# df_test = pd.read_csv('db_test.csv')

# df_test.columns=df_train.columns

# head_db = df_train.head()
# df_full = pd.concat([df_train, df_test], ignore_index=True)

# ===== Загрузка векторизованных данных =====
df = pd.read_csv("vectorized_apartments.csv")
coords = df[["num_lat", "num_lon"]]  # если ты их нормализовал
df["location_cluster"] = KMeans(n_clusters=20, random_state=1801).fit_predict(coords)
X = df.drop(["price"], axis=1).values
Y = np.log1p(df["price"].values)
print(X.shape) 

# ===== Обучение модели XGBoost =====
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=1801)

model = xgb.XGBRegressor(n_estimators=1000, max_depth=12, learning_rate=0.05,subsample=0.9, colsample_bytree=0.8)
model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    early_stopping_rounds=25,
    verbose=True
)

xgb.plot_importance(model, max_num_features=40)
plt.show()

joblib.dump(model, "xgb_model.joblib")
print("xgb_model.joblib")
# ===== Предсказание и вывод ошибки =====

y_test = np.expm1(y_test)
y_pred = np.expm1(model.predict(X_test))
mae = mean_absolute_error(y_test, y_pred)
print(f"MAE: {mae:.2f} ₸")
