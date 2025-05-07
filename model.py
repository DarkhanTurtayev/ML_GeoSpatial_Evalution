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

# print("MPS on" if torch.backends.mps.is_available() else "MPS off.")

# df_train = pd.read_csv('db_train.csv')
# df_test = pd.read_csv('db_test.csv')

# df_test.columns=df_train.columns

# head_db = df_train.head()
# df_full = pd.concat([df_train, df_test], ignore_index=True)

df = pd.read_csv("vectorized_apartments_numeric.csv")
coords = df[["num_lat", "num_lon"]]  #normalized notation
df["location_cluster"] = KMeans(n_clusters=30, random_state=1901).fit_predict(coords)
X = df.drop(["price"], axis=1).values
Y = np.log1p(df["price"].values)
print(X.shape) 


X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.15, random_state=1901)

# XGBoost training via low‑level API to enable MAE early stopping
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest  = xgb.DMatrix(X_test,  label=y_test)

params = {
    "objective": "reg:squarederror",
    "eval_metric": "mae",
    "n_estimators": 4000,
    "eta": 0.01,
    "max_depth": 10,
    "subsample": 0.6,
    "colsample_bytree": 0.7,
    "min_child_weight": 2

}

num_round = 5000
booster = xgb.train(
    params,
    dtrain,
    num_boost_round=num_round,
    evals=[(dtest, "val")],
    early_stopping_rounds=55,
    verbose_eval=True,
)

# feature importance plot matplotlib
xgb.plot_importance(booster, max_num_features=20)
plt.show()

#save booste
booster.save_model("xgb_model.json")
print("xgb_model.json saved")

#MAE calc
y_test_exp  = np.expm1(y_test)
y_pred_exp  = np.expm1(booster.predict(dtest))
mae = mean_absolute_error(y_test_exp, y_pred_exp)
print(f"MAE: {mae:.2f} ₸")
