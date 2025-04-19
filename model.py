import pandas as pd
import torch 

torch.cuda.is_available()
df_train = pd.read_csv('db_train.csv')
df_test = pd.read_csv('db_test.csv')

df_test.columns=df_train.columns

head_db = df_train.head()
df_full = pd.concat([df_train, df_test], ignore_index=True)

print(df_full.head())

