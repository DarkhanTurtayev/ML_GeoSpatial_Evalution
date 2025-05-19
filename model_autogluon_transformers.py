import pandas as pd
from autogluon.tabular import TabularPredictor
#from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import seaborn as sns
#import os

#os.environ["OMP_NUM_THREADS"] = "2"
#os.environ["OPENBLAS_NUM_THREADS"] = "2"
#os.environ["MKL_NUM_THREADS"] = "2"
#os.environ["NUMEXPR_NUM_THREADS"] = "2"
#os.environ["VECLIB_MAXIMUM_THREADS"] = "2"
#os.environ["RAYON_NUM_THREADS"] = "2"

data = pd.read_csv('db_full_cleaned_vectorized.csv')

if "Unnamed: 0" in data.columns:
    data = data.drop(columns=["Unnamed: 0"])



#######################

KEY_LABEL = "price"
MODELS = {
    'FT_TRANSFORMER': {'_max_features': None },    
    'NN_TORCH': {},   
    'GBM': {}                              
}
#######################

#train_data, val_data = train_test_split(df, test_size=0.15, random_state=1801)


predictor = TabularPredictor(label=KEY_LABEL, problem_type="regression", eval_metric="mae").fit( 
    train_data=data,
    #tuning_data=val_data,
    presets="best_quality",  
    time_limit=7200,   
    hyperparameters=MODELS
)

####################
#predictor.leaderboard(silent=False)

preds = predictor.predict(data)
true = data["price"]

leader = predictor.leaderboard(silent=True)
leader.to_csv("leaderboard.csv", index=False)

######VIS BLOCK###############
plt.figure(figsize=(10, 6))
sns.barplot(
    data=leader.sort_values("score_val", ascending=False),
    y="model",
    x="score_val",
    palette="viridis"
)
plt.title("MAE (score_val) AutoGluon models")
plt.xlabel("MAE (score_val)")
plt.ylabel("Model name")
plt.tight_layout()
plt.savefig("leaderboard_plot.png")
plt.close()


plt.figure(figsize=(8, 8))
sns.scatterplot(x=true, y=preds, alpha=0.3)
plt.plot([true.min(), true.max()], [true.min(), true.max()], 'r--')  # линия идеала
plt.xlabel("Real price")
plt.ylabel("Predicted price")
plt.title("Сравнение предсказания и реальной цены")
plt.grid(True)
plt.tight_layout()
plt.savefig("predict_vs_true.png")
plt.close()