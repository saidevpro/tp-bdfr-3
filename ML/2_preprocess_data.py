import pandas as pd
import os
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
import numpy as np
from scipy import sparse

data = pd.read_csv("output/merged_data.csv")
print(f"🔵 Nombre de lignes au départ : {data.shape[0]}")

# Suppression lignes avec trop de NaN, etc. (comme avant)
data = data.dropna(thresh=int(0.8 * len(data.columns)))
data = data.fillna({"mcc_description": "unknown"})

cat_cols = data.select_dtypes(include=["object"]).columns.tolist()
num_cols = data.select_dtypes(include=["int64", "float64"]).columns.tolist()
target_col = "is_fraud"
id_cols = ["transaction_id", "card_id", "client_id"]

for col in id_cols:
    if col in cat_cols:
        cat_cols.remove(col)
for col in id_cols:
    if col in num_cols:
        num_cols.remove(col)
if target_col in num_cols:
    num_cols.remove(target_col)
if target_col in cat_cols:
    cat_cols.remove(target_col)

# Imputation moyenne dans numériques
for col in num_cols:
    if data[col].isnull().any():
        mean_val = data[col].mean()
        data[col].fillna(mean_val, inplace=True)
        print(f"⚠️ Colonne numérique '{col}' avait des NaN, remplacés par la moyenne ({mean_val:.2f})")

zero_var_cols = [col for col in num_cols if data[col].std() == 0]
if zero_var_cols:
    print(f"⚠️ Suppression colonnes numériques à variance nulle : {zero_var_cols}")
    num_cols = [col for col in num_cols if col not in zero_var_cols]

print(f"📊 Colonnes numériques utilisées : {num_cols}")
print(f"📊 Colonnes catégorielles utilisées : {cat_cols}")

preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), num_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),  # sparse=True par défaut
    ]
)

X_num_cat = preprocessor.fit_transform(data)
print(f"🟣 Dimensions après transformation (sparse) : {X_num_cat.shape}")

# La matrice X_num_cat est sparse (scipy.sparse.csr_matrix)

# Sauvegarde au format sparse npz
os.makedirs("output", exist_ok=True)
sparse.save_npz("output/preprocessed_data.npz", X_num_cat)

# Sauvegarde de la cible séparément
data[target_col].to_csv("output/target.csv", index=False)

print("✅ Données prétraitées enregistrées dans :")
print("  - output/preprocessed_data.npz (matrice sparse)")
print("  - output/target.csv (étiquette is_fraud)")





