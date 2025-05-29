import pandas as pd
import json
import os

# === 1. Chargement des données depuis hive
from pyhive import hive

conn = hive.Connection(
    host='localhost',
    port=10000,
    username='root',
    auth='NOSASL'
)
cursor = conn.cursor()


# Nettoyage des noms de colonnes (suppression espaces)
transactions.columns = transactions.columns.str.strip()
users.columns = users.columns.str.strip()
cards.columns = cards.columns.str.strip()



# === 3. Ajout des labels de fraude ===
fraud_df = pd.DataFrame(list(fraud_labels.items()), columns=["transaction_id", "is_fraud"])

transactions["id"] = transactions["id"].astype(str)
fraud_df["transaction_id"] = fraud_df["transaction_id"].astype(str)

transactions = transactions.merge(fraud_df, left_on="id", right_on="transaction_id", how="left")



# Renommage de l'ID de transaction pour clarté
transactions.rename(columns={"id": "transaction_id"}, inplace=True)

# === 4. Ajout des descriptions MCC ===
transactions["mcc"] = transactions["mcc"].astype(str).str.strip()
transactions["mcc_description"] = transactions["mcc"].map(mcc_codes)

# === 5. Fusion avec les données cartes ===
cards["id"] = cards["id"].astype(str)
transactions["card_id"] = transactions["card_id"].astype(str)

data = transactions.merge(cards, left_on="card_id", right_on="id", how="left")

# === 6. Fusion avec les données utilisateurs ===
data["client_id_x"] = data["client_id_x"].astype(str)
users["id"] = users["id"].astype(str)

data = data.merge(users, left_on="client_id_x", right_on="id", how="left")

# Nettoyage final
data.rename(columns={"client_id_x": "client_id"}, inplace=True)
data.drop(columns=["client_id_y", "id"], inplace=True, errors="ignore")

# Sauvegarde du jeu final 
os.makedirs("output", exist_ok=True)  

data.to_csv("output/merged_data.csv", index=False)

print("✅ Données fusionnées enregistrées dans output/merged_data.csv")
print("📐 Dimensions :", data.shape)
print("🖥️ Aperçu :")
print(data.head())
