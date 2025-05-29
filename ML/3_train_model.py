import pandas as pd
import numpy as np
from scipy.sparse import load_npz
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.impute import SimpleImputer
from imblearn.over_sampling import SMOTE
import joblib

print("📥 Chargement des données encodées...")
X = load_npz("output/preprocessed_data.npz")
y = pd.read_csv("output/target.csv").values.ravel()

print(f"📊 Dimensions X : {X.shape}")
print(f"🎯 Dimensions y : {y.shape}")
print("🔍 Distribution des classes dans y :", dict(zip(*np.unique(y, return_counts=True))))

print("🧹 Vérification de la présence de NaN dans la sparse matrix...")
if np.isnan(X.data).any():
    print("⚠️ Des NaN détectés dans la sparse matrix, application d'une imputation...")
    imputer = SimpleImputer(strategy="constant", fill_value=0)
    X = imputer.fit_transform(X)
else:
    print("✅ Aucune valeur manquante détectée dans la sparse matrix.")

print("🧪 Split avec stratification sur y...")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)
print(f"📊 Train : {X_train.shape}, Test : {X_test.shape}")
print("📊 Classes dans le train :", dict(zip(*np.unique(y_train, return_counts=True))))
print("📊 Classes dans le test :", dict(zip(*np.unique(y_test, return_counts=True))))

# Rééquilibrage
if len(np.unique(y_train)) > 1:
    print("🔄 Rééquilibrage avec SMOTE...")
    smote = SMOTE(random_state=42)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    print(f"✅ Après SMOTE : {X_train_resampled.shape}, {y_train_resampled.shape}")
    print("📊 Classes après SMOTE :", dict(zip(*np.unique(y_train_resampled, return_counts=True))))
else:
    print("⚠️ SMOTE ignoré : une seule classe détectée dans y_train.")
    X_train_resampled, y_train_resampled = X_train, y_train

print("🧠 Entraînement du RandomForestClassifier...")
model = RandomForestClassifier(n_jobs=-1, random_state=42)
model.fit(X_train_resampled, y_train_resampled)

print("🧾 Évaluation sur le jeu de test...")
y_pred = model.predict(X_test)
print(confusion_matrix(y_test, y_pred))
print(classification_report(y_test, y_pred, digits=4))

accuracy = np.mean(y_pred == y_test)
print(f"✅ Accuracy : {accuracy:.4f}")

print("💾 Sauvegarde du modèle dans output/random_forest_model.joblib")
joblib.dump(model, "output/random_forest_model.joblib")
