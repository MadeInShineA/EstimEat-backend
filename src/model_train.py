import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# Charger le dataset
df = pd.read_csv("data_cleaned.csv")
VERSION = 2
# Nettoyage basique : on garde seulement les colonnes utiles
df = df[['name', 'year', 'DTV_TJM_TGM']].dropna()

trend_scores = []

# Calcul du trend pour chaque commune
for commune, group in df.groupby('name'):
    if len(group) < 3:
        continue  # trop peu de points pour faire une régression fiable

    X = group['year'].values.reshape(-1, 1)
    y = group['DTV_TJM_TGM'].values

    # Régression linéaire simple
    model = LinearRegression().fit(X, y)
    slope = model.coef_[0]
    trend_score = slope * 100

    trend_scores.append({'name': commune, 'score': trend_score,'version':VERSION})

# Créer le DataFrame final
trend_df = pd.DataFrame(trend_scores)

# Trier (facultatif, juste pour lisibilité)
trend_df = trend_df.sort_values('score', ascending=False)


NAME = "trend_stations_traffic.csv"
# Sauvegarder en CSV
trend_df.to_csv(NAME, index=False)

print(f"✅ Fichier '{NAME}  ' créé avec succès !")
print(trend_df.head(10))
