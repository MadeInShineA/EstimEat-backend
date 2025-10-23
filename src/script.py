import pandas as pd
import json
from rapidfuzz import process, fuzz
import re
import unicodedata

# === CONFIGURATION ===
CSV_PATH = "t01x-freq-bhf-gare-staz-stn-2024.csv"             # ton fichier CSV d'entrée
COLUMN_NAME = "Bahnhof_Gare_Stazione"           # nom de la colonne à corriger
NEW_COLUMN_NAME = "name"  # nom de la nouvelle colonne corrigée
GEOJSON_PATH = "communes.geojson" # ton fichier GeoJSON
OUTPUT_PATH = "data_cleaned.csv"  # sortie du CSV corrigé

# === CHARGEMENT DES DONNÉES ===
df = pd.read_csv(CSV_PATH)

# Charger les noms de référence depuis le GeoJSON
with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson = json.load(f)

# Extraire la liste des noms de communes depuis les features
def _normalize_name(name):
    if pd.isna(name) or name is None:
        return ""
    s = str(name).strip()
    # remove parenthetical content and anything after comma or slash
    s = re.sub(r'\(.*?\)', '', s)
    s = re.split(r'[,/]', s)[0].strip()
    # remove accents
    s = unicodedata.normalize('NFD', s)
    s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
    # remove punctuation except letters, numbers, spaces and hyphens, collapse spaces
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# build normalized, deduplicated reference list
reference_names = []
seen = set()
for feat in geojson.get("features", []):
    raw = feat.get("properties", {}).get("NAME", "")
    norm = _normalize_name(raw)
    if norm and norm not in seen:
        seen.add(norm)
        reference_names.append(norm)

print(f"{reference_names} noms de référence chargés depuis le GeoJSON.")

failed = []

# === FONCTION DE CORRECTION ===
def find_closest_match(word, reference_list):
    """Trouve le mot le plus proche selon la similarité, sinon renvoie le mot original."""
    if pd.isna(word) or not isinstance(word, str):
        return word
    word_modified = _normalize_name(word.strip().split(" ")[0]) # Nettoyer les espaces et prendre la première partie avant une virgule
    # Trouver le meilleur match selon la similarité de RapidFuzz
    best_match, score, _ = process.extractOne(word_modified, reference_list, scorer=fuzz.ratio)

    best_match2, score2, _ = process.extractOne(word, reference_list, scorer=fuzz.ratio)
    THRESHOLD = 95
    if score >= THRESHOLD:
        if score2 > score:
            best_match = best_match2
            score = score2
        print(f"'{word_modified}' corrigé en '{best_match}' (score: {score})")
        return best_match
    else:
        if score2 >= THRESHOLD:
            print(f"'{word_modified}' corrigé en '{best_match2}' (score: {score2})")
            return best_match2
        else:
            print(f"'{word_modified}' non corrigé (meilleur score: {score}) closest match: '{best_match}'")
            failed.append((word_modified,best_match))
            return None

# === APPLICATION SUR LA COLONNE ===
df[NEW_COLUMN_NAME] = df[COLUMN_NAME].apply(lambda x: find_closest_match(x, reference_names))
# === EXPORT DU NOUVEAU CSV ===

df.to_csv(OUTPUT_PATH, index=False)
print(f"✅ Fichier nettoyé enregistré sous : {OUTPUT_PATH}")
print(len(failed),len(reference_names) - len(failed))
