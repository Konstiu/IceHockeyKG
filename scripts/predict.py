"""
IceHockeyKG — Career Continuation Predictor
=============================================
Fragestellung: "Hat Spieler X nach 2010 noch professionell gespielt?"
"""

import pandas as pd
import requests
import sys
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/HockeyKG"
CUTOFF_YEAR      = 2013
MIN_SEASONS      = 2
PROJECT_ROOT     = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
HEADERS = {
    "Accept": "application/sparql-results+json",
    "Content-Type": "application/x-www-form-urlencoded"
}

# ─────────────────────────────────────────────
# SPARQL HELPER
# ─────────────────────────────────────────────
def sparql(query: str) -> pd.DataFrame:
    r = requests.post(
        GRAPHDB_ENDPOINT,
        data={"query": query},
        headers=HEADERS,
        timeout=120
    )
    if r.status_code == 500:
        print("GraphDB Fehler:")
        print(r.text)
    r.raise_for_status()
    bindings = r.json()["results"]["bindings"]
    if not bindings:
        return pd.DataFrame()
    rows = []
    for b in bindings:
        rows.append({k: v["value"] for k, v in b.items()})
    return pd.DataFrame(rows)

# ─────────────────────────────────────────────
# STEP 1 — Features aus EP-Stats holen
# ─────────────────────────────────────────────
print("Schritt 1: Lade Features aus GraphDB...")

features_query = f"""
PREFIX hockey: <http://hockey-kg.org/ontology#>
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>

SELECT ?pid ?name ?birthDate ?nationality
       (COUNT(DISTINCT ?stats) AS ?numSeasons)
       (MAX(COALESCE(?ppg, 0))    AS ?maxPPG)
       (SUM(COALESCE(?gp, 0))     AS ?totalGP)
       (MAX(COALESCE(?goals, 0))  AS ?maxGoals)
       (MAX(COALESCE(?points, 0)) AS ?maxPoints)
       (MIN(?seasonYear)          AS ?firstSeason)
       (MAX(?seasonYear)          AS ?lastSeason)
WHERE {{
  ?player a hockey:Player ;
          foaf:name ?name ;
          hockey:hasStats ?stats .
  BIND(STRAFTER(STR(?player), "player_") AS ?pid)
  OPTIONAL {{ ?player hockey:birthDate ?birthDate }}
  OPTIONAL {{ ?player hockey:nationality ?nationality }}
  ?stats hockey:inSeason ?season .
  OPTIONAL {{ ?stats hockey:pointsPerGame ?ppg }}
  OPTIONAL {{ ?stats hockey:gamesPlayed ?gp }}
  OPTIONAL {{ ?stats hockey:goals ?goals }}
  OPTIONAL {{ ?stats hockey:points ?points }}
  ?season hockey:seasonYear ?seasonYear .
  FILTER(SUBSTR(STR(?seasonYear), 1, 4) < "{CUTOFF_YEAR}")
}}
GROUP BY ?pid ?name ?birthDate ?nationality
"""

features_df = sparql(features_query)
print(f"  → {len(features_df):,} Spieler geladen")

# Numerisch konvertieren + avgPPG in Python berechnen
features_df["numSeasons"] = pd.to_numeric(features_df["numSeasons"], errors="coerce").fillna(0)
features_df["maxPPG"]     = pd.to_numeric(features_df["maxPPG"],     errors="coerce").fillna(0)
features_df["totalGP"]    = pd.to_numeric(features_df["totalGP"],    errors="coerce").fillna(0)
features_df["maxGoals"]   = pd.to_numeric(features_df["maxGoals"],   errors="coerce").fillna(0)
features_df["maxPoints"]  = pd.to_numeric(features_df["maxPoints"],  errors="coerce").fillna(0)
features_df["avgPPG"]     = features_df["maxPPG"] / features_df["numSeasons"].replace(0, 1)

# Mindestanzahl Saisons filtern
features_df = features_df[features_df["numSeasons"] >= MIN_SEASONS]
print(f"  → {len(features_df):,} Spieler mit mind. {MIN_SEASONS} Saisons")

if features_df.empty:
    print("ERROR: Keine Features geladen.")
    sys.exit(1)

# ─────────────────────────────────────────────
# STEP 2 — Labels aus EP-Stats holen
#           (hat der Spieler Saisons NACH 2010?)
# ─────────────────────────────────────────────
print("Schritt 2: Lade Labels aus GraphDB...")

labels_query = f"""
PREFIX hockey: <http://hockey-kg.org/ontology#>

SELECT DISTINCT ?pid
WHERE {{
  ?player a hockey:Player ;
          hockey:hasStats ?stats .
  BIND(STRAFTER(STR(?player), "player_") AS ?pid)
  ?stats hockey:inSeason ?season .
  ?season hockey:seasonYear ?seasonYear .
  FILTER(SUBSTR(STR(?seasonYear), 1, 4) >= "{CUTOFF_YEAR}")
}}
"""

labels_df = sparql(labels_query)
print(f"  → {len(labels_df):,} Spieler haben nach {CUTOFF_YEAR} gespielt (Label=1)")

# ─────────────────────────────────────────────
# STEP 3 — Feature Engineering
# ─────────────────────────────────────────────
print("Schritt 3: Feature Engineering...")

# Alter zum Cutoff
def calc_age(birthdate_str):
    try:
        return CUTOFF_YEAR - int(str(birthdate_str)[:4])
    except:
        return None

features_df["ageAtCutoff"] = features_df["birthDate"].apply(calc_age)

# Karrierelänge
def career_length(first, last):
    try:
        return int(str(last)[:4]) - int(str(first)[:4])
    except:
        return 0

features_df["careerLength"] = features_df.apply(
    lambda r: career_length(r["firstSeason"], r["lastSeason"]), axis=1
)

# NHL gespielt vor Cutoff?
nhl_query = f"""
PREFIX hockey: <http://hockey-kg.org/ontology#>

SELECT DISTINCT ?pid
WHERE {{
  ?player a hockey:Player ;
          hockey:hasStats ?stats .
  BIND(STRAFTER(STR(?player), "player_") AS ?pid)
  ?stats hockey:inLeague <http://hockey-kg.org/resource/league_NHL> ;
         hockey:inSeason ?season .
  ?season hockey:seasonYear ?seasonYear .
  FILTER(SUBSTR(STR(?seasonYear), 1, 4) < "{CUTOFF_YEAR}")
}}
"""

nhl_df = sparql(nhl_query)
nhl_pids = set(nhl_df["pid"].tolist()) if not nhl_df.empty else set()
features_df["playedNHL"] = features_df["pid"].apply(lambda p: 1 if p in nhl_pids else 0)
print(f"  → {len(nhl_pids):,} Spieler haben NHL gespielt")

# Nationalität enkodieren
nat_encoder = LabelEncoder()
features_df["nationality"]    = features_df["nationality"].fillna("Unknown")
features_df["nationalityEnc"] = nat_encoder.fit_transform(features_df["nationality"])

# ─────────────────────────────────────────────
# STEP 4 — Labels zuweisen
# ─────────────────────────────────────────────
print("Schritt 4: Labels zuweisen...")

positive_pids = set(labels_df["pid"].tolist()) if not labels_df.empty else set()
features_df["label"] = features_df["pid"].apply(lambda p: 1 if p in positive_pids else 0)

ml_df = features_df.dropna(subset=["ageAtCutoff"]).copy()
ml_df["label"] = ml_df["label"].astype(int)

ml_df = ml_df[(ml_df["ageAtCutoff"] >= 25) & (ml_df["ageAtCutoff"] <= 35)]

print(f"  → {len(ml_df):,} Spieler mit Features UND Labels")
print(f"     Noch gespielt nach {CUTOFF_YEAR}: {ml_df['label'].sum():,}")
print(f"     Aufgehört vor {CUTOFF_YEAR}:      {(ml_df['label']==0).sum():,}")

if len(ml_df) < 50:
    print("ERROR: Zu wenige Spieler mit Labels.")
    sys.exit(1)

# ─────────────────────────────────────────────
# STEP 5 — Modell trainieren
# ─────────────────────────────────────────────
print("\nSchritt 5: Modell trainieren...")

FEATURE_COLS = [
    "numSeasons", "maxPPG", "avgPPG", "totalGP",
    "maxGoals", "maxPoints", "ageAtCutoff",
    "careerLength", "playedNHL", "nationalityEnc"
]

X = ml_df[FEATURE_COLS].fillna(0)
y = ml_df["label"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

clf = RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42, class_weight="balanced")
clf.fit(X_train, y_train)

y_pred = clf.predict(X_test)
y_prob = clf.predict_proba(X_test)[:, 1]

print(f"\n{'='*50}")
print(f"ERGEBNISSE (Test-Set, n={len(X_test)})")
print(f"{'='*50}")
print(classification_report(y_test, y_pred, target_names=["Aufgehört", "Weiter gespielt"]))

print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))

print("\nWichtigste Features:")
importance = sorted(zip(FEATURE_COLS, clf.feature_importances_), key=lambda x: -x[1])
for feat, imp in importance:
    bar = "█" * int(imp * 40)
    print(f"  {feat:<20} {bar} {imp:.3f}")

# ─────────────────────────────────────────────
# STEP 6 — Interessante Vorhersagen
# ─────────────────────────────────────────────
print(f"\n{'='*50}")
print("BEKANNTE SPIELER — Vorhersage vs. Realität")
print(f"{'='*50}")

test_df = ml_df.loc[X_test.index].copy()
test_df["prob_continue"] = y_prob
test_df["predicted"]     = y_pred
test_df["correct"]       = (test_df["predicted"] == test_df["label"]).astype(int)

print(f"\nTop Vorhersagen 'Wird weiter spielen':")
print(f"{'Name':<30} {'Alter':>5} {'NHL':>4} {'PPG':>6} {'Prob':>6} {'Pred':>8} {'Real':>8} {'OK':>4}")
print("-" * 80)
for _, row in test_df.sort_values("prob_continue", ascending=False).head(30).iterrows():
    pred_str = "✓ weiter" if row["predicted"] == 1 else "✗ stop"
    real_str = "weiter"   if row["label"]     == 1 else "stop"
    ok = "✅" if row["correct"] else "❌"
    print(f"{row['name']:<30} {row['ageAtCutoff']:>5.0f} {row['playedNHL']:>4} "
          f"{row['maxPPG']:>6.2f} {row['prob_continue']:>6.2f} {pred_str:>8} {real_str:>8} {ok:>4}")

print(f"\nÜberraschungen (falsch vorhergesagt):")
for _, row in test_df[test_df["correct"] == 0].sort_values("prob_continue", ascending=False).head(10).iterrows():
    pred_str = "weiter" if row["predicted"] == 1 else "stop"
    real_str = "weiter" if row["label"]     == 1 else "stop"
    print(f"  {row['name']:<30} Pred: {pred_str:<8} Real: {real_str:<8} (PPG: {row['maxPPG']:.2f}, Alter: {row['ageAtCutoff']:.0f})")

# Speichern
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
output_csv = PROCESSED_DATA_DIR / "career_predictions.csv"
test_df[["name", "ageAtCutoff", "playedNHL", "maxPPG", "avgPPG",
         "numSeasons", "totalGP", "prob_continue", "predicted", "label", "correct"]]\
    .sort_values("prob_continue", ascending=False)\
    .to_csv(output_csv, index=False)

print(f"\n✅ Alle Vorhersagen gespeichert: {output_csv}")
