"""
Wikidata Enrichment Script
===========================
Matcht Spieler aus dem Hockey-KG gegen Wikidata über:
  1. EP Player ID (P2481)          → Konfidenz: HIGH
  2. Name + Geburtsdatum + Land    → Konfidenz: MEDIUM  
  3. Name + Geburtsdatum           → Konfidenz: LOW

Output:
  - enrichment.ttl   (neue Tripel: Teams, Wikipedia-Links, Fotos)
  - match_report.csv (welche Spieler gematcht, mit Konfidenz)

Usage:
  pip install pandas requests
  python wikidata_enrichment.py
"""

import pandas as pd
import requests
import re
import time
import json
import sys
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "HockeyKG-Enrichment/1.0 (university project; contact: student@tuwien.ac.at)",
    "Accept": "application/json"
}
SLEEP_BETWEEN_REQUESTS = 1.0   # seconds – be polite to Wikidata
BATCH_SIZE = 50                 # players per SPARQL query
OUTPUT_TTL = "enrichment.ttl"
OUTPUT_CSV = "match_report.csv"

PREFIXES_TTL = """@prefix hockey: <http://hockey-kg.org/ontology#> .
@prefix ep:     <http://hockey-kg.org/resource/> .
@prefix foaf:   <http://xmlns.com/foaf/0.1/> .
@prefix schema: <http://schema.org/> .
@prefix owl:    <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .
@prefix wd:     <http://www.wikidata.org/entity/> .
@prefix wdt:    <http://www.wikidata.org/prop/direct/> .

"""

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_uri(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(value).strip())

def esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

def normalize_name(name: str) -> str:
    """Normalize name for fuzzy matching – remove accents etc."""
    replacements = {
        "á":"a","à":"a","â":"a","ä":"a","ã":"a",
        "é":"e","è":"e","ê":"e","ë":"e",
        "í":"i","ì":"i","î":"i","ï":"i",
        "ó":"o","ò":"o","ô":"o","ö":"o","õ":"o",
        "ú":"u","ù":"u","û":"u","ü":"u",
        "ý":"y","ÿ":"y",
        "č":"c","ć":"c","š":"s","ž":"z","ř":"r",
        "ň":"n","ť":"t","ď":"d","ľ":"l","ĺ":"l",
        "Á":"A","À":"A","Â":"A","Ä":"A",
        "É":"E","È":"E","Ê":"E","Ë":"E",
        "Í":"I","Ì":"I","Î":"I","Ï":"I",
        "Ó":"O","Ò":"O","Ô":"O","Ö":"O",
        "Ú":"U","Ù":"U","Û":"U","Ü":"U",
        "Č":"C","Š":"S","Ž":"Z","Ř":"R",
    }
    for k, v in replacements.items():
        name = name.replace(k, v)
    return name.lower().strip()

def sparql_query(query: str) -> list:
    """Run a SPARQL query against Wikidata and return results."""
    try:
        r = requests.get(
            WIKIDATA_ENDPOINT,
            params={"query": query, "format": "json"},
            headers=HEADERS,
            timeout=30
        )
        r.raise_for_status()
        return r.json().get("results", {}).get("bindings", [])
    except Exception as e:
        print(f"    SPARQL error: {e}")
        return []

# ─────────────────────────────────────────────
# STEP 1 – Load player data from CSV
# ─────────────────────────────────────────────
print("Loading player_dim.csv...")
try:
    dim = pd.read_csv("player_dim.csv", encoding="latin-1")
except FileNotFoundError:
    print("ERROR: player_dim.csv not found!")
    sys.exit(1)

print(f"  → {len(dim):,} players loaded")

# ─────────────────────────────────────────────
# STEP 2 – PASS 1: Match via EP Player ID (P2481)
# ─────────────────────────────────────────────
print("\n[Pass 1] Matching via EP Player ID (P2481)...")

# Get all EP IDs from our dataset
ep_ids = dim["PLAYER_ID"].dropna().astype(int).tolist()

# Query Wikidata in batches
id_matches = {}  # ep_id -> wikidata info

for i in range(0, len(ep_ids), BATCH_SIZE):
    batch = ep_ids[i:i+BATCH_SIZE]
    values = " ".join([f'"{eid}"' for eid in batch])

    query = f"""
    SELECT ?item ?epid ?teamLabel ?teamStart ?teamEnd ?wpEN ?photo ?nationality WHERE {{
      VALUES ?epid {{ {values} }}
      ?item wdt:P2481 ?epid .
      OPTIONAL {{ ?item wdt:P54 ?team .
                  OPTIONAL {{ ?item p:P54 ?stmt .
                             ?stmt ps:P54 ?team ;
                             OPTIONAL {{ ?stmt pq:P580 ?teamStart }}
                             OPTIONAL {{ ?stmt pq:P582 ?teamEnd }}
                  }}
      }}
      OPTIONAL {{ ?wpEN schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?wpName . }}
      OPTIONAL {{ ?item wdt:P18 ?photo . }}
      OPTIONAL {{ ?item wdt:P27 ?natItem . ?natItem rdfs:label ?nationality FILTER(LANG(?nationality)="en") }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """

    results = sparql_query(query)
    for r in results:
        epid = int(r["epid"]["value"])
        if epid not in id_matches:
            id_matches[epid] = {
                "wikidata_uri": r["item"]["value"],
                "teams": [],
                "wikipedia_en": None,
                "photo": None,
                "confidence": "HIGH"
            }
        if "teamLabel" in r:
            team_entry = {"name": r["teamLabel"]["value"]}
            if "teamStart" in r:
                team_entry["start"] = r["teamStart"]["value"][:10]
            if "teamEnd" in r:
                team_entry["end"] = r["teamEnd"]["value"][:10]
            if team_entry not in id_matches[epid]["teams"]:
                id_matches[epid]["teams"].append(team_entry)
        if "wpEN" in r and not id_matches[epid]["wikipedia_en"]:
            # reconstruct Wikipedia URL from page name
            wp_name = r.get("wpName", {}).get("value", "")
            if wp_name:
                id_matches[epid]["wikipedia_en"] = f"https://en.wikipedia.org/wiki/{wp_name.replace(' ', '_')}"
        if "photo" in r and not id_matches[epid]["photo"]:
            id_matches[epid]["photo"] = r["photo"]["value"]

    if (i // BATCH_SIZE) % 5 == 0:
        print(f"  ... batch {i//BATCH_SIZE + 1}, matched so far: {len(id_matches)}")
    time.sleep(SLEEP_BETWEEN_REQUESTS)

print(f"  → Pass 1 complete: {len(id_matches)} matches via EP ID")

# ─────────────────────────────────────────────
# STEP 3 – PASS 2: Match via Name + Birthdate + Country
# ─────────────────────────────────────────────
print("\n[Pass 2] Matching via Name + Birthdate (+ Country)...")

# Only process players NOT already matched in pass 1
unmatched = dim[~dim["PLAYER_ID"].isin(id_matches.keys())].copy()
unmatched = unmatched.dropna(subset=["FIRST_NAME", "LAST_NAME", "DATE_OF_BIRTH"])
print(f"  → {len(unmatched):,} players to try name matching")

name_matches = {}
name_no_country = {}

# Build lookup: normalized_name+birthdate -> ep_id
lookup = {}
for _, row in unmatched.iterrows():
    try:
        pid = int(row["PLAYER_ID"])
        first = normalize_name(str(row["FIRST_NAME"]))
        last  = normalize_name(str(row["LAST_NAME"]))
        bday  = str(row["DATE_OF_BIRTH"])[:10]
        nat   = str(row.get("NATIONALITY", "")).strip().lower()
        key   = (first, last, bday)
        lookup[key] = {"pid": pid, "nationality": nat}
    except Exception:
        continue

# Query Wikidata for ice hockey players with name+birthdate
# We do this in batches by birthdate year to keep queries manageable
years = sorted(set(str(d)[:4] for d in unmatched["DATE_OF_BIRTH"].dropna()))
matched_via_name = 0

for year in years:
    query = f"""
    SELECT ?item ?firstName ?lastName ?birthDate ?countryLabel ?team ?teamLabel
           ?teamStart ?teamEnd ?wpName ?photo WHERE {{
      ?item wdt:P31 wd:Q5 ;
            wdt:P641 wd:Q41323 ;
            wdt:P569 ?birthDate .
      FILTER(YEAR(?birthDate) = {year})
      ?item wdt:P735 ?fnItem . ?fnItem rdfs:label ?firstName FILTER(LANG(?firstName)="en")
      ?item wdt:P734 ?lnItem . ?lnItem rdfs:label ?lastName  FILTER(LANG(?lastName)="en")
      OPTIONAL {{ ?item wdt:P27 ?country . ?country rdfs:label ?countryLabel FILTER(LANG(?countryLabel)="en") }}
      OPTIONAL {{ ?item wdt:P54 ?team .
                  OPTIONAL {{ ?item p:P54 ?stmt . ?stmt ps:P54 ?team .
                    OPTIONAL {{ ?stmt pq:P580 ?teamStart }}
                    OPTIONAL {{ ?stmt pq:P582 ?teamEnd }}
                  }}
      }}
      OPTIONAL {{ ?wpPage schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> ; schema:name ?wpName . }}
      OPTIONAL {{ ?item wdt:P18 ?photo . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """

    results = sparql_query(query)

    for r in results:
        try:
            first_wd = normalize_name(r.get("firstName", {}).get("value", ""))
            last_wd  = normalize_name(r.get("lastName",  {}).get("value", ""))
            bday_wd  = r.get("birthDate", {}).get("value", "")[:10]
            nat_wd   = normalize_name(r.get("countryLabel", {}).get("value", ""))
            wd_uri   = r["item"]["value"]

            key = (first_wd, last_wd, bday_wd)
            if key in lookup:
                pid = lookup[key]["pid"]
                our_nat = lookup[key]["nationality"].lower()

                # Determine confidence
                if nat_wd and our_nat and (nat_wd in our_nat or our_nat in nat_wd):
                    confidence = "MEDIUM"
                else:
                    confidence = "LOW"

                target = name_matches if confidence == "MEDIUM" else name_no_country
                if pid not in target and pid not in name_matches:
                    target[pid] = {
                        "wikidata_uri": wd_uri,
                        "teams": [],
                        "wikipedia_en": None,
                        "photo": None,
                        "confidence": confidence
                    }

                store = name_matches if pid in name_matches else name_no_country
                if pid in store:
                    if "teamLabel" in r:
                        team_entry = {"name": r["teamLabel"]["value"]}
                        if "teamStart" in r:
                            team_entry["start"] = r["teamStart"]["value"][:10]
                        if "teamEnd" in r:
                            team_entry["end"] = r["teamEnd"]["value"][:10]
                        if team_entry not in store[pid]["teams"]:
                            store[pid]["teams"].append(team_entry)
                    if "wpName" in r and not store[pid]["wikipedia_en"]:
                        wp = r["wpName"]["value"]
                        store[pid]["wikipedia_en"] = f"https://en.wikipedia.org/wiki/{wp.replace(' ', '_')}"
                    if "photo" in r and not store[pid]["photo"]:
                        store[pid]["photo"] = r["photo"]["value"]

        except Exception:
            continue

    time.sleep(SLEEP_BETWEEN_REQUESTS)
    if int(year) % 5 == 0:
        total = len(name_matches) + len(name_no_country)
        print(f"  ... year {year} done, name matches so far: {total}")

print(f"  → Pass 2 complete: {len(name_matches)} MEDIUM + {len(name_no_country)} LOW confidence matches")

# ─────────────────────────────────────────────
# STEP 4 – Write enrichment.ttl
# ─────────────────────────────────────────────
all_matches = {**id_matches, **name_matches, **name_no_country}
print(f"\nTotal matched: {len(all_matches):,} players")
print(f"Writing {OUTPUT_TTL}...")

with open(OUTPUT_TTL, "w", encoding="utf-8") as f:
    f.write(PREFIXES_TTL)

    for pid, info in all_matches.items():
        p_uri   = f"ep:player_{pid}"
        wd_uri  = info["wikidata_uri"]
        conf    = info["confidence"]
        lines   = []

        # Link to Wikidata entity
        lines.append(f'{p_uri} owl:sameAs <{wd_uri}> .')
        lines.append(f'{p_uri} hockey:wikidataURI "{esc(wd_uri)}" .')
        lines.append(f'{p_uri} hockey:matchConfidence "{conf}" .')

        # Wikipedia link
        if info["wikipedia_en"]:
            lines.append(f'{p_uri} schema:url "{esc(info["wikipedia_en"])}" .')
            lines.append(f'{p_uri} hockey:wikipediaEN "{esc(info["wikipedia_en"])}" .')

        # Photo
        if info["photo"]:
            lines.append(f'{p_uri} schema:image "{esc(info["photo"])}" .')

        # Teams
        for i, team in enumerate(info["teams"]):
            t_uri = f"ep:wdteam_{pid}_{safe_uri(team['name'])}_{i}"
            lines.append(f'{t_uri} a hockey:Team ; rdfs:label "{esc(team["name"])}" .')
            lines.append(f'{p_uri} hockey:playsFor {t_uri} .')
            if "start" in team:
                lines.append(f'{t_uri} hockey:memberFrom "{team["start"]}"^^xsd:date .')
            if "end" in team:
                lines.append(f'{t_uri} hockey:memberUntil "{team["end"]}"^^xsd:date .')

        f.write("\n".join(lines) + "\n\n")

print(f"  → {OUTPUT_TTL} written!")

# ─────────────────────────────────────────────
# STEP 5 – Write match_report.csv
# ─────────────────────────────────────────────
print(f"Writing {OUTPUT_CSV}...")
report_rows = []
for pid, info in all_matches.items():
    player_row = dim[dim["PLAYER_ID"] == pid]
    name = ""
    if not player_row.empty:
        r = player_row.iloc[0]
        name = f"{r.get('FIRST_NAME','')} {r.get('LAST_NAME','')}".strip()
    report_rows.append({
        "player_id":      pid,
        "name":           name,
        "confidence":     info["confidence"],
        "wikidata_uri":   info["wikidata_uri"],
        "wikipedia_en":   info.get("wikipedia_en", ""),
        "teams_count":    len(info["teams"]),
        "has_photo":      bool(info.get("photo")),
    })

pd.DataFrame(report_rows).to_csv(OUTPUT_CSV, index=False)
print(f"  → {OUTPUT_CSV} written!")

print(f"""
✅ Enrichment complete!
   Total matched:     {len(all_matches):,} players
   HIGH confidence:   {sum(1 for v in all_matches.values() if v['confidence']=='HIGH')}  (EP ID match)
   MEDIUM confidence: {sum(1 for v in all_matches.values() if v['confidence']=='MEDIUM')}  (Name + Birthdate + Country)
   LOW confidence:    {sum(1 for v in all_matches.values() if v['confidence']=='LOW')}  (Name + Birthdate only)

Next steps:
   1. Load hockey_kg.ttl into GraphDB/Fuseki
   2. Load enrichment.ttl into the SAME dataset
   3. Run SPARQL queries that combine both!
""")
