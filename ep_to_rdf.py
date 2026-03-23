"""
EliteProspects CSV → RDF Turtle Pipeline
=========================================
Converts the two Kaggle EliteProspects CSVs into a Hockey Knowledge Graph
in RDF Turtle format (.ttl), ready to load into GraphDB or Apache Jena.

Input files (place in same folder as this script):
  - player_stats.csv
  - player_dim.csv

Output:
  - hockey_kg.ttl

Usage:
  pip install pandas rdflib
  python ep_to_rdf.py
"""

import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS, OWL, XSD
from rdflib.namespace import FOAF
import re
import sys

# ─────────────────────────────────────────────
# 1. NAMESPACES
# ─────────────────────────────────────────────
HOCKEY = Namespace("http://hockey-kg.org/ontology#")
EP     = Namespace("http://hockey-kg.org/resource/")
SCHEMA = Namespace("http://schema.org/")

# ─────────────────────────────────────────────
# 2. LOAD CSVs
# ─────────────────────────────────────────────
print("Loading CSVs...")
try:
    stats = pd.read_csv("player_stats.csv", encoding="latin-1")
    dim   = pd.read_csv("player_dim.csv",   encoding="latin-1")
except FileNotFoundError as e:
    print(f"ERROR: {e}")
    print("Make sure player_stats.csv and player_dim.csv are in the same folder.")
    sys.exit(1)

print(f"  player_stats.csv: {len(stats):,} rows")
print(f"  player_dim.csv:   {len(dim):,} rows")

# ─────────────────────────────────────────────
# 3. HELPER FUNCTIONS
# ─────────────────────────────────────────────
def safe_uri(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(value).strip())

def player_uri(player_id) -> URIRef:
    return EP[f"player_{int(player_id)}"]

def league_uri(league_name: str) -> URIRef:
    return EP[f"league_{safe_uri(league_name)}"]

def season_uri(league_name: str, season_year: str) -> URIRef:
    return EP[f"season_{safe_uri(league_name)}_{safe_uri(str(season_year))}"]

def stats_uri(player_id, league: str, season: str) -> URIRef:
    return EP[f"stats_{int(player_id)}_{safe_uri(league)}_{safe_uri(str(season))}"]

# ─────────────────────────────────────────────
# 4. BUILD THE GRAPH
# ─────────────────────────────────────────────
print("\nBuilding RDF graph...")
g = Graph()

g.bind("hockey", HOCKEY)
g.bind("ep",     EP)
g.bind("schema", SCHEMA)
g.bind("foaf",   FOAF)
g.bind("owl",    OWL)
g.bind("rdfs",   RDFS)

# ── Ontology classes ──
for cls in ["Player", "Team", "League", "Season", "CareerStats"]:
    g.add((HOCKEY[cls], RDF.type, OWL.Class))
    g.add((HOCKEY[cls], RDFS.label, Literal(cls)))

# ── Players (player_dim.csv) ──
print("  Adding players...")
player_count = 0
for _, row in dim.iterrows():
    try:
        pid = int(row["PLAYER_ID"])
    except (ValueError, KeyError):
        continue

    p = player_uri(pid)
    g.add((p, RDF.type, HOCKEY.Player))

    first = str(row.get("FIRST_NAME", "")).strip()
    last  = str(row.get("LAST_NAME",  "")).strip()
    if first and last:
        g.add((p, FOAF.name,        Literal(f"{first} {last}")))
        g.add((p, HOCKEY.firstName, Literal(first)))
        g.add((p, HOCKEY.lastName,  Literal(last)))

    if pd.notna(row.get("DATE_OF_BIRTH")):
        g.add((p, HOCKEY.birthDate, Literal(str(row["DATE_OF_BIRTH"]), datatype=XSD.date)))

    if pd.notna(row.get("PLACE_OF_BIRTH")):
        g.add((p, HOCKEY.birthPlace, Literal(str(row["PLACE_OF_BIRTH"]).strip())))

    if pd.notna(row.get("NATIONALITY")):
        g.add((p, HOCKEY.nationality, Literal(str(row["NATIONALITY"]).strip())))

    if pd.notna(row.get("SHOOTS")):
        g.add((p, HOCKEY.shoots, Literal(str(row["SHOOTS"]).strip())))

    if pd.notna(row.get("HEIGHT_CM")):
        try:
            g.add((p, HOCKEY.heightCm, Literal(int(row["HEIGHT_CM"]), datatype=XSD.integer)))
        except (ValueError, TypeError):
            pass

    if pd.notna(row.get("WEIGHT_KG")):
        try:
            g.add((p, HOCKEY.weightKg, Literal(int(row["WEIGHT_KG"]), datatype=XSD.integer)))
        except (ValueError, TypeError):
            pass

    for draft_col, prop in [("DRAFT_YEAR", HOCKEY.draftYear), ("DRAFT_ROUND", HOCKEY.draftRound), ("DRAFT_OVERALL", HOCKEY.draftOverall)]:
        if pd.notna(row.get(draft_col)):
            try:
                g.add((p, prop, Literal(int(row[draft_col]), datatype=XSD.integer)))
            except (ValueError, TypeError):
                pass

    player_count += 1

print(f"  → {player_count:,} players added")

# ── Leagues ──
print("  Adding leagues...")
leagues = stats["LEAGUE"].dropna().unique()
for league_name in leagues:
    l = league_uri(league_name)
    g.add((l, RDF.type,          HOCKEY.League))
    g.add((l, HOCKEY.leagueName, Literal(str(league_name))))
    g.add((l, RDFS.label,        Literal(str(league_name))))
print(f"  → {len(leagues)} leagues added")

# ── Seasons ──
print("  Adding seasons...")
seasons = stats[["LEAGUE", "LEAGUE_YEAR"]].dropna().drop_duplicates()
for _, row in seasons.iterrows():
    s = season_uri(row["LEAGUE"], row["LEAGUE_YEAR"])
    g.add((s, RDF.type,          HOCKEY.Season))
    g.add((s, HOCKEY.seasonYear, Literal(str(row["LEAGUE_YEAR"]))))
    g.add((s, HOCKEY.inLeague,   league_uri(row["LEAGUE"])))
    g.add((s, RDFS.label,        Literal(f"{row['LEAGUE']} {row['LEAGUE_YEAR']}")))
print(f"  → {len(seasons):,} seasons added")

# ── Career Stats ──
print("  Adding career stats (this may take a moment)...")
stats_count = 0
for _, row in stats.iterrows():
    try:
        pid = int(row["PLAYER_ID"])
    except (ValueError, KeyError):
        continue

    if pd.isna(row.get("LEAGUE")) or pd.isna(row.get("LEAGUE_YEAR")):
        continue

    s_node = stats_uri(pid, row["LEAGUE"], row["LEAGUE_YEAR"])
    g.add((s_node, RDF.type,        HOCKEY.CareerStats))
    g.add((player_uri(pid), HOCKEY.hasStats,  s_node))
    g.add((s_node, HOCKEY.inLeague, league_uri(row["LEAGUE"])))
    g.add((s_node, HOCKEY.inSeason, season_uri(row["LEAGUE"], row["LEAGUE_YEAR"])))

    if pd.notna(row.get("PRIMARY_POS")):
        g.add((s_node, HOCKEY.position, Literal(str(row["PRIMARY_POS"]).strip())))

    if pd.notna(row.get("PLAYER_URL")):
        g.add((player_uri(pid), SCHEMA.url, Literal(str(row["PLAYER_URL"]).strip())))

    stat_fields = {
        "GP":  (HOCKEY.gamesPlayed,    XSD.integer),
        "G":   (HOCKEY.goals,          XSD.integer),
        "A":   (HOCKEY.assists,        XSD.integer),
        "P":   (HOCKEY.points,         XSD.integer),
        "PPG": (HOCKEY.pointsPerGame,  XSD.decimal),
        "PIM": (HOCKEY.penaltyMinutes, XSD.integer),
        "+/-": (HOCKEY.plusMinus,      XSD.integer),
    }
    for col, (prop, dtype) in stat_fields.items():
        val = row.get(col)
        if pd.notna(val):
            try:
                typed_val = float(val) if dtype == XSD.decimal else int(float(val))
                g.add((s_node, prop, Literal(typed_val, datatype=dtype)))
            except (ValueError, TypeError):
                pass

    stats_count += 1
    if stats_count % 50000 == 0:
        print(f"    ... {stats_count:,} stats processed")

print(f"  → {stats_count:,} stat entries added")

# ─────────────────────────────────────────────
# 5. SERIALIZE TO TURTLE
# ─────────────────────────────────────────────
output_file = "hockey_kg.ttl"
print(f"\nSerializing to {output_file}...")
g.serialize(destination=output_file, format="turtle")

print(f"\n✅ Done!")
print(f"   Triples:    {len(g):,}")
print(f"   Players:    {player_count:,}")
print(f"   Leagues:    {len(leagues)}")
print(f"   Seasons:    {len(seasons):,}")
print(f"   Stat rows:  {stats_count:,}")
print(f"   Output:     {output_file}")
print(f"\nNext step: load {output_file} into GraphDB or Apache Jena Fuseki!")
