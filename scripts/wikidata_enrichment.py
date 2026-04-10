"""
Wikidata Enrichment Script v2.1
================================
Fixes vs v2:
  - Pagination überspringt fehlerhafte Seiten statt abzubrechen
  - EP-ID Normalisierung: "9678.0" → "9678" (Wikidata float-Bug)
  - Position-Suffix aus EP-Namen entfernt: "Wayne Gretzky (C)" → "Wayne Gretzky"
  - Wikidata birthdate wird korrekt auf YYYY-MM-DD normalisiert

Voraussetzungen:
  pip install pandas requests rapidfuzz unidecode pyarrow

Input:  data/raw/player_dim.csv
Output: data/processed/wikidata_cache.parquet, data/processed/enrichment.ttl, data/processed/match_report.csv
"""

import pandas as pd
import requests
import re
import time
import json
import sys
from pathlib import Path
from unidecode import unidecode
from rapidfuzz import fuzz, process

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
PROJECT_ROOT       = Path(__file__).resolve().parent.parent
RAW_DATA_DIR       = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
PLAYER_DIM_CSV     = RAW_DATA_DIR / "player_dim.csv"

WIKIDATA_ENDPOINT  = "https://query.wikidata.org/sparql"
CACHE_FILE         = PROCESSED_DATA_DIR / "wikidata_cache.parquet"
OUTPUT_TTL         = PROCESSED_DATA_DIR / "enrichment.ttl"
OUTPUT_CSV         = PROCESSED_DATA_DIR / "match_report.csv"
BASE_TTL           = PROCESSED_DATA_DIR / "hockey_kg.ttl"
MERGED_TTL         = PROCESSED_DATA_DIR / "full_hockey_kg.ttl"
AUTO_MERGE_TTL     = True

FUZZY_LOW_THRESHOLD = 65  # unter diesem Score: kein Match

HEADERS = {
    "User-Agent": "HockeyKG-Enrichment/2.1 (university project; contact: student@tuwien.ac.at)",
    "Accept":     "application/sparql-results+json"
}

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
    """Akzente weg, lowercase, strip."""
    return unidecode(str(name)).lower().strip()

# Mapping: EP-Kurzformen → Wikidata-Vollformen (normalisiert/lowercase)
NATIONALITY_MAP = {
    "usa":              ["united states", "united states of america"],
    "ussr":             ["soviet union"],
    "cis":              ["russia", "soviet union"],
    "czech republic":   ["czech republic", "czechia"],
    "czechia":          ["czech republic", "czechia"],
    "slovak republic":  ["slovakia"],
    "great britain":    ["united kingdom"],
    "uk":               ["united kingdom"],
    "belorussia":       ["belarus"],
    "byelorussia":      ["belarus"],
    "west germany":     ["germany"],
    "east germany":     ["germany"],
}

def normalize_nationality(raw: str) -> list:
    """
    Normalisiert Nationalitäts-String aus EP für Wikidata-Vergleich.
    "Canada/USA"      → ["canada", "united states", "united states of america"]
    "Slovakia/Canada" → ["slovakia", "canada"]
    "USA"             → ["united states", "united states of america"]
    Gibt eine Liste aller möglichen Wikidata-Werte zurück.
    """
    if not raw or str(raw).strip().lower() in ("", "nan", "none"):
        return []

    # Aufteilen nach "/" und jedes Land normalisieren
    parts = [unidecode(p).lower().strip() for p in str(raw).split("/")]
    result = []
    for part in parts:
        # Direkt hinzufügen
        result.append(part)
        # Aus Mapping erweitern (z.B. "usa" → "united states")
        if part in NATIONALITY_MAP:
            result.extend(NATIONALITY_MAP[part])

    return list(dict.fromkeys(result))  # Deduplizieren, Reihenfolge behalten


def nationality_match(ep_nat_raw: str, wd_nat: str) -> bool:
    """
    Prüft ob EP-Nationalität mit Wikidata-Nationalität übereinstimmt.
    Robust gegen Slash-Trennungen, Kurzformen und Varianten.
    """
    if not ep_nat_raw or not wd_nat:
        return False
    ep_nats = normalize_nationality(ep_nat_raw)
    wd_norm = normalize_name(wd_nat)
    return any(
        ep in wd_norm or wd_norm in ep
        for ep in ep_nats
    )

def strip_position(name: str) -> str:
    """
    Entfernt Positions-Suffix aus EP-Namen.
    "Wayne Gretzky (C)" → "Wayne Gretzky"
    "Patrick Roy (G)"   → "Patrick Roy"
    """
    return re.sub(r'\s*\([A-Z/]+\)\s*$', '', name).strip()

def normalize_ep_id(raw: str) -> str:
    """
    Wikidata liefert EP-IDs manchmal als "9678.0" (float).
    Normalisiert zu "9678".
    """
    try:
        return str(int(float(raw)))
    except (ValueError, TypeError):
        return str(raw).strip()

def normalize_birthdate(raw: str) -> str:
    """
    Wikidata: "1961-01-26T00:00:00Z" oder "+1961-01-26T00:00:00Z"
    CSV:      "1961-01-26"
    → gibt immer "YYYY-MM-DD" zurück, oder "" wenn nicht parsebar.
    """
    if not raw:
        return ""
    # Entferne führendes "+"
    raw = raw.lstrip("+")
    # Nur die ersten 10 Zeichen (YYYY-MM-DD)
    date_part = raw[:10]
    # Validierung: muss YYYY-MM-DD sein
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
        return date_part
    return ""

def clean_json_bytes(raw: bytes) -> bytes:
    """Entfernt ungültige JSON-Steuerzeichen aus rohen Response-Bytes."""
    return re.sub(rb'[\x00-\x08\x0b\x0c\x0e-\x1f]', b' ', raw)

# ─────────────────────────────────────────────
# SPARQL PAGED QUERY
# ─────────────────────────────────────────────
def sparql_query_paged(query: str, page_size: int = 10000, max_retries: int = 3) -> list:
    """
    Paginierter SPARQL-Query gegen Wikidata.
    - Bereinigt ungültige Steuerzeichen
    - Halbiert Page-Size bei JSON-Fehler und versucht erneut
    - Überspringt fehlerhafte Seiten statt abzubrechen
    """
    all_results = []
    offset = 0
    current_page_size = page_size
    consecutive_empty = 0

    while True:
        paged_query = query + f"\nLIMIT {current_page_size} OFFSET {offset}"
        bindings = None

        for attempt in range(max_retries):
            try:
                r = requests.get(
                    WIKIDATA_ENDPOINT,
                    params={"query": paged_query, "format": "json"},
                    headers=HEADERS,
                    timeout=90
                )
                r.raise_for_status()
                cleaned = clean_json_bytes(r.content)
                # strict=False: erlaubt Steuerzeichen in JSON-Strings
                text = cleaned.decode("utf-8", errors="replace")
                data = json.loads(text, strict=False)
                bindings = data.get("results", {}).get("bindings", [])
                current_page_size = page_size  # nach Erfolg zurücksetzen
                break

            except json.JSONDecodeError as e:
                print(f"    JSON-Fehler bei offset {offset} (Versuch {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    current_page_size = max(500, current_page_size // 2)
                    print(f"    → Reduziere Page-Size auf {current_page_size}, warte 5s...")
                    time.sleep(5)
                    paged_query = query + f"\nLIMIT {current_page_size} OFFSET {offset}"
                else:
                    print(f"    → Überspringe offset {offset}.")
                    bindings = []

            except requests.exceptions.HTTPError as e:
                print(f"    HTTP-Fehler bei offset {offset}: {e}")
                if attempt < max_retries - 1:
                    wait = 10 * (attempt + 1)
                    print(f"    → Warte {wait}s...")
                    time.sleep(wait)
                else:
                    bindings = []

            except Exception as e:
                print(f"    Fehler bei offset {offset}: {e}")
                bindings = []
                break

        # Leere Seite nach Offset 0 → echter Abschluss oder Fehler
        if not bindings:
            if offset == 0:
                print("    Warnung: Keine Ergebnisse!")
                break
            # Seite übersprungen wegen Fehler → weitermachen
            consecutive_empty += 1
            print(f"    → Leere Seite {consecutive_empty}/3 bei offset {offset}, überspringe...")
            if consecutive_empty >= 3:
                print("    → 3 leere Seiten hintereinander, stoppe.")
                break
            offset += current_page_size
            time.sleep(3)
            continue

        # Erfolg
        consecutive_empty = 0
        all_results.extend(bindings)
        print(f"    ... {len(all_results):,} Einträge (offset {offset}, page_size {current_page_size})")

        if len(bindings) < current_page_size:
            break  # letzte Seite

        offset += current_page_size
        current_page_size = min(page_size, current_page_size * 2)
        time.sleep(1.5)

    return all_results

# ─────────────────────────────────────────────
# STEP 1 – Wikidata Bulk Download / Cache
# ─────────────────────────────────────────────
def fetch_all_hockey_players() -> pd.DataFrame:
    print("\n[Schritt 1] Lade alle Eishockeyspieler aus Wikidata...")

    # 1a: Basisdaten – nur nötigste Felder (schneller, weniger Timeouts)
    base_query = """
    SELECT DISTINCT ?item ?label ?birthDate ?epid WHERE {
      ?item wdt:P31 wd:Q5 ;
            wdt:P641 wd:Q41466 .
      OPTIONAL { ?item wdt:P569 ?birthDate }
      OPTIONAL { ?item wdt:P2481 ?epid }
      ?item rdfs:label ?label FILTER(LANG(?label) = "en")
    }
    """
    base_results = sparql_query_paged(base_query)
    print(f"  → {len(base_results):,} Basisdaten-Einträge geladen")

    # 1a-2: Nationalität (separate Query)
    print("\n  Lade Nationalitäten...")
    nat_query = """
    SELECT ?item ?nationalityLabel WHERE {
      ?item wdt:P31 wd:Q5 ;
            wdt:P641 wd:Q41466 ;
            wdt:P27 ?nat .
      ?nat rdfs:label ?nationalityLabel FILTER(LANG(?nationalityLabel)="en")
    }
    """
    nat_results = sparql_query_paged(nat_query)
    nat_map: dict = {}
    for r in nat_results:
        uri = r["item"]["value"]
        nat = r.get("nationalityLabel", {}).get("value", "")
        if nat and uri not in nat_map:
            nat_map[uri] = nat
    print(f"  → {len(nat_map):,} Spieler mit Nationalität")

    # 1a-3: Wikipedia + Foto (separate Query)
    print("\n  Lade Wikipedia-Links und Fotos...")
    wp_photo_query = """
    SELECT ?item ?wpName ?photo WHERE {
      ?item wdt:P31 wd:Q5 ;
            wdt:P641 wd:Q41466 .
      OPTIONAL { ?wpPage schema:about ?item ;
                          schema:isPartOf <https://en.wikipedia.org/> ;
                          schema:name ?wpName . }
      OPTIONAL { ?item wdt:P18 ?photo }
      FILTER(BOUND(?wpName) || BOUND(?photo))
    }
    """
    wp_photo_results = sparql_query_paged(wp_photo_query)
    wp_map: dict = {}
    photo_map: dict = {}
    for r in wp_photo_results:
        uri = r["item"]["value"]
        if "wpName" in r and uri not in wp_map:
            wpn = r["wpName"]["value"]
            wp_map[uri] = f"https://en.wikipedia.org/wiki/{wpn.replace(' ', '_')}"
        if "photo" in r and uri not in photo_map:
            photo_map[uri] = r["photo"]["value"]
    print(f"  → {len(wp_map):,} Wikipedia-Links, {len(photo_map):,} Fotos")

    # 1b: Alternative Labels
    print("\n  Lade alternative Labels...")
    alt_query = """
    SELECT ?item ?altLabel WHERE {
      ?item wdt:P31 wd:Q5 ;
            wdt:P641 wd:Q41466 .
      ?item skos:altLabel ?altLabel FILTER(LANG(?altLabel) = "en")
    }
    """
    alt_results = sparql_query_paged(alt_query)
    alt_map: dict = {}
    for r in alt_results:
        uri = r["item"]["value"]
        alt = r.get("altLabel", {}).get("value", "")
        if alt:
            alt_map.setdefault(uri, []).append(alt)
    print(f"  → {len(alt_map):,} Spieler mit alternativen Labels")

    # 1c: Teams
    print("\n  Lade Team-Zugehörigkeiten...")
    team_query = """
    SELECT ?item ?teamLabel ?teamStart ?teamEnd WHERE {
      ?item wdt:P31 wd:Q5 ;
            wdt:P641 wd:Q41466 ;
            wdt:P54 ?team .
      ?team rdfs:label ?teamLabel FILTER(LANG(?teamLabel)="en")
      OPTIONAL {
        ?item p:P54 ?stmt . ?stmt ps:P54 ?team .
        OPTIONAL { ?stmt pq:P580 ?teamStart }
        OPTIONAL { ?stmt pq:P582 ?teamEnd }
      }
    }
    """
    team_results = sparql_query_paged(team_query)
    team_map: dict = {}
    for r in team_results:
        uri = r["item"]["value"]
        team_entry = {"name": r["teamLabel"]["value"]}
        if "teamStart" in r:
            team_entry["start"] = r["teamStart"]["value"][:10]
        if "teamEnd" in r:
            team_entry["end"] = r["teamEnd"]["value"][:10]
        teams = team_map.setdefault(uri, [])
        if team_entry not in teams:
            teams.append(team_entry)
    print(f"  → {len(team_map):,} Spieler mit Team-Daten")

    # Zusammenführen
    rows = []
    seen_uris: dict = {}

    for r in base_results:
        uri   = r["item"]["value"]
        label = r.get("label", {}).get("value", "")
        bday  = normalize_birthdate(r.get("birthDate", {}).get("value", "")) if "birthDate" in r else ""
        epid  = normalize_ep_id(r.get("epid", {}).get("value", "")) if "epid" in r else ""
        # Aus separaten Maps (nat_map, wp_map, photo_map)
        nat   = nat_map.get(uri, "")
        photo = photo_map.get(uri, "")
        wp    = wp_map.get(uri, "")

        if uri in seen_uris:
            idx = seen_uris[uri]
            if not rows[idx]["nationality"] and nat:
                rows[idx]["nationality"] = nat
            if not rows[idx]["photo"] and photo:
                rows[idx]["photo"] = photo
            if not rows[idx]["wikipedia_en"] and wp:
                rows[idx]["wikipedia_en"] = wp
            if not rows[idx]["ep_id"] and epid:
                rows[idx]["ep_id"] = epid
        else:
            seen_uris[uri] = len(rows)
            rows.append({
                "wikidata_uri": uri,
                "label":        label,
                "label_norm":   normalize_name(label),
                "birthdate":    bday,
                "ep_id":        epid,
                "nationality":  nat,
                "photo":        photo,
                "wikipedia_en": wp,
                "alt_labels":   json.dumps(alt_map.get(uri, []), ensure_ascii=False),
                "teams":        json.dumps(team_map.get(uri, []), ensure_ascii=False),
            })

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(CACHE_FILE, index=False)
    print(f"\n  ✅ Cache gespeichert: {CACHE_FILE} ({len(df):,} Spieler)")

    # Diagnose-Ausgabe
    with_bday  = (df["birthdate"] != "").sum()
    with_epid  = (df["ep_id"] != "").sum()
    print(f"     davon mit Geburtsdatum: {with_bday:,}")
    print(f"     davon mit EP-ID:        {with_epid:,}")
    return df


def load_wikidata_cache() -> pd.DataFrame:
    if CACHE_FILE.exists():
        print(f"[Schritt 1] Lade lokalen Cache: {CACHE_FILE}")
        df = pd.read_parquet(CACHE_FILE)
        print(f"  → {len(df):,} Spieler geladen")
        with_bday = (df["birthdate"] != "").sum()
        with_epid = (df["ep_id"] != "").sum()
        print(f"     mit Geburtsdatum: {with_bday:,} | mit EP-ID: {with_epid:,}")
        return df
    else:
        return fetch_all_hockey_players()

# ─────────────────────────────────────────────
# STEP 2 – Matching
# ─────────────────────────────────────────────
def compute_confidence(name_score: float, country_match: bool, ep_id_match: bool) -> str:
    score = 0
    if ep_id_match:        score += 100
    # Birthdate ist immer exakt (wir blocken danach)
    score += 40
    if name_score >= 95:   score += 40
    elif name_score >= 80: score += 25
    elif name_score >= 65: score += 10
    if country_match:      score += 20

    if score >= 140: return "HIGH"
    if score >= 80:  return "MEDIUM"
    if score >= 55:  return "LOW"
    return "NO_MATCH"


def match_players(our_players: pd.DataFrame, wd_df: pd.DataFrame) -> dict:
    results = {}

    # EP-ID Index (normalisiert)
    wd_df["ep_id_norm"] = wd_df["ep_id"].apply(lambda x: normalize_ep_id(str(x)) if x else "")
    wd_epid_index = wd_df[wd_df["ep_id_norm"] != ""].set_index("ep_id_norm")

    # Birthdate Index
    wd_bday_index = wd_df[wd_df["birthdate"] != ""].groupby("birthdate")

    available_bdates = set(wd_bday_index.groups.keys())
    print(f"  Wikidata-Index: {len(wd_epid_index):,} EP-IDs, {len(available_bdates):,} Geburtsdaten")

    matched_ep   = 0
    matched_name = 0
    no_match     = 0

    for _, player in our_players.iterrows():
        try:
            pid = int(player["PLAYER_ID"])
        except (ValueError, TypeError):
            continue

        first = str(player.get("FIRST_NAME", "")).strip()
        last  = str(player.get("LAST_NAME",  "")).strip()
        bday  = normalize_birthdate(str(player.get("DATE_OF_BIRTH", "")))
        nat_raw = str(player.get("NATIONALITY", ""))
        nat   = normalize_name(nat_raw)  # für alten Code

        # FIX: Position aus Name entfernen (falls aus player_stats übernommen)
        first = strip_position(first)
        last  = strip_position(last)

        full_name_norm = normalize_name(f"{first} {last}")
        match_info = None

        # ── Pass 1: EP-ID ──
        ep_id_str = str(pid)
        if ep_id_str in wd_epid_index.index:
            row = wd_epid_index.loc[ep_id_str]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            match_info = {
                "wikidata_uri":  row["wikidata_uri"],
                "wikipedia_en":  row["wikipedia_en"],
                "photo":         row["photo"],
                "teams":         json.loads(row["teams"]),
                "confidence":    "HIGH",
                "name_score":    100,
                "matched_label": row["label"],
            }
            matched_ep += 1

        # ── Pass 2: Birthdate + Fuzzy Name ──
        #if match_info is None and bday and bday in available_bdates:
        #    candidates = wd_bday_index.get_group(bday)

        #    # Alle Namen (label + altLabels) der Kandidaten sammeln
        #    candidate_names  = []
        #    candidate_labels = []
        #    for _, cand in candidates.iterrows():
        #        all_names = [cand["label"]] + json.loads(cand["alt_labels"])
        #        for n in all_names:
        #            candidate_names.append(normalize_name(n))
        #            candidate_labels.append(cand["label"])

        #    if candidate_names:
        #        result = process.extractOne(
        #            full_name_norm,
        #            candidate_names,
        #            scorer=fuzz.token_sort_ratio
        #        )

        #        if result and result[1] >= FUZZY_LOW_THRESHOLD:
        #            _, score, idx = result
        #            matched_label = candidate_labels[idx]

        #            # Zugehörige Wikidata-Zeile finden
        #            cand_row = candidates[candidates["label"] == matched_label]
        #            if cand_row.empty:
        #                # Fallback: in altLabels suchen
        #                cand_row = candidates[
        #                    candidates["alt_labels"].apply(
        #                        lambda al: matched_label in json.loads(al)
        #                    )
        #                ]
        #            if cand_row.empty:
        #                no_match += 1
        #                continue
        #            cand_row = cand_row.iloc[0]

        #            wd_nat = cand_row["nationality"]
        #            country_match = nationality_match(nat_raw, wd_nat)

        #            confidence = compute_confidence(
        #                name_score    = score,
        #                country_match = country_match,
        #                ep_id_match   = False
        #            )

        #            if confidence != "NO_MATCH":
        #                match_info = {
        #                    "wikidata_uri":  cand_row["wikidata_uri"],
        #                    "wikipedia_en":  cand_row["wikipedia_en"],
        #                    "photo":         cand_row["photo"],
        #                    "teams":         json.loads(cand_row["teams"]),
        #                    "confidence":    confidence,
        #                    "name_score":    score,
        #                    "matched_label": matched_label,
        #                }
        #                matched_name += 1
        #            else:
        #                no_match += 1
        #        else:
        #            no_match += 1
        #    else:
        #        no_match += 1
        elif match_info is None:
            no_match += 1

        if match_info:
            results[pid] = match_info

        total_done = matched_ep + matched_name + no_match
        if total_done % 5000 == 0 and total_done > 0:
            print(f"  ... {total_done:,} | HIGH: {matched_ep} | MED/LOW: {matched_name} | kein Match: {no_match}")

    print(f"\n  ✅ Matching abgeschlossen:")
    print(f"     HIGH (EP-ID):      {matched_ep:,}")
    print(f"     MEDIUM/LOW (Name): {matched_name:,}")
    print(f"     Kein Match:        {no_match:,}")
    return results

# ─────────────────────────────────────────────
# STEP 3 – enrichment.ttl
# ─────────────────────────────────────────────
def write_ttl(all_matches: dict):
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n[Schritt 3] Schreibe {OUTPUT_TTL}...")
    with open(OUTPUT_TTL, "w", encoding="utf-8") as f:
        f.write(PREFIXES_TTL)
        for pid, info in all_matches.items():
            p_uri = f"ep:player_{pid}"
            lines = [
                f'{p_uri} owl:sameAs <{info["wikidata_uri"]}> .',
                f'{p_uri} hockey:wikidataURI "{esc(info["wikidata_uri"])}" .',
                f'{p_uri} hockey:matchConfidence "{info["confidence"]}" .',
                f'{p_uri} hockey:nameMatchScore "{info["name_score"]}"^^xsd:decimal .',
            ]
            if info["wikipedia_en"]:
                lines.append(f'{p_uri} schema:url "{esc(info["wikipedia_en"])}" .')
                lines.append(f'{p_uri} hockey:wikipediaEN "{esc(info["wikipedia_en"])}" .')
            if info["photo"]:
                lines.append(f'{p_uri} schema:image "{esc(info["photo"])}" .')
            for i, team in enumerate(info["teams"]):
                t_uri = f"ep:wdteam_{pid}_{safe_uri(team['name'])}_{i}"
                lines.append(f'{t_uri} a hockey:Team ; rdfs:label "{esc(team["name"])}" .')
                lines.append(f'{p_uri} hockey:playsFor {t_uri} .')
                if "start" in team:
                    lines.append(f'{t_uri} hockey:memberFrom "{team["start"]}"^^xsd:date .')
                if "end" in team:
                    lines.append(f'{t_uri} hockey:memberUntil "{team["end"]}"^^xsd:date .')
            f.write("\n".join(lines) + "\n\n")
    print(f"  ✅ {OUTPUT_TTL} geschrieben")

# ─────────────────────────────────────────────
# STEP 4 – match_report.csv
# ─────────────────────────────────────────────
def write_csv(all_matches: dict, dim: pd.DataFrame):
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n[Schritt 4] Schreibe {OUTPUT_CSV}...")
    rows = []
    for pid, info in all_matches.items():
        player_row = dim[dim["PLAYER_ID"] == pid]
        name = ""
        if not player_row.empty:
            r = player_row.iloc[0]
            name = f"{r.get('FIRST_NAME','')} {r.get('LAST_NAME','')}".strip()
        rows.append({
            "player_id":     pid,
            "our_name":      name,
            "matched_label": info.get("matched_label", ""),
            "name_score":    info["name_score"],
            "confidence":    info["confidence"],
            "wikidata_uri":  info["wikidata_uri"],
            "wikipedia_en":  info.get("wikipedia_en", ""),
            "teams_count":   len(info["teams"]),
            "has_photo":     bool(info.get("photo")),
        })
    pd.DataFrame(rows).sort_values("confidence").to_csv(OUTPUT_CSV, index=False)
    print(f"  ✅ {OUTPUT_CSV} geschrieben")

# ─────────────────────────────────────────────
# STEP 5 – Merge base KG + enrichment
# ─────────────────────────────────────────────
def merge_ttl_files(base_ttl: Path = BASE_TTL, enrichment_ttl: Path = OUTPUT_TTL, merged_ttl: Path = MERGED_TTL) -> bool:
    print(f"\n[Schritt 5] Merge TTL-Dateien -> {merged_ttl} ...")
    base_path = base_ttl
    enrich_path = enrichment_ttl
    merged_path = merged_ttl

    if not enrich_path.exists():
        print(f"  ⚠️  Merge übersprungen: {enrichment_ttl} nicht gefunden.")
        return False
    if not base_path.exists():
        print(f"  ⚠️  Merge übersprungen: {base_ttl} nicht gefunden.")
        print("  ℹ️  Tipp: Erst `python scripts/ep_to_rdf.py` ausführen oder beide Dateien in GraphDB importieren.")
        return False

    merged_path.parent.mkdir(parents=True, exist_ok=True)
    with open(merged_path, "w", encoding="utf-8") as out_f:
        with open(base_path, "r", encoding="utf-8") as base_f:
            out_f.write(base_f.read().rstrip() + "\n\n")
        with open(enrich_path, "r", encoding="utf-8") as enrich_f:
            out_f.write(enrich_f.read().lstrip())

    print(f"  ✅ {merged_ttl} geschrieben")
    return True

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    try:
        from unidecode import unidecode
        from rapidfuzz import fuzz, process
    except ImportError:
        print("Fehlende Pakete:\n  pip install rapidfuzz unidecode pyarrow")
        sys.exit(1)

    print(f"Lade {PLAYER_DIM_CSV}...")
    try:
        dim = pd.read_csv(PLAYER_DIM_CSV, encoding="latin-1")
    except FileNotFoundError:
        print(f"ERROR: {PLAYER_DIM_CSV} nicht gefunden!")
        sys.exit(1)
    print(f"  → {len(dim):,} Spieler geladen")

    # Diagnose: wie viele haben Geburtsdatum?
    has_bday = dim["DATE_OF_BIRTH"].notna().sum()
    print(f"  → davon mit Geburtsdatum: {has_bday:,}")

    wd_df = load_wikidata_cache()
    all_matches = match_players(dim, wd_df)
    write_ttl(all_matches)
    write_csv(all_matches, dim)
    merged_ok = False
    if AUTO_MERGE_TTL:
        merged_ok = merge_ttl_files()

    conf_counts = {}
    for v in all_matches.values():
        conf_counts[v["confidence"]] = conf_counts.get(v["confidence"], 0) + 1

    print(f"""
╔══════════════════════════════════════╗
║  Enrichment v2.1 abgeschlossen!      ║
╠══════════════════════════════════════╣
║  Gesamt gematcht: {len(all_matches):>6,}             ║
║  HIGH:            {conf_counts.get('HIGH',0):>6,}  (EP-ID)          ║
║  MEDIUM:          {conf_counts.get('MEDIUM',0):>6,}  (Name+Bday+Land) ║
║  LOW:             {conf_counts.get('LOW',0):>6,}  (Name+Bday)      ║
╠══════════════════════════════════════╣
║  enrichment.ttl  match_report.csv    ║
║  {"full_hockey_kg.ttl erstellt" if merged_ok else "full_hockey_kg.ttl nicht erstellt"}      ║
╚══════════════════════════════════════╝

WICHTIG: Alten Cache löschen und neu laden!
  → rm data/processed/wikidata_cache.parquet
  → python scripts/wikidata_enrichment.py
""")
