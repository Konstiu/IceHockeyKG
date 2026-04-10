# IceHockeyKG: Knowledge Graph for Hockey Players and Career Analytics

This project builds an RDF/Turtle knowledge graph from EliteProspects CSV data, enriches players with Wikidata/Wikipedia information, and adds a prediction workflow for career continuation.

Current setup: querying and analytics are done via GraphDB.

## Project Scope

- CSV integration (`data/raw/player_dim.csv`, `data/raw/player_stats.csv`)
- RDF KG construction (`data/processed/hockey_kg.ttl`)
- Entity enrichment with Wikidata links, photos, and team history (`data/processed/enrichment.ttl`)
- SPARQL exploration (`queries/SPARQL_Query.txt`)
- ML-based career continuation prediction (`scripts/predict.py`)

## Data Sources

- Kaggle: Elite Prospects Hockey Stats Dataset  
  https://www.kaggle.com/datasets/mjavon/elite-prospects-hockey-stats-player-data
- Wikidata SPARQL endpoint for enrichment

## Repository Layout

- `scripts/`: executable Python pipelines (`ep_to_rdf.py`, `wikidata_enrichment.py`, `predict.py`)
- `data/raw/`: input datasets (source CSVs)
- `data/processed/`: generated KG/enrichment outputs and prediction outputs
- `queries/`: SPARQL query collection
- `docs/`: portfolio, LO mapping, one-pager files
- `exports/`: upload-ready ZIP artifacts

## Data Availability

Raw and processed data files (`.csv`, `.ttl`, `.parquet`) are excluded from Git via `.gitignore`.
Use the ZIP archives in `exports/`:

- `exports/raw_data.zip` -> extract to `data/raw/`
- `exports/processed_outputs.zip` -> extract to `data/processed/`

## Environment

Recommended Python: `3.10+`

Install dependencies:

```bash
pip install pandas rdflib requests rapidfuzz unidecode pyarrow scikit-learn
```

## Pipeline

1. Build base KG from CSV:

```bash
python scripts/ep_to_rdf.py
```

2. Run Wikidata enrichment:

```bash
python scripts/wikidata_enrichment.py
```

3. `wikidata_enrichment.py` also auto-creates `data/processed/full_hockey_kg.ttl` if `data/processed/hockey_kg.ttl` exists.

4. Load KG into GraphDB and run SPARQL queries from `queries/SPARQL_Query.txt`.

5. Run career prediction (expects a running SPARQL endpoint, default GraphDB):

```bash
python scripts/predict.py
```

## GraphDB Endpoint Notes

- `scripts/predict.py` uses by default:
  - `http://localhost:7200/repositories/HockeyKG`
- If your repository name/port differs, update `GRAPHDB_ENDPOINT` in `scripts/predict.py`.

## Current Snapshot (from files in this repo)

- `data/processed/match_report.csv`: `15,664` matched players, all `HIGH` confidence
- Rows with `wikipedia_en`: `10,987`
- Rows with `has_photo = True`: `6,530`
- `data/processed/career_predictions.csv`: `2,886` predictions (plus header)

## Notes

- Team memberships depend on Wikidata statement completeness.
- Some labels and dates vary with source quality.
- Large generated artifacts (`.ttl`, `.zip`) are included for reproducibility.
