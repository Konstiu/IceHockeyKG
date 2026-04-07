# IceHockeyKG: Knowledge Graph for NHL Players and Statistics

This project builds an RDF Knowledge Graph from elite hockey data and enriches player entities with external Wikidata links and team history.

## Project Goal

Create a practical KG pipeline that supports:
- data integration from heterogeneous CSV sources,
- KG construction in RDF/Turtle,
- entity enrichment with external knowledge,
- SPARQL querying for player, team, and performance analytics.

## Data Sources

- Kaggle: Elite Prospects Hockey Stats Dataset  
  https://www.kaggle.com/datasets/mjavon/elite-prospects-hockey-stats-player-data
- Local CSV inputs:
  - `player_dim.csv`
  - `player_stats.csv`

## Repository Contents

- `ep_to_rdf.py`: CSV -> base KG (`hockey_kg.ttl`)
- `wikidata_teams.py`: Wikidata enrichment (`enrichment.ttl`, `match_report.csv`)
- `hockey_kg.ttl`: base KG
- `enrichment.ttl`: enrichment triples
- `full_hockey_kg.ttl`: merged KG (base + enrichment)
- `SPARQL_Query.txt`: curated SPARQL query collection
- `portfolio.md`: project portfolio report aligned to course learning outcomes

## Environment

Recommended Python version: `3.10+`

Install dependencies:

```bash
pip install pandas rdflib requests
```

## Pipeline

1. Build base KG from CSV:

```bash
python ep_to_rdf.py
```

2. Enrich players with Wikidata and teams:

```bash
python wikidata_teams.py
```

3. Merge outputs to one KG (if needed):

```bash
cat hockey_kg.ttl enrichment.ttl > full_hockey_kg.ttl
```

## Running with Apache Jena Fuseki

Start Fuseki (example):

```bash
./fuseki-server --file=/home/konsti/Documents/Uni/Master/sem2/KG/full_hockey_kg.ttl /hockey
```

Open:

- `http://localhost:3030/`
- dataset path: `/hockey`

Run the queries from `SPARQL_Query.txt` in the Fuseki query UI.

## Current Output Snapshot

Based on generated files in this repository:
- Players in base KG: `109,990`
- Leagues: `29`
- Seasons: `741`
- Career stats nodes: `402,346`
- Enriched players (`match_report.csv`): `16,979` (all `HIGH` confidence via EP ID)

## Notes

- Some team memberships come from Wikidata statements and can be incomplete for specific players.
- Dates and labels depend on source quality.
- The project portfolio and LO mapping are documented in `portfolio.md`.
