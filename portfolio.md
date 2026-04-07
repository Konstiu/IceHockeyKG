# IceHockeyKG Portfolio (VU Knowledge Graphs)

## 1. Project Overview

- **Title:** IceHockeyKG: A Knowledge Graph for NHL Players and Statistics
- **Student:** Konstantin Unterweger
- **Mode:** 6 ECTS
- **Goal:** Build an end-to-end KG pipeline from hockey data, enrich entities with external knowledge, and provide analytics via SPARQL.

The project addresses the limits of tabular data for relation-heavy questions, for example:
- Which teams did a player play for and when?
- How does player performance evolve across leagues and seasons?
- Which players are linked to high-quality external entities (Wikidata/Wikipedia)?

## 2. Data and Inputs

### 2.1 Main Source

- Elite Prospects Hockey Stats dataset (Kaggle)
  - `player_dim.csv` (player metadata)
  - `player_stats.csv` (season-level statistics)

### 2.2 External Enrichment Source

- Wikidata SPARQL endpoint (`https://query.wikidata.org/sparql`)
- Matching strategy in three passes (implemented):
  1. EP Player ID (P2481) - HIGH confidence
  2. Name + date of birth + country - MEDIUM confidence
  3. Name + date of birth - LOW confidence

## 3. KG Design and Modeling

### 3.1 Core Ontology (namespace `http://hockey-kg.org/ontology#`)

Classes:
- `hockey:Player`
- `hockey:League`
- `hockey:Season`
- `hockey:CareerStats`
- `hockey:Team`

Key properties (selection):
- `hockey:hasStats`, `hockey:inLeague`, `hockey:inSeason`
- `hockey:goals`, `hockey:assists`, `hockey:points`, `hockey:pointsPerGame`
- `hockey:playsFor`, `hockey:memberFrom`, `hockey:memberUntil`
- `hockey:matchConfidence`, `hockey:wikipediaEN`

External vocabularies used:
- `foaf:name`
- `schema:url`, `schema:image`
- `owl:sameAs`

### 3.2 URI Strategy

- Stable player URIs based on EP ID: `ep:player_<PLAYER_ID>`
- League/season/stat node URIs generated from normalized identifiers
- Team nodes from enrichment represented as separate resources and linked via `hockey:playsFor`

## 4. Implementation Pipeline

### 4.1 Step A: CSV -> RDF (base KG)

Script: `ep_to_rdf.py`

- Reads player and stat CSV files
- Creates typed literals (`xsd:integer`, `xsd:decimal`, `xsd:date`)
- Serializes graph to `hockey_kg.ttl`

### 4.2 Step B: Wikidata Enrichment

Script: `wikidata_teams.py`

- Queries Wikidata in batches
- Creates links to Wikidata entities (`owl:sameAs`)
- Adds team membership, Wikipedia URL, image URL
- Produces:
  - `enrichment.ttl`
  - `match_report.csv`

### 4.3 Step C: Combined Graph and Querying

- `full_hockey_kg.ttl` contains base + enrichment triples
- Deployed in Apache Jena Fuseki for SPARQL querying

## 5. Results Snapshot

From generated project artifacts:

- Base KG players: **109,990**
- Leagues: **29**
- Seasons: **741**
- Career stats nodes: **402,346**
- Enriched players in `match_report.csv`: **16,979**
  - HIGH confidence: **16,979**
  - MEDIUM confidence: **0**
  - LOW confidence: **0**

Interpretation: EP ID-based linking already covers all matched entities in the generated report, which provides high confidence and low ambiguity for the enriched subset.

## 6. Example Analytical Questions (SPARQL)

The query set in `SPARQL_Query.txt` demonstrates:

- lookup of player entities by name pattern,
- retrieval of team history for a player,
- extraction of players by team membership,
- inspection of external links (`owl:sameAs`, Wikipedia),
- performance analytics (e.g., top PPG, league-level aggregates).

## 7. Learning Outcome Mapping

Course LO reference:
- https://kg.dbai.tuwien.ac.at/kg-course/details/

### 7.1 Focus / Exceeding threshold

- **LO2 (Logical knowledge in KGs):** Implemented RDF model + SPARQL querying + explicit relation modeling in graph form, including typed literals and identity links.
- **LO8 (Apply a system to evolve a Knowledge Graph):** Implemented a full evolution step with external Wikidata enrichment, confidence-aware entity linking, and integration into the existing graph (`enrichment.ttl`, `match_report.csv`).

### 7.2 Basic proficiency demonstrated

- **LO1:** Positioned KG embeddings in the project context as a concrete extension path for link prediction and quality scoring.
- **LO4:** Compared tabular vs graph data models and selected graph model for relation-heavy analytics.
- **LO5:** Designed a practical architecture (ingestion, transformation, enrichment, storage, query layer).
- **LO6:** Applied scalable querying/reasoning style via SPARQL over large graph artifacts.
- **LO7:** Implemented KG creation pipeline from heterogeneous input data.
- **LO9:** Built a real-world, domain-specific KG application (sports analytics).
- **LO10:** Transferability discussed: methods map to financial KG patterns (entity resolution, lineage, integration).
- **LO11:** Implemented service interface via SPARQL queries over deployed KG.

### 7.3 Not included by design

- **LO3:** Graph Neural Networks not implemented.
- **LO12:** Broad AI/ML/KG synthesis discussed only briefly, not as a dedicated deep analysis.

## 8. Limitations and Future Work

- Team history quality depends on completeness of Wikidata statements.
- Temporal precision can be improved by more robust date normalization.
- Future extension options:
  - KG embedding-based link prediction for missing relations,
  - data quality constraints (SHACL),
  - interactive frontend for player/team analytics.

## 9. Reproducibility

Minimal steps:

1. `pip install pandas rdflib requests`
2. `python ep_to_rdf.py`
3. `python wikidata_teams.py`
4. `cat hockey_kg.ttl enrichment.ttl > full_hockey_kg.ttl`
5. Load `full_hockey_kg.ttl` in Fuseki and execute `SPARQL_Query.txt`

All required scripts and generated artifacts are present in the repository.
