# Learning Outcome Coverage (IceHockeyKG)

## Focus LOs (from the one-pager): detailed

### LO1 - Understand and apply Knowledge Graph Embeddings

In this project, Knowledge Graph Embeddings were not implemented as a full training pipeline, but they were explicitly treated as a methodological component in the architecture and future extension path. The core idea of LO1 - making symbolic knowledge usable in a vector space - was operationalized by preparing the graph in a way that an embedding stage can be integrated directly. The RDF model is structured into clear entities (`Player`, `Team`, `League`, `Season`, `CareerStats`) and relations (`hasStats`, `playsFor`, `inSeason`, `inLeague`). This structure is exactly the prerequisite for applying triple-based embedding methods such as TransE or ComplEx.

The concrete LO1 contribution is therefore **application-oriented operationalization**: the project provides a realistic, sufficiently large KG setting with typical integration challenges (heterogeneous sources, entity linking, temporal memberships), which naturally supports embedding use cases. Examples include link prediction for missing team relations, scoring candidate matches in uncertain entity resolution, and similarity analysis between players based on relational patterns rather than only tabular features. This demonstrates that embeddings are positioned not as isolated ML tools, but as part of a KG lifecycle (creation -> enrichment -> analysis -> improvement).

LO1 is also addressed by showing the limits of purely rule/query-based methods: SPARQL gives precise answers over explicitly stored facts, but it does not probabilistically recover missing edges. This is exactly where embeddings become methodologically relevant. For the portfolio, this means LO1 is not only mentioned theoretically, but anchored as a concrete technical next step with direct value for the current HockeyKG.

### LO2 - Understand and apply logical knowledge in KGs

LO2 is directly implemented at the core of the project. The HockeyKG models domain knowledge explicitly as an RDF graph with defined classes, properties, and typed literals. Players, seasons, leagues, statistical entries, and team memberships are no longer stored as disconnected tables, but as semantically connected resources. This makes complex questions formally queryable, such as career trajectories over time, team history, and cross-league performance patterns.

The application of logical KG concepts appears on multiple layers: first in modeling choices (separating entities from event/stat nodes), second in identity linking (`owl:sameAs` to Wikidata), and third in declarative querying with SPARQL. The query set demonstrates selection, filtering, aggregation, grouping, and optional graph patterns. This is the practical use of logical knowledge representation: knowledge is explicitly formalized and then evaluated via a formal query language.

A key point is also graph evolution: enrichment adds new knowledge to existing entities without breaking the base model. This reflects a central strength of logical KGs, namely incremental extensibility with consistent semantics. In addition, `matchConfidence` documents uncertainty transparently instead of hiding it. Overall, the project demonstrates solid practical proficiency in LO2: logical modeling, explicit representation, formal querying, and controlled evolution of the knowledge base.

## Basic-proficiency LOs: short justification

### LO4 - Compare different KG data models
Covered through an explicit comparison of the tabular model vs. graph model, and the choice of RDF because relational and temporal structures (e.g., team transfers, season context) are more naturally represented in a graph.

### LO5 - Design and implement architectures of a KG
Covered via a clear pipeline architecture: data sources -> transformation script (`ep_to_rdf.py`) -> enrichment (`wikidata_teams.py`) -> Turtle storage -> Fuseki deployment -> SPARQL service.

### LO6 - Describe and apply scalable reasoning methods
Covered in practice through SPARQL analytics over large graph artifacts (hundreds of thousands of stat nodes), including aggregations, grouping, and relational analysis.

### LO7 - Apply a system to create a KG
Directly covered: heterogeneous CSV data is systematically transformed into a consistent RDF KG, including URI strategy, datatype handling, and ontology structure.

### LO8 - Apply a system to evolve a KG
Directly covered: the existing KG is extended with an external knowledge source (Wikidata), including entity linking, new relations (`playsFor`), and a quality/provenance signal (`matchConfidence`).

### LO9 - Describe and design real-world KG applications
Covered through the concrete domain application of hockey analytics, with real questions about careers, teams, and performance development.

### LO10 - Describe financial KG applications
Covered at basic level via transferability: the same core mechanisms (entity resolution, integration pipeline, query services, evolution) are structurally transferable to financial KGs.

### LO11 - Apply a system to provide services through a KG
Covered through the operational query service in Fuseki: the KG is not only stored, but actively used as an information service via SPARQL.

## Evidence: where each LO is used in this project

### LO1
- Current evidence: discussed in [LO_Coverage.md](/home/konsti/Documents/Uni/Master/sem2/KG/LO_Coverage.md) (section LO1) as a concrete next step on top of the existing graph.
- Project anchor points for embedding use: [ep_to_rdf.py](/home/konsti/Documents/Uni/Master/sem2/KG/ep_to_rdf.py) (entity/relation structure) and [wikidata_teams.py](/home/konsti/Documents/Uni/Master/sem2/KG/wikidata_teams.py) (linked entities for prediction tasks).
- Basic extension statement (acceptable as basic proficiency): "This KG can be extended with TransE/ComplEx to predict missing `playsFor` links and score uncertain entity matches."

### LO2
- Implemented in [ep_to_rdf.py](/home/konsti/Documents/Uni/Master/sem2/KG/ep_to_rdf.py): explicit RDF classes/properties, typed literals, URI design.
- Implemented in [SPARQL_Query.txt](/home/konsti/Documents/Uni/Master/sem2/KG/SPARQL_Query.txt): formal graph querying (filters, joins, aggregation, grouping).
- Implemented in [wikidata_teams.py](/home/konsti/Documents/Uni/Master/sem2/KG/wikidata_teams.py): logical identity linking via `owl:sameAs`.

### LO4
- Evidence in [portfolio.md](/home/konsti/Documents/Uni/Master/sem2/KG/portfolio.md) (KG design/modeling sections): explicit tabular-vs-graph rationale.
- Practical evidence: graph modeling choices in [ep_to_rdf.py](/home/konsti/Documents/Uni/Master/sem2/KG/ep_to_rdf.py) (`Player`, `Season`, `CareerStats` separation).
- Basic extension statement: "A temporal RDF pattern (e.g., reified memberships) could further improve transfer/history representation."

### LO5
- Evidence in [README.md](/home/konsti/Documents/Uni/Master/sem2/KG/README.md): end-to-end architecture and execution flow.
- Evidence in [portfolio.md](/home/konsti/Documents/Uni/Master/sem2/KG/portfolio.md) (implementation pipeline): source -> transform -> enrich -> store -> serve.
- Basic extension statement: "The architecture can be extended with a SHACL validation step before deployment."

### LO6
- Evidence in [SPARQL_Query.txt](/home/konsti/Documents/Uni/Master/sem2/KG/SPARQL_Query.txt): aggregate and analytical queries over large graph data.
- Data scale evidence documented in [README.md](/home/konsti/Documents/Uni/Master/sem2/KG/README.md) (players, stats, seasons).
- Basic extension statement: "Materialized views or precomputed aggregates can be added for faster repeated analytics queries."

### LO7
- Evidence in [ep_to_rdf.py](/home/konsti/Documents/Uni/Master/sem2/KG/ep_to_rdf.py): full KG creation from heterogeneous CSV inputs.
- Evidence in [README.md](/home/konsti/Documents/Uni/Master/sem2/KG/README.md) (pipeline steps).
- Basic extension statement: "A third input source can be integrated by adding a mapper that reuses the same URI strategy."

### LO8
- Evidence in [wikidata_teams.py](/home/konsti/Documents/Uni/Master/sem2/KG/wikidata_teams.py): KG evolution by enrichment, linking, and confidence annotation.
- Evidence files: [enrichment.ttl](/home/konsti/Documents/Uni/Master/sem2/KG/enrichment.ttl) and [match_report.csv](/home/konsti/Documents/Uni/Master/sem2/KG/match_report.csv).
- Basic extension statement: "An update job can periodically refresh Wikidata links and append changed memberships."

### LO9
- Evidence in [portfolio.md](/home/konsti/Documents/Uni/Master/sem2/KG/portfolio.md): real-world hockey analytics use case definition.
- Operational evidence via query service in [SPARQL_Query.txt](/home/konsti/Documents/Uni/Master/sem2/KG/SPARQL_Query.txt).
- Basic extension statement: "A lightweight dashboard can expose these SPARQL insights to non-technical users."

### LO10
- Evidence in [LO_Coverage.md](/home/konsti/Documents/Uni/Master/sem2/KG/LO_Coverage.md) and [portfolio.md](/home/konsti/Documents/Uni/Master/sem2/KG/portfolio.md): explicit transfer argument from this pipeline to financial KG scenarios.
- Basic extension statement: "The same entity-linking and lineage approach can be applied to institutions, legal entities, and ownership relations."

### LO11
- Evidence in [README.md](/home/konsti/Documents/Uni/Master/sem2/KG/README.md): deployment and service usage through Fuseki endpoint.
- Evidence in [SPARQL_Query.txt](/home/konsti/Documents/Uni/Master/sem2/KG/SPARQL_Query.txt): concrete service-level queries.
- Basic extension statement: "A simple REST layer can expose selected SPARQL templates as reusable analytics endpoints."

## Not included (as stated in the one-pager)

- LO3 (Graph Neural Networks): not implemented.
- LO12 (connections between KG, AI, and ML): only briefly discussed, not developed as a dedicated deep section.

## Grading Alignment (based on course organization page)

- Basic proficiency covered in this portfolio: **10 LOs** (`LO1`, `LO2`, `LO4`-`LO11`)
- Exceeding-threshold focus: **2 LOs** (`LO2`, `LO8`)
- Not covered by design: `LO3`, `LO12`

This matches the published grading logic on the organization page:
- B3 requires basic proficiency in at least 10 LOs.
- U2 requires that plus exceeding threshold in at least 1 LO.
- S1 requires that plus exceeding threshold in at least 2 LOs.
