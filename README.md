# 🏒 Hockey Knowledge Graph

This project builds a Knowledge Graph (RDF) of ice hockey players, their statistics, and team history.

---

## 📦 Dataset

The initial data comes from Kaggle:

* Elite Prospects Hockey Stats Dataset
  [https://www.kaggle.com/datasets/mjavon/elite-prospects-hockey-stats-player-data?resource=download&select=player_stats.csv](https://www.kaggle.com/datasets/mjavon/elite-prospects-hockey-stats-player-data?resource=download&select=player_stats.csv)

It includes:

* player information (`player_dim.csv`)
* player statistics (`player_stats.csv`)

💡 Note:
The dataset can be extended or improved by scraping data directly from Elite Prospects.

---

## ⚙️ Setup (Apache Jena Fuseki)

Download Apache Jena Fuseki here:

* Apache Jena Fuseki
  [https://jena.apache.org/download/](https://jena.apache.org/download/)

---

## ▶️ Running the Knowledge Graph

Start the Fuseki server with:

```bash
./fuseki-server --file=/home/konsti/Documents/Uni/Master/sem2/KG/full_hockey_kg.ttl /hockey
```

Then open in browser:

```
http://localhost:3030/hockey
```

---

## 📁 Project Structure

This repository includes:

* CSV files (raw data)
* RDF files (`.ttl`)
* scripts to generate the Knowledge Graph

### 🔴 Important

The ZIP archive contains:

* all CSV files
* all generated `.ttl` files

➡️ everything needed to run the project is included in the same archive

---

## 🧠 Data Processing

The pipeline:

1. CSV → RDF (`ep_to_rdf.py`)
2. Wikidata enrichment (`wikidata_teams.py`)
3. Combined into:

   ```
   full_hockey_kg.ttl
   ```

---

## 🔎 Querying

Use the Fuseki UI to run SPARQL queries on the dataset.

Example:

```sparql
SELECT ?player ?team
WHERE {
  ?player <http://hockey-kg.org/ontology#playsFor> ?team .
}
LIMIT 10
```

---

## 🚀 Notes / Possible Improvements

* Improve data quality (e.g. remove outliers)
* Add more detailed statistics (games played, assists, goals)
* Improve team modeling (separate membership entity)
* Use a web scraper for more complete Elite Prospects data

---

## ✅ That’s it

You should now be able to:

* load the dataset
* start Fuseki
* run SPARQL queries

---
