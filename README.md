# BookWorm Data Platform

**Senior Data Engineer Assignment — PIA Group**

A production-grade data platform built on Azure Databricks, dbt, Delta Lake and Unity Catalog.
Analyses 2.3 million real GoodReads books and 15.7 million reader reviews to identify
the highest-potential audiobook candidates for BookWorm Publishing.

---

## Business Answer

> **Top audiobook candidate: Harry Potter and the Sorcerer's Stone**
> Score: 0.9367 | Rating: 4.45 | Pages: 320 (ideal) | 4.7M ratings | Genre: Fantasy/Paranormal

| Rank | Title | Genre | Score | Pages |
|------|-------|-------|-------|-------|
| 1 | Harry Potter and the Sorcerer's Stone | fantasy_paranormal | 0.9367 | 320 ideal |
| 2 | The Hunger Games | young-adult | 0.9223 | 374 ideal |
| 3 | To Kill a Mockingbird | fiction | 0.9151 | 324 ideal |
| 4 | Harry Potter and the Chamber of Secrets | fantasy_paranormal | 0.9115 | 341 ideal |
| 5 | Harry Potter and the Prisoner of Azkaban | fiction | 0.9075 | 435 good |

---

## Live Dashboard

[BookWorm Audiobook Intelligence Dashboard](https://adb-7405608220287115.15.azuredatabricks.net/dashboardsv3/01f1382f39881c339c2f7e69ee559dcf/published?o=7405608220287115)

5 interactive charts built on real GoodReads data:
- Top 10 audiobook candidates ranked by weighted score
- Genre performance for portfolio strategy
- Score breakdown showing what drives each ranking
- Data quality distribution across 2.3M books
- Best book per genre for editorial decisions

---

## Two Pipeline Implementations

This platform was implemented in two ways to demonstrate different approaches
to the same problem. Both produce identical business results.

### Implementation A — Full PySpark Pipeline (Databricks Notebook)

    databricks/notebooks/01_bookworm_pipeline.py

The complete end-to-end pipeline in a single Databricks notebook.
Databricks handles Bronze ingestion, Silver transformation and Gold scoring.
All logic is in PySpark — fast to develop, easy to run interactively,
familiar to any Spark engineer.

    RAW → Bronze (Auto Loader) → Silver (PySpark) → Gold (PySpark) → Unity Catalog

When to use this approach: small teams, rapid prototyping, when analysts
do not need to contribute to transformation logic.

### Implementation B — Clean Separation (Databricks + dbt)

    databricks/notebooks/01_bookworm_pipeline.py  ← Bronze ingestion only
    dbt/models/                                   ← Silver and Gold

The Databricks notebook does one thing only — Bronze ingestion.
dbt owns all transformation from Silver onwards — version-controlled SQL,
automated testing, dbt contracts, lineage graph.

    RAW → Bronze (Databricks Auto Loader)
              → Silver (dbt staging + intermediate)
                    → Gold (dbt marts)

When to use this approach: larger teams, when analysts contribute to
transformation logic, when data contracts and automated testing are required.
This is the recommended production pattern for PIA Group.

### Why Both Exist

The full PySpark notebook was built first to explore the real GoodReads schema
interactively. Once the schema was understood, the dbt models were built with
that knowledge — correct first time. This mirrors the correct professional
workflow: explore in a notebook, formalise in dbt.

---

## Architecture

```
RAW (ADLS Gen2)
    goodreads/books/      1.94GB compressed JSON
    goodreads/reviews/    5.1GB compressed JSON
    goodreads/authors/    17MB
    goodreads/genres/     23MB
    goodreads/series/     27MB
          |
          | Databricks Auto Loader
          | Bootstrap + incremental pattern
          | Exactly-once via checkpointing
          v
BRONZE (Delta Lake)
    bronze_books          2,360,668 records
    bronze_reviews        15,739,967 records
    bronze_authors        829,529 records
    bronze_genres         2,360,655 records
    bronze_series         400,390 records
          |
          |------------------------------------
          |                                  |
          | Implementation A                 | Implementation B
          | PySpark in notebook              | dbt SQL models
          |                                  |
          v                                  v
SILVER                                   SILVER (dbt)
  PySpark Silver transformation            stg_books
  Genre join                               stg_reviews
  Sentiment aggregation                    stg_genres
  SHA256 PII hashing                       int_books_enriched
          |                                  |
          v                                  v
GOLD                                     GOLD (dbt)
  PySpark Gold scoring                     mart_audiobook_candidates
  4-component weighted score               mart_genre_performance
  Audiobook ranking                        dbt contracts enforced
          |------------------------------------
          |
          v
UNITY CATALOG
    6 tables registered and governed
    Role-based access control
    Audit logging via system tables
          |
          v
DASHBOARD
    5 live Databricks SQL charts
```

---

## Scoring Formula

    weighted_score = (rating     x 35%)
                   + (popularity  x 25%)
                   + (sentiment   x 25%)
                   + (length      x 15%)

| Component | Weight | Logic | Why |
|-----------|--------|-------|-----|
| Rating | 35% | average_rating / 5.0 | Quality is primary — a bad book makes a bad audiobook |
| Popularity | 25% | LN(ratings_count) / LN(5M) | Log scale prevents mega-popular books dominating |
| Sentiment | 25% | positive_review_pct from 15.7M real reviews | Actual reader enthusiasm |
| Length | 15% | 200-400 pages = 1.0, 800+ pages = 0.2 | Production economics — 800+ pages = 40hr recording |

All weights parameterised in dbt/dbt_project.yml. Change three numbers and run dbt run.

---

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Storage | Azure ADLS Gen2 North Europe | GDPR data residency |
| Compute | Azure Databricks | Native Delta Lake + Auto Loader + Unity Catalog |
| Table format | Delta Lake | ACID transactions, time travel, schema evolution |
| Transformation | dbt-databricks 1.10.19 | Version-controlled SQL, testing, data contracts |
| Governance | Unity Catalog | Centralised access control |
| CI/CD | GitHub Actions | dbt compile on every commit |
| Dashboard | Databricks SQL | Live charts on Unity Catalog tables |

---

## Repository Structure

```
bookworm-data-platform/
├── databricks/
│   └── notebooks/
│       └── 01_bookworm_pipeline.py   Full PySpark pipeline (Implementation A)
│                                     Also serves as Bronze-only for Implementation B
├── dbt/
│   ├── dbt_project.yml               Weights and thresholds configured here
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/
│       │   ├── sources.yml           Bronze table definitions
│       │   ├── stg_books.sql         Clean book metadata
│       │   ├── stg_reviews.sql       Reviews + SHA256 PII hashing
│       │   ├── stg_genres.sql        Genre UNPIVOT
│       │   └── schema.yml            Tests + dbt contracts
│       ├── intermediate/
│       │   ├── int_books_enriched.sql Books + genre + sentiment joined
│       │   └── schema.yml
│       └── marts/
│           ├── mart_audiobook_candidates.sql  Gold scoring
│           ├── mart_genre_performance.sql     Genre strategy
│           └── schema.yml            Tests + dbt contracts
├── docs/
│   ├── architecture.md
│   └── gdpr_and_compliance.md
└── .github/
    └── workflows/
        └── dbt_ci.yml                GitHub Actions dbt compile on push
```

---

## How to Run

### Option A — Full PySpark Pipeline

Open databricks/notebooks/01_bookworm_pipeline.py in Databricks.
Add your Azure storage key to Cell 1 and run all 8 cells in order.
Runtime: approximately 30 minutes on a single node cluster.

### Option B — Databricks Bronze + dbt Silver/Gold

Step 1: Run Cells 1-4 in the Databricks notebook (Bronze ingestion only).

Step 2: Run dbt for Silver and Gold transformation.

    cd dbt
    cp profiles.yml.example profiles.yml
    dbt run
    dbt test

Step 3: Register Bronze tables in Unity Catalog (Cell 7 in notebook).

Step 4: View results in the live dashboard or query Unity Catalog directly.

---

## Data Quality

| Layer | Records | Tests |
|-------|---------|-------|
| bronze_books | 2,360,668 | Source tests |
| bronze_reviews | 15,739,967 | Source tests |
| stg_books | 2,353,073 | dbt contract enforced |
| stg_reviews | 15,188,082 | dbt contract enforced |
| mart_audiobook_candidates | 10,670 | dbt contract enforced |

43 dbt tests passing — not_null, unique, relationships, accepted_values, dbt contracts.

---

## GDPR and Compliance

See docs/gdpr_and_compliance.md for full details.

- PII hashing: user_id SHA256 hashed in stg_reviews — no raw PII reaches Gold
- Data residency: All data in Azure North Europe Frankfurt
- Audit trail: Delta Lake time travel — query any historical state
- Access control: Unity Catalog role-based access per persona
- Right to erasure: DELETE + VACUUM procedure documented

---

## Known Limitations

Sentiment analysis is rating-based. Production upgrade: Spark NLP on review text.
See the demonstration cell in the Databricks notebook.

genre_non-fiction and genre_young-adult contain hyphens causing UNPIVOT syntax issues.
These genres fall back to shelf_genre. Production fix: rename at Bronze boundary.

dbt runs locally. Production: dbt Cloud or Databricks Workflows for scheduling.
GitHub Actions validates SQL on every commit.

---

## Scoring Strategies

| Strategy | rating_weight | popularity_weight | sentiment_weight | length_weight |
|----------|--------------|-------------------|-----------------|--------------|
| Default | 0.35 | 0.25 | 0.25 | 0.15 |
| Conservative | 0.50 | 0.35 | 0.10 | 0.05 |
| Discovery | 0.60 | 0.10 | 0.25 | 0.05 |
| Sentiment-first | 0.25 | 0.20 | 0.50 | 0.05 |

---

Built by Ather Nawaz | Senior Data Engineer
Stack: Azure ADLS Gen2 · Databricks · Delta Lake · dbt · Unity Catalog