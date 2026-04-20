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

## Architecture

```
RAW (ADLS Gen2)
    goodreads/books/      — 1.94GB compressed JSON
    goodreads/reviews/    — 5.1GB compressed JSON
    goodreads/authors/    — 17MB
    goodreads/genres/     — 23MB
    goodreads/series/     — 27MB
          |
          | Databricks Auto Loader
          | Bootstrap + incremental pattern
          | Exactly-once via checkpointing
          v
BRONZE (Delta Lake)
    bronze_books          — 2,360,668 records
    bronze_reviews        — 15,739,967 records
    bronze_authors        — 829,529 records
    bronze_genres         — 2,360,655 records
    bronze_series         — 400,390 records
          |
          | dbt-databricks
          | Version-controlled SQL transformations
          | 43 automated data quality tests
          v
SILVER (dbt staging + intermediate)
    stg_books             — 2,353,073 deduplicated, validated books
    stg_reviews           — 15,188,082 reviews with SHA256 PII hashing
    stg_genres            — primary genre per book via UNPIVOT
    int_books_enriched    — books joined with genre and real sentiment metrics
          |
          | dbt marts
          | Parameterised scoring formula
          | dbt contracts enforced
          v
GOLD (dbt marts)
    mart_audiobook_candidates  — 10,670 ranked books
    mart_genre_performance     — 9 genre performance metrics
          |
          v
UNITY CATALOG + DASHBOARD
    Governed access via Unity Catalog
    Live Databricks SQL dashboard
```

---

## Scoring Formula

```
weighted_score = (rating     x 35%)
               + (popularity  x 25%)
               + (sentiment   x 25%)
               + (length      x 15%)
```

| Component | Weight | Logic | Why |
|-----------|--------|-------|-----|
| Rating | 35% | average_rating / 5.0 | Quality is primary — a bad book makes a bad audiobook |
| Popularity | 25% | LN(ratings_count) / LN(5M) | Log scale market validation — prevents mega-popular books dominating |
| Sentiment | 25% | positive_review_pct / 100 from 15.7M real reviews | Actual reader enthusiasm |
| Length | 15% | 200-400 pages = 1.0, 800+ pages = 0.2 | Production economics — 800+ pages = 40+ hour recording |

All weights are parameterised in `dbt/dbt_project.yml`. Change three numbers and run `dbt run` — new rankings in minutes.

---

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Storage | Azure ADLS Gen2 (North Europe) | GDPR data residency — German data stays in Germany |
| Compute | Azure Databricks | Native Delta Lake + Auto Loader + Unity Catalog |
| Table format | Delta Lake | ACID transactions, time travel, schema evolution |
| Transformation | dbt-databricks 1.10.19 | Version-controlled SQL, built-in testing, data contracts |
| Governance | Unity Catalog | Centralised access control across all workspaces |
| CI/CD | GitHub Actions | dbt compile on every commit — SQL validation automated |
| Dashboard | Databricks SQL | Live charts on real Unity Catalog tables |

---

## Repository Structure

```
bookworm-data-platform/
├── databricks/
│   └── notebooks/
│       └── 01_bookworm_pipeline.py   # Bronze ingestion only
├── dbt/
│   ├── dbt_project.yml               # Weights and thresholds configured here
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/
│       │   ├── sources.yml           # Bronze table definitions
│       │   ├── stg_books.sql         # Clean book metadata
│       │   ├── stg_reviews.sql       # Reviews + SHA256 PII hashing
│       │   ├── stg_genres.sql        # Genre UNPIVOT
│       │   └── schema.yml            # Tests + dbt contracts
│       ├── intermediate/
│       │   ├── int_books_enriched.sql # Books + genre + sentiment joined
│       │   └── schema.yml
│       └── marts/
│           ├── mart_audiobook_candidates.sql  # Gold scoring model
│           ├── mart_genre_performance.sql     # Genre strategy
│           └── schema.yml            # Tests + dbt contracts
├── docs/
│   ├── architecture.md
│   └── gdpr_and_compliance.md
└── .github/
    └── workflows/
        └── dbt_ci.yml                # GitHub Actions — dbt compile on push
```

---

## How to Run

### Step 1 — Bronze Ingestion (Databricks)

Open `databricks/notebooks/01_bookworm_pipeline.py` in Databricks.
Add your Azure storage key to Cell 1 and run all cells in order.
Runtime: approximately 30 minutes on a single node cluster.

### Step 2 — Silver and Gold Transformation (dbt)

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your Databricks connection details

dbt run    # Build all 6 models
dbt test   # Run 43 data quality tests
```

### Step 3 — View Results

Query Unity Catalog tables directly:

```sql
SELECT audiobook_rank, title, primary_genre,
       average_rating, num_pages, length_category,
       weighted_score
FROM piagroup_assessment_bookworm.bookworm_gold.mart_audiobook_candidates
ORDER BY audiobook_rank
LIMIT 10
```

Or open the live dashboard link above.

---

## Data Quality

| Layer | Records | Null check | Tests |
|-------|---------|-----------|-------|
| bronze_books | 2,360,668 | PERMISSIVE mode captures corrupt records | Source tests |
| bronze_reviews | 15,739,967 | PERMISSIVE mode | Source tests |
| stg_books | 2,353,073 | 0 nulls on book_id, title, average_rating | dbt contract enforced |
| stg_reviews | 15,188,082 | 0 nulls on review_id, book_id, rating | dbt contract enforced |
| mart_audiobook_candidates | 10,670 | 0 nulls on all key columns | dbt contract enforced |

**43 dbt tests passing** — not_null, unique, relationships, accepted_values, and dbt contracts.

---

## GDPR and Compliance

See `docs/gdpr_and_compliance.md` for full details.

- PII hashing: `user_id` SHA256 hashed in `stg_reviews` — no raw PII reaches Gold
- Data residency: All data in Azure North Europe (Frankfurt)
- Audit trail: Delta Lake time travel — query any historical state
- Access control: Unity Catalog role-based access per persona
- Right to erasure: DELETE + VACUUM procedure documented

---

## Known Limitations

**Sentiment analysis is rating-based**
Current implementation classifies sentiment from star ratings (4+ = positive).
Production upgrade: Spark NLP on review text for audiobook-specific signals
(narrator quality, pacing, listening experience). Zero additional cost on existing cluster.
See the demonstration cell in the Databricks notebook.

**genre_non-fiction and genre_young-adult in UNPIVOT**
These two genre columns contain hyphens which cause SQL syntax issues in UNPIVOT.
Books in these genres fall back to `shelf_genre` from popular_shelves.
Production fix: rename columns at Bronze ingestion boundary.

**dbt runs locally**
dbt currently runs from developer machine via CLI.
Production: dbt Cloud or Databricks Workflows for scheduled automated runs.
GitHub Actions CI/CD validates SQL on every commit.

---

## Scoring Strategies

Adjust weights in `dbt/dbt_project.yml` and run `dbt run` — no SQL changes needed.

| Strategy | rating_weight | popularity_weight | sentiment_weight | length_weight |
|----------|--------------|-------------------|-----------------|--------------|
| Default | 0.35 | 0.25 | 0.25 | 0.15 |
| Conservative (proven hits) | 0.50 | 0.35 | 0.10 | 0.05 |
| Discovery (hidden gems) | 0.60 | 0.10 | 0.25 | 0.05 |
| Sentiment-first (fan communities) | 0.25 | 0.20 | 0.50 | 0.05 |

---

## CI/CD

GitHub Actions runs `dbt compile` on every push to `dbt/` — validates all SQL models
and dependency resolution without requiring a live database connection.

[![dbt CI](https://github.com/ATHARNAWAZ/bookworm-data-platform/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/ATHARNAWAZ/bookworm-data-platform/actions/workflows/dbt_ci.yml)

---

**Built by Ather Nawaz | Senior Data Engineer**
**Stack: Azure ADLS Gen2 · Databricks · Delta Lake · dbt · Unity Catalog**
