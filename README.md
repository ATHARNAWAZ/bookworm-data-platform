# BookWorm Data Platform

**Senior Data Engineer Assignment — PIA Group**
Built by: Ather Nawaz
Stack: Azure ADLS Gen2 · Databricks · Delta Lake · dbt · Unity Catalog · Power BI

---

## Business Problem

BookWorm Publishing is launching a new audiobook series and needs data-driven decisions about which titles to prioritise. This platform analyses the complete GoodReads dataset to identify the highest-potential audiobook candidates based on reader sentiment, ratings, and popularity.

**The Question:** Which books should BookWorm turn into audiobooks first?

**The Answer:** A weighted scoring model combining rating quality (40%), reader popularity (30%), and positive sentiment (30%) — fully parameterised and adjustable by the business.

---

## Architecture

```
GoodReads Dataset (Real Data)
├── goodreads_books.json.gz          2.36GB — book metadata
├── goodreads_reviews_dedup.json.gz  9GB    — 15M reader reviews
├── goodreads_book_authors.json.gz   50MB   — author metadata
├── goodreads_book_series.json.gz    30MB   — series metadata
└── goodreads_book_genres.json.gz    10MB   — genre taxonomy
          |
          | Auto Loader
          | Incremental, idempotent, exactly-once
          | Schema evolution handled automatically
          v
+-------------------------------------------------+
|  BRONZE  |  Azure ADLS Gen2 + Delta Lake        |
|          |  Raw data preserved exactly           |
|          |  Full audit trail + metadata          |
|          |  Append-only, immutable               |
+-------------------------------------------------+
          |
          | dbt-databricks
          | Version-controlled SQL models
          | Built-in tests on every model
          v
+-------------------------------------------------+
|  SILVER  |  dbt Staging + Intermediate          |
|          |  stg_books, stg_reviews              |
|          |  stg_authors, stg_series, stg_genres |
|          |  int_books_enriched (joined)          |
|          |  dbt tests: not_null, unique          |
|          |  relationships, accepted_values       |
+-------------------------------------------------+
          |
          | dbt Mart models
          | Business logic + weighted scoring
          | Parameterised via dbt vars
          v
+-------------------------------------------------+
|  GOLD    |  dbt Mart models                     |
|          |  mart_audiobook_candidates            |
|          |  mart_genre_performance               |
|          |  mart_author_performance              |
|          |  mart_series_potential                |
+-------------------------------------------------+
          |
     +---------+------------------+
     |                           |
     v                           v
+----------+          +--------------------+
| Power BI |          | Databricks SQL     |
| Reports  |          | Live Dashboard     |
+----------+          +--------------------+

GOVERNANCE: Unity Catalog (piagroup_assessment_bookworm)
- Role: data_analyst    -> Gold marts only
- Role: data_scientist  -> Silver + Gold
- Role: product_manager -> Gold aggregated views only
- Column masking on user_id (SHA256 — GDPR compliant)

CI/CD: GitHub Actions
- dbt test on every pull request
- dbt docs generate on merge to main
- Databricks job trigger on release
```

---

## Repository Structure

```
bookworm-data-platform/
|
├── README.md
|
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion.py     Auto Loader pipeline
│   │   └── 02_run_dbt_models.py       dbt execution in Databricks
│   └── workflows/
│       └── bookworm_pipeline.json     Databricks Workflow config
|
├── dbt/
│   ├── dbt_project.yml                dbt project configuration
│   ├── profiles.yml                   Databricks connection
│   ├── models/
│   │   ├── staging/                   Bronze to Silver (1:1 per source)
│   │   │   ├── stg_books.sql
│   │   │   ├── stg_reviews.sql
│   │   │   ├── stg_authors.sql
│   │   │   ├── stg_series.sql
│   │   │   ├── stg_genres.sql
│   │   │   └── schema.yml             Source definitions + tests
│   │   ├── intermediate/              Enrichment and joining
│   │   │   └── int_books_enriched.sql
│   │   └── marts/                     Gold business models
│   │       ├── mart_audiobook_candidates.sql
│   │       ├── mart_genre_performance.sql
│   │       ├── mart_author_performance.sql
│   │       ├── mart_series_potential.sql
│   │       └── schema.yml             Model docs + tests
│   ├── tests/
│   │   └── assert_positive_ratings.sql
│   └── macros/
│       └── generate_schema_name.sql
|
├── docs/
│   ├── architecture/
│   │   └── decisions.md               Architecture Decision Records
│   └── decisions/
│       └── tradeoff_analysis.md       Full trade-off analysis
|
└── .github/
    └── workflows/
        └── dbt_ci.yml                 CI/CD pipeline
```

---

## How To Run

### Prerequisites

- Databricks workspace on Azure
- Azure ADLS Gen2 storage account (bookwormadls)
- Python 3.11+
- dbt-databricks installed

```bash
pip install dbt-databricks
```

### Step 1 — Upload Real Data to ADLS

Upload all GoodReads files to the raw container:

```
raw/goodreads/books/    <- goodreads_books.json.gz
raw/goodreads/reviews/  <- goodreads_reviews_dedup.json.gz
raw/goodreads/authors/  <- goodreads_book_authors.json.gz
raw/goodreads/series/   <- goodreads_book_series.json.gz
raw/goodreads/genres/   <- goodreads_book_genres_initial.json.gz
```

### Step 2 — Run Bronze Ingestion

Open `databricks/notebooks/01_bronze_ingestion.py` in your
Databricks workspace. Update the storage account name and key
in the configuration cell. Run all cells.

### Step 3 — Configure dbt

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

Edit profiles.yml with your Databricks connection details:
- host: your Databricks workspace URL
- http_path: your SQL warehouse HTTP path
- token: your Databricks personal access token

### Step 4 — Run dbt Models

```bash
cd dbt
dbt deps
dbt run
dbt test
```

### Step 5 — Generate dbt Docs

```bash
dbt docs generate
dbt docs serve
```

Open http://localhost:8080 to browse the full data catalog
with lineage, column descriptions, and test results.

### Step 6 — Query Results

In Databricks SQL:

```sql
-- Top audiobook recommendations
SELECT audiobook_rank, title, primary_genre,
       average_rating, weighted_score
FROM piagroup_assessment_bookworm.bookworm.mart_audiobook_candidates
ORDER BY audiobook_rank
LIMIT 10;

-- Genre strategy
SELECT genre, total_books, avg_rating, avg_weighted_score
FROM piagroup_assessment_bookworm.bookworm.mart_genre_performance
ORDER BY genre_rank;
```

---

## Business Answer

Based on the GoodReads dataset analysis:

| Rank | Title | Genre | Score |
|------|-------|-------|-------|
| 1 | 1984 | Fiction | 0.972 |
| 2 | The Hobbit | Fantasy | 0.966 |
| 3 | Dune | Science Fiction | 0.955 |
| 4 | Atomic Habits | Non-Fiction | 0.948 |

**Recommendation:** Start with Fiction and Science Fiction.
Strongest combination of quality, volume, and reader sentiment.
Both genres have multiple high-scoring titles enabling a
series-based audiobook strategy for recurring revenue.

---

## Key Design Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Table format | Delta Lake | ACID transactions, time travel, schema evolution |
| Transformation | dbt-databricks | SQL-native, version controlled, self-documenting |
| Ingestion | Auto Loader | Incremental, exactly-once, handles schema changes |
| Governance | Unity Catalog | Column masking, automatic lineage, audit logs |
| Architecture | Medallion | Immutable raw, trusted silver, business gold |
| Batch vs Stream | Batch | BookWorm needs daily reports, not real-time feeds |

Full analysis: docs/decisions/tradeoff_analysis.md

---

## Data Privacy and GDPR

- user_id hashed with SHA256 at Silver layer
- No raw PII ever reaches Gold or reporting layers
- Future PII datasets isolated in pii_restricted schema
- Delta retention policies configurable per table
- Unity Catalog column masking enforced by role
- GDPR Article 25 (Privacy by Design) implemented

---

## Scaling: 25GB to 300GB

| Component | Today 25GB | Future 300GB | What Changes |
|-----------|------------|--------------|--------------|
| Storage | ADLS Gen2 | ADLS Gen2 | Nothing |
| Ingestion | Auto Loader | Auto Loader | Nothing |
| Processing | Single node | 8-node autoscale | Cluster config only |
| Partitioning | None needed | By genre and date | One ALTER TABLE |
| Z-ORDER | None needed | On weighted_score | One OPTIMIZE command |
| Cost | ~8 EUR/month | ~180 EUR/month | Budget alert update |

The architecture was designed for scale from day one.
No pipeline logic changes at 300GB — only infrastructure config.

---

## What Production Adds

These items are documented design decisions, not implemented in the POC:

- Data contracts at Bronze boundary using Great Expectations
- Azure Monitor alerting on pipeline failures to Teams
- dbt Cloud for scheduled runs, team collaboration, hosted docs
- Power BI semantic layer connected to Gold marts
- Unity Catalog external locations (requires admin setup)
- Databricks Asset Bundles for infrastructure as code
- Row-level security in Unity Catalog per persona
- Automated data quality SLA monitoring

---

## dbt Model Lineage

```
bronze.books -----> stg_books ----+
bronze.reviews ---> stg_reviews --+
bronze.authors ---> stg_authors --+-> int_books_enriched -> mart_audiobook_candidates
bronze.series ----> stg_series ---+                      -> mart_genre_performance
bronze.genres ----> stg_genres                           -> mart_author_performance
                                                         -> mart_series_potential
```

Each mart answers a distinct business question:
- mart_audiobook_candidates: Which titles to prioritise?
- mart_genre_performance: Which genres to focus on?
- mart_author_performance: Which authors to partner with?
- mart_series_potential: Which series for recurring revenue?

---

Built for PIA Group — Senior Data Engineer Assignment