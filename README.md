# BookWorm Data Platform
**Senior Data Engineer**

A production-grade data platform built on Azure Databricks, dbt, Delta Lake and Unity Catalog. Analyses 2.3 million real GoodReads books and 15.7 million reader reviews to identify the highest-potential audiobook candidates for BookWorm Publishing.

---

## Business Answer

**Top audiobook candidate: Harry Potter and the Sorcerer's Stone**
Score: 0.9367 | Rating: 4.45 | Pages: 320 (ideal) | 4.7M ratings | Genre: Fantasy/Paranormal

| Rank | Title | Genre | Score | Pages |
|------|-------|-------|-------|-------|
| 1 | Harry Potter and the Sorcerer's Stone | fantasy_paranormal | 0.9367 | 320 ideal |
| 2 | The Hunger Games | young-adult | 0.9223 | 374 ideal |
| 3 | To Kill a Mockingbird | fiction | 0.9151 | 324 ideal |
| 4 | Harry Potter and the Chamber of Secrets | fantasy_paranormal | 0.9115 | 341 ideal |
| 5 | Harry Potter and the Prisoner of Azkaban | fiction | 0.9075 | 435 good |

---

## Live Dashboard

[BookWorm Audiobook Intelligence Dashboard](https://adb-7405608220287115.15.azuredatabricks.net/dashboardsv3/01f1382f39881c339c2f7e69ee559dcf/published)

5 charts built on real GoodReads data:
- Top 10 audiobook candidates ranked by weighted score
- Genre performance for portfolio strategy
- Score breakdown showing what drives each ranking
- Data quality distribution across 2.3M books
- Best book per genre for editorial decisions

---

## Two Pipeline Implementations

Both produce identical business results. Both are available in this repository.

### Implementation A — Full PySpark Pipeline
`databricks/notebooks/01_bookworm_pipeline.py`

End-to-end pipeline in a single Databricks notebook. Bronze ingestion, Silver transformation and Gold scoring all in PySpark.

```
RAW → Bronze (Auto Loader) → Silver (PySpark) → Gold (PySpark) → Unity Catalog
```

**When to use:** Small teams, rapid prototyping, when analysts do not need to contribute to transformation logic.

### Implementation B — Databricks + dbt (Recommended)
`databricks/notebooks/01_bookworm_pipeline.py` ← Bronze ingestion only
`dbt/models/` ← Silver and Gold

The notebook does Bronze ingestion only. dbt owns all transformation — version-controlled SQL, automated testing, dbt contracts, lineage graph.

```
RAW → Bronze (Databricks Auto Loader)
          → Silver (dbt staging + intermediate)
                → Gold (dbt marts)
```

**When to use:** Production environments, larger teams, when data contracts and automated testing are required. **This is the recommended pattern for PIA Group.**

### Why Both Exist

The PySpark notebook was built first to explore the real GoodReads schema interactively. Once the schema was understood, dbt models were built with that knowledge — correct first time. This mirrors the correct professional workflow: explore in a notebook, formalise in dbt.

---

## Architecture

```
RAW (ADLS Gen2 — North Europe / Frankfurt)
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
          v
SILVER (dbt)                          SILVER (PySpark — Implementation A)
    stg_books                             PySpark Silver transformation
    stg_reviews                           Genre join
    stg_genres                            Sentiment aggregation
    int_books_enriched                    SHA256 PII hashing
          |
          v
GOLD (dbt)                            GOLD (PySpark — Implementation A)
    mart_audiobook_candidates             4-component weighted score
    mart_genre_performance                Audiobook ranking
    dbt contracts enforced
          |
          v
UNITY CATALOG
    6 tables registered and governed
    Role-based access control (3 personas)
    Audit logging via system.access.audit
          |
          v
DASHBOARD + BI
    Databricks SQL Dashboard (5 live charts)
    Power BI via native Databricks connector
```

---

## Scoring Formula

```
weighted_score = (rating      × 35%)
               + (popularity  × 25%)
               + (sentiment   × 25%)
               + (length      × 15%)
```

| Component | Weight | Formula | Why |
|-----------|--------|---------|-----|
| Rating | 35% | `average_rating / 5.0` | Quality is primary — a bad book makes a bad audiobook |
| Popularity | 25% | `LN(ratings_count) / LN(5M)` | Log scale prevents mega-popular books dominating |
| Sentiment | 25% | `positive_review_pct` from 15.7M real reviews | Actual reader enthusiasm not just star average |
| Length | 15% | 200-400pp = 1.0, 800+pp = 0.2 | Production economics — 800+ pages = 40hr recording |

All weights parameterised in `dbt/dbt_project.yml`. Change three numbers and run `dbt run` — new rankings in 2 minutes with zero code changes.

---

## Data Dictionary

### bronze_books
| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| book_id | string | GoodReads unique book identifier | Primary key. May have duplicates — same book different editions |
| title | string | Full book title including series info | e.g. "Harry Potter and the Sorcerer's Stone (Harry Potter, #1)" |
| average_rating | string | Average star rating 0.00–5.00 | Stored as string in source — cast to DOUBLE in Silver |
| ratings_count | string | Total number of star ratings | Cast to BIGINT in Silver |
| text_reviews_count | string | Number of written text reviews | Subset of ratings_count |
| num_pages | string | Page count of edition | NULL for many records — handled in scoring |
| isbn | string | 10-digit ISBN | May be NULL or invalid |
| isbn13 | string | 13-digit ISBN | May be NULL or invalid |
| language_code | string | Publication language | e.g. "eng", "fre", "spa" |
| _ingestion_timestamp | timestamp | When record was loaded to Bronze | Added by Auto Loader |
| _source_file | string | ADLS path of source file | Lineage tracking |
| _batch_id | string | Pipeline run identifier | Format: YYYYMMDD_HHMMSS |

### bronze_reviews
| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| review_id | string | Unique review identifier | Primary key |
| user_id | string | GoodReads user identifier | RAW PII — hashed to SHA256 in Silver. Never reaches Gold |
| book_id | string | References bronze_books.book_id | Note: ID system differs from books file — joined on book_id directly |
| rating | string | Star rating given by reviewer | Valid values: 1, 2, 3, 4, 5. Cast to INT in Silver |
| review_text | string | Full text of written review | Used for future Spark NLP implementation |
| date_added | string | Date review was posted | |
| _ingestion_timestamp | timestamp | When record was loaded to Bronze | |

### bronze_genres
| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| book_id | string | References bronze_books.book_id | |
| genre_fantasy_paranormal | integer | Count of readers who shelved in this genre | UNPIVOTED to long format in stg_genres |
| genre_fiction | integer | Count of readers who shelved in this genre | |
| genre_romance | integer | Count of readers who shelved in this genre | |
| genre_mystery_thriller_crime | integer | Count of readers who shelved in this genre | |
| genre_young_adult | integer | Count of readers who shelved in this genre | |
| genre_history_biography | integer | Count of readers who shelved in this genre | |
| genre_children | integer | Count of readers who shelved in this genre | |
| genre_non_fiction | integer | Count of readers who shelved in this genre | |
| genre_poetry | integer | Count of readers who shelved in this genre | |

### silver_books (stg_books output)
| Column | Type | Description | Valid Values |
|--------|------|-------------|--------------|
| book_id | string | Deduplicated book identifier | Unique — one row per book |
| title | string | Book title | Not null |
| average_rating | double | Star rating normalised | 0.0 – 5.0 |
| ratings_count | bigint | Total star ratings | Not null, > 0 |
| num_pages | integer | Page count | NULL allowed |
| data_quality_flag | string | Statistical reliability tier | high_confidence (≥100K ratings), medium_confidence (≥10K), low_confidence |
| length_category | string | Audiobook production classification | ideal (200-400pp), good (400-600pp), short (100-200pp), long (600-800pp), very_long (800+pp), unknown |
| primary_genre | string | Top genre by reader shelf count | fantasy_paranormal, fiction, romance, young_adult, mystery_thriller_crime, history_biography, children, non_fiction, poetry, Uncategorised |

### silver_reviews (stg_reviews output)
| Column | Type | Description | Valid Values |
|--------|------|-------------|--------------|
| review_id | string | Unique review identifier | Not null, unique |
| user_id_hashed | string | SHA256 hash of original user_id | 64-character hex string. Raw user_id never stored |
| book_id | string | References silver_books | Not null |
| rating | integer | Star rating | 1, 2, 3, 4, 5 |
| sentiment | string | Derived from rating | positive (≥4), neutral (=3), negative (≤2) |
| review_text | string | Full review text | May be NULL |

### gold_audiobook_candidates (mart output)
| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| audiobook_rank | integer | Overall rank by weighted_score | 1 = best candidate |
| book_id | string | Unique book identifier | |
| title | string | Book title | |
| primary_genre | string | Top genre | |
| average_rating | double | Star rating | |
| ratings_count | bigint | Total ratings | |
| num_pages | integer | Page count | |
| length_category | string | Production classification | |
| total_reviews | bigint | Review count from reviews dataset | May be 0 if no matching reviews |
| positive_review_pct | double | % of reviews rated ≥4 | NULL if no reviews — falls back to rating proxy |
| negative_review_pct | double | % of reviews rated ≤2 | |
| rating_score | double | Rating component (35%) | 0.0 – 0.35 |
| popularity_score | double | Popularity component (25%) | 0.0 – 0.25 |
| sentiment_score | double | Sentiment component (25%) | 0.0 – 0.25 |
| length_score | double | Length component (15%) | 0.0 – 0.15 |
| weighted_score | double | Total score | 0.0 – 1.0 |

---

## Data Governance Design

### Personas and Access Control

Three personas are defined for Unity Catalog role-based access:

| Persona | Schema Access | Use Case |
|---------|--------------|----------|
| `data_analyst` | Gold only | Editorial team — view rankings and recommendations |
| `data_scientist` | Silver + Gold | Analytics team — build models on enriched data |
| `product_manager` | Aggregated Gold views only | Leadership — genre summary, no individual book details |

**Unity Catalog implementation (production):**
```sql
-- Grant Gold access to analysts
GRANT SELECT ON SCHEMA piagroup_assessment_bookworm.bookworm_gold
    TO data_analyst;

-- Grant Silver + Gold to data scientists
GRANT SELECT ON SCHEMA piagroup_assessment_bookworm.bookworm_silver
    TO data_scientist;
GRANT SELECT ON SCHEMA piagroup_assessment_bookworm.bookworm_gold
    TO data_scientist;

-- Row-level filter for country-based access (PIA Group cross-border)
CREATE ROW FILTER filter_by_country
    ON silver_books (country_code)
    AS (country_code = current_user_country());
```

### Audit Logging

Unity Catalog automatically logs every data access to `system.access.audit`:

```sql
-- Query who accessed what and when
SELECT
    event_time,
    user_name,
    action_name,
    request_params.table_full_name AS table_accessed,
    request_params.operation_type
FROM system.access.audit
WHERE action_name IN ('SELECT', 'DESCRIBE')
  AND event_time > CURRENT_TIMESTAMP - INTERVAL 30 DAYS
ORDER BY event_time DESC
```

### Data Residency

All infrastructure is provisioned in **Azure North Europe (Frankfurt)**. German data never physically leaves Germany. Delta Share provides governed cross-border views — aggregated data only, no raw client data crosses borders. This satisfies GDPR and German BDSG requirements.

---

## Data Quality — Great Expectations

Data quality expectations are documented at the Bronze boundary. In production these run automatically after every Auto Loader write.

### bronze_books expectations

```python
# expectations/bronze_books_suite.py
import great_expectations as ge

context = ge.get_context()
suite   = context.create_expectation_suite("bronze_books_suite",
                                           overwrite_existing=True)

validator = context.get_validator(
    datasource_name  = "databricks_bronze",
    data_asset_name  = "bronze_books",
    expectation_suite_name = "bronze_books_suite"
)

# Primary key integrity
validator.expect_column_values_to_not_be_null("book_id")
validator.expect_column_values_to_be_of_type("book_id", "StringType")

# Rating validity
validator.expect_column_values_to_not_be_null("average_rating")
validator.expect_column_values_to_be_between(
    "average_rating", min_value=0.0, max_value=5.0,
    mostly=0.99  # allow 1% malformed
)

# Volume check — alert if record count drops more than 5%
validator.expect_table_row_count_to_be_between(
    min_value=2_000_000,
    max_value=3_000_000
)

# ratings_count must be positive
validator.expect_column_values_to_be_between(
    "ratings_count", min_value=0, mostly=0.99
)

validator.save_expectation_suite(discard_failed_expectations=False)
```

### bronze_reviews expectations

```python
# expectations/bronze_reviews_suite.py
validator.expect_column_values_to_not_be_null("review_id")
validator.expect_column_values_to_not_be_null("book_id")
validator.expect_column_values_to_not_be_null("rating")

# Rating must be 1-5
validator.expect_column_values_to_be_in_set(
    "rating", [1, 2, 3, 4, 5], mostly=0.98
)

# Volume check
validator.expect_table_row_count_to_be_between(
    min_value=14_000_000,
    max_value=17_000_000
)
```

**Production integration:** Great Expectations checkpoints run after every Bronze write in Databricks Workflows. If any critical expectation fails — volume drops more than 5%, ratings outside 0-5 — the pipeline halts and Slack is notified before Silver processing begins.

---

## Pipeline Run Log

Every pipeline execution writes one row to the run log table in Unity Catalog. This provides full operational visibility without additional tooling.

### Table Definition

```sql
CREATE TABLE IF NOT EXISTS
    piagroup_assessment_bookworm.bookworm.pipeline_runs (
        run_id                STRING    COMMENT 'Unique run identifier — YYYYMMDD_HHMMSS',
        run_timestamp         TIMESTAMP COMMENT 'Pipeline start time UTC',
        trigger               STRING    COMMENT 'manual | scheduled | triggered',
        bronze_books_count    BIGINT    COMMENT 'Records in bronze_books after run',
        bronze_reviews_count  BIGINT    COMMENT 'Records in bronze_reviews after run',
        silver_books_count    BIGINT    COMMENT 'Records in silver_books after run',
        silver_reviews_count  BIGINT    COMMENT 'Records in silver_reviews after run',
        gold_candidates_count BIGINT    COMMENT 'Records in gold_audiobook_candidates',
        dbt_tests_passed      INT       COMMENT 'dbt tests passing this run',
        dbt_tests_failed      INT       COMMENT 'dbt tests failing this run',
        duplicates_removed    BIGINT    COMMENT 'Books removed by deduplication',
        duration_seconds      INT       COMMENT 'Total pipeline runtime in seconds',
        status                STRING    COMMENT 'success | failed | partial',
        error_message         STRING    COMMENT 'Error detail if status = failed'
    )
USING DELTA
COMMENT 'Operational run log — one row per pipeline execution'
```

### Query Run History

```sql
-- Last 10 pipeline runs
SELECT
    run_id,
    run_timestamp,
    bronze_books_count,
    silver_books_count,
    gold_candidates_count,
    dbt_tests_passed,
    dbt_tests_failed,
    duration_seconds,
    status
FROM piagroup_assessment_bookworm.bookworm.pipeline_runs
ORDER BY run_timestamp DESC
LIMIT 10;

-- Alert: any failed runs in last 7 days
SELECT * FROM piagroup_assessment_bookworm.bookworm.pipeline_runs
WHERE status = 'failed'
  AND run_timestamp > CURRENT_TIMESTAMP - INTERVAL 7 DAYS;

-- Trend: record growth over time
SELECT
    DATE(run_timestamp)       AS run_date,
    MAX(bronze_books_count)   AS bronze_books,
    MAX(silver_books_count)   AS silver_books,
    MAX(gold_candidates_count) AS gold_candidates
FROM piagroup_assessment_bookworm.bookworm.pipeline_runs
WHERE status = 'success'
GROUP BY DATE(run_timestamp)
ORDER BY run_date;
```

---

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Storage | Azure ADLS Gen2 North Europe | GDPR data residency — Frankfurt |
| Compute | Azure Databricks | Native Delta Lake + Auto Loader + Unity Catalog |
| Table format | Delta Lake | ACID transactions, time travel, schema evolution |
| Transformation | dbt-databricks 1.10.19 | Version-controlled SQL, testing, data contracts |
| Governance | Unity Catalog | Centralised access control across all tools |
| Data Quality | Great Expectations | Expectation suites at Bronze boundary |
| CI/CD | GitHub Actions | dbt compile on every commit |
| Dashboard | Databricks SQL | Live charts on Unity Catalog tables |
| BI | Power BI | Native Databricks connector |

---

## Repository Structure

```
bookworm-data-platform/
├── databricks/
│   └── notebooks/
│       └── 01_bookworm_pipeline.py     Full PySpark pipeline (Implementation A)
│                                        Also serves as Bronze-only for Implementation B
├── dbt/
│   ├── dbt_project.yml                 Weights and thresholds configured here
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/
│       │   ├── sources.yml             Bronze table definitions
│       │   ├── stg_books.sql           Deduplication + cleaning
│       │   ├── stg_reviews.sql         SHA256 PII hashing + sentiment
│       │   ├── stg_genres.sql          UNPIVOT wide to long
│       │   └── schema.yml              Tests + dbt contracts
│       ├── intermediate/
│       │   ├── int_books_enriched.sql  Books + genre + sentiment joined
│       │   └── schema.yml
│       └── marts/
│           ├── mart_audiobook_candidates.sql   Gold scoring + ranking
│           ├── mart_genre_performance.sql       Genre strategy
│           └── schema.yml              Tests + dbt contracts
├── expectations/
│   ├── bronze_books_suite.py           Great Expectations — books
│   └── bronze_reviews_suite.py         Great Expectations — reviews
├── docs/
│   ├── architecture.md
│   ├── gdpr_and_compliance.md
│   └── data_dictionary.md              Full column reference
└── .github/
    └── workflows/
        └── dbt_ci.yml                  GitHub Actions — dbt compile on push
```

---

## How to Run

### Option A — Full PySpark Pipeline

Open `databricks/notebooks/01_bookworm_pipeline.py` in Databricks. Add your Azure storage key to Cell 1 and run all 8 cells in order. Runtime: approximately 30 minutes on a single node cluster.

### Option B — Databricks Bronze + dbt Silver/Gold

**Step 1:** Run Cells 1–4 in the Databricks notebook (Bronze ingestion only).

**Step 2:** Run dbt for Silver and Gold:
```bash
cd dbt
cp profiles.yml.example profiles.yml
dbt run
dbt test
```

**Step 3:** Register Bronze tables in Unity Catalog (Cell 7 in notebook).

**Step 4:** View results in the live dashboard or query Unity Catalog directly.

---

## Data Quality Summary

| Layer | Records | Tests | Status |
|-------|---------|-------|--------|
| bronze_books | 2,360,668 | Great Expectations suite | Defined |
| bronze_reviews | 15,739,967 | Great Expectations suite | Defined |
| stg_books | 2,353,073 | dbt contract enforced | PASS |
| stg_reviews | 15,188,082 | dbt contract enforced | PASS |
| mart_audiobook_candidates | 10,673 | dbt contract enforced | PASS |

**43 dbt tests passing — PASS=43 WARN=0 ERROR=0**
Tests: not_null (12), unique (6), relationships (1), accepted_values (4), dbt contracts (20)

---

## GDPR and Compliance

See `docs/gdpr_and_compliance.md` for full details.

- **PII hashing:** `user_id` SHA256 hashed in `stg_reviews` — no raw PII reaches Gold or dashboards
- **Data residency:** All data in Azure North Europe Frankfurt — satisfies BDSG and GDPR
- **Audit trail:** Delta Lake time travel — query any historical state with `VERSION AS OF`
- **Access control:** Unity Catalog RBAC — three personas, enforced at platform level
- **Right to erasure:** `DELETE` + `VACUUM` procedure documented in compliance doc
- **Audit logging:** `system.access.audit` — every data access logged automatically

---

## Known Limitations

1. **Sentiment is rating-based.** Production upgrade: Spark NLP on review text. Demonstration cell in the Databricks notebook. Same cluster — zero additional cost.

2. **genre_non-fiction and genre_young-adult** contain hyphens causing UNPIVOT syntax issues. These genres fall back to `shelf_genre`. Production fix: rename at Bronze boundary.

3. **dbt runs locally.** Production: dbt Cloud or Databricks Workflows for scheduling. GitHub Actions validates SQL on every commit.

4. **Great Expectations defined but not yet scheduled.** Production: integrate as Databricks Workflows task before Silver processing.

5. **Pipeline run log table defined but not yet populated.** Production: Cell 8 writes one row per execution.

6. **Row-level security designed but not configured.** Three personas documented. Unity Catalog row filter implementation: 30 minutes.

---

## Scoring Strategies

| Strategy | rating_weight | popularity_weight | sentiment_weight | length_weight |
|----------|--------------|------------------|-----------------|---------------|
| Default | 0.35 | 0.25 | 0.25 | 0.15 |
| Conservative | 0.50 | 0.35 | 0.10 | 0.05 |
| Discovery | 0.60 | 0.10 | 0.25 | 0.05 |
| Sentiment-first | 0.25 | 0.20 | 0.50 | 0.05 |

---

*Built by Ather Nawaz | Senior Data Engineer*
*Stack: Azure ADLS Gen2 · Databricks · Delta Lake · dbt · Unity Catalog · Great Expectations*
