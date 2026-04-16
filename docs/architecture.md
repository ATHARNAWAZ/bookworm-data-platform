# BookWorm Data Platform — Architecture

## Overview

Production-grade data platform analysing 2.3 million real GoodReads books
and 15.7 million reader reviews to identify audiobook candidates.

The platform was implemented in two ways — a full PySpark pipeline and a
clean Databricks + dbt separation — to demonstrate different architectural
approaches to the same business problem.

---

## Two Implementations

### Implementation A — Full PySpark Pipeline

The complete Bronze to Gold pipeline lives in a single Databricks notebook.
Databricks Auto Loader handles ingestion. PySpark handles all transformation
and scoring. Unity Catalog registers the output tables.

    RAW (ADLS) → Bronze (Auto Loader) → Silver (PySpark) → Gold (PySpark) → Unity Catalog

Advantages: Single tool, fast development, easy interactive exploration,
familiar to any Spark engineer, no additional tooling required.

When to choose this: small teams, rapid prototyping, when analysts do not
need to contribute to transformation logic.

### Implementation B — Clean Separation (Databricks + dbt)

The Databricks notebook handles Bronze ingestion only.
dbt owns all transformation from Silver onwards.

    RAW (ADLS) → Bronze (Databricks Auto Loader)
                       → Silver (dbt staging + intermediate)
                               → Gold (dbt marts with contracts)

Advantages: Clean separation of concerns. Ingestion and transformation are
independently deployable and testable. dbt provides version-controlled SQL
that any analyst can read and contribute to. Data contracts validate schema
at every boundary. The lineage graph documents the entire data flow automatically.

When to choose this: larger teams where analysts contribute to transformation,
when data contracts and automated testing are required, when the transformation
layer needs to be independently versioned and reviewed.

### Why Both Were Built

The full PySpark notebook was built first to explore the real GoodReads schema
interactively. The data has nested arrays, structs with invalid field names,
strings that should be numerics, and NULL values in unexpected places.
Interactive Spark exploration revealed these issues before any dbt model was written.

Once the schema was understood, the dbt models were built with that knowledge —
correct first time with no schema surprises. This mirrors the correct professional
workflow: explore in a notebook, formalise in dbt.

### Recommended Production Pattern for PIA Group

Implementation B. The Databricks notebook handles Bronze ingestion — loading raw
client company data from whatever format it arrives in. dbt handles the mapping
to PIA Group's standard German model — version-controlled, testable, auditable SQL
that the analytics team can review and understand without Python knowledge.

---

## Design Principles

One responsibility per layer. Bronze stores raw data exactly as received — immutable,
never modified. Silver cleans, validates and enriches. Gold serves business questions.

One tool per job. Databricks handles file ingestion at scale. dbt handles
transformation. Neither tool does the other job.

Parameterised over hardcoded. Scoring weights, quality thresholds and filters
are configured in dbt_project.yml. Business strategy changes without code changes.

Honest over optimistic. Known limitations are documented. Production upgrade paths
are specified. Nothing is presented as more complete than it is.

---

## Layer Design

### RAW Layer — Azure ADLS Gen2

Five GoodReads datasets land in the RAW container as compressed JSON files.
No transformation happens here. RAW is a landing zone only.

    raw/goodreads/books/      1.94GB    2.36M book records
    raw/goodreads/reviews/    5.10GB    15.7M reader reviews
    raw/goodreads/authors/    17MB      829K author records
    raw/goodreads/genres/     23MB      genre classification per book
    raw/goodreads/series/     27MB      series metadata

### BRONZE Layer — Delta Lake

The Databricks notebook reads from RAW and writes to Bronze Delta tables.
Bronze is the immutable audit trail — every record that arrived is preserved.

Ingestion pattern: Bootstrap on first run, Auto Loader incremental afterwards.
First run uses spark.read to load all historical files. A checkpoint is written
marking all files as processed. Every subsequent run uses cloudFiles format
to detect and process only new files — exactly-once guarantee.

Why Delta Lake over Parquet: ACID transactions prevent corrupt partial writes.
Time travel enables point-in-time queries for audit and compliance.
Schema evolution handles new fields from GoodReads without pipeline changes.

Why PERMISSIVE mode: Real GoodReads JSON has malformed records. PERMISSIVE captures
them in a corrupt_record column rather than silently dropping them. In a finance
context, silent data loss is never acceptable.

Why Auto Loader over Azure Data Factory: ADF suits SaaS API and database sources.
For file-based JSON at this scale, Auto Loader detects new files via Azure Event Grid,
handles schema evolution automatically, and provides exactly-once semantics without
separate orchestration infrastructure.

### SILVER Layer

#### Implementation A — PySpark

Silver reads from Bronze Delta paths directly via spark.read. Transformations include
deduplication, type casting, genre join, sentiment aggregation from reviews,
SHA256 PII hashing. Output written to Delta Lake SILVER container.

#### Implementation B — dbt Models

stg_books: Deduplicates by book_id, casts numeric fields from string, extracts
primary_author_id from nested authors array, calculates data_quality_flag,
applies dbt contract schema validation.

stg_reviews: Validates rating range 1-5, SHA256 hashes user_id for GDPR compliance,
classifies sentiment from rating, parses review date, applies dbt contract.

stg_genres: UNPIVOT converts wide genre table (one column per genre) to long format,
selects the genre with highest reader count as primary_genre per book.

int_books_enriched: Joins stg_books with stg_genres and aggregated review metrics
from stg_reviews. Calculates positive_review_pct and negative_review_pct from
15.7 million real reader reviews. All Gold marts reference this single model.

Why views for staging and intermediate: Views recompute on every query — no storage
cost, always reflect the latest Bronze data. Gold marts are materialised as tables
because analysts and dashboards query them repeatedly.

Why dbt contracts: Schema validation at the Silver boundary. If GoodReads renames
a Bronze column, the contract fails immediately before any bad data reaches analysts.

### GOLD Layer

#### Implementation A — PySpark

Gold reads Silver Delta tables, applies the four-component weighted scoring formula,
ranks candidates by score, calculates genre summaries. Output written to Delta Lake
GOLD container and registered in Unity Catalog.

#### Implementation B — dbt Marts

mart_audiobook_candidates: 10,670 books scored and ranked by audiobook potential.
Parameterised weights configurable in dbt_project.yml without SQL changes.
dbt contract enforced — schema validated on every run.

mart_genre_performance: Nine genre aggregations showing average score, rating,
total ratings and sentiment. Supports portfolio strategy decisions.

### Why Not Star Schema

The medallion Lakehouse pattern suits this specific business question better than
a traditional dimensional model. A fact_reviews table and dim_books dimension would
add modelling complexity without benefit for the current use case.

For PIA Group's client master data — tracking company names, VAT numbers and legal
entities across acquisitions — SCD Type 2 is the correct pattern and would be
implemented for those dimensions specifically.

### Why Not SCD Type 2 Here

Delta Lake time travel solves point-in-time querying without the overhead of
surrogate keys and current flags. For external reference data like GoodReads,
time travel is sufficient. For PIA Group's own client master data, SCD Type 2
would be implemented.

---

## Scoring Formula

    weighted_score = (rating     x 35%)
                   + (popularity  x 25%)
                   + (sentiment   x 25%)
                   + (length      x 15%)

Rating (35%): average_rating / 5.0
Quality is the primary signal. A poorly rated book makes a poor audiobook.

Popularity (25%): LN(ratings_count + 1) / LN(5,000,001)
Log scale normalisation prevents mega-popular books dominating. LN(4.7M) vs
LN(500K) is 15.4 vs 13.1 — meaningful but not overwhelming.

Sentiment (25%): positive_review_pct / 100 from 15.7M real reviews.
Falls back to rating proxy for books with no reviews.
Current limitation: rating-based classification. Production: Spark NLP.

Length (15%): CASE on num_pages.
200-400 pages = 1.0 (ideal 6-12 hour audiobook).
400-600 pages = 0.8 (good).
600-800 pages = 0.4 (long, high cost).
800+ pages = 0.2 (very long, 40+ hours, high risk).
NULL = 0.5 (neutral).

---

## Data Quality

43 dbt tests on every pipeline execution.

not_null tests on all critical columns.
unique tests on primary keys.
relationships test — every review references a valid book.
accepted_values tests on categorical columns.
dbt contracts — schema validation at Silver and Gold boundaries.

---

## GDPR and Compliance

Full details in docs/gdpr_and_compliance.md.

PII handling: user_id SHA256 hashed at Silver boundary. No raw PII reaches Gold.
Data residency: All infrastructure in Azure North Europe Frankfurt.
Audit logging: Unity Catalog system tables log every data access.
Right to erasure: DELETE + VACUUM documented procedure.

---

## Scaling Strategy

At 25GB current: Single node, 30 minutes. No changes needed.
At 100GB: Two worker nodes, 15 minutes. Cluster config change only.
At 300GB: Eight-node autoscale, 45 minutes. Config change only.

Auto Loader, Delta Lake and dbt SQL all scale horizontally without code changes.

---

## Production Additions

Spark NLP for true sentiment on review text. Same cluster, zero incremental cost.
Three weeks to implement. 15-20% improvement estimated.

Azure Key Vault for secrets management. Currently storage key in notebook config.

dbt Cloud or Databricks Workflows for scheduled runs. Currently manual.

Great Expectations for Bronze boundary schema contracts.

Row-level security in Unity Catalog. Column masking for future PII datasets.