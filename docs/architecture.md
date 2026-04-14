# BookWorm Data Platform — Architecture

## Overview
Production-grade data platform built on Azure Databricks + dbt + Delta Lake.
Analyses 2.3 million real GoodReads books to identify audiobook candidates.

## Stack
- Azure ADLS Gen2 — storage layer
- Databricks — compute and orchestration
- Delta Lake — table format (ACID, time travel)
- dbt-databricks — transformation layer
- Unity Catalog — governance and access control

## Layers

### Bronze
Raw GoodReads JSON ingested via Auto Loader.
Bootstrap on first run, incremental on subsequent runs.
Exactly-once processing via checkpointing.

### Silver
dbt staging models clean and validate raw data.
PII hashed (SHA256) at this layer — GDPR compliant.
dbt intermediate model joins books with review metrics.

### Gold
dbt mart models calculate weighted audiobook scores.
Formula: Rating (40%) + Popularity (30%) + Sentiment (30%).
Fully parameterised — adjust weights in dbt_project.yml.

## Data Quality
- 2,360,668 books ingested into Bronze
- 2,360,131 books in Silver — zero nulls on critical columns
- 11,124 high/medium confidence books scored in Gold
- 24 dbt tests passing across all models

## Scaling Strategy
| Scale | Cluster | Change Required |
|-------|---------|-----------------|
| 25GB  | Single node | None |
| 100GB | 2 workers | Cluster config only |
| 300GB | 8-node autoscale | Cluster config + partitioning |

Auto Loader and Delta Lake scale automatically — zero code changes.
EOF

# Create dashboards readme
mkdir -p dashboards
cat > dashboards/README.md << 'EOF'
# BookWorm Audiobook Intelligence Dashboard

Live dashboard built on Databricks SQL:
https://adb-7405608220287115.15.azuredatabricks.net/dashboardsv3/01f1382f39881c339c2f7e69ee559dcf/published?o=7405608220287115

## Charts
1. Top 10 Audiobook Candidates — ranked by weighted score
2. Genre Performance — which genres to focus on
3. Score Breakdown — what drives each ranking
4. Data Quality Distribution — 2.3M books filtered to 11K reliable
5. Best Book Per Genre — one actionable title per genre

## Business Answer
Top candidate: Harry Potter and the Sorcerer's Stone (score: 0.9551)
Best genre: Fantasy/Paranormal
