# GDPR and Data Compliance Design

## Overview

BookWorm Data Platform is designed with privacy and compliance as foundational
requirements — not afterthoughts. This document covers the compliance architecture
relevant to PIA Group's finance and accounting context.

---

## 1. PII Handling — Implemented in POC

### What We Do

All user identifiers are SHA256 hashed at the Silver layer before any downstream
processing. The raw user_id never leaves the Bronze layer.

Location: dbt/models/staging/stg_reviews.sql

    SHA2(CAST(user_id AS STRING), 256) AS user_id_hashed

### Why SHA256

- One-way hash — original identity cannot be recovered
- Consistent — same user always produces the same hash
- We can still track user behaviour patterns anonymously
- No raw PII ever reaches Gold, dashboards or analysts
- Satisfies GDPR Article 25 — Privacy by Design

### What This Means Per Layer

| Layer | user_id | Access |
|---|---|---|
| Bronze | Raw original value | Restricted — engineering only |
| Silver | SHA256 hash | Data scientists |
| Gold | Not present | Analysts and product managers |

---

## 2. Role-Based Access Control

### Three Personas in Unity Catalog

| Persona | Access | Rationale |
|---|---|---|
| Data Analysts | Gold layer only | Need KPIs, not raw data |
| Data Scientists | Silver + Gold | Need row-level data for models |
| Product Managers | Gold aggregated views | Business decisions only |

### Why Unity Catalog

Unity Catalog enforces access at the catalog level — not at the application level.
This means access control is consistent regardless of which tool the user connects
with — SQL, Python, Power BI or Tableau.

One policy governs all workspaces. An analyst cannot bypass the restriction by
connecting directly to ADLS with a different tool.

### Production SQL — Grant and Revoke

Grant analyst access to Gold only:

    GRANT SELECT ON SCHEMA piagroup_assessment_bookworm.bookworm_gold
    TO data_analysts;

Deny Bronze and Silver explicitly:

    REVOKE ALL ON SCHEMA piagroup_assessment_bookworm.bookworm_silver
    FROM data_analysts;

### Column Masking for Future PII Datasets

When PIA Group onboards datasets containing real client PII — tax IDs, account
numbers, personal identifiers — Unity Catalog column masking ensures analysts
see masked values automatically without any application changes.

Example mask function:

    CREATE OR REPLACE FUNCTION mask_tax_id(tax_id STRING)
    RETURNS STRING
    RETURN CASE
        WHEN is_account_group_member('data_analysts')
        THEN CONCAT('***-**-', RIGHT(tax_id, 4))
        ELSE tax_id
    END;

---

## 3. Data Retention and Right to Erasure

### GDPR Article 17 — Right to Erasure

When a user requests deletion of their data, the process is:

Step 1 — Identify affected records using the hash:

    SELECT * FROM bronze_reviews
    WHERE SHA2(CAST(user_id AS STRING), 256) = 'target_hash'

Step 2 — Delete from Bronze:

    DELETE FROM bronze_reviews
    WHERE SHA2(CAST(user_id AS STRING), 256) = 'target_hash'

Step 3 — Remove historical Delta versions:

    VACUUM bronze_reviews RETAIN 0 HOURS

Step 4 — Reprocess Silver and Gold:
Run dbt — affected records are automatically removed from all downstream layers.

### Delta Time Travel Retention Policy

Different layers have different retention requirements:

| Layer | Retention | Reason |
|---|---|---|
| Bronze | 90 days | Regulatory audit requirement |
| Silver | 30 days | Operational reprocessing window |
| Gold | 7 days | Report reproducibility |

Set retention per table:

    ALTER TABLE bronze_reviews
    SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 90 days',
        'delta.deletedFileRetentionDuration' = 'interval 90 days'
    );

---

## 4. Audit Logging

### What Unity Catalog Provides Automatically

Every data access is logged without any additional instrumentation:

- Who accessed which table
- When they accessed it
- What query they ran
- How many rows were returned
- Which tool they used (SQL, Python, Power BI)

### Why This Matters for PIA Group

In an accounting and audit context, regulators may ask:
"Who accessed client revenue data on December 31st?"

Unity Catalog system tables answer this question instantly.

Query audit logs:

    SELECT
        user_identity.email   AS user,
        event_time,
        action_name,
        request_params.table_full_name AS table_accessed
    FROM system.access.audit
    WHERE request_params.table_full_name
        LIKE 'piagroup_assessment_bookworm%'
    AND event_time > '2026-01-01'
    ORDER BY event_time DESC

---

## 5. Data Residency — German Data Stays in Germany

### PIA Group Context

German client data must remain within Germany for legal compliance under
GDPR and German data protection law (BDSG).

### Our Architecture Supports This

All data is stored in Azure North Europe region (Frankfurt data centre):

- Storage account: bookwormadls — North Europe
- Databricks workspace: PIAGroup_Assessment_bookworm — North Europe
- Unity Catalog: piagroup_assessment_bookworm — North Europe

### Delta Share for Cross-Border Governance

Delta Share allows PIA Group to share governed views internationally without
copying raw data across borders:

    German Workspace (North Europe — Frankfurt)
    Raw client data — German team access only
           |
           | Delta Share
           v
    International Catalog
    Aggregated views only — no raw client data
    Read-only access for international teams

Raw data never leaves Germany. International teams see only pre-approved
aggregated views with no personal or client-identifiable information.

---

## 6. Data Quality as a Compliance Control

### Why Data Quality Matters in Finance

In an accounting context, incorrect data in a dashboard is not just a data
quality issue — it is a compliance risk. An auditor relying on incorrect
figures makes incorrect decisions.

### Our Automated Test Suite

24 dbt tests run automatically on every pipeline execution:

| Test Type | What It Checks | Why It Matters |
|---|---|---|
| not_null | No missing critical identifiers | Cannot have books without IDs |
| unique | No duplicate records | Duplicates distort aggregations |
| relationships | Referential integrity | Reviews must reference valid books |
| accepted_values | Only valid categories | Prevents corrupt sentiment labels |

If any test fails — the pipeline stops. No bad data reaches analysts.

Result from last run:

    PASS=24  WARN=0  ERROR=0  SKIP=0

This is the data engineering equivalent of a financial audit control.

### PERMISSIVE Mode — No Silent Data Loss

Bronze ingestion uses PERMISSIVE mode — corrupt records are captured in a
dedicated column rather than silently dropped:

    spark.read
        .option("mode", "PERMISSIVE")
        .json(raw_path)

This means we always know exactly what was rejected and why. In a finance
context, silent data loss is unacceptable — every record must be accounted for.

---

## 7. What Production Adds

The following items are documented design decisions not implemented in the POC.
Each has a clear implementation path on the existing infrastructure.

| Item | Description | Effort |
|---|---|---|
| Great Expectations | Schema contracts at Bronze boundary | 1 week |
| Azure Purview | Enterprise data catalog and lineage | 2 weeks |
| Azure Key Vault | Secrets management for all credentials | 2 days |
| Automated PII scanning | Detect PII in new datasets on arrival | 1 week |
| Dynamic column masking | Role-based masking via Unity Catalog functions | 3 days |
| Data quality SLA alerts | Azure Monitor alerts on test failures | 2 days |

---

## Summary

| Requirement | Status | Implementation |
|---|---|---|
| PII hashing | Implemented | SHA256 in stg_reviews.sql |
| Role-based access | Designed | Unity Catalog personas documented |
| Data retention | Designed | Delta time travel policies documented |
| Right to erasure | Designed | DELETE + VACUUM procedure documented |
| Audit logging | Available | Unity Catalog system.access.audit |
| Data residency | Implemented | Azure North Europe region |
| Data quality controls | Implemented | 24 dbt tests passing |
| Schema validation | Designed | Great Expectations in production path |