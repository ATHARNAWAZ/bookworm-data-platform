/*
  Model    : stg_reviews
  Layer    : Staging (Silver)
  Source   : bronze.bronze_reviews
  Consumers: int_books_enriched

  Cleans and validates 15.7M GoodReads reader reviews.
  Applies GDPR-compliant PII hashing and sentiment classification.

  GDPR note:
  user_id is SHA256 hashed — GDPR Article 25 Privacy by Design.
  The hash preserves user behaviour pattern analysis without
  storing any identifiable information. No raw user_id ever
  reaches downstream layers.

  Sentiment note:
  Classification is rating-based (4+ = positive).
  Production upgrade: Spark NLP on review_text for true sentiment
  including audiobook-specific signals like narrator quality.
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_reviews') }}
),

validated AS (
    SELECT *
    FROM source
    WHERE review_id IS NOT NULL
      AND book_id IS NOT NULL
      AND rating BETWEEN 1 AND 5
),

transformed AS (
    SELECT
        review_id,
        book_id,
        -- GDPR: SHA256 hash — one-way, cannot recover original user_id
        SHA2(CAST(user_id AS STRING), 256)              AS user_id_hashed,
        CAST(rating AS INTEGER)                         AS rating,
        -- Sentiment from star rating
        -- Fallback until Spark NLP is implemented
        CASE
            WHEN rating >= 4 THEN 'positive'
            WHEN rating = 3  THEN 'neutral'
            ELSE                  'negative'
        END                                             AS sentiment,
        TO_DATE(date_added, 'yyyy-MM-dd')               AS review_date,
        YEAR(TO_DATE(date_added, 'yyyy-MM-dd'))         AS review_year,
        review_text,
        _ingestion_timestamp,
        CURRENT_TIMESTAMP()                             AS _silver_timestamp
    FROM validated
)

SELECT * FROM transformed