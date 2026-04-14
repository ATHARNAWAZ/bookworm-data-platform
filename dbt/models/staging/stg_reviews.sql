/*
  Model    : stg_reviews
  Layer    : Staging (Silver)
  Source   : bronze_reviews
  Consumer : int_books_enriched

  Business Purpose:
  Clean review data. Hash PII. Classify sentiment.
  
  Privacy: user_id SHA256 hashed — GDPR Article 25.
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_reviews') }}
),

validated AS (
    SELECT *
    FROM source
    WHERE rating BETWEEN 1 AND 5
      AND book_id IS NOT NULL
      AND review_id IS NOT NULL
),

transformed AS (
    SELECT
        review_id,
        book_id,

        -- WHY: Hash raw user_id for GDPR compliance
        -- Original user identity never stored downstream
        SHA2(CAST(user_id AS STRING), 256)          AS user_id_hashed,

        CAST(rating AS INT)                         AS rating,

        CASE
            WHEN rating >= 4 THEN 'positive'
            WHEN rating = 3  THEN 'neutral'
            ELSE                  'negative'
        END                                         AS sentiment,

        review_text,

        TO_DATE(date_added, 'yyyy-MM-dd')           AS review_date,

        _ingestion_timestamp,
        CURRENT_TIMESTAMP()                         AS _silver_timestamp

    FROM validated
)

SELECT * FROM transformed