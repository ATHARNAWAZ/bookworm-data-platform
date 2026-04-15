/*
  Model    : stg_books
  Layer    : Staging (Silver)
  Source   : bronze_books (real GoodReads — 2.3M books)
  Consumer : int_books_enriched

  Business Purpose:
  Standardise and validate real GoodReads book metadata.
  One row per unique book. Foundation for all analysis.

  Key Transformations:
  - Deduplicate using latest ingestion timestamp
  - Extract primary author from nested authors array
  - Extract primary genre from genres array
  - Calculate data quality confidence flag
  - Cast numeric fields from string to proper types
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_books') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY book_id
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM source
),

cleaned AS (
    SELECT
        book_id,
        title,

        -- WHY: Extract first author from nested array
        -- authors is array of structs: [{author_id, role}]
        -- Primary author drives audiobook rights decisions
        authors[0].author_id                        AS primary_author_id,

        -- WHY: Cast to double for numerical scoring
        -- Source stores all ratings as strings
        CAST(average_rating AS DOUBLE)              AS average_rating,
        CAST(ratings_count AS BIGINT)               AS ratings_count,
        CAST(text_reviews_count AS BIGINT)          AS text_reviews_count,
        CAST(num_pages AS INTEGER)              AS num_pages,
        description,

        -- WHY: Extract primary genre from array
        -- First genre is the dominant classification
        -- Fallback to Uncategorised for null genres
        COALESCE(genres[0], 'Uncategorised')        AS primary_genre,

        -- WHY: Quality flag based on ratings count
        -- Statistical reliability threshold:
        -- 5 reviews at 5 stars = unreliable signal
        -- 100K reviews at 4.5 stars = highly reliable
        -- Only high/medium confidence books reach Gold
        CASE
            WHEN CAST(ratings_count AS BIGINT)
                >= {{ var('min_ratings_high') }}
                THEN 'high_confidence'
            WHEN CAST(ratings_count AS BIGINT)
                >= {{ var('min_ratings_medium') }}
                THEN 'medium_confidence'
            ELSE 'low_confidence'
        END                                         AS data_quality_flag,

        _ingestion_timestamp,
        CURRENT_TIMESTAMP()                         AS _silver_timestamp

    FROM deduplicated
    WHERE rn = 1
      AND book_id IS NOT NULL
      AND title IS NOT NULL
      AND average_rating IS NOT NULL
      -- WHY: Some GoodReads records have non-numeric
      -- average_rating values. CAST produces NULL.
      -- We filter these at staging boundary.
      AND CAST(average_rating AS DOUBLE) IS NOT NULL
      AND CAST(average_rating AS DOUBLE) > 0
)

SELECT * FROM cleaned