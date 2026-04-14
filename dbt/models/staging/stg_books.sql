/*
  Model    : stg_books
  Layer    : Staging (Silver)
  Source   : bronze_books (raw GoodReads JSON)
  Consumer : int_books_enriched

  Business Purpose:
  Clean and standardise raw book data from GoodReads.
  One row per unique book. Foundation for all book analysis.
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

        -- WHY: Cast to double for numerical analysis
        -- Source has ratings as strings
        CAST(average_rating AS DOUBLE)              AS average_rating,
        CAST(ratings_count AS BIGINT)               AS ratings_count,

        -- WHY: Extract first genre from array as primary
        COALESCE(
            genres[0], 'Uncategorised'
        )                                           AS primary_genre,

        -- WHY: Keep all genres as string for multi-genre analysis
        CONCAT_WS(', ', genres)                     AS all_genres,

        description,

        -- WHY: Calculate quality flag here in staging
        -- Tells analysts how much to trust the rating signal
        -- 5 reviews at 5 stars means nothing statistically
        -- 500,000 reviews at 4.5 stars is highly reliable
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
      AND average_rating IS NOT NULL
      AND title IS NOT NULL
      AND book_id IS NOT NULL
)

SELECT * FROM cleaned