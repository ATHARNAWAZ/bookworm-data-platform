/*
  Model    : stg_books
  Layer    : Staging (Silver)
  Source   : bronze.bronze_books
  Consumers: int_books_enriched

  Cleans and standardises raw GoodReads book metadata.
  One row per unique book_id — latest version kept on deduplication.

  Key transformations:
  - Deduplicate by book_id keeping latest ingestion
  - Cast numeric fields from string to proper types
  - Extract primary_author_id from authors array
  - Extract shelf_genre from popular_shelves as genre fallback
  - Calculate data_quality_flag based on ratings count
  - Filter books with no valid rating
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_books') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY book_id
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM source
    WHERE book_id IS NOT NULL
),

cleaned AS (
    SELECT
        book_id,
        title,

        -- Extract primary author from nested array
        authors[0].author_id                            AS primary_author_id,

        -- Cast from string — GoodReads stores all numerics as strings
        CAST(average_rating AS DOUBLE)                  AS average_rating,
        CAST(ratings_count AS BIGINT)                   AS ratings_count,
        CAST(text_reviews_count AS BIGINT)              AS text_reviews_count,
        CAST(num_pages AS INTEGER)                      AS num_pages,

        -- Book metadata
        language_code,
        publisher,
        format,
        description,
        isbn,
        isbn13,

        -- Genre fallback — most books have NULL in genres array
        -- popular_shelves contains reader-assigned shelf names
        -- which are the best genre proxy available in GoodReads
        popular_shelves[0].name                         AS shelf_genre,

        -- Data quality signal for downstream filtering
        -- Books with fewer than 10K ratings are statistically unreliable
        CASE
            WHEN CAST(ratings_count AS BIGINT)
                >= {{ var('min_ratings_high') }}
                THEN 'high_confidence'
            WHEN CAST(ratings_count AS BIGINT)
                >= {{ var('min_ratings_medium') }}
                THEN 'medium_confidence'
            ELSE 'low_confidence'
        END                                             AS data_quality_flag,

        _ingestion_timestamp,
        CURRENT_TIMESTAMP()                             AS _silver_timestamp

    FROM deduplicated
    WHERE rn = 1
      AND title IS NOT NULL
      AND CAST(average_rating AS DOUBLE) IS NOT NULL
      AND CAST(average_rating AS DOUBLE) > 0
)

SELECT * FROM cleaned