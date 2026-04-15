/*
  Model    : int_books_enriched
  Layer    : Intermediate (Silver)
  Sources  : stg_books, stg_reviews
  Consumer : All mart models

  Business Purpose:
  Single enriched book entity joining metadata
  with review sentiment metrics.

  All Gold marts reference this one model.
  Adding a new mart = one new SQL file only.
  No re-joining ever needed.
*/

WITH books AS (
    SELECT * FROM {{ ref('stg_books') }}
),

review_metrics AS (
    SELECT
        book_id,
        COUNT(review_id)                        AS total_reviews,
        AVG(CAST(rating AS DOUBLE))             AS avg_review_rating,
        ROUND(
            SUM(CASE WHEN sentiment = 'positive'
                THEN 1 ELSE 0 END)
            * 100.0 / COUNT(review_id), 2
        )                                       AS positive_review_pct,
        ROUND(
            SUM(CASE WHEN sentiment = 'neutral'
                THEN 1 ELSE 0 END)
            * 100.0 / COUNT(review_id), 2
        )                                       AS neutral_review_pct,
        ROUND(
            SUM(CASE WHEN sentiment = 'negative'
                THEN 1 ELSE 0 END)
            * 100.0 / COUNT(review_id), 2
        )                                       AS negative_review_pct,
        MIN(review_date)                        AS first_review_date,
        MAX(review_date)                        AS latest_review_date
    FROM {{ ref('stg_reviews') }}
    GROUP BY book_id
),

enriched AS (
    SELECT
        b.book_id,
        b.title,
        b.primary_author_id,
        b.average_rating,
        b.ratings_count,
        b.text_reviews_count,
        b.num_pages,
        b.primary_genre,
        b.description,
        b.data_quality_flag,
        COALESCE(r.total_reviews, 0)            AS total_reviews,
        COALESCE(r.avg_review_rating, 0)        AS avg_review_rating,
        COALESCE(r.positive_review_pct, 0)      AS positive_review_pct,
        COALESCE(r.neutral_review_pct, 0)       AS neutral_review_pct,
        COALESCE(r.negative_review_pct, 0)      AS negative_review_pct,
        r.first_review_date,
        r.latest_review_date,
        b._ingestion_timestamp,
        CURRENT_TIMESTAMP()                     AS _enriched_timestamp
    FROM books b
    LEFT JOIN review_metrics r ON b.book_id = r.book_id
)

SELECT * FROM enriched