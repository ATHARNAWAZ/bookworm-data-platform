/*
  Model    : mart_author_performance
  Layer    : Gold
  Source   : int_books_enriched
  Consumer : Power BI, Contract Negotiations

  Business Purpose:
  Author performance for partnership decisions.
  Consistently high-performing authors = lower risk
  audiobook investments.
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_books_enriched') }}
),

author_metrics AS (
    SELECT
        primary_genre                               AS genre,
        COUNT(DISTINCT book_id)                     AS total_books,
        ROUND(AVG(average_rating), 2)               AS avg_rating,
        SUM(ratings_count)                          AS total_ratings,
        ROUND(AVG(positive_review_pct), 1)          AS avg_positive_pct,
        ROUND(AVG(
            (average_rating / 5.0
                * {{ var('rating_weight') }}) +
            (LN(ratings_count + 1) / LN(500001)
                * {{ var('popularity_weight') }}) +
            (positive_review_pct / 100.0
                * {{ var('sentiment_weight') }})
        ), 4)                                       AS avg_weighted_score,
        ROUND(STDDEV(average_rating), 3)            AS rating_stddev
    FROM enriched
    WHERE data_quality_flag IN (
        'high_confidence', 'medium_confidence'
    )
    GROUP BY primary_genre
    HAVING COUNT(DISTINCT book_id) >= 1
)

SELECT
    RANK() OVER (
        ORDER BY avg_weighted_score DESC
    )                                               AS author_rank,
    genre,
    total_books,
    avg_rating,
    total_ratings,
    avg_positive_pct,
    avg_weighted_score,
    CASE
        WHEN rating_stddev < 0.3 THEN 'highly_consistent'
        WHEN rating_stddev < 0.6 THEN 'consistent'
        ELSE 'variable'
    END                                             AS consistency_rating,
    CURRENT_TIMESTAMP()                             AS _mart_timestamp
FROM author_metrics
ORDER BY author_rank