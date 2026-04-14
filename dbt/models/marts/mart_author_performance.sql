/*
  Model    : mart_author_performance
  Layer    : Gold
  Source   : int_books_enriched
  Consumer : Power BI, Contract Negotiations

  Business Purpose:
  Author performance for partnership decisions.
  Consistently high-performing authors = lower risk
  audiobook investments.

  Key Insight:
  An author with 5 books all rating 4.5+ is more
  valuable than one book at 5.0 and four at 2.0.
  Consistency score captures this.
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_books_enriched') }}
),

author_metrics AS (
    SELECT
        primary_author_id                       AS author_id,
        COUNT(DISTINCT book_id)                 AS total_books,
        ROUND(AVG(average_rating), 2)           AS avg_rating,
        SUM(ratings_count)                      AS total_ratings,
        ROUND(AVG(positive_review_pct), 1)      AS avg_positive_pct,
        ROUND(AVG(
            (average_rating / 5.0
                * {{ var('rating_weight') }}) +
            (LN(ratings_count + 1) / LN(5000001)
                * {{ var('popularity_weight') }}) +
            (CASE
                WHEN average_rating >= 4.0 THEN 1.0
                WHEN average_rating >= 3.0 THEN 0.5
                ELSE 0.0
             END * {{ var('sentiment_weight') }})
        ), 4)                                   AS avg_weighted_score,
        ROUND(STDDEV(average_rating), 3)        AS rating_stddev
    FROM enriched
    WHERE data_quality_flag IN (
        'high_confidence', 'medium_confidence'
    )
    AND primary_author_id IS NOT NULL
    GROUP BY primary_author_id
    HAVING COUNT(DISTINCT book_id) >= 1
)

SELECT
    RANK() OVER (
        ORDER BY avg_weighted_score DESC
    )                                           AS author_rank,
    author_id,
    total_books,
    avg_rating,
    total_ratings,
    avg_positive_pct,
    avg_weighted_score,
    CASE
        WHEN rating_stddev < 0.3 THEN 'highly_consistent'
        WHEN rating_stddev < 0.6 THEN 'consistent'
        ELSE 'variable'
    END                                         AS consistency_rating,
    CURRENT_TIMESTAMP()                         AS _mart_timestamp
FROM author_metrics
ORDER BY author_rank