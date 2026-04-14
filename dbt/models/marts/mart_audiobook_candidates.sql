/*
  Model    : mart_audiobook_candidates
  Layer    : Gold
  Source   : int_books_enriched
  Consumer : Power BI, Databricks Dashboard, Product Managers

  Business Purpose:
  THE primary output. Ranks books by audiobook potential.
  
  Scoring Formula:
  weighted_score = (rating × 40%) 
                 + (log popularity × 30%)
                 + (positive sentiment × 30%)
  
  WHY parameterised: Business can adjust weights
  in dbt_project.yml without touching SQL.
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_books_enriched') }}
),

scored AS (
    SELECT
        *,
        LN(ratings_count + 1)                       AS log_ratings,
        LN(500001)                                  AS max_log_ratings
    FROM enriched
    WHERE data_quality_flag IN (
        'high_confidence', 'medium_confidence'
    )
),

weighted AS (
    SELECT
        *,
        ROUND(
            (average_rating / 5.0
                * {{ var('rating_weight') }}) +
            (log_ratings / max_log_ratings
                * {{ var('popularity_weight') }}) +
            (positive_review_pct / 100.0
                * {{ var('sentiment_weight') }}),
        4)                                          AS weighted_score
    FROM scored
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            ORDER BY weighted_score DESC
        )                                           AS audiobook_rank,
        RANK() OVER (
            PARTITION BY primary_genre
            ORDER BY weighted_score DESC
        )                                           AS genre_rank
    FROM weighted
)

SELECT
    audiobook_rank,
    genre_rank,
    book_id,
    title,
    primary_genre,
    all_genres,
    average_rating,
    ratings_count,
    total_reviews,
    positive_review_pct,
    negative_review_pct,
    weighted_score,
    ROUND(average_rating / 5.0
        * {{ var('rating_weight') }}, 4)            AS rating_contribution,
    ROUND(log_ratings / max_log_ratings
        * {{ var('popularity_weight') }}, 4)        AS popularity_contribution,
    ROUND(positive_review_pct / 100.0
        * {{ var('sentiment_weight') }}, 4)         AS sentiment_contribution,
    data_quality_flag,
    _enriched_timestamp
FROM ranked
ORDER BY audiobook_rank