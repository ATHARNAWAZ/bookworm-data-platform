/*
  Model    : mart_audiobook_candidates
  Layer    : Gold
  Source   : int_books_enriched
  Consumer : Power BI, Databricks Dashboard, Product Managers

  Business Purpose:
  Primary business output. Ranks 11,124 statistically
  reliable books by audiobook potential.

  Scoring Formula:
  weighted_score = (rating    × 40%)
                 + (popularity × 30%)
                 + (sentiment  × 30%)

  WHY these weights:
  Rating (40%): Quality is primary signal.
  A bad book makes a bad audiobook regardless of popularity.

  Popularity (30%): Market validation reduces investment risk.
  LOG scale — LN(5M) vs LN(500K) is reasonable not 10x.

  Sentiment (30%): Forward-looking reader enthusiasm.
  Currently rating-based proxy. Production uses Spark NLP
  on review text for audiobook-specific sentiment signals.

  WHY parameterised via dbt vars:
  Business can change weights without touching SQL.
  Three named strategies in dbt_project.yml vars.
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_books_enriched') }}
),

scored AS (
    SELECT
        *,
        LN(ratings_count + 1)                   AS log_ratings,
        LN(5000001)                             AS max_log_ratings,

        -- Sentiment proxy — rating heuristic
        CASE
            WHEN average_rating >= 4.0 THEN 1.0
            WHEN average_rating >= 3.0 THEN 0.5
            ELSE 0.0
        END                                     AS sentiment_score

    FROM enriched
    WHERE data_quality_flag IN (
        'high_confidence', 'medium_confidence'
    )
    AND primary_genre NOT IN (
        'comics_graphic', 'Uncategorised'
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
            (sentiment_score
                * {{ var('sentiment_weight') }}),
        4)                                      AS weighted_score
    FROM scored
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            ORDER BY weighted_score DESC
        )                                       AS audiobook_rank,
        RANK() OVER (
            PARTITION BY primary_genre
            ORDER BY weighted_score DESC
        )                                       AS genre_rank
    FROM weighted
)

SELECT
    audiobook_rank,
    genre_rank,
    book_id,
    title,
    --title_without_series,
    primary_author_id,
    primary_genre,
    average_rating,
    ratings_count,
    text_reviews_count,
    total_reviews,
    positive_review_pct,
    negative_review_pct,
    weighted_score,
    ROUND(average_rating / 5.0
        * {{ var('rating_weight') }}, 4)        AS rating_contribution,
    ROUND(log_ratings / max_log_ratings
        * {{ var('popularity_weight') }}, 4)    AS popularity_contribution,
    ROUND(sentiment_score
        * {{ var('sentiment_weight') }}, 4)     AS sentiment_contribution,
    data_quality_flag,
    _enriched_timestamp
FROM ranked
ORDER BY audiobook_rank