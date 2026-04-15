/*
  Model    : mart_audiobook_candidates
  Layer    : Gold
  Source   : int_books_enriched
  Consumer : Power BI, Databricks Dashboard, Product Managers

  Business Purpose:
  Primary business output. Ranks books by audiobook potential.

  Scoring Formula:
  weighted_score = (rating     × 35%)
                 + (popularity  × 25%)
                 + (sentiment   × 25%)
                 + (length      × 15%)

  WHY four components:
  Rating (35%): Quality is primary signal.
  A bad book makes a bad audiobook regardless of popularity.

  Popularity (25%): Market validation reduces investment risk.
  LOG scale — prevents mega-popular books dominating.

  Sentiment (25%): Forward-looking reader enthusiasm.
  Currently rating-based proxy. Production: Spark NLP on
  review text for audiobook-specific sentiment signals
  like narrator quality and listening experience.

  Length (15%): Audiobook production economics.
  200-400 pages = ideal 6-12 hour audiobook.
  800+ pages = 40+ hour production = high cost high risk.
  NULL pages = neutral score — data not available.

  WHY parameterised:
  All four weights adjustable in dbt_project.yml.
  Business can shift strategy without touching SQL.
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
        -- Production: replace with Spark NLP on review text
        CASE
            WHEN average_rating >= 4.0 THEN 1.0
            WHEN average_rating >= 3.0 THEN 0.5
            ELSE 0.0
        END                                     AS sentiment_score,

        -- WHY length score:
        -- Audiobook production cost scales with length.
        -- A 1200 page book = 40+ hour recording = high risk.
        -- 200-400 pages = ideal 6-12 hour audiobook.
        -- NULL num_pages gets neutral 0.5 score.
        CASE
            WHEN num_pages IS NULL          THEN 0.5
            WHEN num_pages BETWEEN 200 AND 400 THEN 1.0
            WHEN num_pages BETWEEN 400 AND 600 THEN 0.8
            WHEN num_pages BETWEEN 100 AND 200 THEN 0.6
            WHEN num_pages BETWEEN 600 AND 800 THEN 0.4
            ELSE 0.2
        END                                     AS length_score,

        -- Length category for analyst readability
        CASE
            WHEN num_pages IS NULL             THEN 'unknown'
            WHEN num_pages BETWEEN 200 AND 400 THEN 'ideal'
            WHEN num_pages BETWEEN 400 AND 600 THEN 'good'
            WHEN num_pages BETWEEN 100 AND 200 THEN 'short'
            WHEN num_pages BETWEEN 600 AND 800 THEN 'long'
            ELSE 'very_long'
        END                                     AS length_category

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
                * {{ var('sentiment_weight') }}) +
            (length_score
                * {{ var('length_weight') }}),
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
    primary_author_id,
    primary_genre,
    average_rating,
    ratings_count,
    COALESCE(num_pages, 0)                      AS num_pages,
    length_category,
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
    ROUND(length_score
        * {{ var('length_weight') }}, 4)        AS length_contribution,
    data_quality_flag,
    _enriched_timestamp
FROM ranked
ORDER BY audiobook_rank