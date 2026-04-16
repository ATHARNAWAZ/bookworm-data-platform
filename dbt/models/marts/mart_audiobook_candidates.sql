/*
  Model    : mart_audiobook_candidates
  Layer    : Gold (Mart)
  Source   : int_books_enriched
  Consumers: Power BI dashboard, Databricks SQL, Product Managers

  PRIMARY BUSINESS OUTPUT.
  Answers: Which books should BookWorm prioritise for audiobooks?

  Scoring formula:
    weighted_score = (rating     x 35%)
                   + (popularity  x 25%)
                   + (sentiment   x 25%)
                   + (length      x 15%)

  All weights are parameterised in dbt_project.yml vars.
  Change weights and run dbt run — new rankings in minutes.
  No SQL or Python knowledge required.

  Filters applied:
  - Only high_confidence and medium_confidence books (10K+ ratings)
  - Excludes comics_graphic (visual format incompatible with audio)
  - Excludes Uncategorised books
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_books_enriched') }}
),

scored AS (
    SELECT
        *,

        -- Popularity: log scale prevents mega-popular books dominating
        -- LN(4.7M) vs LN(500K) = 15.4 vs 13.1 — reasonable, not 10x
        LN(ratings_count + 1)                           AS log_ratings,
        LN(5000001)                                     AS max_log_ratings,

        -- Sentiment: real positive review percentage from 15.7M reviews
        -- Falls back to rating proxy for books with no reviews
        CASE
            WHEN positive_review_pct IS NOT NULL
              AND total_reviews > 0
                THEN positive_review_pct / 100.0
            WHEN average_rating >= 4.0 THEN 1.0
            WHEN average_rating >= 3.0 THEN 0.5
            ELSE 0.0
        END                                             AS sentiment_score,

        -- Length: audiobook production economics
        -- 200-400 pages = ideal 6-12 hour audiobook
        -- 800+ pages = 40+ hour recording = high cost, high risk
        CASE
            WHEN num_pages IS NULL                      THEN 0.5
            WHEN num_pages BETWEEN 200 AND 400          THEN 1.0
            WHEN num_pages BETWEEN 400 AND 600          THEN 0.8
            WHEN num_pages BETWEEN 100 AND 200          THEN 0.6
            WHEN num_pages BETWEEN 600 AND 800          THEN 0.4
            ELSE 0.2
        END                                             AS length_score,

        CASE
            WHEN num_pages IS NULL                      THEN 'unknown'
            WHEN num_pages BETWEEN 200 AND 400          THEN 'ideal'
            WHEN num_pages BETWEEN 400 AND 600          THEN 'good'
            WHEN num_pages BETWEEN 100 AND 200          THEN 'short'
            WHEN num_pages BETWEEN 600 AND 800          THEN 'long'
            ELSE 'very_long'
        END                                             AS length_category

    FROM enriched
    WHERE data_quality_flag IN ('high_confidence', 'medium_confidence')
      AND primary_genre NOT IN ('comics_graphic', 'Uncategorised')
),

weighted AS (
    SELECT
        *,
        ROUND(
            (average_rating / 5.0          * {{ var('rating_weight') }}) +
            (log_ratings / max_log_ratings * {{ var('popularity_weight') }}) +
            (sentiment_score               * {{ var('sentiment_weight') }}) +
            (length_score                  * {{ var('length_weight') }}),
        4)                                              AS weighted_score
    FROM scored
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            ORDER BY weighted_score DESC
        )                                               AS audiobook_rank,
        RANK() OVER (
            PARTITION BY primary_genre
            ORDER BY weighted_score DESC
        )                                               AS genre_rank
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
    num_pages,
    length_category,
    total_reviews,
    positive_review_pct,
    negative_review_pct,
    weighted_score,

    -- Score breakdown — shows WHY each book ranks where it does
    ROUND(average_rating / 5.0          * {{ var('rating_weight') }},     4) AS rating_contribution,
    ROUND(log_ratings / max_log_ratings * {{ var('popularity_weight') }}, 4) AS popularity_contribution,
    ROUND(sentiment_score               * {{ var('sentiment_weight') }},  4) AS sentiment_contribution,
    ROUND(length_score                  * {{ var('length_weight') }},     4) AS length_contribution,

    data_quality_flag,
    _enriched_timestamp

FROM ranked
ORDER BY audiobook_rank