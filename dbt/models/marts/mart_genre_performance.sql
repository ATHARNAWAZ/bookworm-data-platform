/*
  Model    : mart_genre_performance
  Layer    : Gold (Mart)
  Source   : int_books_enriched
  Consumers: Power BI dashboard, Product Strategy

  Genre-level portfolio view.
  Answers: Which genres should BookWorm focus their audiobook programme on?

  Excludes comics_graphic — visual format incompatible with audiobooks.
*/

WITH enriched AS (
    SELECT * FROM {{ ref('int_books_enriched') }}
),

genre_metrics AS (
    SELECT
        primary_genre                                   AS genre,
        COUNT(DISTINCT book_id)                         AS total_books,
        ROUND(AVG(average_rating), 2)                   AS avg_rating,
        SUM(ratings_count)                              AS total_ratings,
        ROUND(AVG(positive_review_pct), 1)              AS avg_positive_pct,
        ROUND(AVG(negative_review_pct), 1)              AS avg_negative_pct,
        ROUND(AVG(
            (average_rating / 5.0 * {{ var('rating_weight') }}) +
            (LN(ratings_count + 1) / LN(5000001) * {{ var('popularity_weight') }}) +
            (CASE
                WHEN positive_review_pct IS NOT NULL AND total_reviews > 0
                    THEN positive_review_pct / 100.0
                WHEN average_rating >= 4.0 THEN 1.0
                WHEN average_rating >= 3.0 THEN 0.5
                ELSE 0.0
             END * {{ var('sentiment_weight') }})
        ), 4)                                           AS avg_weighted_score

    FROM enriched
    WHERE data_quality_flag IN ('high_confidence', 'medium_confidence')
      AND primary_genre NOT IN ('comics_graphic', 'Uncategorised')
    GROUP BY primary_genre
)

SELECT
    RANK() OVER (ORDER BY avg_weighted_score DESC)      AS genre_rank,
    genre,
    total_books,
    avg_rating,
    total_ratings,
    avg_positive_pct,
    avg_negative_pct,
    avg_weighted_score,
    CURRENT_TIMESTAMP()                                 AS _mart_timestamp
FROM genre_metrics
ORDER BY genre_rank