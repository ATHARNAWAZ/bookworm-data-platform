/*
  Model    : int_books_enriched
  Layer    : Intermediate (Silver)
  Sources  : stg_books, stg_reviews, stg_genres
  Consumers: All Gold marts

  Single enriched book entity combining:
  - Book metadata from stg_books
  - Primary genre from stg_genres
  - Real sentiment metrics from 15.7M reviews in stg_reviews

  All Gold marts reference this one model.
  Adding a new Gold mart = write one SQL file, reference int_books_enriched.
  No re-joining ever needed.
*/

WITH books AS (
    SELECT * FROM {{ ref('stg_books') }}
),

genres AS (
    SELECT * FROM {{ ref('stg_genres') }}
),

review_metrics AS (
    SELECT
        book_id,
        COUNT(review_id)                                AS total_reviews,
        ROUND(AVG(CAST(rating AS DOUBLE)), 2)           AS avg_review_rating,

        -- Percentage of reviews that are positive
        -- This is the real sentiment signal from 15.7M reader opinions
        ROUND(
            SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)
            * 100.0 / COUNT(review_id), 2
        )                                               AS positive_review_pct,

        ROUND(
            SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END)
            * 100.0 / COUNT(review_id), 2
        )                                               AS neutral_review_pct,

        ROUND(
            SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)
            * 100.0 / COUNT(review_id), 2
        )                                               AS negative_review_pct,

        MIN(review_date)                                AS first_review_date,
        MAX(review_date)                                AS latest_review_date

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
        b.language_code,
        b.publisher,
        b.format,
        b.description,
        b.data_quality_flag,

        -- Genre: use dedicated genres table, fall back to shelf_genre
        COALESCE(
            g.primary_genre,
            b.shelf_genre,
            'Uncategorised'
        )                                               AS primary_genre,

        -- Real sentiment from reviews
        COALESCE(r.total_reviews, 0)                    AS total_reviews,
        COALESCE(r.avg_review_rating, 0)                AS avg_review_rating,
        COALESCE(r.positive_review_pct, 0)              AS positive_review_pct,
        COALESCE(r.neutral_review_pct, 0)               AS neutral_review_pct,
        COALESCE(r.negative_review_pct, 0)              AS negative_review_pct,
        r.first_review_date,
        r.latest_review_date,

        b._ingestion_timestamp,
        CURRENT_TIMESTAMP()                             AS _enriched_timestamp

    FROM books b
    LEFT JOIN genres g ON b.book_id = g.book_id
    LEFT JOIN review_metrics r ON b.book_id = r.book_id
)

SELECT * FROM enriched