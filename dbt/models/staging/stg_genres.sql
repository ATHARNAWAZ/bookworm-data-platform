WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_genres') }}
),

ranked AS (
    SELECT
        book_id,
        genre_name,
        genre_count,
        ROW_NUMBER() OVER (
            PARTITION BY book_id
            ORDER BY genre_count DESC
        ) AS rn
    FROM source
    UNPIVOT (
        genre_count FOR genre_name IN (
            genre_children,
            genre_comics_graphic,
            genre_fantasy_paranormal,
            genre_fiction,
            genre_history_historical_fiction_biography,
            genre_mystery_thriller_crime,
            genre_poetry,
            genre_romance
        )
    )
    WHERE genre_count > 0
),

ranked_final AS (
    SELECT
        book_id,
        REGEXP_REPLACE(genre_name, '^genre_', '') AS primary_genre
    FROM ranked
    WHERE rn = 1
)

SELECT * FROM ranked_final