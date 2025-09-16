{{ config(materialized='table', tags=['anl']) }}

WITH base AS (
  SELECT
    TCONST                               AS MOVIE_KEY,
    PRIMARYTITLE,
    ORIGINALTITLE,
    STARTYEAR,
    DECADE,
    RUNTIMEMINUTES::NUMBER              AS RUNTIME_MIN,
    GENRE,
    GENRES,
    AVERAGERATING::FLOAT                AS AVG_RATING,
    NUMVOTES::NUMBER                    AS NUM_VOTES,
    DIRECTORS,
    WRITERS,
    TOPACTORS,
    TOPACTORS_NCONST,
    OSCARWINNER,
    RUN_DATE_DT                         AS RUN_DATE
  FROM {{ source('imdb','MOVIE_FACTS') }}
)
SELECT
  *,
  /* convenience labels for visuals */
  TO_CHAR(DECADE) || 's' AS DECADE_LABEL,
  CASE
    WHEN NUM_VOTES >= 50000 THEN '50k+'
    WHEN NUM_VOTES >= 10000 THEN '10k–49k'
    WHEN NUM_VOTES >= 1000  THEN '1k–9k'
    WHEN NUM_VOTES IS NULL  THEN 'unknown'
    ELSE '<1k'
  END AS VOTE_BUCKET,
  CASE
    WHEN AVG_RATING >= 8 THEN 'Excellent'
    WHEN AVG_RATING >= 7 THEN 'Great'
    WHEN AVG_RATING >= 6 THEN 'Good'
    WHEN AVG_RATING IS NULL THEN 'unknown'
    ELSE 'OK'
  END AS RATING_BAND
FROM base
