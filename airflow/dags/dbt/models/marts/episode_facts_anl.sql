{{ config(materialized='table', tags=['anl']) }}

SELECT
  TCONST                        AS EPISODE_KEY,
  SERIESID                      AS SERIES_KEY,
  SERIESTITLE,
  SERIESSTARTYEAR,
  SERIES_DECADE,
  SEASONNUMBER,
  EPISODENUMBER,
  EPISODETITLE,
  EPISODEYEAR,
  AVERAGERATING                 AS AVG_RATING,
  NUMVOTES                      AS NUM_VOTES,
  ISPILOT,
  ISFINALE,
  ISSPECIAL,
  RUN_DATE_DT                   AS RUN_DATE
FROM {{ source('imdb','EPISODE_FACTS') }}
