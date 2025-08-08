SELECT
  primaryTitle,
  startYear,
  endYear,
  (CAST(endYear AS INT) - CAST(startYear AS INT)) AS run_years
FROM
  imdb_raw_db.title_basics
WHERE
  titleType = 'tvSeries'
  AND startYear <> '\N'
  AND endYear <> '\N'
ORDER BY
  run_years DESC
LIMIT 10;
