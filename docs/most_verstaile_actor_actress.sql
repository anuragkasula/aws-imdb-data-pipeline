SELECT
  p.nconst,
  COUNT(DISTINCT genre) AS genre_count,
  MAX(n.primaryName) AS actor_name
FROM
  imdb_raw_db.title_principals p
JOIN
  imdb_raw_db.title_basics b
    ON p.tconst = b.tconst
JOIN
  imdb_raw_db.name_basics n
    ON p.nconst = n.nconst
CROSS JOIN
  UNNEST(split(b.genres, ',')) AS t(genre)
WHERE
  p.category IN ('actor', 'actress')
  AND b.genres IS NOT NULL AND b.genres <> '\N'
GROUP BY
  p.nconst
ORDER BY
  genre_count DESC
LIMIT 10;
