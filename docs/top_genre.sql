SELECT
  genre,
  COUNT(*) AS num_titles
FROM
  imdb_raw_db.title_basics
CROSS JOIN UNNEST(split(genres, ',')) AS t(genre)
WHERE
  genres IS NOT NULL AND genres <> '\N'
GROUP BY
  genre
ORDER BY
  num_titles DESC
LIMIT 10;
