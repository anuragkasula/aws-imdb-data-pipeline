SELECT
  b.tconst,
  b.primaryTitle,
  r.averageRating,
  r.numVotes,
  b.genres
FROM
  imdb_raw_db.title_basics b
JOIN
  imdb_raw_db.title_ratings r
    ON b.tconst = r.tconst
WHERE
  b.titleType = 'movie'
  AND r.numVotes > 10000
ORDER BY
  r.averageRating DESC,
  r.numVotes DESC
LIMIT 10;
