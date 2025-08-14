# Glue Job: Advanced ETL for IMDb Movies + Episodes (append-only)
# Outputs:
#   processed/analytics_movie_facts/         (partitioned by decade, genre)
#   processed/analytics_episode_facts/       (partitioned by series_decade, seasonNumber)
#   processed/series_season_summary/         (per-series rollups)
#   processed/analytics_*_quality/           (data quality snapshots)
#
# Features:
# - Lakehouse zones (raw -> processed), Spark joins, explode(), window functions
# - Feature engineering (decade, oscar flag, genre explode, pilot/finale flags)
# - Partitioning strategy for cheap Athena/Tableau
# - DQ metrics as first-class artifacts

import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, concat_ws, collect_list, when, lower, lit, length,
    row_number, avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window

# ---- Glue / Spark bootstrap ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---- Resolve run_date (YYYYMMDD) ----
# Airflow passes --run_ts; fall back to UTC today if not present (manual runs)
run_date = datetime.utcnow().strftime('%Y%m%d')
if '--run_ts' in sys.argv:
    try:
        args = getResolvedOptions(sys.argv, ['run_ts'])
        run_date = args['run_ts'][:8]
    except Exception:
        pass
print(f"[GLUE] Using run_date={run_date}")

# ---- S3 paths ----
RAW  = "s3://imdb-data-raw-ak/raw/"
PROC = "s3://imdb-data-raw-ak/processed/"

# ---- Load raw TSVs (header + tab-delimited) ----
read_opts = {"delimiter": "\t", "header": "true"}
b    = spark.read.options(**read_opts).csv(RAW + "title_basics/title.basics.tsv.gz")
r    = spark.read.options(**read_opts).csv(RAW + "title_ratings/title.ratings.tsv.gz")
crew = spark.read.options(**read_opts).csv(RAW + "title.crew/title.crew.tsv.gz")
ppl  = spark.read.options(**read_opts).csv(RAW + "name.basics/name.basics.tsv.gz")
prin = spark.read.options(**read_opts).csv(RAW + "title_principals/title.principals.tsv.gz")
akas = spark.read.options(**read_opts).csv(RAW + "title_akas/title.akas.tsv.gz")
epi  = spark.read.options(**read_opts).csv(RAW + "title.episode/title.episode.tsv.gz")

# =========================
# MOVIES PIPELINE
# =========================

movies = (
    b.filter(
        (col("titleType") == "movie") &
        (col("startYear").isNotNull()) & (col("startYear") != "\\N") &
        (col("primaryTitle").isNotNull()) &
        (col("genres").isNotNull()) & (col("genres") != "\\N")
    )
    .withColumn("startYear", col("startYear").cast("int"))
    .withColumn("runtimeMinutes",
                when(col("runtimeMinutes") != "\\N", col("runtimeMinutes")).otherwise(None).cast("int"))
    .withColumn("decade", (col("startYear")/10).cast("int")*10)
)

# ratings
movies = (
    movies.join(r.select("tconst","averageRating","numVotes"), on="tconst", how="left")
          .withColumn("averageRating", col("averageRating").cast("double"))
          .withColumn("numVotes", col("numVotes").cast("int"))
)

# crew (directors/writers are comma-separated nconsts)
movies = movies.join(crew.select("tconst","directors","writers"), on="tconst", how="left")

# top 3 actors/actresses
main_cast = (
    prin.filter(col("category").isin("actor","actress"))
        .withColumn("ordering", when(col("ordering")=="\\N", None).otherwise(col("ordering")).cast("int"))
        .filter(col("ordering") <= 3)
        .select("tconst","nconst","ordering")
        .join(ppl.select("nconst","primaryName"), on="nconst", how="left")
)

w_cast = Window.partitionBy("tconst").orderBy("ordering")
main_cast = main_cast.withColumn("rn", row_number().over(w_cast)).filter(col("rn")<=3)

cast_agg = (main_cast.groupBy("tconst")
            .agg(concat_ws(", ", collect_list("primaryName")).alias("topActors"),
                 concat_ws(", ", collect_list("nconst")).alias("topActors_nconst")))

movies = movies.join(cast_agg, on="tconst", how="left")

# explode genres for per-genre analytics/partitioning
movies = movies.withColumn("genre", explode(split(col("genres"), ",")))

# Oscar/Academy flag from alternate titles
oscar_titles = (
    akas.filter( lower(col("title")).like("%oscar%") | lower(col("title")).like("%academy award%") )
        .select("titleId").distinct()
        .withColumn("oscarWinner", lit(1))
)
movies = (movies.join(oscar_titles, movies.tconst == oscar_titles.titleId, "left")
                .drop("titleId")
                .withColumn("oscarWinner", when(col("oscarWinner")==1, lit(1)).otherwise(lit(0))))

# Data Quality snapshot (movies)
movies_row_count      = movies.count()
movies_null_titles    = movies.filter((col("primaryTitle").isNull()) | (length(col("primaryTitle"))==0)).count()
movies_null_ratings   = movies.filter(col("averageRating").isNull()).count()
dq_movies = spark.createDataFrame([{
    "row_count": movies_row_count,
    "num_null_titles": movies_null_titles,
    "num_null_ratings": movies_null_ratings
}])
dq_movies.write.mode("overwrite").parquet(PROC + "analytics_movie_facts_quality/")

# Final movie analytics selection (+ run_date)
movies_out = movies.select(
    "tconst","primaryTitle","originalTitle","startYear","decade","runtimeMinutes",
    "genre","genres","averageRating","numVotes","directors","writers",
    "topActors","topActors_nconst","oscarWinner"
).withColumn("run_date", lit(run_date))

# APPEND (no overwrite), keep same partitioning
(movies_out.write
    .partitionBy("decade","genre")
    .mode("append")
    .parquet(PROC + "analytics_movie_facts/")
)

# =========================
# EPISODES PIPELINE
# =========================

episodes = (
    epi.select("tconst","parentTconst","seasonNumber","episodeNumber")
       .withColumn("seasonNumber", when(col("seasonNumber")=="\\N", None).otherwise(col("seasonNumber")).cast("int"))
       .withColumn("episodeNumber", when(col("episodeNumber")=="\\N", None).otherwise(col("episodeNumber")).cast("int"))
)

# Episode metadata from basics/ratings
ep_meta = (
    episodes.join(b.select(
                    col("tconst").alias("ep_tconst"),
                    col("primaryTitle").alias("episodeTitle"),
                    col("startYear").alias("episodeYear"),
                    col("titleType").alias("epType")
                  ),
                  episodes.tconst == col("ep_tconst"), "left")
             .drop("ep_tconst")
             .join(r.select(col("tconst").alias("rt_tconst"), "averageRating","numVotes"),
                   episodes.tconst == col("rt_tconst"), "left")
             .drop("rt_tconst")
)

# Parent series title & startYear (for series decade)
series_meta = b.select(
    col("tconst").alias("seriesId"),
    col("primaryTitle").alias("seriesTitle"),
    col("startYear").alias("seriesStartYear"),
    col("genres").alias("seriesGenres")
)

ep_full = (
    ep_meta.join(series_meta, ep_meta.parentTconst == series_meta.seriesId, "left")
           .withColumn("episodeYear", when(col("episodeYear")=="\\N", None).otherwise(col("episodeYear")).cast("int"))
           .withColumn("seriesStartYear", when(col("seriesStartYear")=="\\N", None).otherwise(col("seriesStartYear")).cast("int"))
           .withColumn("series_decade", (col("seriesStartYear")/10).cast("int")*10)
           .withColumn("averageRating", col("averageRating").cast("double"))
           .withColumn("numVotes", col("numVotes").cast("int"))
)

# Pilot / Finale flags
ep_full = (
    ep_full.withColumn("isPilot",  when(col("episodeNumber")==1, lit(1)).otherwise(lit(0)))
           .withColumn("isFinale", when( (col("seasonNumber").isNotNull()) &
                                         (col("episodeNumber").isNotNull()),
                                         lit(0)).otherwise(lit(0)))  # placeholder, overwrite below
)

# Finale per (series, season): max episodeNumber in that season
w_season = Window.partitionBy("seriesId","seasonNumber")
ep_full = ep_full.withColumn("maxEpisodeInSeason", spark_max("episodeNumber").over(w_season))
ep_full = ep_full.withColumn("isFinale", when(col("episodeNumber")==col("maxEpisodeInSeason"), lit(1)).otherwise(col("isFinale")))

# Optional top-3 episode actors
ep_cast = (
    prin.filter(col("tconst").isNotNull())
        .withColumn("ordering", when(col("ordering")=="\\N", None).otherwise(col("ordering")).cast("int"))
        .filter(col("ordering").isNotNull() & (col("ordering")<=3))
        .select("tconst","nconst","ordering")
        .join(ppl.select("nconst","primaryName"), on="nconst", how="left")
)
w_ep = Window.partitionBy("tconst").orderBy("ordering")
ep_cast = ep_cast.withColumn("rn", row_number().over(w_ep)).filter(col("rn")<=3)
ep_cast_agg = (ep_cast.groupBy("tconst")
               .agg(concat_ws(", ", collect_list("primaryName")).alias("epTopActors")))
ep_full = ep_full.join(ep_cast_agg, on="tconst", how="left")

# Episode analytics (+ run_date)
episodes_out = ep_full.select(
    "tconst",
    "episodeTitle","episodeYear",
    "parentTconst","seriesId","seriesTitle","seriesStartYear","seriesGenres","series_decade",
    "seasonNumber","episodeNumber","averageRating","numVotes",
    "isPilot","isFinale","epTopActors"
).withColumn("run_date", lit(run_date))

# APPEND (no overwrite), keep same partitioning
(episodes_out.write
    .partitionBy("series_decade","seasonNumber")
    .mode("append")
    .parquet(PROC + "analytics_episode_facts/")
)

# ---- Per-series, per-season rollups (+ run_date) ----
season_rollup = (
    episodes_out.groupBy("seriesId","seriesTitle","series_decade","seasonNumber")
                .agg(
                    count("*").alias("episodesInSeason"),
                    avg("averageRating").alias("avgSeasonRating"),
                    spark_min("episodeYear").alias("seasonStartYear"),
                    spark_max("episodeYear").alias("seasonEndYear")
                )
).withColumn("run_date", lit(run_date))

(season_rollup.write
    .partitionBy("series_decade")
    .mode("append")
    .parquet(PROC + "series_season_summary/")
)

print("✅ Movies written (append)   ->", PROC + "analytics_movie_facts/")
print("✅ Episodes written (append) ->", PROC + "analytics_episode_facts/")
print("✅ Season rollups (append)   ->", PROC + "series_season_summary/")
print("✅ DQ snapshots (overwrite)  ->", PROC + "analytics_*_quality/")