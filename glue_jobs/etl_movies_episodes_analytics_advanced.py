# Glue Job: Advanced ETL for IMDb Movies + Episodes (append-only)
# Outputs:
#   processed/analytics_movie_facts/         (partitioned by decade, genre)
#   processed/analytics_episode_facts/       (partitioned by series_decade, seasonNumber)
#   processed/series_season_summary/         (per-series rollups)
#   processed/analytics_quality/           (data quality snapshots)
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
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ---- Glue / Spark bootstrap ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- File size / task tuning (fewer S3 PUT/GET) ---
spark.conf.set("spark.sql.shuffle.partitions", 96)        # lower than default 200
spark.conf.set("spark.sql.files.maxRecordsPerFile", 5_000_000)  # cap rows per output file

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
RAW  = "s3://imdb-data-raw-ak/imdb/raw/"
PROC = "s3://imdb-data-raw-ak/imdb/processed/"

# ---- Load raw TSVs (header + tab-delimited) ----
read_opts = {"delimiter": "\t", "header": "true"}
b    = spark.read.options(**read_opts).csv(RAW + "title_basics/latest/title.basics.tsv.gz")
r    = spark.read.options(**read_opts).csv(RAW + "title_ratings/latest/title.ratings.tsv.gz")
crew = spark.read.options(**read_opts).csv(RAW + "title_crew/latest/title.crew.tsv.gz")
ppl  = spark.read.options(**read_opts).csv(RAW + "name_basics/latest/name.basics.tsv.gz")
prin = spark.read.options(**read_opts).csv(RAW + "title_principals/latest/title.principals.tsv.gz")
akas = spark.read.options(**read_opts).csv(RAW + "title_akas/latest/title.akas.tsv.gz")
epi  = spark.read.options(**read_opts).csv(RAW + "title_episode/latest/title.episode.tsv.gz")

# =========================
# DATA QUALITY
# =========================


def dq_metrics(
    df: DataFrame,
    dataset: str,
    run_date: str,
    *,
    numeric_cols=None,
    key_cols=None,
    text_cols=None
) -> DataFrame:
    r"""
    Creates a one-row DQ summary for a dataframe.
    - row_count
    - null_* for key/text/numeric columns (treats \\N and empty strings as null)
    - min_*/max_* for numeric columns
    - distinct_* for key/text columns (approximate)
    """
    numeric_cols = numeric_cols or []
    key_cols     = key_cols or []
    text_cols    = text_cols or []

    # Everything we’ll scan once
    agg_exprs = [F.count(F.lit(1)).alias("row_count")]

    def is_nullish(c):
        col = F.col(c)
        return (
            col.isNull() |
            (col == F.lit("\\N")) |
            (F.length(F.trim(col)) == 0)
        )

    for c in (key_cols + text_cols + numeric_cols):
        agg_exprs.append(F.sum(F.when(is_nullish(c), F.lit(1)).otherwise(F.lit(0))).alias(f"null_{c}"))

    for c in numeric_cols:
        agg_exprs.append(F.min(F.col(c)).alias(f"min_{c}"))
        agg_exprs.append(F.max(F.col(c)).alias(f"max_{c}"))

    for c in (key_cols + text_cols):
        agg_exprs.append(F.approx_count_distinct(F.col(c)).alias(f"distinct_{c}"))

    out = df.agg(*agg_exprs)\
            .withColumn("dataset", F.lit(dataset))\
            .withColumn("run_date", F.lit(run_date))

    # Standardize column order (nice to read in Athena/Snowflake)
    cols = ["dataset", "run_date", "row_count"] + [c for c in out.columns if c not in {"dataset","run_date","row_count"}]
    return out.select(*cols)



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

cast_agg = (
    main_cast.groupBy("tconst")
    .agg(
        F.sort_array(F.collect_list(F.struct("ordering","primaryName"))).alias("names"),
        F.sort_array(F.collect_list(F.struct("ordering","nconst"))).alias("ids")
    )
    .select(
        "tconst",
        F.expr("concat_ws(', ', transform(names, x -> x.primaryName))").alias("topActors"),
        F.expr("concat_ws(', ', transform(ids,   x -> x.nconst))").alias("topActors_nconst")
    )
)

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

# Final movie analytics selection (+ run_date)
movies_out = movies.select(
    "tconst","primaryTitle","originalTitle","startYear","decade","runtimeMinutes",
    "genre","genres","averageRating","numVotes","directors","writers",
    "topActors","topActors_nconst","oscarWinner"
).withColumn("run_date", lit(run_date))

movies_out   = movies_out.withColumn("run_date_dt", F.to_date(F.col("run_date"), "yyyyMMdd"))

# APPEND (no overwrite), keep same partitioning
# Align shuffle to partition columns, then write by run_date for narrow daily loads
(movies_out
    .repartition("decade", "genre")                 # helps create 1–few files per partition
    .write
    .partitionBy("run_date", "decade", "genre")     # new: run_date as top partition
    .mode("append")
    .parquet(PROC + "analytics_movie_facts_v2/")    # new v2 folder
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
    prin.filter(col("category").isin("actor","actress"))
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

episodes_out = episodes_out.withColumn(
    "isSpecial", F.col("seasonNumber").isNull().cast("int")
).withColumn(
    "seasonNumber", F.when(F.col("seasonNumber").isNull(), F.lit(-1)).otherwise(F.col("seasonNumber"))
)

episodes_out = episodes_out.withColumn("run_date_dt", F.to_date(F.col("run_date"), "yyyyMMdd"))

# APPEND (no overwrite), keep same partitioning
(episodes_out
    .repartition("series_decade", "seasonNumber")
    .write
    .partitionBy("run_date", "series_decade", "seasonNumber")
    .mode("append")
    .parquet(PROC + "analytics_episode_facts_v2/")
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

season_rollup= season_rollup.withColumn("run_date_dt", F.to_date(F.col("run_date"), "yyyyMMdd"))

(season_rollup
    .repartition("series_decade")
    .write
    .partitionBy("run_date", "series_decade")
    .mode("append")
    .parquet(PROC + "series_season_summary_v2/")
)

dq_movies = dq_metrics(
    movies_out,
    dataset="analytics_movie_facts",
    run_date=run_date,
    numeric_cols=["startYear","decade","runtimeMinutes","averageRating","numVotes"],
    key_cols=["tconst"],
    text_cols=["primaryTitle","genre","directors","writers","topActors"]
)

dq_eps = dq_metrics(
    episodes_out,
    dataset="analytics_episode_facts",
    run_date=run_date,
    numeric_cols=["seriesStartYear","seasonNumber","episodeNumber","episodeYear","averageRating","numVotes"],
    key_cols=["tconst","seriesId"],
    text_cols=["seriesTitle","episodeTitle","seriesGenres"]
)

dq_seasons = dq_metrics(
    season_rollup,
    dataset="series_season_summary",
    run_date=run_date,
    numeric_cols=["seasonNumber","episodesInSeason","avgSeasonRating","seasonStartYear","seasonEndYear"],
    key_cols=["seriesId"],
    text_cols=["seriesTitle"]
)

(
    dq_movies
      .unionByName(dq_eps, allowMissingColumns=True)
      .unionByName(dq_seasons, allowMissingColumns=True)
      .write
      .mode("append")
      .partitionBy("dataset","run_date")
      .parquet(PROC + "analytics_quality/")
)

print("✅ DQ snapshots (append)     ->", PROC + "analytics_quality/")
print("✅ Movies written (append)   ->", PROC + "analytics_movie_facts/")
print("✅ Episodes written (append) ->", PROC + "analytics_episode_facts/")
print("✅ Season rollups (append)   ->", PROC + "series_season_summary/")