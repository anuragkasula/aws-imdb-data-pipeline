# Glue Job: Advanced ETL for IMDb Analytics Portfolio
# Features: Partitioning, Explode Genres, Decade Calculation, Data Quality, Oscar Flag
# Author: Anurag Kasula, 2025

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, collect_list, concat_ws, when, countDistinct, desc, lower, lit,
    regexp_extract, length, row_number
)
from awsglue.context import GlueContext
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ====== S3 Paths ======
RAW = "s3://imdb-data-raw-<your-bucket>/raw/"
PROC = "s3://imdb-data-raw-<your-bucket>/processed/"

# ====== 1. Load Data ======
df_basics = spark.read.option("delimiter", "\t").option("header", True).csv(RAW + "title_basics/title.basics.tsv.gz")
df_ratings = spark.read.option("delimiter", "\t").option("header", True).csv(RAW + "title_ratings/title.ratings.tsv.gz")
df_crew = spark.read.option("delimiter", "\t").option("header", True).csv(RAW + "title_crew/title.crew.tsv.gz")
df_principals = spark.read.option("delimiter", "\t").option("header", True).csv(RAW + "title_principals/title.principals.tsv.gz")
df_names = spark.read.option("delimiter", "\t").option("header", True).csv(RAW + "name_basics/name.basics.tsv.gz")
df_akas = spark.read.option("delimiter", "\t").option("header", True).csv(RAW + "title_akas/title.akas.tsv.gz")

# ====== 2. Clean/Transform ======

# Filter only movies with required fields
movies = (
    df_basics
    .filter(
        (col("titleType") == "movie") &
        (col("startYear").isNotNull()) &
        (col("primaryTitle").isNotNull()) &
        (col("genres").isNotNull()) &
        (col("startYear") != "\\N") &
        (col("genres") != "\\N")
    )
    .withColumn("startYear", col("startYear").cast("int"))
    .withColumn("runtimeMinutes", when(col("runtimeMinutes") != "\\N", col("runtimeMinutes")).otherwise(None).cast("int"))
)

# Calculate "decade"
movies = movies.withColumn("decade", (col("startYear") / 10).cast("int") * 10)

# Join with ratings
movies = (
    movies.join(df_ratings, on="tconst", how="left")
    .withColumn("averageRating", col("averageRating").cast("double"))
    .withColumn("numVotes", col("numVotes").cast("int"))
)

# Join with crew
movies = (
    movies.join(df_crew, on="tconst", how="left")
)

# Explode genres (one row per movie-genre)
movies = (
    movies.withColumn("genre", explode(split(col("genres"), ",")))
)

# ====== 3. Get Top 3 Actors per Movie ======
main_cast = (
    df_principals
    .filter(col("category").isin("actor", "actress"))
    .filter((col("ordering").cast("int") <= 3))
    .select("tconst", "nconst", "ordering")
    .join(df_names.select("nconst", "primaryName"), on="nconst", how="left")
)

w = Window.partitionBy("tconst").orderBy("ordering")
main_cast = main_cast.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3)

cast_agg = (
    main_cast
    .groupBy("tconst")
    .agg(
        concat_ws(", ", collect_list("primaryName")).alias("topActors"),
        concat_ws(", ", collect_list("nconst")).alias("topActors_nconst")
    )
)

movies = movies.join(cast_agg, on="tconst", how="left")

# ====== 4. Oscar Winner Flag (from alternate titles) ======
## "Oscar" or "Academy Award" in alternate title (case-insensitive)
oscar_titles = (
    df_akas
    .filter(
        (lower(col("title")).like("%oscar%")) | (lower(col("title")).like("%academy award%"))
    )
    .select("titleId")
    .distinct()
    .withColumn("oscarWinner", lit(1))
)

movies = movies.join(oscar_titles, movies.tconst == oscar_titles.titleId, how="left") \
    .drop("titleId") \
    .withColumn("oscarWinner", when(col("oscarWinner") == 1, lit(1)).otherwise(lit(0)))

# ====== 5. Data Quality Metrics (row counts, NULLs) ======
row_count = movies.count()
num_null_titles = movies.filter(col("primaryTitle").isNull() | (length(col("primaryTitle")) == 0)).count()
num_null_ratings = movies.filter(col("averageRating").isNull()).count()

quality_summary = spark.createDataFrame([{
    "row_count": row_count,
    "num_null_titles": num_null_titles,
    "num_null_ratings": num_null_ratings
}])

quality_summary.write.mode("overwrite").parquet(PROC + "analytics_movie_facts_quality/")

# ====== 6. Write Final Analytics Table: Partitioned by Decade, Genre ======
output_path = PROC + "analytics_movie_facts/"

analytics = (
    movies
    .select(
        "tconst", "primaryTitle", "originalTitle", "startYear", "decade", "runtimeMinutes",
        "genre", "genres", "averageRating", "numVotes", "directors", "writers",
        "topActors", "topActors_nconst", "oscarWinner"
    )
)

(
    analytics
    .write
    .partitionBy("decade", "genre")
    .mode("overwrite")
    .parquet(output_path)
)

print("✅ Advanced analytics table written to", output_path)
print("✅ Data quality summary written to", PROC + "analytics_movie_facts_quality/")
