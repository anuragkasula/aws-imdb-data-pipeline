# Validates processed IMDb data in Athena with Great Expectations (Dataset-style).
import os
import pandas as pd
from pyathena import connect
import great_expectations as ge

# --- Config (env vars override these) ---
ATHENA_DB = os.getenv("ATHENA_DB", "imdb_processed_db")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://imdb-data-raw-ak/athena-results/")
REGION = os.getenv("AWS_REGION", "us-east-1")
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")

# --- Connect to Athena ---
conn = connect(
    s3_staging_dir=ATHENA_OUTPUT,
    region_name=REGION,
    work_group=WORKGROUP,
)

# ---------- MOVIES CHECKS ----------
movies_sql = f"""
SELECT tconst, primaryTitle, averageRating, numVotes, runtimeMinutes
FROM {ATHENA_DB}.analytics_movie_facts
WHERE decade = '2010' AND genre = 'Drama'
LIMIT 5000
"""
movies_df = pd.read_sql(movies_sql, conn)

# robust casting
for col in ["averageRating", "numVotes", "runtimeMinutes"]:
    if col in movies_df.columns:
        movies_df[col] = pd.to_numeric(movies_df[col], errors="coerce")

movies_ge = ge.from_pandas(movies_df)

movies_ge.expect_column_values_to_not_be_null("tconst")
movies_ge.expect_column_values_to_be_between(
    "averageRating", min_value=0, max_value=10, mostly=0.995
)
movies_ge.expect_column_values_to_be_between(
    "numVotes", min_value=0  # >= 0
)
movies_ge.expect_column_values_to_be_between(
    "runtimeMinutes", min_value=1, mostly=0.98  # >= 1, allow a few shorts/odd cases
)

movies_result = movies_ge.validate()

# ---------- EPISODES CHECKS ----------
episodes_sql = f"""
SELECT tconst, episodeTitle, averageRating, seasonNumber, episodeNumber
FROM {ATHENA_DB}.analytics_episode_facts
WHERE series_decade = '2010' AND seasonNumber = '1'
LIMIT 5000
"""
eps_df = pd.read_sql(episodes_sql, conn)

for col in ["averageRating", "seasonNumber", "episodeNumber"]:
    if col in eps_df.columns:
        eps_df[col] = pd.to_numeric(eps_df[col], errors="coerce")

eps_ge = ge.from_pandas(eps_df)

eps_ge.expect_column_values_to_not_be_null("tconst")
eps_ge.expect_column_values_to_be_between(
    "averageRating", min_value=0, max_value=10, mostly=0.99
)
eps_ge.expect_column_values_to_be_between("seasonNumber", min_value=1, mostly=0.99)
eps_ge.expect_column_values_to_be_between("episodeNumber", min_value=1, mostly=0.99)

eps_result = eps_ge.validate()

# ---------- Final gate ----------
if not (movies_result["success"] and eps_result["success"]):
    raise SystemExit("Great Expectations validation failed ❌")
print("Great Expectations validation passed ✅")
