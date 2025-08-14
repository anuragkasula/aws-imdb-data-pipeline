{{ config(materialized='table') }}

-- 1) Read from the SOURCE and extract run_key from the S3 path immediately.
with base as (
  select
    e.*,
    -- $path is only available directly on the base table scan.
    regexp_extract("$path", 'run_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as run_key
  from {{ source('imdb','analytics_episode_facts') }} e
),

-- 2) Keep only rows we care about.
filtered as (
  select *
  from base
  where seasonNumber is not null
),

-- 3) Find the most recent run across ALL rows (don’t rely on a physical run_date column).
latest as (
  select max(run_key) as run_key
  from base
)

-- 4) Aggregate metrics for the latest run.
select
  seriesid,
  seriestitle,
  series_decade,
  seasonnumber,
  avg(averagerating) as avg_rating,
  sum(numvotes)      as total_votes,
  count(*)           as episode_count
from filtered f
cross join latest l
where f.run_key = l.run_key
group by 1,2,3,4
