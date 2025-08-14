{{ config(materialized='view') }}

with base as (
  select
    m.*,
    regexp_extract("$path", 'run_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as run_key
  from {{ source('imdb','analytics_movie_facts') }} m
),
latest as (
  select max(run_key) as run_key from base
)

select b.*
from base b
cross join latest l
where b.run_key = l.run_key
  and b.averageRating is not null
  and b.numVotes is not null
  and b.numVotes >= 0
