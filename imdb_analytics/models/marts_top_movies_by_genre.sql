{{ config(materialized='table') }}

with ranked as (
  select
    genre,
    decade,
    primaryTitle,
    averageRating,
    numVotes,
    row_number() over (
      partition by genre, decade
      order by averageRating desc, numVotes desc
    ) as rn
  from {{ ref('marts_movie_facts_view') }}
  where numVotes >= 1000
)
select *
from ranked
where rn <= 25
