{{ config(materialized='table') }}

with source_data as (
    select * from {{ source("web_crawlers", "test") }}
)

select *
from source_data