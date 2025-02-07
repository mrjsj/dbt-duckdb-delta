
{{ config(materialized='table') }}

select * from {{ ref('my_destination_table_1') }}

union all by name

select * from {{ ref('my_destination_table_2') }}

