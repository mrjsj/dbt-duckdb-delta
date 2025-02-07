{{
    config(
        materialized = 'external',
        format = 'delta',
        plugin = 'delta',
        mode = 'overwrite',
        location = "./data/my_destination_table_2",
    )    
}}

select *
from {{ source('landing','my_source_table_1') }}
