{{
    config(
        materialized = 'external',
        plugin = 'delta',
        format = 'delta',
        mode = 'overwrite',
        location = './data/my_destination_table_2' 
    )    
}}

select *
from {{ source('landing','my_source_table_2') }}
