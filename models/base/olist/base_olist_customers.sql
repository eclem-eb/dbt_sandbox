with source as (

    select *
    from {{ source('olist', 'customers') }}

)

select *
from source
