with source as (

    select *
    from {{ source('olist', 'products') }}

)

select *
from source
