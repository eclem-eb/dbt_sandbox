with source as (

    select *
    from {{ source('olist', 'orders') }}

)

select *
from source
