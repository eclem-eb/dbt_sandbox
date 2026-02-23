with source as (

    select *
    from {{ source('olist', 'order_items') }}

)

select *
from source
