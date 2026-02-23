with source as (

    select *
    from {{ source('olist', 'order_reviews') }}

)

select *
from source
